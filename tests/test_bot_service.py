import json
import sqlite3
import stat
import time
from pathlib import Path

import pytest

from app.bot import BotService, direct_media_size_error
from app.config import Settings
from app.storage import identity_token, prepare_directories


class FakeBot:
    def __init__(self, content: bytes):
        self.content = content
        self.messages = []
        self.download_calls = 0

    async def download(self, *, file, destination):
        self.download_calls += 1
        Path(destination).write_bytes(self.content)
        return destination

    async def send_message(self, chat_id, text):
        self.messages.append((chat_id, text))


def make_settings(tmp_path: Path) -> Settings:
    return Settings(
        bot_token="test-token",
        api_id=12345,
        api_hash="test-hash",
        owner_telegram_id=123456,
        data_dir=tmp_path / "state",
        complete_dir=tmp_path / "downloads" / "complete" / "telegram",
        incomplete_dir=tmp_path / "downloads" / "incomplete" / "telegram",
    )


@pytest.mark.asyncio
async def test_direct_media_is_fsynced_and_atomically_promoted(tmp_path: Path, monkeypatch):
    content = b"telegram test media" * 128
    settings = make_settings(tmp_path)
    prepare_directories(settings.complete_dir, settings.incomplete_dir)
    service = BotService(settings, client=object())
    task_id = service.tasks.enqueue(
        "direct-media",
        json.dumps(
            {
                "file_id": "opaque-file-id",
                "filename": "sample.bin",
                "file_size": len(content),
                "source_key": "bot-123-456",
            }
        ),
    )
    task = service.tasks.claim_next()
    bot = FakeBot(content)
    identity = identity_token("bot-api-file-id:opaque-file-id")
    final = settings.complete_dir / f"bot-123-456-{identity}-sample.bin"
    opened_modes = []
    original_open = Path.open

    def track_open(path, mode="r", *args, **kwargs):
        if path == final:
            opened_modes.append(mode)
        return original_open(path, mode, *args, **kwargs)

    monkeypatch.setattr(Path, "open", track_open)

    detail = await service.execute_task(task, bot)

    partial = settings.incomplete_dir / f"bot-123-456-{identity}.part"
    assert task.task_id == task_id
    assert detail == final.name
    assert final.read_bytes() == content
    assert "rb+" in opened_modes
    assert stat.S_IMODE(final.stat().st_mode) == 0o644
    assert stat.S_IMODE(settings.complete_dir.stat().st_mode) == 0o755
    assert not partial.exists()
    [stored] = service.tasks.recent()
    assert stored.progress_bytes == stored.total_bytes == len(content)
    assert stored.current_file == "sample.bin"
    assert bot.messages and bot.messages[0][0] == settings.owner_telegram_id


@pytest.mark.asyncio
async def test_direct_media_rejects_size_mismatch_for_worker_retry(tmp_path: Path):
    settings = make_settings(tmp_path)
    service = BotService(settings, client=object())
    task_id = service.tasks.enqueue(
        "direct-media",
        json.dumps(
            {
                "file_id": "opaque-file-id",
                "filename": "sample.bin",
                "file_size": 100,
                "source_key": "bot-123-789",
            }
        ),
    )
    task = service.tasks.claim_next()
    bot = FakeBot(b"short")

    from app.worker import RetryableTaskError

    with pytest.raises(RetryableTaskError, match="file size mismatch"):
        await service.execute_task(task, bot)

    [stored] = service.tasks.recent()
    assert stored.task_id == task_id
    assert stored.status == "downloading"
    assert stored.progress_bytes == 5
    assert stored.total_bytes == 100


@pytest.mark.asyncio
async def test_mtproto_authorization_waits_for_persisted_global_cooldown(tmp_path: Path):

    class FakeClient:
        def __init__(self):
            self.calls = 0

        async def is_user_authorized(self):
            self.calls += 1
            return True

    client = FakeClient()
    service = BotService(make_settings(tmp_path), client)
    service.note_flood_wait(0.05)

    started = time.monotonic()
    assert await service.is_authorized() is True
    elapsed = time.monotonic() - started

    assert elapsed >= 0.04
    assert client.calls == 1
    assert service.tasks.cooldown_remaining() <= 0.01


@pytest.mark.asyncio
async def test_cancel_during_completion_notification_keeps_download_completed(tmp_path: Path):
    import asyncio

    class BlockingNotificationBot(FakeBot):
        def __init__(self, content: bytes):
            super().__init__(content)
            self.notification_started = asyncio.Event()

        async def send_message(self, chat_id, text):
            self.notification_started.set()
            await asyncio.Future()

    content = b"durable media" * 128
    settings = make_settings(tmp_path)
    prepare_directories(settings.complete_dir, settings.incomplete_dir)
    service = BotService(settings, client=object())
    task_id = service.tasks.enqueue(
        "direct-media",
        json.dumps({
            "file_id": "opaque-file-id",
            "filename": "committed.bin",
            "file_size": len(content),
            "source_key": "bot-123-999",
        }),
    )
    task = service.tasks.claim_next()
    bot = BlockingNotificationBot(content)

    operation = asyncio.create_task(service.execute_task(task, bot))
    await asyncio.wait_for(bot.notification_started.wait(), timeout=2)
    operation.cancel()
    result = await asyncio.wait_for(operation, timeout=2)

    assert task.task_id == task_id
    identity = identity_token("bot-api-file-id:opaque-file-id")
    assert result == f"bot-123-999-{identity}-committed.bin"
    assert (settings.complete_dir / result).read_bytes() == content


@pytest.mark.asyncio
async def test_existing_direct_media_file_is_made_readable(tmp_path: Path):
    settings = make_settings(tmp_path)
    prepare_directories(settings.complete_dir, settings.incomplete_dir)
    service = BotService(settings, client=object())
    identity = identity_token("bot-api-file-id:opaque-file-id")
    final = settings.complete_dir / f"bot-123-777-{identity}-existing.bin"
    final.write_bytes(b"already committed")
    final.chmod(0o600)
    task_id = service.tasks.enqueue(
        "direct-media",
        json.dumps({
            "file_id": "opaque-file-id",
            "filename": "existing.bin",
            "file_size": len(b"already committed"),
            "source_key": "bot-123-777",
        }),
    )
    task = service.tasks.claim_next()

    result = await service.execute_task(task, FakeBot(b"unused"))

    assert task.task_id == task_id
    assert result == final.name
    assert stat.S_IMODE(final.stat().st_mode) == 0o644


@pytest.mark.asyncio
async def test_direct_media_promotion_syncs_both_directories(tmp_path: Path):
    content = b"durable direct media" * 64
    settings = make_settings(tmp_path)
    prepare_directories(settings.complete_dir, settings.incomplete_dir)
    service = BotService(settings, client=object())
    task_id = service.tasks.enqueue(
        "direct-media",
        json.dumps(
            {
                "file_id": "opaque-file-id",
                "filename": "durable.bin",
                "file_size": len(content),
                "source_key": "bot-123-654",
            }
        ),
    )
    task = service.tasks.claim_next()
    bot = FakeBot(content)
    synced = []
    original_sync = service._sync_directory

    def record_sync(directory):
        synced.append(Path(directory))
        original_sync(directory)

    service._sync_directory = record_sync

    await service.execute_task(task, bot)

    assert task.task_id == task_id
    assert settings.complete_dir in synced
    assert settings.incomplete_dir in synced


@pytest.mark.asyncio
async def test_direct_media_identity_separates_same_name_and_size(tmp_path: Path):
    settings = make_settings(tmp_path)
    prepare_directories(settings.complete_dir, settings.incomplete_dir)
    service = BotService(settings, client=object())
    first_identity = identity_token("bot-api-file-unique-id:first")
    second_identity = identity_token("bot-api-file-unique-id:second")
    first_final = settings.complete_dir / f"bot-1-2-{first_identity}-same.bin"
    first_final.write_bytes(b"a" * 32)
    task_id = service.tasks.enqueue(
        "direct-media",
        json.dumps({
            "file_id": "new-download-handle",
            "filename": "same.bin",
            "file_size": 32,
            "source_key": "bot-1-2",
            "media_identity": second_identity,
        }),
    )
    task = service.tasks.claim_next()

    result = await service.execute_task(task, FakeBot(b"b" * 32))

    assert task.task_id == task_id
    assert result == f"bot-1-2-{second_identity}-same.bin"
    assert (settings.complete_dir / result).read_bytes() == b"b" * 32
    assert first_final.read_bytes() == b"a" * 32


@pytest.mark.asyncio
async def test_authorized_session_recovers_auth_required_tasks(tmp_path: Path):
    class AuthorizedClient:
        async def is_user_authorized(self):
            return True

    service = BotService(make_settings(tmp_path), AuthorizedClient())
    task_id = service.tasks.enqueue("message-link", "https://t.me/example/123")
    task = service.tasks.claim_next()
    service.tasks.require_auth(task.task_id, "Authorization required")

    authorized, resumed = await service.reconcile_authorization()

    [stored] = service.tasks.recent()
    assert task_id == stored.task_id
    assert authorized is True
    assert resumed == 1
    assert stored.status == "queued"
    assert stored.attempts == 0


@pytest.mark.asyncio
async def test_unauthorized_session_keeps_auth_required_tasks_paused(tmp_path: Path):
    class UnauthorizedClient:
        async def is_user_authorized(self):
            return False

    service = BotService(make_settings(tmp_path), UnauthorizedClient())
    service.tasks.enqueue("message-link", "https://t.me/example/123")
    task = service.tasks.claim_next()
    service.tasks.require_auth(task.task_id, "Authorization required")

    authorized, resumed = await service.reconcile_authorization()

    [stored] = service.tasks.recent()
    assert authorized is False
    assert resumed == 0
    assert stored.status == "auth_required"


@pytest.mark.parametrize("protected", [True, False, None])
def test_direct_media_protection_guard(protected):
    from types import SimpleNamespace

    from app.bot import direct_media_protection_error

    message = SimpleNamespace(has_protected_content=protected)
    result = direct_media_protection_error(message)
    if protected:
        assert "protected" in result
    else:
        assert result is None


@pytest.mark.parametrize(
    ("file_size", "expected"),
    [
        (None, "valid file size"),
        (0, "valid file size"),
        (-1, "valid file size"),
        (20 * 1024 * 1024, None),
        (20 * 1024 * 1024 + 1, "exceeds"),
    ],
)
def test_direct_media_size_guard_fails_closed(file_size, expected):
    result = direct_media_size_error(file_size)
    if expected is None:
        assert result is None
    else:
        assert expected in result


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("handler_name", "initial_state", "pending"),
    [
        ("phone", "phone", (None, None)),
        ("code", "code", ("+123456789", "code-hash")),
        ("password", "password", ("+123456789", "code-hash")),
    ],
)
@pytest.mark.parametrize(
    ("failure_type", "expected_message"),
    [
        (ConnectionError, "Temporary Telegram network failure"),
        (sqlite3.OperationalError, "Local session storage is temporarily unavailable"),
    ],
)
async def test_transient_auth_failures_preserve_login_flow(
    handler_name, initial_state, pending, failure_type, expected_message, monkeypatch
):
    import asyncio
    from types import SimpleNamespace

    import app.bot as bot_module

    class FakeState:
        def __init__(self):
            self.current = initial_state
            self.cleared = False

        async def get_state(self):
            return None if self.current is None else f"AuthFlow:{self.current}"

        async def set_state(self, value):
            self.current = value

        async def clear(self):
            self.cleared = True
            self.current = None

    class FakeMessage:
        chat = SimpleNamespace(type="private")
        from_user = SimpleNamespace(id=123456)

        def __init__(self, text):
            self.text = text
            self.answers = []

        async def delete(self):
            return None

        async def answer(self, text):
            self.answers.append(text)

    class BrokenClient:
        async def send_code_request(self, phone):
            raise failure_type("temporary login operation failure")

        async def sign_in(self, **kwargs):
            raise failure_type("temporary login operation failure")

    settings = make_settings(Path(".").resolve() / ".test-state-unused")
    service = SimpleNamespace(
        settings=settings,
        client=BrokenClient(),
        mtproto_lock=asyncio.Lock(),
        pending_phone=pending[0],
        pending_code_hash=pending[1],
        auth_flow_generation=0,
        auth_state_lock=asyncio.Lock(),
        is_owner=lambda message: True,
        wait_for_mtproto_cooldown=lambda: asyncio.sleep(0),
        note_flood_wait=lambda seconds: None,
    )
    monkeypatch.setattr(bot_module, "get_service", lambda: service)
    state = FakeState()
    message = FakeMessage(
        "+123456789" if handler_name == "phone" else "12345" if handler_name == "code" else "secret"
    )
    handler = {
        "phone": bot_module.auth_phone,
        "code": bot_module.auth_code,
        "password": bot_module.auth_password,
    }[handler_name]

    await handler(message, state)

    assert state.cleared is False
    assert any(expected_message in text for text in message.answers)
    assert service.pending_phone == pending[0]
    assert service.pending_code_hash == pending[1]


@pytest.mark.asyncio
@pytest.mark.parametrize("file_size", [None, 0, -1, "1024", 1.5, True, 20 * 1024 * 1024 + 1])
async def test_legacy_direct_media_without_valid_supported_size_fails_closed(
    tmp_path: Path, file_size
):
    from app.worker import PermanentTaskError

    settings = make_settings(tmp_path)
    service = BotService(settings, client=object())
    task_id = service.tasks.enqueue(
        "direct-media",
        json.dumps({
            "file_id": "legacy-file-id",
            "filename": "legacy.bin",
            "file_size": file_size,
            "source_key": "bot-123-456",
        }),
    )
    task = service.tasks.claim_next()
    bot = FakeBot(b"must-not-download")

    with pytest.raises(PermanentTaskError, match="cannot be resumed safely"):
        await service.execute_task(task, bot)

    assert bot.download_calls == 0
    assert bot.messages
    assert "cannot be resumed safely" in bot.messages[0][1]
    [stored] = service.tasks.recent()
    assert stored.task_id == task_id
    assert stored.status == "downloading"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("source_key", "../../outside"),
        ("media_identity", "../escape"),
    ],
)
async def test_direct_media_rejects_path_components_from_persisted_payload(
    tmp_path: Path, field: str, value: str
):
    from app.worker import PermanentTaskError

    settings = make_settings(tmp_path)
    service = BotService(settings, client=object())
    payload = {
        "file_id": "opaque-file-id",
        "filename": "safe.bin",
        "file_size": 10,
        "source_key": "bot-123-456",
        "media_identity": identity_token("bot-api-file-unique-id:valid"),
    }
    payload[field] = value
    task_id = service.tasks.enqueue("direct-media", json.dumps(payload))
    task = service.tasks.claim_next()
    bot = FakeBot(b"must-not-download")

    with pytest.raises(PermanentTaskError, match="invalid"):
        await service.execute_task(task, bot)

    assert bot.download_calls == 0
    assert bot.messages
    [stored] = service.tasks.recent()
    assert stored.task_id == task_id
    assert stored.status == "downloading"


@pytest.mark.asyncio
@pytest.mark.parametrize("error_name", ["AuthKeyInvalidError", "SessionExpiredError"])
async def test_authorization_probe_recognizes_expired_auth_key(tmp_path: Path, error_name):
    error_type = type(error_name, (Exception,), {})

    class ExpiredClient:
        async def is_user_authorized(self):
            raise error_type("authorization expired")

    service = BotService(make_settings(tmp_path), ExpiredClient())

    assert await service.is_authorized() is False


@pytest.mark.asyncio
async def test_message_link_progress_storage_failure_is_deferred(tmp_path: Path):
    from app.worker import StorageUnavailable

    service = BotService(make_settings(tmp_path), client=object())
    task_id = service.tasks.enqueue("message-link", "https://t.me/example_channel/123")
    task = service.tasks.claim_next()

    def unavailable_progress(*args, **kwargs):
        raise sqlite3.OperationalError("database is locked")

    service.tasks.update_progress = unavailable_progress

    class ProgressReportingDownloader:
        async def download(self, link, progress_callback=None):
            progress_callback(4096, 8192, "asset.bin")
            return []

    service.downloader = ProgressReportingDownloader()

    with pytest.raises(StorageUnavailable, match="database is temporarily unavailable"):
        await service.execute_task(task, FakeBot(b""))

    [stored] = service.tasks.recent()
    assert stored.task_id == task_id
    assert stored.status == "downloading"


@pytest.mark.asyncio
async def test_direct_media_sqlite_failure_is_deferred_before_download(tmp_path: Path):
    import sqlite3

    from app.worker import StorageUnavailable

    settings = make_settings(tmp_path)
    service = BotService(settings, client=object())
    task_id = service.tasks.enqueue(
        "direct-media",
        json.dumps({
            "file_id": "opaque-file-id",
            "filename": "sample.bin",
            "file_size": 10,
            "source_key": "bot-123-654",
        }),
    )
    task = service.tasks.claim_next()
    bot = FakeBot(b"0123456789")

    def unavailable_progress(*args, **kwargs):
        raise sqlite3.OperationalError("database is locked")

    service.tasks.update_progress = unavailable_progress

    with pytest.raises(StorageUnavailable, match="database is temporarily unavailable"):
        await service.execute_task(task, bot)

    assert bot.download_calls == 0
    [stored] = service.tasks.recent()
    assert stored.task_id == task_id
    assert stored.status == "downloading"


@pytest.mark.asyncio
async def test_auth_password_preserves_leading_and_trailing_spaces(tmp_path: Path, monkeypatch):
    from types import SimpleNamespace

    import app.bot as bot_module
    from app.bot import AuthFlow, BotService

    class PasswordClient:
        def __init__(self):
            self.received_password = None

        async def sign_in(self, **kwargs):
            self.received_password = kwargs["password"]

    class FakeState:
        current = AuthFlow.password.state

        async def get_state(self):
            return self.current

        async def clear(self):
            self.current = None

    class FakeMessage:
        chat = SimpleNamespace(type="private")
        from_user = SimpleNamespace(id=123456)

        def __init__(self, text):
            self.text = text
            self.answers = []

        async def delete(self):
            return None

        async def answer(self, text):
            self.answers.append(text)

    client = PasswordClient()
    service = BotService(make_settings(tmp_path), client)
    monkeypatch.setattr(bot_module, "get_service", lambda: service)
    message = FakeMessage("  secret with spaces  ")
    state = FakeState()

    await bot_module.auth_password(message, state)

    assert client.received_password == "  secret with spaces  "
    assert state.current is None
    assert any("Authorization successful" in answer for answer in message.answers)


@pytest.mark.asyncio
async def test_incorrect_2fa_password_keeps_password_state_for_retry(tmp_path: Path, monkeypatch):
    from types import SimpleNamespace

    import app.bot as bot_module
    from app.bot import AuthFlow, BotService

    class PasswordClient:
        async def sign_in(self, **kwargs):
            error = type("PasswordHashInvalidError", (Exception,), {})
            raise error("password rejected")

    class FakeState:
        current = AuthFlow.password.state

        async def get_state(self):
            return self.current

        async def clear(self):
            self.current = None

    class FakeMessage:
        chat = SimpleNamespace(type="private")
        from_user = SimpleNamespace(id=123456)
        text = "incorrect password"

        def __init__(self):
            self.answers = []

        async def delete(self):
            return None

        async def answer(self, text):
            self.answers.append(text)

    service = BotService(make_settings(tmp_path), PasswordClient())
    monkeypatch.setattr(bot_module, "get_service", lambda: service)
    state = FakeState()
    message = FakeMessage()

    await bot_module.auth_password(message, state)

    assert state.current == AuthFlow.password.state
    assert service.auth_flow_generation == 0
    assert message.answers == ["Incorrect 2FA password. Try again."]


@pytest.mark.asyncio
async def test_cancelled_phone_request_cannot_restore_auth_fsm(tmp_path: Path, monkeypatch):
    import asyncio
    from types import SimpleNamespace

    from app.bot import AuthFlow, BotService, auth_phone, cancel

    started = asyncio.Event()
    release = asyncio.Event()

    class SlowClient:
        async def send_code_request(self, phone):
            started.set()
            await release.wait()
            return SimpleNamespace(phone_code_hash="late-code-hash")

    class FakeState:
        current = AuthFlow.phone.state

        async def get_state(self):
            return self.current

        async def set_state(self, value):
            self.current = value.state

        async def clear(self):
            self.current = None

    class FakeMessage:
        chat = SimpleNamespace(type="private")
        from_user = SimpleNamespace(id=123456)

        def __init__(self, text):
            self.text = text
            self.answers = []

        async def delete(self):
            return None

        async def answer(self, text):
            self.answers.append(text)

    settings = make_settings(tmp_path)
    service = BotService(settings, SlowClient())
    monkeypatch.setattr("app.bot.get_service", lambda: service)
    state = FakeState()
    phone_message = FakeMessage("+123456789")
    cancel_message = FakeMessage("/cancel")

    phone_task = asyncio.create_task(auth_phone(phone_message, state))
    await asyncio.wait_for(started.wait(), timeout=2)
    await cancel(cancel_message, state, SimpleNamespace(args=None))
    release.set()
    await asyncio.wait_for(phone_task, timeout=2)

    assert state.current is None
    assert service.pending_phone is None
    assert service.pending_code_hash is None
    assert service.auth_flow_generation == 1


@pytest.mark.asyncio
async def test_authorization_callback_rejects_non_private_chat(monkeypatch):
    from types import SimpleNamespace

    from app.bot import auth_callback

    class Service:
        settings = SimpleNamespace(owner_telegram_id=123456)

    class FakeCallback:
        from_user = SimpleNamespace(id=123456)
        message = SimpleNamespace(chat=SimpleNamespace(type="group"))

        def __init__(self):
            self.answers = []

        async def answer(self, text, show_alert=False):
            self.answers.append((text, show_alert))

    class FakeState:
        pass

    monkeypatch.setattr("app.bot.get_service", lambda: Service())
    callback = FakeCallback()

    await auth_callback(callback, FakeState())

    assert callback.answers == [
        ("Please authorize through a private chat with the Bot.", True)
    ]


@pytest.mark.asyncio
async def test_auth_start_reports_temporary_authorization_check_failure(
    tmp_path: Path, monkeypatch
):
    from app.bot import auth_start

    class UnavailableService:
        settings = make_settings(tmp_path)
        auth_flow_generation = 0

        def is_owner(self, message):
            return True

        async def is_authorized(self):
            raise ConnectionError("temporary network outage")

    class FakeMessage:
        from_user = type("User", (), {"id": 123456})()

        def __init__(self):
            self.answers = []

        async def answer(self, text):
            self.answers.append(text)

    class FakeState:
        async def set_state(self, state):
            raise AssertionError("FSM must not advance when authorization status is unknown")

    monkeypatch.setattr("app.bot.get_service", lambda: UnavailableService())
    message = FakeMessage()

    await auth_start(message, FakeState())

    assert len(message.answers) == 1
    assert "暂时无法检查" in message.answers[0]
    assert "不会将账号误判为未授权" in message.answers[0]

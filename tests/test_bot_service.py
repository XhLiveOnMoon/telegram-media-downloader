import json
import stat
import time
from pathlib import Path

import pytest

from app.bot import BotService
from app.config import Settings
from app.storage import identity_token, prepare_directories


class FakeBot:
    def __init__(self, content: bytes):
        self.content = content
        self.messages = []

    async def download(self, *, file, destination):
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

    partial = settings.incomplete_dir / f"bot-123-456-{identity}-sample.bin.part"
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


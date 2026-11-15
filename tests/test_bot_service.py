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
    final = settings.complete_dir / "bot-123-456-{identity_token("bot-api-file-id:opaque-file-id")}-sample.bin"
    opened_modes = []
    original_open = Path.open

    def track_open(path, mode="r", *args, **kwargs):
        if path == final:
            opened_modes.append(mode)
        return original_open(path, mode, *args, **kwargs)

    monkeypatch.setattr(Path, "open", track_open)

    detail = await service.execute_task(task, bot)

    partial = settings.incomplete_dir / "bot-123-456-{identity_token("bot-api-file-id:opaque-file-id")}-sample.bin.part"
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
    assert result == "bot-123-999-{identity_token("bot-api-file-id:opaque-file-id")}-committed.bin"
    assert (settings.complete_dir / result).read_bytes() == content


@pytest.mark.asyncio
async def test_existing_direct_media_file_is_made_readable(tmp_path: Path):
    settings = make_settings(tmp_path)
    prepare_directories(settings.complete_dir, settings.incomplete_dir)
    service = BotService(settings, client=object())
    final = settings.complete_dir / "bot-123-777-{identity_token("bot-api-file-id:opaque-file-id")}-existing.bin"
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
async def test_replaced_media_does_not_reuse_partial_or_completed_file(tmp_path: Path):
    original = make_message()
    replacement = make_message()
    replacement.document.id = 9002
    client = FakeClient([b"n" * 8192])
    downloader = MediaDownloader(client, tmp_path / "complete", tmp_path / "incomplete")
    old_key = f"{get_peer_id(original.peer_id)}-{original.id}-{media_identity(original)}"
    old_partial = stable_partial_path(tmp_path / "incomplete", old_key, "asset.bin")
    old_partial.write_bytes(b"o" * 4096)
    old_final = tmp_path / "complete" / f"{old_key}-asset.bin"
    old_final.parent.mkdir(parents=True)
    old_final.write_bytes(b"o" * 8192)

    result = await downloader._download_one(replacement)

    assert client.requested_offset == 0
    assert result != old_final
    assert result.read_bytes() == b"n" * 8192
    assert old_partial.read_bytes() == b"o" * 4096
    assert old_final.read_bytes() == b"o" * 8192


@pytest.mark.asyncio
async def test_file_reference_refresh_rejects_replaced_media(tmp_path: Path):
    message = make_message()

    class FileReferenceExpiredError(Exception):
        pass

    class ReplacingClient:
        async def get_messages(self, entity, ids):
            replacement = make_message()
            replacement.document.id = 9002
            return replacement

        async def iter_download(self, media, *, offset, request_size, chunk_size):
            yield b"x" * 4096
            raise FileReferenceExpiredError("stale reference")

    downloader = MediaDownloader(
        ReplacingClient(), tmp_path / "complete", tmp_path / "incomplete",
        max_network_retries=1, retry_base_delay=0.001,
    )

    with pytest.raises(DownloadError, match="media changed"):
        await downloader._download_one(message, entity=object())


@pytest.mark.asyncio
async def test_media_without_stable_telegram_id_is_rejected(tmp_path: Path):
    message = make_message()
    message.document.id = None
    downloader = MediaDownloader(FakeClient([]), tmp_path / "complete", tmp_path / "incomplete")

    with pytest.raises(DownloadError, match="no stable"):
        await downloader._download_one(message)


def test_photo_identity_uses_telegram_photo_id():
    message = SimpleNamespace(document=None, photo=SimpleNamespace(id=123456))

    assert media_identity(message) == "photo-123456"


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


import json
import stat
import time
from pathlib import Path

import pytest

from app.bot import BotService
from app.config import Settings
from app.storage import prepare_directories


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
        complete_dir=tmp_path / "downloads" / "complete",
        incomplete_dir=tmp_path / "downloads" / "incomplete",
    )


@pytest.mark.asyncio
async def test_direct_media_is_fsynced_and_atomically_promoted(tmp_path: Path):
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

    detail = await service.execute_task(task, bot)

    final = settings.complete_dir / "bot-123-456-sample.bin"
    partial = settings.incomplete_dir / "bot-123-456-sample.bin.part"
    assert task.task_id == task_id
    assert detail == final.name
    assert final.read_bytes() == content
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
    assert stored.progress_bytes == 4
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

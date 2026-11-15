import sqlite3
from pathlib import Path

import pytest

from app.task_store import DownloadTaskStore


def test_task_lifecycle_is_persisted(tmp_path: Path):
    store = DownloadTaskStore(tmp_path / "state" / "tasks.sqlite3")
    task_id = store.enqueue("message-link", "https://t.me/example/123")
    assert store.claim_next().task_id == task_id
    store.complete(task_id, "video.mp4")
    [task] = store.recent()
    assert task.task_id == task_id
    assert task.task_type == "message-link"
    assert task.status == "completed"
    assert task.detail == "video.mp4"


def test_legacy_running_tasks_are_migrated_to_interrupted(tmp_path: Path):
    database = tmp_path / "tasks.sqlite3"
    with sqlite3.connect(database) as connection:
        connection.execute(
            """
            CREATE TABLE download_tasks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                task_type TEXT NOT NULL,
                source TEXT NOT NULL,
                status TEXT NOT NULL CHECK (
                    status IN ('running', 'completed', 'failed', 'interrupted')
                ),
                detail TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        connection.execute(
            "CREATE INDEX idx_download_tasks_created ON download_tasks(created_at DESC)"
        )
        connection.execute(
            "INSERT INTO download_tasks "
            "(task_type, source, status, created_at, updated_at) "
            "VALUES ('message-link', 'https://t.me/example/456', 'running', 'old', 'old')"
        )

    store = DownloadTaskStore(database)
    [task] = store.recent()
    assert task.status == "interrupted"
    assert "restarted" in task.detail.lower()
    assert task.attempts == 0


def test_queue_claim_is_atomic_and_fifo(tmp_path: Path):
    database = tmp_path / "tasks.sqlite3"
    first = DownloadTaskStore(database)
    second = DownloadTaskStore(database)
    first_id = first.enqueue("message-link", "https://t.me/example/1")
    second_id = first.enqueue("message-link", "https://t.me/example/2")

    claimed = first.claim_next()
    assert claimed is not None
    assert claimed.task_id == first_id
    assert claimed.status == "downloading"
    assert claimed.attempts == 1

    # A second scheduler cannot claim the task already claimed by the first.
    assert second.claim_next() is not None
    assert second.claim_next() is None
    [remaining] = second.recent(limit=1)
    assert remaining.task_id == second_id
    assert remaining.status == "downloading"


def test_retry_wait_is_not_claimed_before_due_time(tmp_path: Path):
    store = DownloadTaskStore(tmp_path / "tasks.sqlite3")
    task_id = store.enqueue("message-link", "https://t.me/example/3")
    assert store.claim_next(now=100) is not None
    store.retry_later(task_id, "temporary network issue", delay_seconds=60)
    # Compare against the persisted due timestamp without sleeping in the test.
    [waiting] = store.recent()
    assert store.claim_next(now=waiting.next_attempt_at - 1) is None
    claimed = store.claim_next(now=waiting.next_attempt_at)
    assert claimed is not None
    assert claimed.task_id == task_id
    assert claimed.attempts == 2


def test_global_floodwait_cooldown_survives_store_recreation(tmp_path: Path):
    database = tmp_path / "tasks.sqlite3"
    store = DownloadTaskStore(database)
    task_id = store.enqueue("message-link", "https://t.me/example/4")
    store.set_global_cooldown(120, now=1000)

    restarted = DownloadTaskStore(database)
    assert restarted.cooldown_remaining(now=1050) == 70
    assert restarted.claim_next(now=1050) is None
    assert restarted.claim_next(now=1120).task_id == task_id


def test_global_cooldown_never_shortens(tmp_path: Path):
    store = DownloadTaskStore(tmp_path / "tasks.sqlite3")
    assert store.set_global_cooldown(300, now=1000) == 1300
    assert store.set_global_cooldown(20, now=1010) == 1300


def test_interrupted_tasks_can_be_recovered_once(tmp_path: Path):
    database = tmp_path / "tasks.sqlite3"
    first_instance = DownloadTaskStore(database)
    task_id = first_instance.enqueue("message-link", "https://t.me/example/789")
    assert first_instance.claim_next().task_id == task_id
    restarted_instance = DownloadTaskStore(database)

    [interrupted] = restarted_instance.recent()
    assert interrupted.task_id == task_id
    assert interrupted.status == "interrupted"

    recovered = restarted_instance.claim_next()
    assert recovered.task_id == task_id
    assert recovered.status == "downloading"
    assert recovered.attempts == 2


def test_authorization_blocked_task_remains_recoverable(tmp_path: Path):
    store = DownloadTaskStore(tmp_path / "tasks.sqlite3")
    task_id = store.enqueue("message-link", "https://t.me/example/100")
    assert store.claim_next().task_id == task_id
    store.require_auth(task_id, "Telegram authorization required")

    [task] = store.recent()
    assert task.task_id == task_id
    assert task.status == "auth_required"
    assert "authorization" in task.detail.lower()


def test_sqlite_uses_wal_journal_mode(tmp_path: Path):
    database = tmp_path / "tasks.sqlite3"
    DownloadTaskStore(database)
    with sqlite3.connect(database) as connection:
        [mode] = connection.execute("PRAGMA journal_mode").fetchone()
    assert mode.lower() == "wal"


def test_failed_task_and_recent_limit(tmp_path: Path):
    store = DownloadTaskStore(tmp_path / "tasks.sqlite3")
    old_id = store.enqueue("direct-media", "old.bin")
    assert store.claim_next().task_id == old_id
    store.fail(old_id, "network error")
    new_id = store.enqueue("direct-media", "new.bin")
    records = store.recent(limit=1)
    assert len(records) == 1
    assert records[0].task_id == new_id
    assert records[0].status == "queued"


def test_only_worker_claimed_tasks_can_complete(tmp_path: Path):
    store = DownloadTaskStore(tmp_path / "tasks.sqlite3")
    task_id = store.enqueue("message-link", "https://t.me/example/222")

    with pytest.raises(RuntimeError, match="not in downloading state"):
        store.complete(task_id, "should-not-complete")

    [task] = store.recent()
    assert task.status == "queued"


def test_queued_task_can_be_cancelled_without_claiming(tmp_path: Path):
    store = DownloadTaskStore(tmp_path / "tasks.sqlite3")
    task_id = store.enqueue("message-link", "https://t.me/example/333")

    assert store.cancel(task_id) is True
    assert store.cancel(task_id) is False
    [task] = store.recent()
    assert task.status == "cancelled"


def test_failed_task_can_be_requeued_once(tmp_path: Path):
    store = DownloadTaskStore(tmp_path / "tasks.sqlite3")
    task_id = store.enqueue("message-link", "https://t.me/example/444")
    assert store.claim_next().task_id == task_id
    store.fail(task_id, "network failure")

    assert store.requeue(task_id) is True
    assert store.requeue(task_id) is False
    [task] = store.recent()
    assert task.status == "queued"
    assert task.attempts == 0


def test_active_task_can_be_cancelled_only_after_worker_claim(tmp_path: Path):
    store = DownloadTaskStore(tmp_path / "tasks.sqlite3")
    task_id = store.enqueue("message-link", "https://t.me/example/555")

    assert store.cancel_active(task_id) is False
    assert store.claim_next().task_id == task_id
    assert store.cancel_active(task_id) is True
    assert store.cancel_active(task_id) is False
    [task] = store.recent()
    assert task.status == "cancelled"



def test_download_progress_is_persisted_and_finalized(tmp_path: Path):
    store = DownloadTaskStore(tmp_path / "tasks.sqlite3")
    task_id = store.enqueue("message-link", "https://t.me/example/888")

    assert store.update_progress(task_id, 1024, 4096, "asset.bin") is False
    assert store.claim_next().task_id == task_id
    assert store.update_progress(task_id, 2048, 4096, "asset.bin") is True
    [task] = store.recent()
    assert task.progress_bytes == 2048
    assert task.total_bytes == 4096
    assert task.current_file == "asset.bin"

    store.complete(task_id, "asset.bin")
    [task] = store.recent()
    assert task.status == "completed"
    assert task.progress_bytes == task.total_bytes == 4096

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

    # A second scheduler cannot claim another task while one is active.
    assert second.claim_next() is None
    [remaining] = second.recent(limit=1)
    assert remaining.task_id == second_id
    assert remaining.status == "queued"
    first.complete(first_id, "first.bin")
    next_claim = second.claim_next()
    assert next_claim is not None
    assert next_claim.task_id == second_id
    assert next_claim.status == "downloading"


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
    assert recovered.attempts == 1


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


def test_existing_queue_schema_is_upgraded_with_progress_columns(tmp_path: Path):
    database = tmp_path / "queue-v1.sqlite3"
    with sqlite3.connect(database) as connection:
        connection.execute(
            """
            CREATE TABLE download_tasks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                task_type TEXT NOT NULL,
                source TEXT NOT NULL,
                status TEXT NOT NULL,
                detail TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                attempts INTEGER NOT NULL DEFAULT 0,
                next_attempt_at REAL
            )
            """
        )
        connection.execute(
            "INSERT INTO download_tasks "
            "(task_type, source, status, created_at, updated_at) "
            "VALUES ('message-link', 'https://t.me/example/999', 'queued', 'old', 'old')"
        )

    store = DownloadTaskStore(database)
    [task] = store.recent()
    assert task.status == "queued"
    assert task.progress_bytes == 0
    assert task.total_bytes is None
    assert task.current_file == ""
    assert store.cooldown_remaining() == 0


def test_existing_queue_schema_repairs_missing_scheduler_state(tmp_path: Path):
    database = tmp_path / "tasks.sqlite3"
    with sqlite3.connect(database) as connection:
        connection.execute(
            """
            CREATE TABLE download_tasks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                task_type TEXT NOT NULL,
                source TEXT NOT NULL,
                status TEXT NOT NULL,
                detail TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                attempts INTEGER NOT NULL DEFAULT 0,
                next_attempt_at REAL,
                progress_bytes INTEGER NOT NULL DEFAULT 0,
                total_bytes INTEGER,
                current_file TEXT NOT NULL DEFAULT ''
            )
            """
        )
        connection.execute(
            "INSERT INTO download_tasks "
            "(task_type, source, status, created_at, updated_at) "
            "VALUES ('message-link', 'https://t.me/example/900', 'queued', 'now', 'now')"
        )

    store = DownloadTaskStore(database)
    claimed = store.claim_next()
    assert claimed is not None
    assert claimed.source.endswith("/900")
    assert store.cooldown_remaining() == 0


def test_floodwait_retry_does_not_consume_attempt_budget(tmp_path: Path):
    store = DownloadTaskStore(tmp_path / "tasks.sqlite3")
    task_id = store.enqueue("message-link", "https://t.me/example/901")
    assert store.claim_next() is not None

    store.retry_later(task_id, "FloodWait", 60, preserve_attempts=True)

    [task] = store.recent()
    assert task.status == "retry_wait"
    assert task.attempts == 0


def test_authorization_resume_resets_retry_budget(tmp_path: Path):
    store = DownloadTaskStore(tmp_path / "tasks.sqlite3")
    task_id = store.enqueue("message-link", "https://t.me/example/902")
    assert store.claim_next() is not None
    store.require_auth(task_id, "authorization required")
    assert store.resume_auth_required() == 1

    [task] = store.recent()
    assert task.status == "queued"
    assert task.attempts == 0


def test_intermediate_schema_with_old_status_check_is_rebuilt(tmp_path: Path):
    database = tmp_path / "tasks.sqlite3"
    with sqlite3.connect(database) as connection:
        connection.execute(
            """
            CREATE TABLE download_tasks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                task_type TEXT NOT NULL,
                source TEXT NOT NULL,
                status TEXT NOT NULL CHECK (
                    status IN ('queued', 'downloading', 'completed', 'failed', 'interrupted')
                ),
                detail TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                attempts INTEGER NOT NULL DEFAULT 0,
                next_attempt_at REAL,
                progress_bytes INTEGER NOT NULL DEFAULT 0,
                total_bytes INTEGER,
                current_file TEXT NOT NULL DEFAULT ''
            )
            """
        )
        connection.execute(
            "INSERT INTO download_tasks "
            "(task_type, source, status, detail, created_at, updated_at, attempts, "
            "progress_bytes, total_bytes, current_file) "
            "VALUES ('message-link', 'https://t.me/example/903', 'queued', '', "
            "'now', 'now', 2, 1024, 4096, 'movie.bin')"
        )

    store = DownloadTaskStore(database)
    [task] = store.recent()
    assert task.status == "queued"
    assert task.attempts == 2
    assert task.progress_bytes == 1024
    assert task.total_bytes == 4096
    assert task.current_file == "movie.bin"
    assert store.claim_next().task_id == task.task_id


def test_interruption_does_not_consume_retry_budget(tmp_path: Path):
    store = DownloadTaskStore(tmp_path / "tasks.sqlite3")
    task_id = store.enqueue("message-link", "https://t.me/example/904")
    assert store.claim_next().attempts == 1

    store.interrupt(task_id, "service stopping")

    [task] = store.recent()
    assert task.status == "interrupted"
    assert task.attempts == 0


def test_enqueue_deduplicates_nonterminal_tasks_atomically(tmp_path: Path):
    database = tmp_path / "tasks.sqlite3"
    first_store = DownloadTaskStore(database)
    second_store = DownloadTaskStore(database)
    source = "https://t.me/example/123"

    first_id = first_store.enqueue("message-link", source)
    duplicate_id = second_store.enqueue("message-link", source)

    assert duplicate_id == first_id
    assert len(first_store.recent()) == 1

    claimed = first_store.claim_next()
    assert claimed.task_id == first_id
    active_duplicate_id = second_store.enqueue("message-link", source)
    assert active_duplicate_id == first_id
    assert len(first_store.recent()) == 1


def test_enqueue_allows_new_task_after_previous_task_is_terminal(tmp_path: Path):
    store = DownloadTaskStore(tmp_path / "tasks.sqlite3")
    source = "https://t.me/example/456"

    first_id = store.enqueue("message-link", source)
    assert store.claim_next().task_id == first_id
    store.complete(first_id, "asset.bin")

    second_id = store.enqueue("message-link", source)

    assert second_id != first_id
    records = store.recent(limit=2)
    assert {record.status for record in records} == {"completed", "queued"}


def test_floodwait_deferral_and_global_cooldown_are_atomic(tmp_path: Path):
    database = tmp_path / "tasks.sqlite3"
    store = DownloadTaskStore(database)
    task_id = store.enqueue("message-link", "https://t.me/example/905")
    assert store.claim_next().task_id == task_id

    until = store.defer_floodwait(task_id, 120, now=1000)

    [task] = store.recent()
    assert task.status == "retry_wait"
    assert task.attempts == 0
    assert task.next_attempt_at == until == 1120
    assert store.cooldown_remaining(now=1000) == 120

    restarted = DownloadTaskStore(database)
    assert restarted.cooldown_remaining(now=1050) == 70
    assert restarted.claim_next(now=1050) is None


def test_enqueue_with_status_reports_reused_and_created_tasks(tmp_path: Path):
    store = DownloadTaskStore(tmp_path / "tasks.sqlite3")
    source = "https://t.me/example/7890"

    first_id, created = store.enqueue_with_status("message-link", source)
    duplicate_id, duplicate_created = store.enqueue_with_status("message-link", source)

    assert created is True
    assert duplicate_created is False
    assert duplicate_id == first_id
    assert len(store.recent()) == 1


def test_clear_history_deletes_only_terminal_records(tmp_path: Path):
    store = DownloadTaskStore(tmp_path / "tasks.sqlite3")

    completed_id = store.enqueue("message-link", "https://t.me/example/completed")
    assert store.claim_next().task_id == completed_id
    store.complete(completed_id, "completed.bin")

    failed_id = store.enqueue("message-link", "https://t.me/example/failed")
    assert store.claim_next().task_id == failed_id
    store.fail(failed_id, "permanent failure")

    cancelled_id = store.enqueue("message-link", "https://t.me/example/cancelled")
    assert store.cancel(cancelled_id) is True

    queued_id = store.enqueue("message-link", "https://t.me/example/queued")

    retry_id = store.enqueue("message-link", "https://t.me/example/retry")
    assert store.claim_next().task_id == retry_id
    store.retry_later(retry_id, "temporary failure", delay_seconds=3600)

    interrupted_id = store.enqueue("message-link", "https://t.me/example/interrupted")
    assert store.claim_next().task_id == interrupted_id
    store.interrupt(interrupted_id, "worker interrupted")

    auth_id = store.enqueue("message-link", "https://t.me/example/auth")
    assert store.claim_next().task_id == auth_id
    store.require_auth(auth_id, "authorization required")

    assert store.clear_history() == 3
    assert store.clear_history() == 0

    remaining = {task.task_id: task.status for task in store.recent(limit=50)}
    assert remaining == {
        queued_id: "queued",
        retry_id: "retry_wait",
        interrupted_id: "interrupted",
        auth_id: "auth_required",
    }

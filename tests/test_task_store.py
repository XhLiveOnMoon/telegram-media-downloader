from pathlib import Path

from app.task_store import DownloadTaskStore


def test_task_lifecycle_is_persisted(tmp_path: Path):
    database = tmp_path / "state" / "tasks.sqlite3"
    store = DownloadTaskStore(database)

    task_id = store.create("message-link", "https://t.me/example/123")
    store.complete(task_id, "video.mp4")

    [task] = store.recent()
    assert task.task_id == task_id
    assert task.task_type == "message-link"
    assert task.status == "completed"
    assert task.detail == "video.mp4"


def test_running_tasks_are_marked_interrupted_after_restart(tmp_path: Path):
    database = tmp_path / "tasks.sqlite3"
    first_instance = DownloadTaskStore(database)
    task_id = first_instance.create("message-link", "https://t.me/example/456")

    restarted_instance = DownloadTaskStore(database)
    [task] = restarted_instance.recent()

    assert task.task_id == task_id
    assert task.status == "interrupted"
    assert "restarted" in task.detail.lower()


def test_interrupted_tasks_can_be_recovered_and_marked_running(tmp_path: Path):
    database = tmp_path / "tasks.sqlite3"
    first_instance = DownloadTaskStore(database)
    task_id = first_instance.create("message-link", "https://t.me/example/789")

    restarted_instance = DownloadTaskStore(database)
    [interrupted] = restarted_instance.interrupted()
    assert interrupted.task_id == task_id
    assert interrupted.status == "interrupted"

    assert restarted_instance.mark_running(task_id) is True
    assert restarted_instance.mark_running(task_id) is False
    [recovered] = restarted_instance.recent()
    assert recovered.status == "running"
    assert "resumed" in recovered.detail.lower()


def test_authorization_blocked_task_remains_interrupted(tmp_path: Path):
    store = DownloadTaskStore(tmp_path / "tasks.sqlite3")
    task_id = store.create("message-link", "https://t.me/example/100")
    store.interrupt(task_id, "Telegram authorization required")

    [task] = store.interrupted()
    assert task.task_id == task_id
    assert task.status == "interrupted"
    assert "authorization" in task.detail.lower()


def test_sqlite_uses_wal_journal_mode(tmp_path: Path):
    database = tmp_path / "tasks.sqlite3"
    DownloadTaskStore(database)
    import sqlite3

    with sqlite3.connect(database) as connection:
        [mode] = connection.execute("PRAGMA journal_mode").fetchone()
    assert mode.lower() == "wal"


def test_failed_task_and_recent_limit(tmp_path: Path):
    store = DownloadTaskStore(tmp_path / "tasks.sqlite3")
    old_id = store.create("direct-media", "old.bin")
    store.fail(old_id, "network error")
    new_id = store.create("direct-media", "new.bin")

    records = store.recent(limit=1)

    assert len(records) == 1
    assert records[0].task_id == new_id
    assert records[0].status == "running"

import asyncio
import time

import pytest

from app.task_store import DownloadTaskStore
from app.worker import AuthorizationRequired, DownloadWorker, FloodWait, RetryableTaskError


@pytest.mark.asyncio
async def test_worker_claims_and_completes_one_queued_task(tmp_path):
    store = DownloadTaskStore(tmp_path / "tasks.sqlite3")
    task_id = store.enqueue("message-link", "https://t.me/example/123")
    handled = []

    async def handler(task):
        handled.append(task.task_id)
        return "saved.bin"

    worker = DownloadWorker(store, handler, poll_interval=0.01)
    run = asyncio.create_task(worker.run())
    for _ in range(100):
        [task] = store.recent()
        if task.status == "completed":
            break
        await asyncio.sleep(0.01)
    run.cancel()
    await asyncio.gather(run, return_exceptions=True)

    [task] = store.recent()
    assert handled == [task_id]
    assert task.status == "completed"
    assert task.detail == "saved.bin"


@pytest.mark.asyncio
async def test_floodwait_persists_global_cooldown_and_requeues_task(tmp_path):
    store = DownloadTaskStore(tmp_path / "tasks.sqlite3")
    task_id = store.enqueue("message-link", "https://t.me/example/456")
    entered = asyncio.Event()

    async def handler(task):
        entered.set()
        raise FloodWait(3600)

    worker = DownloadWorker(store, handler, poll_interval=0.01)
    run = asyncio.create_task(worker.run())
    await asyncio.wait_for(entered.wait(), timeout=2)
    for _ in range(100):
        [task] = store.recent()
        if task.status == "retry_wait":
            break
        await asyncio.sleep(0.01)
    run.cancel()
    await asyncio.gather(run, return_exceptions=True)

    [task] = store.recent()
    assert task.task_id == task_id
    assert task.status == "retry_wait"
    assert store.cooldown_remaining() > 3500

    restarted = DownloadTaskStore(tmp_path / "tasks.sqlite3")
    assert restarted.claim_next() is None
    assert restarted.cooldown_remaining() > 3500


@pytest.mark.asyncio
async def test_authorization_error_pauses_task_without_retry(tmp_path):
    store = DownloadTaskStore(tmp_path / "tasks.sqlite3")
    task_id = store.enqueue("message-link", "https://t.me/example/789")
    entered = asyncio.Event()

    async def handler(task):
        entered.set()
        raise AuthorizationRequired("Run /auth")

    worker = DownloadWorker(store, handler, poll_interval=0.01)
    run = asyncio.create_task(worker.run())
    await asyncio.wait_for(entered.wait(), timeout=2)
    for _ in range(100):
        [task] = store.recent()
        if task.status == "auth_required":
            break
        await asyncio.sleep(0.01)
    run.cancel()
    await asyncio.gather(run, return_exceptions=True)

    [task] = store.recent()
    assert task.task_id == task_id
    assert task.status == "auth_required"
    assert task.attempts == 1
    assert store.resume_auth_required() == 1
    [task] = store.recent()
    assert task.status == "queued"


@pytest.mark.asyncio
async def test_transient_failure_uses_persistent_retry_schedule(tmp_path):
    store = DownloadTaskStore(tmp_path / "tasks.sqlite3")
    task_id = store.enqueue("message-link", "https://t.me/example/987")
    entered = asyncio.Event()

    async def handler(task):
        entered.set()
        raise RetryableTaskError("connection reset")

    worker = DownloadWorker(
        store, handler, poll_interval=0.01, retry_base_delay=120, retry_max_delay=120
    )
    run = asyncio.create_task(worker.run())
    await asyncio.wait_for(entered.wait(), timeout=2)
    for _ in range(100):
        [task] = store.recent()
        if task.status == "retry_wait":
            break
        await asyncio.sleep(0.01)
    run.cancel()
    await asyncio.gather(run, return_exceptions=True)

    [task] = store.recent()
    assert task.task_id == task_id
    assert task.status == "retry_wait"
    assert task.next_attempt_at is not None
    assert task.next_attempt_at > time.time() + 100



@pytest.mark.asyncio
async def test_worker_shutdown_marks_active_task_interrupted(tmp_path):
    database = tmp_path / "tasks.sqlite3"
    store = DownloadTaskStore(database)
    task_id = store.enqueue("message-link", "https://t.me/example/321")
    entered = asyncio.Event()

    async def handler(task):
        entered.set()
        await asyncio.Future()

    worker = DownloadWorker(store, handler, poll_interval=0.01)
    run = asyncio.create_task(worker.run())
    await asyncio.wait_for(entered.wait(), timeout=2)
    run.cancel()
    await asyncio.gather(run, return_exceptions=True)

    [interrupted] = store.recent()
    assert interrupted.task_id == task_id
    assert interrupted.status == "interrupted"

    restarted = DownloadTaskStore(database)
    recovered = restarted.claim_next()
    assert recovered.task_id == task_id
    assert recovered.attempts == 2


@pytest.mark.asyncio
async def test_worker_stops_retrying_after_configured_attempt_limit(tmp_path):
    store = DownloadTaskStore(tmp_path / "tasks.sqlite3")
    task_id = store.enqueue("message-link", "https://t.me/example/654")
    calls = 0

    async def handler(task):
        nonlocal calls
        calls += 1
        raise RetryableTaskError("temporary failure")

    worker = DownloadWorker(
        store,
        handler,
        poll_interval=0.01,
        max_attempts=3,
        retry_base_delay=0.01,
        retry_max_delay=0.02,
    )
    run = asyncio.create_task(worker.run())
    for _ in range(300):
        [task] = store.recent()
        if task.status == "failed":
            break
        await asyncio.sleep(0.01)
    run.cancel()
    await asyncio.gather(run, return_exceptions=True)

    [task] = store.recent()
    assert calls == 3
    assert task.task_id == task_id
    assert task.status == "failed"
    assert "retry limit" in task.detail.lower()



@pytest.mark.asyncio
async def test_link_and_direct_media_tasks_share_one_serial_worker(tmp_path):
    store = DownloadTaskStore(tmp_path / "tasks.sqlite3")
    first_id = store.enqueue("message-link", "https://t.me/example/1")
    second_id = store.enqueue("direct-media", '{"file_id":"opaque"}')
    active = 0
    peak_active = 0
    handled = []

    async def handler(task):
        nonlocal active, peak_active
        active += 1
        peak_active = max(peak_active, active)
        handled.append(task.task_id)
        await asyncio.sleep(0.02)
        active -= 1
        return "done"

    worker = DownloadWorker(store, handler, poll_interval=0.01)
    run = asyncio.create_task(worker.run())
    for _ in range(100):
        records = store.recent(limit=2)
        if all(record.status == "completed" for record in records):
            break
        await asyncio.sleep(0.01)
    run.cancel()
    await asyncio.gather(run, return_exceptions=True)

    assert handled == [first_id, second_id]
    assert peak_active == 1

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
    assert task.attempts == 0
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
    assert task.attempts == 0


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
    assert recovered.attempts == 1


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


@pytest.mark.asyncio
async def test_user_cancellation_stops_one_task_but_worker_continues(tmp_path):
    store = DownloadTaskStore(tmp_path / "tasks.sqlite3")
    cancelled_id = store.enqueue("message-link", "https://t.me/example/777")
    next_id = store.enqueue("direct-media", '{"file_id":"opaque"}')
    started = asyncio.Event()
    handled = []

    async def handler(task):
        handled.append(task.task_id)
        if task.task_id == cancelled_id:
            started.set()
            await asyncio.Future()
        return "completed"

    worker = DownloadWorker(store, handler, poll_interval=0.01)
    run = asyncio.create_task(worker.run())
    await asyncio.wait_for(started.wait(), timeout=2)
    assert worker.cancel_task(cancelled_id) is True

    for _ in range(100):
        records = store.recent(limit=2)
        states = {record.task_id: record.status for record in records}
        if states.get(cancelled_id) == "cancelled" and states.get(next_id) == "completed":
            break
        await asyncio.sleep(0.01)
    run.cancel()
    await asyncio.gather(run, return_exceptions=True)

    states = {record.task_id: record.status for record in store.recent(limit=2)}
    assert states[cancelled_id] == "cancelled"
    assert states[next_id] == "completed"
    assert handled == [cancelled_id, next_id]



@pytest.mark.asyncio
async def test_worker_recovers_orphaned_task_after_state_write_failure(tmp_path):
    store = DownloadTaskStore(tmp_path / "tasks.sqlite3")
    task_id = store.enqueue("message-link", "https://t.me/example/9876")
    original_complete = store.complete
    complete_calls = 0
    handler_calls = 0

    def flaky_complete(current_task_id, detail=""):
        nonlocal complete_calls
        complete_calls += 1
        if complete_calls == 1:
            raise OSError("simulated temporary SQLite write failure")
        original_complete(current_task_id, detail)

    def flaky_interrupt(current_task_id, detail):
        raise OSError("simulated temporary SQLite write failure")

    store.complete = flaky_complete
    store.interrupt = flaky_interrupt

    async def handler(task):
        nonlocal handler_calls
        handler_calls += 1
        return "saved.bin"

    worker = DownloadWorker(store, handler, poll_interval=0.01)
    run = asyncio.create_task(worker.run())
    for _ in range(200):
        [task] = store.recent()
        if task.status == "completed":
            break
        await asyncio.sleep(0.01)
    run.cancel()
    await asyncio.gather(run, return_exceptions=True)

    [task] = store.recent()
    assert task.task_id == task_id
    assert task.status == "completed"
    # Recovering a task after a SQLite state-write failure must not consume
    # the network retry budget; the handler is safely re-entered idempotently.
    assert task.attempts == 1
    assert handler_calls == 2



@pytest.mark.asyncio
async def test_worker_retries_orphan_recovery_after_recovery_write_failure(tmp_path):
    store = DownloadTaskStore(tmp_path / "tasks.sqlite3")
    task_id = store.enqueue("message-link", "https://t.me/example/9877")
    assert store.claim_next().task_id == task_id

    original_recover = store.recover_orphaned_downloads
    recovery_calls = 0

    def flaky_recover():
        nonlocal recovery_calls
        recovery_calls += 1
        if recovery_calls == 1:
            raise OSError("simulated temporary SQLite recovery failure")
        return original_recover()

    store.recover_orphaned_downloads = flaky_recover
    handled = []

    async def handler(task):
        handled.append(task.task_id)
        return "recovered.bin"

    worker = DownloadWorker(store, handler, poll_interval=0.01)
    worker.needs_orphan_recovery = True

    # A failed recovery must leave the recovery flag set. The next worker
    # iteration must retry recovery before claim_next sees the orphaned row.
    with pytest.raises(OSError, match="temporary SQLite recovery failure"):
        await worker._run_once()

    assert worker.needs_orphan_recovery is True
    await worker._run_once()

    [task] = store.recent()
    assert recovery_calls == 2
    assert handled == [task_id]
    assert task.status == "completed"
    assert task.detail == "recovered.bin"
    assert task.attempts == 1

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Awaitable, Callable

from .task_store import DownloadTask, DownloadTaskStore

logger = logging.getLogger(__name__)


class RetryableTaskError(RuntimeError):
    """A transient failure that may succeed when the task is retried later."""


class AuthorizationRequired(RuntimeError):
    """The Telegram user session must be authorized before work can continue."""


class FloodWait(RuntimeError):
    """Telegram explicitly instructed the client to wait before another request."""

    def __init__(self, seconds: int) -> None:
        self.seconds = max(0, int(seconds))
        super().__init__(f"Telegram FloodWait: {self.seconds} seconds")


TaskHandler = Callable[[DownloadTask], Awaitable[str | None]]


class DownloadWorker:
    """Single-consumer durable scheduler shared by every download source."""

    def __init__(
        self,
        store: DownloadTaskStore,
        handler: TaskHandler,
        *,
        poll_interval: float = 1.0,
        max_attempts: int = 5,
        retry_base_delay: float = 5.0,
        retry_max_delay: float = 300.0,
    ) -> None:
        self.store = store
        self.handler = handler
        self.poll_interval = max(0.1, poll_interval)
        self.max_attempts = max(1, max_attempts)
        self.retry_base_delay = max(0.1, retry_base_delay)
        self.retry_max_delay = max(self.retry_base_delay, retry_max_delay)
        self.active_task_id: int | None = None
        self.local_cooldown_until = 0.0

    async def run(self) -> None:
        """Run until cancelled; recover from transient scheduler/database errors."""
        while True:
            try:
                await self._run_once()
            except asyncio.CancelledError:
                raise
            except Exception:
                task_id = self.active_task_id
                if task_id is not None:
                    try:
                        self.store.interrupt(
                            task_id, "Worker loop failed; task will be recovered on restart"
                        )
                    except Exception:
                        logger.exception("Could not persist interrupted state for task %s", task_id)
                self.active_task_id = None
                logger.exception("Download scheduler loop failed; retrying in 5 seconds")
                await asyncio.sleep(5)

    async def _run_once(self) -> None:
        cooldown = max(
            self.store.cooldown_remaining(),
            self.local_cooldown_until - time.time(),
        )
        if cooldown > 0:
            logger.warning("Global Telegram cooldown active for %.1f more seconds", cooldown)
            await asyncio.sleep(cooldown)
            return

        task = self.store.claim_next()
        if task is None:
            await asyncio.sleep(self.poll_interval)
            return

        self.active_task_id = task.task_id
        try:
            detail = await self.handler(task)
        except asyncio.CancelledError:
            try:
                self.store.interrupt(
                    task.task_id, "Worker stopped; task will resume on restart"
                )
            except Exception:
                logger.exception("Could not persist shutdown state for task %s", task.task_id)
            self.active_task_id = None
            raise
        except FloodWait as exc:
            # Enforce the wait in memory immediately, even if SQLite is temporarily unavailable.
            self.local_cooldown_until = max(
                self.local_cooldown_until, time.time() + exc.seconds
            )
            until = self.store.set_global_cooldown(exc.seconds)
            self.local_cooldown_until = max(self.local_cooldown_until, until)
            delay = max(0.0, until - time.time())
            self.store.retry_later(
                task.task_id,
                f"Telegram requested a global cooldown of {exc.seconds} seconds",
                delay,
            )
            logger.warning(
                "Task %s paused by Telegram FloodWait; global cooldown %.1fs",
                task.task_id,
                delay,
            )
        except AuthorizationRequired as exc:
            self.store.require_auth(task.task_id, str(exc))
            logger.warning("Task %s paused pending Telegram authorization", task.task_id)
        except RetryableTaskError as exc:
            if task.attempts >= self.max_attempts:
                self.store.fail(
                    task.task_id,
                    f"Retry limit reached after {task.attempts} attempts: {exc}",
                )
            else:
                delay = min(
                    self.retry_base_delay * (2 ** (task.attempts - 1)),
                    self.retry_max_delay,
                )
                self.store.retry_later(task.task_id, str(exc), delay)
                logger.warning(
                    "Task %s will retry in %.1fs (attempt %s/%s)",
                    task.task_id,
                    delay,
                    task.attempts,
                    self.max_attempts,
                )
        except Exception as exc:
            logger.exception("Unclassified worker failure for task %s", task.task_id)
            self.store.fail(task.task_id, f"Internal error: {type(exc).__name__}")
        else:
            self.store.complete(task.task_id, detail or "Completed")
        self.active_task_id = None

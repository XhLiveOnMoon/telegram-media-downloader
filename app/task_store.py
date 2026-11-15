from __future__ import annotations

import sqlite3
import time
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path


@dataclass(frozen=True, slots=True)
class DownloadTask:
    task_id: int
    task_type: str
    source: str
    status: str
    detail: str
    created_at: str
    updated_at: str
    attempts: int = 0
    next_attempt_at: float | None = None
    progress_bytes: int = 0
    total_bytes: int | None = None
    current_file: str = ""


class DownloadTaskStore:
    """Durable task ledger and global Telegram cooldown state (SSD-backed SQLite)."""

    _STATUSES = (
        "queued", "downloading", "retry_wait", "completed",
        "failed", "interrupted", "auth_required", "cancelled",
    )

    def __init__(self, database: Path) -> None:
        self.database = database
        self.database.parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(self.database, timeout=30) as bootstrap:
            bootstrap.execute("PRAGMA journal_mode=WAL")
        with self._connect() as connection:
            self._migrate(connection)
            connection.execute(
                "UPDATE download_tasks SET status='interrupted', "
                "attempts=MAX(0, attempts - 1), next_attempt_at=NULL, "
                "detail='Service restarted before task completion', updated_at=? "
                "WHERE status='downloading'",
                (self._now(),),
            )

    @contextmanager
    def _connect(self) -> Iterator[sqlite3.Connection]:
        connection = sqlite3.connect(self.database, timeout=30, isolation_level=None)
        connection.row_factory = sqlite3.Row
        try:
            connection.execute("PRAGMA busy_timeout=30000")
            connection.execute("PRAGMA synchronous=FULL")
            connection.execute("BEGIN IMMEDIATE")
            yield connection
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    @staticmethod
    def _now() -> str:
        return datetime.now(UTC).isoformat(timespec="seconds")

    def _migrate(self, connection: sqlite3.Connection) -> None:
        exists = connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='download_tasks'"
        ).fetchone()
        if not exists:
            self._create_schema(connection)
            return

        columns = {
            row["name"] for row in connection.execute("PRAGMA table_info(download_tasks)")
        }
        schema_row = connection.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name='download_tasks'"
        ).fetchone()
        schema_sql = schema_row["sql"] or ""
        has_all_statuses = all(f"'{status}'" in schema_sql for status in self._STATUSES)
        has_retry_columns = {"attempts", "next_attempt_at"}.issubset(columns)
        if has_retry_columns and has_all_statuses:
            if "progress_bytes" not in columns:
                connection.execute(
                    "ALTER TABLE download_tasks ADD COLUMN progress_bytes "
                    "INTEGER NOT NULL DEFAULT 0"
                )
            if "total_bytes" not in columns:
                connection.execute(
                    "ALTER TABLE download_tasks ADD COLUMN total_bytes INTEGER"
                )
            if "current_file" not in columns:
                connection.execute(
                    "ALTER TABLE download_tasks ADD COLUMN current_file "
                    "TEXT NOT NULL DEFAULT ''"
                )
            connection.execute(
                "CREATE INDEX IF NOT EXISTS idx_download_tasks_queue "
                "ON download_tasks(status, next_attempt_at, id)"
            )
            connection.execute(
                "CREATE INDEX IF NOT EXISTS idx_download_tasks_created "
                "ON download_tasks(created_at DESC)"
            )
            self._ensure_scheduler_schema(connection)
            return

        # Rebuild older/intermediate schemas when retry columns or the status
        # CHECK constraint are behind. Preserve progress and retry metadata.
        has_attempts = "attempts" in columns
        has_next_attempt = "next_attempt_at" in columns
        has_progress = "progress_bytes" in columns
        has_total = "total_bytes" in columns
        has_current_file = "current_file" in columns
        connection.execute("ALTER TABLE download_tasks RENAME TO download_tasks_legacy")
        connection.execute("DROP INDEX IF EXISTS idx_download_tasks_created")
        connection.execute("DROP INDEX IF EXISTS idx_download_tasks_queue")
        self._create_schema(connection)

        attempts_expr = "attempts" if has_attempts else "0"
        attempts_expr = (
            f"CASE WHEN status IN ('running','downloading') "
            f"THEN MAX(0, {attempts_expr} - 1) ELSE {attempts_expr} END"
        )
        next_attempt_expr = "next_attempt_at" if has_next_attempt else "NULL"
        progress_expr = "progress_bytes" if has_progress else "0"
        total_expr = "total_bytes" if has_total else "NULL"
        current_file_expr = "current_file" if has_current_file else "''"
        connection.execute(
            f"""
            INSERT INTO download_tasks
                (id, task_type, source, status, detail, created_at, updated_at,
                 attempts, next_attempt_at, progress_bytes, total_bytes, current_file)
            SELECT id, task_type, source,
                CASE status
                    WHEN 'running' THEN 'interrupted'
                    WHEN 'downloading' THEN 'interrupted'
                    WHEN 'interrupted' THEN 'interrupted'
                    WHEN 'queued' THEN 'queued'
                    WHEN 'retry_wait' THEN 'retry_wait'
                    WHEN 'auth_required' THEN 'auth_required'
                    WHEN 'cancelled' THEN 'cancelled'
                    WHEN 'completed' THEN 'completed'
                    WHEN 'failed' THEN 'failed'
                    ELSE 'failed'
                END,
                CASE
                    WHEN status IN ('running', 'downloading') AND detail=''
                    THEN 'Service restarted before task completion'
                    ELSE detail
                END,
                created_at, updated_at, {attempts_expr}, {next_attempt_expr},
                {progress_expr}, {total_expr}, {current_file_expr}
            FROM download_tasks_legacy
            """
        )
        connection.execute("DROP TABLE download_tasks_legacy")

    @classmethod
    def _create_schema(cls, connection: sqlite3.Connection) -> None:
        statuses = ", ".join(f"'{status}'" for status in cls._STATUSES)
        connection.execute(
            f"""
            CREATE TABLE download_tasks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                task_type TEXT NOT NULL,
                source TEXT NOT NULL,
                status TEXT NOT NULL CHECK (status IN ({statuses})),
                detail TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
                next_attempt_at REAL,
                progress_bytes INTEGER NOT NULL DEFAULT 0 CHECK (progress_bytes >= 0),
                total_bytes INTEGER CHECK (total_bytes IS NULL OR total_bytes >= 0),
                current_file TEXT NOT NULL DEFAULT ''
            )
            """
        )
        connection.execute(
            "CREATE INDEX idx_download_tasks_queue "
            "ON download_tasks(status, next_attempt_at, id)"
        )
        connection.execute(
            "CREATE INDEX idx_download_tasks_created "
            "ON download_tasks(created_at DESC)"
        )
        cls._ensure_scheduler_schema(connection)

    @staticmethod
    def _ensure_scheduler_schema(connection: sqlite3.Connection) -> None:
        connection.execute(
            "CREATE TABLE IF NOT EXISTS scheduler_state "
            "(key TEXT PRIMARY KEY, value REAL NOT NULL)"
        )
        connection.execute(
            "INSERT OR IGNORE INTO scheduler_state(key, value) VALUES ('cooldown_until', 0)"
        )

    @staticmethod
    def _task(row: sqlite3.Row) -> DownloadTask:
        return DownloadTask(**dict(row))

    def enqueue(self, task_type: str, source: str) -> int:
        """Persist a task, reusing an identical task that is already non-terminal."""
        task_id, _created = self.enqueue_with_status(task_type, source)
        return task_id

    def enqueue_with_status(self, task_type: str, source: str) -> tuple[int, bool]:
        """Return the task ID and whether a new queue record was created."""
        normalized_source = source[:4000]
        now = self._now()
        with self._connect() as connection:
            existing = connection.execute(
                "SELECT id FROM download_tasks "
                "WHERE task_type=? AND source=? "
                "AND status IN ('queued','downloading','retry_wait','interrupted','auth_required') "
                "ORDER BY id DESC LIMIT 1",
                (task_type, normalized_source),
            ).fetchone()
            if existing is not None:
                return int(existing["id"]), False
            cursor = connection.execute(
                "INSERT INTO download_tasks "
                "(task_type, source, status, created_at, updated_at) "
                "VALUES (?, ?, 'queued', ?, ?)",
                (task_type, normalized_source, now, now),
            )
            return int(cursor.lastrowid), True

    def claim_next(self, now: float | None = None) -> DownloadTask | None:
        """Atomically claim the oldest due task, respecting the global FloodWait."""
        timestamp = time.time() if now is None else now
        with self._connect() as connection:
            active = connection.execute(
                "SELECT 1 FROM download_tasks WHERE status='downloading' LIMIT 1"
            ).fetchone()
            if active is not None:
                return None
            cooldown = connection.execute(
                "SELECT value FROM scheduler_state WHERE key='cooldown_until'"
            ).fetchone()
            if cooldown and cooldown["value"] > timestamp:
                return None
            row = connection.execute(
                """
                SELECT id FROM download_tasks
                WHERE status='queued'
                   OR (status='retry_wait' AND
                       (next_attempt_at IS NULL OR next_attempt_at <= ?))
                   OR status='interrupted'
                ORDER BY id ASC LIMIT 1
                """,
                (timestamp,),
            ).fetchone()
            if row is None:
                return None
            task_id = int(row["id"])
            cursor = connection.execute(
                "UPDATE download_tasks SET status='downloading', detail=?, "
                "attempts=attempts+1, next_attempt_at=NULL, updated_at=? "
                "WHERE id=? AND status IN ('queued','retry_wait','interrupted')",
                ("Claimed by the download worker", self._now(), task_id),
            )
            if cursor.rowcount != 1:
                return None
            claimed = connection.execute(
                "SELECT id AS task_id, task_type, source, status, detail, created_at, "
                "updated_at, attempts, next_attempt_at, progress_bytes, total_bytes, "
                "current_file FROM download_tasks WHERE id=?",
                (task_id,),
            ).fetchone()
            return self._task(claimed)

    def complete(self, task_id: int, detail: str = "") -> None:
        self._set_status(task_id, "completed", detail)

    def fail(self, task_id: int, detail: str) -> None:
        self._set_status(task_id, "failed", detail)

    def interrupt(self, task_id: int, detail: str) -> None:
        """Interrupt a task without charging the network retry budget."""
        with self._connect() as connection:
            cursor = connection.execute(
                "UPDATE download_tasks SET status='interrupted', detail=?, "
                "attempts=MAX(0, attempts - 1), next_attempt_at=NULL, updated_at=? "
                "WHERE id=? AND status='downloading'",
                (detail[:1000], self._now(), task_id),
            )
            if cursor.rowcount != 1:
                raise RuntimeError(f"Task {task_id} is not in downloading state")

    def require_auth(self, task_id: int, detail: str) -> None:
        self._set_status(task_id, "auth_required", detail)

    def update_progress(
        self,
        task_id: int,
        progress_bytes: int,
        total_bytes: int | None,
        current_file: str,
    ) -> bool:
        progress = max(0, int(progress_bytes))
        total = None if total_bytes is None else max(0, int(total_bytes))
        with self._connect() as connection:
            cursor = connection.execute(
                "UPDATE download_tasks SET progress_bytes=?, total_bytes=?, "
                "current_file=?, updated_at=? WHERE id=? AND status='downloading'",
                (progress, total, current_file[:180], self._now(), task_id),
            )
            return cursor.rowcount == 1

    def cancel(self, task_id: int, detail: str = "Cancelled by owner") -> bool:
        with self._connect() as connection:
            cursor = connection.execute(
                "UPDATE download_tasks SET status='cancelled', detail=?, "
                "next_attempt_at=NULL, updated_at=? WHERE id=? "
                "AND status IN ('queued','retry_wait','interrupted','auth_required')",
                (detail[:1000], self._now(), task_id),
            )
            return cursor.rowcount == 1

    def cancel_active(self, task_id: int, detail: str = "Cancelled by owner") -> bool:
        with self._connect() as connection:
            cursor = connection.execute(
                "UPDATE download_tasks SET status='cancelled', detail=?, "
                "next_attempt_at=NULL, updated_at=? WHERE id=? AND status='downloading'",
                (detail[:1000], self._now(), task_id),
            )
            return cursor.rowcount == 1

    def requeue(self, task_id: int) -> bool:
        """Allow explicit retry only for terminal failed/cancelled tasks."""
        with self._connect() as connection:
            cursor = connection.execute(
                "UPDATE download_tasks SET status='queued', detail=?, attempts=0, "
                "next_attempt_at=NULL, updated_at=? WHERE id=? "
                "AND status IN ('failed','cancelled')",
                ("Manually requeued by owner", self._now(), task_id),
            )
            return cursor.rowcount == 1

    def retry_later(
        self,
        task_id: int,
        detail: str,
        delay_seconds: float,
        *,
        preserve_attempts: bool = False,
    ) -> None:
        delay = max(0.0, delay_seconds)
        with self._connect() as connection:
            cursor = connection.execute(
                "UPDATE download_tasks SET status='retry_wait', detail=?, "
                "next_attempt_at=?, updated_at=?, "
                "attempts=CASE WHEN ? THEN MAX(0, attempts - 1) ELSE attempts END "
                "WHERE id=? AND status='downloading'",
                (detail[:1000], time.time() + delay, self._now(), int(preserve_attempts), task_id),
            )
            if cursor.rowcount != 1:
                raise RuntimeError(f"Task {task_id} is not in downloading state")

    def defer_floodwait(
        self, task_id: int, seconds: float, now: float | None = None
    ) -> float:
        """Atomically persist a FloodWait cooldown and defer its active task."""
        base = time.time() if now is None else now
        until = base + max(0.0, seconds)
        with self._connect() as connection:
            connection.execute(
                "UPDATE scheduler_state SET value=MAX(value, ?) WHERE key='cooldown_until'",
                (until,),
            )
            row = connection.execute(
                "SELECT value FROM scheduler_state WHERE key='cooldown_until'"
            ).fetchone()
            persisted_until = float(row["value"])
            cursor = connection.execute(
                "UPDATE download_tasks SET status='retry_wait', detail=?, "
                "next_attempt_at=?, updated_at=?, attempts=MAX(0, attempts - 1) "
                "WHERE id=? AND status='downloading'",
                (
                    "Paused by Telegram FloodWait",
                    persisted_until,
                    self._now(),
                    task_id,
                ),
            )
            if cursor.rowcount != 1:
                raise RuntimeError(f"Task {task_id} is not in downloading state")
            return persisted_until

    def set_global_cooldown(self, seconds: float, now: float | None = None) -> float:
        """Persist a server-mandated cooldown; never shorten an existing cooldown."""
        base = time.time() if now is None else now
        until = base + max(0.0, seconds)
        with self._connect() as connection:
            connection.execute(
                "UPDATE scheduler_state SET value=MAX(value, ?) WHERE key='cooldown_until'",
                (until,),
            )
            row = connection.execute(
                "SELECT value FROM scheduler_state WHERE key='cooldown_until'"
            ).fetchone()
            return float(row["value"])

    def cooldown_remaining(self, now: float | None = None) -> float:
        base = time.time() if now is None else now
        with self._connect() as connection:
            row = connection.execute(
                "SELECT value FROM scheduler_state WHERE key='cooldown_until'"
            ).fetchone()
        return max(0.0, float(row["value"]) - base)

    def _set_status(self, task_id: int, status: str, detail: str) -> None:
        with self._connect() as connection:
            cursor = connection.execute(
                "UPDATE download_tasks SET status=?, detail=?, next_attempt_at=NULL, "
                "updated_at=?, progress_bytes=CASE WHEN ?='completed' "
                "AND total_bytes IS NOT NULL THEN total_bytes ELSE progress_bytes END "
                "WHERE id=? AND status='downloading'",
                (status, detail[:1000], self._now(), status, task_id),
            )
            if cursor.rowcount != 1:
                raise RuntimeError(f"Task {task_id} is not in downloading state")

    def recover_orphaned_downloads(self) -> int:
        """Recover claimed work without charging a failed network attempt."""
        with self._connect() as connection:
            cursor = connection.execute(
                "UPDATE download_tasks SET status='interrupted', detail=?, "
                "attempts=MAX(0, attempts - 1), next_attempt_at=NULL, updated_at=? "
                "WHERE status='downloading'",
                ("Recovered after a worker persistence failure", self._now()),
            )
            return cursor.rowcount

    def resume_auth_required(self) -> int:
        """Requeue authorization-blocked work after the owner completes /auth."""
        with self._connect() as connection:
            cursor = connection.execute(
                "UPDATE download_tasks SET status='queued', detail=?, attempts=0, "
                "next_attempt_at=NULL, updated_at=? WHERE status='auth_required'",
                ("Requeued after Telegram authorization", self._now()),
            )
            return cursor.rowcount

    def recent(self, limit: int = 10) -> list[DownloadTask]:
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT id AS task_id, task_type, source, status, detail, created_at, "
                "updated_at, attempts, next_attempt_at, progress_bytes, total_bytes, "
                "current_file FROM download_tasks ORDER BY id DESC LIMIT ?",
                (max(1, min(limit, 50)),),
            ).fetchall()
        return [self._task(row) for row in rows]

from __future__ import annotations

import sqlite3
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


class DownloadTaskStore:
    """Durable SQLite task ledger stored on the SSD-backed application volume."""

    def __init__(self, database: Path) -> None:
        self.database = database
        self.database.parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as connection:
            # WAL improves reader/writer concurrency; FULL asks SQLite to sync commits.
            connection.execute("PRAGMA journal_mode=WAL")
            connection.execute("PRAGMA synchronous=FULL")
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS download_tasks (
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
                "CREATE INDEX IF NOT EXISTS idx_download_tasks_created "
                "ON download_tasks(created_at DESC)"
            )
            connection.execute(
                "UPDATE download_tasks SET status = 'interrupted', "
                "detail = 'Service restarted before task completion', updated_at = ? "
                "WHERE status = 'running'",
                (self._now(),),
            )

    @contextmanager
    def _connect(self) -> Iterator[sqlite3.Connection]:
        connection = sqlite3.connect(self.database, timeout=30)
        connection.row_factory = sqlite3.Row
        try:
            connection.execute("PRAGMA busy_timeout=30000")
            connection.execute("PRAGMA synchronous=FULL")
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

    def create(self, task_type: str, source: str) -> int:
        now = self._now()
        with self._connect() as connection:
            cursor = connection.execute(
                "INSERT INTO download_tasks "
                "(task_type, source, status, created_at, updated_at) "
                "VALUES (?, ?, 'running', ?, ?)",
                (task_type, source[:500], now, now),
            )
            return int(cursor.lastrowid)

    def complete(self, task_id: int, detail: str = "") -> None:
        self._set_status(task_id, "completed", detail)

    def fail(self, task_id: int, detail: str) -> None:
        self._set_status(task_id, "failed", detail)

    def interrupt(self, task_id: int, detail: str) -> None:
        self._set_status(task_id, "interrupted", detail)

    def _set_status(self, task_id: int, status: str, detail: str) -> None:
        with self._connect() as connection:
            connection.execute(
                "UPDATE download_tasks SET status = ?, detail = ?, updated_at = ? WHERE id = ?",
                (status, detail[:1000], self._now(), task_id),
            )

    def interrupted(self, limit: int = 50) -> list[DownloadTask]:
        """Return restart-interrupted tasks oldest first for deterministic recovery."""
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT id AS task_id, task_type, source, status, detail, created_at, updated_at "
                "FROM download_tasks WHERE status = 'interrupted' ORDER BY id ASC LIMIT ?",
                (max(1, min(limit, 500)),),
            ).fetchall()
        return [DownloadTask(**dict(row)) for row in rows]

    def mark_running(self, task_id: int) -> None:
        """Mark a recovered task as running before attempting its replay."""
        with self._connect() as connection:
            connection.execute(
                "UPDATE download_tasks SET status = 'running', detail = ?, updated_at = ? "
                "WHERE id = ? AND status = 'interrupted'",
                ("Automatically resumed after service restart", self._now(), task_id),
            )

    def recent(self, limit: int = 10) -> list[DownloadTask]:
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT id AS task_id, task_type, source, status, detail, created_at, updated_at "
                "FROM download_tasks ORDER BY id DESC LIMIT ?",
                (max(1, min(limit, 50)),),
            ).fetchall()
        return [DownloadTask(**dict(row)) for row in rows]

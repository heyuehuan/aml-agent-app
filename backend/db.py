"""SQLite job store for AML agent demo."""

from __future__ import annotations

import json
import sqlite3
import uuid
from datetime import datetime, timezone
from pathlib import Path
from threading import Lock
from typing import Any

DB_PATH = Path(__file__).parent / "jobs.db"
_lock = Lock()


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(str(DB_PATH), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    with _lock, _conn() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS jobs (
                id          TEXT PRIMARY KEY,
                subject     TEXT NOT NULL,
                status      TEXT NOT NULL DEFAULT 'queued',
                created_at  TEXT NOT NULL,
                started_at  TEXT,
                finished_at TEXT,
                risk_level  TEXT,
                report_md   TEXT,
                events_json TEXT,
                error       TEXT
            )
        """)
        conn.commit()


def _row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    d = dict(row)
    if d.get("events_json"):
        d["events"] = json.loads(d["events_json"])
    else:
        d["events"] = []
    del d["events_json"]
    return d


def create_job(subject: str) -> dict[str, Any]:
    job_id = str(uuid.uuid4())
    now = datetime.now(timezone.utc).isoformat()
    with _lock, _conn() as conn:
        conn.execute(
            "INSERT INTO jobs (id, subject, status, created_at) VALUES (?, ?, 'queued', ?)",
            (job_id, subject, now),
        )
        conn.commit()
    return get_job(job_id)  # type: ignore[return-value]


def list_jobs() -> list[dict[str, Any]]:
    with _lock, _conn() as conn:
        rows = conn.execute(
            "SELECT id, subject, status, created_at, started_at, finished_at, risk_level, error "
            "FROM jobs ORDER BY created_at DESC"
        ).fetchall()
    return [dict(r) for r in rows]


def get_job(job_id: str) -> dict[str, Any] | None:
    with _lock, _conn() as conn:
        row = conn.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
    if row is None:
        return None
    return _row_to_dict(row)


def mark_running(job_id: str) -> None:
    now = datetime.now(timezone.utc).isoformat()
    with _lock, _conn() as conn:
        conn.execute(
            "UPDATE jobs SET status='running', started_at=? WHERE id=?",
            (now, job_id),
        )
        conn.commit()


def mark_complete(
    job_id: str,
    events: list[dict],
    report_md: str,
    risk_level: str | None,
) -> None:
    now = datetime.now(timezone.utc).isoformat()
    with _lock, _conn() as conn:
        conn.execute(
            """UPDATE jobs
               SET status='completed', finished_at=?, risk_level=?,
                   report_md=?, events_json=?
               WHERE id=?""",
            (now, risk_level, report_md, json.dumps(events), job_id),
        )
        conn.commit()


def mark_failed(job_id: str, error: str, events: list[dict]) -> None:
    now = datetime.now(timezone.utc).isoformat()
    with _lock, _conn() as conn:
        conn.execute(
            """UPDATE jobs
               SET status='failed', finished_at=?, error=?, events_json=?
               WHERE id=?""",
            (now, error[:2000], json.dumps(events), job_id),
        )
        conn.commit()


def update_events_partial(job_id: str, events: list[dict]) -> None:
    """Persist accumulated events for a running job (allows reconnect replay)."""
    with _lock, _conn() as conn:
        conn.execute(
            "UPDATE jobs SET events_json=? WHERE id=? AND status='running'",
            (json.dumps(events), job_id),
        )
        conn.commit()


def reset_to_queued(job_id: str) -> None:
    with _lock, _conn() as conn:
        conn.execute(
            """UPDATE jobs
               SET status='queued', started_at=NULL, finished_at=NULL,
                   error=NULL, risk_level=NULL, events_json=NULL, report_md=NULL
               WHERE id=?""",
            (job_id,),
        )
        conn.commit()


def delete_job(job_id: str) -> None:
    with _lock, _conn() as conn:
        conn.execute("DELETE FROM jobs WHERE id=?", (job_id,))
        conn.commit()

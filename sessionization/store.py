"""SQLite persistence layer (schema + helpers)."""
from __future__ import annotations

import os
import sqlite3
from typing import Iterator

DDL = """
CREATE TABLE IF NOT EXISTS dim_users (
    user_id TEXT PRIMARY KEY,
    country TEXT
);
CREATE TABLE IF NOT EXISTS events_clean (
    event_id       TEXT PRIMARY KEY,
    user_id        TEXT NOT NULL,
    event_type     TEXT NOT NULL,
    event_ts       TEXT NOT NULL,
    event_epoch_us INTEGER NOT NULL,
    page_url       TEXT,
    session_id     TEXT,
    source_file    TEXT NOT NULL,
    source_line    INTEGER NOT NULL,
    run_id         TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_events_user_time ON events_clean (user_id, event_epoch_us, event_id);
CREATE INDEX IF NOT EXISTS ix_events_session ON events_clean (session_id);
CREATE TABLE IF NOT EXISTS sessions (
    session_id        TEXT PRIMARY KEY,
    user_id           TEXT NOT NULL,
    session_start_ts  TEXT NOT NULL,
    session_end_ts    TEXT NOT NULL,
    duration_seconds  REAL NOT NULL,
    event_count       INTEGER NOT NULL,
    first_event_id    TEXT NOT NULL,
    last_event_id     TEXT NOT NULL,
    first_event_type  TEXT NOT NULL,
    last_event_type   TEXT NOT NULL,
    run_id            TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_sessions_user ON sessions (user_id);
CREATE TABLE IF NOT EXISTS rejects (
    reject_id    INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id       TEXT NOT NULL,
    source_file  TEXT NOT NULL,
    source_line  INTEGER NOT NULL,
    reason       TEXT NOT NULL,
    detail       TEXT,
    raw_record   TEXT
);
CREATE INDEX IF NOT EXISTS ix_rejects_file ON rejects (source_file);
CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id           TEXT PRIMARY KEY,
    source_file      TEXT NOT NULL,
    file_sha256      TEXT,
    status           TEXT NOT NULL,          -- RUNNING | SUCCEEDED | FAILED | ABANDONED
    started_at       TEXT NOT NULL,
    ended_at         TEXT,
    input_rows       INTEGER,
    accepted_new     INTEGER,
    replayed         INTEGER,
    rejected         INTEGER,
    users_affected   INTEGER,
    sessions_written INTEGER,
    error            TEXT
);
"""


def connect(db_path: str) -> sqlite3.Connection:
    d = os.path.dirname(os.path.abspath(db_path))
    os.makedirs(d, exist_ok=True)
    conn = sqlite3.connect(db_path, isolation_level=None)  # explicit BEGIN/COMMIT
    conn.row_factory = sqlite3.Row
    return conn


def init_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(DDL)


def chunked(items: list, n: int) -> Iterator[list]:
    for i in range(0, len(items), n):
        yield items[i:i + n]

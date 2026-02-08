# src/masphd/dao/sqlite.py
from __future__ import annotations

import sqlite3
from typing import Optional
from pathlib import Path


def connect_sqlite(
    db_path: str,
    *,
    wal: bool = True,
    synchronous: str = "NORMAL",
    busy_timeout_ms: int = 5000,
    row_factory: bool = False,
) -> sqlite3.Connection:
    """
    Create a SQLite connection with sensible defaults for concurrent read/write.

    Notes:
    - WAL improves concurrency (readers don't block writer as much).
    - busy_timeout helps when DB is briefly locked.
    """
    # Ensure parent directory exists (ignore paths like "app.db")
    parent = db_path.parent
    if parent and parent != Path("."):
        parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(db_path)
    if row_factory:
        conn.row_factory = sqlite3.Row

    if wal:
        conn.execute("PRAGMA journal_mode=WAL;")

    if synchronous:
        conn.execute(f"PRAGMA synchronous={synchronous};")

    if busy_timeout_ms and busy_timeout_ms > 0:
        conn.execute(f"PRAGMA busy_timeout={int(busy_timeout_ms)};")

    return conn

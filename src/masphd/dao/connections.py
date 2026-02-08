from __future__ import annotations

import sqlite3

from .schema import apply_pragmas, ensure_schema


def connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    apply_pragmas(conn)
    ensure_schema(conn)
    return conn

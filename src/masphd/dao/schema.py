# src/masphd/dao/schema.py
from __future__ import annotations

import sqlite3


def _column_exists(conn: sqlite3.Connection, table: str, column: str) -> bool:
    rows = conn.execute(f"PRAGMA table_info({table});").fetchall()
    for r in rows:
        # row format: (cid, name, type, notnull, dflt_value, pk)
        if r[1] == column:
            return True
    return False


def _add_column_if_missing(conn: sqlite3.Connection, table: str, column_def_sql: str, column_name: str) -> None:
    if not _column_exists(conn, table, column_name):
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {column_def_sql};")


def ensure_schema(conn: sqlite3.Connection) -> None:
    """
    Ensure all tables exist + apply small additive migrations.
    Uses provided conn only.
    """

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS predictions_all (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at_utc TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),

            rid TEXT NOT NULL,
            ssd TEXT,
            first TEXT NOT NULL,
            second TEXT NOT NULL,

            planned_dep TEXT,
            dep_time TEXT,
            dep_time_kind TEXT,
            has_actual_dep INTEGER NOT NULL,
            actual_dep_confirmed TEXT,

            departure_delay REAL,
            dwell_delay REAL,

            peak INTEGER,
            day_of_week TEXT,
            day_of_month INTEGER,
            hour_of_day INTEGER,
            weekend INTEGER,
            season TEXT,
            month INTEGER,
            holiday INTEGER,

            predicted_delay REAL,

            UNIQUE(rid, first, second, planned_dep)
        );
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS predictions_actual (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at_utc TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),

            rid TEXT NOT NULL,
            ssd TEXT,
            first TEXT NOT NULL,
            second TEXT NOT NULL,

            planned_dep TEXT,
            dep_time TEXT,
            dep_time_kind TEXT,
            has_actual_dep INTEGER NOT NULL,
            actual_dep_confirmed TEXT,

            departure_delay REAL,
            dwell_delay REAL,

            peak INTEGER,
            day_of_week TEXT,
            day_of_month INTEGER,
            hour_of_day INTEGER,
            weekend INTEGER,
            season TEXT,
            month INTEGER,
            holiday INTEGER,

            predicted_delay REAL,

            UNIQUE(rid, first, second, planned_dep)
        );
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS actual_arrivals_hsp (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at_utc TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),

            rid TEXT NOT NULL,
            ssd TEXT,
            first TEXT NOT NULL,
            second TEXT NOT NULL,
            planned_dep TEXT,

            is_main_journey INTEGER NOT NULL DEFAULT 0,

            predicted_delay REAL,

            planned_arr TEXT,
            actual_arr TEXT,
            actual_arr_delay REAL,

            toc_code TEXT,
            hsp_location_crs TEXT,
            hsp_tpls TEXT,

            UNIQUE(rid, first, second, planned_dep)
        );
        """
    )

    # ---- additive migrations (safe if table already exists) ----
    # If you created the table earlier without is_main_journey, add it.
    _add_column_if_missing(conn, "actual_arrivals_hsp", "is_main_journey INTEGER NOT NULL DEFAULT 0", "is_main_journey")
    _add_column_if_missing(conn, "actual_arrivals_hsp", "predicted_delay REAL", "predicted_delay")
    _add_column_if_missing(conn, "actual_arrivals_hsp", "planned_arr TEXT", "planned_arr")
    _add_column_if_missing(conn, "actual_arrivals_hsp", "actual_arr TEXT", "actual_arr")
    _add_column_if_missing(conn, "actual_arrivals_hsp", "actual_arr_delay REAL", "actual_arr_delay")
    _add_column_if_missing(conn, "actual_arrivals_hsp", "toc_code TEXT", "toc_code")
    _add_column_if_missing(conn, "actual_arrivals_hsp", "hsp_location_crs TEXT", "hsp_location_crs")
    _add_column_if_missing(conn, "actual_arrivals_hsp", "hsp_tpls TEXT", "hsp_tpls")
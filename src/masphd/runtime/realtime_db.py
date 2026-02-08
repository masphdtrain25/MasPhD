# src/masphd/runtime/realtime_db.py
# (Updated to import from dao, keeping the old import path working)
from __future__ import annotations

from masphd.dao.realtime_store import RealTimeSQLiteStore

__all__ = ["RealTimeSQLiteStore"]

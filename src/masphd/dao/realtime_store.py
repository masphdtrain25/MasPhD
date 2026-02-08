# src/masphd/dao/realtime_store.py
from __future__ import annotations

import sqlite3
import threading
from queue import Queue, Empty
from typing import Dict, Any, Optional, Tuple

from .schema import ensure_schema
from .sqlite import connect_sqlite


_SENTINEL: Tuple[Optional[str], Optional[Dict[str, Any]]] = (None, None)


class RealTimeSQLiteStore:
    """
    Threaded writer for realtime inserts.

    - One connection created and used in the writer thread only.
    - Inserts are queued to avoid blocking the realtime listener.
    - Clean shutdown via sentinel to avoid interpreter-shutdown crashes.
    """

    def __init__(self, db_path: str | None = None, *, queue_maxsize: int = 5000):
        self.db_path = db_path or "data/realtime_predictions.db"

        self._q: "Queue[Tuple[Optional[str], Optional[Dict[str, Any]]]]" = Queue(maxsize=queue_maxsize)
        self._stop = threading.Event()
        self._closed = False

        # IMPORTANT: non-daemon thread so Python waits for it to finish cleanly
        self._writer = threading.Thread(target=self._writer_loop, name="sqlite-writer", daemon=False)
        self._writer.start()

    def _writer_loop(self):
        conn = connect_sqlite(self.db_path, wal=True, synchronous="NORMAL", busy_timeout_ms=5000)
        try:
            ensure_schema(conn)
            conn.commit()

            while True:
                try:
                    item = self._q.get(timeout=0.5)
                except Empty:
                    if self._stop.is_set():
                        # if stop requested and queue is empty, exit
                        break
                    continue

                table, rec = item

                try:
                    # Sentinel means exit after draining prior items
                    if table is None and rec is None:
                        break

                    # Normal insert
                    assert table is not None and rec is not None
                    self._insert_with_conn(conn, table, rec)
                    conn.commit()
                finally:
                    self._q.task_done()
        finally:
            conn.close()

    def close(self, *, drain: bool = True, join_timeout: Optional[float] = 10.0):
        """
        Stop the writer thread safely.

        drain=True  -> wait until queued items are processed before stopping.
        drain=False -> stop sooner (may drop queued inserts).

        join_timeout: seconds to wait for thread to exit. None means wait forever.
        """
        if self._closed:
            return
        self._closed = True

        if drain:
            # Wait for queue to flush before stopping
            try:
                self._q.join()
            except Exception:
                # If join fails for any reason, still attempt shutdown
                pass

        self._stop.set()

        # Unblock writer even if it's waiting
        try:
            self._q.put_nowait(_SENTINEL)
        except Exception:
            # If queue is full, don't block; writer will exit on stop when queue drains
            pass

        self._writer.join(timeout=join_timeout)

    # Public API: enqueue instead of writing directly
    def insert_all(self, rec: Dict[str, Any]) -> bool:
        return self._enqueue("predictions_all", rec)

    def insert_actual(self, rec: Dict[str, Any]) -> bool:
        return self._enqueue("predictions_actual", rec)

    def _enqueue(self, table: str, rec: Dict[str, Any]) -> bool:
        if self._closed:
            return False
        try:
            self._q.put((table, rec), block=False)
            return True
        except Exception:
            # queue full (or closing). We do not want to crash realtime pipeline.
            return False

    # Use conn created in writer thread
    def _insert_with_conn(self, conn: sqlite3.Connection, table: str, rec: Dict[str, Any]):
        cols = list(rec.keys())
        vals = [rec[c] for c in cols]
        placeholders = ",".join(["?"] * len(cols))
        sql = f"INSERT OR IGNORE INTO {table} ({','.join(cols)}) VALUES ({placeholders})"
        conn.execute(sql, vals)

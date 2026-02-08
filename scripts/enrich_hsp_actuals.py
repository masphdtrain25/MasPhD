# scripts/enrich_hsp_actuals.py
from __future__ import annotations

import argparse
import logging
import time
from collections import defaultdict
from datetime import datetime, date
from zoneinfo import ZoneInfo
from typing import Any, Dict, List, Optional, Tuple

from masphd.dao.schema import ensure_schema
from masphd.io.paths import DATABASE
from masphd.dao.sqlite import connect_sqlite
from masphd.hsp.client import HSPClient
from masphd.hsp.parser import extract_service_locations
from masphd.dao.actual_arrivals_hsp import (
    build_hsp_index_by_tiploc2,
    make_actual_arrival_record,
    upsert_actual_arrival,
)

log = logging.getLogger(__name__)
LONDON = ZoneInfo("Europe/London")


def _today_london() -> str:
    return datetime.now(LONDON).date().isoformat()


def build_argparser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="masphd-hsp-enrich",
        description="Post-process: fetch HSP service details and store actual arrival ground truth for predictions_actual.",
    )
    p.add_argument(
        "--db",
        type=str,
        default=DATABASE,
        help="Path to SQLite database (default: masphd.io.paths.DATABASE).",
    )
    p.add_argument(
        "--before-date",
        type=str,
        default=None,
        help="Only process rows with ssd < this YYYY-MM-DD (default: today in Europe/London).",
    )
    p.add_argument(
        "--limit-rows",
        type=int,
        default=50000,
        help="Max number of candidate prediction rows to scan (default 50000).",
    )
    p.add_argument(
        "--max-rids",
        type=int,
        default=2000,
        help="Max distinct RIDs to call HSP for in one run (default 2000).",
    )
    p.add_argument(
        "--sleep",
        type=float,
        default=0.0,
        help="Sleep seconds between HSP requests (default 0).",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Do not write to DB, just log actions.",
    )
    p.add_argument(
        "--verbose",
        action="store_true",
        help="Verbose logging.",
    )
    return p


def _fetch_candidate_predictions(
    conn,
    *,
    before_date: str,
    limit_rows: int,
) -> List[Dict[str, Any]]:
    """
    Fetch only rows that:
      - are from before_date (ssd < before_date)
      - are NOT already processed into actual_arrivals_hsp (by unique key)
    """
    sql = """
    SELECT
        p.rid, p.ssd, p.first, p.second, p.planned_dep, p.predicted_delay
    FROM predictions_actual p
    WHERE
        p.ssd IS NOT NULL
        AND p.ssd < ?
        AND NOT EXISTS (
            SELECT 1
            FROM actual_arrivals_hsp a
            WHERE a.rid = p.rid
              AND a.first = p.first
              AND a.second = p.second
              AND (
                    (a.planned_dep IS NULL AND p.planned_dep IS NULL)
                 OR (a.planned_dep = p.planned_dep)
              )
        )
    ORDER BY p.ssd ASC
    LIMIT ?
    """
    rows = conn.execute(sql, (before_date, limit_rows)).fetchall()
    # sqlite.Row -> dict
    return [dict(r) for r in rows]


def main():
    args = build_argparser().parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s\t%(message)s",
    )

    before_date = args.before_date or _today_london()
    log.info("DB=%s | before_date=%s | dry_run=%s", args.db, before_date, args.dry_run)

    conn = connect_sqlite(args.db, wal=True, synchronous="NORMAL", busy_timeout_ms=10000, row_factory=True)

    ensure_schema(conn)
    conn.commit()

    try:
        candidates = _fetch_candidate_predictions(conn, before_date=before_date, limit_rows=args.limit_rows)
        if not candidates:
            log.info("No unprocessed predictions_actual rows found (ssd < %s).", before_date)
            return

        log.info("Candidate rows=%d", len(candidates))

        # Group prediction rows by RID so we call HSP once per RID.
        by_rid: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        for r in candidates:
            rid = r.get("rid")
            if rid:
                by_rid[rid].append(r)

        rids = list(by_rid.keys())[: args.max_rids]
        log.info("Distinct RIDs=%d (processing up to %d)", len(by_rid), len(rids))

        hsp = HSPClient(timeout_secs=25.0)

        written = 0
        skipped_no_hsp = 0
        skipped_no_match = 0
        skipped_no_times = 0

        for i, rid in enumerate(rids, start=1):
            if args.sleep and args.sleep > 0:
                time.sleep(args.sleep)

            raw = hsp.get_service_details_raw(rid)
            if not raw:
                skipped_no_hsp += len(by_rid[rid])
                continue

            hsp_rows = extract_service_locations(raw)
            if not hsp_rows:
                skipped_no_hsp += len(by_rid[rid])
                continue

            # Index by TIPLOC2 (converted from CRS) so it matches predictions_actual first/second.
            hsp_by_t2 = build_hsp_index_by_tiploc2(hsp_rows)

            # Build and upsert one row per prediction row for this RID.
            for pred_row in by_rid[rid]:
                rec = make_actual_arrival_record(pred_row=pred_row, hsp_by_tiploc2=hsp_by_t2, tz=LONDON)

                if rec is None:
                    # Determine rough reason (best-effort)
                    second = pred_row.get("second")
                    if not second or second not in hsp_by_t2:
                        skipped_no_match += 1
                    else:
                        skipped_no_times += 1
                    continue

                if args.dry_run:
                    written += 1
                    continue

                upsert_actual_arrival(conn, rec)
                written += 1

            # commit periodically (avoid huge transactions)
            if not args.dry_run and (i % 50 == 0):
                conn.commit()
                log.info("Progress: %d/%d RIDs | written=%d", i, len(rids), written)

        if not args.dry_run:
            conn.commit()

        log.info(
            "Done. written=%d | skipped_no_hsp=%d | skipped_no_match=%d | skipped_no_times=%d",
            written,
            skipped_no_hsp,
            skipped_no_match,
            skipped_no_times,
        )

    finally:
        conn.close()


if __name__ == "__main__":
    main()

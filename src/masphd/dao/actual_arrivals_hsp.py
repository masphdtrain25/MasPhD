# src/masphd/dao/actual_arrivals_hsp.py
from __future__ import annotations

import sqlite3
from typing import Any, Dict, List, Optional

from masphd.darwin.time_utils import combine_date_time_smart, diff_minutes_wrap
from masphd.darwin.station_pairs import CRS_TO_TIPLOC2, PAIR_SET


def _clean_str(v: Any) -> Optional[str]:
    if v is None:
        return None
    if isinstance(v, str):
        s = v.strip()
        return s or None
    s = str(v).strip()
    return s or None

def _hhmm_to_hh_colon_mm(v: Any) -> Optional[str]:
    """
    Convert HSP time formats to Darwin-compatible ones.

    - HSP: "0657"  -> "06:57"
    - Darwin already: "06:57" or "06:57:30" -> unchanged
    """
    s = _clean_str(v)
    if not s:
        return None
    if ":" in s:
        return s
    if len(s) == 4 and s.isdigit():
        return f"{s[:2]}:{s[2:]}"
    return None

def _crs_to_tiploc2(crs: Optional[str]) -> Optional[str]:
    if not crs:
        return None
    return CRS_TO_TIPLOC2.get(crs)


def build_hsp_index_by_tiploc2(hsp_rows: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """
    HSP rows are expected to be flat per-location rows (Darwin-like),
    where row['tpl'] is CRS (confirmed by you).

    Returns: tiploc2 -> row (last wins).
    """
    out: Dict[str, Dict[str, Any]] = {}
    for r in hsp_rows or []:
        crs = _clean_str(r.get("tpl"))
        t2 = _crs_to_tiploc2(crs)
        if t2:
            out[t2] = r
    return out


def compute_actual_arrival_delay_min(
    *,
    ssd: Optional[str],
    planned_arr_hhmm: Optional[str],
    actual_arr_hhmm: Optional[str],
    base_hhmm: Optional[str] = None,
    tz=None,
) -> Optional[float]:
    """
    Compute (actual_arr - planned_arr) in minutes with wrap safety.

    NOTE:
    - Darwin utilities expect "HH:MM" or "HH:MM:SS"
    - HSP provides "HHMM", so we normalise before combining.
    """
    if not ssd or not planned_arr_hhmm or not actual_arr_hhmm:
        return None

    planned_s = _hhmm_to_hh_colon_mm(planned_arr_hhmm)
    actual_s = _hhmm_to_hh_colon_mm(actual_arr_hhmm)
    base_s = _hhmm_to_hh_colon_mm(base_hhmm) if base_hhmm else None

    if not planned_s or not actual_s:
        return None

    base_dt = combine_date_time_smart(ssd, base_s, tz=tz) if base_s else None
    planned_dt = combine_date_time_smart(ssd, planned_s, base_dt=base_dt, tz=tz)
    actual_dt = combine_date_time_smart(ssd, actual_s, base_dt=planned_dt, tz=tz)

    return diff_minutes_wrap(planned_dt, actual_dt)


def _is_main_journey_pair(first: Optional[str], second: Optional[str]) -> int:
    """
    Returns 1 if (first, second) is one of the tracked main-journey station pairs,
    otherwise 0.
    """
    if not first or not second:
        return 0
    return 1 if (first, second) in PAIR_SET else 0


def make_actual_arrival_record(
    *,
    pred_row: Dict[str, Any],
    hsp_by_tiploc2: Dict[str, Dict[str, Any]],
    tz=None,
) -> Optional[Dict[str, Any]]:
    """
    Build one record for actual_arrivals_hsp from:
      - a predictions_actual row (TIPLOC2 first/second)
      - HSP rows indexed by TIPLOC2

    Rules:
      - Use SECOND station actual arrival: actual_ta - gbtt_pta
      - Always save TIPLOC2 in DB
      - Add is_main_journey flag:
          * default 0
          * set 1 only when (first, second) is in STATION_PAIRS (PAIR_SET)
    """
    rid = _clean_str(pred_row.get("rid"))
    first = _clean_str(pred_row.get("first"))
    second = _clean_str(pred_row.get("second"))
    planned_dep = _clean_str(pred_row.get("planned_dep"))
    ssd = _clean_str(pred_row.get("ssd"))

    if not rid or not first or not second:
        return None

    hsp_loc = hsp_by_tiploc2.get(second)
    if not hsp_loc:
        return None

    # From your HSP parser (Darwin-like keys):
    # planned arrival is pta (gbtt_pta), actual arrival is ata (actual_ta)
    planned_arr = _hhmm_to_hh_colon_mm(hsp_loc.get("pta"))
    actual_arr = _hhmm_to_hh_colon_mm(hsp_loc.get("ata"))

    # If actual arrival missing, skip (as per your latest requirement)
    if not planned_arr or not actual_arr:
        return None

    actual_arr_delay = compute_actual_arrival_delay_min(
        ssd=ssd,
        planned_arr_hhmm=planned_arr,
        actual_arr_hhmm=actual_arr,
        base_hhmm=planned_dep,
        tz=tz,
    )

    rec: Dict[str, Any] = {
        "rid": rid,
        "ssd": ssd,
        "first": first,      # TIPLOC2
        "second": second,    # TIPLOC2
        "planned_dep": planned_dep,

        # new flag field
        "is_main_journey": int(hsp_loc.get("is_main_journey") or 0),

        "predicted_delay": pred_row.get("predicted_delay"),

        "planned_arr": planned_arr,
        "actual_arr": actual_arr,
        "actual_arr_delay": actual_arr_delay,

        "toc_code": _clean_str(hsp_loc.get("toc_code")),
        "hsp_location_crs": _clean_str(hsp_loc.get("tpl")),  # CRS used to match
        "hsp_tpls": _clean_str(hsp_loc.get("hsp_tpls"))
    }

    return rec


def upsert_actual_arrival(conn: sqlite3.Connection, rec: Dict[str, Any]) -> None:
    """
    UPSERT into actual_arrivals_hsp so re-running daily updates existing rows.
    """
    cols = list(rec.keys())
    placeholders = ",".join(["?"] * len(cols))
    col_list = ",".join(cols)

    # update everything except the unique-key columns
    key_cols = {"rid", "first", "second", "planned_dep"}
    update_cols = [c for c in cols if c not in key_cols]

    set_clause = ", ".join([f"{c}=excluded.{c}" for c in update_cols]) if update_cols else ""

    sql = f"""
    INSERT INTO actual_arrivals_hsp ({col_list})
    VALUES ({placeholders})
    ON CONFLICT(rid, first, second, planned_dep)
    DO UPDATE SET {set_clause}
    """

    vals = [rec[c] for c in cols]
    conn.execute(sql, vals)

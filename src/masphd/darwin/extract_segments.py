# src/masphd/darwin/extract_segments.py
from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

from .station_pairs import STATION_PAIRS
from .time_utils import combine_date_time_smart, diff_minutes_wrap, parse_hms


# -----------------------------
# Small helpers
# -----------------------------
def _first_non_empty(loc: Dict[str, Any], keys: List[str]) -> Optional[str]:
    for k in keys:
        v = loc.get(k)
        if v:
            return v
    return None


def _build_tpl_index(forecasts: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """
    tpl -> location dict
    If duplicates occur, last wins (usually latest update).
    """
    by_tpl: Dict[str, Dict[str, Any]] = {}
    for row in forecasts:
        tpl = row.get("tpl")
        if tpl:
            by_tpl[tpl] = row
    return by_tpl


# -----------------------------
# Planned vs Actual pickers
# -----------------------------
def _pick_planned_arr(loc: Dict[str, Any]) -> Optional[str]:
    return _first_non_empty(loc, ["pta", "wta"])


def _pick_planned_dep(loc: Dict[str, Any]) -> Optional[str]:
    return _first_non_empty(loc, ["ptd", "wtd"])


def _pick_actual_arr(loc: Dict[str, Any]) -> Optional[str]:
    """
    'Actual' in real time means 'best available operational value right now':
    actual arrives may be estimated/working-etc.
    """
    return _first_non_empty(loc, ["arr_at", "arr_et", "arr_wet", "wta", "ata"])


def _pick_confirmed_actual_dep(loc: Dict[str, Any]) -> Optional[str]:
    # confirmed actual departure
    return _first_non_empty(loc, ["atd", "dep_at"])


def _pick_estimated_dep(loc: Dict[str, Any]) -> Optional[str]:
    # estimated departure (Darwin standard is 'etd')
    return _first_non_empty(loc, ["etd", "dep_et"])


def _pick_working_dep(loc: Dict[str, Any]) -> Optional[str]:
    return _first_non_empty(loc, ["wtd"])


def _pick_planned_dep(loc: Dict[str, Any]) -> Optional[str]:
    return _first_non_empty(loc, ["ptd"])


# -----------------------------
# Direction filtering using schedules (best) + fallback
# -----------------------------
def _schedule_endpoints(schedules: List[Dict[str, Any]]) -> Tuple[Optional[str], Optional[str]]:
    """
    Returns (origin_tpl, dest_tpl) from schedule rows when present.
    schedule row example:
      {'tpl': 'CREWE', 'type': 'OR', ...}
      {'tpl': 'EUSTON', 'type': 'DT', ...}
    """
    origin = None
    dest = None
    for r in schedules or []:
        t = r.get("type")
        tpl = r.get("tpl")
        if not tpl or not t:
            continue
        if t == "OR":
            origin = tpl
        elif t == "DT":
            dest = tpl
    return origin, dest


def _rid_matches_station_pairs_direction(schedules: List[Dict[str, Any]]) -> Optional[bool]:
    """
    Uses station_pairs to define direction:
      required_origin = STATION_PAIRS[0][0]
      required_dest   = STATION_PAIRS[-1][1]

    Returns:
      True/False if schedule endpoints are present,
      None if schedule endpoints are missing (unknown).
    """
    if not STATION_PAIRS:
        return None

    required_origin = STATION_PAIRS[0][0]
    required_dest = STATION_PAIRS[-1][1]

    origin, dest = _schedule_endpoints(schedules)
    if origin is None and dest is None:
        return None
    if origin is None or dest is None:
        return None
    return (origin == required_origin) and (dest == required_dest)


def _is_reverse_by_vote(by_tpl: Dict[str, Dict[str, Any]]) -> bool:
    """
    Fallback when schedules are missing:
    Vote using time-of-day only, using whatever is available:
      planned_dep/actual_dep at first station vs planned_arr/actual_arr at second station.

    Strong reverse signal if we see any segment where B's time-of-day is >=10 minutes earlier
    than A's time-of-day (not a midnight crossover).
    """
    forward_votes = 0
    reverse_votes = 0

    for a_code, b_code in STATION_PAIRS:
        a = by_tpl.get(a_code)
        b = by_tpl.get(b_code)
        if not a or not b:
            continue

        dep_s = _first_non_empty(a, ["ptd", "wtd", "dep_et", "dep_at"])
        arr_s = _first_non_empty(b, ["pta", "wta", "arr_et", "arr_wet", "arr_at"])

        dep_t = parse_hms(dep_s)
        arr_t = parse_hms(arr_s)
        if dep_t is None or arr_t is None:
            continue

        dep_min = dep_t.hour * 60 + dep_t.minute + dep_t.second / 60.0
        arr_min = arr_t.hour * 60 + arr_t.minute + arr_t.second / 60.0
        delta = arr_min - dep_min

        # midnight crossover safety: if it's huge negative, treat as next day
        if delta < -720:
            delta += 1440

        if delta < 0:
            reverse_votes += 1
            if delta <= -10:
                return True
        else:
            forward_votes += 1

    if forward_votes + reverse_votes >= 2:
        return reverse_votes > forward_votes

    return False


# -----------------------------
# Main extractor: outputs features for departure_delay & dwell_delay
# -----------------------------
def extract_segments(
    forecasts: List[Dict[str, Any]],
    schedules: Optional[List[Dict[str, Any]]] = None,
    *,
    tz=None,
    drop_wrong_direction: bool = True,
) -> List[Dict[str, Any]]:
    """
    Extract one record per station-pair (A->B) from STATION_PAIRS.

    Key idea:
      - Always extract segments for analysis/prediction.
      - Provide departure time at station A in two ways:
          1) confirmed actual departure if available (atd/dep_at)
          2) otherwise best-available operational departure (dep_et or working/planned)

    Output fields (important):
      - planned_dep: planned departure at A (ptd/wtd)
      - dep_time_for_prediction: best available dep time to use online
      - dep_time_kind: 'actual' | 'estimate' | 'missing'
      - has_actual_dep: True if confirmed actual dep exists (atd/dep_at)
      - actual_dep_confirmed: actual departure string if exists else None
      - departure_delay_min: diff(planned_dep, dep_time_for_prediction) using wrap logic

    Dwell delay logic (your old idea):
      - For first station of the whole route: dwell_delay = departure_delay
      - For middle stations: if we have arrival_delay at A, dwell = dep_delay - arr_delay
      - Otherwise dwell is None
    """
    if not forecasts:
        return []

    rid = forecasts[0].get("rid")
    ssd = forecasts[0].get("ssd")

    by_tpl = _build_tpl_index(forecasts)

    # Direction filtering (schedule endpoints if present, otherwise fallback vote)
    if drop_wrong_direction:
        schedule_match = _rid_matches_station_pairs_direction(schedules or [])
        if schedule_match is False:
            return []
        if schedule_match is None:
            if _is_reverse_by_vote(by_tpl):
                return []

    out: List[Dict[str, Any]] = []
    first_station = STATION_PAIRS[0][0] if STATION_PAIRS else None

    for a_code, b_code in STATION_PAIRS:
        loc_a = by_tpl.get(a_code)
        loc_b = by_tpl.get(b_code)
        if not loc_a or not loc_b:
            continue

        planned_dep = _first_non_empty(loc_a, ["ptd", "wtd"])  # planned for delay baseline

        actual_dep_confirmed = _pick_confirmed_actual_dep(loc_a)
        has_actual_dep = actual_dep_confirmed is not None

        dep_estimate = _pick_estimated_dep(loc_a)
        dep_working = _pick_working_dep(loc_a)

        # Choose best operational departure time
        if has_actual_dep:
            dep_time_for_prediction = actual_dep_confirmed
            dep_time_kind = "actual"
        elif dep_estimate:
            dep_time_for_prediction = dep_estimate
            dep_time_kind = "estimate"
        elif dep_working:
            dep_time_for_prediction = dep_working
            dep_time_kind = "estimate"
        else:
            dep_time_for_prediction = _pick_planned_dep(loc_a)
            dep_time_kind = "estimate" if dep_time_for_prediction else "missing"

        # ---- compute departure delay (planned vs best available) ----
        planned_dep_dt = combine_date_time_smart(ssd, planned_dep, tz=tz) if (ssd and planned_dep) else None
        dep_pred_dt = (
            combine_date_time_smart(ssd, dep_time_for_prediction, base_dt=planned_dep_dt, tz=tz)
            if (ssd and dep_time_for_prediction and planned_dep_dt is not None)
            else None
        )
        departure_delay_min = diff_minutes_wrap(planned_dep_dt, dep_pred_dt)

        # ---- arrival delay at station A (for dwell delay) ----
        planned_arr_a = _pick_planned_arr(loc_a)

        # confirmed actual arrival (rare mid-stream) + estimate arrival
        actual_arr_confirmed = _first_non_empty(loc_a, ["ata", "arr_at"])
        arr_estimate = _first_non_empty(loc_a, ["arr_et", "arr_wet"])

        # choose best available arrival time at A for dwell calculations
        arr_time_for_dwell = actual_arr_confirmed or arr_estimate
        planned_arr_a_dt = (
            combine_date_time_smart(ssd, planned_arr_a, base_dt=planned_dep_dt, tz=tz)
            if (ssd and planned_arr_a and planned_dep_dt is not None)
            else None
        )
        arr_dwell_dt = (
            combine_date_time_smart(ssd, arr_time_for_dwell, base_dt=planned_dep_dt, tz=tz)
            if (ssd and arr_time_for_dwell and planned_dep_dt is not None)
            else None
        )
        arrival_delay_min = diff_minutes_wrap(planned_arr_a_dt, arr_dwell_dt)

        # ---- dwell delay ----
        if a_code == first_station:
            dwell_delay_min = departure_delay_min
        else:
            if departure_delay_min is not None and arrival_delay_min is not None:
                dwell_delay_min = departure_delay_min - arrival_delay_min
            else:
                dwell_delay_min = None

        out.append(
            {
                "rid": rid,
                "ssd": ssd,
                "first": a_code,
                "second": b_code,

                # planned at A
                "planned_dep": planned_dep,
                "planned_arr": planned_arr_a,

                # departure at A (for prediction + storage decisions)
                "dep_time_for_prediction": dep_time_for_prediction,
                "dep_time_kind": dep_time_kind,               # 'actual' or 'estimate' or 'missing'
                "has_actual_dep": has_actual_dep,
                "actual_dep_confirmed": actual_dep_confirmed, # None unless confirmed

                # computed features
                "departure_delay_min": departure_delay_min,
                "arrival_delay_min": arrival_delay_min,  # only for dwell
                "dwell_delay_min": dwell_delay_min,

                # raw rows for notebook debugging
                "loc_first": loc_a,
                "loc_second": loc_b,
            }
        )

    return out

# src/masphd/darwin/realtime_filter.py
from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from .time_utils import combine_date_time_smart


def _to_aware(dt: datetime, tz) -> datetime:
    """
    Ensure dt is timezone-aware with tz if tz is given.
    """
    if tz is None:
        return dt
    if dt.tzinfo is None:
        return dt.replace(tzinfo=tz)
    return dt


def _planned_dep_dt(seg: Dict[str, Any], tz=None) -> Optional[datetime]:
    ssd = seg.get("ssd")
    t = seg.get("planned_dep")
    if not ssd or not t:
        return None
    return combine_date_time_smart(ssd, t, tz=tz)


def _planned_arr_dt(seg: Dict[str, Any], tz=None) -> Optional[datetime]:
    """
    Planned arrival is at the *destination station of the segment*.
    In our extracted record we stored planned_arr at the first station (for dwell),
    so for in-progress filtering we need the planned arrival at the second station.

    Therefore, filter functions expect the caller to have stored:
      seg["planned_arr_second"]
    If it's not present, we return None.
    """
    ssd = seg.get("ssd")
    t = seg.get("planned_arr_second")
    base = seg.get("_planned_dep_dt_for_filter")
    if not ssd or not t:
        return None
    return combine_date_time_smart(ssd, t, base_dt=base, tz=tz)


def filter_segments_by_now(
    segments: List[Dict[str, Any]],
    *,
    now: datetime,
    tz=None,
    mode: str = "near_departure",
    # near_departure parameters:
    before_mins: int = 30,
    after_mins: int = 180,
    # in_progress parameters:
    dep_grace_after_now_mins: int = 5,
    arr_grace_before_now_mins: int = 2,
) -> List[Dict[str, Any]]:
    """
    Filter segment dicts relative to `now`.

    Modes:

    1) mode="near_departure"
       Keep segments where planned departure is within:
         [now - before_mins, now + after_mins]

       This is good for debugging / seeing upcoming segments.

    2) mode="in_progress"  (recommended for live prediction)
       Keep segments where:
         planned_dep <= now + dep_grace_after_now_mins
         and
         planned_arr >= now - arr_grace_before_now_mins

       This corresponds to "segment has started (or about to start) and not finished yet".

    Important:
      - This filter uses PLANNED times only (by design).
      - For "in_progress", we need planned arrival at the destination station of the segment.
        That must be stored as seg["planned_arr_second"] by the extractor/notebook.
    """
    now = _to_aware(now, tz)

    if mode not in ("near_departure", "in_progress"):
        raise ValueError("mode must be 'near_departure' or 'in_progress'")

    out: List[Dict[str, Any]] = []

    if mode == "near_departure":
        win_start = now - timedelta(minutes=before_mins)
        win_end = now + timedelta(minutes=after_mins)

        for seg in segments:
            dep_dt = _planned_dep_dt(seg, tz=tz)
            if dep_dt is None:
                continue
            dep_dt = _to_aware(dep_dt, tz)

            if win_start <= dep_dt <= win_end:
                out.append(seg)

        return out

    # mode == "in_progress"
    dep_limit = now + timedelta(minutes=dep_grace_after_now_mins)
    arr_limit = now - timedelta(minutes=arr_grace_before_now_mins)

    for seg in segments:
        dep_dt = _planned_dep_dt(seg, tz=tz)
        if dep_dt is None:
            continue
        dep_dt = _to_aware(dep_dt, tz)

        # Save base for arrival rollover handling
        seg["_planned_dep_dt_for_filter"] = dep_dt

        arr_dt = _planned_arr_dt(seg, tz=tz)
        if arr_dt is None:
            # If we don't know planned arrival at destination, we cannot confidently say "in progress"
            # so skip it.
            seg.pop("_planned_dep_dt_for_filter", None)
            continue
        arr_dt = _to_aware(arr_dt, tz)

        seg.pop("_planned_dep_dt_for_filter", None)

        # in-progress condition with grace
        if dep_dt <= dep_limit and arr_dt >= arr_limit:
            out.append(seg)

    return out

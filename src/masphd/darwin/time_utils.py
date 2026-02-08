# src/masphd/darwin/time_utils.py
from __future__ import annotations

from datetime import datetime, time, timedelta
from typing import Optional


def parse_hms(value: Optional[str]) -> Optional[time]:
    """
    Parse Darwin time strings like:
      - "09:43"
      - "09:47:30"
    Returns datetime.time or None.
    """
    if not value:
        return None

    s = str(value).strip()
    if not s:
        return None

    parts = s.split(":")
    try:
        if len(parts) == 2:
            hh, mm = parts
            return time(hour=int(hh), minute=int(mm), second=0)
        if len(parts) == 3:
            hh, mm, ss = parts
            return time(hour=int(hh), minute=int(mm), second=int(ss))
    except ValueError:
        return None

    return None


def combine_date_time(ssd: str, t: Optional[str], tz=None) -> Optional[datetime]:
    """
    Combine service start date (YYYY-MM-DD) and a Darwin time string into datetime.
    If tz is provided, returns timezone-aware datetime in that timezone.
    """
    tt = parse_hms(t)
    if tt is None:
        return None

    try:
        d = datetime.strptime(ssd, "%Y-%m-%d").date()
    except ValueError:
        return None

    dt = datetime.combine(d, tt)
    if tz is not None:
        dt = dt.replace(tzinfo=tz)
    return dt


def combine_date_time_smart(
    ssd: str,
    t: Optional[str],
    *,
    base_dt: Optional[datetime] = None,
    tz=None,
    rollover_threshold_hours: float = 2.0,
) -> Optional[datetime]:
    """
    Combine date + time, and if base_dt is given, roll to the next day only when
    it looks like a genuine midnight crossover.

    rollover_threshold_hours=2 means:
      - if dt is earlier than base_dt by more than 2 hours, assume next day.
    """
    dt = combine_date_time(ssd, t, tz=tz)
    if dt is None:
        return None

    if base_dt is None:
        return dt

    # Ensure awareness matches for comparison
    if (base_dt.tzinfo is not None) and (dt.tzinfo is None):
        dt = dt.replace(tzinfo=base_dt.tzinfo)
    if (base_dt.tzinfo is None) and (dt.tzinfo is not None):
        dt = dt.replace(tzinfo=None)

    if dt < base_dt:
        gap = base_dt - dt
        if gap > timedelta(hours=rollover_threshold_hours):
            dt = dt + timedelta(days=1)

    return dt


def diff_minutes_smart(start: Optional[datetime], end: Optional[datetime]) -> Optional[float]:
    """
    Returns (end - start) in minutes.
    If end < start, assumes end is next day (adds 1 day).
    """
    if start is None or end is None:
        return None

    # Ensure awareness matches
    if (start.tzinfo is not None) and (end.tzinfo is None):
        end = end.replace(tzinfo=start.tzinfo)
    if (start.tzinfo is None) and (end.tzinfo is not None):
        end = end.replace(tzinfo=None)

    if end < start:
        end = end + timedelta(days=1)

    return (end - start).total_seconds() / 60.0


def diff_minutes_wrap(planned: Optional[datetime], actual: Optional[datetime]) -> Optional[float]:
    """
    Your old safe wrap logic, best for delay minutes:
      minutes = actual - planned
      if minutes > 1200: minutes -= 1440
      if minutes < -1200: minutes += 1440

    This avoids crazy '1430 minutes' artefacts unless it really crosses midnight.
    """
    if planned is None or actual is None:
        return None

    # Ensure awareness matches
    if (planned.tzinfo is not None) and (actual.tzinfo is None):
        actual = actual.replace(tzinfo=planned.tzinfo)
    if (planned.tzinfo is None) and (actual.tzinfo is not None):
        actual = actual.replace(tzinfo=None)

    minutes = (actual - planned).total_seconds() / 60.0

    if minutes > 1200:
        minutes -= 1440
    if minutes < -1200:
        minutes += 1440

    return minutes


def format_mmss(minutes_float: Optional[float]) -> str:
    """
    Format float minutes as MM:SS (supports negative). "NA" if None.
    """
    if minutes_float is None:
        return "NA"

    total_seconds = int(round(minutes_float * 60))
    sign = "-" if total_seconds < 0 else ""
    total_seconds = abs(total_seconds)
    mm = total_seconds // 60
    ss = total_seconds % 60
    return f"{sign}{mm:02d}:{ss:02d}"

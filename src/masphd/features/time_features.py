# src/masphd/features/time_features.py
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Optional, Union

# Optional dependency (recommended):
#   pip install holidays
try:
    import holidays as _holidays
except Exception:
    _holidays = None


Y = 2000  # dummy leap year for season mapping
SEASONS = [
    ("Winter", (date(Y, 1, 1), date(Y, 3, 20))),
    ("Spring", (date(Y, 3, 21), date(Y, 6, 20))),
    ("Summer", (date(Y, 6, 21), date(Y, 9, 22))),
    ("Autumn", (date(Y, 9, 23), date(Y, 12, 20))),
    ("Winter", (date(Y, 12, 21), date(Y, 12, 31))),
]


def _get_season(d: date) -> str:
    d2 = d.replace(year=Y)
    for season, (start, end) in SEASONS:
        if start <= d2 <= end:
            return season
    return "Winter"


@dataclass(frozen=True)
class TimeFeatures:
    peak: int
    day_of_week: str
    day_of_month: int
    hour_of_day: int
    weekend: int
    season: str
    month: int
    holiday: int


class TimeFeatureExtractor:
    """
    Extracts calendar/time features from a datetime.

    Assumptions aligned with your old code:
      - weekend: Saturday/Sunday
      - peak: weekday and (7-9) or (16-19) inclusive-ish, matching your rules
      - holiday: UK holiday flag (uses `holidays` library if installed)
    """

    def __init__(self, *, holiday_region: str = "GB", holiday_subregion: Optional[str] = None):
        self._holiday_region = holiday_region
        self._holiday_subregion = holiday_subregion

        if _holidays is not None:
            try:
                # For UK: holidays.country_holidays("GB", subdiv="ENG") etc.
                self._holiday_calendar = _holidays.country_holidays(
                    holiday_region,
                    subdiv=holiday_subregion,
                )
            except Exception:
                self._holiday_calendar = None
        else:
            self._holiday_calendar = None

    def is_holiday(self, d: date) -> int:
        if self._holiday_calendar is None:
            return 0
        return 1 if d in self._holiday_calendar else 0

    @staticmethod
    def weekend_flag(day_of_week: str) -> int:
        return 1 if day_of_week in ("Saturday", "Sunday") else 0

    @staticmethod
    def peak_flag(hour_of_day: int, weekend: int) -> int:
        if weekend == 1:
            return 0
        # Match your old logic
        if 6 < hour_of_day < 10:
            return 1
        if 16 <= hour_of_day <= 19:
            return 1
        return 0

    def extract(self, dt: Union[datetime, date]) -> TimeFeatures:
        if isinstance(dt, datetime):
            d = dt.date()
            hour = int(dt.hour)
        else:
            d = dt
            hour = 0

        day_of_week = d.strftime("%A")
        weekend = self.weekend_flag(day_of_week)
        peak = self.peak_flag(hour, weekend)
        season = _get_season(d)
        holiday = self.is_holiday(d)

        return TimeFeatures(
            peak=peak,
            day_of_week=day_of_week,
            day_of_month=int(d.day),
            hour_of_day=int(hour),
            weekend=weekend,
            season=season,
            month=int(d.month),
            holiday=int(holiday),
        )

# src/masphd/features/segment_features.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional

from masphd.darwin.time_utils import combine_date_time_smart
from .time_features import TimeFeatureExtractor


FEATURE_ORDER = [
    "departure_delay",
    "dwell_delay",
    "peak",
    "day_of_week",
    "day_of_month",
    "hour_of_day",
    "weekend",
    "season",
    "month",
    "holiday",
]


@dataclass
class SegmentFeatures:
    # numeric
    departure_delay: float
    dwell_delay: float
    peak: int
    day_of_month: int
    hour_of_day: int
    weekend: int
    month: int
    holiday: int
    # categorical (keep as string for now; your pipeline may encode later)
    day_of_week: str
    season: str

    def as_dict(self) -> Dict[str, Any]:
        return {
            "departure_delay": self.departure_delay,
            "dwell_delay": self.dwell_delay,
            "peak": self.peak,
            "day_of_week": self.day_of_week,
            "day_of_month": self.day_of_month,
            "hour_of_day": self.hour_of_day,
            "weekend": self.weekend,
            "season": self.season,
            "month": self.month,
            "holiday": self.holiday,
        }


class SegmentFeatureBuilder:
    """
    Build feature dict from one extracted segment.

    Notes:
      - If dwell_delay is None, we set it to 0.0 by default (common in early updates).
        If you prefer to skip those segments, you can do that in the notebook.
      - Calendar features are taken from (ssd + planned_dep) because it is stable and available.
    """

    def __init__(self, time_extractor: Optional[TimeFeatureExtractor] = None, *, tz=None):
        self._time_extractor = time_extractor or TimeFeatureExtractor(holiday_region="GB", holiday_subregion="ENG")
        self._tz = tz

    def build(self, segment: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        ssd = segment.get("ssd")
        planned_dep = segment.get("planned_dep")

        # We need a timestamp anchor for time features
        if not ssd or not planned_dep:
            return None

        dep_anchor_dt = combine_date_time_smart(ssd, planned_dep, tz=self._tz)
        tf = self._time_extractor.extract(dep_anchor_dt)

        dep_delay = segment.get("departure_delay_min")
        dwell_delay = segment.get("dwell_delay_min")

        # departure_delay is essential for real-time prediction
        if dep_delay is None:
            return None

        # dwell_delay can be missing early; choose a safe default
        if dwell_delay is None:
            dwell_delay = 0.0

        feat = SegmentFeatures(
            departure_delay=float(dep_delay),
            dwell_delay=float(dwell_delay),
            peak=int(tf.peak),
            day_of_week=str(tf.day_of_week),
            day_of_month=int(tf.day_of_month),
            hour_of_day=int(tf.hour_of_day),
            weekend=int(tf.weekend),
            season=str(tf.season),
            month=int(tf.month),
            holiday=int(tf.holiday),
        )

        return feat.as_dict()

    @staticmethod
    def order_features(feat_dict: Dict[str, Any]) -> Dict[str, Any]:
        """
        Returns a dict in FEATURE_ORDER. Useful for consistent debug/printing.
        """
        return {k: feat_dict.get(k) for k in FEATURE_ORDER}

# src/masphd/hsp/parser.py
from __future__ import annotations

from typing import Any, Dict, List, Optional, Set

from masphd.darwin.station_pairs import CRS_TO_TIPLOC2, CRSS


def _clean_str(v: Any) -> Optional[str]:
    if v is None:
        return None
    if isinstance(v, str):
        s = v.strip()
        return s or None
    s = str(v).strip()
    return s or None


def _pick_time_hhmm(v: Any) -> Optional[str]:
    """
    HSP times are typically 'HHMM' strings.
    Keep them as-is (string), but normalise empty/None.
    """
    return _clean_str(v)


def _route_crs_set() -> Set[str]:
    """
    CRSS is a list like [Optional[str], ...] for the route in station_pairs.py.
    Build a set of non-empty CRS strings.
    """
    return {c for c in (CRSS or []) if isinstance(c, str) and c.strip()}


def extract_service_locations(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Convert raw HSP JSON into a Darwin-like flat list: one dict per location.

    Also sets service-level fields:
      - is_main_journey = 1 if HSP locations include *all* CRS stations in CRSS, else 0
      - hsp_tpls = comma-separated CRS list seen in this service (sorted, unique)

    Input example:
    {
      "serviceAttributesDetails": {
        "date_of_service": "2026-02-03",
        "toc_code": "SW",
        "rid": "202602037672804",
        "locations": [
          {"location":"POO","gbtt_ptd":"0650","gbtt_pta":"","actual_td":"0649","actual_ta":"","late_canc_reason":""},
          ...
        ]
      }
    }
    """
    sad = payload.get("serviceAttributesDetails")
    if not isinstance(sad, dict):
        return []

    rid = _clean_str(sad.get("rid"))
    if not rid:
        return []

    ssd = _clean_str(sad.get("date_of_service"))
    toc_code = _clean_str(sad.get("toc_code"))

    locs = sad.get("locations") or []
    if not isinstance(locs, list):
        return []

    # Determine service-level "main journey" based on CRS coverage
    route_crs = _route_crs_set()
    seen_crs: Set[str] = set()

    for loc in locs:
        if not isinstance(loc, dict):
            continue
        crs = _clean_str(loc.get("location"))
        if crs:
            seen_crs.add(crs)

    is_main_journey = 1 if (route_crs and route_crs.issubset(seen_crs)) else 0

    # Comma-separated sequence of all CRS locations in this service (sorted, unique)
    # (Keep stable ordering for easy diff/debugging.)
    hsp_tpls = ",".join(sorted(seen_crs))

    base: Dict[str, Any] = {
        "rid": rid,
        "ssd": ssd,
        "toc_code": toc_code,
        "is_main_journey": is_main_journey,
        "hsp_tpls": hsp_tpls,
    }

    out: List[Dict[str, Any]] = []
    for loc in locs:
        if not isinstance(loc, dict):
            continue

        tpl = _clean_str(loc.get("location"))  # CRS
        if not tpl:
            continue

        item: Dict[str, Any] = dict(base)

        # Darwin-like keys (note: tpl is CRS here by your confirmation)
        item["tpl"] = tpl
        item["pta"] = _pick_time_hhmm(loc.get("gbtt_pta"))
        item["ptd"] = _pick_time_hhmm(loc.get("gbtt_ptd"))
        item["ata"] = _pick_time_hhmm(loc.get("actual_ta"))
        item["atd"] = _pick_time_hhmm(loc.get("actual_td"))
        item["late_canc_reason"] = _clean_str(loc.get("late_canc_reason"))

        # Helpful extra: route TIPLOC2 if this CRS is on your tracked route
        t2 = CRS_TO_TIPLOC2.get(tpl)
        if t2:
            item["tiploc2"] = t2

        # Keep raw fields (helps debugging and future endpoint changes)
        item["hsp_location"] = tpl
        item["hsp_gbtt_pta"] = _pick_time_hhmm(loc.get("gbtt_pta"))
        item["hsp_gbtt_ptd"] = _pick_time_hhmm(loc.get("gbtt_ptd"))
        item["hsp_actual_ta"] = _pick_time_hhmm(loc.get("actual_ta"))
        item["hsp_actual_td"] = _pick_time_hhmm(loc.get("actual_td"))

        out.append(item)

    return out

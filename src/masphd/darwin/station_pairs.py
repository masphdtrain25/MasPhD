# src/masphd/darwin/station_pairs.py
"""
Ordered station-to-station segments for a single journey direction.

Important:
- These codes are treated as TIPLOC-like values (e.g., 'WEYMTH', 'CLPHMJM').
- Reverse pairs are intentionally NOT included.
"""

from __future__ import annotations

from functools import lru_cache
from typing import Final, Iterable, List, Set, Tuple, Dict, Optional

from masphd.utils.station_lookup import (
    get_crs_by_tiploc2,
    get_name_by_tiploc2,
    get_tiploc_by_tiploc2,
)

# -----------------------------
# Ordered pairs (do not reverse)
# -----------------------------
STATION_PAIRS: Final[List[Tuple[str, str]]] = [
    ("WEYMTH", "UPWEY"),
    ("UPWEY", "DRCHS"),
    ("DRCHS", "WOOL"),
    ("WOOL", "WARHAM"),
    ("WARHAM", "HMWTHY"),
    ("HMWTHY", "POOLE"),
    ("POOLE", "PSTONE"),
    ("PSTONE", "BRANKSM"),
    ("BRANKSM", "BOMO"),
    ("BOMO", "POKSDWN"),
    ("POKSDWN", "CHRISTC"),
    ("CHRISTC", "NMILTON"),
    ("NMILTON", "BKNHRST"),
    ("BKNHRST", "SOTON"),
    ("SOTON", "SOTPKWY"),
    ("SOTPKWY", "WNCHSTR"),
    ("WNCHSTR", "BSNGSTK"),
    ("BSNGSTK", "CLPHMJM"),
    ("CLPHMJM", "WATRLMN"),
]

# Fast lookups
PAIR_SET: Final[Set[Tuple[str, str]]] = set(STATION_PAIRS)
ORIGIN_SET: Final[Set[str]] = {a for a, _ in STATION_PAIRS}
DEST_SET: Final[Set[str]] = {b for _, b in STATION_PAIRS}

# Convenience helpers
def is_tracked_pair(a: str, b: str) -> bool:
    """True if (a,b) is in the ordered tracked station pairs."""
    return (a, b) in PAIR_SET

def iter_pairs() -> Iterable[Tuple[str, str]]:
    """Iterate through station pairs in the correct journey order."""
    return iter(STATION_PAIRS)

# -----------------------------
# Route stations in TIPLOC2 order
# -----------------------------

def iter_route_tiploc2s() -> Iterable[str]:
    """
    Unique stations in journey order, derived from STATION_PAIRS.

    NOTE:
    STATION_PAIRS values are TIPLOC2 codes in your DB/mapping (e.g., WATRLMN),
    NOT the TIPLOC column (e.g., WATRLOO).
    """
    if not STATION_PAIRS:
        return iter(())

    stations: List[str] = [STATION_PAIRS[0][0]]
    for _, b in STATION_PAIRS:
        stations.append(b)
    return iter(stations)


TIPLOC2_ROUTE: Final[List[str]] = list(iter_route_tiploc2s())


# -----------------------------
# Cached lookups
# -----------------------------
@lru_cache(maxsize=1)
def route_crss() -> List[Optional[str]]:
    """TIPLOC2 -> CRS for the route (cached)."""
    return [get_crs_by_tiploc2(t2) for t2 in TIPLOC2_ROUTE]


@lru_cache(maxsize=1)
def route_names() -> List[Optional[str]]:
    """TIPLOC2 -> NAME for the route (cached)."""
    return [get_name_by_tiploc2(t2) for t2 in TIPLOC2_ROUTE]


@lru_cache(maxsize=1)
def route_tiplocs() -> List[Optional[str]]:
    """TIPLOC2 -> TIPLOC (your CSV TIPLOC column) for the route (cached)."""
    return [get_tiploc_by_tiploc2(t2) for t2 in TIPLOC2_ROUTE]


@lru_cache(maxsize=1)
def route_maps() -> tuple[
    Dict[str, Optional[str]],  # tiploc2 -> crs
    Dict[str, Optional[str]],  # tiploc2 -> name
    Dict[str, Optional[str]],  # tiploc2 -> tiploc
    Dict[str, str],            # crs -> tiploc2 (route-canonical)
]:
    """
    Returns cached dicts (route-scoped):

      - tiploc2 -> crs
      - tiploc2 -> name
      - tiploc2 -> tiploc
      - crs -> tiploc2 (ONE canonical mapping per CRS on this route)

    If a CRS appears multiple times on this route, the FIRST occurrence
    in journey order is kept.
    """
    crss = route_crss()
    names = route_names()
    tiplocs = route_tiplocs()

    tiploc2_to_crs: Dict[str, Optional[str]] = {}
    tiploc2_to_name: Dict[str, Optional[str]] = {}
    tiploc2_to_tiploc: Dict[str, Optional[str]] = {}
    crs_to_tiploc2: Dict[str, str] = {}

    for t2, crs, name, tp in zip(TIPLOC2_ROUTE, crss, names, tiplocs):
        tiploc2_to_crs[t2] = crs
        tiploc2_to_name[t2] = name
        tiploc2_to_tiploc[t2] = tp

        if crs and crs not in crs_to_tiploc2:
            crs_to_tiploc2[crs] = t2

    return tiploc2_to_crs, tiploc2_to_name, tiploc2_to_tiploc, crs_to_tiploc2


# -----------------------------
# Simple cached variables
# -----------------------------
CRSS: Final[List[Optional[str]]] = route_crss()
NAMES: Final[List[Optional[str]]] = route_names()
TIPLOCS: Final[List[Optional[str]]] = route_tiplocs()

TIPLOC2_TO_CRS, TIPLOC2_TO_NAME, TIPLOC2_TO_TIPLOC, CRS_TO_TIPLOC2 = route_maps()

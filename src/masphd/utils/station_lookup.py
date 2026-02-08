from __future__ import annotations

from functools import lru_cache
from typing import Optional

import pandas as pd

from masphd.io.paths import TIPLOC_MAP

# Your canonical columns (as in the CSV)
_COLS = ("NAME", "TIPLOC", "TIPLOC2", "CRS")


@lru_cache(maxsize=1)
def _load_station_df() -> pd.DataFrame:
    """
    Load station lookup CSV once and cache it.

    Expected columns:
        NAME, TIPLOC, TIPLOC2, CRS
    """
    df = pd.read_csv(TIPLOC_MAP)

    # Ensure required columns exist
    missing = [c for c in _COLS if c not in df.columns]
    if missing:
        raise ValueError(f"TIPLOC_MAP is missing columns: {missing}. Found: {list(df.columns)}")

    # Normalise for safe matching (codes uppercase, name kept as string)
    df["TIPLOC"] = df["TIPLOC"].astype(str).str.strip().str.upper()
    df["TIPLOC2"] = df["TIPLOC2"].astype(str).str.strip().str.upper()
    df["CRS"] = df["CRS"].astype(str).str.strip().str.upper()
    df["NAME"] = df["NAME"].astype(str).str.strip()

    return df


def _normalise_value(column: str, value: str) -> str:
    v = str(value).strip()
    if column in ("TIPLOC", "TIPLOC2", "CRS"):
        v = v.upper()
    return v


def get_value(by: str, value: str, target: str) -> Optional[str]:
    """
    Generic lookup:
        target_value = get_value(by="TIPLOC", value="SOTON", target="TIPLOC2")

    Returns None if not found.
    If multiple rows match (shouldn't happen), returns the first.
    """
    if not value:
        return None

    by = by.strip().upper()
    target = target.strip().upper()

    if by not in _COLS:
        raise ValueError(f"Invalid 'by' column: {by}. Allowed: {_COLS}")
    if target not in _COLS:
        raise ValueError(f"Invalid 'target' column: {target}. Allowed: {_COLS}")

    df = _load_station_df()
    key = _normalise_value(by, value)

    row = df.loc[df[by] == key]
    if row.empty:
        return None

    out = row.iloc[0][target]
    if pd.isna(out):
        return None
    return str(out).strip()


# ---------------------------
# Convenience functions
# (all combinations)
# ---------------------------

# By TIPLOC
def get_name_by_tiploc(tiploc: str) -> Optional[str]:
    return get_value("TIPLOC", tiploc, "NAME")

def get_tiploc2_by_tiploc(tiploc: str) -> Optional[str]:
    return get_value("TIPLOC", tiploc, "TIPLOC2")

def get_crs_by_tiploc(tiploc: str) -> Optional[str]:
    return get_value("TIPLOC", tiploc, "CRS")


# By TIPLOC2
def get_name_by_tiploc2(tiploc2: str) -> Optional[str]:
    return get_value("TIPLOC2", tiploc2, "NAME")

def get_tiploc_by_tiploc2(tiploc2: str) -> Optional[str]:
    return get_value("TIPLOC2", tiploc2, "TIPLOC")

def get_crs_by_tiploc2(tiploc2: str) -> Optional[str]:
    return get_value("TIPLOC2", tiploc2, "CRS")


# By CRS
def get_name_by_crs(crs: str) -> Optional[str]:
    return get_value("CRS", crs, "NAME")

def get_tiploc_by_crs(crs: str) -> Optional[str]:
    return get_value("CRS", crs, "TIPLOC")

def get_tiploc2_by_crs(crs: str) -> Optional[str]:
    return get_value("CRS", crs, "TIPLOC2")


# By NAME (note: names may not be unique)
def get_tiploc_by_name(name: str) -> Optional[str]:
    return get_value("NAME", name, "TIPLOC")

def get_tiploc2_by_name(name: str) -> Optional[str]:
    return get_value("NAME", name, "TIPLOC2")

def get_crs_by_name(name: str) -> Optional[str]:
    return get_value("NAME", name, "CRS")

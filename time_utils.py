# time_utils.py
"""
Utilities for season formatting and Hockey-Reference page normalization.

- Season helpers:
    yyyy_to_season(2014) -> "13-14"
    season_from_filename(Path("..._2014.csv")) -> "13-14"

- DataFrame cleaners:
    normalize_columns(df)                 # headers, player text, gp numeric, cf_rel float
    compute_total_toi(df, toi_col="toi")  # mm:ss (avg) or mmmm:ss / HH:MM:SS (total) -> season totals
    attach_season(df, season="24-25")     # or from_filename=Path("...2014.csv")

Author: Eric Winiecke
September 26, 2025
"""

from __future__ import annotations

import re
from pathlib import Path

import pandas as pd

# -----------------------------------------------------------
# __all__ method is a list of strings in a module or.       =
# package that declares its public API...what gets imported.=
# -----------------------------------------------------------


__all__ = [
    "yyyy_to_season",
    "season_from_filename",
    "normalize_columns",
    "compute_total_toi",
    "attach_season",
    "seconds_to_hms",
]

# ---------------------------
# Season / filename helpers
# ---------------------------


def yyyy_to_season(yyyy: int | str) -> str:
    """
    Map a 4-digit year (season end year) to 'YY-YY'.
    Example: 2014 -> '13-14'
    """
    y = int(str(yyyy)[:4])
    return f"{(y - 1) % 100:02d}-{y % 100:02d}"


def season_from_filename(path: str | Path) -> str | None:
    """
    Extract a 4-digit year from filename and map to 'YY-YY'.
    Returns None if no 4-digit year is present.
    """
    p = Path(path)
    m = re.search(r"(\d{4})", p.stem)
    return yyyy_to_season(int(m.group(1))) if m else None


# ---------------------------
# Column normalization
# ---------------------------


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    - Lowercase/snake headers
    - Ensure 'player' text is normalized
    - Coerce 'gp' numeric
    - Normalize 'CF% Rel' variants -> 'cf_rel' (float, % stripped)
    """
    df = df.rename(
        columns={c: re.sub(r"[^A-Za-z0-9]+", "_", c).strip("_").lower() for c in df.columns}
    )

    if "player" in df.columns:
        df["player"] = df["player"].astype(str).str.replace(r"\s+", " ", regex=True).str.strip()

    if "gp" in df.columns:
        df["gp"] = pd.to_numeric(df["gp"], errors="coerce")

    # Map CF% Rel variations
    for cand in ("cf_rel", "cf__rel", "cf%_rel", "corsi_for_pct_rel", "cf_pct_rel"):
        if cand in df.columns:
            s = (
                df[cand]
                .astype(str)
                .str.replace("%", "", regex=False)
                .str.replace(r"[^\d\.\-\+]", "", regex=True)
            )
            df["cf_rel"] = pd.to_numeric(s, errors="coerce")
            break

    return df


# ---------------------------
# TOI parsing & totals
# ---------------------------

_mmss_re = re.compile(r"^\s*(\d{1,2}):([0-5]\d)\s*$")  # avg per game, 0–59 min
_mmmmss_re = re.compile(r"^\s*(\d{1,6}):([0-5]\d)\s*$")  # season total minutes (many digits)
_hhmmss_re = re.compile(r"^\s*(\d{1,3}):([0-5]\d):([0-5]\d)\s*$")  # rare HH:MM:SS


def _to_seconds_mmss(s: str) -> int | None:
    m = _mmss_re.match(s or "")
    if not m:
        return None
    mnt, sec = map(int, m.groups())
    return mnt * 60 + sec


def _to_seconds_totalish(s: str) -> int | None:
    s = (s or "").strip()
    m3 = _hhmmss_re.match(s)
    if m3:
        h, mnt, sec = map(int, m3.groups())
        return h * 3600 + mnt * 60 + sec
    m = _mmmmss_re.match(s)
    if m:
        mnt, sec = map(int, m.groups())
        return mnt * 60 + sec
    return None


def seconds_to_hms(x: int | None) -> str:
    if x is None:
        return ""
    h, r = divmod(int(x), 3600)
    m, s = divmod(r, 60)
    return f"{h}:{m:02d}:{s:02d}"


def compute_total_toi(df: pd.DataFrame, *, toi_col: str = "toi") -> pd.DataFrame:
    """
    Add season-total TOI columns regardless of source format.

    Inputs:
      - df[toi_col] is either:
          * average per game 'mm:ss'
          * season total 'mmmm:ss' OR 'HH:MM:SS'
      - df['gp'] optional (used when mm:ss avg is detected)

    Outputs:
      - 'toi_seconds_total' : Int64 seconds
      - 'toi_total_hms'     : 'H:MM:SS'
      - (if average input) 'toi_seconds_avg' and 'toi_avg_mmss'
    """
    if toi_col not in df.columns:
        raise ValueError(f"Missing '{toi_col}' column")

    out = df.copy()
    s = out[toi_col].astype(str).str.strip()

    mmss_sec = s.map(_to_seconds_mmss)
    tot_sec = s.map(_to_seconds_totalish)

    has_gp = "gp" in out.columns
    all_mmss = mmss_sec.notna().all()
    all_totish = tot_sec.notna().all()

    if all_mmss and has_gp:
        out["toi_seconds_avg"] = mmss_sec.astype("Int64")
        out["toi_avg_mmss"] = s
        out["gp"] = pd.to_numeric(out["gp"], errors="coerce").fillna(0).astype("Int64")
        out["toi_seconds_total"] = (
            out["gp"].astype("int64") * out["toi_seconds_avg"].fillna(0).astype("int64")
        ).astype("Int64")
    elif all_totish:
        out["toi_seconds_total"] = tot_sec.astype("Int64")
    else:
        # mixed / messy: prefer totals; fallback to gp * avg when gp present
        tentative = tot_sec
        if has_gp:
            gp = pd.to_numeric(out["gp"], errors="coerce").fillna(0).astype("int64")
            avg = mmss_sec.fillna(0).astype("int64")
            fallback = gp * avg
            out["toi_seconds_total"] = tentative.fillna(fallback).astype("Int64")
        else:
            out["toi_seconds_total"] = tentative.astype("Int64")

    out["toi_total_hms"] = out["toi_seconds_total"].map(
        lambda x: seconds_to_hms(int(x)) if pd.notna(x) else ""
    )
    return out


# ---------------------------
# Season attachment
# ---------------------------


def attach_season(
    df: pd.DataFrame,
    *,
    season: str | None = None,
    from_filename: str | Path | None = None,
) -> pd.DataFrame:
    """
    Add/overwrite a 'season' column using either:
      - explicit season='YY-YY'
      - from_filename=Path(...YYYY...) -> 'YY-YY'
    Does nothing if neither is provided or filename lacks a 4-digit year.
    """
    out = df.copy()
    if season:
        out["season"] = season
        return out
    if from_filename:
        s = season_from_filename(from_filename)
        if s:
            out["season"] = s
    return out

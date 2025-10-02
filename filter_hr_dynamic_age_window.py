# #!/usr/bin/env python3
"""
Filter Hockey-Reference final set with a dynamic 25–29 age window and a 5-season qualification rule.

Pipeline
1) Read input CSV (expects REQUIRED_COLS).
2) Parse 'season' -> 'season_end' (handles '13-14', '2013-14', '2014', etc.).
3) Dynamic age bounds by season_end (expand→hold→shrink; clamped to [25,29]).
4) Keep rows within that season’s [min_age, max_age].
5) For each player, find a 5-consecutive-season window where:
     - each season has EV TOI >= PER_SEASON_MIN,
     - 5-season average EV TOI >= FIVE_YEAR_AVG,
     - if multiple windows exist, prefer the one whose rounded ages are exactly {25..29};
       otherwise pick the window closest to 25..29 (min sum of |age−target|), breaking ties
       by larger 5-year avg minutes and then by earliest start.
6) Keep only rows in the chosen window; add rel_age (≈ round(age-27)), sort, write CSV.

Usage:
  python filter_hr_dynamic_age_window.py \
      --input  data/outputs/hockeyref_final.csv \
      --output data/outputs/hr_filtered_sorted_relage.csv
"""

from __future__ import annotations

import argparse
import logging
import re
from pathlib import Path

import numpy as np
import pandas as pd

# ---- Config ----
PER_SEASON_MIN = 301.0  # minutes per season (gate)
FIVE_YEAR_AVG = 500.0  # minimum average over the 5-season window
REQUIRED_COLS = {
    "player",
    "season",  # parsed -> season_end
    "gp",
    "age",
    "tm",
    "toi_seconds_total_ev",  # used if minutes column missing
    "toi_total_hms_ev",  # used if seconds missing
    "toi_even_strength_min",  # preferred minutes column if present
}
# ---------------


# ----------------- helpers -----------------
def setup_logger(name="hr_filter", level="INFO"):
    log = logging.getLogger(name)
    if not log.handlers:
        log.setLevel(getattr(logging, level))
        h = logging.StreamHandler()
        h.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(name)s - %(message)s"))
        log.addHandler(h)
    return log


def require_cols(df: pd.DataFrame, cols: set, label: str):
    miss = cols - set(df.columns)
    if miss:
        raise KeyError(f"{label}: missing columns {sorted(miss)}")


def season_end_from_str(s: str):
    """Parse '13-14','2013-14','2013/14','2013–14','2014','99-00','1999-00' -> end year or NaN."""
    if s is None:
        return np.nan
    s = str(s).strip().replace("–", "-").replace("—", "-").replace("/", "-")
    m = re.fullmatch(r"(\d{4})-(\d{4})", s)
    if m:
        y1, y2 = map(int, m.groups())
        return y2 + (100 if y2 < y1 else 0)
    m = re.fullmatch(r"(\d{4})-(\d{2})", s)
    if m:
        y1, y2 = int(m.group(1)), int(m.group(2))
        y2 = (y1 // 100) * 100 + y2
        return y2 + (100 if y2 < y1 else 0)
    m = re.fullmatch(r"(\d{2})-(\d{4})", s)
    if m:
        a, y2 = int(m.group(1)), int(m.group(2))
        y1 = (y2 // 100) * 100 + a
        return y2 - (100 if y1 > y2 else 0)
    m = re.fullmatch(r"(\d{2})-(\d{2})", s)
    if m:
        a, b = int(m.group(1)), int(m.group(2))
        century = 1900 if a >= 90 else 2000
        y1, y2 = century + a, century + b
        return y2 + (100 if y2 < y1 else 0)
    if re.fullmatch(r"\d{4}", s):
        return int(s)
    return np.nan


def dynamic_age_bounds(season_end: int) -> tuple[float, float]:
    """Expand→hold→shrink; clamp to [25,29]."""
    if pd.isna(season_end):
        return (np.nan, np.nan)
    y = float(season_end)
    mx = min(29, 25 + (y - 2014))  # ramps to 29 by 2018
    mn = max(25, 25 + (y - 2021))  # stays 25 through 2021, then rises to 29 by 2025
    return max(25, min(mn, 29)), max(25, min(mx, 29))


def maybe_compute_minutes(df: pd.DataFrame, log: logging.Logger) -> pd.DataFrame:
    """Ensure 'toi_even_strength_min' is numeric minutes; compute from EV seconds/HMS if needed."""
    col = "toi_even_strength_min"
    if col in df.columns and df[col].notna().any():
        df[col] = pd.to_numeric(df[col], errors="coerce")
        log.info("Using existing '%s' (minutes).", col)
        return df

    sec = "toi_seconds_total_ev" if "toi_seconds_total_ev" in df.columns else None
    hms = "toi_total_hms_ev" if "toi_total_hms_ev" in df.columns else None

    # compute from seconds if available; else from H:M:S; very compact
    if sec and df[sec].notna().any():
        df[col] = pd.to_numeric(df[sec], errors="coerce") / 60.0
        log.info("Computed '%s' from '%s' / 60.", col, sec)
    elif hms and df[hms].notna().any():

        def _to_min(x):
            try:
                h, m, s = map(int, str(x).split(":"))
                return h * 60 + m + s / 60.0
            except Exception:
                return np.nan

        df[col] = df[hms].map(_to_min)
        log.info("Computed '%s' from '%s'.", col, hms)
    else:
        raise RuntimeError("No EV minutes/seconds/HMS columns to derive minutes.")
    return df


# ---------------------------------------------


# --------- window selection logic ----------
TARGET_AGES = np.array([25, 26, 27, 28, 29], dtype=float)


def _choose_best_5yr_window(sub: pd.DataFrame) -> list[int]:
    """
    sub: rows for ONE player (already age-window filtered), must contain
         season_end (int), age (float), toi_even_strength_min (float).
    Returns the chosen list of 5 season_end years (possibly empty if none qualifies).
    """
    # collapse to one row per season_end (keep mean minutes and rounded age)
    x = (
        sub[["season_end", "age", "toi_even_strength_min"]]
        .dropna(subset=["season_end"])
        .groupby("season_end", as_index=False)
        .agg(age=("age", "mean"), ev_min=("toi_even_strength_min", "mean"))
    )
    if x.empty:
        return []

    x["season_end"] = x["season_end"].astype(int)
    x["age_r"] = np.rint(x["age"]).astype(float)

    # keep only seasons that individually meet the per-season gate
    x = x.loc[x["ev_min"] >= PER_SEASON_MIN].sort_values("season_end")
    if len(x) < 5:
        return []

    seasons = x["season_end"].to_numpy()
    ages = x["age_r"].to_numpy()
    mins = x["ev_min"].to_numpy()

    best = (
        None  # tuple(score_is_exact, age_distance, -avg_minutes, start_year, idx_range, years_list)
    )

    # scan all 5-consecutive-season subwindows within any longer consecutive run
    # we can just try every i..i+4 and enforce consecutiveness by year deltas
    for i in range(0, len(seasons) - 4):
        yrs = seasons[i : i + 5]
        if not np.all(np.diff(yrs) == 1):
            continue  # not consecutive seasons

        a = ages[i : i + 5]
        m = mins[i : i + 5]
        avg5 = float(np.mean(m))

        if avg5 < FIVE_YEAR_AVG:
            continue

        # exact age set match?
        exact = np.array_equal(np.sort(a), TARGET_AGES)

        # distance to target 25..29 (allow small rounding mismatches)
        # Sort both and compute L1 distance; exact match -> 0
        age_dist = float(np.sum(np.abs(np.sort(a) - TARGET_AGES)))

        # ranking key: prefer exact match, then smaller age_dist, then larger avg5, then earlier start
        rank = (
            0 if exact else 1,
            age_dist,
            -avg5,
            yrs[0],
        )
        cand = (rank, (i, i + 5), yrs.tolist())

        if best is None or cand[0] < best[0]:
            best = cand

    return best[2] if best else []


# -------------------------------------------


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", default="data/outputs/hockeyref_final.csv")
    ap.add_argument("--output", default="data/outputs/hr_filtered_sorted_relage.csv")
    args = ap.parse_args()

    log = setup_logger("hr_filter", level="INFO")

    inp, outp = Path(args.input), Path(args.output)
    log.info("Loading HR CSV: %s", inp)
    df = pd.read_csv(inp)
    df.columns = [str(c).strip() for c in df.columns]
    log.info("Loaded rows: %d", len(df))
    require_cols(df, REQUIRED_COLS, "HR CSV")

    # Normalize types
    df["age"] = pd.to_numeric(df["age"], errors="coerce")
    df = maybe_compute_minutes(df, log)

    # season parsing + dynamic age filter
    df["season_end"] = df["season"].map(season_end_from_str).astype("Int64")
    bad = int(df["season_end"].isna().sum())
    if bad:
        log.warning("season_end parse failures: %d rows (they will drop out).", bad)

    bounds = df["season_end"].apply(dynamic_age_bounds)
    df[["age_min_allowed", "age_max_allowed"]] = pd.DataFrame(bounds.tolist(), index=df.index)
    mask_age = (df["age"] >= df["age_min_allowed"]) & (df["age"] <= df["age_max_allowed"])
    df_age_ok = df.loc[mask_age].copy().reset_index(drop=True)
    log.info(
        "Age-window filter: kept %d/%d (%.1f%%).",
        len(df_age_ok),
        len(df),
        100 * len(df_age_ok) / max(1, len(df)),
    )

    # # pick the *best* 5-season window per player (handles runs > 5 cleanly)
    keep_pairs = set()
    for player, sub in df_age_ok.groupby("player"):
        yrs = _choose_best_5yr_window(sub)
        keep_pairs.update((player, y) for y in yrs)

    if not keep_pairs:
        raise SystemExit("No player meets the 5-season rule with the required averages.")

    key = list(zip(df_age_ok["player"], df_age_ok["season_end"].astype(int), strict=False))
    mask_keep = pd.Series(key).isin(keep_pairs)
    df_filt = df_age_ok.loc[mask_keep].copy()

    # add rel_age ≈ round(age-27); sort and write
    df_filt["rel_age"] = (
        (pd.to_numeric(df_filt["age"], errors="coerce") - 27).round().astype("Int64")
    )
    df_filt = df_filt.sort_values(["player", "season_end", "tm"]).reset_index(drop=True)

    outp.parent.mkdir(parents=True, exist_ok=True)
    df_filt.to_csv(outp, index=False)
    log.info("[WRITE] -> %s", outp)
    log.info("[STATS] players: %d | rows: %d", df_filt["player"].nunique(), len(df_filt))


if __name__ == "__main__":
    main()

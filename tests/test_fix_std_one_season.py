#!/usr/bin/env python3
"""
Test one season (standard/seasons file only) — no concatenation, no merging.

- Reads a single CSV (standard seasons page export for one season)
- Normalizes columns, ensures a season string is present
- Makes the table strictly 1 row per (player, season):
    * If an nTM row (e.g., '2TM','3TM') exists for a player, keep exactly that row (set tm='TOT'),
      drop the other rows for that player-season.
    * If no nTM exists for a player, aggregate that player's team rows -> a synthetic 'TOT' row.
- Writes before/after CSVs and duplicate diagnostics to an output directory.

Usage:
  python test_fix_std_one_season.py --in path/to/nhl_player_seasons_2014.csv --out data/outputs/test_2014
"""

from __future__ import annotations

import argparse
import logging
import re
from pathlib import Path

import numpy as np
import pandas as pd

# If you already have these, great; they keep headers/TOI consistent with your pipeline.
# If not, the script still runs without compute_total_toi (it’s optional here).
try:
    from time_utils import (
        attach_season,
        compute_total_toi,
        normalize_columns,
        season_from_filename,
        seconds_to_hms,
    )
except Exception:
    normalize_columns = None
    season_from_filename = None
    attach_season = None
    compute_total_toi = None

_NTM_RE = re.compile(r"^\d+TM$", re.IGNORECASE)
PLAYER_ALIASES = ["player", "player_name", "name"]
TEAM_ALIASES = ["tm", "team", "teams", "franchise"]
SEASON_ALIASES = ["season", "yr", "year", "season_str"]
logger = logging.getLogger("test_fix_std_one_season")


def _setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
    )


def _mode_or_first(s: pd.Series):
    m = s.mode()
    if not m.empty:
        return m.iloc[0]
    nz = s.dropna()
    return nz.iloc[0] if not nz.empty else np.nan


def _seconds_to_hms_local(x) -> str:
    """Fallback pretty time if time_utils.seconds_to_hms is unavailable."""
    if pd.isna(x):
        return ""
    x = int(x)
    h, r = divmod(x, 3600)
    m, s = divmod(r, 60)
    return f"{h}:{m:02d}:{s:02d}"


def _ensure_season_column(df: pd.DataFrame, in_path: Path) -> pd.DataFrame:
    if "season" in df.columns and df["season"].notna().any():
        return df
    if season_from_filename is not None:
        s = season_from_filename(in_path)
        if s:
            return attach_season(df, season=s) if attach_season else df.assign(season=s)
    # last resort: try to infer from any 4-digit year in the filename
    m = re.search(r"(\d{4})", in_path.stem)
    if m:
        yyyy = int(m.group(1))
        s = f"{(yyyy - 1) % 100:02d}-{yyyy % 100:02d}"
        return df.assign(season=s)
    return df


def fix_one_season_std(df: pd.DataFrame) -> pd.DataFrame:
    """
    Make this single-season DataFrame 1 row per (player, season).

    Pass 1: If any nTM row exists in a (player, season) group, keep exactly one row
            (that nTM row → tm='TOT'), drop the rest of the group.
    Pass 2: If no nTM exists in the group but there are multiple team rows,
            aggregate numerics and create a synthetic TOT row.
    """
    if df.empty:
        return df.copy()

    e = df.copy()

    # normalize key columns used by the rule
    for col in ("player", "season", "tm"):
        if col not in e.columns:
            raise ValueError(f"Missing required column: {col}")

    e["player"] = e["player"].astype(str).str.rstrip().str.lower()
    e["season"] = e["season"].astype(str).str.rstrip()
    e["tm"] = e["tm"].astype(str).str.rstrip().str.upper()

    # Pass 1: keep first nTM row, drop others in that group
    e["_is_ntm"] = e["tm"].str.match(_NTM_RE)

    keep_idx = []
    for (_p, _s), g in e.groupby(["player", "season"], dropna=False):
        if g["_is_ntm"].any():
            keep_idx.append(g.loc[g["_is_ntm"]].index[0])  # first nTM row
        else:
            keep_idx.extend(g.index.tolist())  # decide in Pass 2

    e = e.loc[keep_idx].copy()
    e.loc[e["_is_ntm"], "tm"] = "TOT"
    e.drop(columns=["_is_ntm"], inplace=True)

    # Pass 2: groups still >1 row with no nTM → aggregate to synthetic TOT
    counts = e.groupby(["player", "season"], dropna=False).size().reset_index(name="rows")
    multi = counts.loc[counts["rows"] > 1, ["player", "season"]]

    if not multi.empty:
        key = ["player", "season"]

        e_multi = e.merge(multi, on=key, how="inner")
        e_unique = e.merge(multi, on=key, how="left", indicator=True)
        e_unique = e_unique[e_unique["_merge"] == "left_only"].drop(columns=["_merge", "rows"])

        # sum numeric columns
        exclude = {"player", "season", "tm", "pos", "toi_total_hms"}
        num_cols = [
            c
            for c in e_multi.columns
            if c not in exclude and pd.api.types.is_numeric_dtype(e_multi[c])
        ]
        g = e_multi.groupby(key, dropna=False)
        agg_num = g[num_cols].sum() if num_cols else pd.DataFrame(index=g.size().index)

        # representative pos
        if "pos" in e_multi.columns:
            pos_mode = g["pos"].apply(_mode_or_first).reset_index(name="pos")
        else:
            pos_mode = pd.DataFrame(columns=key + ["pos"])

        agg_df = agg_num.reset_index()
        if "pos" in pos_mode.columns:
            agg_df = agg_df.merge(pos_mode, on=key, how="left")
        agg_df["tm"] = "TOT"

        # recompute pretty TOI if we have seconds
        if "toi_seconds_total" in agg_df.columns:
            if "seconds_to_hms" in globals() and callable(seconds_to_hms):
                agg_df["toi_total_hms"] = agg_df["toi_seconds_total"].map(
                    lambda v: seconds_to_hms(int(v)) if pd.notna(v) else ""
                )
            else:
                agg_df["toi_total_hms"] = agg_df["toi_seconds_total"].map(_seconds_to_hms_local)

        # align columns and combine
        common_cols = sorted(set(e.columns) | set(agg_df.columns))
        e_unique = e_unique.reindex(columns=common_cols, fill_value=np.nan)
        agg_df = agg_df.reindex(columns=common_cols, fill_value=np.nan)

        e = pd.concat([e_unique, agg_df], ignore_index=True)

    # final safety: one row per (player, season)
    e = e.drop_duplicates(subset=["player", "season"], keep="first").reset_index(drop=True)
    return e


def coerce_required_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Rename common aliases to required: player, tm, season. Build player if first/last exist."""
    e = df.copy()

    # header names are already normalized (lower, snake) by normalize_columns()
    cols = {c.lower(): c for c in e.columns}

    def find_first(cands):
        for k in cands:
            if k in cols:
                return cols[k]
        return None

    # player
    player_col = find_first(PLAYER_ALIASES)
    if not player_col and {"first_name", "last_name"}.issubset(cols):
        # build player from first+last
        e["player"] = (
            (
                e[cols["first_name"]].astype(str).str.strip()
                + " "
                + e[cols["last_name"]].astype(str).str.strip()
            )
            .str.replace(r"\s+", " ", regex=True)
            .str.strip()
            .str.lower()
        )
    else:
        if player_col and player_col != "player":
            e = e.rename(columns={player_col: "player"})
        if "player" in e.columns:
            e["player"] = (
                e["player"].astype(str).str.replace(r"\s+", " ", regex=True).str.strip().str.lower()
            )

    # team → tm
    team_col = find_first(TEAM_ALIASES)
    if team_col and team_col != "tm":
        e = e.rename(columns={team_col: "tm"})
    if "tm" in e.columns:
        e["tm"] = e["tm"].astype(str).str.rstrip().str.upper()

    # season
    season_col = find_first(SEASON_ALIASES)
    if season_col and season_col != "season":
        e = e.rename(columns={season_col: "season"})
    if "season" in e.columns:
        e["season"] = e["season"].astype(str).str.rstrip()

    return e


def main():
    _setup_logging()
    ap = argparse.ArgumentParser(description="Test fix on a single seasons.csv (no concat/merge).")
    ap.add_argument(
        "--in", dest="in_path", required=True, help="Path to single seasons CSV (one season)."
    )
    ap.add_argument(
        "--out",
        dest="out_dir",
        default="data/outputs/test_one_season",
        help="Output folder for debug CSVs.",
    )
    ap.add_argument("--toi-col", default="toi", help="TOI column name if present (optional).")
    args = ap.parse_args()

    in_path = Path(args.in_path)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    # 1) Read
    df = pd.read_csv(in_path)
    logging.info("Loaded %s (%d rows, %d cols)", in_path, len(df), df.shape[1])

    # 2) Normalize headers (optional)
    if normalize_columns:
        df = normalize_columns(df)

    # 3) Ensure season col exists (from filename if needed)
    df = _ensure_season_column(df, in_path)

    df = coerce_required_columns(df)

    # quick introspection
    logging.info("Columns after coercion: %s", list(df.columns))
    missing = [c for c in ("player", "tm", "season") if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required column(s) after coercion: {missing}")

    # 4) Optional: compute_total_toi (kept harmless; skip if you want)
    if compute_total_toi and args.toi_col in df.columns:
        df = compute_total_toi(df, toi_col=args.toi_col)

    # 5) Write BEFORE & dupes BEFORE
    before_path = out_dir / "std_before_fix.csv"
    df.to_csv(before_path, index=False)
    logging.info("Wrote %s", before_path)

    dupes_before = (
        df.groupby(["player", "season"], dropna=False)
        .size()
        .reset_index(name="rows")
        .query("rows > 1")
    )
    dupes_before_path = out_dir / "std_dupes_before.csv"
    dupes_before.to_csv(dupes_before_path, index=False)
    logging.info("Wrote %s (groups=%d)", dupes_before_path, len(dupes_before))

    # 6) Fix
    fixed = fix_one_season_std(df)

    # 7) Write AFTER & dupes AFTER
    after_path = out_dir / "std_after_fix.csv"
    fixed.to_csv(after_path, index=False)
    logging.info("Wrote %s", after_path)

    dupes_after = (
        fixed.groupby(["player", "season"], dropna=False)
        .size()
        .reset_index(name="rows")
        .query("rows > 1")
    )
    dupes_after_path = out_dir / "std_dupes_after.csv"
    dupes_after.to_csv(dupes_after_path, index=False)
    if len(dupes_after):
        logging.warning(
            "Still have duplicate (player, season) rows after fix: %d groups", len(dupes_after)
        )
    else:
        logging.info("All good: one row per (player, season).")


if __name__ == "__main__":
    main()

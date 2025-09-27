#!/usr/bin/env python3
"""
Concatenate, normalize, and merge Hockey-Reference datasets.

- Standard (seasons) files are fixed PER-FILE *before* concatenation:
  * If an nTM row (e.g., '2TM','3TM') exists for a player-season, keep that row (tm='TOT') and drop the rest.
  * If no nTM row exists but there are multiple team rows, aggregate numerics to a synthetic TOT row.

- Even-strength files: DO NOT collapse by team. Only convert avg TOI (mm:ss) × GP → season totals.

- Merge: uses a temporary (player, season) aggregation of even-strength for the join only
  (sum TOI seconds, minutes-weighted CF% if available).

Outputs:
  data/outputs/hockeyref_std_concat.csv
  data/outputs/hockeyref_even_concat.csv
  data/outputs/hockeyref_final.csv
  (plus a few diagnostics when helpful)
"""

from __future__ import annotations

import argparse
import logging
import re
from collections.abc import Iterable
from pathlib import Path

import numpy as np
import pandas as pd
from log_utils import setup_logger
from time_utils import (
    attach_season,
    compute_total_toi,  # can parse mm:ss or total and fill toi_seconds_total / toi_total_hms
    normalize_columns,
    season_from_filename,
    seconds_to_hms,
)

logger = logging.getLogger(__name__)
_NTM_RE = re.compile(r"^\d+TM$", re.IGNORECASE)


# -------------------------
# Small utilities
# -------------------------
def list_csvs(folder: Path, pattern: str = "*.csv") -> list[Path]:
    return sorted([p for p in folder.rglob(pattern) if p.is_file()])


def _coerce_std_required_columns(df: pd.DataFrame, *, debug_tag: str = "") -> pd.DataFrame:
    """
    Map common aliases to required std columns:
      player ← {player, player_name, name} or first_name+last_name
      tm     ← many aliases; if not found, auto-detect by regex on sample values
      season ← {season, year, yr, season_str, season_end}
    """
    e = df.copy()
    cols = {c.lower(): c for c in e.columns}

    def get(*cands):
        for k in cands:
            if k in cols:
                return cols[k]
        return None

    # --- player ---
    player_col = get("player", "player_name", "name")
    if not player_col and "first_name" in cols and "last_name" in cols:
        e["player"] = (
            e[cols["first_name"]].astype(str).str.strip()
            + " "
            + e[cols["last_name"]].astype(str).str.strip()
        )
    elif player_col and player_col != "player":
        e = e.rename(columns={player_col: "player"})

    # --- season ---
    season_col = get("season", "year", "yr", "season_str", "season_end")
    if season_col and season_col != "season":
        e = e.rename(columns={season_col: "season"})

    # --- team (tm) aliases first ---
    tm_alias = get(
        "tm",
        "team",
        "teams",
        "team_name_abbr",
        "team_abbr",
        "team_short",
        "team_id",
        "franchise",
        "franchise_id",
        "club",
        "club_id",
    )
    if tm_alias and tm_alias != "tm":
        e = e.rename(columns={tm_alias: "tm"})

    # --- auto-detect 'tm' if still missing ---
    if "tm" not in e.columns:
        # Heuristic: look for a column where many values look like team codes (BOS, NYR, OTT, TOT, 2TM, 3TM)
        candidates = []
        sample = e.head(200)  # sufficient for detection
        for c in e.columns:
            s = sample[c].astype(str).str.strip().str.upper()
            if s.empty:
                continue
            m = s.str.match(r"^(?:[A-Z]{2,3}|TOT|\d+TM)$", na=False)
            ratio = m.mean() if len(m) else 0.0
            # require at least 30% of sampled values to look like codes
            if ratio >= 0.30:
                candidates.append((c, ratio))
        if candidates:
            # pick the strongest match
            candidates.sort(key=lambda x: x[1], reverse=True)
            best, score = candidates[0]
            logger.info(
                "Auto-detected team column '%s' (score=%.2f)%s",
                best,
                score,
                f" [{debug_tag}]" if debug_tag else "",
            )
            if best != "tm":
                e = e.rename(columns={best: "tm"})

    # --- tidy keys (now that names are mapped) ---
    if "player" in e.columns:
        e["player"] = (
            e["player"].astype(str).str.replace(r"\s+", " ", regex=True).str.rstrip().str.lower()
        )
    if "tm" in e.columns:
        e["tm"] = e["tm"].astype(str).str.rstrip().str.upper()
    if "season" in e.columns:
        e["season"] = e["season"].astype(str).str.rstrip()
    if "pos" in e.columns:
        e["pos"] = e["pos"].astype(str).str.rstrip().str.upper()

    # --- final safety & diagnostics ---
    missing = [k for k in ("player", "season", "tm") if k not in e.columns]
    if missing:
        # Drop a quick diagnostic to help identify the offending file/columns
        diag_dir = Path("data/outputs/_diagnostics")
        diag_dir.mkdir(parents=True, exist_ok=True)
        # keep just head() to keep files small
        diag_path = (
            diag_dir
            / f"std_missing_{'_'.join(missing)}{('_' + debug_tag) if debug_tag else ''}.csv"
        )
        e.head(200).to_csv(diag_path, index=False)
        logger.error(
            "Std file missing required column(s): %s. Wrote sample to %s", missing, diag_path
        )
    return e


def _write_csv(df: pd.DataFrame, path: Path, name: str | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)
    # label = name or path.name
    logger.info("Wrote %s (%d rows, %d cols)", path, len(df), df.shape[1])


def _post_name_normalization(df: pd.DataFrame) -> pd.DataFrame:
    """
    Harmonize keys used in merges/groupbys:
      - rstrip() for end whitespace
      - lower() for 'player'
      - upper() for 'tm'
    """
    out = df.copy()
    if "player" in out.columns:
        out["player"] = (
            out["player"].astype(str).str.replace(r"\s+", " ", regex=True).str.rstrip().str.lower()
        )
    for c in ("tm",):
        if c in out.columns:
            out[c] = out[c].astype(str).str.rstrip().str.upper()
    if "season" in out.columns:
        out["season"] = out["season"].astype(str).str.rstrip()
    if "pos" in out.columns:
        out["pos"] = out["pos"].astype(str).str.rstrip().str.upper()
    return out


def _mode_or_first(s: pd.Series):
    m = s.mode()
    if not m.empty:
        return m.iloc[0]
    nz = s.dropna()
    return nz.iloc[0] if not nz.empty else np.nan


# -------------------------
# Core fix: standard (seasons) — per-file
# -------------------------
def fix_std_one_file(df: pd.DataFrame, season_str: str) -> pd.DataFrame:
    """
    Make a single-season 'standard' DataFrame 1 row per (player, season).

    Pass 1: If any nTM row exists (e.g., '2TM','3TM'), keep exactly one nTM row
            (convert to tm='TOT'), drop the rest of that player-season group.

    Pass 2: For any player-season still having multiple rows with NO nTM,
            aggregate numerics to a synthetic TOT row (sum numerics, mode pos),
            recompute toi_total_hms from toi_seconds_total if present.
    """
    if df.empty:
        return df.copy()

    e = df.copy()
    e["season"] = season_str

    # Coerce aliases & auto-detect tm
    e = _coerce_std_required_columns(e, debug_tag=f"season_{season_str}")

    # If we still don't have the essentials, bail gracefully (diagnostics already written)
    if not {"player", "tm", "season"}.issubset(e.columns):
        logger.error("Cannot fix std file for season %s — missing keys after coercion.", season_str)
        return e

    # Normalize merge keys
    e = _post_name_normalization(e)

    # Pass 1: keep first nTM row, drop others for that (player, season)
    e["_is_ntm"] = e["tm"].str.match(_NTM_RE)
    keep_idx: list[int] = []
    for (_p, _s), g in e.groupby(["player", "season"], dropna=False):
        if g["_is_ntm"].any():
            keep_idx.append(g.loc[g["_is_ntm"]].index[0])  # keep first nTM row
        else:
            keep_idx.extend(g.index.tolist())  # handle in Pass 2

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
        # FIX: only drop _merge (rows is not present here)
        e_unique = e_unique[e_unique["_merge"] == "left_only"].drop(columns=["_merge"])

        # Numeric columns to sum (exclude identifiers / textuals)
        exclude = {"player", "season", "tm", "pos", "toi_total_hms"}
        num_cols = [
            c
            for c in e_multi.columns
            if c not in exclude and pd.api.types.is_numeric_dtype(e_multi[c])
        ]
        g = e_multi.groupby(key, dropna=False)
        agg_num = g[num_cols].sum() if num_cols else pd.DataFrame(index=g.size().index)

        # Representative position
        if "pos" in e_multi.columns:
            pos_mode = g["pos"].apply(_mode_or_first).reset_index(name="pos")
        else:
            pos_mode = pd.DataFrame(columns=key + ["pos"])

        agg_df = agg_num.reset_index()
        if "pos" in pos_mode.columns:
            agg_df = agg_df.merge(pos_mode, on=key, how="left")

        agg_df["tm"] = "TOT"

        # Pretty H:MM:SS if we have seconds
        if "toi_seconds_total" in agg_df.columns:
            agg_df["toi_total_hms"] = agg_df["toi_seconds_total"].map(
                lambda v: seconds_to_hms(int(v)) if pd.notna(v) else ""
            )

        # Align columns & combine
        common_cols = sorted(set(e.columns) | set(agg_df.columns))
        # FIX: reindex BOTH sides to the same columns before concat
        e_unique = e_unique.reindex(columns=common_cols, fill_value=np.nan)
        agg_df = agg_df.reindex(columns=common_cols, fill_value=np.nan)

        # FIX: remove duplicate second filtering of e_unique (that line caused the KeyError later)
        e = pd.concat([e_unique, agg_df], ignore_index=True)

    # Final safety: strictly 1 row per (player, season)
    e = e.drop_duplicates(subset=["player", "season"], keep="first").reset_index(drop=True)
    return e


# -------------------------
# Even-strength: ONLY compute season totals from avg mm:ss × GP
# -------------------------
def ensure_even_totals(df_even: pd.DataFrame, toi_col: str = "toi") -> pd.DataFrame:
    """
    Even-strength data:
      - Normalize keys
      - Convert per-game TOI (mm:ss or hh:mm:ss) × GP → season totals
      - Adds/ensures:
          * toi_seconds_total (Int64)
          * toi_total_hms (H:MM:SS)
      - DOES NOT collapse or change 'tm'
    """
    if df_even.empty:
        return df_even

    e = _post_name_normalization(df_even)

    # If time_utils.compute_total_toi is robust across both avg and total, use it directly:
    if toi_col in e.columns:
        e = compute_total_toi(e, toi_col=toi_col)

    # Some even files might already have totals; above call is idempotent.
    # Ensure the friendly H:MM:SS string exists.
    if "toi_seconds_total" in e.columns and "toi_total_hms" not in e.columns:
        e["toi_total_hms"] = e["toi_seconds_total"].map(
            lambda v: seconds_to_hms(int(v)) if pd.notna(v) else ""
        )

    return e


# -------------------------
# Reader: per-folder, with per-file std fix
# -------------------------
def read_concat_folder(
    folder: Path,
    *,
    derive_season_from_name: bool = True,
    toi_col: str = "toi",
    fix_std_ntm: bool = False,  # apply std per-file nTM/TOT fix
) -> pd.DataFrame:
    files = list_csvs(folder)
    if not files:
        logger.warning("No CSVs found in %s", folder)
        return pd.DataFrame()

    frames: list[pd.DataFrame] = []
    for f in files:
        df = pd.read_csv(f)
        df = normalize_columns(df)

        # attach or derive season for this file
        season_str = None
        if derive_season_from_name and "season" not in df.columns:
            season_str = season_from_filename(f)
            if season_str:
                df = attach_season(df, season=season_str)
        else:
            season_str = df["season"].iloc[0] if "season" in df.columns else None

        # compute total seconds / H:MM:SS if a TOI column exists
        if toi_col in df.columns:
            df = compute_total_toi(df, toi_col=toi_col)

        # std per-file fix (only for the seasons folder)
        if fix_std_ntm:
            if not season_str:
                season_str = season_from_filename(f) or ""
            df = fix_std_one_file(df, season_str)

        df = _post_name_normalization(df)
        frames.append(df)

    out = pd.concat(frames, ignore_index=True)
    logger.info(
        "Loaded %d files from %s (rows=%d, cols=%d)", len(files), folder, len(out), out.shape[1]
    )
    return out


# -------------------------
# Filters and drops
# -------------------------
def drop_columns(df: pd.DataFrame, cols: Iterable[str]) -> pd.DataFrame:
    to_drop = [c for c in cols if c in df.columns]
    if to_drop:
        logger.info("Dropping columns: %s", ", ".join(to_drop))
        return df.drop(columns=to_drop)
    return df


def filter_players(
    df: pd.DataFrame,
    *,
    min_toi_minutes: int | None = None,
    age_min: int | None = None,
    age_max: int | None = None,
    positions: Iterable[str] | None = None,
) -> pd.DataFrame:
    if df.empty:
        return df
    mask = pd.Series(True, index=df.index)

    if min_toi_minutes is not None and "toi_seconds_total" in df.columns:
        mask &= df["toi_seconds_total"].fillna(0) >= int(min_toi_minutes) * 60
        logger.info("Filter: TOI >= %d min -> %d rows", min_toi_minutes, int(mask.sum()))

    if (age_min is not None or age_max is not None) and "age" in df.columns:
        age = pd.to_numeric(df["age"], errors="coerce")
        if age_min is not None:
            mask &= age >= age_min
        if age_max is not None:
            mask &= age <= age_max
        logger.info("Filter: age [%s, %s] -> %d rows", age_min, age_max, int(mask.sum()))

    if positions and "pos" in df.columns:
        wanted = {p.strip().upper() for p in positions if p.strip()}
        mask &= df["pos"].astype(str).str.upper().isin(wanted)
        logger.info("Filter: positions %s -> %d rows", sorted(wanted), int(mask.sum()))

    return df.loc[mask].copy()


# -------------------------
# Public API
# -------------------------
def build_ref_hockey(
    std_dir: Path | str = "data/seasons",
    even_dir: Path | str = "data/even_strength",
    *,
    drop_cols: list[str] | None = None,
    min_toi_minutes: int | None = None,
    age_min: int | None = None,
    age_max: int | None = None,
    positions: list[str] | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Returns: (std_concat_df, even_concat_df, merged_filtered_df)

    - std_dir: seasons pages (we fix per-file nTM→TOT / synthesize TOT, 1 row per player-season)
    - even_dir: even-strength pages (NO collapsing; only avg mm:ss × GP → season totals)
    """
    std_dir = Path(std_dir)
    even_dir = Path(even_dir)

    # 1) Read / fix / concat
    df_std = read_concat_folder(std_dir, fix_std_ntm=True)  # per-file nTM/TOT fix
    df_even = read_concat_folder(even_dir, fix_std_ntm=False)  # no collapsing on even
    df_even = ensure_even_totals(df_even, toi_col="toi")  # avg→season totals only

    # --- NEW: dedupe even-strength on (player, season_end) removing goalies, duplicates(6) ---
    if "player" in df_even.columns:
        key_col = (
            "season_end"
            if "season_end" in df_even.columns
            else ("season" if "season" in df_even.columns else None)
        )
        if key_col:
            before = len(df_even)
            df_even = df_even.drop_duplicates(subset=["player", key_col])
            logger.info(
                "Even-strength dedupe on ['player','%s']: %d -> %d rows (removed %d)",
                key_col,
                before,
                len(df_even),
                before - len(df_even),
            )
        else:
            logger.warning(
                "Even-strength dedupe skipped: no 'season_end' or 'season' column found."
            )
    else:
        logger.warning("Even-strength dedupe skipped: no 'player' column found.")

    # Diagnostics: write concatenated sources for inspection
    out_dir = Path("data/outputs")
    _write_csv(df_std, out_dir / "hockeyref_std_concat.csv", "std_concat")
    _write_csv(df_even, out_dir / "hockeyref_even_concat.csv", "even_concat")

    # 2) Prepare merge slices
    std_keep = [
        c
        for c in (
            "player",
            "season",
            "gp",
            "age",
            "tm",
            "pos",
            "toi_seconds_total",
            "toi_total_hms",
        )
        if c in df_std.columns
    ]
    even_keep = [
        c
        for c in (
            "player",
            "season",
            "tm",
            "pos",
            "gp",
            "cf_rel",
            "toi_seconds_total",
            "toi_total_hms",
        )
        if c in df_even.columns
    ]

    std_slim = df_std[std_keep].copy() if std_keep else pd.DataFrame()
    even_slim = df_even[even_keep].copy() if even_keep else pd.DataFrame()

    # 3) Temporary (player, season) aggregate for even (for MERGE ONLY)
    if not std_slim.empty and not even_slim.empty:
        # sum additive fields (TOI seconds)
        ev_tot = even_slim.groupby(["player", "season"], as_index=False)["toi_seconds_total"].sum()

        # weighted CF% if present (weight = TOI seconds), fallback to simple mean
        if "cf_rel" in even_slim.columns:
            grp = even_slim.copy()
            grp["cf_rel_num"] = pd.to_numeric(grp["cf_rel"], errors="coerce")
            grp["toi_sec"] = pd.to_numeric(grp["toi_seconds_total"], errors="coerce").fillna(0)

            valid = grp.dropna(subset=["cf_rel_num"])
            w_sum = valid.groupby(["player", "season"])["toi_sec"].sum()
            wx_sum = (
                (valid["cf_rel_num"] * valid["toi_sec"])
                .groupby([valid["player"], valid["season"]])
                .sum()
            )
            wavg = (wx_sum / w_sum).to_frame("cf_rel")

            mean_cf = grp.groupby(["player", "season"])["cf_rel_num"].mean().to_frame("cf_rel_mean")

            cf_rel_w = (
                wavg.merge(mean_cf, left_index=True, right_index=True, how="outer")
                .assign(cf_rel=lambda d: d["cf_rel"].fillna(d["cf_rel_mean"]))
                .drop(columns=["cf_rel_mean"])
                .reset_index()  # → columns: player, season, cf_rel
            )
            ev_tot = ev_tot.merge(cf_rel_w, on=["player", "season"], how="left")

        # friendly H:MM:SS
        if "toi_seconds_total" in ev_tot.columns:
            ev_tot["toi_total_hms"] = ev_tot["toi_seconds_total"].map(
                lambda v: seconds_to_hms(int(v)) if pd.notna(v) else ""
            )

        # merge on (player, season) to avoid row multiplication
        merged = std_slim.merge(
            ev_tot, on=["player", "season"], how="left", suffixes=("_std", "_ev")
        )
        logger.info(
            "Merged on ['player','season'] -> rows=%d, cols=%d", len(merged), merged.shape[1]
        )
    else:
        merged = std_slim if not std_slim.empty else even_slim
        logger.info("Only one side present; skipping merge. Rows=%d", len(merged))

    # 4) Drop columns (optional)
    if drop_cols:
        merged = drop_columns(merged, drop_cols)

    # 5) Filters (optional)
    merged_filtered = filter_players(
        merged,
        min_toi_minutes=min_toi_minutes,
        age_min=age_min,
        age_max=age_max,
        positions=positions,
    )

    return df_std, df_even, merged_filtered


def write_outputs(df: pd.DataFrame, out_dir: Path, base_name: str = "hockeyref_final") -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    csv_path = out_dir / f"{base_name}.csv"
    df.to_csv(csv_path, index=False)
    logger.info("Wrote %s (%d rows)", csv_path, len(df))


# -------------------------
# CLI
# -------------------------
def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        description="Build merged Hockey-Reference dataset from two folders."
    )
    ap.add_argument("--std-dir", default="data/seasons")
    ap.add_argument("--even-dir", default="data/even_strength")
    ap.add_argument("--out-dir", default="data/outputs")

    ap.add_argument("--drop-cols", default="toi,toi_avg,toi_avg_mmss,toi_seconds_avg")
    ap.add_argument("--min-toi-minutes", type=int, default=None)
    ap.add_argument("--age-min", type=int, default=None)
    ap.add_argument("--age-max", type=int, default=None)
    ap.add_argument("--positions", default="")  # e.g., "C,LW,RW,D,G"
    ap.add_argument("--log-level", default="INFO")
    return ap.parse_args()


#     write_outputs(merged, Path(args.out_dir))
def main():
    setup_logger(None, level=logging.INFO)
    args = parse_args()
    setup_logger(None, level=getattr(logging, args.log_level.upper(), logging.INFO))

    drops = [x.strip() for x in args.drop_cols.split(",") if x.strip()]
    pos = [x.strip() for x in args.positions.split(",") if x.strip()]

    std_df, even_df, merged = build_ref_hockey(
        args.std_dir,
        args.even_dir,
        drop_cols=drops,
        min_toi_minutes=args.min_toi_minutes,
        age_min=args.age_min,
        age_max=args.age_max,
        positions=pos or None,
    )

    write_outputs(merged, Path(args.out_dir))


if __name__ == "__main__":
    main()

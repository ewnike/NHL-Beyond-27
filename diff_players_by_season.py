#!/usr/bin/env python3
# diff_players_by_season.py

import argparse
import logging
import sys
from pathlib import Path

import pandas as pd
from log_utils import setup_logger

logger = logging.getLogger(__name__)

# ---------- helpers ----------
POSSIBLE_PLAYER_COLS = ["player_id", "playerId", "player", "Player", "player_name", "name"]
POSSIBLE_SEASON_COLS = ["season", "Season", "year", "Year"]


def read_any(path: Path) -> pd.DataFrame:
    ext = path.suffix.lower()
    if ext in [".csv", ".tsv"]:
        return pd.read_csv(
            path, sep="," if ext == ".csv" else "\t", dtype=str, keep_default_na=False
        )
    if ext in [".parquet", ".pq"]:
        return pd.read_parquet(path)
    if ext in [".json", ".jsonl", ".ndjson"]:
        lines = ext in [".jsonl", ".ndjson"]
        return pd.read_json(path, lines=lines, dtype=str)
    raise ValueError(f"Unsupported file type: {ext}")


def guess_col(df: pd.DataFrame, candidates) -> str | None:
    cols = {c.lower(): c for c in df.columns}  # map lower->actual
    for c in candidates:
        if c.lower() in cols:
            return cols[c.lower()]
    return None


def normalize_cols(df: pd.DataFrame, player_col: str, season_col: str) -> pd.DataFrame:
    out = df.copy()
    # stringify and trim/normalize spaces
    out[player_col] = out[player_col].astype(str).str.strip().str.replace(r"\s+", " ", regex=True)
    out[season_col] = out[season_col].astype(str).str.strip()
    return out[[player_col, season_col]]


def to_keyset(df: pd.DataFrame, player_col: str, season_col: str) -> set[tuple[str, str]]:
    return set(map(tuple, df[[player_col, season_col]].itertuples(index=False, name=None)))


def print_sample(label: str, tuples: list[tuple[str, str]], max_rows: int = 20):
    print(f"\n{label} (showing up to {max_rows}):")
    for p, s in tuples[:max_rows]:
        print(f"  player={p} | season={s}")
    if len(tuples) > max_rows:
        print(f"  ... (+{len(tuples) - max_rows} more)")


# ---------- main ----------
def main():
    # 1) initialize logging (this sets BOTH console + rotating file logs)
    setup_logger(__name__)  # defaults to level=INFO and log_dir="logs"

    logger.info("Starting diff...")

    # ... your existing logic ...
    # replace prints like this:
    # print("=== Summary ===")
    logger.info("=== Summary ===")

    ap = argparse.ArgumentParser(
        description="Print (player, season) pairs that differ between two files."
    )
    ap.add_argument("file_a", type=Path, help="First file (CSV/Parquet/JSON)")
    ap.add_argument("file_b", type=Path, help="Second file (CSV/Parquet/JSON)")
    ap.add_argument("--player-col", help="Player column name (auto-detected if omitted)")
    ap.add_argument("--season-col", help="Season column name (auto-detected if omitted)")
    ap.add_argument("--save-csv", type=Path, help="Optional: directory to save CSV outputs")
    ap.add_argument("--a-name", default="A", help="Label for file A in output")
    ap.add_argument("--b-name", default="B", help="Label for file B in output")
    args = ap.parse_args()

    # Load
    try:
        df_a = read_any(args.file_a)
        df_b = read_any(args.file_b)
    except Exception as e:
        print(f"Error reading files: {e}", file=sys.stderr)
        sys.exit(1)

    # Detect columns
    player_col_a = args.player_col or guess_col(df_a, POSSIBLE_PLAYER_COLS)
    season_col_a = args.season_col or guess_col(df_a, POSSIBLE_SEASON_COLS)
    player_col_b = args.player_col or guess_col(df_b, POSSIBLE_PLAYER_COLS)
    season_col_b = args.season_col or guess_col(df_b, POSSIBLE_SEASON_COLS)

    for lbl, pc, sc in [
        (args.a_name, player_col_a, season_col_a),
        (args.b_name, player_col_b, season_col_b),
    ]:
        if not pc or not sc:
            print(
                f"Could not detect columns in {lbl}. Use --player-col and --season-col explicitly.",
                file=sys.stderr,
            )
            sys.exit(2)

    # Normalize & select
    a = normalize_cols(df_a, player_col_a, season_col_a).rename(
        columns={player_col_a: "player", season_col_a: "season"}
    )
    b = normalize_cols(df_b, player_col_b, season_col_b).rename(
        columns={player_col_b: "player", season_col_b: "season"}
    )

    # Unique rows only (in case of duplicates)
    a = a.drop_duplicates()
    b = b.drop_duplicates()

    # Build sets
    set_a = to_keyset(a, "player", "season")
    set_b = to_keyset(b, "player", "season")

    only_in_a = sorted(list(set_a - set_b))  # noqa: C414
    only_in_b = sorted(list(set_b - set_a))  # noqa: C414

    logger.info("=== Summary ===")
    logger.info("A: %d unique (player, season)", len(a))
    logger.info("B: %d unique (player, season)", len(b))
    logger.info("Only in A: %d", len(only_in_a))
    logger.info("Only in B: %d", len(only_in_b))
    logger.info("Net difference (A - B): %d", len(only_in_a) - len(only_in_b))

    # samples
    def log_sample(label, rows, k=20):
        logger.info("%s (showing up to %d):", label, k)
        for p, s in rows[:k]:
            logger.info("  player=%s | season=%s", p, s)
        if len(rows) > k:
            logger.info("  ... (+%d more)", len(rows) - k)

    log_sample("Pairs only in A", only_in_a)
    log_sample("Pairs only in B", only_in_b)


if __name__ == "__main__":
    main()

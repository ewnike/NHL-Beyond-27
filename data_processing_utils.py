"""
Tiny Orchestrator: Data Processing Utility
=========================================

Keeps this file small and readable. All heavy lifting now lives in:
  • data_pipeline.py          ← core CSV→Pandas→DB pipeline (generic)
  • hockeyref_workflows.py    ← domain logic (Hockey-Ref cleaners & merge→DataFrame)

This script wires configs + CLI flags and delegates work to those modules.
"""

from __future__ import annotations

import argparse
from pathlib import Path

# Core + domain imports
from data_pipeline import DatasetConfig, process_dataset, process_dataset_age_filtered
from db_utils import get_db_engine
from log_utils import setup_logger

logger = setup_logger("dp_orchestrator")

# -----------------------------------------------------------------------------
# Minimal, explicit configs you can explain in 30 seconds
# -----------------------------------------------------------------------------


def hockeyref_config(bucket: str | None, prefix: str | None) -> DatasetConfig:
    return DatasetConfig(
        name="hockeyref_players",
        table_name="hockeyref_players",
        conflict_cols=["player", "season"],
        columns_in_order=[
            # adjust as needed to match your destination table
            "player",
            "season",
            "age",
            "gp",
            "g",
            "a",
            "pts",
        ],
        numeric_cols=["age", "gp", "g", "a", "pts"],
        s3_bucket=bucket,
        s3_prefix=prefix,
        file_regex=r".*\.csv$",
    )


# -----------------------------------------------------------------------------
# CLI
# -----------------------------------------------------------------------------


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Tiny orchestrator for the data pipeline")

    # Connection
    p.add_argument(
        "--engine-url", required=False, help="Optional SQLAlchemy URL (else env via db_utils)"
    )

    # Ingest: Hockey-Ref -> DB
    p.add_argument("--run-hockeyref", action="store_true", help="Load Hockey-Ref CSVs to DB")
    p.add_argument("--s3-bucket", default=None, help="S3 bucket for HR CSVs")
    p.add_argument("--s3-prefix", default=None, help="S3 prefix/folder with season CSVs")
    p.add_argument(
        "--s3-keys",
        default=None,
        help="Comma-separated list of explicit keys (skip prefix listing)",
    )

    # Optional age filter
    p.add_argument("--age-min", type=int, default=None)
    p.add_argument("--age-max", type=int, default=None)
    p.add_argument("--min-prefix", type=int, default=1, help="min seasons before an age to keep")
    p.add_argument("--min-suffix", type=int, default=1, help="min seasons after an age to keep")
    p.add_argument("--require-contiguous", action="store_true")
    p.add_argument("--keep-longest-run", action="store_true")

    # Demo: Merge to DataFrame (no DB writes)
    p.add_argument(
        "--merge-df",
        action="store_true",
        help="Join HR ↔︎ player_peak_season and return a DataFrame",
    )
    p.add_argument("--left-table", default="hockeyref_players")
    p.add_argument("--right-table", default="player_peak_season")
    p.add_argument(
        "--join-keys",
        default="player,season",
        help="Comma-separated join keys; default player,season",
    )
    p.add_argument("--out-csv", default=None, help="Optional file path to save merged DataFrame")

    return p.parse_args(argv)


# -----------------------------------------------------------------------------
# Run
# -----------------------------------------------------------------------------


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)

    # Engine
    if args.engine_url:
        from sqlalchemy import create_engine as _ce

        engine = _ce(args.engine_url, future=True)
    else:
        engine = get_db_engine()

    # Parse explicit S3 keys
    parsed_s3_keys = None
    if args.s3_keys:
        parsed_s3_keys = [k.strip() for k in args.s3_keys.split(",") if k.strip()]

    # 1) Ingest Hockey-Ref CSVs → DB
    if args.run_hockeyref:
        cfg = hockeyref_config(args.s3_bucket, args.s3_prefix)
        # Decide whether to apply age filtering
        wants_age_filter = (
            any(v is not None for v in (args.age_min, args.age_max))
            or args.require_contiguous
            or args.keep_longest_run
            or args.min_prefix != 1
            or args.min_suffix != 1
        )

        if wants_age_filter:
            staged = process_dataset_age_filtered(
                engine,
                cfg,
                age_filter={
                    "group_cols": ["player"],  # Hockey-Ref uses name, not numeric id
                    "age_col": "age",
                    "min_age": args.age_min,
                    "max_age": args.age_max,
                    "min_prefix": args.min_prefix,
                    "min_suffix": args.min_suffix,
                    "require_contiguous": args.require_contiguous,
                    "keep_longest_run": args.keep_longest_run,
                },
                s3_keys=parsed_s3_keys,
            )
        else:
            staged = process_dataset(engine, cfg, s3_keys=parsed_s3_keys)
        logger.info("Hockey-Ref staged CSV: %s", staged)

    # 2) Demo merge: HR ↔︎ player_peak_season → DataFrame (no DB writes)
    if args.merge_df:
        join_keys = [k.strip() for k in args.join_keys.split(",") if k.strip()]
        df = merge_as_dataframe(  # noqa: F821
            engine,
            left_table=args.left_table,
            right_table=args.right_table,
            join_keys=join_keys,
        )
        logger.info("Merged DataFrame shape: %%s", df.shape)
        if args.out_csv:
            out = Path(args.out_csv)
            out.parent.mkdir(parents=True, exist_ok=True)
            df.to_csv(out, index=False)
            logger.info("Saved merged DataFrame → %s", out)

    logger.info("Done.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

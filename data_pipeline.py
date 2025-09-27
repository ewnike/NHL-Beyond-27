"""
Core Data Pipeline
==================

Holds the generic, reusable machinery:
  • DatasetConfig model
  • S3 listing/downloading (delegates to s3_utils when available)
  • CSV→Pandas→schema enforcement→stage CSV
  • COPY→INSERT ... ON CONFLICT upsert (expects a copy_csv_to_table helper)
  • process_dataset / process_dataset_age_filtered

Domain-specific cleaning/merging lives in a separate module (e.g. hockeyref_workflows.py).
"""

from __future__ import annotations

import argparse
import csv
import re
import tempfile
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path

import pandas as pd

# -----------------------------------------------------------------------------
# Logging
# -----------------------------------------------------------------------------
from log_utils import setup_logger

logger = setup_logger("data_pipeline")

# -----------------------------------------------------------------------------
# DB wiring
# -----------------------------------------------------------------------------
try:
    from db_utils import copy_csv_to_table  # type: ignore

    HAVE_DB_COPY = True
except Exception:
    copy_csv_to_table = None  # type: ignore
    HAVE_DB_COPY = False

# -----------------------------------------------------------------------------
# S3 wiring (prefer user's s3_utils)
# -----------------------------------------------------------------------------
try:
    from s3_utils import download_from_s3, download_many, list_keys  # type: ignore

    HAVE_S3_UTILS = True
except Exception:
    list_keys = None  # type: ignore
    download_many = None  # type: ignore
    download_from_s3 = None  # type: ignore
    HAVE_S3_UTILS = False

# -----------------------------------------------------------------------------
# Types & Config
# -----------------------------------------------------------------------------
CleanerFn = Callable[[pd.DataFrame], pd.DataFrame]


@dataclass
class DatasetConfig:
    name: str
    table_name: str
    conflict_cols: list[str]
    columns_in_order: list[str]
    numeric_cols: list[str] = field(default_factory=list)
    s3_bucket: str | None = None
    s3_prefix: str | None = None  # Folder/prefix containing CSVs
    file_regex: str = r".*\.csv$"  # Which files to include under prefix
    cleaner: CleanerFn | None = None


# -----------------------------------------------------------------------------
# Minimal generic cleaning helpers (domain-specific cleaners live elsewhere)
# -----------------------------------------------------------------------------
SNAKE_RE = re.compile(r"[^A-Za-z0-9]+")


def to_snake(name: str) -> str:
    name = SNAKE_RE.sub("_", name).strip("_")
    return re.sub(r"__+", "_", name).lower()


def basic_trim_and_types(df: pd.DataFrame) -> pd.DataFrame:
    for c in df.columns:
        if df[c].dtype == object:
            df[c] = df[c].astype(str).str.strip()
    return df


# -----------------------------------------------------------------------------
# CSV stack / schema / staging
# -----------------------------------------------------------------------------


def load_and_clean_csvs(paths: list[Path], cleaner: CleanerFn | None) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for p in paths:
        logger.info("Reading %s", p)
        df = pd.read_csv(p)
        if cleaner:
            df = cleaner(df)
        frames.append(df)
    if not frames:
        raise RuntimeError("No CSVs loaded to process.")
    df_all = pd.concat(frames, ignore_index=True)
    logger.info("Stacked %d CSV(s); total rows: %d", len(frames), len(df_all))
    return df_all


def enforce_schema(
    df: pd.DataFrame, columns_in_order: list[str], numeric_cols: list[str]
) -> pd.DataFrame:
    for c in columns_in_order:
        if c not in df.columns:
            df[c] = pd.NA
    df = df[columns_in_order]
    for c in numeric_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df


def write_temp_csv(df: pd.DataFrame, columns_in_order: list[str]) -> Path:
    tmpdir = Path(tempfile.mkdtemp(prefix="dp_util_"))
    out = tmpdir / "staged.csv"
    logger.info("Writing staged CSV: %s", out)
    df.to_csv(out, index=False, columns=columns_in_order, quoting=csv.QUOTE_MINIMAL)
    return out


# -----------------------------------------------------------------------------
# Upsert helper (expects copy_csv_to_table)
# -----------------------------------------------------------------------------


def load_mode_upsert(
    engine,
    table_name: str,
    csv_path: str,
    conflict_cols: list[str],
    COLUMNS_IN_ORDER: list[str],
    NUMERIC_COLS: list[str] | None = None,
):
    stage = f"{table_name}_stage"
    logger.info("CREATE TEMP TABLE %s LIKE %s", stage, table_name)
    col_list = ", ".join(f'"{c}"' for c in COLUMNS_IN_ORDER)
    ins_cols = ", ".join(f'"{c}"' for c in COLUMNS_IN_ORDER)
    conflict_cols_sql = ", ".join(f'"{c}"' for c in conflict_cols)
    update_cols = [c for c in COLUMNS_IN_ORDER if c not in conflict_cols]
    update_set = ", ".join([f'"{c}" = EXCLUDED."{c}"' for c in update_cols])

    if not HAVE_DB_COPY or copy_csv_to_table is None:
        raise RuntimeError(
            "copy_csv_to_table helper is required but not available (db_utils or ingest module)."
        )

    conn = engine.raw_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                f'CREATE TEMP TABLE "{stage}" (LIKE "public"."{table_name}" INCLUDING ALL) ON COMMIT DROP'
            )
        logger.info("COPY into %s from %s", stage, csv_path)
        copy_csv_to_table(
            conn,
            stage,
            csv_path,
            COLUMNS_IN_ORDER,
            force_null_extra=(NUMERIC_COLS or []),
            schema=None,
        )
        with conn.cursor() as cur:
            logger.info(
                "UPSERT from %s -> %s ON CONFLICT (%s)", stage, table_name, ", ".join(conflict_cols)
            )
            cur.execute(
                f'INSERT INTO "public"."{table_name}" ({ins_cols}) '
                f'SELECT {col_list} FROM "{stage}" '
                f"ON CONFLICT ({conflict_cols_sql}) DO UPDATE SET {update_set}"
            )
        conn.commit()
    finally:
        conn.close()


# -----------------------------------------------------------------------------
# S3 helpers (delegating to s3_utils)
# -----------------------------------------------------------------------------


def download_keys_via_s3_utils(bucket: str, keys: list[str], dest_dir: Path) -> list[Path]:
    if download_many is not None:
        dest_dir.mkdir(parents=True, exist_ok=True)
        logger.info("Downloading %d key(s) via s3_utils.download_many", len(keys))
        return download_many(bucket, keys, dest_dir, overwrite=True)
    if download_from_s3 is None:
        raise RuntimeError("s3_utils download helpers not available.")
    dests: list[Path] = []
    for k in keys:
        local = dest_dir / Path(k).name
        dest_dir.mkdir(parents=True, exist_ok=True)
        logger.info("Downloading (via s3_utils.single) s3://%s/%s -> %s", bucket, k, local)
        download_from_s3(bucket, k, str(local), overwrite=True)
        dests.append(local)
    return dests


# -----------------------------------------------------------------------------
# Public API: process_dataset (+ age-filter variant)
# -----------------------------------------------------------------------------


def process_dataset(
    engine,
    cfg: DatasetConfig,
    *,
    local_csvs: list[Path] | None = None,
    s3_keys: list[str] | None = None,
) -> Path:
    """Download→(stack)→clean→enforce schema→stage CSV→upsert.
    Returns the path to the staged CSV written to disk.
    """
    if not cfg.columns_in_order:
        raise ValueError(f"columns_in_order must be provided for {cfg.name}")

    if local_csvs is None:
        if not cfg.s3_bucket:
            raise ValueError("Either local_csvs must be passed, or s3_bucket must be set.")
        tmpdir = Path(tempfile.mkdtemp(prefix="csvs_"))
        if s3_keys:
            paths = download_keys_via_s3_utils(cfg.s3_bucket, s3_keys, tmpdir)
        else:
            if not cfg.s3_prefix:
                raise ValueError("s3_prefix is required when --s3-keys is not provided.")
            if list_keys is None:
                raise RuntimeError(
                    "s3_utils.list_keys not available; provide --s3-keys or add list_keys to s3_utils."
                )
            keys = list_keys(cfg.s3_bucket, cfg.s3_prefix, cfg.file_regex)
            paths = download_keys_via_s3_utils(cfg.s3_bucket, keys, tmpdir)
    else:
        paths = local_csvs

    df = load_and_clean_csvs(paths, cfg.cleaner)
    df = enforce_schema(df, cfg.columns_in_order, cfg.numeric_cols)
    staged = write_temp_csv(df, cfg.columns_in_order)

    logger.info("Upserting %s rows into %s", len(df), cfg.table_name)
    load_mode_upsert(
        engine=engine,
        table_name=cfg.table_name,
        csv_path=str(staged),
        conflict_cols=cfg.conflict_cols,
        COLUMNS_IN_ORDER=cfg.columns_in_order,
        NUMERIC_COLS=cfg.numeric_cols,
    )
    return staged


def process_dataset_age_filtered(
    engine,
    cfg: DatasetConfig,
    *,
    age_filter: dict | None = None,
    local_csvs: list[Path] | None = None,
    s3_keys: list[str] | None = None,
) -> Path:
    if local_csvs is None:
        if not cfg.s3_bucket:
            raise ValueError("Either local_csvs must be passed, or s3_bucket must be set.")
        tmpdir = Path(tempfile.mkdtemp(prefix="csvs_"))
        if s3_keys:
            paths = download_keys_via_s3_utils(cfg.s3_bucket, s3_keys, tmpdir)
        else:
            if not cfg.s3_prefix:
                raise ValueError("s3_prefix is required when --s3-keys is not provided.")
            if list_keys is None:
                raise RuntimeError(
                    "s3_utils.list_keys not available; provide --s3-keys or add list_keys to s3_utils."
                )
            keys = list_keys(cfg.s3_bucket, cfg.s3_prefix, cfg.file_regex)
            paths = download_keys_via_s3_utils(cfg.s3_bucket, keys, tmpdir)
    else:
        paths = local_csvs

    df = load_and_clean_csvs(paths, cfg.cleaner)

    if age_filter:
        group_cols = age_filter.get("group_cols", ["player"])  # domain default
        age_col = age_filter.get("age_col", "age")
        min_age = age_filter.get("min_age")
        max_age = age_filter.get("max_age")
        min_prefix = age_filter.get("min_prefix", 1)
        min_suffix = age_filter.get("min_suffix", 1)
        require_contiguous = age_filter.get("require_contiguous", False)
        keep_longest_run = age_filter.get("keep_longest_run", False)

        df = filter_by_age_with_prefix_suffix(
            df,
            group_cols=group_cols,
            age_col=age_col,
            min_age=min_age,
            max_age=max_age,
            min_prefix=min_prefix,
            min_suffix=min_suffix,
            require_contiguous=require_contiguous,
            keep_longest_run=keep_longest_run,
        )

    df = enforce_schema(df, cfg.columns_in_order, cfg.numeric_cols)
    staged = write_temp_csv(df, cfg.columns_in_order)

    logger.info("Upserting %s rows into %s (age-filtered)", len(df), cfg.table_name)
    load_mode_upsert(
        engine=engine,
        table_name=cfg.table_name,
        csv_path=str(staged),
        conflict_cols=cfg.conflict_cols,
        COLUMNS_IN_ORDER=cfg.columns_in_order,
        NUMERIC_COLS=cfg.numeric_cols,
    )
    return staged


# -----------------------------------------------------------------------------
# Prefix/Suffix age filter (generic)
# -----------------------------------------------------------------------------


def filter_by_age_with_prefix_suffix(
    df: pd.DataFrame,
    *,
    group_cols: list[str] = ["player"],  # noqa: B006
    age_col: str = "age",
    min_age: int | None = None,
    max_age: int | None = None,
    min_prefix: int = 1,
    min_suffix: int = 1,
    require_contiguous: bool = False,
    keep_longest_run: bool = False,
) -> pd.DataFrame:
    if age_col not in df.columns:
        logger.warning("Age column '%s' not found; skipping age filter.", age_col)
        return df

    work = df.copy()
    base_mask = pd.Series(True, index=work.index)
    if min_age is not None:
        base_mask &= work[age_col].ge(min_age)
    if max_age is not None:
        base_mask &= work[age_col].le(max_age)
    work["_valid"] = base_mask

    def _apply_group(g: pd.DataFrame) -> pd.DataFrame:
        g = g.sort_values(age_col, kind="mergesort").reset_index()
        valid = g["_valid"].to_numpy()
        prefix = valid.cumsum() - valid
        suffix = (valid[::-1].cumsum() - valid[::-1])[::-1]
        keep = valid.copy()
        if min_prefix > 0:
            keep &= prefix >= min_prefix
        if min_suffix > 0:
            keep &= suffix >= min_suffix
        if require_contiguous or keep_longest_run:
            age_vals = g[age_col].to_numpy()
            run_id = (pd.Series(age_vals).diff().ne(1)).cumsum().to_numpy()
            if require_contiguous:
                run_ok = pd.Series(keep).groupby(run_id).transform("all").to_numpy()
                keep &= run_ok
            if keep_longest_run:
                run_lengths = pd.Series(keep).groupby(run_id).transform("sum")
                max_len = run_lengths.max() if len(run_lengths) else 0
                keep &= (run_lengths == max_len).to_numpy()
        g["_keep"] = keep
        return g.set_index("index")

    work = work.groupby(group_cols, dropna=False, group_keys=False).apply(_apply_group)
    out = work[work["_keep"]].drop(columns=["_valid", "_keep"])
    logger.info("Age filter reduced rows from %d to %d", len(df), len(out))
    return out


# -----------------------------------------------------------------------------
# CLI (optional lightweight runner for generic steps)
# -----------------------------------------------------------------------------


def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Generic CSV→Pandas→DB pipeline (core)")
    p.add_argument(
        "--engine-url",
        required=False,
        help="Optional SQLAlchemy URL (else env via db_utils.get_db_engine())",
    )
    p.add_argument("--dataset", default=None)
    p.add_argument("--s3-keys", default=None)
    return p

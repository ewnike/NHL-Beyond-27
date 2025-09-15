"""
Build the 25–29 alignment table in Python from Postgres and write it back.

What this script does
---------------------
1) Loads `public.player_peak_season_one_row` into a pandas DataFrame.
2) Derives `start_year` from the `season` string.
3) Normalizes `time_on_ice` into seconds, robust to "MM:SS", "HH:MM:SS", or numeric.
4) If TOI looks per-game (<=3600s) and a Games Played column exists, multiplies TOI by GP to get season totals.
5) Keeps only ages 25..29 for players who have all five ages.
6) Aligns at age 27 (rel_age = age - 27), computes each player’s `peak_year` (start_year at age 27), and
   their 5-year average season TOI in minutes.
7) Writes the result to Postgres as `public.player_age25_29_aligned` (replace if exists).

Usage
-----
- Set DATABASE_URL env var (e.g., postgresql+psycopg2://user:pass@host:5432/dbname)
- Run: python align_age25_29_python.py

Notes
-----
- If your GP column has a custom name, add it to GP_CANDIDATES below or set GP_COLUMN env var.
- If your `time_on_ice` is already a season total (in seconds), the GP scaling step will be skipped
  as it won’t look like a per-game value.
"""
from __future__ import annotations

import os
import re
import logging
from typing import Optional, Sequence

import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.types import Integer, Float, Text

# ---------- Config ----------
SOURCE_SCHEMA = os.getenv("SOURCE_SCHEMA", "public")
SOURCE_TABLE = os.getenv("SOURCE_TABLE", "player_peak_season_one_row")
TARGET_SCHEMA = os.getenv("TARGET_SCHEMA", "public")
TARGET_TABLE = os.getenv("TARGET_TABLE", "player_age25_29_aligned")
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql+psycopg2://user:pass@localhost:5432/dbname")
# If you know the exact GP column, set GP_COLUMN; else we'll detect from GP_CANDIDATES
GP_COLUMN = os.getenv("GP_COLUMN")
GP_CANDIDATES: Sequence[str] = ("gp", "games_played", "gp_total", "games", "GP", "GamesPlayed")

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)


# ---------- Helpers ----------

def get_engine() -> Engine:
    """Create a SQLAlchemy engine from DATABASE_URL."""
    return create_engine(DATABASE_URL)


def parse_start_year(season: Optional[str]) -> Optional[int]:
    """Derive season start year from common formats: '19-20', '2019-20', '20192020', '2019'."""
    if season is None:
        return None
    s = str(season)
    try:
        if re.fullmatch(r"\d{2}-\d{2}", s):
            return 2000 + int(s[:2])
        if re.fullmatch(r"\d{4}-\d{2}", s):
            return int(s[:4])
        if re.fullmatch(r"\d{8}", s):
            return int(s[:4])
        if re.fullmatch(r"\d{4}", s):
            return int(s)
    except Exception:
        return None
    return None


def toi_to_seconds(value) -> int:
    """Convert TOI values to seconds. Accepts 'MM:SS', 'HH:MM:SS', numeric strings, or ints.
    Returns 0 for None/NaN/unparseable.
    """
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return 0
    try:
        s = str(value)
        if ":" in s:
            parts = s.split(":")
            if len(parts) == 2:
                mm, ss = int(parts[0]), int(parts[1])
                return mm * 60 + ss
            if len(parts) == 3:
                hh, mm, ss = int(parts[0]), int(parts[1]), int(parts[2])
                return hh * 3600 + mm * 60 + ss
            return 0
        # numeric: assume seconds
        return int(float(s))
    except Exception:
        return 0


def find_gp_column(df: pd.DataFrame) -> Optional[str]:
    if GP_COLUMN and GP_COLUMN in df.columns:
        return GP_COLUMN
    for c in GP_CANDIDATES:
        if c in df.columns:
            return c
    return None


def build_aligned_df(engine: Engine) -> pd.DataFrame:
    logger.info("Loading source table %s.%s", SOURCE_SCHEMA, SOURCE_TABLE)
    src_sql = f"SELECT * FROM {SOURCE_SCHEMA}.{SOURCE_TABLE}"
    df = pd.read_sql_query(src_sql, engine)

    required_cols = {"player", "season", "age", "time_on_ice"}
    missing = required_cols - set(df.columns)
    if missing:
        raise RuntimeError(f"Missing required columns in source: {missing}")

    # Types & derived columns
    logger.info("Deriving start_year and normalizing TOI to seconds…")
    df["start_year"] = df["season"].apply(parse_start_year)
    df["age"] = pd.to_numeric(df["age"], errors="coerce").astype("Int64")
    df["toi_seconds_raw"] = df["time_on_ice"].apply(toi_to_seconds)

    gp_col = find_gp_column(df)
    if gp_col:
        logger.info("Detected GP column: %s", gp_col)
        df[gp_col] = pd.to_numeric(df[gp_col], errors="coerce")
    else:
        logger.info("No GP column detected; will not scale per-game TOI.")

    looks_per_game = df["toi_seconds_raw"] <= 3600
    if gp_col:
        df["season_toi_seconds"] = np.where(
            looks_per_game & df[gp_col].notna(),
            df["toi_seconds_raw"] * df[gp_col].fillna(0),
            df["toi_seconds_raw"],
        )
    else:
        df["season_toi_seconds"] = df["toi_seconds_raw"]

    # If duplicates exist (shouldn't for *_one_row), collapse to one row per player-season.
    gb_keys = ["player", "season", "start_year", "age"]
    if df.duplicated(gb_keys).any():
        logger.info("Collapsing duplicate player-season rows with SUM(season_toi_seconds)…")
        df = (
            df.groupby(gb_keys, as_index=False)["season_toi_seconds"].sum()
        )
    else:
        df = df[gb_keys + ["season_toi_seconds"]]

    # Filter to ages 25..29 and keep only players who have all five ages
    logger.info("Filtering to ages 25–29 and players with complete five-year windows…")
    df = df[(df["age"] >= 25) & (df["age"] <= 29)].copy()

    by_player = df.groupby("player", dropna=False)
    # players with exactly ages {25,26,27,28,29}
    mask_complete = (
        (by_player["age"].transform("nunique") == 5)
        & (by_player["age"].transform("min") == 25)
        & (by_player["age"].transform("max") == 29)
    )
    df = df[mask_complete].copy()

    # Alignment fields
    df["rel_age"] = df["age"] - 27
    peak_year_map = df.loc[df["age"] == 27, ["player", "start_year"]].drop_duplicates().set_index("player")["start_year"]
    df["peak_year"] = df["player"].map(peak_year_map).astype("Int64")

    # 5-year average season TOI (seconds) per player
    df["avg_toi_5yr_seconds"] = df.groupby("player")["season_toi_seconds"].transform("mean")

    # Final formatting
    df_out = df.copy()
    df_out["avg_toi_5yr_minutes_per_season"] = (df_out["avg_toi_5yr_seconds"] / 60.0).round(1)
    df_out["season_toi_minutes"] = (df_out["season_toi_seconds"] / 60.0).round(1)

    cols = [
        "player",
        "peak_year",
        "rel_age",
        "start_year",
        "season",
        "age",
        "avg_toi_5yr_minutes_per_season",
        "season_toi_minutes",
    ]
    df_out = df_out[cols].sort_values(["player", "rel_age"], kind="mergesort").reset_index(drop=True)

    logger.info("Players: %d | Rows: %d", df_out["player"].nunique(), len(df_out))
    return df_out


def write_to_db(df: pd.DataFrame, engine: Engine) -> None:
    logger.info("Writing to %s.%s (replace)…", TARGET_SCHEMA, TARGET_TABLE)
    df.to_sql(
        TARGET_TABLE,
        engine,
        schema=TARGET_SCHEMA,
        if_exists="replace",
        index=False,
        method="multi",
        dtype={
            "player": Text(),
            "peak_year": Integer(),
            "rel_age": Integer(),
            "start_year": Integer(),
            "season": Text(),
            "age": Integer(),
            "avg_toi_5yr_minutes_per_season": Float(),
            "season_toi_minutes": Float(),
        },
    )
    # Add a helpful composite index (can’t add PK via to_sql)
    with engine.begin() as conn:
        conn.execute(text(
            f"""
            DO $$ BEGIN
              IF NOT EXISTS (
                SELECT 1 FROM pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE c.relname = '{TARGET_TABLE}_player_peak_rel_idx'
                  AND n.nspname = '{TARGET_SCHEMA}'
              ) THEN
                CREATE INDEX {TARGET_TABLE}_player_peak_rel_idx
                  ON {TARGET_SCHEMA}.{TARGET_TABLE} (player, peak_year, rel_age);
              END IF;
            END $$;
            """
        ))


def main() -> None:
    engine = get_engine()
    df = build_aligned_df(engine)
    write_to_db(df, engine)

    # Sanity prints
    print("\nSanity checks:")
    print("Players:", df["player"].nunique())
    bad = df.groupby("player").size()
    bad = bad[bad != 5]
    if not bad.empty:
        print("Players without exactly 5 rows:")
        print(bad.head(20))
    print(df[["avg_toi_5yr_minutes_per_season"]].describe())


if __name__ == "__main__":
    main()

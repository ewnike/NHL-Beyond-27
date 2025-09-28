"""
build_player_five_year_aligned_z.py

Compute a static z-score snapshot table from player_five_year_aligned.

- spicy defines how "hot" (or not) a player's current season is compared to their 5-year baseline.
- CF60 is already TOI-normalized (per-60 rate).
- cf60_z (and cf_pct_z, ca60_z) are standardized **within the player** (centered & scaled by that player’s own mean/std).

This answers: “Relative to this same player’s five-year baseline, is this season hot or cold?”
It does not compare to league/season or rel-age cohorts—that’s intentional for within-player heat.

Author: Eric Winiecke
Date: September, 2025.
"""

import argparse
import logging

from db_utils import (
    create_player_five_year_aligned_z_table,  # your factory (make sure it has 'position')
    create_table,
    get_db_engine,
    get_metadata,
)
from sqlalchemy import MetaData, text

# ---- logging (reuse your project logger if present)
try:
    from log_utils import setup_logger

    setup_logger()
except Exception:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
    )
logger = logging.getLogger(__name__)

Z_TABLE = "player_five_year_aligned_z"
SRC_TABLE = "player_five_year_aligned"

Z_SELECT = f"""
WITH base AS (
  SELECT
    a.*,
    (cf_pct)::numeric AS cf_pct_num,
    (cf60)::numeric   AS cf60_num,
    (ca60)::numeric   AS ca60_num,
    -- per-player means/stds
    AVG((cf_pct)::numeric)       OVER (PARTITION BY player) AS avg_cf_pct,
    STDDEV_SAMP((cf_pct)::numeric) OVER (PARTITION BY player) AS std_cf_pct,
    AVG((cf60)::numeric)         OVER (PARTITION BY player) AS avg_cf60,
    STDDEV_SAMP((cf60)::numeric)   OVER (PARTITION BY player) AS std_cf60,
    AVG((ca60)::numeric)         OVER (PARTITION BY player) AS avg_ca60,
    STDDEV_SAMP((ca60)::numeric)   OVER (PARTITION BY player) AS std_ca60
  FROM public.{SRC_TABLE} a
),
z AS (
  SELECT
    player,
    position,
    (peak_year + 1)       AS peak_year,        -- shift to season end-year
    rel_age,
    start_year,
    season,
    age,
    cf_pct_num            AS cf_pct,
    cf60_num              AS cf60,
    ca60_num              AS ca60,

    CASE WHEN std_cf_pct IS NULL OR std_cf_pct = 0 THEN NULL
         ELSE (cf_pct_num - avg_cf_pct)/std_cf_pct
    END AS cf_pct_z,
    CASE WHEN std_cf60 IS NULL  OR std_cf60  = 0 THEN NULL
         ELSE (cf60_num  - avg_cf60 )/std_cf60
    END AS cf60_z,
    CASE WHEN std_ca60 IS NULL  OR std_ca60  = 0 THEN NULL  -- we keep ca60_z as + (we'll subtract in composites)
         ELSE (ca60_num  - avg_ca60 )/std_ca60
    END AS ca60_z,

    -- equal-weight spicy (your existing logic)
    (
      COALESCE(CASE WHEN std_cf_pct IS NULL OR std_cf_pct=0 THEN NULL ELSE (cf_pct_num-avg_cf_pct)/std_cf_pct END, 0) +
      COALESCE(CASE WHEN std_cf60  IS NULL OR std_cf60 =0 THEN NULL ELSE (cf60_num -avg_cf60 )/std_cf60  END, 0) -
      COALESCE(CASE WHEN std_ca60  IS NULL OR std_ca60 =0 THEN NULL ELSE (ca60_num -avg_ca60 )/std_ca60  END, 0)
    )
    / NULLIF(
        (CASE WHEN std_cf_pct IS NULL OR std_cf_pct=0 THEN 0 ELSE 1 END) +
        (CASE WHEN std_cf60  IS NULL OR std_cf60 =0 THEN 0 ELSE 1 END) +
        (CASE WHEN std_ca60  IS NULL OR std_ca60 =0 THEN 0 ELSE 1 END),
        0
      ) AS spicy_score,

    -- role-aware weighted spicy (fixed weights; D vs everyone else)
    CASE
      WHEN position ILIKE 'D%' THEN
        (
          0.5 * (CASE WHEN std_cf_pct IS NULL OR std_cf_pct=0 THEN NULL ELSE (cf_pct_num-avg_cf_pct)/std_cf_pct END) +
          0.2 * (CASE WHEN std_cf60  IS NULL OR std_cf60 =0 THEN NULL ELSE (cf60_num -avg_cf60 )/std_cf60  END) -
          0.3 * (CASE WHEN std_ca60  IS NULL OR std_ca60 =0 THEN NULL ELSE (ca60_num -avg_ca60 )/std_ca60  END)
        )
        / NULLIF(
            (CASE WHEN std_cf_pct IS NULL OR std_cf_pct=0 THEN 0 ELSE 0.5 END) +
            (CASE WHEN std_cf60  IS NULL OR std_cf60 =0 THEN 0 ELSE 0.2 END) +
            (CASE WHEN std_ca60  IS NULL OR std_ca60 =0 THEN 0 ELSE 0.3 END), 0
          )
      ELSE
        (
          0.5 * (CASE WHEN std_cf_pct IS NULL OR std_cf_pct=0 THEN NULL ELSE (cf_pct_num-avg_cf_pct)/std_cf_pct END) +
          0.3 * (CASE WHEN std_cf60  IS NULL OR std_cf60 =0 THEN NULL ELSE (cf60_num -avg_cf60 )/std_cf60  END) -
          0.2 * (CASE WHEN std_ca60  IS NULL OR std_ca60 =0 THEN NULL ELSE (ca60_num -avg_ca60 )/std_ca60  END)
        )
        / NULLIF(
            (CASE WHEN std_cf_pct IS NULL OR std_cf_pct=0 THEN 0 ELSE 0.5 END) +
            (CASE WHEN std_cf60  IS NULL OR std_cf60 =0 THEN 0 ELSE 0.3 END) +
            (CASE WHEN std_ca60  IS NULL OR std_ca60 =0 THEN 0 ELSE 0.2 END), 0
          )
    END AS spicy_weighted
  FROM base
),
z_with_peak AS (
  SELECT
    z.*,
    -- baselines at rel_age=0 per (player, peak_year)
    MAX(CASE WHEN rel_age = 0 THEN cf_pct_z      END) OVER (PARTITION BY player, peak_year) AS cf_pct_z_peak,
    MAX(CASE WHEN rel_age = 0 THEN cf60_z        END) OVER (PARTITION BY player, peak_year) AS cf60_z_peak,
    MAX(CASE WHEN rel_age = 0 THEN ca60_z        END) OVER (PARTITION BY player, peak_year) AS ca60_z_peak,
    MAX(CASE WHEN rel_age = 0 THEN spicy_score   END) OVER (PARTITION BY player, peak_year) AS spicy_unw_peak,
    MAX(CASE WHEN rel_age = 0 THEN spicy_weighted END) OVER (PARTITION BY player, peak_year) AS spicy_w_peak
  FROM z
)
SELECT
  player, position, peak_year, rel_age, start_year, season, age,
  cf_pct, cf60, ca60,
  cf_pct_z, cf60_z, ca60_z,
  spicy_score,
  spicy_weighted,
  -- curvature vs peak (signed deltas): value_k - value_peak
  (CASE WHEN cf_pct_z_peak   IS NULL THEN NULL ELSE cf_pct_z    - cf_pct_z_peak   END) AS cf_pct_dz,
  (CASE WHEN cf60_z_peak     IS NULL THEN NULL ELSE cf60_z      - cf60_z_peak     END) AS cf60_dz,
  (CASE WHEN ca60_z_peak     IS NULL THEN NULL ELSE ca60_z      - ca60_z_peak     END) AS ca60_dz,
  (CASE WHEN spicy_unw_peak  IS NULL THEN NULL ELSE spicy_score - spicy_unw_peak  END) AS spicy_unw_dz,
  (CASE WHEN spicy_w_peak    IS NULL THEN NULL ELSE spicy_weighted - spicy_w_peak END) AS spicy_w_dz
FROM z_with_peak
"""


def parse_args(argv=None):
    """Make functiion for creating, updating, and recreating table."""
    p = argparse.ArgumentParser(
        description="Build static z-score table from player_five_year_aligned"
    )
    p.add_argument(
        "--mode",
        choices=["upsert", "replace", "recreate"],
        default="upsert",
        help="upsert merges by PK; replace truncates then inserts; recreate drops table then rebuilds",
    )
    return p.parse_args(argv)


def ensure_z_table(engine, metadata):
    """Create the z table if missing; ensure it has 'position'."""
    z_tbl = create_player_five_year_aligned_z_table(
        Z_TABLE, metadata
    )  # your factory should include 'position'
    create_table(engine, metadata, z_tbl)
    # safety: if the physical table pre-existed without 'position', add it
    with engine.begin() as conn:
        conn.exec_driver_sql(
            """
          ALTER TABLE public.player_five_year_aligned_z
          ADD COLUMN IF NOT EXISTS position text
        """
        )


def build_z_recreate(engine, metadata):
    """
    Drop the z table, recreate with the latest schema (from db_utils factory),
    then INSERT fresh rows using the extended Z_SELECT.
    """
    # Drop if exists
    with engine.begin() as conn:
        conn.exec_driver_sql(f"DROP TABLE IF EXISTS public.{Z_TABLE} CASCADE")

    # Recreate schema from your factory (make sure factory includes new cols)
    z_tbl = create_player_five_year_aligned_z_table(Z_TABLE, metadata)
    create_table(engine, metadata, z_tbl)

    # Insert fresh data
    sql = f"""
    INSERT INTO public.{Z_TABLE}
      (player, position, peak_year, rel_age, start_year, season, age,
       cf_pct, cf60, ca60,
       cf_pct_z, cf60_z, ca60_z,
       spicy_score, spicy_weighted,
       cf_pct_dz, cf60_dz, ca60_dz, spicy_unw_dz, spicy_w_dz)
    {Z_SELECT}
    """
    with engine.begin() as conn:
        conn.execute(text(sql))


def build_z_replace(engine):
    """Snapshot rebuild: TRUNCATE then INSERT."""
    sql = f"""
    TRUNCATE TABLE public.{Z_TABLE};
    INSERT INTO public.{Z_TABLE}
      (player, position, peak_year, rel_age, start_year, season, age,
       cf_pct_z, cf60_z, ca60_z, spicy_score, spicy_weighted,
       cf_pct_dz, cf60_dz, ca60_dz, spicy_unw_dz, spicy_w_dz)
    {Z_SELECT}
    """
    with engine.begin() as conn:
        conn.exec_driver_sql(sql)


def build_z_upsert(engine):
    """Idempotent rebuild: INSERT .. ON CONFLICT DO UPDATE (keeps PK = (player, peak_year, rel_age))."""
    sql = f"""
    INSERT INTO public.{Z_TABLE}
      (player, position, peak_year, rel_age, start_year, season, age,
       cf_pct, cf60, ca60, cf_pct_z, cf60_z, ca60_z, spicy_score, spicy_weighted,
       cf_pct_dz, cf60_dz, ca60_dz, spicy_unw_dz, spicy_w_dz)
    {Z_SELECT}
    ON CONFLICT (player, peak_year, rel_age) DO UPDATE SET
      position    = EXCLUDED.position,
      start_year  = EXCLUDED.start_year,
      season      = EXCLUDED.season,
      age         = EXCLUDED.age,
      cf_pct      = EXCLUDED.cf_pct,
      cf60        = EXCLUDED.cf60,
      ca60        = EXCLUDED.ca60,
      cf_pct_z    = EXCLUDED.cf_pct_z,
      cf60_z      = EXCLUDED.cf60_z,
      ca60_z      = EXCLUDED.ca60_z,
      spicy_score = EXCLUDED.spicy_score,
      spicy_weighted = EXCLUDED.spicy_weighted,
      cf_pct_dz = EXCLUDED.cf_pct_dz,
      cf60_dz = EXCLUDED.cf60_dz,
      ca60_dz = EXCLUDED.ca60_dz,
      spicy_unw_dz = EXCLUDED.spicy_unw_dz,
      spicy_w_dz = EXCLUDED.spicy_w_dz,
      created_at  = now();
    """
    with engine.begin() as conn:
        conn.exec_driver_sql(sql)


def main(mode: str = "upsert"):
    engine = get_db_engine()
    md = get_metadata()

    # ensure source exists...
    with engine.begin() as conn:
        n_src = conn.execute(text(f"SELECT COUNT(*) FROM public.{SRC_TABLE}")).scalar_one()
    if n_src == 0:
        raise RuntimeError(...)

    if mode == "replace":
        logging.info("[REPLACE] ...")
        ensure_z_table(engine, md)
        build_z_replace(engine)
    elif mode == "recreate":
        logging.info("[RECREATE] Dropping, creating, and inserting z-scores …")
        # fresh MetaData to avoid “already defined” error
        build_z_recreate(engine, MetaData())
    else:
        logging.info("[UPSERT] ...")
        ensure_z_table(engine, md)
        build_z_upsert(engine)


def cli():
    args = parse_args()
    main(mode=args.mode)


if __name__ == "__main__":
    cli()
    """
    Call function from the command line
    using the following arguements:
    python build_player_five_year_aligned_z.py --mode upsert
    python build_player_five_year_aligned_z.py --mode replace
    python build_player_five_year_aligned_z.py --mode recreate
    """

"""
build_player_five_year_aligned_z.py
Compute a static z-score snapshot table from player_five_year_aligned.
spicy defines how "hot" (or not) a player's current season is compared there 5 year baseline.
CF60 itself is already TOI-normalized (per-60 rate).
cf60_z is standardized within the player (centered & scaled by that player’s own mean/std). It answers “relative to this player’s baseline, was this season high or low?”
"""

import logging
from sqlalchemy import text

from db_utils import (
    get_db_engine, get_metadata,
    create_player_five_year_aligned_z_table,  # your factory (make sure it has 'position')
    create_table,
)

# ---- logging (reuse your project logger if present)
try:
    from log_utils import setup_logger
    setup_logger()
except Exception:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(name)s - %(message)s")
logger = logging.getLogger(__name__)

Z_TABLE = "player_five_year_aligned_z"
SRC_TABLE = "player_five_year_aligned"

# Core SELECT that produces the z-scored rows from the source table
Z_SELECT = f"""
WITH base AS (
  SELECT
    a.*,
    AVG(cf_pct)         OVER (PARTITION BY player) AS avg_cf_pct,
    STDDEV_SAMP(cf_pct) OVER (PARTITION BY player) AS std_cf_pct,
    AVG(cf60)           OVER (PARTITION BY player) AS avg_cf60,
    STDDEV_SAMP(cf60)   OVER (PARTITION BY player) AS std_cf60,
    AVG(ca60)           OVER (PARTITION BY player) AS avg_ca60,
    STDDEV_SAMP(ca60)   OVER (PARTITION BY player) AS std_ca60
  FROM public.{SRC_TABLE} a
)
SELECT
  player, position, peak_year, rel_age, start_year, season, age,
  cf_pct, cf60, ca60,

  /* per-player z-scores (NULL if stddev=0/NULL) */
  CASE WHEN std_cf_pct IS NULL OR std_cf_pct=0 THEN NULL ELSE (cf_pct-avg_cf_pct)/std_cf_pct END AS cf_pct_z,
  CASE WHEN std_cf60  IS NULL OR std_cf60 =0 THEN NULL ELSE (cf60 -avg_cf60 )/std_cf60  END AS cf60_z,
  CASE WHEN std_ca60  IS NULL OR std_ca60 =0 THEN NULL ELSE (ca60 -avg_ca60 )/std_ca60  END AS ca60_z,

  /* spicy_score = mean of available components: cf_pct_z + cf60_z - ca60_z */
  (
    COALESCE(CASE WHEN std_cf_pct IS NULL OR std_cf_pct=0 THEN NULL ELSE (cf_pct-avg_cf_pct)/std_cf_pct END, 0) +
    COALESCE(CASE WHEN std_cf60  IS NULL OR std_cf60 =0 THEN NULL ELSE (cf60 -avg_cf60 )/std_cf60  END, 0) -
    COALESCE(CASE WHEN std_ca60  IS NULL OR std_ca60 =0 THEN NULL ELSE (ca60 -avg_ca60 )/std_ca60  END, 0)
  )
  / NULLIF(
      (CASE WHEN std_cf_pct IS NULL OR std_cf_pct=0 THEN 0 ELSE 1 END) +
      (CASE WHEN std_cf60  IS NULL OR std_cf60 =0 THEN 0 ELSE 1 END) +
      (CASE WHEN std_ca60  IS NULL OR std_ca60 =0 THEN 0 ELSE 1 END),
      0
    )  AS spicy_score
FROM base
"""

def ensure_z_table(engine, metadata):
    """Create the z table if missing; ensure it has 'position'."""
    z_tbl = create_player_five_year_aligned_z_table(Z_TABLE, metadata)  # your factory should include 'position'
    create_table(engine, metadata, z_tbl)
    # safety: if the physical table pre-existed without 'position', add it
    with engine.begin() as conn:
        conn.exec_driver_sql("""
          ALTER TABLE public.player_five_year_aligned_z
          ADD COLUMN IF NOT EXISTS position text
        """)

def build_z_replace(engine):
    """Snapshot rebuild: TRUNCATE then INSERT."""
    sql = f"""
    TRUNCATE TABLE public.{Z_TABLE};
    INSERT INTO public.{Z_TABLE}
      (player, position, peak_year, rel_age, start_year, season, age,
       cf_pct, cf60, ca60, cf_pct_z, cf60_z, ca60_z, spicy_score)
    {Z_SELECT}
    """
    with engine.begin() as conn:
        conn.exec_driver_sql(sql)

def build_z_upsert(engine):
    """Idempotent rebuild: INSERT .. ON CONFLICT DO UPDATE (keeps PK = (player, peak_year, rel_age))."""
    sql = f"""
    INSERT INTO public.{Z_TABLE}
      (player, position, peak_year, rel_age, start_year, season, age,
       cf_pct, cf60, ca60, cf_pct_z, cf60_z, ca60_z, spicy_score)
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
      created_at  = now();
    """
    with engine.begin() as conn:
        conn.exec_driver_sql(sql)

def main(mode: str = "upsert"):
    engine = get_db_engine()
    md = get_metadata()

    # ensure source exists & has rows
    with engine.begin() as conn:
        n_src = conn.execute(text(f"SELECT COUNT(*) FROM public.{SRC_TABLE}")).scalar_one()
    if n_src == 0:
        raise RuntimeError(f"Source table public.{SRC_TABLE} is empty. Build it first.")

    # ensure destination schema
    ensure_z_table(engine, md)

    if mode == "replace":
        logging.info("[REPLACE] Truncating and inserting z-scores …")
        build_z_replace(engine)
    else:
        logging.info("[UPSERT] Upserting z-scores …")
        build_z_upsert(engine)

    # quick sanity
    with engine.begin() as conn:
        n = conn.execute(text(f"SELECT COUNT(*) FROM public.{Z_TABLE}")).scalar_one()
    print(f"{Z_TABLE} rows: {n}")

if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser(description="Build static z-score table from player_five_year_aligned")
    p.add_argument("--mode", choices=["upsert","replace"], default="upsert",
                   help="upsert merges by PK; replace truncates then inserts")
    args = p.parse_args()
    main(mode=args.mode)

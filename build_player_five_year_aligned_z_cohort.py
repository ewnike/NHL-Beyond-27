"""
build_player_five_year_aligned_z_cohort.py
Compute cohort-relative (by rel_age) z-scores into a static table.
"""

import logging
from sqlalchemy import text
from db_utils import (
    get_db_engine, get_metadata, create_table,
    create_player_five_year_aligned_z_cohort_table,
)

try:
    from log_utils import setup_logger
    setup_logger()
except Exception:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(name)s - %(message)s")
logger = logging.getLogger(__name__)

SRC_TABLE = "player_five_year_aligned"
DST_TABLE = "player_five_year_aligned_z_cohort"

# --- Cohort (by rel_age) standardization ---
Z_SELECT_COHORT = f"""
WITH base AS (
  SELECT
    a.player,
    a.peak_year,
    a.rel_age,
    a.start_year,
    a.season,
    a.age,
    COALESCE(a.position, v.position) AS position,
    a.cf_pct,
    a.cf60,
    a.ca60,
    AVG(a.cf_pct)   OVER (PARTITION BY a.rel_age) AS mu_cf_pct,
    STDDEV_SAMP(a.cf_pct) OVER (PARTITION BY a.rel_age) AS sd_cf_pct,
    AVG(a.cf60)     OVER (PARTITION BY a.rel_age) AS mu_cf60,
    STDDEV_SAMP(a.cf60)   OVER (PARTITION BY a.rel_age) AS sd_cf60,
    AVG(a.ca60)     OVER (PARTITION BY a.rel_age) AS mu_ca60,
    STDDEV_SAMP(a.ca60)   OVER (PARTITION BY a.rel_age) AS sd_ca60
  FROM public.{SRC_TABLE} a
  LEFT JOIN public.player_peak_season_one_row v
    ON v.player = a.player AND v.season = a.season
)
SELECT
  player, position, peak_year, rel_age, start_year, season, age,
  cf_pct, cf60, ca60,
  CASE WHEN sd_cf_pct IS NULL OR sd_cf_pct=0 THEN NULL ELSE (cf_pct - mu_cf_pct)/sd_cf_pct END AS cf_pct_z,
  CASE WHEN sd_cf60  IS NULL OR sd_cf60 =0 THEN NULL ELSE (cf60  - mu_cf60 )/sd_cf60  END AS cf60_z,
  CASE WHEN sd_ca60  IS NULL OR sd_ca60 =0 THEN NULL ELSE (ca60  - mu_ca60 )/sd_ca60  END AS ca60_z,
  (
    COALESCE(CASE WHEN sd_cf_pct IS NULL OR sd_cf_pct=0 THEN NULL ELSE (cf_pct - mu_cf_pct)/sd_cf_pct END, 0) +
    COALESCE(CASE WHEN sd_cf60  IS NULL OR sd_cf60 =0 THEN NULL ELSE (cf60  - mu_cf60 )/sd_cf60  END, 0) -
    COALESCE(CASE WHEN sd_ca60  IS NULL OR sd_ca60 =0 THEN NULL ELSE (ca60  - mu_ca60 )/sd_ca60  END, 0)
  )
  / NULLIF(
      (CASE WHEN sd_cf_pct IS NULL OR sd_cf_pct=0 THEN 0 ELSE 1 END) +
      (CASE WHEN sd_cf60  IS NULL OR sd_cf60 =0 THEN 0 ELSE 1 END) +
      (CASE WHEN sd_ca60  IS NULL OR sd_ca60 =0 THEN 0 ELSE 1 END),
      0
    )  AS spicy_score
FROM base
"""

def ensure_table(engine, md):
    tbl = create_player_five_year_aligned_z_cohort_table(DST_TABLE, md)
    create_table(engine, md, tbl)  # create if missing

def build(mode: str = "upsert"):
    engine = get_db_engine()
    md = get_metadata()

    # ensure source exists and has rows
    with engine.begin() as conn:
        n_src = conn.execute(text(f"SELECT COUNT(*) FROM public.{SRC_TABLE}")).scalar_one()
        if n_src == 0:
            raise RuntimeError(f"Source public.{SRC_TABLE} is empty; build it first.")

    ensure_table(engine, md)

    if mode == "replace":
        sql = f"""
        TRUNCATE TABLE public.{DST_TABLE};
        INSERT INTO public.{DST_TABLE}
          (player, position, peak_year, rel_age, start_year, season, age,
           cf_pct, cf60, ca60, cf_pct_z, cf60_z, ca60_z, spicy_score)
        {Z_SELECT_COHORT}
        """
    else:
        sql = f"""
        INSERT INTO public.{DST_TABLE}
          (player, position, peak_year, rel_age, start_year, season, age,
           cf_pct, cf60, ca60, cf_pct_z, cf60_z, ca60_z, spicy_score)
        {Z_SELECT_COHORT}
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

    with engine.begin() as conn:
        n = conn.execute(text(f"SELECT COUNT(*) FROM public.{DST_TABLE}")).scalar_one()
    print(f"{DST_TABLE} rows: {n}")

if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser(description="Build cohort-relative z-score table from player_five_year_aligned")
    p.add_argument("--mode", choices=["upsert","replace"], default="upsert",
                   help="upsert merges by PK; replace truncates then inserts")
    args = p.parse_args()
    build(mode=args.mode)

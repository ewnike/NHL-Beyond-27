"""
ingest_peak_season.py.
Remove unused config_helpers; using direct ingest script
Authored by Eric Winiecke.
September 7, 2025.
"""

import os
from dotenv import load_dotenv
load_dotenv()  # load .env before other imports

from db_utils import (
    get_db_engine, get_metadata,
    define_player_peak_season, create_table
)
from s3_utils import download_from_s3
from constants import S3_BUCKET_NAME, local_download_path
from sqlalchemy import text

# logging setup (re-uses your project logger if available)
import logging
try:
    from log_utils import setup_logger  # you already use this elsewhere
    setup_logger()                      # idempotent in your project
except Exception:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
    )
logger = logging.getLogger(__name__)

# ---- config for THIS file ----
S3_KEY = "peak_player_season_stats.csv"
LOCAL_CSV_PATH = os.path.join(local_download_path, S3_KEY)
TABLE_NAME = "player_peak_season"

# Must match Postgres column names exactly (including punctuation)
COLUMNS_IN_ORDER = [
    "player", "eh_id", "api_id", "season", "team", "position", "shoots",
    "birthday", "age", "draft_year", "draft_rnd", "draft_overall", "games_played",
    "time_on_ice",
    "GF%", "SF%", "FF%", "CF%", "xGF%",
    "GF/60", "GA/60", "SF/60", "SA/60", "FF/60", "FA/60", "CF/60", "CA/60", "xGF/60", "xGA/60",
    "G+-/60", "S+-/60", "F+-/60", "C+-/60", "xG+-/60",
    "Sh%", "Sv%",
]

NUMERIC_COLS = [
    "time_on_ice",
    "GF%", "SF%", "FF%", "CF%", "xGF%",
    "GF/60","GA/60","SF/60","SA/60","FF/60","FA/60","CF/60","CA/60","xGF/60","xGA/60",
    "G+-/60","S+-/60","F+-/60","C+-/60","xG+-/60",
    "Sh%","Sv%",
]
INT_COLS = ['api_id','age','draft_year','draft_rnd','draft_overall','games_played']

def copy_csv_to_table(conn, table_name: str, csv_path: str,
                      columns: list[str], force_null_extra=(), schema: str | None = "public"):
    cols_sql = ", ".join(f'"{c}"' for c in columns)
    force_null = list(INT_COLS) + list(force_null_extra)
    force_null_sql = ", ".join(f'"{c}"' for c in force_null) if force_null else ""

    # build table ref correctly
    if schema:
        table_ref = f'"{schema}"."{table_name}"'
    else:
        table_ref = f'"{table_name}"'  # temp tables live in pg_temp; don't qualify with public

    copy_sql = (
        f'COPY {table_ref} ({cols_sql}) '
        "FROM STDIN WITH (FORMAT csv, HEADER true, NULL 'NA'"
        + (f", FORCE_NULL ({force_null_sql})" if force_null_sql else "")
        + ")"
    )
    with conn.cursor() as cur, open(csv_path, "r", encoding="utf-8", newline="") as f:
        cur.copy_expert(copy_sql, f)


def ensure_unique_index(engine, table_name: str, cols: list[str], name: str):
    """
    Create a UNIQUE index needed for ON CONFLICT.
    Use SQLAlchemy's transactional connection (context-manager friendly).
    """
    cols_sql = ", ".join(f'"{c}"' for c in cols)
    sql = f'CREATE UNIQUE INDEX IF NOT EXISTS "{name}" ON "public"."{table_name}" ({cols_sql})'
    # engine.begin() provides a Connection that *is* a context manager and auto-commits
    with engine.begin() as conn:
        conn.exec_driver_sql(sql)   # or: conn.execute(text(sql))


def load_mode_replace(engine, table_name: str, csv_path: str):
    logger.info("TRUNCATE %s", table_name)
    conn = engine.raw_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(f'TRUNCATE TABLE "public"."{table_name}"')
            logger.info("COPY into %s from %s", table_name, csv_path)
        copy_csv_to_table(conn, table_name, csv_path, COLUMNS_IN_ORDER,
                          force_null_extra=NUMERIC_COLS, schema="public")
        conn.commit()
    finally:
        conn.close()

def load_mode_upsert(engine, table_name: str, csv_path: str, conflict_cols: list[str]):
    stage = f"{table_name}_stage"
    logger.info("CREATE TEMP TABLE %s LIKE %s", stage, table_name)
    col_list = ", ".join(f'"{c}"' for c in COLUMNS_IN_ORDER)
    ins_cols = ", ".join(f'"{c}"' for c in COLUMNS_IN_ORDER)
    conflict_cols_sql = ", ".join(f'"{c}"' for c in conflict_cols)
    update_cols = [c for c in COLUMNS_IN_ORDER if c not in conflict_cols]
    update_set = ", ".join([f'"{c}" = EXCLUDED."{c}"' for c in update_cols])

    conn = engine.raw_connection()
    try:
        with conn.cursor() as cur:
            # TEMP lives in pg_temp; add ON COMMIT DROP for cleanup
            cur.execute(
                f'CREATE TEMP TABLE "{stage}" (LIKE "public"."{table_name}" INCLUDING ALL) ON COMMIT DROP'
            )
        logger.info("COPY into %s from %s", stage, csv_path)
        # <-- COPY into TEMP: schema=None so we don't prefix with public
        copy_csv_to_table(conn, stage, csv_path, COLUMNS_IN_ORDER,
                          force_null_extra=NUMERIC_COLS, schema=None)

        with conn.cursor() as cur:
            logger.info("UPSERT from %s -> %s ON CONFLICT (%s)", stage, table_name, ", ".join(conflict_cols))
            cur.execute(
                f'INSERT INTO "public"."{table_name}" ({ins_cols}) '
                f'SELECT {col_list} FROM "{stage}" '
                f'ON CONFLICT ({conflict_cols_sql}) DO UPDATE SET {update_set}'
            )
        conn.commit()
    finally:
        conn.close()

def main(mode: str = "upsert", conflict_key: str = "player_season_team"):
    # 1) Download CSV (skip if already present)
    os.makedirs(os.path.dirname(LOCAL_CSV_PATH), exist_ok=True)
    logger.info("Downloading %s from s3://%s to %s", S3_KEY, S3_BUCKET_NAME, LOCAL_CSV_PATH)
    print(f"Downloading s3://{S3_BUCKET_NAME}/{S3_KEY} -> {LOCAL_CSV_PATH}")
    download_from_s3(S3_BUCKET_NAME, S3_KEY, LOCAL_CSV_PATH, overwrite=False)

    # 2) Ensure table exists
    engine = get_db_engine()
    metadata = get_metadata()
    table = define_player_peak_season(metadata)
    logger.info("Creating/verifying table %s", TABLE_NAME)
    create_table(engine, metadata, table)

    # Determine conflict columns
    if conflict_key == "player_season_team":
        conflict_cols = ["player","season","team"]  # per-team rows are allowed (trades)
        index_name = "ux_pps_player_season_team"
    else:
        conflict_cols = ["player","season"]         # use only if you truly store 1 row per season
        index_name = "ux_pps_player_season"

    # Create UNIQUE index that matches ON CONFLICT target (required by Postgres)
    logger.info("Ensuring unique index on (%s)", ", ".join(conflict_cols))
    ensure_unique_index(engine, TABLE_NAME, conflict_cols, index_name)

    # 3) Load
    if mode == "replace":
        print(f"[REPLACE] Reloading {TABLE_NAME} from CSV …")
        logger.info("[REPLACE] Truncating and reloading %s from CSV", TABLE_NAME)
        load_mode_replace(engine, TABLE_NAME, LOCAL_CSV_PATH)
    else:
        print(f"[UPSERT on ({', '.join(conflict_cols)})] Merging CSV into {TABLE_NAME} …")
        logger.info("[UPSERT on %s] loading CSV -> stage -> %s", conflict_cols, TABLE_NAME)
        load_mode_upsert(engine, TABLE_NAME, LOCAL_CSV_PATH, conflict_cols)

    print("Done.")

if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser(description="Ingest peak season CSV into Postgres")
    p.add_argument("--mode", choices=["upsert","replace"], default="upsert",
                   help="upsert merges by key; replace truncates then loads")
    p.add_argument("--conflict-key", choices=["player_season_team","player_season"], default="player_season_team",
                   help="which uniqueness key to use for upsert")
    args = p.parse_args()
    main(mode=args.mode, conflict_key=args.conflict_key)

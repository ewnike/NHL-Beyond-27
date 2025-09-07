# ingest_peak_season.py
import os
from dotenv import load_dotenv
load_dotenv()  # <-- load .env into the process *before* other imports
from db_utils import get_db_engine, get_metadata, define_player_peak_season, create_table
from s3_utils import download_from_s3
from constants import S3_BUCKET_NAME, local_download_path

# ---- config for THIS file ----
S3_KEY = "peak_player_season_stats.csv"  # the key in your bucket
LOCAL_CSV_PATH = os.path.join(local_download_path, S3_KEY)  # where to save locally
TABLE_NAME = "player_peak_season"

# These must match your Postgres column names exactly (including punctuation).
# We quote them later, so % and / are fine.
COLUMNS_IN_ORDER = [
    "player", "eh_id", "api_id", "season", "team", "position", "shoots",
    "birthday", "age", "draft_year", "draft_rnd", "draft_overall", "games_played",
    "time_on_ice",
    "GF%", "SF%", "FF%", "CF%", "xGF%",
    "GF/60", "GA/60", "SF/60", "SA/60", "FF/60", "FA/60", "CF/60", "CA/60", "xGF/60", "xGA/60",
    "G+-/60", "S+-/60", "F+-/60", "C+-/60", "xG+-/60",
    "Sh%", "Sv%",
]


def copy_csv_to_postgres(engine, table_name: str, csv_path: str, columns: list[str]):
    cols_sql = ", ".join(f'"{c}"' for c in columns)

    # Columns that are integers—treat empty quoted fields as NULL too
    force_null_cols = ['api_id','age','draft_year','draft_rnd','draft_overall','games_played']
    force_null_sql = ", ".join(f'"{c}"' for c in force_null_cols)

    copy_sql = (
        f'COPY "{table_name}" ({cols_sql}) '
        f"FROM STDIN WITH (FORMAT csv, HEADER true, NULL 'NA', FORCE_NULL ({force_null_sql}))"
    )

    conn = engine.raw_connection()
    try:
        with conn.cursor() as cur, open(csv_path, "r", encoding="utf-8", newline="") as f:
            cur.copy_expert(copy_sql, f)
        conn.commit()
    finally:
        conn.close()



def main():
    # 1) Download CSV from S3 (skip if already present)
    os.makedirs(os.path.dirname(LOCAL_CSV_PATH), exist_ok=True)
    print(f"Downloading s3://{S3_BUCKET_NAME}/{S3_KEY} -> {LOCAL_CSV_PATH}")
    download_from_s3(S3_BUCKET_NAME, S3_KEY, LOCAL_CSV_PATH, overwrite=False)

    # 2) Get engine and ensure table exists
    engine = get_db_engine()
    metadata = get_metadata()
    table = define_player_peak_season(metadata)  # schema you already wrote
    create_table(engine, metadata, table)

    # 3) COPY into Postgres
    print(f"Loading CSV into table {TABLE_NAME} ...")
    copy_csv_to_postgres(engine, TABLE_NAME, LOCAL_CSV_PATH, COLUMNS_IN_ORDER)
    print("Done!")

if __name__ == "__main__":
    main()

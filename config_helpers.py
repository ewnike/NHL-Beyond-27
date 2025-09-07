"""
Helper function.

Author: Eric Winiecke
Date: September 7, 2025

"""
from __future__ import annotations
from typing import Callable

from constants import S3_BUCKET_NAME, local_download_path, local_extract_path
from db_utils import (
    define_player_peak_season,
    get_db_engine,
)

# 🔹 Get shared engine once
engine = get_db_engine()


# ------------------------------------------------------------------
# PANDAS dtype mappings keyed by table name (no dates here)
# ------------------------------------------------------------------
COLUMN_MAPPINGS: dict[str, dict[str, str]] = {
    # ----------------- player peak season --------------------
    "player_peak_season": {
        "player": "string",
        "eh_id": "string",
        "api_id": "Int64",
        "season": "string",
        "team": "string",
        "position": "string",
        "shoots": "string",
        # "birthday": handled via parse_dates (see note below)
        "age": "Int64",
        "draft_year": "Int64",
        "draft_rnd": "Int64",
        "draft_overall": "Int64",
        "games_played": "Int64",

        "time_on_ice": "Float64",

        "GF%": "Float64",
        "SF%": "Float64",
        "FF%": "Float64",
        "CF%": "Float64",
        "xGF%": "Float64",

        "GF/60": "Float64",
        "GA/60": "Float64",
        "SF/60": "Float64",
        "SA/60": "Float64",
        "FF/60": "Float64",
        "FA/60": "Float64",
        "CF/60": "Float64",
        "CA/60": "Float64",
        "xGF/60": "Float64",
        "xGA/60": "Float64",

        "G+-/60": "Float64",
        "S+-/60": "Float64",
        "F+-/60": "Float64",
        "C+-/60": "Float64",
        "xG+-/60": "Float64",

        "Sh%": "Float64",
        "Sv%": "Float64",
    },
}


# Which columns should be parsed as datetimes per table
DATE_COLS: dict[str, list[str]] = {
    "player_peak_season": ["birthday"],
}


# pylint: disable=too-many-arguments
def build_processing_config(
    *,
    bucket_name: str,
    s3_file_key: str,
    table_definition_function: Callable,
    table_name: str,
    column_mapping: dict[str,str],
    engine,
    expected_csv_filename: str | None = None,
    is_zip: bool | None = None,
    local_download_path: str | None = None,
    local_extract_path: str | None = None,
    date_cols: list[str] | None = None,

) -> dict:
    """
    Build a standardized config dictionary for S3 extraction and data processing.

    Args:
    ----
        bucket_name (str): Name of the S3 bucket.
        s3_file_key (str): Key to the ZIP file in the S3 bucket.
        local_zip_path (str): Local path to download the ZIP file.
        local_extract_path (str): Local path to extract the CSV contents.
        expected_csv_filename (str): Name of the expected CSV file inside the ZIP.
        table_definition_function (Callable): SQLAlchemy function to define the table.
        table_name (str): Name of the PostgreSQL table to insert into.
        column_mapping (dict): Column names and types for cleaning.
        engine (sqlalchemy.Engine): SQLAlchemy engine instance for database connection.
        local_download_path (str): Directory for downloading the ZIP file.

    Returns:
    -------
        dict: Config dictionary with all values needed for process_and_insert_data().
    """
    if is_zip is None:
        is_zip = s3_file_key.lower().endswith(".zip")

    return {
        "bucket_name": bucket_name,
        "s3_file_key": s3_file_key,
        "is_zip": is_zip,
        "local_download_path": local_download_path,
        "local_extract_path": local_extract_path,
        "expected_csv_filename": expected_csv_filename,
        "table_definition_function": table_definition_function,
        "table_name": table_name,
        "column_mapping": column_mapping,
        "engine": engine,
        "date_cols": date_cols or [],
    }

def player_peak_season_config() -> dict:
    """Predefined config for peak player season table."""
    return build_processing_config(
        bucket_name=S3_BUCKET_NAME,
        s3_file_key="peak_player_season_stats.csv",
        table_definition_function=define_player_peak_season,
        table_name="peak_player_season",
        column_mapping=COLUMN_MAPPINGS["player_peak_season"],
        local_download_path=local_download_path,
        local_extract_path=None,
        expected_csv_filename= "peak_player_season_stats.csv",
        engine=engine,
        is_zip=False,
        date_cols=DATE_COLS["player_peak_season"],
    )



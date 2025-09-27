"""
upload ref hockey data from
s3 bucket. Clean and parse data.
Merge with peak_player_season
on player name and season.

Authored by Eric Winiecke.
September 22, 2025.
"""

import logging
import os
import re

import pandas as pd
from constants import S3_BUCKET_NAME, local_download_path
from dotenv import load_dotenv
from s3_utils import download_from_s3

# optional logger util; import only (no execution yet)
try:
    from log_utils import setup_logger  # noqa: F401
except Exception:  # if module not present, we'll fall back later
    setup_logger = None  # type: ignore

# ---- now do executable code (ok after all imports) ----
load_dotenv()  # load .env before reading env vars

if setup_logger:
    try:
        setup_logger()
    except Exception:
        logging.basicConfig(level=logging.INFO)
else:
    logging.basicConfig(level=logging.INFO)

logger = logging.getLogger(__name__)

# ---- config for THIS file ----
S3_KEY = "nhl_player_seasons_data/nhl_player_seasons_{}.csv"
LOCAL_CSV_PATH = os.path.join(local_download_path, S3_KEY)
DATA_TABLE_NAME = "hockeyref_players"

# conflict_cols=["player", "season"],


def yyyy_to_season(yyyy):
    y = int(str(yyyy)[:4])
    return f"{(y - 1) % 100:02d}-{y % 100:02d}"


def clean_hockeyref(df: pd.DataFrame) -> pd.DataFrame:
    df = df.rename(
        columns={c: re.sub(r"[^A-Za-z0-9]+", "_", c).strip("_").lower() for c in df.columns}
    )
    if {"first_name", "last_name"}.issubset(df.columns) and "player" not in df.columns:
        df["player"] = (df["first_name"].fillna("") + " " + df["last_name"].fillna("")).str.strip()
    if "player" not in df.columns and "player_name" in df.columns:
        df = df.rename(columns={"player_name": "player"})
    if "season" in df.columns and df["season"].astype(str).str.fullmatch(r"\d{4}").any():
        df["season"] = df["season"].apply(yyyy_to_season)
    elif "year" in df.columns:
        df["season"] = df["year"].apply(yyyy_to_season)
    for c in ("age", "gp", "g", "a", "pts"):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    if "player" in df.columns:
        df["player"] = df["player"].astype(str).str.replace(r"\s+", " ", regex=True).str.strip()
    return df


def main(mode: str = "upsert", conflict_key: str = "player_season_team"):
    # 1) Download CSV (skip if already present)
    os.makedirs(os.path.dirname(LOCAL_CSV_PATH), exist_ok=True)
    logger.info("Downloading %s from s3://%s to %s", S3_KEY, S3_BUCKET_NAME, LOCAL_CSV_PATH)
    print(f"Downloading s3://{S3_BUCKET_NAME}/{S3_KEY} -> {LOCAL_CSV_PATH}")
    download_from_s3(S3_BUCKET_NAME, S3_KEY, LOCAL_CSV_PATH, overwrite=False)

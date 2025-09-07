"""
Make a file for global constants.

Eric Winiecke.
September 7, 2025.
"""

import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()  # so imports see .env values

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(exist_ok=True)

# paths your other modules use
local_download_path = str(DATA_DIR)

S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
if not S3_BUCKET_NAME:
    raise EnvironmentError("S3_BUCKET_NAME is not set in environment variables.")

# (optional, but handy defaults)
S3_FILE_KEY = os.getenv("S3_FILE_KEY", "peak_player_season_stats.csv")

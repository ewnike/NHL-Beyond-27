#!/usr/bin/env python3
"""
helper script that loads from S3 all of the data
scraped from Hockey Reference website.
Authored by: Eric Winiecke
September 26, 2025.
"""

from __future__ import annotations

from pathlib import Path

from constants import S3_BUCKET_NAME
from dotenv import load_dotenv
from log_utils import setup_logger  # ✅ your utility
from s3_utils import download_prefix  # uses the same logger


def main():
    load_dotenv()
    logger = setup_logger(None, level=20)  # INFO; None = root (your util adds console+file)
    # optionally change log dir via LOG_DIR env var

    n1 = download_prefix(S3_BUCKET_NAME, "nhl_player_seasons_data/", Path("data/seasons"))
    n2 = download_prefix(S3_BUCKET_NAME, "hockey_ref_even_strength/", Path("data/even_strength"))
    n3 = download_prefix(S3_BUCKET_NAME, "goalies/", Path("data/goalies"))
    n4 = download_prefix(S3_BUCKET_NAME, "player_cap_hits/", Path("data/cap_hits"))
    logger.info("Downloads complete — seasons=%d, even=%d, goalies=%d, cap_hits=%d", n1, n2, n3, n4)


if __name__ == "__main__":
    main()

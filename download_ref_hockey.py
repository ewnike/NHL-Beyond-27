#!/usr/bin/env python3
"""
helper script that loads from S3 all of the data
scraped from Hockey Reference website.
Authored by: Eric Winiecke
September 26, 2025.
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

from constants import S3_BUCKET_NAME  # keep your existing constants  local_download_path
from dotenv import load_dotenv
from s3_utils import download_prefix

# If you prefer constants, you can also define:
# S3_PREFIX = "nhl_player_seasons_data/"
# S3_PREFIX_II = "hockey_ref_even_strength/"


def main():
    ap = argparse.ArgumentParser(description="Download two S3 folders into local data dir")
    ap.add_argument("--prefix1", default="nhl_player_seasons_data/", help="First S3 prefix")
    ap.add_argument("--prefix2", default="hockey_ref_even_strength/", help="Second S3 prefix")
    ap.add_argument(
        "--bucket", default=None, help="S3 bucket (defaults to constants.S3_BUCKET_NAME)"
    )
    ap.add_argument("--out1", default="data/seasons", help="Local folder for prefix1")
    ap.add_argument("--out2", default="data/even_strength", help="Local folder for prefix2")
    ap.add_argument("--overwrite", action="store_true", help="Overwrite existing files")
    ap.add_argument("--log-level", default="INFO")
    args = ap.parse_args()

    load_dotenv()
    logging.basicConfig(level=getattr(logging, args.log_level.upper(), logging.INFO))
    bucket = args.bucket or S3_BUCKET_NAME

    out1 = Path(args.out1)
    out2 = Path(args.out2)

    n1 = download_prefix(bucket, args.prefix1, out1, overwrite=args.overwrite)
    n2 = download_prefix(bucket, args.prefix2, out2, overwrite=args.overwrite)

    logging.getLogger(__name__).info(
        "Downloaded: %d files -> %s ; %d files -> %s", n1, out1, n2, out2
    )


if __name__ == "__main__":
    main()

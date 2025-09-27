"""
s3_utils.py.

This module provides functionality to download files from an AWS S3 bucket.
It initializes an S3 client and retrieves environment variables for the bucket name
and file paths.

Example Usage:
    Ensure the following environment variables are set:
        - S3_BUCKET_NAME: The name of the S3 bucket
        - S3_FILE_KEY: The key (path) of the file in S3
        - LOCAL_FILE_PATH: The local path where the file should be saved

    Run the script:
        ```python
        python s3_utils.py
        ```

Author: Eric Winiecke
Date: September 2025
"""

# from __future__ import annotations

# import logging
# from collections.abc import Iterable
# from pathlib import Path

# import boto3
# from botocore.exceptions import ClientError

# log = logging.getLogger(__name__)
# _s3 = boto3.client("s3")


# def _ensure_parent(path: Path) -> None:
#     path.parent.mkdir(parents=True, exist_ok=True)


# def download_from_s3(
#     bucket: str, key: str, local_path: str | Path, *, overwrite: bool = False
# ) -> bool:
#     """
#     Download a single S3 object to a local path.
#     Returns True if downloaded (or already present and kept), False if skipped by error.
#     """
#     lp = Path(local_path)
#     if lp.exists() and not overwrite:
#         log.info("Exists (skip): %s", lp)
#         return True
#     try:
#         _ensure_parent(lp)
#         _s3.download_file(bucket, key, str(lp))
#         log.info("Downloaded s3://%s/%s -> %s", bucket, key, lp)
#         return True
#     except ClientError as e:
#         log.error("Failed: s3://%s/%s -> %s  (%s)", bucket, key, lp, e)
#         return False


# def iter_s3_keys(bucket: str, prefix: str) -> Iterable[str]:
#     """
#     Yield all keys under s3://bucket/prefix (non-recursive in S3 terms, but Prefix is a prefix match).
#     """
#     paginator = _s3.get_paginator("list_objects_v2")
#     for page in paginator.paginate(Bucket=bucket, Prefix=prefix.rstrip("/") + "/"):
#         for obj in page.get("Contents", []) or []:
#             yield obj["Key"]


# def download_prefix(
#     bucket: str,
#     prefix: str,
#     local_dir: str | Path,
#     *,
#     overwrite: bool = False,
#     preserve_subpaths: bool = True,
# ) -> int:
#     """
#     Download all objects under s3://bucket/prefix into local_dir.
#     If preserve_subpaths=True, keeps subpath after 'prefix/' under local_dir.
#     Returns number of files downloaded or validated.
#     """
#     local_root = Path(local_dir)
#     local_root.mkdir(parents=True, exist_ok=True)

#     count = 0
#     base = prefix.rstrip("/") + "/"
#     for key in iter_s3_keys(bucket, prefix):
#         # compute relative path after prefix/
#         rel = key[len(base) :] if key.startswith(base) else Path(key).name
#         lp = local_root / (rel if preserve_subpaths else Path(key).name)
#         if download_from_s3(bucket, key, lp, overwrite=overwrite):
#             count += 1
#     log.info("Done: %d files from s3://%s/%s -> %s", count, bucket, prefix, local_root)
#     return count
# s3_utils.py
from __future__ import annotations

import logging  # noqa: F401
from pathlib import Path

import boto3
from botocore.exceptions import ClientError
from log_utils import setup_logger  # ✅ use your utility

logger = setup_logger(__name__)  # ✅ named logger using your format/handlers
_s3 = boto3.client("s3")


def _ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def download_from_s3(
    bucket: str, key: str, local_path: str | Path, *, overwrite: bool = False
) -> bool:
    lp = Path(local_path)
    logger.debug("prepare: s3://%s/%s -> %s (overwrite=%s)", bucket, key, lp, overwrite)
    if lp.exists() and not overwrite:
        logger.info("Exists (skip): %s", lp)
        return True
    try:
        _ensure_parent(lp)
        _s3.download_file(bucket, key, str(lp))
        logger.info("Downloaded s3://%s/%s -> %s", bucket, key, lp)
        return True
    except ClientError as e:
        logger.warning("Failed: s3://%s/%s -> %s  (%s)", bucket, key, lp, e)
        return False


def iter_s3_keys(bucket: str, prefix: str):
    logger.info("Listing s3://%s/%s", bucket, prefix.rstrip("/"))
    paginator = _s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix.rstrip("/") + "/"):
        for obj in page.get("Contents", []) or []:
            yield obj["Key"]


def download_prefix(
    bucket: str,
    prefix: str,
    local_dir: str | Path,
    *,
    overwrite: bool = False,
    preserve_subpaths: bool = True,
) -> int:
    from pathlib import Path as _P

    local_root = _P(local_dir)
    local_root.mkdir(parents=True, exist_ok=True)

    base = prefix.rstrip("/") + "/"
    count = 0
    for key in iter_s3_keys(bucket, prefix):
        rel = key[len(base) :] if key.startswith(base) else key
        lp = local_root / rel
        if download_from_s3(bucket, key, lp, overwrite=overwrite):
            count += 1
    logger.info(
        "Done: %d files from s3://%s/%s -> %s", count, bucket, prefix.rstrip("/"), local_root
    )
    return count

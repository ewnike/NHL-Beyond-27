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

import logging
import os

import pandas as pd
import boto3
import botocore
from botocore.config import Config

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

# Initialize the S3 client
s3_client = boto3.client("s3")
bucket_name = os.getenv("S3_BUCKET_NAME")  # Get the bucket name from environment variables


def get_s3_env_vars():
    """
    Return required S3 config pulled from environment variables.

    Returns:
        dict: Contains 'bucket', 'key', and 'local_path' values.

    """
    return {
        "bucket": os.getenv("S3_BUCKET_NAME"),
        "key": os.getenv("S3_FILE_KEY"),
        "local_path": os.getenv("LOCAL_FILE_PATH"),
    }


def download_from_s3(bucket: str, key: str, download_path: str, overwrite: bool = False) -> None:
    """
    Download a file from an S3 bucket and save it to a local path.

    Args:
    ----
        bucket (str): Name of the S3 bucket.
        key (str): S3 object key (file path inside the bucket).
        download_path (str): Local file path to save the downloaded file.
        overwrite (bool): If True, overwrite existing files (default: False).

    Raises:
    ------
        botocore.exceptions.ClientError: If the file doesn't exist in S3 or another S3 error occurs.
        PermissionError: If the script lacks permission to write to the directory.

    Logs:
    ------
        - INFO: When a download starts and completes.
        - WARNING: If the file already exists and overwrite is False.
        - ERROR: If the file is not found or another error occurs.

    """
    logging.debug(f"Initiating download with path: {download_path}")  # Additional debug logging
    if not download_path:
        logging.error("Download path is empty. Skipping download.")
        return

    try:
        # Check if the file already exists
        if os.path.exists(download_path) and not overwrite:
            logging.warning(f"File already exists: {download_path}. Skipping download.")
            return

        # Ensure the directory exists before downloading
        os.makedirs(os.path.dirname(download_path), exist_ok=True)

        logging.info(f"Downloading from bucket: {bucket}, key: {key}, to: {download_path}")
        s3_client.download_file(bucket, key, download_path)
        logging.info(f"Download complete: {download_path}")

    except botocore.exceptions.NoCredentialsError:
        logging.error("AWS credentials not found. Ensure they are set in the environment.")
        raise

    except botocore.exceptions.PartialCredentialsError:
        logging.error("AWS credentials are incomplete. Check your AWS configuration.")
        raise

    except botocore.exceptions.ClientError as e:
        error_code = e.response["Error"]["Code"]
        if error_code == "404":
            logging.error(f"File not found: {key} in bucket {bucket}.")
        elif error_code == "403":
            logging.error(f"Access denied to {key} in bucket {bucket}. Check permissions.")
        else:
            logging.error(f"Unexpected S3 error: {e}")
        raise

    except PermissionError:
        logging.error(f"Permission denied: Cannot write to {download_path}.")
        raise

    except Exception as e:
        logging.error(f"An unexpected error occurred while downloading {key}: {e}")
        raise


if __name__ == "__main__":
    # Read environment variables for bucket and file paths
    S3_FILE_KEY = os.getenv("S3_FILE_KEY")
    local_file_path = os.getenv("LOCAL_FILE_PATH")

    if not bucket_name:
        logging.error("Bucket name not found. Please set the S3_BUCKET_NAME environment variable.")
    elif not S3_FILE_KEY or not local_file_path:
        logging.error(
            "S3_FILE_KEY or LOCAL_FILE_PATH not set. Ensure these are provided in the environment."
        )
    else:
        download_from_s3(bucket_name, S3_FILE_KEY, local_file_path, overwrite=False)

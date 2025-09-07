"""
Helper functions for log files.

Author: Eric Winiecke
Date: April 2025
"""

import logging
import os
from logging.handlers import RotatingFileHandler


def setup_logger(log_file_path="data_processing.log"):
    """Set up a logger with rotating file handler and console output."""
    log_dir = os.path.dirname(log_file_path)
    os.makedirs(log_dir, exist_ok=True)

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    # Avoid duplicate handlers
    if not logger.handlers:
        # File handler
        file_handler = RotatingFileHandler(log_file_path, maxBytes=5 * 1024 * 1024, backupCount=3)
        file_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
        logger.addHandler(file_handler)

        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
        logger.addHandler(console_handler)

    return logger

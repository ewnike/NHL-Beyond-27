"""
Helper functions for log files.

Author: Eric Winiecke
Date: April 2025
"""

import logging
import os
from logging.handlers import RotatingFileHandler

# log_utils.py


def setup_logger(name: str | None = None, *, level=logging.INFO, log_dir: str | None = None):
    """
    Configure root or named logger. If log_dir is falsy, default to 'logs'.
    Adds both console and rotating file handlers (file only if a dir is set).
    """
    logger = logging.getLogger(name)
    if logger.handlers:  # avoid duplicate handlers on repeated calls
        return logger

    logger.setLevel(level)
    fmt = logging.Formatter("%(asctime)s - %(levelname)s - %(name)s - %(message)s")

    # Console handler
    sh = logging.StreamHandler()
    sh.setLevel(level)
    sh.setFormatter(fmt)
    logger.addHandler(sh)

    # File handler (with safe default)
    log_dir = (log_dir or os.getenv("LOG_DIR") or "logs").strip()
    if log_dir:
        os.makedirs(log_dir, exist_ok=True)
        logfile = os.path.join(log_dir, "app.log")
        fh = RotatingFileHandler(logfile, maxBytes=5_000_000, backupCount=3)
        fh.setLevel(level)
        fh.setFormatter(fmt)
        logger.addHandler(fh)

    return logger

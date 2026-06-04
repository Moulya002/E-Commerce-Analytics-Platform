"""
Shared logging configuration for all platform components.
"""

import logging
import sys
from typing import Optional


def setup_logging(
    name: str,
    level: Optional[str] = None,
) -> logging.Logger:
    """Configure structured logging for a module."""
    log_level = getattr(logging, (level or "INFO").upper(), logging.INFO)

    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)

    logger = logging.getLogger(name)
    logger.setLevel(log_level)

    if not logger.handlers:
        logger.addHandler(handler)

    return logger

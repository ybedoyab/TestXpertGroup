from __future__ import annotations

"""Logging configuration for the pipeline."""

import logging


def configure_logging(level: int = logging.INFO, *, logger_name: str | None = None) -> None:
    logger = logging.getLogger(logger_name) if logger_name else logging.getLogger()
    logger.setLevel(level)

    if not any(isinstance(h, logging.StreamHandler) for h in logger.handlers):
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            fmt="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)


"""Structured, JSON-first logging.

Uses ``structlog`` so every log line is machine-parseable and carries bound
context (job, table, batch_id). In production these lines are shipped to Loki /
Elasticsearch; locally they render as coloured key-values.
"""

from __future__ import annotations

import logging
import sys
from typing import Any

import structlog

_CONFIGURED = False


def configure_logging(level: str = "INFO", fmt: str = "json") -> None:
    """Idempotently configure the root logger and structlog pipeline."""

    global _CONFIGURED
    if _CONFIGURED:
        return

    logging.basicConfig(format="%(message)s", stream=sys.stdout, level=level.upper())

    shared = [
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso", utc=True),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
    ]
    renderer = (
        structlog.processors.JSONRenderer()
        if fmt == "json"
        else structlog.dev.ConsoleRenderer()
    )
    structlog.configure(
        processors=[*shared, renderer],
        wrapper_class=structlog.make_filtering_bound_logger(
            logging.getLevelName(level.upper())
        ),
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )
    _CONFIGURED = True


def get_logger(name: str, **initial: Any) -> structlog.stdlib.BoundLogger:
    """Return a bound logger, optionally pre-seeded with context."""

    return structlog.get_logger(name).bind(**initial)

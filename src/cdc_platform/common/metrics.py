"""Prometheus metrics for the streaming jobs.

Metrics are pushed to a Pushgateway for short-lived batch jobs and scraped
directly for long-running streams. Names follow Prometheus conventions
(``_total`` for counters, ``_seconds`` for histograms).
"""

from __future__ import annotations

from prometheus_client import Counter, Gauge, Histogram

CDC_EVENTS_TOTAL = Counter(
    "cdc_events_total",
    "CDC events processed",
    labelnames=("layer", "table", "op"),
)

BATCH_ROWS = Gauge(
    "cdc_batch_rows",
    "Rows in the most recent micro-batch",
    labelnames=("layer", "table"),
)

BATCH_DURATION = Histogram(
    "cdc_batch_duration_seconds",
    "Micro-batch processing latency",
    labelnames=("layer", "table"),
    buckets=(0.5, 1, 2, 5, 10, 30, 60, 120, 300),
)

END_TO_END_LAG = Gauge(
    "cdc_end_to_end_lag_seconds",
    "Wall-clock lag between source commit and lakehouse commit",
    labelnames=("table",),
)

DQ_VALIDATION_FAILURES = Counter(
    "cdc_dq_validation_failures_total",
    "Great Expectations expectation failures",
    labelnames=("suite", "table"),
)

"""Reusable streaming-job engine (Template Method pattern).

Concrete jobs (Bronze, Silver) override :meth:`transform` and :meth:`sink`; the
engine owns the cross-cutting lifecycle: Kafka source construction, checkpoint
management, metrics, structured logging, and graceful shutdown. This keeps every
job small and consistent (SOLID: SRP + OCP).
"""

from __future__ import annotations

import abc
import os
import time

from prometheus_client import REGISTRY, push_to_gateway
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.streaming import StreamingQuery

from cdc_platform.common.config import Settings, get_settings
from cdc_platform.common.logging import get_logger
from cdc_platform.common.metrics import BATCH_DURATION, BATCH_ROWS, CDC_EVENTS_TOTAL


class StreamingJob(abc.ABC):
    """Base class for a foreachBatch-style streaming job for a single table."""

    layer: str = "base"

    def __init__(self, table: str, spark: SparkSession, settings: Settings | None = None) -> None:
        self.table = table
        self.spark = spark
        self.settings = settings or get_settings()
        self.log = get_logger(f"{self.layer}.{table}", layer=self.layer, table=table)

    # ---- Kafka source -----------------------------------------------------
    def read_kafka(self, topic: str, starting_offsets: str = "earliest") -> DataFrame:
        cfg = self.settings.kafka
        return (
            self.spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", cfg.bootstrap_servers)
            .option("subscribe", topic)
            .option("startingOffsets", starting_offsets)
            .option("maxOffsetsPerTrigger", self.settings.spark.max_offsets_per_trigger)
            .option("failOnDataLoss", "false")
            .load()
        )

    def checkpoint_location(self) -> str:
        return f"{self.settings.spark.checkpoint_root}/{self.layer}/{self.table}"

    # ---- Template methods overridden by subclasses ------------------------
    @abc.abstractmethod
    def source(self) -> DataFrame:
        """Return the streaming input DataFrame."""

    @abc.abstractmethod
    def process_batch(self, batch_df: DataFrame, batch_id: int) -> None:
        """Idempotently process one micro-batch (must be replay-safe)."""

    # ---- Lifecycle --------------------------------------------------------
    def _instrumented_batch(self, batch_df: DataFrame, batch_id: int) -> None:
        start = time.monotonic()
        batch_df.persist()
        try:
            rows = batch_df.count()
            self.process_batch(batch_df, batch_id)
            BATCH_ROWS.labels(self.layer, self.table).set(rows)
            BATCH_DURATION.labels(self.layer, self.table).observe(time.monotonic() - start)
            self._push_metrics()
            self.log.info("batch_committed", batch_id=batch_id, rows=rows)
        except Exception:
            self.log.exception("batch_failed", batch_id=batch_id)
            raise
        finally:
            batch_df.unpersist()

    def _push_metrics(self) -> None:
        """Push the current metric values to the Prometheus Pushgateway.

        Streaming drivers are long-lived but their metrics live in-process, so we
        push after every micro-batch. Prometheus scrapes the Pushgateway; a
        per-(layer, table) grouping key keeps concurrent jobs from overwriting
        one another. Best-effort: a metrics failure must never break a batch.
        """

        gateway = os.getenv("PROMETHEUS_PUSHGATEWAY", "")
        if not gateway:
            return
        try:
            push_to_gateway(
                gateway.replace("http://", "").replace("https://", ""),
                job="cdc_streaming",
                registry=REGISTRY,
                grouping_key={"layer": self.layer, "table": self.table},
            )
        except Exception:  # noqa: BLE001 - observability must not break ingestion
            self.log.warning("metrics_push_failed")

    def start(self, trigger_interval: str = "30 seconds") -> StreamingQuery:
        self.log.info("starting_stream", checkpoint=self.checkpoint_location())
        return (
            self.source()
            .writeStream.queryName(f"{self.layer}_{self.table}")
            .option("checkpointLocation", self.checkpoint_location())
            .trigger(processingTime=trigger_interval)
            .foreachBatch(self._instrumented_batch)
            .start()
        )

    def _record_ops(self, batch_df: DataFrame) -> None:
        """Emit per-op counters for observability (best-effort)."""

        try:
            for row in batch_df.groupBy("op").count().collect():
                CDC_EVENTS_TOTAL.labels(self.layer, self.table, row["op"]).inc(row["count"])
        except Exception:  # pragma: no cover - metrics must never break a batch
            self.log.warning("op_metrics_failed")

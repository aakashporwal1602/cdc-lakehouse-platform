"""Bronze streaming job: Kafka (Avro CDC) -> immutable Iceberg landing table.

Design:
  * Append-only. Bronze is never mutated, so it is a perfect audit log and a
    replayable source of truth for rebuilding Silver/Gold.
  * We decode the Confluent-framed Avro value (5-byte magic header stripped) with
    the latest registry schema, then persist the full Debezium envelope
    (before/after/op/source) plus Kafka coordinates for lineage.
  * Exactly-once: Kafka offsets are committed in the same checkpoint as the
    Iceberg append; replays are safe because Silver de-duplicates on primary key.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from cdc_platform.common.tables import TableSpec, load_registry
from cdc_platform.streaming.decode import decode_cdc
from cdc_platform.streaming.engine import StreamingJob
from cdc_platform.streaming.session import build_spark


class BronzeJob(StreamingJob):
    """Land raw CDC events for a single table into ``lakehouse.bronze.<table>``."""

    layer = "bronze"

    def __init__(self, spec: TableSpec, spark, settings=None) -> None:  # noqa: ANN001
        super().__init__(spec.name, spark, settings)
        self.spec = spec
        self.topic = spec.topic(self.settings.cdc_topic_prefix, load_registry().source_schema)
        self.target = f"{self.settings.iceberg.catalog_name}.bronze.{spec.name}"

    def source(self) -> DataFrame:
        return decode_cdc(self.read_kafka(self.topic), self.topic)

    def _table_exists(self) -> bool:
        try:
            self.spark.table(self.target)
            return True
        except Exception:  # noqa: BLE001 - table simply doesn't exist yet
            return False

    def ensure_table(self, sample: DataFrame) -> None:
        """Create the Bronze table from the streaming schema if absent (idempotent)."""

        self.spark.sql(
            f"CREATE NAMESPACE IF NOT EXISTS {self.settings.iceberg.catalog_name}.bronze"
        )
        if not self._table_exists():
            (
                sample.limit(0)
                .writeTo(self.target)
                .using("iceberg")
                .tableProperty("format-version", "2")
                .create()
            )

    def process_batch(self, batch_df: DataFrame, batch_id: int) -> None:
        enriched = batch_df.withColumn("bronze_ingested_at", F.current_timestamp())
        self.ensure_table(enriched)
        enriched.writeTo(self.target).append()
        self._record_ops(batch_df)


def run(table: str) -> None:
    spark = build_spark(f"bronze-{table}")
    spec = load_registry().get(table)
    BronzeJob(spec, spark).start().awaitTermination()

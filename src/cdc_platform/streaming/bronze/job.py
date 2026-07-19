"""Bronze streaming job: Kafka (Avro CDC) -> immutable Iceberg landing table.

Design:
  * Append-only. We never mutate Bronze, which makes it a perfect audit log and
    a replayable source of truth for rebuilding Silver/Gold.
  * We decode the Confluent-framed Avro value (5-byte magic header stripped) with
    the latest registry schema, then persist both the decoded envelope columns and
    Kafka coordinates (topic/partition/offset) for lineage.
  * Exactly-once: Kafka offsets are committed in the same checkpoint as the
    Iceberg append; on replay the append is idempotent because Bronze is keyed by
    (partition, offset) which we de-dup on read into Silver.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.avro.functions import from_avro

from cdc_platform.common.schema_registry import value_schema
from cdc_platform.common.tables import TableSpec, load_registry
from cdc_platform.streaming.engine import StreamingJob
from cdc_platform.streaming.session import build_spark

# Confluent wire format = 1 magic byte + 4-byte schema id + Avro body.
_CONFLUENT_HEADER_BYTES = 5


class BronzeJob(StreamingJob):
    """Land raw CDC events for a single table into ``lakehouse.bronze.<table>``."""

    layer = "bronze"

    def __init__(self, spec: TableSpec, spark, settings=None) -> None:  # noqa: ANN001
        super().__init__(spec.name, spark, settings)
        self.spec = spec
        self.topic = spec.topic(self.settings.cdc_topic_prefix, load_registry().source_schema)
        self.target = f"{self.settings.iceberg.catalog_name}.bronze.{spec.name}"

    def _decoded(self, kafka_df: DataFrame) -> DataFrame:
        schema = value_schema(self.topic)
        stripped = F.expr(f"substring(value, {_CONFLUENT_HEADER_BYTES + 1}, length(value))")
        return kafka_df.select(
            F.col("topic"),
            F.col("partition").alias("kafka_partition"),
            F.col("offset").alias("kafka_offset"),
            F.col("timestamp").alias("kafka_timestamp"),
            from_avro(stripped, schema).alias("payload"),
        ).select(
            "topic", "kafka_partition", "kafka_offset", "kafka_timestamp", "payload.*"
        )

    def source(self) -> DataFrame:
        return self._decoded(self.read_kafka(self.topic))

    def ensure_table(self, sample: DataFrame) -> None:
        """Create the Bronze table on first run from the streaming schema."""

        cols = ", ".join(f"{f.name} {f.dataType.simpleString()}" for f in sample.schema.fields)
        self.spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {self.settings.iceberg.catalog_name}.bronze")
        self.spark.sql(
            f"CREATE TABLE IF NOT EXISTS {self.target} ({cols}) "
            "USING iceberg "
            "PARTITIONED BY (days(kafka_timestamp)) "
            "TBLPROPERTIES ('format-version'='2', 'write.distribution-mode'='hash')"
        )

    def process_batch(self, batch_df: DataFrame, batch_id: int) -> None:
        if batch_id == 0:
            self.ensure_table(batch_df)
        enriched = batch_df.withColumn("bronze_ingested_at", F.current_timestamp())
        enriched.writeTo(self.target).append()
        self._record_ops(batch_df)


def run(table: str) -> None:
    spark = build_spark(f"bronze-{table}")
    spec = load_registry().get(table)
    BronzeJob(spec, spark).start().awaitTermination()

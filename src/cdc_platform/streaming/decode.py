"""Shared Confluent-Avro CDC decoding used by both Bronze and Silver.

Both layers consume the same Kafka topic independently (a common medallion
pattern that keeps each layer's checkpoint self-contained and avoids coupling
Silver to Bronze's storage). This module centralises the Debezium envelope
decode so the logic lives in exactly one place.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.avro.functions import from_avro

from cdc_platform.common.schema_registry import value_schema

# Confluent wire format = 1 magic byte + 4-byte schema id + Avro body.
_CONFLUENT_HEADER_BYTES = 5


def decode_cdc(kafka_df: DataFrame, topic: str) -> DataFrame:
    """Decode a Kafka source DataFrame into the flattened Debezium envelope.

    Returns Kafka coordinates (for lineage) plus the envelope fields
    (``before``, ``after``, ``op``, ``ts_ms``, ``source`` ...).
    """

    schema = value_schema(topic)
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

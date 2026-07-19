"""Debezium envelope decoding utilities.

A Debezium (Avro) change event has the shape::

    { "before": {...}, "after": {...}, "op": "c|u|d|r",
      "ts_ms": <int>, "source": { "lsn": <int>, "ts_ms": <int>, ... } }

These helpers flatten that envelope into a normalised record used by every
Silver job, regardless of table (Liskov: all tables share one contract).
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

# Debezium operation codes.
OP_CREATE = "c"
OP_UPDATE = "u"
OP_DELETE = "d"
OP_READ = "r"  # snapshot read
DELETE_OPS = (OP_DELETE,)


def is_delete(op_col: Column) -> Column:
    return op_col.isin(list(DELETE_OPS))


def normalise(df: DataFrame, payload_cols: list[str]) -> DataFrame:
    """Flatten a decoded Debezium envelope into one row per change.

    For deletes the ``after`` image is null, so we coalesce onto ``before`` to
    preserve the primary key needed for the downstream MERGE/tombstone.
    """

    image = F.when(F.col("op").isin([OP_DELETE]), F.col("before")).otherwise(F.col("after"))
    projected = [image.getField(c).alias(c) for c in payload_cols]
    return df.select(
        *projected,
        F.col("op"),
        F.col("ts_ms").alias("event_ts_ms"),
        F.col("source.lsn").alias("source_lsn"),
        F.col("source.ts_ms").alias("source_ts_ms"),
        F.col("source.table").alias("source_table"),
    )

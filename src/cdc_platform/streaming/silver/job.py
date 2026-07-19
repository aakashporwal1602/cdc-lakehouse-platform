"""Silver streaming job: Bronze (raw CDC) -> deduplicated current-state table.

Correctness core of the platform:

  * **Flatten**: the Debezium envelope stored in Bronze is collapsed to one image
    row per event (``after`` for insert/update, ``before`` for delete), with the
    source LSN/timestamp lifted out for ordering.
  * **Deduplication + ordering**: within each micro-batch we rank rows per primary
    key by ``(source_lsn, source_ts_ms)`` and keep the newest. LSN ordering means
    out-of-order Kafka delivery cannot produce a stale winner.
  * **Idempotent upsert**: an Iceberg ``MERGE INTO`` applies the winning row and
    is guarded by the LSN, so replays after a checkpoint failure are safe
    (effectively exactly-once). Deletes become ``WHEN MATCHED THEN DELETE``.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from cdc_platform.common.tables import TableSpec, load_registry
from cdc_platform.streaming.debezium import OP_DELETE
from cdc_platform.streaming.decode import decode_cdc
from cdc_platform.streaming.engine import StreamingJob
from cdc_platform.streaming.session import build_spark

# Columns that are technical metadata rather than business payload.
_TECH_COLS = {"op", "source_lsn", "source_ts_ms", "silver_updated_at", "__is_delete"}


class SilverJob(StreamingJob):
    """Maintain ``lakehouse.silver.<table>`` as the deduplicated current state."""

    layer = "silver"

    def __init__(self, spec: TableSpec, spark, settings=None) -> None:  # noqa: ANN001
        super().__init__(spec.name, spark, settings)
        self.spec = spec
        cat = self.settings.iceberg.catalog_name
        self.topic = spec.topic(self.settings.cdc_topic_prefix, load_registry().source_schema)
        self.target = f"{cat}.silver.{spec.name}"

    def source(self) -> DataFrame:
        """Consume the CDC topic directly (independent of Bronze storage)."""

        return decode_cdc(self.read_kafka(self.topic), self.topic)

    def flatten(self, df: DataFrame) -> DataFrame:
        """Collapse the Debezium envelope into a flat current-image row."""

        image = F.coalesce(F.col("after"), F.col("before"))
        return df.withColumn("__img", image).select(
            "__img.*",
            F.col("op"),
            F.col("source.lsn").alias("source_lsn"),
            F.col("source.ts_ms").alias("source_ts_ms"),
        )

    def business_columns(self, flat: DataFrame) -> list[str]:
        return [c for c in flat.columns if c not in _TECH_COLS]

    def deduplicate(self, flat: DataFrame) -> DataFrame:
        """Keep the newest row per primary key using LSN/ts ordering."""

        order = [F.col(c).desc_nulls_last() for c in self.spec.order_by]
        window = Window.partitionBy(*self.spec.primary_key).orderBy(*order)
        return (
            flat.withColumn("_rn", F.row_number().over(window))
            .filter(F.col("_rn") == 1)
            .drop("_rn")
        )

    def _table_exists(self) -> bool:
        try:
            self.spark.table(self.target)
            return True
        except Exception:  # noqa: BLE001 - table simply doesn't exist yet
            return False

    def ensure_table(self, flat: DataFrame) -> None:
        cat = self.settings.iceberg.catalog_name
        self.spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {cat}.silver")
        if not self._table_exists():
            (
                flat.limit(0)
                .writeTo(self.target)
                .using("iceberg")
                .tableProperty("format-version", "2")
                .tableProperty("write.merge.mode", "merge-on-read")
                .create()
            )

    def build_merge_sql(self, cols: list[str]) -> str:
        """Compose the tombstone-aware, order-preserving MERGE statement."""

        pk_join = " AND ".join(f"t.{k} = s.{k}" for k in self.spec.primary_key)
        update_set = ", ".join(
            f"t.{c} = s.{c}" for c in cols if c not in self.spec.primary_key
        )
        insert_cols = ", ".join(cols)
        insert_vals = ", ".join(f"s.{c}" for c in cols)
        return f"""
            MERGE INTO {self.target} t
            USING silver_updates s
            ON {pk_join}
            WHEN MATCHED AND s.__is_delete = true THEN DELETE
            WHEN MATCHED AND (t.source_lsn IS NULL OR s.source_lsn IS NULL
                              OR s.source_lsn >= t.source_lsn)
                THEN UPDATE SET {update_set}
            WHEN NOT MATCHED AND s.__is_delete = false
                THEN INSERT ({insert_cols}) VALUES ({insert_vals})
        """

    def process_batch(self, batch_df: DataFrame, batch_id: int) -> None:
        # In foreachBatch the batch DataFrame belongs to a cloned session; the
        # temp view and the MERGE must use *that* session, not self.spark.
        session = batch_df.sparkSession
        flat = self.flatten(batch_df)
        business = self.business_columns(flat)
        winners = (
            self.deduplicate(flat)
            .withColumn("silver_updated_at", F.current_timestamp())
            .withColumn("__is_delete", F.col("op") == OP_DELETE)
        )
        persist_cols = [*business, "source_lsn", "source_ts_ms", "silver_updated_at"]

        self.ensure_table(winners.select(*persist_cols))
        winners.select(*persist_cols, "__is_delete").createOrReplaceTempView("silver_updates")
        session.sql(self.build_merge_sql(persist_cols))
        self._record_ops(batch_df)


def run(table: str) -> None:
    spark = build_spark(f"silver-{table}")
    spec = load_registry().get(table)
    SilverJob(spec, spark).start().awaitTermination()

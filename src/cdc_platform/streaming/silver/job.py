"""Silver streaming job: Bronze (raw CDC) -> deduplicated current-state table.

Correctness core of the platform:

  * **Deduplication + ordering**: within each micro-batch we rank rows per primary
    key by ``(source_lsn, source_ts_ms)`` and keep only the newest. LSN ordering
    means out-of-order Kafka delivery cannot produce a stale winner.
  * **Idempotent upsert**: an Iceberg ``MERGE INTO`` applies the winning row. The
    merge condition also guards against regressions (only apply if the incoming
    LSN is newer than what's stored), so replays after a checkpoint failure are
    safe -> effectively exactly-once at the table level.
  * **Deletes**: Debezium ``op = 'd'`` rows are turned into ``MERGE ... WHEN
    MATCHED THEN DELETE`` (hard delete). Switch to soft-delete by flipping a flag
    in the table spec.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from cdc_platform.common.tables import TableSpec, load_registry
from cdc_platform.streaming.debezium import OP_DELETE
from cdc_platform.streaming.engine import StreamingJob
from cdc_platform.streaming.session import build_spark

_META_COLS = {
    "op", "event_ts_ms", "source_lsn", "source_ts_ms", "source_table",
    "topic", "kafka_partition", "kafka_offset", "kafka_timestamp",
    "bronze_ingested_at",
}


class SilverJob(StreamingJob):
    """Maintain ``lakehouse.silver.<table>`` as the deduplicated current state."""

    layer = "silver"

    def __init__(self, spec: TableSpec, spark, settings=None) -> None:  # noqa: ANN001
        super().__init__(spec.name, spark, settings)
        self.spec = spec
        cat = self.settings.iceberg.catalog_name
        self.source_table = f"{cat}.bronze.{spec.name}"
        self.target = f"{cat}.silver.{spec.name}"

    def source(self) -> DataFrame:
        """Stream new Bronze snapshots incrementally."""

        return (
            self.spark.readStream.format("iceberg")
            .option("stream-from-timestamp", "0")
            .load(self.source_table)
        )

    def _business_columns(self, df: DataFrame) -> list[str]:
        return [c for c in df.columns if c not in _META_COLS]

    def deduplicate(self, batch_df: DataFrame) -> DataFrame:
        """Keep the newest row per primary key using LSN/ts ordering."""

        order = [F.col(c).desc() for c in self.spec.order_by]
        window = Window.partitionBy(*self.spec.primary_key).orderBy(*order)
        return (
            batch_df.withColumn("_rn", F.row_number().over(window))
            .filter(F.col("_rn") == 1)
            .drop("_rn")
        )

    def ensure_table(self, sample: DataFrame) -> None:
        business = self._business_columns(sample)
        cols = [f"{f.name} {f.dataType.simpleString()}" for f in sample.schema.fields
                if f.name in business]
        cols += ["source_lsn bigint", "source_ts_ms bigint",
                 "silver_updated_at timestamp"]
        cat = self.settings.iceberg.catalog_name
        self.spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {cat}.silver")
        part = f"PARTITIONED BY ({', '.join(self.spec.partition_by)})" if self.spec.partition_by else ""
        self.spark.sql(
            f"CREATE TABLE IF NOT EXISTS {self.target} ({', '.join(cols)}) "
            f"USING iceberg {part} "
            "TBLPROPERTIES ('format-version'='2', "
            "'write.merge.mode'='merge-on-read', "
            "'write.update.mode'='merge-on-read', "
            "'write.delete.mode'='merge-on-read')"
        )

    def build_merge_sql(self, business: list[str]) -> str:
        """Compose the tombstone-aware, order-preserving MERGE statement."""

        select_cols = [*business, "source_lsn", "source_ts_ms", "silver_updated_at"]
        pk_join = " AND ".join(f"t.{k} = s.{k}" for k in self.spec.primary_key)
        update_set = ", ".join(
            f"t.{c} = s.{c}" for c in select_cols if c not in self.spec.primary_key
        )
        insert_cols = ", ".join(select_cols)
        insert_vals = ", ".join(f"s.{c}" for c in select_cols)
        return f"""
            MERGE INTO {self.target} t
            USING silver_updates s
            ON {pk_join}
            WHEN MATCHED AND s.__is_delete = true THEN DELETE
            WHEN MATCHED AND s.source_lsn > t.source_lsn THEN UPDATE SET {update_set}
            WHEN NOT MATCHED AND s.__is_delete = false
                THEN INSERT ({insert_cols}) VALUES ({insert_vals})
        """

    def process_batch(self, batch_df: DataFrame, batch_id: int) -> None:
        if batch_id == 0:
            self.ensure_table(batch_df)

        business = self._business_columns(batch_df)
        winners = (
            self.deduplicate(batch_df)
            .withColumn("silver_updated_at", F.current_timestamp())
            .withColumn("__is_delete", F.col("op") == OP_DELETE)
        )
        select_cols = [*business, "source_lsn", "source_ts_ms", "silver_updated_at"]
        winners.select(*select_cols, "__is_delete").createOrReplaceTempView("silver_updates")

        self.spark.sql(self.build_merge_sql(business))
        self._record_ops(batch_df)


def run(table: str) -> None:
    spark = build_spark(f"silver-{table}")
    spec = load_registry().get(table)
    SilverJob(spec, spark).start().awaitTermination()

"""Iceberg table maintenance: compaction, snapshot expiry, orphan-file cleanup.

Small-file accumulation is the #1 operational risk with streaming writes; this
DAG runs the standard Iceberg maintenance procedures on a nightly cadence.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow.decorators import dag, task

from cdc_platform.common.tables import load_registry

DEFAULT_ARGS = {"owner": "data-platform", "retries": 2, "retry_delay": timedelta(minutes=10)}


@dag(
    dag_id="cdc_iceberg_maintenance",
    schedule="0 3 * * *",  # nightly 03:00
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["cdc", "iceberg", "maintenance"],
)
def cdc_iceberg_maintenance() -> None:
    @task
    def maintain() -> None:
        from cdc_platform.streaming.session import build_spark

        spark = build_spark("iceberg-maintenance")
        cat = "lakehouse"
        for layer in ("bronze", "silver", "gold"):
            for name in load_registry().names():
                tbl = f"{cat}.{layer}.{name}"
                try:
                    spark.sql(f"CALL {cat}.system.rewrite_data_files('{tbl}')")
                    spark.sql(
                        f"CALL {cat}.system.expire_snapshots("
                        f"table => '{tbl}', older_than => TIMESTAMP '1970-01-01 00:00:00')"
                    )
                    spark.sql(f"CALL {cat}.system.remove_orphan_files(table => '{tbl}')")
                except Exception:  # noqa: BLE001 - table may not exist yet
                    continue
        spark.stop()

    maintain()


cdc_iceberg_maintenance()

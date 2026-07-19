"""Airflow DAG orchestrating the medallion pipeline.

Streaming (Bronze/Silver) runs continuously outside Airflow; this DAG owns the
*batch* concerns: connector health, Gold rebuild, data-quality gates, and dbt.
Tasks are intentionally small and idempotent so any single failure is retriable.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator

DEFAULT_ARGS = {
    "owner": "data-platform",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "execution_timeout": timedelta(hours=1),
}


@dag(
    dag_id="cdc_medallion",
    description="Batch orchestration for the CDC lakehouse (Gold + DQ + dbt)",
    schedule="*/30 * * * *",  # every 30 min
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["cdc", "medallion", "iceberg"],
)
def cdc_medallion() -> None:
    @task(task_id="check_connectors")
    def check_connectors() -> None:
        from cdc_platform.ingestion.connector_manager import ConnectorManager

        mgr = ConnectorManager()
        name = "commerce-postgres-connector"
        if not mgr.is_healthy(name):
            raise RuntimeError(f"Connector {name} is not healthy: {mgr.status(name)}")

    gold = BashOperator(
        task_id="build_gold",
        bash_command="bash /opt/app/scripts/submit_spark.sh gold",
    )

    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command="cd /opt/app/dbt && dbt build --profiles-dir . --fail-fast",
    )

    @task(task_id="run_quality_gates")
    def run_quality_gates() -> None:
        from cdc_platform.quality.run_checkpoints import run_all

        run_all()

    check_connectors() >> gold >> dbt_build >> run_quality_gates()


cdc_medallion()

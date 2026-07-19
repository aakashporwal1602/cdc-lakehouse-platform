"""Gold batch job: build conformed business marts from Silver.

The canonical Gold transformations live in dbt (see ``dbt/models/marts``) which
is easier to test and document. This Spark job exists for the marts that benefit
from low-latency incremental refresh and can be scheduled far more frequently
than the dbt build.
"""

from __future__ import annotations

from pyspark.sql import SparkSession

from cdc_platform.common.config import get_settings
from cdc_platform.common.logging import configure_logging, get_logger
from cdc_platform.streaming.session import build_spark

log = get_logger("gold")


class GoldBuilder:
    """Materialises Gold marts via CREATE OR REPLACE against Iceberg."""

    def __init__(self, spark: SparkSession) -> None:
        self.spark = spark
        self.cat = get_settings().iceberg.catalog_name
        self.spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {self.cat}.gold")

    def _replace(self, name: str, query: str) -> None:
        self.spark.sql(f"CREATE OR REPLACE TABLE {self.cat}.gold.{name} USING iceberg AS {query}")
        log.info("gold_mart_built", mart=name)

    def revenue_daily(self) -> None:
        self._replace(
            "revenue_daily",
            f"""
            SELECT date(p.payment_ts)              AS revenue_date,
                   count(*)                        AS payments,
                   sum(CASE WHEN p.status='captured' THEN p.amount ELSE 0 END) AS captured_revenue,
                   sum(CASE WHEN p.status='refunded' THEN p.amount ELSE 0 END) AS refunded_revenue,
                   count(DISTINCT p.order_id)      AS paying_orders
            FROM {self.cat}.silver.payments p
            GROUP BY date(p.payment_ts)
            """,
        )

    def inventory_health(self) -> None:
        self._replace(
            "inventory_health",
            f"""
            SELECT i.product_id, pr.name AS product_name, pr.category,
                   i.warehouse_id, i.quantity_on_hand, i.reorder_level,
                   (i.quantity_on_hand <= i.reorder_level) AS needs_reorder,
                   CASE WHEN i.quantity_on_hand = 0 THEN 'stockout'
                        WHEN i.quantity_on_hand <= i.reorder_level THEN 'low'
                        ELSE 'healthy' END AS stock_status
            FROM {self.cat}.silver.inventory i
            JOIN {self.cat}.silver.products pr ON pr.product_id = i.product_id
            """,
        )

    def order_metrics(self) -> None:
        self._replace(
            "order_metrics",
            f"""
            SELECT date(o.order_ts)   AS order_date,
                   o.status,
                   count(*)           AS orders,
                   sum(o.order_total) AS gross_order_value,
                   avg(o.order_total) AS avg_order_value
            FROM {self.cat}.silver.orders o
            GROUP BY date(o.order_ts), o.status
            """,
        )

    def build_all(self) -> None:
        # Each mart is independent; skip (with a warning) any whose source Silver
        # table isn't built yet, so Gold works incrementally as tables land.
        for name, builder in (
            ("revenue_daily", self.revenue_daily),
            ("inventory_health", self.inventory_health),
            ("order_metrics", self.order_metrics),
        ):
            try:
                builder()
            except Exception as exc:  # noqa: BLE001
                log.warning("gold_mart_skipped", mart=name, reason=str(exc)[:200])


def run() -> None:
    configure_logging()
    spark = build_spark("gold-builder")
    GoldBuilder(spark).build_all()
    spark.stop()

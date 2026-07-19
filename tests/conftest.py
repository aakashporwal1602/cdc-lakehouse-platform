"""Shared pytest fixtures."""

from __future__ import annotations

import pytest

from cdc_platform.common.tables import TableRegistry, TableSpec


@pytest.fixture()
def registry() -> TableRegistry:
    tables = {
        "orders": TableSpec(
            name="orders",
            primary_key=["order_id"],
            order_by=["source_lsn", "source_ts_ms"],
            partition_by=["days(order_ts)"],
            dq_suite="orders_suite",
        ),
        "customers": TableSpec(name="customers", primary_key=["customer_id"]),
    }
    return TableRegistry(version=1, source_schema="public", tables=tables)

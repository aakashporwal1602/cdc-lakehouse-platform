"""Validator that executes expectation predicates as Trino SQL.

Each expectation compiles to a COUNT of violating rows; zero violations passes.
This keeps data-quality checks running *in the warehouse* (push-down) rather than
pulling data into Python.
"""

from __future__ import annotations

from typing import Any

from cdc_platform.common.config import get_settings
from cdc_platform.common.logging import get_logger

log = get_logger("quality.trino")


class TrinoValidator:
    """Compiles a small subset of GE expectation types into Trino SQL."""

    def __init__(self, connection: Any | None = None) -> None:
        self._conn = connection  # injected in tests; lazily created otherwise

    def _cursor(self) -> Any:
        if self._conn is None:  # pragma: no cover - requires live Trino
            import trino

            cfg = get_settings()
            self._conn = trino.dbapi.connect(
                host=cfg.iceberg.catalog_name and "trino",
                port=8080,
                user="analytics",
                catalog="lakehouse",
            )
        return self._conn.cursor()

    def _violations(self, sql: str) -> int:
        cur = self._cursor()
        cur.execute(sql)
        return int(cur.fetchone()[0])

    def check(self, expectation: dict) -> bool:
        kind = expectation["expectation_type"]
        kwargs = expectation["kwargs"]
        table = kwargs["table"]
        col = kwargs.get("column")

        if kind == "expect_column_values_to_not_be_null":
            sql = f"SELECT count(*) FROM {table} WHERE {col} IS NULL"
        elif kind == "expect_column_values_to_be_unique":
            sql = (
                f"SELECT count(*) FROM (SELECT {col} FROM {table} "
                f"GROUP BY {col} HAVING count(*) > 1)"
            )
        elif kind == "expect_column_values_to_be_between":
            lo, hi = kwargs["min_value"], kwargs["max_value"]
            sql = f"SELECT count(*) FROM {table} WHERE {col} < {lo} OR {col} > {hi}"
        elif kind == "expect_column_values_to_be_in_set":
            allowed = ", ".join(f"'{v}'" for v in kwargs["value_set"])
            sql = f"SELECT count(*) FROM {table} WHERE {col} NOT IN ({allowed})"
        else:
            raise ValueError(f"Unsupported expectation: {kind}")

        violations = self._violations(sql)
        return violations == 0

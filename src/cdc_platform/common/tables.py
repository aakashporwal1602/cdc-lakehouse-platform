"""Typed access to the table registry (``configs/tables.yml``).

The registry drives every table-specific behaviour (primary keys, dedup
ordering, partitioning, DQ suite) so that the streaming engine stays fully
generic. This is the Open/Closed principle in practice: new tables extend the
config, not the code.
"""

from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path

import yaml
from pydantic import BaseModel, Field

_DEFAULT_REGISTRY = Path(
    os.getenv("TABLES_REGISTRY_PATH", "configs/tables.yml")
)


class TableSpec(BaseModel):
    """Declarative specification for one replicated table."""

    name: str
    primary_key: list[str]
    order_by: list[str] = Field(default_factory=lambda: ["source_lsn", "source_ts_ms"])
    partition_by: list[str] = Field(default_factory=list)
    scd: str = "type1"
    dq_suite: str | None = None

    def topic(self, prefix: str, schema: str) -> str:
        """Debezium topic name for this table (``prefix.schema.table``)."""

        return f"{prefix}.{schema}.{self.name}"


class TableRegistry(BaseModel):
    """The full registry loaded from YAML."""

    version: int
    source_schema: str
    tables: dict[str, TableSpec]

    def get(self, name: str) -> TableSpec:
        if name not in self.tables:
            raise KeyError(f"Unknown table '{name}'. Known: {list(self.tables)}")
        return self.tables[name]

    def names(self) -> list[str]:
        return list(self.tables)


@lru_cache(maxsize=1)
def load_registry(path: str | os.PathLike[str] | None = None) -> TableRegistry:
    """Load and validate the table registry (cached)."""

    registry_path = Path(path) if path else _DEFAULT_REGISTRY
    raw = yaml.safe_load(registry_path.read_text(encoding="utf-8"))
    tables = {
        name: TableSpec(name=name, **spec) for name, spec in raw["tables"].items()
    }
    return TableRegistry(
        version=raw["version"],
        source_schema=raw["source_schema"],
        tables=tables,
    )

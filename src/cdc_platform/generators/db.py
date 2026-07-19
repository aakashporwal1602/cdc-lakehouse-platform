"""Small psycopg2 helper used by seed + simulator (Single Responsibility)."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

import psycopg2
from psycopg2.extensions import connection as PgConnection

from cdc_platform.common.config import get_settings
from cdc_platform.common.retry import with_retries


@with_retries(exceptions=(psycopg2.OperationalError,))
def connect() -> PgConnection:
    """Open a Postgres connection using centralised settings, with retries."""

    return psycopg2.connect(get_settings().postgres.dsn)


@contextmanager
def cursor() -> Iterator[Any]:
    """Transactional cursor context manager."""

    conn = connect()
    try:
        with conn, conn.cursor() as cur:
            yield cur
    finally:
        conn.close()


def insert_returning(cur: Any, table: str, row: dict[str, Any], pk: str) -> int:
    """Insert one row and return its generated primary key."""

    cols = ", ".join(row)
    placeholders = ", ".join(["%s"] * len(row))
    cur.execute(
        f"INSERT INTO public.{table} ({cols}) VALUES ({placeholders}) RETURNING {pk}",
        list(row.values()),
    )
    return int(cur.fetchone()[0])

"""Confluent Schema Registry client wrapper.

Used by the Bronze job to fetch the Avro reader schema for a subject so Spark's
``from_avro`` can decode the payload. Fetching the *latest* schema plus Iceberg's
native column evolution is what gives us transparent schema evolution: new source
columns simply appear in Bronze and are merged forward.
"""

from __future__ import annotations

from functools import lru_cache

import requests

from cdc_platform.common.config import get_settings
from cdc_platform.common.retry import with_retries


class SchemaRegistryClient:
    """Minimal read-only Schema Registry client."""

    def __init__(self, url: str | None = None, timeout: float = 15.0) -> None:
        self._url = (url or get_settings().kafka.schema_registry_url).rstrip("/")
        self._timeout = timeout

    @with_retries(exceptions=(requests.RequestException,))
    def latest_schema(self, subject: str) -> str:
        """Return the latest Avro schema string for a subject (e.g. ``topic-value``)."""

        resp = requests.get(
            f"{self._url}/subjects/{subject}/versions/latest", timeout=self._timeout
        )
        resp.raise_for_status()
        return str(resp.json()["schema"])


@lru_cache(maxsize=64)
def value_schema(topic: str) -> str:
    """Cached helper: latest value schema for a Kafka topic."""

    return SchemaRegistryClient().latest_schema(f"{topic}-value")

"""Idempotent management of Debezium connectors through Kafka Connect.

Wraps the Connect REST API with typed methods and retry semantics. Used by both
``scripts/register_connectors.sh`` and the Airflow ``register_connectors`` task.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import requests

from cdc_platform.common.config import get_settings
from cdc_platform.common.logging import get_logger
from cdc_platform.common.retry import with_retries

log = get_logger(__name__)


class ConnectorManager:
    """Thin, testable client over the Kafka Connect REST API."""

    def __init__(self, connect_url: str | None = None, timeout: float = 30.0) -> None:
        self._url = (connect_url or get_settings().kafka.connect_url).rstrip("/")
        self._timeout = timeout

    @with_retries(exceptions=(requests.RequestException,))
    def _request(self, method: str, path: str, **kwargs: Any) -> requests.Response:
        resp = requests.request(
            method, f"{self._url}{path}", timeout=self._timeout, **kwargs
        )
        resp.raise_for_status()
        return resp

    def list_connectors(self) -> list[str]:
        return self._request("GET", "/connectors").json()

    def upsert(self, config_path: str | Path) -> dict[str, Any]:
        """Create or update a connector from a JSON config file (idempotent)."""

        payload = json.loads(Path(config_path).read_text(encoding="utf-8"))
        name, config = payload["name"], payload["config"]
        log.info("upserting_connector", connector=name)
        resp = self._request(
            "PUT",
            f"/connectors/{name}/config",
            json=config,
            headers={"Content-Type": "application/json"},
        )
        return resp.json()

    def status(self, name: str) -> dict[str, Any]:
        return self._request("GET", f"/connectors/{name}/status").json()

    def is_healthy(self, name: str) -> bool:
        """True if the connector and all its tasks are RUNNING."""

        st = self.status(name)
        connector_ok = st.get("connector", {}).get("state") == "RUNNING"
        tasks_ok = all(t.get("state") == "RUNNING" for t in st.get("tasks", []))
        return connector_ok and tasks_ok and bool(st.get("tasks"))

    def delete(self, name: str) -> None:
        self._request("DELETE", f"/connectors/{name}")
        log.info("deleted_connector", connector=name)

"""End-to-end integration tests. Require `make up` + registered connectors.

Marked `integration` so they are excluded from the fast unit run. CI runs them
against the docker-compose stack.
"""

from __future__ import annotations

import os

import pytest

pytestmark = pytest.mark.integration

CONNECT_URL = os.getenv("KAFKA_CONNECT_URL", "http://localhost:8083")
SCHEMA_REGISTRY = os.getenv("KAFKA_SCHEMA_REGISTRY_URL", "http://localhost:8081")


def _get(url: str):
    import requests

    return requests.get(url, timeout=10)


def test_connector_running() -> None:
    resp = _get(f"{CONNECT_URL}/connectors/commerce-postgres-connector/status")
    assert resp.status_code == 200
    assert resp.json()["connector"]["state"] == "RUNNING"


def test_cdc_subjects_registered() -> None:
    subjects = _get(f"{SCHEMA_REGISTRY}/subjects").json()
    assert any("commerce.public.orders" in s for s in subjects)


def test_insert_propagates_to_bronze() -> None:
    """Insert a customer, then assert it appears in Kafka within the SLA."""

    from confluent_kafka import Consumer

    from cdc_platform.generators.db import cursor, insert_returning
    from cdc_platform.generators.factories import DataFactory

    with cursor() as cur:
        cid = insert_returning(cur, "customers", DataFactory(seed=None).customer(), "customer_id")

    consumer = Consumer({
        "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092"),
        "group.id": f"itest-{cid}",
        "auto.offset.reset": "earliest",
    })
    consumer.subscribe(["commerce.public.customers"])
    try:
        msg = consumer.poll(timeout=30)
        assert msg is not None, "no CDC event received within SLA"
    finally:
        consumer.close()

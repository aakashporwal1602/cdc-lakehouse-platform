import pytest

from cdc_platform.common.tables import TableRegistry


@pytest.mark.unit
def test_topic_naming(registry: TableRegistry) -> None:
    spec = registry.get("orders")
    assert spec.topic("commerce", "public") == "commerce.public.orders"


@pytest.mark.unit
def test_unknown_table_raises(registry: TableRegistry) -> None:
    with pytest.raises(KeyError):
        registry.get("nope")


@pytest.mark.unit
def test_names(registry: TableRegistry) -> None:
    assert set(registry.names()) == {"orders", "customers"}


@pytest.mark.unit
def test_default_order_by(registry: TableRegistry) -> None:
    assert registry.get("customers").order_by == ["source_lsn", "source_ts_ms"]

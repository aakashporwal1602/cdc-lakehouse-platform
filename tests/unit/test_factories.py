import pytest

from cdc_platform.generators.factories import DataFactory


@pytest.mark.unit
def test_customer_shape() -> None:
    row = DataFactory(seed=1).customer()
    assert {"email", "full_name", "tier", "country", "is_active"} <= set(row)
    assert row["tier"] in {"standard", "silver", "gold", "platinum"}


@pytest.mark.unit
def test_payment_amount_matches_order() -> None:
    f = DataFactory(seed=1)
    order = f.order(customer_id=10)
    pay = f.payment(order_id=5, amount=order["order_total"])
    assert pay["amount"] == order["order_total"]
    assert pay["order_id"] == 5


@pytest.mark.unit
def test_deterministic_with_seed() -> None:
    assert DataFactory(seed=7).product()["sku"] == DataFactory(seed=7).product()["sku"]

"""Deterministic-ish domain object factories built on Faker.

Kept free of any database concern (Dependency Inversion): factories emit plain
dicts, and callers decide how to persist them. This makes them trivially unit
testable without a live Postgres.
"""

from __future__ import annotations

import random
from datetime import datetime, timezone
from decimal import Decimal

from faker import Faker

_TIERS = ("standard", "silver", "gold", "platinum")
_CATEGORIES = ("electronics", "apparel", "home", "grocery", "toys", "beauty")
_ORDER_STATUS = ("pending", "confirmed", "shipped", "delivered", "cancelled")
_PAY_METHODS = ("card", "paypal", "apple_pay", "bank_transfer")
_PAY_STATUS = ("authorized", "captured", "failed", "refunded")
_WAREHOUSES = ("WH-US-EAST", "WH-US-WEST", "WH-EU-CENTRAL", "WH-APAC")


class DataFactory:
    """Generates domain records with referential integrity across entities."""

    def __init__(self, seed: int | None = 42) -> None:
        self._fake = Faker()
        if seed is not None:
            Faker.seed(seed)
            random.seed(seed)

    @staticmethod
    def _now() -> datetime:
        return datetime.now(tz=timezone.utc)

    def customer(self) -> dict[str, object]:
        return {
            "email": self._fake.unique.email(),
            "full_name": self._fake.name(),
            "tier": random.choices(_TIERS, weights=(60, 25, 12, 3))[0],
            "country": self._fake.country_code(),
            "is_active": random.random() > 0.05,
        }

    def product(self) -> dict[str, object]:
        return {
            "sku": self._fake.unique.bothify("SKU-####-????").upper(),
            "name": self._fake.catch_phrase(),
            "category": random.choice(_CATEGORIES),
            "unit_price": Decimal(str(round(random.uniform(4.99, 999.99), 2))),
            "is_active": True,
        }

    def inventory(self, product_id: int) -> dict[str, object]:
        return {
            "product_id": product_id,
            "warehouse_id": random.choice(_WAREHOUSES),
            "quantity_on_hand": random.randint(0, 5000),
            "reorder_level": random.choice((10, 25, 50, 100)),
        }

    def order(self, customer_id: int) -> dict[str, object]:
        return {
            "customer_id": customer_id,
            "status": random.choices(_ORDER_STATUS, weights=(20, 25, 20, 30, 5))[0],
            "order_total": Decimal(str(round(random.uniform(9.99, 2500.0), 2))),
            "currency": "USD",
        }

    def payment(self, order_id: int, amount: Decimal) -> dict[str, object]:
        return {
            "order_id": order_id,
            "method": random.choice(_PAY_METHODS),
            "amount": amount,
            "status": random.choices(_PAY_STATUS, weights=(15, 70, 10, 5))[0],
        }

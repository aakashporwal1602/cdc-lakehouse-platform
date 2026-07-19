"""Seed the source database with a realistic, referentially-consistent dataset.

Run: ``python -m cdc_platform.generators.seed_source [--customers N]``
"""

from __future__ import annotations

import argparse

from cdc_platform.common.logging import configure_logging, get_logger
from cdc_platform.generators.db import cursor, insert_returning
from cdc_platform.generators.factories import DataFactory

log = get_logger(__name__)


def seed(customers: int, products: int, orders: int) -> None:
    factory = DataFactory()
    with cursor() as cur:
        product_ids = [
            insert_returning(cur, "products", factory.product(), "product_id")
            for _ in range(products)
        ]
        for pid in product_ids:
            insert_returning(cur, "inventory", factory.inventory(pid), "inventory_id")

        customer_ids = [
            insert_returning(cur, "customers", factory.customer(), "customer_id")
            for _ in range(customers)
        ]

        for _ in range(orders):
            cid = __import__("random").choice(customer_ids)
            order = factory.order(cid)
            oid = insert_returning(cur, "orders", order, "order_id")
            payment = factory.payment(oid, order["order_total"])  # type: ignore[arg-type]
            insert_returning(cur, "payments", payment, "payment_id")

    log.info(
        "seed_complete",
        customers=customers,
        products=products,
        orders=orders,
    )


def main() -> None:
    configure_logging()
    parser = argparse.ArgumentParser(description="Seed the commerce source DB")
    parser.add_argument("--customers", type=int, default=1000)
    parser.add_argument("--products", type=int, default=300)
    parser.add_argument("--orders", type=int, default=5000)
    args = parser.parse_args()
    seed(args.customers, args.products, args.orders)


if __name__ == "__main__":
    main()

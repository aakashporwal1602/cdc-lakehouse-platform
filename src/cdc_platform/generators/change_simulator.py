"""Continuously emit INSERT / UPDATE / DELETE traffic to exercise CDC.

Produces a realistic mixed workload so the whole pipeline (Debezium -> Kafka ->
Spark -> Iceberg) can be observed under motion, including deletes (tombstones)
and rapid updates (dedup / out-of-order handling).

Run: ``python -m cdc_platform.generators.change_simulator --rate 20``
"""

from __future__ import annotations

import argparse
import random
import time
from decimal import Decimal

from cdc_platform.common.logging import configure_logging, get_logger
from cdc_platform.generators.db import cursor, insert_returning
from cdc_platform.generators.factories import DataFactory

log = get_logger(__name__)

_ORDER_STATUS = ("pending", "confirmed", "shipped", "delivered", "cancelled")


class ChangeSimulator:
    """Applies weighted random mutations to keep the CDC stream flowing."""

    def __init__(self, factory: DataFactory | None = None) -> None:
        self._factory = factory or DataFactory(seed=None)

    def _ids(self, cur: object, table: str, pk: str) -> list[int]:
        cur.execute(f"SELECT {pk} FROM public.{table} ORDER BY random() LIMIT 200")  # type: ignore[attr-defined]
        return [int(r[0]) for r in cur.fetchall()]  # type: ignore[attr-defined]

    def tick(self) -> str:
        """Perform a single randomly-chosen mutation; return its label."""

        action = random.choices(
            ("insert_order", "update_order", "update_inventory",
             "update_customer", "delete_payment"),
            weights=(35, 30, 20, 10, 5),
        )[0]
        with cursor() as cur:
            if action == "insert_order":
                cid = random.choice(self._ids(cur, "customers", "customer_id"))
                order = self._factory.order(cid)
                oid = insert_returning(cur, "orders", order, "order_id")
                insert_returning(
                    cur, "payments",
                    self._factory.payment(oid, order["order_total"]),  # type: ignore[arg-type]
                    "payment_id",
                )
            elif action == "update_order":
                oid = random.choice(self._ids(cur, "orders", "order_id"))
                cur.execute(
                    "UPDATE public.orders SET status=%s WHERE order_id=%s",
                    (random.choice(_ORDER_STATUS), oid),
                )
            elif action == "update_inventory":
                iid = random.choice(self._ids(cur, "inventory", "inventory_id"))
                cur.execute(
                    "UPDATE public.inventory SET quantity_on_hand=%s WHERE inventory_id=%s",
                    (random.randint(0, 5000), iid),
                )
            elif action == "update_customer":
                cid = random.choice(self._ids(cur, "customers", "customer_id"))
                cur.execute(
                    "UPDATE public.customers SET tier=%s WHERE customer_id=%s",
                    (random.choice(("standard", "silver", "gold", "platinum")), cid),
                )
            else:  # delete_payment
                pids = self._ids(cur, "payments", "payment_id")
                if pids:
                    cur.execute(
                        "DELETE FROM public.payments WHERE payment_id=%s",
                        (random.choice(pids),),
                    )
        return action

    def run(self, rate_per_sec: float, duration_sec: float | None) -> None:
        interval = 1.0 / max(rate_per_sec, 0.1)
        started = time.monotonic()
        emitted = 0
        while duration_sec is None or (time.monotonic() - started) < duration_sec:
            label = self.tick()
            emitted += 1
            if emitted % 50 == 0:
                log.info("emitted", count=emitted, last=label)
            time.sleep(interval)


def main() -> None:
    configure_logging()
    parser = argparse.ArgumentParser(description="CDC change simulator")
    parser.add_argument("--rate", type=float, default=10.0, help="mutations/sec")
    parser.add_argument("--duration", type=float, default=None, help="seconds (default: forever)")
    args = parser.parse_args()
    ChangeSimulator().run(args.rate, args.duration)


if __name__ == "__main__":
    main()

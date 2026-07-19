"""Unified streaming CLI.

Examples::

    python -m cdc_platform.streaming bronze --table orders
    python -m cdc_platform.streaming silver --table orders
    python -m cdc_platform.streaming gold
"""

from __future__ import annotations

import argparse

from cdc_platform.common.config import get_settings
from cdc_platform.common.logging import configure_logging
from cdc_platform.streaming.bronze import job as bronze_job
from cdc_platform.streaming.gold import job as gold_job
from cdc_platform.streaming.silver import job as silver_job


def main() -> None:
    settings = get_settings()
    configure_logging(settings.log_level, settings.log_format)

    parser = argparse.ArgumentParser(prog="cdc_platform.streaming")
    sub = parser.add_subparsers(dest="layer", required=True)

    for layer in ("bronze", "silver"):
        p = sub.add_parser(layer, help=f"run the {layer} streaming job")
        p.add_argument("--table", required=True, help="registered table name")

    sub.add_parser("gold", help="run the gold batch build")

    args = parser.parse_args()
    if args.layer == "bronze":
        bronze_job.run(args.table)
    elif args.layer == "silver":
        silver_job.run(args.table)
    else:
        gold_job.run()


if __name__ == "__main__":
    main()

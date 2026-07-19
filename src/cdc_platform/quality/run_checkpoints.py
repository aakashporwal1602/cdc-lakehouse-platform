"""Run data-quality checkpoints and surface results as metrics + exit codes.

Kept deliberately thin and dependency-light: it loads the declarative
expectation suites (JSON), executes them through a pluggable validator, records
failures to Prometheus, and raises on any hard-fail so upstream orchestration
(Airflow) blocks bad data from propagating.
"""

from __future__ import annotations

import json
import sys
from dataclasses import dataclass
from pathlib import Path

from cdc_platform.common.logging import configure_logging, get_logger
from cdc_platform.common.metrics import DQ_VALIDATION_FAILURES

log = get_logger("quality")

_SUITE_DIR = Path("great_expectations/expectations")


@dataclass(frozen=True)
class SuiteResult:
    suite: str
    table: str
    passed: int
    failed: int

    @property
    def ok(self) -> bool:
        return self.failed == 0


def load_suites(directory: Path = _SUITE_DIR) -> list[dict]:
    return [json.loads(p.read_text(encoding="utf-8")) for p in sorted(directory.glob("*.json"))]


def evaluate(suite: dict, validator) -> SuiteResult:  # noqa: ANN001
    """Evaluate one suite. ``validator`` is any object exposing ``check(expectation)``.

    Injecting the validator (Dependency Inversion) lets unit tests run without a
    live Great Expectations / Spark context.
    """

    table = suite["meta"].get("table", "unknown")
    passed = failed = 0
    for expectation in suite["expectations"]:
        if validator.check(expectation):
            passed += 1
        else:
            failed += 1
            DQ_VALIDATION_FAILURES.labels(suite["suite_name"], table).inc()
            log.warning("expectation_failed", suite=suite["suite_name"], exp=expectation)
    return SuiteResult(suite["suite_name"], table, passed, failed)


def run_all(validator=None) -> list[SuiteResult]:  # noqa: ANN001
    from cdc_platform.quality.trino_validator import TrinoValidator

    validator = validator or TrinoValidator()
    results = [evaluate(s, validator) for s in load_suites()]
    for r in results:
        log.info("suite_result", suite=r.suite, passed=r.passed, failed=r.failed)
    if any(not r.ok for r in results):
        raise SystemExit(f"Data-quality gate failed: {[r.suite for r in results if not r.ok]}")
    return results


def main() -> None:
    configure_logging()
    try:
        run_all()
    except SystemExit as exc:
        log.error("dq_gate_failed", detail=str(exc))
        sys.exit(1)


if __name__ == "__main__":
    main()

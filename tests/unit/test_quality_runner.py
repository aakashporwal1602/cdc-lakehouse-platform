import pytest

from cdc_platform.quality.run_checkpoints import SuiteResult, evaluate, run_all


class _StubValidator:
    def __init__(self, fail_types: set[str] | None = None) -> None:
        self._fail = fail_types or set()

    def check(self, expectation: dict) -> bool:
        return expectation["expectation_type"] not in self._fail


_SUITE = {
    "suite_name": "orders_suite",
    "meta": {"table": "lakehouse.silver.orders"},
    "expectations": [
        {"expectation_type": "expect_column_values_to_not_be_null",
         "kwargs": {"table": "t", "column": "order_id"}},
        {"expectation_type": "expect_column_values_to_be_unique",
         "kwargs": {"table": "t", "column": "order_id"}},
    ],
}


@pytest.mark.unit
def test_all_pass() -> None:
    r = evaluate(_SUITE, _StubValidator())
    assert isinstance(r, SuiteResult)
    assert r.ok and r.passed == 2 and r.failed == 0


@pytest.mark.unit
def test_some_fail() -> None:
    r = evaluate(_SUITE, _StubValidator({"expect_column_values_to_be_unique"}))
    assert not r.ok and r.failed == 1


@pytest.mark.unit
def test_run_all_raises_on_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    from cdc_platform.quality import run_checkpoints

    monkeypatch.setattr(run_checkpoints, "load_suites", lambda: [_SUITE])
    with pytest.raises(SystemExit):
        run_all(validator=_StubValidator({"expect_column_values_to_not_be_null"}))

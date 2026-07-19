import pytest

from cdc_platform.quality.trino_validator import TrinoValidator


class _FakeCursor:
    def __init__(self, value: int) -> None:
        self._value = value
        self.last_sql = ""

    def execute(self, sql: str) -> None:
        self.last_sql = sql

    def fetchone(self):
        return (self._value,)


class _FakeConn:
    def __init__(self, value: int) -> None:
        self._cur = _FakeCursor(value)

    def cursor(self):
        return self._cur


@pytest.mark.unit
def test_not_null_passes_when_zero_violations() -> None:
    v = TrinoValidator(connection=_FakeConn(0))
    assert v.check({
        "expectation_type": "expect_column_values_to_not_be_null",
        "kwargs": {"table": "t", "column": "c"},
    })


@pytest.mark.unit
def test_range_fails_when_violations() -> None:
    v = TrinoValidator(connection=_FakeConn(3))
    assert not v.check({
        "expectation_type": "expect_column_values_to_be_between",
        "kwargs": {"table": "t", "column": "amount", "min_value": 0, "max_value": 10},
    })


@pytest.mark.unit
def test_unsupported_expectation_raises() -> None:
    v = TrinoValidator(connection=_FakeConn(0))
    with pytest.raises(ValueError):
        v.check({"expectation_type": "nope", "kwargs": {"table": "t"}})

import pytest

from cdc_platform.common.retry import with_retries


@pytest.mark.unit
def test_retries_then_succeeds() -> None:
    calls = {"n": 0}

    @with_retries(attempts=3, min_wait=0.001, max_wait=0.002, exceptions=(ValueError,))
    def flaky() -> str:
        calls["n"] += 1
        if calls["n"] < 3:
            raise ValueError("transient")
        return "ok"

    assert flaky() == "ok"
    assert calls["n"] == 3


@pytest.mark.unit
def test_reraises_after_exhaustion() -> None:
    @with_retries(attempts=2, min_wait=0.001, max_wait=0.002, exceptions=(KeyError,))
    def always_fail() -> None:
        raise KeyError("boom")

    with pytest.raises(KeyError):
        always_fail()

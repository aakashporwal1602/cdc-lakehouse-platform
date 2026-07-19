import pytest

from cdc_platform.ingestion.connector_manager import ConnectorManager


class _Resp:
    def __init__(self, payload: dict) -> None:
        self._payload = payload

    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict:
        return self._payload


@pytest.mark.unit
def test_is_healthy_true(monkeypatch: pytest.MonkeyPatch) -> None:
    mgr = ConnectorManager(connect_url="http://x")
    monkeypatch.setattr(
        mgr, "status",
        lambda name: {"connector": {"state": "RUNNING"},
                      "tasks": [{"state": "RUNNING"}]},
    )
    assert mgr.is_healthy("c")


@pytest.mark.unit
def test_is_healthy_false_when_task_failed(monkeypatch: pytest.MonkeyPatch) -> None:
    mgr = ConnectorManager(connect_url="http://x")
    monkeypatch.setattr(
        mgr, "status",
        lambda name: {"connector": {"state": "RUNNING"},
                      "tasks": [{"state": "FAILED"}]},
    )
    assert not mgr.is_healthy("c")

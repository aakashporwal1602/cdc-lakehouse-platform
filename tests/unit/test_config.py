import pytest

from cdc_platform.common.config import Settings, get_settings


@pytest.mark.unit
def test_defaults_are_sane() -> None:
    s = Settings()
    assert s.postgres.port == 5432
    assert s.postgres.jdbc_url.startswith("jdbc:postgresql://")
    assert "dbname" in s.postgres.dsn


@pytest.mark.unit
def test_env_override(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("POSTGRES_PORT", "6543")
    from cdc_platform.common.config import PostgresSettings

    assert PostgresSettings().port == 6543


@pytest.mark.unit
def test_settings_cached() -> None:
    assert get_settings() is get_settings()

"""Centralised, typed, environment-driven configuration (12-factor).

All runtime configuration flows through :class:`Settings`. Nothing in the
codebase reads ``os.environ`` directly, which keeps configuration testable and
avoids hardcoded values scattered across modules (SOLID: single source of truth).
"""

from __future__ import annotations

from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class PostgresSettings(BaseSettings):
    """Source OLTP database connection + logical-replication settings."""

    model_config = SettingsConfigDict(env_prefix="POSTGRES_", extra="ignore")

    host: str = "localhost"
    port: int = 5432
    db: str = "commerce"
    user: str = "cdc_admin"
    password: str = "change_me_in_prod"
    replication_slot: str = "debezium_slot"
    publication: str = "dbz_publication"

    @property
    def jdbc_url(self) -> str:
        return f"jdbc:postgresql://{self.host}:{self.port}/{self.db}"

    @property
    def dsn(self) -> str:
        return (
            f"host={self.host} port={self.port} dbname={self.db} "
            f"user={self.user} password={self.password}"
        )


class KafkaSettings(BaseSettings):
    """Kafka + Connect + Schema Registry endpoints."""

    model_config = SettingsConfigDict(env_prefix="KAFKA_", extra="ignore")

    bootstrap_servers: str = "localhost:9092"
    schema_registry_url: str = "http://localhost:8081"
    connect_url: str = "http://localhost:8083"


class IcebergSettings(BaseSettings):
    """Iceberg REST catalog + object-store warehouse settings."""

    model_config = SettingsConfigDict(env_prefix="ICEBERG_", extra="ignore")

    catalog_uri: str = "http://localhost:8181"
    catalog_name: str = "lakehouse"
    warehouse: str = "s3a://lakehouse/warehouse"


class S3Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="S3_", extra="ignore")

    endpoint: str = "http://localhost:9000"
    access_key: str = "minioadmin"
    secret_key: str = "minioadmin"
    region: str = "us-east-1"


class SparkSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="SPARK_", extra="ignore")

    master_url: str = "local[*]"
    checkpoint_root: str = "s3a://lakehouse/_checkpoints"
    max_offsets_per_trigger: int = 50_000
    shuffle_partitions: int = 64


class Settings(BaseSettings):
    """Root settings aggregate. Import :func:`get_settings` everywhere."""

    model_config = SettingsConfigDict(extra="ignore")

    environment: str = Field(default="local")
    log_level: str = Field(default="INFO")
    log_format: str = Field(default="json")
    cdc_topic_prefix: str = Field(default="commerce", alias="CDC_TOPIC_PREFIX")

    postgres: PostgresSettings = Field(default_factory=PostgresSettings)
    kafka: KafkaSettings = Field(default_factory=KafkaSettings)
    iceberg: IcebergSettings = Field(default_factory=IcebergSettings)
    s3: S3Settings = Field(default_factory=S3Settings)
    spark: SparkSettings = Field(default_factory=SparkSettings)


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return a process-wide cached :class:`Settings` instance."""

    return Settings()

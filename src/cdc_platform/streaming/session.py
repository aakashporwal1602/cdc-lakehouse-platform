"""Factory for a correctly-configured SparkSession.

All Iceberg / Kafka / S3A wiring lives here so the individual jobs never repeat
configuration (DRY). Everything is derived from :class:`Settings` — no literals.
"""

from __future__ import annotations

from pyspark.sql import SparkSession

from cdc_platform.common.config import Settings, get_settings

_ICEBERG_EXT = "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"


def build_spark(app_name: str, settings: Settings | None = None) -> SparkSession:
    """Create a SparkSession wired for Iceberg REST catalog + MinIO/S3 + Kafka."""

    cfg = settings or get_settings()
    ib, s3, spark = cfg.iceberg, cfg.s3, cfg.spark

    builder = (
        SparkSession.builder.appName(app_name)
        .master(spark.master_url)
        .config("spark.sql.extensions", _ICEBERG_EXT)
        .config(f"spark.sql.catalog.{ib.catalog_name}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{ib.catalog_name}.type", "rest")
        .config(f"spark.sql.catalog.{ib.catalog_name}.uri", ib.catalog_uri)
        .config(f"spark.sql.catalog.{ib.catalog_name}.warehouse", ib.warehouse)
        .config(f"spark.sql.catalog.{ib.catalog_name}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config(f"spark.sql.catalog.{ib.catalog_name}.s3.endpoint", s3.endpoint)
        .config(f"spark.sql.catalog.{ib.catalog_name}.s3.path-style-access", "true")
        # Iceberg S3FileIO (AWS SDK v2) needs an explicit region + credentials
        # when talking to MinIO -- the default provider chain finds none.
        .config(f"spark.sql.catalog.{ib.catalog_name}.client.region", s3.region)
        .config(f"spark.sql.catalog.{ib.catalog_name}.s3.access-key-id", s3.access_key)
        .config(f"spark.sql.catalog.{ib.catalog_name}.s3.secret-access-key", s3.secret_key)
        .config("spark.sql.defaultCatalog", ib.catalog_name)
        .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse")
        .config("spark.sql.shuffle.partitions", str(spark.shuffle_partitions))
        # S3A (for checkpoints)
        .config("spark.hadoop.fs.s3a.endpoint", s3.endpoint)
        .config("spark.hadoop.fs.s3a.access.key", s3.access_key)
        .config("spark.hadoop.fs.s3a.secret.key", s3.secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        # sane streaming defaults
        .config("spark.sql.streaming.stateStore.stateSchemaCheck", "true")
        .config("spark.sql.adaptive.enabled", "true")
    )
    return builder.getOrCreate()

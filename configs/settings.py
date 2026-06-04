"""
Central configuration for the e-commerce data platform.

Local portfolio mode (default):
  - Data lake: parquet files under ./data/lake (mounted in Docker)
  - Warehouse: PostgreSQL `warehouse` schema (simulates Redshift)
  - Glue/Redshift: reference implementations only (see glue_jobs/, redshift/)

Production mode: set DATA_LAKE_MODE=s3 and AWS_* variables.
"""

import os
from dataclasses import dataclass, field
from typing import Optional


def _env_int(key: str, default: int) -> int:
    return int(os.getenv(key, str(default)))


def _env_bool(key: str, default: bool) -> bool:
    return os.getenv(key, str(default)).lower() in ("1", "true", "yes")


@dataclass(frozen=True)
class KafkaConfig:
    bootstrap_servers: str = field(
        default_factory=lambda: os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
    )
    bootstrap_servers_external: str = field(
        default_factory=lambda: os.getenv(
            "KAFKA_BOOTSTRAP_SERVERS_EXTERNAL", "localhost:9092"
        )
    )
    orders_topic: str = "orders"
    payments_topic: str = "payments"
    clicks_topic: str = "clicks"
    users_topic: str = "users"
    inventory_updates_topic: str = "inventory_updates"
    cdc_topic_prefix: str = "ecommerce_cdc"
    default_partitions: int = 6
    replication_factor: int = 1


@dataclass(frozen=True)
class PostgresConfig:
    host: str = field(default_factory=lambda: os.getenv("POSTGRES_HOST", "postgres"))
    port: int = field(default_factory=lambda: _env_int("POSTGRES_PORT", 5432))
    database: str = field(default_factory=lambda: os.getenv("POSTGRES_DB", "ecommerce"))
    user: str = field(
        default_factory=lambda: os.getenv("POSTGRES_USER", "ecommerce_user")
    )
    password: str = field(
        default_factory=lambda: os.getenv("POSTGRES_PASSWORD", "ecommerce_pass")
    )
    # Metrics + warehouse live in the same DB (simpler local setup)
    metrics_database: str = field(
        default_factory=lambda: os.getenv("POSTGRES_METRICS_DB", "ecommerce")
    )
    metrics_schema: str = field(
        default_factory=lambda: os.getenv("POSTGRES_METRICS_SCHEMA", "realtime")
    )
    warehouse_schema: str = field(
        default_factory=lambda: os.getenv("POSTGRES_WAREHOUSE_SCHEMA", "warehouse")
    )

    @property
    def jdbc_url(self) -> str:
        return f"jdbc:postgresql://{self.host}:{self.port}/{self.database}"

    @property
    def metrics_jdbc_url(self) -> str:
        return f"jdbc:postgresql://{self.host}:{self.port}/{self.metrics_database}"

    @property
    def connection_string(self) -> str:
        return (
            f"postgresql://{self.user}:{self.password}"
            f"@{self.host}:{self.port}/{self.metrics_database}"
        )


@dataclass(frozen=True)
class SparkConfig:
    master: str = field(
        default_factory=lambda: os.getenv("SPARK_MASTER", "spark://spark-master:7077")
    )
    app_name: str = field(
        default_factory=lambda: os.getenv("SPARK_APP_NAME", "ecommerce-streaming")
    )
    checkpoint_base: str = field(
        default_factory=lambda: os.getenv(
            "SPARK_CHECKPOINT_BASE", "/data-lake/checkpoints"
        )
    )
    trigger_interval: str = field(
        default_factory=lambda: os.getenv("SPARK_TRIGGER_INTERVAL", "30 seconds")
    )
    watermark_delay: str = field(
        default_factory=lambda: os.getenv("SPARK_WATERMARK_DELAY", "10 minutes")
    )


@dataclass(frozen=True)
class DataLakeConfig:
    """
    Local mode: file:///data-lake (Docker volume → ./data/lake)
    S3 mode:    s3a://bucket/... (LocalStack, MinIO, or real AWS)
    """

    mode: str = field(
        default_factory=lambda: os.getenv("DATA_LAKE_MODE", "local")
    )
    root: str = field(
        default_factory=lambda: os.getenv("DATA_LAKE_ROOT", "/data-lake")
    )
    bucket: str = field(
        default_factory=lambda: os.getenv("S3_DATA_LAKE_BUCKET", "ecommerce-data-lake")
    )
    endpoint_url: Optional[str] = field(
        default_factory=lambda: os.getenv("S3_ENDPOINT_URL") or None
    )

    @property
    def bronze_path(self) -> str:
        if self.mode == "local":
            return f"file://{self.root}/bronze/"
        return f"s3a://{self.bucket}/bronze/"

    @property
    def silver_path(self) -> str:
        if self.mode == "local":
            return f"file://{self.root}/silver/"
        return f"s3a://{self.bucket}/silver/"

    @property
    def gold_path(self) -> str:
        if self.mode == "local":
            return f"file://{self.root}/gold/"
        return f"s3a://{self.bucket}/gold/"


@dataclass(frozen=True)
class RedshiftConfig:
    """Simulated locally via PostgreSQL warehouse schema."""
    host: str = field(default_factory=lambda: os.getenv("REDSHIFT_HOST", ""))
    port: int = field(default_factory=lambda: _env_int("REDSHIFT_PORT", 5439))
    database: str = field(
        default_factory=lambda: os.getenv("REDSHIFT_DB", "ecommerce_dw")
    )
    user: str = field(default_factory=lambda: os.getenv("REDSHIFT_USER", "admin"))
    password: str = field(default_factory=lambda: os.getenv("REDSHIFT_PASSWORD", ""))
    iam_role: str = field(
        default_factory=lambda: os.getenv(
            "REDSHIFT_IAM_ROLE", "arn:aws:iam::000000000000:role/RedshiftCopyRole"
        )
    )
    simulated_locally: bool = field(
        default_factory=lambda: _env_bool("REDSHIFT_SIMULATED", True)
    )


@dataclass(frozen=True)
class GlueConfig:
    """Glue jobs in glue_jobs/ are reference scripts; local ETL uses Spark batch."""
    database: str = field(
        default_factory=lambda: os.getenv("GLUE_DATABASE", "ecommerce_analytics")
    )
    simulated_locally: bool = field(
        default_factory=lambda: _env_bool("GLUE_SIMULATED", True)
    )


@dataclass(frozen=True)
class ProducerConfig:
    events_per_second: int = field(
        default_factory=lambda: _env_int("PRODUCER_EVENTS_PER_SECOND", 15)
    )
    batch_size: int = field(default_factory=lambda: _env_int("PRODUCER_BATCH_SIZE", 100))


@dataclass(frozen=True)
class DashboardConfig:
    refresh_seconds: int = field(
        default_factory=lambda: _env_int("DASHBOARD_REFRESH_SECONDS", 5)
    )
    metrics_source: str = field(
        default_factory=lambda: os.getenv("METRICS_SOURCE", "postgres")
    )


@dataclass
class Settings:
    kafka: KafkaConfig = field(default_factory=KafkaConfig)
    postgres: PostgresConfig = field(default_factory=PostgresConfig)
    spark: SparkConfig = field(default_factory=SparkConfig)
    data_lake: DataLakeConfig = field(default_factory=DataLakeConfig)
    redshift: RedshiftConfig = field(default_factory=RedshiftConfig)
    glue: GlueConfig = field(default_factory=GlueConfig)
    producer: ProducerConfig = field(default_factory=ProducerConfig)
    dashboard: DashboardConfig = field(default_factory=DashboardConfig)
    log_level: str = field(default_factory=lambda: os.getenv("LOG_LEVEL", "INFO"))

    # Backward-compatible alias
    @property
    def s3(self) -> DataLakeConfig:
        return self.data_lake


settings = Settings()

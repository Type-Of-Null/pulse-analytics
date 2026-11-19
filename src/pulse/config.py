from functools import lru_cache

from pydantic import BaseModel, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class KafkaSettings(BaseModel):
    bootstrap_servers: str = "localhost:9092"
    events_topic: str = "events.raw"
    dlq_topic: str = "events.dlq"
    keys_topic: str = "projects.api-keys"
    events_partitions: int = 12
    replication_factor: int = 1
    writer_group: str = "pulse-writer"


class ClickHouseSettings(BaseModel):
    host: str = "localhost"
    port: int = 8123
    user: str = "default"
    password: SecretStr = SecretStr("")
    database: str = "pulse"


class PostgresSettings(BaseModel):
    dsn: SecretStr = SecretStr("postgresql+asyncpg://pulse:pulse@localhost:5432/pulse")
    pool_size: int = 5


class IngestSettings(BaseModel):
    max_batch_size: int = 500
    max_future_skew_seconds: int = 300
    max_event_age_days: int = 30


class WriterSettings(BaseModel):
    batch_size: int = 5000
    flush_interval_seconds: float = 1.0
    max_insert_attempts: int = 5
    retry_base_delay_seconds: float = 0.5


class OutboxSettings(BaseModel):
    poll_interval_seconds: float = 0.5
    batch_size: int = 100


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="PULSE_", env_nested_delimiter="__")

    service_name: str = "pulse"
    log_level: str = "INFO"
    log_json: bool = True
    http_host: str = "0.0.0.0"
    http_port: int = 8000
    admin_token: SecretStr = SecretStr("change-me")

    kafka: KafkaSettings = KafkaSettings()
    clickhouse: ClickHouseSettings = ClickHouseSettings()
    postgres: PostgresSettings = PostgresSettings()
    ingest: IngestSettings = IngestSettings()
    writer: WriterSettings = WriterSettings()
    outbox: OutboxSettings = OutboxSettings()


@lru_cache
def get_settings() -> Settings:
    return Settings()

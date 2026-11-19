"""Event contracts: what clients send and what travels through Kafka."""

from datetime import UTC, datetime
from typing import Annotated
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field, StringConstraints, field_validator

SCHEMA_VERSION = 1
MAX_PROPERTIES = 50
MAX_PROPERTY_KEY_LENGTH = 64
MAX_PROPERTY_VALUE_LENGTH = 1024

EventName = Annotated[str, StringConstraints(pattern=r"^[A-Za-z0-9_.:$-]{1,64}$")]
PropertyValue = str | int | float | bool | None


def _as_utc(value: datetime) -> datetime:
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)


class IncomingEvent(BaseModel):
    """Event as sent by a client SDK."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    event_id: UUID = Field(default_factory=uuid4, description="Client-generated idempotency key")
    name: EventName
    user_id: str = Field(min_length=1, max_length=128)
    session_id: str = Field(default="", max_length=128)
    timestamp: datetime | None = None
    properties: dict[str, PropertyValue] = Field(default_factory=dict)

    @field_validator("timestamp")
    @classmethod
    def _normalize_timestamp(cls, value: datetime | None) -> datetime | None:
        return _as_utc(value) if value is not None else None

    @field_validator("properties")
    @classmethod
    def _limit_properties(cls, value: dict[str, PropertyValue]) -> dict[str, PropertyValue]:
        if len(value) > MAX_PROPERTIES:
            raise ValueError(f"at most {MAX_PROPERTIES} properties are allowed")
        for key, item in value.items():
            if not key or len(key) > MAX_PROPERTY_KEY_LENGTH:
                raise ValueError(f"property key must be 1..{MAX_PROPERTY_KEY_LENGTH} characters")
            if isinstance(item, str) and len(item) > MAX_PROPERTY_VALUE_LENGTH:
                raise ValueError(f"property '{key}' exceeds {MAX_PROPERTY_VALUE_LENGTH} characters")
        return value


def stringify_property(value: PropertyValue) -> str:
    if value is None:
        return ""
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


class EnrichedEvent(BaseModel):
    """Event after authentication and normalization; the Kafka message contract (v1)."""

    model_config = ConfigDict(extra="ignore", frozen=True)

    v: int = SCHEMA_VERSION
    project_id: UUID
    event_id: UUID
    name: EventName
    user_id: str
    session_id: str = ""
    timestamp: datetime
    received_at: datetime
    properties: dict[str, str] = Field(default_factory=dict)

    @field_validator("timestamp", "received_at")
    @classmethod
    def _utc(cls, value: datetime) -> datetime:
        return _as_utc(value)

    @property
    def partition_key(self) -> bytes:
        """All events of one user land in one partition, preserving per-user order."""
        return f"{self.project_id}:{self.user_id}".encode()

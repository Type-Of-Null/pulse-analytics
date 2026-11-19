"""Ports (hexagonal architecture): the application layer depends only on these protocols."""

from collections.abc import Mapping, Sequence
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass
from datetime import date
from typing import Protocol
from uuid import UUID

from pulse.domain.analytics import (
    EventCount,
    Granularity,
    TimeRange,
    TimeseriesPoint,
)
from pulse.domain.events import EnrichedEvent
from pulse.domain.projects import ApiKey, Project


@dataclass(frozen=True, slots=True)
class RawMessage:
    topic: str
    partition: int
    offset: int
    key: bytes | None
    value: bytes | None


TopicPartitionKey = tuple[str, int]


class EventPublisher(Protocol):
    async def publish(self, events: Sequence[EnrichedEvent]) -> None:
        """Return only after the broker acknowledged every event."""


class ApiKeyResolver(Protocol):
    @property
    def ready(self) -> bool: ...

    def resolve(self, key_hash: str) -> UUID | None: ...


class MessageSource(Protocol):
    async def poll(self, max_records: int, timeout_ms: int) -> Sequence[RawMessage]: ...

    async def commit(self, offsets: Mapping[TopicPartitionKey, int]) -> None:
        """Commit the *next* offset to consume for each partition."""


class EventSink(Protocol):
    async def write(self, events: Sequence[EnrichedEvent], dedup_token: str) -> None: ...


class DeadLetterPublisher(Protocol):
    async def send(self, message: RawMessage, reason: str, error: str) -> None: ...


class AnalyticsRepository(Protocol):
    async def timeseries(
        self, project_id: UUID, event: str | None, period: TimeRange, granularity: Granularity
    ) -> list[TimeseriesPoint]: ...

    async def top_events(
        self, project_id: UUID, period: TimeRange, limit: int
    ) -> list[EventCount]: ...

    async def funnel_levels(
        self, project_id: UUID, steps: Sequence[str], window_seconds: int, period: TimeRange
    ) -> dict[int, int]: ...

    async def retention_counts(
        self, project_id: UUID, start_event: str, return_event: str, cohort_date: date, days: int
    ) -> list[int]: ...


@dataclass(frozen=True, slots=True)
class OutboxMessage:
    topic: str
    key: str
    payload: Mapping[str, object] | None


class UnitOfWork(Protocol):
    """One database transaction: domain state and outbox messages commit atomically."""

    async def add_project(self, project: Project) -> None: ...

    async def get_project(self, project_id: UUID) -> Project | None: ...

    async def list_projects(self) -> list[Project]: ...

    async def add_api_key(self, key: ApiKey) -> None: ...

    async def get_api_key(self, project_id: UUID, key_id: UUID) -> ApiKey | None: ...

    async def list_api_keys(self, project_id: UUID) -> list[ApiKey]: ...

    async def revoke_api_key(self, key: ApiKey) -> ApiKey: ...

    async def add_outbox(self, message: OutboxMessage) -> None: ...

    async def commit(self) -> None: ...


class UnitOfWorkFactory(Protocol):
    def __call__(self) -> AbstractAsyncContextManager[UnitOfWork]: ...

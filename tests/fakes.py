import asyncio
from collections.abc import AsyncIterator, Mapping, Sequence
from contextlib import asynccontextmanager
from datetime import date
from uuid import UUID

from pulse.domain.analytics import EventCount, Granularity, TimeRange, TimeseriesPoint
from pulse.domain.events import EnrichedEvent
from pulse.domain.projects import ApiKey, Project, hash_api_key
from pulse.ports import OutboxMessage, RawMessage, TopicPartitionKey


class FakePublisher:
    def __init__(self, error: Exception | None = None) -> None:
        self.published: list[EnrichedEvent] = []
        self.error = error

    async def publish(self, events: Sequence[EnrichedEvent]) -> None:
        if self.error:
            raise self.error
        self.published.extend(events)


class StaticResolver:
    def __init__(self, keys: Mapping[str, UUID], ready: bool = True) -> None:
        self._keys = {hash_api_key(k): v for k, v in keys.items()}
        self.ready = ready

    def resolve(self, key_hash: str) -> UUID | None:
        return self._keys.get(key_hash)


class FakeSource:
    def __init__(self, batches: list[list[RawMessage]], stop: asyncio.Event | None = None) -> None:
        self._batches = batches
        self._stop = stop
        self.commits: list[dict[TopicPartitionKey, int]] = []

    async def poll(self, max_records: int, timeout_ms: int) -> Sequence[RawMessage]:
        if self._batches:
            batch = self._batches.pop(0)
            if len(batch) > max_records:
                self._batches.insert(0, batch[max_records:])
            return batch[:max_records]
        if self._stop:
            self._stop.set()
        await asyncio.sleep(0)
        return []

    async def commit(self, offsets: Mapping[TopicPartitionKey, int]) -> None:
        self.commits.append(dict(offsets))


class FakeSink:
    def __init__(self, failures: int = 0) -> None:
        self.failures = failures
        self.writes: list[tuple[list[EnrichedEvent], str]] = []
        self.attempts = 0

    async def write(self, events: Sequence[EnrichedEvent], dedup_token: str) -> None:
        self.attempts += 1
        if self.failures:
            self.failures -= 1
            raise ConnectionError("clickhouse down")
        self.writes.append((list(events), dedup_token))


class FakeDlq:
    def __init__(self) -> None:
        self.messages: list[tuple[RawMessage, str]] = []

    async def send(self, message: RawMessage, reason: str, error: str) -> None:
        self.messages.append((message, reason))


class FakeAnalyticsRepository:
    def __init__(self) -> None:
        self.levels: dict[int, int] = {}
        self.retention: list[int] = []
        self.calls: list[tuple[str, object]] = []

    async def timeseries(
        self, project_id: UUID, event: str | None, period: TimeRange, granularity: Granularity
    ) -> list[TimeseriesPoint]:
        self.calls.append(("timeseries", (project_id, event, granularity)))
        return [TimeseriesPoint(bucket=period.start, events=10, users=3)]

    async def top_events(self, project_id: UUID, period: TimeRange, limit: int) -> list[EventCount]:
        self.calls.append(("top_events", limit))
        return [EventCount(name="page_view", events=42, users=7)]

    async def funnel_levels(
        self, project_id: UUID, steps: Sequence[str], window_seconds: int, period: TimeRange
    ) -> dict[int, int]:
        self.calls.append(("funnel", (list(steps), window_seconds)))
        return self.levels

    async def retention_counts(
        self, project_id: UUID, start_event: str, return_event: str, cohort_date: date, days: int
    ) -> list[int]:
        self.calls.append(("retention", days))
        return self.retention


class InMemoryUnitOfWork:
    def __init__(self, store: "InMemoryStore") -> None:
        self._store = store
        self._projects = dict(store.projects)
        self._keys = dict(store.keys)
        self._outbox: list[OutboxMessage] = []

    async def add_project(self, project: Project) -> None:
        self._projects[project.id] = project

    async def get_project(self, project_id: UUID) -> Project | None:
        return self._projects.get(project_id)

    async def list_projects(self) -> list[Project]:
        return list(self._projects.values())

    async def add_api_key(self, key: ApiKey) -> None:
        self._keys[key.id] = key

    async def get_api_key(self, project_id: UUID, key_id: UUID) -> ApiKey | None:
        key = self._keys.get(key_id)
        return key if key and key.project_id == project_id else None

    async def list_api_keys(self, project_id: UUID) -> list[ApiKey]:
        return [k for k in self._keys.values() if k.project_id == project_id]

    async def revoke_api_key(self, key: ApiKey) -> ApiKey:
        from dataclasses import replace
        from datetime import UTC, datetime

        revoked = replace(key, revoked_at=datetime.now(UTC))
        self._keys[key.id] = revoked
        return revoked

    async def add_outbox(self, message: OutboxMessage) -> None:
        self._outbox.append(message)

    async def commit(self) -> None:
        self._store.projects = self._projects
        self._store.keys = self._keys
        self._store.outbox.extend(self._outbox)


class InMemoryStore:
    def __init__(self) -> None:
        self.projects: dict[UUID, Project] = {}
        self.keys: dict[UUID, ApiKey] = {}
        self.outbox: list[OutboxMessage] = []

    @asynccontextmanager
    async def __call__(self) -> AsyncIterator[InMemoryUnitOfWork]:
        yield InMemoryUnitOfWork(self)

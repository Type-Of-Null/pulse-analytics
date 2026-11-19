from datetime import UTC, date, datetime, timedelta
from typing import Any
from uuid import UUID, uuid4

import pytest

from pulse.adapters.clickhouse.migrate import migrate_clickhouse
from pulse.adapters.clickhouse.repository import ClickHouseAnalyticsRepository
from pulse.adapters.clickhouse.sink import ClickHouseEventSink
from pulse.config import ClickHouseSettings
from pulse.domain.analytics import Granularity, TimeRange
from pulse.domain.events import EnrichedEvent

pytestmark = pytest.mark.integration

T0 = datetime(2026, 3, 2, 10, 0, tzinfo=UTC)


def event(project: UUID, name: str, user: str, at: datetime) -> EnrichedEvent:
    return EnrichedEvent(
        project_id=project,
        event_id=uuid4(),
        name=name,
        user_id=user,
        timestamp=at,
        received_at=at,
        properties={"source": "test"},
    )


async def test_migrations_are_idempotent(
    clickhouse: Any, clickhouse_settings: ClickHouseSettings
) -> None:
    assert await migrate_clickhouse(clickhouse_settings) == []


async def test_duplicate_batch_with_same_token_is_inserted_once(clickhouse: Any) -> None:
    sink = ClickHouseEventSink(clickhouse)
    project = uuid4()
    batch = [event(project, "e", "u1", T0), event(project, "e", "u2", T0)]

    await sink.write(batch, dedup_token="token-1")
    await sink.write(batch, dedup_token="token-1")

    count = await clickhouse.query(
        "SELECT count() FROM events WHERE project_id = {p:UUID}", parameters={"p": project}
    )
    assert count.result_rows[0][0] == 2


async def test_redelivered_events_collapse_by_event_id(clickhouse: Any) -> None:
    sink = ClickHouseEventSink(clickhouse)
    project = uuid4()
    batch = [event(project, "e", "u1", T0)]

    await sink.write(batch, dedup_token="a")
    await sink.write(batch, dedup_token="b")
    await clickhouse.command("OPTIMIZE TABLE events FINAL")

    count = await clickhouse.query(
        "SELECT count() FROM events WHERE project_id = {p:UUID}", parameters={"p": project}
    )
    assert count.result_rows[0][0] == 1


async def test_analytics_queries(clickhouse: Any) -> None:
    project, other = uuid4(), uuid4()
    rows = [
        # u1 completes the funnel, u2 drops after signup, u3 only visits.
        event(project, "visit", "u1", T0),
        event(project, "signup", "u1", T0 + timedelta(minutes=5)),
        event(project, "pay", "u1", T0 + timedelta(minutes=10)),
        event(project, "visit", "u2", T0),
        event(project, "signup", "u2", T0 + timedelta(minutes=1)),
        event(project, "visit", "u3", T0 + timedelta(hours=1)),
        # Retention: u1 and u2 return the next day, u1 also on day 2.
        event(project, "visit", "u1", T0 + timedelta(days=1)),
        event(project, "visit", "u2", T0 + timedelta(days=1)),
        event(project, "visit", "u1", T0 + timedelta(days=2)),
        event(other, "visit", "x", T0),
    ]
    await ClickHouseEventSink(clickhouse).write(rows, dedup_token="seed")
    repo = ClickHouseAnalyticsRepository(clickhouse)
    day = TimeRange(T0 - timedelta(hours=1), T0 + timedelta(hours=23))

    top = await repo.top_events(project, day, limit=10)
    assert [(e.name, e.events, e.users) for e in top] == [
        ("visit", 3, 3),
        ("signup", 2, 2),
        ("pay", 1, 1),
    ]

    series = await repo.timeseries(project, "visit", day, Granularity.HOUR)
    assert len(series) == 24
    assert [(p.events, p.users) for p in series if p.events] == [(2, 2), (1, 1)]

    levels = await repo.funnel_levels(project, ["visit", "signup", "pay"], 3600, day)
    assert levels == {1: 1, 2: 1, 3: 1}

    retention = await repo.retention_counts(project, "visit", "visit", date(2026, 3, 2), 3)
    assert retention == [3, 2, 1, 0]

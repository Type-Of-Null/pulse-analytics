from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest

from pulse.application.ingest import IngestEvents, IngestPolicy
from pulse.domain.errors import EventRejectedError
from pulse.domain.events import IncomingEvent
from tests.fakes import FakePublisher

NOW = datetime(2026, 6, 1, 12, tzinfo=UTC)


def make_use_case(publisher: FakePublisher, **policy: object) -> IngestEvents:
    return IngestEvents(publisher, IngestPolicy(**policy), clock=lambda: NOW)  # type: ignore[arg-type]


async def test_events_are_enriched_and_published() -> None:
    publisher = FakePublisher()
    project = uuid4()
    event = IncomingEvent(name="signup", user_id="u1", properties={"paid": True, "n": 3})

    result = await make_use_case(publisher)(project, [event])

    assert result.accepted == 1
    [published] = publisher.published
    assert published.project_id == project
    assert published.event_id == event.event_id
    assert published.timestamp == published.received_at == NOW
    assert published.properties == {"paid": "true", "n": "3"}


async def test_future_timestamps_are_clamped_to_server_time() -> None:
    publisher = FakePublisher()
    future = IncomingEvent(name="e", user_id="u", timestamp=NOW + timedelta(hours=1))
    slightly_ahead = IncomingEvent(name="e", user_id="u", timestamp=NOW + timedelta(minutes=1))

    result = await make_use_case(publisher)(uuid4(), [future, slightly_ahead])

    assert result.clamped == 1
    assert publisher.published[0].timestamp == NOW
    assert publisher.published[1].timestamp == NOW + timedelta(minutes=1)


async def test_events_older_than_retention_reject_the_batch() -> None:
    publisher = FakePublisher()
    old = IncomingEvent(name="e", user_id="u", timestamp=NOW - timedelta(days=31))

    with pytest.raises(EventRejectedError, match="older"):
        await make_use_case(publisher)(uuid4(), [old])
    assert publisher.published == []


async def test_batch_size_limits() -> None:
    use_case = make_use_case(FakePublisher(), max_batch_size=2)
    event = IncomingEvent(name="e", user_id="u")

    with pytest.raises(EventRejectedError, match="empty"):
        await use_case(uuid4(), [])
    with pytest.raises(EventRejectedError, match="exceeds"):
        await use_case(uuid4(), [event] * 3)

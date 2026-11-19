from datetime import UTC, datetime, timedelta, timezone
from uuid import uuid4

import pytest
from pydantic import ValidationError

from pulse.domain.events import EnrichedEvent, IncomingEvent, stringify_property


def test_naive_and_offset_timestamps_are_normalized_to_utc() -> None:
    naive = IncomingEvent(name="click", user_id="u1", timestamp=datetime(2026, 1, 1, 12))
    msk = timezone(timedelta(hours=3))
    aware = IncomingEvent(
        name="click", user_id="u1", timestamp=datetime(2026, 1, 1, 15, tzinfo=msk)
    )

    assert naive.timestamp == aware.timestamp == datetime(2026, 1, 1, 12, tzinfo=UTC)


@pytest.mark.parametrize("name", ["", "has space", "x" * 65, "drop;table"])
def test_invalid_event_names_are_rejected(name: str) -> None:
    with pytest.raises(ValidationError):
        IncomingEvent(name=name, user_id="u1")


def test_too_many_properties_are_rejected() -> None:
    with pytest.raises(ValidationError, match="at most 50"):
        IncomingEvent(name="e", user_id="u", properties={f"k{i}": i for i in range(51)})


def test_long_property_value_is_rejected() -> None:
    with pytest.raises(ValidationError, match="exceeds"):
        IncomingEvent(name="e", user_id="u", properties={"k": "x" * 1025})


def test_unknown_fields_are_forbidden() -> None:
    with pytest.raises(ValidationError):
        IncomingEvent.model_validate({"name": "e", "user_id": "u", "extra": 1})


@pytest.mark.parametrize(
    ("value", "expected"), [(True, "true"), (False, "false"), (None, ""), (1.5, "1.5"), ("a", "a")]
)
def test_stringify_property(value: object, expected: str) -> None:
    assert stringify_property(value) == expected  # type: ignore[arg-type]


def test_partition_key_groups_events_by_project_and_user() -> None:
    project = uuid4()
    now = datetime.now(UTC)
    event = EnrichedEvent(
        project_id=project, event_id=uuid4(), name="e", user_id="u1", timestamp=now, received_at=now
    )
    assert event.partition_key == f"{project}:u1".encode()


def test_enriched_event_roundtrips_through_json() -> None:
    now = datetime.now(UTC)
    event = EnrichedEvent(
        project_id=uuid4(),
        event_id=uuid4(),
        name="e",
        user_id="u",
        timestamp=now,
        received_at=now,
        properties={"plan": "pro"},
    )
    assert EnrichedEvent.model_validate_json(event.model_dump_json()) == event

from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from uuid import UUID

from pulse import metrics
from pulse.domain.errors import EventRejectedError
from pulse.domain.events import EnrichedEvent, IncomingEvent, stringify_property
from pulse.ports import EventPublisher


@dataclass(frozen=True, slots=True)
class IngestPolicy:
    max_batch_size: int = 500
    max_future_skew: timedelta = timedelta(minutes=5)
    max_event_age: timedelta = timedelta(days=30)


@dataclass(frozen=True, slots=True)
class IngestResult:
    accepted: int
    clamped: int


def _utcnow() -> datetime:
    return datetime.now(UTC)


class IngestEvents:
    """Normalizes a client batch and publishes it to the event log.

    Clock policy: a missing timestamp means "now"; timestamps from the future (client clock skew)
    are clamped to the server time; events older than `max_event_age` are rejected so that late
    data cannot rewrite closed ClickHouse partitions.
    """

    def __init__(
        self,
        publisher: EventPublisher,
        policy: IngestPolicy | None = None,
        clock: Callable[[], datetime] = _utcnow,
    ) -> None:
        self._publisher = publisher
        self._policy = policy or IngestPolicy()
        self._clock = clock

    async def __call__(self, project_id: UUID, batch: Sequence[IncomingEvent]) -> IngestResult:
        if not batch:
            raise EventRejectedError("batch is empty")
        if len(batch) > self._policy.max_batch_size:
            raise EventRejectedError(f"batch exceeds {self._policy.max_batch_size} events")

        now = self._clock()
        enriched: list[EnrichedEvent] = []
        clamped = 0
        for event in batch:
            timestamp = event.timestamp or now
            if timestamp > now + self._policy.max_future_skew:
                timestamp, clamped = now, clamped + 1
            elif timestamp < now - self._policy.max_event_age:
                raise EventRejectedError(
                    f"event {event.event_id} is older than the retention window"
                )
            enriched.append(
                EnrichedEvent(
                    project_id=project_id,
                    event_id=event.event_id,
                    name=event.name,
                    user_id=event.user_id,
                    session_id=event.session_id,
                    timestamp=timestamp,
                    received_at=now,
                    properties={k: stringify_property(v) for k, v in event.properties.items()},
                )
            )

        with metrics.PUBLISH_LATENCY.time():
            await self._publisher.publish(enriched)
        if clamped:
            metrics.CLOCK_SKEW_CLAMPED.inc(clamped)
        return IngestResult(accepted=len(enriched), clamped=clamped)

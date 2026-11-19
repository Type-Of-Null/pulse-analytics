"""Value objects and pure calculations for analytics queries."""

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from enum import StrEnum

from pulse.domain.errors import InvalidQueryError


class Granularity(StrEnum):
    MINUTE = "minute"
    HOUR = "hour"
    DAY = "day"

    @property
    def max_span(self) -> timedelta:
        return {
            Granularity.MINUTE: timedelta(days=1),
            Granularity.HOUR: timedelta(days=31),
            Granularity.DAY: timedelta(days=366),
        }[self]


@dataclass(frozen=True, slots=True)
class TimeRange:
    start: datetime
    end: datetime

    def __post_init__(self) -> None:
        if self.end <= self.start:
            raise InvalidQueryError("'to' must be after 'from'")

    def ensure_fits(self, granularity: Granularity) -> None:
        if self.end - self.start > granularity.max_span:
            raise InvalidQueryError(
                f"range is too wide for '{granularity}' granularity "
                f"(max {granularity.max_span.days} days)"
            )


@dataclass(frozen=True, slots=True)
class TimeseriesPoint:
    bucket: datetime
    events: int
    users: int


@dataclass(frozen=True, slots=True)
class EventCount:
    name: str
    events: int
    users: int


@dataclass(frozen=True, slots=True)
class FunnelStep:
    name: str
    users: int
    conversion_from_start: float
    conversion_from_previous: float


@dataclass(frozen=True, slots=True)
class RetentionPoint:
    day: int
    date: date
    users: int
    rate: float


MAX_FUNNEL_STEPS = 10
MAX_RETENTION_DAYS = 30


def validate_funnel_steps(steps: Sequence[str]) -> None:
    if not 2 <= len(steps) <= MAX_FUNNEL_STEPS:
        raise InvalidQueryError(f"funnel needs 2..{MAX_FUNNEL_STEPS} steps")


def _ratio(numerator: int, denominator: int) -> float:
    return round(numerator / denominator, 4) if denominator else 0.0


def funnel_from_levels(steps: Sequence[str], level_counts: Mapping[int, int]) -> list[FunnelStep]:
    """Convert ClickHouse `windowFunnel` output (users per max reached level) into a funnel.

    A user with level N reached steps 1..N, so users at step i = users with level >= i.
    """
    reached = [
        sum(count for level, count in level_counts.items() if level >= index)
        for index in range(1, len(steps) + 1)
    ]
    start = reached[0] if reached else 0
    result = []
    for index, (name, users) in enumerate(zip(steps, reached, strict=True)):
        previous = reached[index - 1] if index else users
        result.append(
            FunnelStep(
                name=name,
                users=users,
                conversion_from_start=_ratio(users, start),
                conversion_from_previous=_ratio(users, previous),
            )
        )
    return result


def retention_from_counts(cohort_date: date, counts: Sequence[int]) -> list[RetentionPoint]:
    """`counts[0]` is the cohort size, `counts[i]` - users of the cohort who returned on day i."""
    cohort = counts[0] if counts else 0
    return [
        RetentionPoint(
            day=day,
            date=cohort_date + timedelta(days=day),
            users=users,
            rate=_ratio(users, cohort),
        )
        for day, users in enumerate(counts)
    ]

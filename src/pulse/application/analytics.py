from collections.abc import Sequence
from datetime import date
from uuid import UUID

from pulse.domain.analytics import (
    MAX_RETENTION_DAYS,
    EventCount,
    FunnelStep,
    Granularity,
    RetentionPoint,
    TimeRange,
    TimeseriesPoint,
    funnel_from_levels,
    retention_from_counts,
    validate_funnel_steps,
)
from pulse.domain.errors import InvalidQueryError
from pulse.ports import AnalyticsRepository

MAX_FUNNEL_WINDOW_SECONDS = 30 * 24 * 3600


class AnalyticsService:
    def __init__(self, repository: AnalyticsRepository) -> None:
        self._repo = repository

    async def timeseries(
        self, project_id: UUID, event: str | None, period: TimeRange, granularity: Granularity
    ) -> list[TimeseriesPoint]:
        period.ensure_fits(granularity)
        return await self._repo.timeseries(project_id, event, period, granularity)

    async def top_events(self, project_id: UUID, period: TimeRange, limit: int) -> list[EventCount]:
        return await self._repo.top_events(project_id, period, limit)

    async def funnel(
        self, project_id: UUID, steps: Sequence[str], window_seconds: int, period: TimeRange
    ) -> list[FunnelStep]:
        validate_funnel_steps(steps)
        if not 0 < window_seconds <= MAX_FUNNEL_WINDOW_SECONDS:
            raise InvalidQueryError("window must be between 1 second and 30 days")
        levels = await self._repo.funnel_levels(project_id, steps, window_seconds, period)
        return funnel_from_levels(steps, levels)

    async def retention(
        self, project_id: UUID, start_event: str, return_event: str, cohort_date: date, days: int
    ) -> list[RetentionPoint]:
        if not 1 <= days <= MAX_RETENTION_DAYS:
            raise InvalidQueryError(f"days must be between 1 and {MAX_RETENTION_DAYS}")
        counts = await self._repo.retention_counts(
            project_id, start_event, return_event, cohort_date, days
        )
        return retention_from_counts(cohort_date, counts)

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, date, datetime, timedelta
from typing import Annotated
from uuid import UUID

from fastapi import Depends, FastAPI, Query
from pydantic import BaseModel, Field

from pulse.adapters.clickhouse.client import create_client
from pulse.adapters.clickhouse.repository import ClickHouseAnalyticsRepository
from pulse.adapters.kafka.key_registry import KafkaKeyRegistryFeed
from pulse.application.analytics import AnalyticsService
from pulse.application.key_registry import ApiKeyRegistry
from pulse.config import Settings, get_settings
from pulse.domain.analytics import (
    EventCount,
    FunnelStep,
    Granularity,
    RetentionPoint,
    TimeRange,
    TimeseriesPoint,
)
from pulse.entrypoints.common import api_key_auth, install_common
from pulse.ports import ApiKeyResolver


def _now() -> datetime:
    return datetime.now(UTC)


def _utc(value: datetime) -> datetime:
    """Bounds without an offset are interpreted as UTC."""
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value


def _period(start: datetime | None, end: datetime | None) -> TimeRange:
    end = _utc(end) if end else _now()
    return TimeRange(start=_utc(start) if start else end - timedelta(days=7), end=end)


class FunnelRequest(BaseModel):
    steps: list[str] = Field(min_length=2, max_length=10, examples=[["signup", "activate", "pay"]])
    window_seconds: int = Field(default=86400, gt=0)
    start: datetime | None = Field(default=None, alias="from")
    end: datetime | None = Field(default=None, alias="to")


def build_app(
    service: AnalyticsService, resolver: ApiKeyResolver, lifespan: object | None = None
) -> FastAPI:
    app = FastAPI(title="Pulse Analytics API", version="1.0.0", lifespan=lifespan)  # type: ignore[arg-type]
    install_common(app, lambda: resolver.ready)
    Project = Annotated[UUID, Depends(api_key_auth(resolver))]  # noqa: N806

    @app.get("/v1/analytics/timeseries", response_model=list[TimeseriesPoint])
    async def timeseries(
        project_id: Project,
        event: str | None = None,
        granularity: Granularity = Granularity.HOUR,
        start: Annotated[datetime | None, Query(alias="from")] = None,
        end: Annotated[datetime | None, Query(alias="to")] = None,
    ) -> list[TimeseriesPoint]:
        return await service.timeseries(project_id, event, _period(start, end), granularity)

    @app.get("/v1/analytics/top-events", response_model=list[EventCount])
    async def top_events(
        project_id: Project,
        limit: Annotated[int, Query(ge=1, le=100)] = 10,
        start: Annotated[datetime | None, Query(alias="from")] = None,
        end: Annotated[datetime | None, Query(alias="to")] = None,
    ) -> list[EventCount]:
        return await service.top_events(project_id, _period(start, end), limit)

    @app.post("/v1/analytics/funnel", response_model=list[FunnelStep])
    async def funnel(project_id: Project, body: FunnelRequest) -> list[FunnelStep]:
        return await service.funnel(
            project_id, body.steps, body.window_seconds, _period(body.start, body.end)
        )

    @app.get("/v1/analytics/retention", response_model=list[RetentionPoint])
    async def retention(
        project_id: Project,
        start_event: str,
        return_event: str,
        cohort_date: date,
        days: Annotated[int, Query(ge=1, le=30)] = 7,
    ) -> list[RetentionPoint]:
        return await service.retention(project_id, start_event, return_event, cohort_date, days)

    return app


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or get_settings()
    registry = ApiKeyRegistry()
    feed = KafkaKeyRegistryFeed(
        settings.kafka.bootstrap_servers, settings.kafka.keys_topic, registry
    )
    repository = ClickHouseAnalyticsRepository()
    service = AnalyticsService(repository)

    @asynccontextmanager
    async def lifespan(_: FastAPI) -> AsyncIterator[None]:
        client = await create_client(settings.clickhouse)
        repository.bind(client)
        await feed.start()
        try:
            yield
        finally:
            await feed.stop()
            await client.close()

    return build_app(service, registry, lifespan)

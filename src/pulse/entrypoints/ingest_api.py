from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import timedelta
from typing import Annotated
from uuid import UUID

from aiokafka.errors import KafkaError
from fastapi import Depends, FastAPI, HTTPException, status
from pydantic import BaseModel, Field

from pulse import metrics
from pulse.adapters.kafka.key_registry import KafkaKeyRegistryFeed
from pulse.adapters.kafka.producer import KafkaEventPublisher, create_producer
from pulse.application.ingest import IngestEvents, IngestPolicy
from pulse.application.key_registry import ApiKeyRegistry
from pulse.config import Settings, get_settings
from pulse.domain.events import IncomingEvent
from pulse.entrypoints.common import api_key_auth, install_common
from pulse.ports import ApiKeyResolver


class EventBatch(BaseModel):
    events: list[IncomingEvent] = Field(min_length=1)


class IngestResponse(BaseModel):
    accepted: int
    clamped: int


def build_app(
    ingest: IngestEvents, resolver: ApiKeyResolver, lifespan: object | None = None
) -> FastAPI:
    app = FastAPI(title="Pulse Ingest API", version="1.0.0", lifespan=lifespan)  # type: ignore[arg-type]
    install_common(app, lambda: resolver.ready)
    auth = api_key_auth(resolver)

    @app.post("/v1/events", status_code=status.HTTP_202_ACCEPTED, response_model=IngestResponse)
    async def ingest_events(
        batch: EventBatch, project_id: Annotated[UUID, Depends(auth)]
    ) -> IngestResponse:
        try:
            result = await ingest(project_id, batch.events)
        except KafkaError as exc:
            metrics.INGESTED_EVENTS.labels(status="failed").inc(len(batch.events))
            raise HTTPException(
                status.HTTP_503_SERVICE_UNAVAILABLE, "event log unavailable"
            ) from exc
        metrics.INGESTED_EVENTS.labels(status="accepted").inc(result.accepted)
        return IngestResponse(accepted=result.accepted, clamped=result.clamped)

    return app


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or get_settings()
    producer = create_producer(settings.kafka.bootstrap_servers, client_id="ingest-api")
    registry = ApiKeyRegistry()
    feed = KafkaKeyRegistryFeed(
        settings.kafka.bootstrap_servers, settings.kafka.keys_topic, registry
    )
    policy = IngestPolicy(
        max_batch_size=settings.ingest.max_batch_size,
        max_future_skew=timedelta(seconds=settings.ingest.max_future_skew_seconds),
        max_event_age=timedelta(days=settings.ingest.max_event_age_days),
    )
    ingest = IngestEvents(KafkaEventPublisher(producer, settings.kafka.events_topic), policy)

    @asynccontextmanager
    async def lifespan(_: FastAPI) -> AsyncIterator[None]:
        await producer.start()
        await feed.start()
        try:
            yield
        finally:
            await feed.stop()
            await producer.stop()

    return build_app(ingest, registry, lifespan)

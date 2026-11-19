import asyncio
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

import orjson
import pytest
from sqlalchemy.ext.asyncio import create_async_engine

from pulse.adapters.postgres.migrate import migrate_postgres
from pulse.adapters.postgres.outbox_relay import OutboxRelay
from pulse.adapters.postgres.uow import PostgresUnitOfWorkFactory
from pulse.application.admin import ProjectService
from pulse.domain.projects import Project
from pulse.ports import OutboxMessage

pytestmark = pytest.mark.integration


class RecordingProducer:
    def __init__(self) -> None:
        self.sent: list[tuple[str, bytes, bytes | None]] = []

    async def send(self, topic: str, key: bytes, value: bytes | None) -> "asyncio.Future[Any]":
        self.sent.append((topic, key, value))
        future: asyncio.Future[Any] = asyncio.get_running_loop().create_future()
        future.set_result(None)
        return future


@pytest.fixture(scope="module")
def migrated(postgres_dsn: str) -> str:
    migrate_postgres(postgres_dsn)
    migrate_postgres(postgres_dsn)  # idempotent
    return postgres_dsn


async def test_outbox_is_relayed_in_order_exactly_once_per_row(migrated: str) -> None:
    engine = create_async_engine(migrated)
    service = ProjectService(PostgresUnitOfWorkFactory(engine), "keys")
    producer = RecordingProducer()
    relay = OutboxRelay(engine, producer, batch_size=100)

    project, issued = await service.create_project("shop")
    await service.revoke_key(project.id, issued.key.id)

    assert await relay.relay_once() == 2
    assert await relay.relay_once() == 0
    (_, key1, value1), (_, key2, value2) = producer.sent[-2:]
    assert key1 == key2 == issued.key.key_hash.encode()
    assert orjson.loads(value1 or b"") == {"project_id": str(project.id)}
    assert value2 is None
    await engine.dispose()


async def test_only_one_relay_replica_publishes_at_a_time(migrated: str) -> None:
    engine = create_async_engine(migrated)
    service = ProjectService(PostgresUnitOfWorkFactory(engine), "keys")
    for i in range(5):
        await service.create_project(f"p{i}")
    producers = [RecordingProducer(), RecordingProducer()]
    relays = [OutboxRelay(engine, p, batch_size=100) for p in producers]

    results = await asyncio.gather(*(r.relay_once() for r in relays))

    assert sorted(results) == [0, 5]
    assert sum(len(p.sent) for p in producers) == 5
    await engine.dispose()


async def _add_project_then_fail(factory: PostgresUnitOfWorkFactory) -> None:
    async with factory() as uow:
        await uow.add_project(Project(uuid4(), "ghost", datetime.now(UTC)))
        await uow.add_outbox(OutboxMessage("keys", "k", {"project_id": "x"}))
        raise RuntimeError("boom")


async def test_rolled_back_transaction_leaves_no_outbox_rows(migrated: str) -> None:
    engine = create_async_engine(migrated)
    with pytest.raises(RuntimeError):
        await _add_project_then_fail(PostgresUnitOfWorkFactory(engine))
    relay = OutboxRelay(engine, RecordingProducer(), batch_size=100)
    assert await relay.relay_once() == 0
    await engine.dispose()

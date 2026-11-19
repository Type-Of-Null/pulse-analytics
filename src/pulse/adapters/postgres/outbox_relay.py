import asyncio
import contextlib

import orjson
import sqlalchemy as sa
import structlog
from aiokafka import AIOKafkaProducer
from sqlalchemy.ext.asyncio import AsyncEngine

from pulse import metrics
from pulse.adapters.postgres.tables import outbox

log = structlog.get_logger(__name__)

# Any constant shared by all relay replicas.
RELAY_LOCK_ID = 0x50554C5345  # "PULSE"


class OutboxRelay:
    """Publishes committed outbox rows to Kafka (Transactional Outbox pattern).

    Every batch runs inside a transaction holding `pg_try_advisory_xact_lock`, so replicas never
    publish concurrently: per-key order (issue -> revoke) is preserved, and any replica can take
    over instantly if another dies. Rows are marked published only after Kafka acked them, so
    delivery is at-least-once; the compacted topic makes replays idempotent.
    """

    def __init__(
        self,
        engine: AsyncEngine,
        producer: AIOKafkaProducer,
        batch_size: int = 100,
        poll_interval: float = 0.5,
    ) -> None:
        self._engine = engine
        self._producer = producer
        self._batch_size = batch_size
        self._poll_interval = poll_interval

    async def run(self, stop: asyncio.Event) -> None:
        while not stop.is_set():
            try:
                published = await self.relay_once()
            except Exception:
                log.exception("outbox_relay_failed")
                published = 0
            if published < self._batch_size:
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(stop.wait(), timeout=self._poll_interval)

    async def relay_once(self) -> int:
        async with self._engine.begin() as conn:
            locked = await conn.scalar(sa.select(sa.func.pg_try_advisory_xact_lock(RELAY_LOCK_ID)))
            metrics.OUTBOX_LEADER.set(1 if locked else 0)
            if not locked:
                return 0
            rows = (
                await conn.execute(
                    sa.select(outbox.c.id, outbox.c.topic, outbox.c.key, outbox.c.payload)
                    .where(outbox.c.published_at.is_(None))
                    .order_by(outbox.c.id)
                    .limit(self._batch_size)
                )
            ).all()
            if not rows:
                return 0
            futures = [
                await self._producer.send(
                    row.topic,
                    key=row.key.encode(),
                    value=orjson.dumps(row.payload) if row.payload is not None else None,
                )
                for row in rows
            ]
            await asyncio.gather(*futures)
            await conn.execute(
                outbox.update()
                .where(outbox.c.id.in_([row.id for row in rows]))
                .values(published_at=sa.func.now())
            )
        metrics.OUTBOX_PUBLISHED.inc(len(rows))
        log.info("outbox_relayed", count=len(rows))
        return len(rows)

import asyncio
import hashlib
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass

import structlog
from pydantic import ValidationError

from pulse import metrics
from pulse.domain.events import EnrichedEvent
from pulse.ports import DeadLetterPublisher, EventSink, MessageSource, RawMessage, TopicPartitionKey

log = structlog.get_logger(__name__)


@dataclass(frozen=True, slots=True)
class WriterConfig:
    batch_size: int = 5000
    flush_interval: float = 1.0
    max_insert_attempts: int = 5
    retry_base_delay: float = 0.5


class SinkUnavailableError(RuntimeError):
    pass


def dedup_token(messages: Sequence[RawMessage]) -> str:
    """Deterministic token for a batch: identical batches are inserted into ClickHouse once."""
    ranges: dict[TopicPartitionKey, tuple[int, int]] = {}
    for m in messages:
        lo, hi = ranges.get((m.topic, m.partition), (m.offset, m.offset))
        ranges[(m.topic, m.partition)] = (min(lo, m.offset), max(hi, m.offset))
    raw = ";".join(f"{t}:{p}:{lo}-{hi}" for (t, p), (lo, hi) in sorted(ranges.items()))
    return hashlib.sha256(raw.encode()).hexdigest()


def next_offsets(messages: Sequence[RawMessage]) -> dict[TopicPartitionKey, int]:
    offsets: dict[TopicPartitionKey, int] = {}
    for m in messages:
        tp = (m.topic, m.partition)
        offsets[tp] = max(offsets.get(tp, 0), m.offset + 1)
    return offsets


class BatchWriter:
    """Kafka -> ClickHouse with at-least-once delivery.

    Offsets are committed only after the batch is durably written, so a crash can only cause
    re-delivery, never loss. Duplicates are neutralized by the insert dedup token (exact retries)
    and by ReplacingMergeTree on `event_id` (re-delivery after a rebalance).
    Poison messages go to the DLQ instead of blocking the partition.
    """

    def __init__(
        self,
        source: MessageSource,
        sink: EventSink,
        dlq: DeadLetterPublisher,
        config: WriterConfig | None = None,
        sleep: Callable[[float], object] = asyncio.sleep,
    ) -> None:
        self._source = source
        self._sink = sink
        self._dlq = dlq
        self._config = config or WriterConfig()
        self._sleep = sleep
        self._buffer: list[RawMessage] = []
        self._lock = asyncio.Lock()
        self.last_heartbeat = time.monotonic()

    async def run(self, stop: asyncio.Event) -> None:
        deadline = time.monotonic() + self._config.flush_interval
        while not stop.is_set():
            self.last_heartbeat = time.monotonic()
            timeout_ms = max(int((deadline - time.monotonic()) * 1000), 0)
            room = self._config.batch_size - len(self._buffer)
            # Await first: a rebalance during poll may flush and replace `self._buffer`.
            records = await self._source.poll(max_records=room, timeout_ms=timeout_ms)
            self._buffer.extend(records)
            if len(self._buffer) >= self._config.batch_size or time.monotonic() >= deadline:
                await self.flush()
                deadline = time.monotonic() + self._config.flush_interval
        await self.flush()

    async def flush(self) -> None:
        async with self._lock:
            if not self._buffer:
                return
            batch, self._buffer = self._buffer, []
            events = await self._decode(batch)
            if events:
                await self._write_with_retry(events, dedup_token(batch))
            await self._source.commit(next_offsets(batch))
            metrics.WRITER_BATCH_SIZE.observe(len(batch))
            metrics.WRITER_EVENTS.inc(len(events))
            log.debug("batch_flushed", messages=len(batch), written=len(events))

    async def _decode(self, batch: Sequence[RawMessage]) -> list[EnrichedEvent]:
        events = []
        for message in batch:
            if message.value is None:
                await self._dead_letter(message, "empty", "message has no value")
                continue
            try:
                events.append(EnrichedEvent.model_validate_json(message.value))
            except ValidationError as exc:
                await self._dead_letter(message, "invalid", str(exc))
        return events

    async def _dead_letter(self, message: RawMessage, reason: str, error: str) -> None:
        await self._dlq.send(message, reason, error)
        metrics.WRITER_DLQ.labels(reason=reason).inc()

    async def _write_with_retry(self, events: Sequence[EnrichedEvent], token: str) -> None:
        for attempt in range(1, self._config.max_insert_attempts + 1):
            try:
                with metrics.WRITER_INSERT_LATENCY.time():
                    await self._sink.write(events, token)
                return
            except Exception as exc:
                if attempt == self._config.max_insert_attempts:
                    raise SinkUnavailableError("ClickHouse insert failed, giving up") from exc
                delay = self._config.retry_base_delay * 2 ** (attempt - 1)
                metrics.WRITER_RETRIES.inc()
                log.warning("insert_failed", attempt=attempt, retry_in=delay, error=str(exc))
                await self._sleep(delay)  # type: ignore[misc]

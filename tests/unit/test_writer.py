import asyncio
from datetime import UTC, datetime
from uuid import uuid4

import pytest

from pulse.application.writer import (
    BatchWriter,
    SinkUnavailableError,
    WriterConfig,
    dedup_token,
    next_offsets,
)
from pulse.domain.events import EnrichedEvent
from pulse.ports import RawMessage
from tests.fakes import FakeDlq, FakeSink, FakeSource


def raw(offset: int, value: bytes | None) -> RawMessage:
    return RawMessage("events.raw", 0, offset, b"k", value)


def message(offset: int, partition: int = 0) -> RawMessage:
    now = datetime.now(UTC)
    value = (
        EnrichedEvent(
            project_id=uuid4(),
            event_id=uuid4(),
            name="e",
            user_id="u",
            timestamp=now,
            received_at=now,
        )
        .model_dump_json()
        .encode()
    )
    return RawMessage("events.raw", partition, offset, b"k", value)


async def no_sleep(_: float) -> None:
    return None


def make_writer(source: FakeSource, sink: FakeSink, dlq: FakeDlq, **config: object) -> BatchWriter:
    cfg = WriterConfig(**{"flush_interval": 0.01, "retry_base_delay": 0, **config})  # type: ignore[arg-type]
    return BatchWriter(source, sink, dlq, cfg, sleep=no_sleep)


async def test_offsets_are_committed_after_successful_write() -> None:
    stop = asyncio.Event()
    source = FakeSource([[message(0), message(1), message(0, partition=1)]], stop)
    sink, dlq = FakeSink(), FakeDlq()

    await make_writer(source, sink, dlq).run(stop)

    assert len(sink.writes[0][0]) == 3
    assert source.commits == [{("events.raw", 0): 2, ("events.raw", 1): 1}]


async def test_poison_messages_go_to_dlq_and_do_not_block_the_partition() -> None:
    stop = asyncio.Event()
    source = FakeSource([[message(0), raw(1, b"not json"), raw(2, None)]], stop)
    sink, dlq = FakeSink(), FakeDlq()

    await make_writer(source, sink, dlq).run(stop)

    assert [reason for _, reason in dlq.messages] == ["invalid", "empty"]
    assert len(sink.writes[0][0]) == 1
    assert source.commits == [{("events.raw", 0): 3}]


async def test_transient_sink_failures_are_retried_with_the_same_dedup_token() -> None:
    sink = FakeSink(failures=2)
    source = FakeSource([[message(0)]])
    writer = make_writer(source, sink, FakeDlq(), batch_size=1)
    source._stop = asyncio.Event()

    await writer.run(source._stop)

    assert sink.attempts == 3
    assert len(sink.writes) == 1
    assert source.commits


async def test_offsets_are_not_committed_when_sink_is_down() -> None:
    stop = asyncio.Event()
    source = FakeSource([[message(0)]], stop)
    sink = FakeSink(failures=10)

    with pytest.raises(SinkUnavailableError):
        await make_writer(source, sink, FakeDlq(), max_insert_attempts=3).run(stop)

    assert sink.attempts == 3
    assert source.commits == []


async def test_batch_is_flushed_when_full() -> None:
    stop = asyncio.Event()
    source = FakeSource([[message(i) for i in range(5)]], stop)
    sink = FakeSink()

    await make_writer(source, sink, FakeDlq(), batch_size=2, flush_interval=60).run(stop)

    assert [len(events) for events, _ in sink.writes] == [2, 2, 1]


def test_dedup_token_is_deterministic_and_order_independent() -> None:
    batch = [message(5), message(6), message(1, partition=2)]
    assert dedup_token(batch) == dedup_token(list(reversed(batch)))
    assert dedup_token(batch) != dedup_token(batch[:2])


def test_next_offsets_takes_max_per_partition() -> None:
    assert next_offsets([message(3), message(9), message(4, partition=1)]) == {
        ("events.raw", 0): 10,
        ("events.raw", 1): 5,
    }

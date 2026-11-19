from collections.abc import Awaitable, Callable, Mapping, Sequence
from typing import Any

import structlog
from aiokafka import AIOKafkaConsumer, ConsumerRebalanceListener, TopicPartition
from aiokafka.errors import CommitFailedError

from pulse.ports import RawMessage, TopicPartitionKey

log = structlog.get_logger(__name__)


def create_writer_consumer(bootstrap_servers: str, group_id: str) -> AIOKafkaConsumer:
    return AIOKafkaConsumer(
        bootstrap_servers=bootstrap_servers,
        group_id=group_id,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        isolation_level="read_committed",
        max_partition_fetch_bytes=4 * 1024 * 1024,
        session_timeout_ms=30_000,
        max_poll_interval_ms=300_000,
    )


class FlushOnRevoke(ConsumerRebalanceListener):  # type: ignore[misc]
    """Flush buffered messages before partitions move to another consumer.

    Without this, the new owner would re-read everything after the last commit.
    """

    def __init__(self) -> None:
        self.flush: Callable[[], Awaitable[None]] | None = None

    async def on_partitions_revoked(self, revoked: Any) -> None:
        if self.flush is not None and revoked:
            await self.flush()
        log.info("partitions_revoked", partitions=[f"{tp.topic}:{tp.partition}" for tp in revoked])

    async def on_partitions_assigned(self, assigned: Any) -> None:
        log.info(
            "partitions_assigned", partitions=[f"{tp.topic}:{tp.partition}" for tp in assigned]
        )


class KafkaMessageSource:
    def __init__(self, consumer: AIOKafkaConsumer) -> None:
        self._consumer = consumer

    async def poll(self, max_records: int, timeout_ms: int) -> Sequence[RawMessage]:
        data = await self._consumer.getmany(timeout_ms=timeout_ms, max_records=max_records)
        return [
            RawMessage(r.topic, r.partition, r.offset, r.key, r.value)
            for records in data.values()
            for r in records
        ]

    async def commit(self, offsets: Mapping[TopicPartitionKey, int]) -> None:
        try:
            await self._consumer.commit({TopicPartition(t, p): o for (t, p), o in offsets.items()})
        except CommitFailedError:
            # The group rebalanced; the new owner re-reads from the last commit and
            # ClickHouse deduplicates by event_id. Safe to continue.
            log.warning("commit_failed_after_rebalance", partitions=len(offsets))

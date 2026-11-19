import asyncio
import contextlib

import structlog
from aiokafka import AIOKafkaConsumer, TopicPartition
from aiokafka.admin import AIOKafkaAdminClient

from pulse.application.key_registry import ApiKeyRegistry

log = structlog.get_logger(__name__)


class KafkaKeyRegistryFeed:
    """Replays the compacted API-key topic from the beginning, then tails it.

    The registry reports `ready` only after it caught up with the end offsets observed at
    startup; Kubernetes readiness probes keep the pod out of rotation until then.
    """

    def __init__(self, bootstrap_servers: str, topic: str, registry: ApiKeyRegistry) -> None:
        self._registry = registry
        self._topic = topic
        self._bootstrap_servers = bootstrap_servers
        self._consumer = AIOKafkaConsumer(
            bootstrap_servers=bootstrap_servers,
            group_id=None,
            enable_auto_commit=False,
            auto_offset_reset="earliest",
        )
        self._task: asyncio.Task[None] | None = None

    async def start(self) -> None:
        partitions = await self._partitions()
        await self._consumer.start()
        tps = [TopicPartition(self._topic, p) for p in sorted(partitions)]
        self._consumer.assign(tps)
        await self._consumer.seek_to_beginning(*tps)
        end_offsets = await self._consumer.end_offsets(tps)
        self._task = asyncio.create_task(self._run(end_offsets), name="key-registry-feed")
        self._task.add_done_callback(self._on_feed_stopped)

    def _on_feed_stopped(self, task: asyncio.Task[None]) -> None:
        if task.cancelled():
            return
        self._registry.mark_stale()
        log.error("key_registry_feed_stopped", error=repr(task.exception()))

    async def _partitions(self) -> list[int]:
        admin = AIOKafkaAdminClient(bootstrap_servers=self._bootstrap_servers)
        await admin.start()
        try:
            (topic,) = await admin.describe_topics([self._topic])
        finally:
            await admin.close()
        partitions = [p["partition"] for p in topic.get("partitions", [])]
        if topic.get("error_code") or not partitions:
            raise RuntimeError(f"topic '{self._topic}' does not exist; run `pulse migrate kafka`")
        return partitions

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
        await self._consumer.stop()

    async def _run(self, end_offsets: dict[TopicPartition, int]) -> None:
        pending = {tp: end for tp, end in end_offsets.items() if end > 0}
        if not pending:
            self._ready()
        while True:
            batch = await self._consumer.getmany(timeout_ms=1000)
            for records in batch.values():
                for record in records:
                    self._registry.apply(record.key, record.value)
            if pending:
                for tp in list(pending):
                    if await self._consumer.position(tp) >= pending[tp]:
                        del pending[tp]
                if not pending:
                    self._ready()

    def _ready(self) -> None:
        self._registry.mark_ready()
        log.info("key_registry_ready", keys=len(self._registry))

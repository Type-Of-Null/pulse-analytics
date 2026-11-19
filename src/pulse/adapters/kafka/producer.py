import asyncio
from collections.abc import Sequence

import orjson
from aiokafka import AIOKafkaProducer

from pulse.domain.events import SCHEMA_VERSION, EnrichedEvent
from pulse.ports import RawMessage

MAX_ERROR_HEADER_LENGTH = 2048


def create_producer(bootstrap_servers: str, client_id: str) -> AIOKafkaProducer:
    # Idempotent producer: no duplicates or reordering on internal retries;
    # acks=all: an event is acknowledged only after all in-sync replicas have it.
    return AIOKafkaProducer(
        bootstrap_servers=bootstrap_servers,
        client_id=client_id,
        enable_idempotence=True,
        acks="all",
        compression_type="lz4",
        linger_ms=5,
        max_batch_size=256 * 1024,
    )


class KafkaEventPublisher:
    def __init__(self, producer: AIOKafkaProducer, topic: str) -> None:
        self._producer = producer
        self._topic = topic

    async def publish(self, events: Sequence[EnrichedEvent]) -> None:
        headers = [("schema_version", str(SCHEMA_VERSION).encode())]
        # send() only enqueues into the producer's batch; gather waits for broker acks,
        # so a single HTTP request costs roughly one round trip instead of N.
        futures = [
            await self._producer.send(
                self._topic,
                value=orjson.dumps(event.model_dump(mode="json")),
                key=event.partition_key,
                headers=headers,
            )
            for event in events
        ]
        await asyncio.gather(*futures)


class KafkaDeadLetterPublisher:
    def __init__(self, producer: AIOKafkaProducer, topic: str) -> None:
        self._producer = producer
        self._topic = topic

    async def send(self, message: RawMessage, reason: str, error: str) -> None:
        await self._producer.send_and_wait(
            self._topic,
            value=message.value,
            key=message.key,
            headers=[
                ("dlq_reason", reason.encode()),
                ("dlq_error", error[:MAX_ERROR_HEADER_LENGTH].encode()),
                ("source_topic", message.topic.encode()),
                ("source_partition", str(message.partition).encode()),
                ("source_offset", str(message.offset).encode()),
            ],
        )

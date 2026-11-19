"""Long-running background workers: Kafka->ClickHouse writer and the outbox relay."""

import asyncio
import time

import structlog

from pulse.adapters.clickhouse.client import create_client
from pulse.adapters.clickhouse.sink import ClickHouseEventSink
from pulse.adapters.kafka.consumer import FlushOnRevoke, KafkaMessageSource, create_writer_consumer
from pulse.adapters.kafka.producer import KafkaDeadLetterPublisher, create_producer
from pulse.adapters.postgres.outbox_relay import OutboxRelay
from pulse.adapters.postgres.uow import create_engine
from pulse.application.writer import BatchWriter, WriterConfig
from pulse.config import Settings
from pulse.entrypoints.common import install_signal_handlers
from pulse.http_probe import ProbeServer

log = structlog.get_logger(__name__)

LIVENESS_TIMEOUT_SECONDS = 60


async def run_writer(settings: Settings) -> None:
    stop = asyncio.Event()
    install_signal_handlers(stop)

    listener = FlushOnRevoke()
    consumer = create_writer_consumer(settings.kafka.bootstrap_servers, settings.kafka.writer_group)
    consumer.subscribe([settings.kafka.events_topic], listener=listener)
    producer = create_producer(settings.kafka.bootstrap_servers, client_id="writer-dlq")
    client = await create_client(settings.clickhouse)
    writer = BatchWriter(
        source=KafkaMessageSource(consumer),
        sink=ClickHouseEventSink(client),
        dlq=KafkaDeadLetterPublisher(producer, settings.kafka.dlq_topic),
        config=WriterConfig(
            batch_size=settings.writer.batch_size,
            flush_interval=settings.writer.flush_interval_seconds,
            max_insert_attempts=settings.writer.max_insert_attempts,
            retry_base_delay=settings.writer.retry_base_delay_seconds,
        ),
    )
    listener.flush = writer.flush
    probe = ProbeServer(
        settings.http_host,
        settings.http_port,
        is_alive=lambda: time.monotonic() - writer.last_heartbeat < LIVENESS_TIMEOUT_SECONDS,
    )

    await probe.start()
    await producer.start()
    await consumer.start()
    log.info("writer_started", topic=settings.kafka.events_topic)
    try:
        await writer.run(stop)
    finally:
        await consumer.stop()
        await producer.stop()
        await client.close()
        await probe.stop()
        log.info("writer_stopped")


async def run_outbox_relay(settings: Settings) -> None:
    stop = asyncio.Event()
    install_signal_handlers(stop)

    engine = create_engine(settings.postgres)
    producer = create_producer(settings.kafka.bootstrap_servers, client_id="outbox-relay")
    relay = OutboxRelay(
        engine,
        producer,
        batch_size=settings.outbox.batch_size,
        poll_interval=settings.outbox.poll_interval_seconds,
    )
    probe = ProbeServer(settings.http_host, settings.http_port, is_alive=lambda: True)

    await probe.start()
    await producer.start()
    log.info("outbox_relay_started")
    try:
        await relay.run(stop)
    finally:
        await producer.stop()
        await engine.dispose()
        await probe.stop()

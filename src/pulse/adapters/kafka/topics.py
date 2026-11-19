import structlog
from aiokafka.admin import AIOKafkaAdminClient, NewTopic

from pulse.config import KafkaSettings

log = structlog.get_logger(__name__)

DAY_MS = 24 * 3600 * 1000


def desired_topics(cfg: KafkaSettings) -> list[NewTopic]:
    rf = cfg.replication_factor
    durability = {"min.insync.replicas": str(min(2, rf))}
    return [
        NewTopic(
            cfg.events_topic,
            num_partitions=cfg.events_partitions,
            replication_factor=rf,
            topic_configs={**durability, "retention.ms": str(7 * DAY_MS)},
        ),
        NewTopic(
            cfg.dlq_topic,
            num_partitions=3,
            replication_factor=rf,
            topic_configs={**durability, "retention.ms": str(30 * DAY_MS)},
        ),
        # Single partition: one global order of key changes; the topic is tiny.
        NewTopic(
            cfg.keys_topic,
            num_partitions=1,
            replication_factor=rf,
            topic_configs={
                **durability,
                "cleanup.policy": "compact",
                "min.cleanable.dirty.ratio": "0.1",
                "delete.retention.ms": str(DAY_MS),
            },
        ),
    ]


async def ensure_topics(cfg: KafkaSettings) -> None:
    admin = AIOKafkaAdminClient(bootstrap_servers=cfg.bootstrap_servers)
    await admin.start()
    try:
        existing = set(await admin.list_topics())
        missing = [t for t in desired_topics(cfg) if t.name not in existing]
        if missing:
            await admin.create_topics(missing)
        log.info("kafka_topics_ready", created=[t.name for t in missing])
    finally:
        await admin.close()

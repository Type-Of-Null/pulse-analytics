from prometheus_client import Counter, Gauge, Histogram

INGESTED_EVENTS = Counter(
    "pulse_ingest_events_total", "Events accepted or rejected by ingest-api", ["status"]
)
CLOCK_SKEW_CLAMPED = Counter(
    "pulse_ingest_clock_skew_clamped_total", "Events whose future timestamp was clamped"
)
PUBLISH_LATENCY = Histogram(
    "pulse_ingest_publish_seconds", "Time to get Kafka acks for one ingest batch"
)
API_KEYS = Gauge("pulse_api_keys", "Active API keys materialized from the compacted topic")

WRITER_BATCH_SIZE = Histogram(
    "pulse_writer_batch_size",
    "Messages per ClickHouse insert",
    buckets=(1, 10, 100, 500, 1000, 2500, 5000, 10000, 25000),
)
WRITER_INSERT_LATENCY = Histogram("pulse_writer_insert_seconds", "ClickHouse insert latency")
WRITER_EVENTS = Counter("pulse_writer_events_total", "Events written to ClickHouse")
WRITER_DLQ = Counter("pulse_writer_dlq_total", "Messages sent to the dead letter topic", ["reason"])
WRITER_RETRIES = Counter("pulse_writer_insert_retries_total", "ClickHouse insert retries")

OUTBOX_PUBLISHED = Counter("pulse_outbox_published_total", "Outbox records relayed to Kafka")
OUTBOX_LEADER = Gauge("pulse_outbox_is_leader", "1 if this relay replica holds the leader lock")

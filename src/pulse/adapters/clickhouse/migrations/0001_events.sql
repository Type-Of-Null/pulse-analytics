-- Raw events. ReplacingMergeTree collapses re-delivered copies (same sorting key incl. event_id)
-- during background merges; partitions by month make TTL and late-data handling cheap.
CREATE TABLE IF NOT EXISTS events
(
    project_id   UUID,
    event_id     UUID,
    name         LowCardinality(String),
    user_id      String,
    session_id   String,
    timestamp    DateTime64(3, 'UTC'),
    received_at  DateTime64(3, 'UTC'),
    properties   Map(LowCardinality(String), String) CODEC(ZSTD(3)),
    INDEX idx_user user_id TYPE bloom_filter(0.01) GRANULARITY 4
)
ENGINE = ReplacingMergeTree(received_at)
PARTITION BY toYYYYMM(timestamp)
ORDER BY (project_id, name, toDate(timestamp), user_id, event_id)
TTL toDateTime(timestamp) + INTERVAL 1 YEAR
SETTINGS non_replicated_deduplication_window = 1000;

-- Pre-aggregated rollup maintained by ClickHouse on every insert into `events`.
-- Dashboards read thousands of rows instead of scanning billions of raw events.
-- Counts are distinct event_ids, so re-sent events don't inflate them (the MV sees every insert).
CREATE TABLE IF NOT EXISTS events_by_minute
(
    project_id UUID,
    name       LowCardinality(String),
    minute     DateTime('UTC'),
    events     AggregateFunction(uniq, UUID),
    users      AggregateFunction(uniq, String)
)
ENGINE = AggregatingMergeTree
PARTITION BY toYYYYMM(minute)
ORDER BY (project_id, name, minute)
TTL minute + INTERVAL 2 YEAR;

CREATE MATERIALIZED VIEW IF NOT EXISTS events_by_minute_mv TO events_by_minute AS
SELECT
    project_id,
    name,
    toStartOfMinute(timestamp) AS minute,
    uniqState(event_id) AS events, -- client retries reuse event_id: not double-counted
    uniqState(user_id) AS users
FROM events
GROUP BY project_id, name, minute;

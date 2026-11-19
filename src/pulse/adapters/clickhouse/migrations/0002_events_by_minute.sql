-- Pre-aggregated rollup maintained by ClickHouse on every insert into `events`.
-- Dashboards read thousands of rows instead of scanning billions of raw events.
CREATE TABLE IF NOT EXISTS events_by_minute
(
    project_id UUID,
    name       LowCardinality(String),
    minute     DateTime('UTC'),
    events     AggregateFunction(count),
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
    countState() AS events,
    uniqState(user_id) AS users
FROM events
GROUP BY project_id, name, minute;

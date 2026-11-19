"""Analytics queries. All user input goes through server-side parameter binding."""

from collections.abc import Sequence
from datetime import UTC, date, datetime
from typing import Any
from uuid import UUID

from pulse.domain.analytics import EventCount, Granularity, TimeRange, TimeseriesPoint

_INTERVAL = {Granularity.MINUTE: "MINUTE", Granularity.HOUR: "HOUR", Granularity.DAY: "DAY"}


def _naive_utc(value: datetime) -> datetime:
    return value.astimezone(UTC).replace(tzinfo=None) if value.tzinfo else value


def _period_params(project_id: UUID, period: TimeRange) -> dict[str, Any]:
    return {"project": project_id, "start": _naive_utc(period.start), "end": _naive_utc(period.end)}


def build_funnel_query(step_count: int, window_seconds: int) -> str:
    conditions = ", ".join(f"name = {{s{i}:String}}" for i in range(step_count))
    return f"""
        SELECT level, count() AS users
        FROM (
            SELECT user_id,
                   windowFunnel({int(window_seconds) * 1000})(
                       toUInt64(toUnixTimestamp64Milli(timestamp)), {conditions}
                   ) AS level
            FROM events
            WHERE project_id = {{project:UUID}}
              AND timestamp >= {{start:DateTime64(3, 'UTC')}}
              AND timestamp < {{end:DateTime64(3, 'UTC')}}
              AND name IN {{steps:Array(String)}}
            GROUP BY user_id
        )
        WHERE level > 0
        GROUP BY level
    """


def build_retention_query(days: int) -> str:
    conditions = ["name = {start_event:String} AND toDate(timestamp) = {cohort:Date}"]
    conditions += [
        f"name = {{return_event:String}} AND toDate(timestamp) = {{cohort:Date}} + {int(day)}"
        for day in range(1, days + 1)
    ]
    return f"""
        SELECT sumForEach(r) AS counts
        FROM (
            SELECT user_id, retention({", ".join(conditions)}) AS r
            FROM events
            WHERE project_id = {{project:UUID}}
              AND toDate(timestamp) BETWEEN {{cohort:Date}} AND {{cohort:Date}} + {int(days)}
              AND name IN ({{start_event:String}}, {{return_event:String}})
            GROUP BY user_id
        )
    """


class ClickHouseAnalyticsRepository:
    """Timeseries and top events read the per-minute rollup: they cover every minute that
    overlaps the requested range. Funnels and retention read raw events."""

    def __init__(self, client: Any = None) -> None:
        self._client = client

    def bind(self, client: Any) -> None:
        self._client = client

    async def timeseries(
        self, project_id: UUID, event: str | None, period: TimeRange, granularity: Granularity
    ) -> list[TimeseriesPoint]:
        interval = f"INTERVAL 1 {_INTERVAL[granularity]}"
        event_filter = "AND name = {event:String}" if event else ""
        sql = f"""
            SELECT toStartOfInterval(minute, {interval}) AS bucket,
                   uniqMerge(events) AS events,
                   uniqMerge(users) AS users
            FROM events_by_minute
            WHERE project_id = {{project:UUID}}
              AND minute >= toStartOfMinute({{start:DateTime('UTC')}})
              AND minute < {{end:DateTime('UTC')}}
              {event_filter}
            GROUP BY bucket
            ORDER BY bucket WITH FILL
                FROM toStartOfInterval({{start:DateTime('UTC')}}, {interval})
                TO {{end:DateTime('UTC')}}
                STEP {interval}
        """
        params = _period_params(project_id, period) | ({"event": event} if event else {})
        result = await self._client.query(sql, parameters=params)
        return [
            TimeseriesPoint(bucket=b.replace(tzinfo=UTC), events=e, users=u)
            for b, e, u in result.result_rows
        ]

    async def top_events(self, project_id: UUID, period: TimeRange, limit: int) -> list[EventCount]:
        sql = """
            SELECT name, uniqMerge(events) AS events, uniqMerge(users) AS users
            FROM events_by_minute
            WHERE project_id = {project:UUID}
              AND minute >= toStartOfMinute({start:DateTime('UTC')})
              AND minute < {end:DateTime('UTC')}
            GROUP BY name
            ORDER BY events DESC
            LIMIT {limit:UInt32}
        """
        params = _period_params(project_id, period) | {"limit": limit}
        result = await self._client.query(sql, parameters=params)
        return [EventCount(name=n, events=e, users=u) for n, e, u in result.result_rows]

    async def funnel_levels(
        self, project_id: UUID, steps: Sequence[str], window_seconds: int, period: TimeRange
    ) -> dict[int, int]:
        params = _period_params(project_id, period) | {"steps": list(steps)}
        params |= {f"s{i}": step for i, step in enumerate(steps)}
        result = await self._client.query(
            build_funnel_query(len(steps), window_seconds), parameters=params
        )
        return {int(level): int(users) for level, users in result.result_rows}

    async def retention_counts(
        self, project_id: UUID, start_event: str, return_event: str, cohort_date: date, days: int
    ) -> list[int]:
        params = {
            "project": project_id,
            "start_event": start_event,
            "return_event": return_event,
            "cohort": cohort_date,
        }
        result = await self._client.query(build_retention_query(days), parameters=params)
        counts = [int(c) for c in (result.result_rows[0][0] if result.result_rows else [])]
        return counts + [0] * (days + 1 - len(counts))

    async def ping(self) -> bool:
        return bool(await self._client.ping())

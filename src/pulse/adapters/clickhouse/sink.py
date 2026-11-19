from collections.abc import Sequence
from typing import Any

from pulse.domain.events import EnrichedEvent

COLUMNS = [
    "project_id",
    "event_id",
    "name",
    "user_id",
    "session_id",
    "timestamp",
    "received_at",
    "properties",
]


class ClickHouseEventSink:
    def __init__(self, client: Any, table: str = "events") -> None:
        self._client = client
        self._table = table

    async def write(self, events: Sequence[EnrichedEvent], dedup_token: str) -> None:
        rows = [
            [
                e.project_id,
                e.event_id,
                e.name,
                e.user_id,
                e.session_id,
                e.timestamp,
                e.received_at,
                e.properties,
            ]
            for e in events
        ]
        # Same token => ClickHouse silently skips the block. Makes "insert succeeded but the
        # response was lost, so we retried" exactly-once.
        await self._client.insert(
            self._table,
            rows,
            column_names=COLUMNS,
            settings={"insert_deduplication_token": dedup_token},
        )

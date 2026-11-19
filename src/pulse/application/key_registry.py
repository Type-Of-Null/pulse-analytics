from uuid import UUID

import orjson

from pulse import metrics


class ApiKeyRegistry:
    """In-memory view of API keys, rebuilt from a log-compacted Kafka topic.

    Topic contract: key = sha256(api key), value = {"project_id": ...} or a tombstone (revoked).
    Ingest and analytics services authenticate requests from memory and never query Postgres.
    """

    def __init__(self) -> None:
        self._keys: dict[str, UUID] = {}
        self._ready = False

    @property
    def ready(self) -> bool:
        return self._ready

    def mark_ready(self) -> None:
        self._ready = True

    def mark_stale(self) -> None:
        """The feed stopped: revocations are no longer seen, so stop authenticating."""
        self._ready = False

    def resolve(self, key_hash: str) -> UUID | None:
        return self._keys.get(key_hash)

    def apply(self, key: bytes | None, value: bytes | None) -> None:
        if key is None:
            return
        key_hash = key.decode()
        if value is None:
            self._keys.pop(key_hash, None)
        else:
            self._keys[key_hash] = UUID(orjson.loads(value)["project_id"])
        metrics.API_KEYS.set(len(self._keys))

    def __len__(self) -> int:
        return len(self._keys)

from uuid import uuid4

import orjson

from pulse.application.key_registry import ApiKeyRegistry


def test_registry_applies_upserts_and_tombstones() -> None:
    registry = ApiKeyRegistry()
    project = uuid4()
    value = orjson.dumps({"project_id": str(project)})

    registry.apply(b"hash-1", value)
    registry.apply(b"hash-2", value)
    registry.apply(b"hash-1", None)

    assert registry.resolve("hash-1") is None
    assert registry.resolve("hash-2") == project
    assert len(registry) == 1


def test_registry_ignores_records_without_key_and_unknown_tombstones() -> None:
    registry = ApiKeyRegistry()
    registry.apply(None, b"{}")
    registry.apply(b"missing", None)
    assert len(registry) == 0


def test_registry_is_not_ready_until_marked() -> None:
    registry = ApiKeyRegistry()
    assert not registry.ready
    registry.mark_ready()
    assert registry.ready

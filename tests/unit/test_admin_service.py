from pulse.application.admin import ProjectService
from pulse.domain.projects import hash_api_key
from tests.fakes import InMemoryStore


async def test_raw_key_is_never_persisted() -> None:
    store = InMemoryStore()
    service = ProjectService(store, "keys")
    project, issued = await service.create_project("shop")

    [stored] = store.keys.values()
    assert stored.key_hash == hash_api_key(issued.raw_key)
    assert issued.raw_key not in repr(stored)
    [outbox] = store.outbox
    assert outbox.key == stored.key_hash
    assert outbox.payload == {"project_id": str(project.id)}


async def test_revoking_twice_is_idempotent() -> None:
    store = InMemoryStore()
    service = ProjectService(store, "keys")
    project, issued = await service.create_project("shop")

    first = await service.revoke_key(project.id, issued.key.id)
    second = await service.revoke_key(project.id, issued.key.id)

    assert first.revoked_at == second.revoked_at
    assert len(store.outbox) == 2


async def test_uncommitted_changes_are_discarded() -> None:
    store = InMemoryStore()
    async with store() as uow:
        from datetime import UTC, datetime
        from uuid import uuid4

        from pulse.domain.projects import Project

        await uow.add_project(Project(uuid4(), "draft", datetime.now(UTC)))
    assert store.projects == {}

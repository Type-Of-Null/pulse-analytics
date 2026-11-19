from datetime import UTC, datetime
from uuid import UUID, uuid4

from pulse.domain.errors import NotFoundError
from pulse.domain.projects import ApiKey, IssuedApiKey, Project, generate_api_key, hash_api_key
from pulse.ports import OutboxMessage, UnitOfWork, UnitOfWorkFactory


class ProjectService:
    """Control plane. Every key change is written to the outbox in the same transaction,
    so the data plane (compacted Kafka topic) can never diverge from Postgres."""

    def __init__(self, uow_factory: UnitOfWorkFactory, keys_topic: str) -> None:
        self._uow = uow_factory
        self._keys_topic = keys_topic

    async def create_project(self, name: str) -> tuple[Project, IssuedApiKey]:
        async with self._uow() as uow:
            project = Project(id=uuid4(), name=name, created_at=datetime.now(UTC))
            await uow.add_project(project)
            issued = await self._issue(uow, project.id)
            await uow.commit()
        return project, issued

    async def list_projects(self) -> list[Project]:
        async with self._uow() as uow:
            return await uow.list_projects()

    async def list_keys(self, project_id: UUID) -> list[ApiKey]:
        async with self._uow() as uow:
            await self._require_project(uow, project_id)
            return await uow.list_api_keys(project_id)

    async def issue_key(self, project_id: UUID) -> IssuedApiKey:
        async with self._uow() as uow:
            await self._require_project(uow, project_id)
            issued = await self._issue(uow, project_id)
            await uow.commit()
        return issued

    async def revoke_key(self, project_id: UUID, key_id: UUID) -> ApiKey:
        async with self._uow() as uow:
            key = await uow.get_api_key(project_id, key_id)
            if key is None:
                raise NotFoundError("api key not found")
            if not key.is_active:
                return key
            revoked = await uow.revoke_api_key(key)
            await uow.add_outbox(OutboxMessage(self._keys_topic, key.key_hash, payload=None))
            await uow.commit()
        return revoked

    async def _issue(self, uow: UnitOfWork, project_id: UUID) -> IssuedApiKey:
        raw = generate_api_key()
        key = ApiKey(
            id=uuid4(),
            project_id=project_id,
            key_hash=hash_api_key(raw),
            display_prefix=raw[:10],
            created_at=datetime.now(UTC),
        )
        await uow.add_api_key(key)
        await uow.add_outbox(
            OutboxMessage(self._keys_topic, key.key_hash, payload={"project_id": str(project_id)})
        )
        return IssuedApiKey(key=key, raw_key=raw)

    @staticmethod
    async def _require_project(uow: UnitOfWork, project_id: UUID) -> Project:
        project = await uow.get_project(project_id)
        if project is None:
            raise NotFoundError("project not found")
        return project

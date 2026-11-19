from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from typing import Any
from uuid import UUID

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine, create_async_engine

from pulse.adapters.postgres.tables import api_keys, outbox, projects
from pulse.config import PostgresSettings
from pulse.domain.projects import ApiKey, Project
from pulse.ports import OutboxMessage


def create_engine(cfg: PostgresSettings) -> AsyncEngine:
    return create_async_engine(
        cfg.dsn.get_secret_value(), pool_size=cfg.pool_size, pool_pre_ping=True
    )


def _project(row: Any) -> Project:
    return Project(id=row.id, name=row.name, created_at=row.created_at)


def _key(row: Any) -> ApiKey:
    return ApiKey(
        id=row.id,
        project_id=row.project_id,
        key_hash=row.key_hash,
        display_prefix=row.display_prefix,
        created_at=row.created_at,
        revoked_at=row.revoked_at,
    )


class PostgresUnitOfWork:
    def __init__(self, conn: AsyncConnection) -> None:
        self._conn = conn

    async def add_project(self, project: Project) -> None:
        await self._conn.execute(
            projects.insert().values(
                id=project.id, name=project.name, created_at=project.created_at
            )
        )

    async def get_project(self, project_id: UUID) -> Project | None:
        row = (
            await self._conn.execute(projects.select().where(projects.c.id == project_id))
        ).first()
        return _project(row) if row else None

    async def list_projects(self) -> list[Project]:
        rows = await self._conn.execute(projects.select().order_by(projects.c.created_at))
        return [_project(r) for r in rows]

    async def add_api_key(self, key: ApiKey) -> None:
        await self._conn.execute(
            api_keys.insert().values(
                id=key.id,
                project_id=key.project_id,
                key_hash=key.key_hash,
                display_prefix=key.display_prefix,
                created_at=key.created_at,
            )
        )

    async def get_api_key(self, project_id: UUID, key_id: UUID) -> ApiKey | None:
        query = api_keys.select().where(
            api_keys.c.id == key_id, api_keys.c.project_id == project_id
        )
        row = (await self._conn.execute(query.with_for_update())).first()
        return _key(row) if row else None

    async def list_api_keys(self, project_id: UUID) -> list[ApiKey]:
        rows = await self._conn.execute(
            api_keys.select()
            .where(api_keys.c.project_id == project_id)
            .order_by(api_keys.c.created_at)
        )
        return [_key(r) for r in rows]

    async def revoke_api_key(self, key: ApiKey) -> ApiKey:
        row = (
            await self._conn.execute(
                api_keys.update()
                .where(api_keys.c.id == key.id)
                .values(revoked_at=datetime.now(UTC))
                .returning(*api_keys.c)
            )
        ).one()
        return _key(row)

    async def add_outbox(self, message: OutboxMessage) -> None:
        await self._conn.execute(
            outbox.insert().values(
                topic=message.topic,
                key=message.key,
                payload=dict(message.payload) if message.payload is not None else None,
            )
        )

    async def commit(self) -> None:
        await self._conn.commit()


class PostgresUnitOfWorkFactory:
    def __init__(self, engine: AsyncEngine) -> None:
        self._engine = engine

    @asynccontextmanager
    async def __call__(self) -> AsyncIterator[PostgresUnitOfWork]:
        async with self._engine.connect() as conn:
            await conn.begin()
            try:
                yield PostgresUnitOfWork(conn)
            finally:
                if conn.in_transaction():
                    await conn.rollback()


async def ping(engine: AsyncEngine) -> bool:
    async with engine.connect() as conn:
        await conn.execute(sa.text("SELECT 1"))
    return True

import secrets
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Annotated
from uuid import UUID

from fastapi import Depends, FastAPI, HTTPException, Security, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import BaseModel, Field

from pulse.adapters.postgres.uow import PostgresUnitOfWorkFactory, create_engine
from pulse.application.admin import ProjectService
from pulse.config import Settings, get_settings
from pulse.domain.projects import ApiKey, IssuedApiKey, Project
from pulse.entrypoints.common import install_common


class CreateProject(BaseModel):
    name: str = Field(min_length=1, max_length=200)


class ProjectOut(BaseModel):
    id: UUID
    name: str
    created_at: datetime

    @classmethod
    def of(cls, project: Project) -> "ProjectOut":
        return cls(id=project.id, name=project.name, created_at=project.created_at)


class ApiKeyOut(BaseModel):
    id: UUID
    project_id: UUID
    display_prefix: str
    created_at: datetime
    revoked_at: datetime | None

    @classmethod
    def of(cls, key: ApiKey) -> "ApiKeyOut":
        return cls(
            id=key.id,
            project_id=key.project_id,
            display_prefix=key.display_prefix,
            created_at=key.created_at,
            revoked_at=key.revoked_at,
        )


class IssuedKeyOut(ApiKeyOut):
    api_key: str = Field(description="Shown only once; store it securely")

    @classmethod
    def issued(cls, issued: IssuedApiKey) -> "IssuedKeyOut":
        return cls(**ApiKeyOut.of(issued.key).model_dump(), api_key=issued.raw_key)


class CreatedProjectOut(BaseModel):
    project: ProjectOut
    key: IssuedKeyOut


def build_app(service: ProjectService, admin_token: str, lifespan: object | None = None) -> FastAPI:
    app = FastAPI(title="Pulse Admin API", version="1.0.0", lifespan=lifespan)  # type: ignore[arg-type]
    install_common(app, lambda: True)

    bearer = HTTPBearer(auto_error=False)

    def require_admin(
        credentials: Annotated[HTTPAuthorizationCredentials | None, Security(bearer)],
    ) -> None:
        if credentials is None or not secrets.compare_digest(
            credentials.credentials.encode(), admin_token.encode()
        ):
            raise HTTPException(status.HTTP_401_UNAUTHORIZED, "admin token required")

    admin = [Depends(require_admin)]

    @app.post("/v1/projects", status_code=201, dependencies=admin)
    async def create_project(body: CreateProject) -> CreatedProjectOut:
        project, issued = await service.create_project(body.name)
        return CreatedProjectOut(project=ProjectOut.of(project), key=IssuedKeyOut.issued(issued))

    @app.get("/v1/projects", dependencies=admin)
    async def list_projects() -> list[ProjectOut]:
        return [ProjectOut.of(p) for p in await service.list_projects()]

    @app.get("/v1/projects/{project_id}/keys", dependencies=admin)
    async def list_keys(project_id: UUID) -> list[ApiKeyOut]:
        return [ApiKeyOut.of(k) for k in await service.list_keys(project_id)]

    @app.post("/v1/projects/{project_id}/keys", status_code=201, dependencies=admin)
    async def issue_key(project_id: UUID) -> IssuedKeyOut:
        return IssuedKeyOut.issued(await service.issue_key(project_id))

    @app.delete("/v1/projects/{project_id}/keys/{key_id}", dependencies=admin)
    async def revoke_key(project_id: UUID, key_id: UUID) -> ApiKeyOut:
        return ApiKeyOut.of(await service.revoke_key(project_id, key_id))

    return app


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or get_settings()
    admin_token = settings.admin_token.get_secret_value()
    if not admin_token:
        raise RuntimeError("PULSE_ADMIN_TOKEN must be set")
    engine = create_engine(settings.postgres)
    service = ProjectService(PostgresUnitOfWorkFactory(engine), settings.kafka.keys_topic)

    @asynccontextmanager
    async def lifespan(_: FastAPI) -> AsyncIterator[None]:
        try:
            yield
        finally:
            await engine.dispose()

    return build_app(service, admin_token, lifespan)

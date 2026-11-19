import asyncio
import signal
from collections.abc import Callable
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, FastAPI, Header, HTTPException, Request, status
from fastapi.responses import JSONResponse, Response
from prometheus_client import make_asgi_app

from pulse.domain.errors import EventRejectedError, InvalidQueryError, NotFoundError
from pulse.domain.projects import hash_api_key
from pulse.ports import ApiKeyResolver


def api_key_auth(resolver: ApiKeyResolver) -> Callable[..., UUID]:
    def dependency(x_api_key: Annotated[str | None, Header()] = None) -> UUID:
        if not resolver.ready:
            raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE, "key registry is warming up")
        project_id = resolver.resolve(hash_api_key(x_api_key)) if x_api_key else None
        if project_id is None:
            raise HTTPException(status.HTTP_401_UNAUTHORIZED, "invalid or revoked API key")
        return project_id

    return dependency


def health_router(is_ready: Callable[[], bool]) -> APIRouter:
    router = APIRouter(tags=["ops"])

    @router.get("/healthz", include_in_schema=False)
    async def healthz() -> Response:
        return Response("ok")

    @router.get("/readyz", include_in_schema=False)
    async def readyz() -> Response:
        if is_ready():
            return Response("ok")
        return Response("not ready", status_code=status.HTTP_503_SERVICE_UNAVAILABLE)

    return router


def install_common(app: FastAPI, is_ready: Callable[[], bool]) -> None:
    app.include_router(health_router(is_ready))
    app.mount("/metrics", make_asgi_app())

    @app.exception_handler(InvalidQueryError)
    @app.exception_handler(EventRejectedError)
    async def _unprocessable(_: Request, exc: Exception) -> JSONResponse:
        return JSONResponse({"detail": str(exc)}, status_code=422)

    @app.exception_handler(NotFoundError)
    async def _not_found(_: Request, exc: Exception) -> JSONResponse:
        return JSONResponse({"detail": str(exc)}, status_code=404)


def install_signal_handlers(stop: asyncio.Event) -> None:
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stop.set)

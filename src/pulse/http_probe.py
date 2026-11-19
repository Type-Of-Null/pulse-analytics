"""Minimal HTTP server for workers without a web framework: /healthz, /readyz, /metrics."""

import asyncio
from collections.abc import Callable

from prometheus_client import CONTENT_TYPE_LATEST, generate_latest


class ProbeServer:
    def __init__(
        self,
        host: str,
        port: int,
        is_alive: Callable[[], bool],
        is_ready: Callable[[], bool] | None = None,
    ) -> None:
        self._host = host
        self._port = port
        self._is_alive = is_alive
        self._is_ready = is_ready or is_alive
        self._server: asyncio.Server | None = None

    async def start(self) -> None:
        self._server = await asyncio.start_server(self._handle, self._host, self._port)

    async def stop(self) -> None:
        if self._server:
            self._server.close()
            await self._server.wait_closed()

    async def _handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            request_line = await asyncio.wait_for(reader.readline(), timeout=5)
            parts = request_line.decode(errors="replace").split()
            path = parts[1] if len(parts) > 1 else "/"
            if path == "/metrics":
                status, body, ctype = 200, generate_latest(), CONTENT_TYPE_LATEST
            elif path == "/healthz":
                status, body, ctype = self._probe(self._is_alive)
            elif path == "/readyz":
                status, body, ctype = self._probe(self._is_ready)
            else:
                status, body, ctype = 404, b"not found", "text/plain"
            reason = {200: "OK", 404: "Not Found", 503: "Service Unavailable"}[status]
            head = (
                f"HTTP/1.1 {status} {reason}\r\nContent-Type: {ctype}\r\n"
                f"Content-Length: {len(body)}\r\nConnection: close\r\n\r\n"
            )
            writer.write(head.encode() + body)
            await writer.drain()
        except (TimeoutError, ConnectionError):
            pass
        finally:
            writer.close()

    @staticmethod
    def _probe(check: Callable[[], bool]) -> tuple[int, bytes, str]:
        return (200, b"ok", "text/plain") if check() else (503, b"unavailable", "text/plain")

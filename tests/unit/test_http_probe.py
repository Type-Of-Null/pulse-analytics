import asyncio

from pulse.http_probe import ProbeServer


async def _get(port: int, path: str) -> str:
    reader, writer = await asyncio.open_connection("127.0.0.1", port)
    writer.write(f"GET {path} HTTP/1.1\r\nHost: x\r\n\r\n".encode())
    await writer.drain()
    data = await reader.read()
    writer.close()
    return data.decode()


async def test_probe_server_reports_health_and_metrics() -> None:
    alive = {"value": True}
    server = ProbeServer("127.0.0.1", 18765, is_alive=lambda: alive["value"])
    await server.start()
    try:
        assert " 200 " in await _get(18765, "/healthz")
        assert "python_info" in await _get(18765, "/metrics")
        assert " 404 " in await _get(18765, "/nope")
        alive["value"] = False
        assert " 503 " in await _get(18765, "/readyz")
    finally:
        await server.stop()

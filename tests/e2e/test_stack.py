"""End-to-end test against a running stack (docker compose or kind).

docker compose up -d --wait && uv run pytest -m e2e
"""

import os
import time
import uuid
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx
import pytest

pytestmark = pytest.mark.e2e

INGEST = os.getenv("PULSE_E2E_INGEST_URL", "http://localhost:8001")
ANALYTICS = os.getenv("PULSE_E2E_ANALYTICS_URL", "http://localhost:8002")
ADMIN = os.getenv("PULSE_E2E_ADMIN_URL", "http://localhost:8003")
ADMIN_TOKEN = os.getenv("PULSE_E2E_ADMIN_TOKEN", "dev-admin-token")


def eventually[T](probe: Callable[[], T | None], timeout: float = 30.0) -> T:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if (result := probe()) is not None:
            return result
        time.sleep(0.5)
    raise AssertionError("condition not met in time")


@pytest.fixture(scope="module")
def project() -> dict[str, Any]:
    response = httpx.post(
        f"{ADMIN}/v1/projects",
        json={"name": f"e2e-{uuid.uuid4().hex[:6]}"},
        headers={"Authorization": f"Bearer {ADMIN_TOKEN}"},
    )
    response.raise_for_status()
    created: dict[str, Any] = response.json()
    return created


def send(api_key: str, events: list[dict[str, Any]]) -> httpx.Response:
    return httpx.post(
        f"{INGEST}/v1/events", json={"events": events}, headers={"X-Api-Key": api_key}
    )


def test_full_pipeline(project: dict[str, Any]) -> None:
    key = project["key"]["api_key"]
    now = datetime.now(UTC) - timedelta(minutes=5)
    funnel = [("visit", 0), ("signup", 60), ("pay", 120)]
    events = [
        {"name": name, "user_id": f"u{u}", "timestamp": (now + timedelta(seconds=dt)).isoformat()}
        for u in range(10)
        for name, dt in funnel[: 1 + u % 3]
    ]
    duplicate = {**events[0], "event_id": str(uuid.uuid4())}

    # Key propagates Postgres -> outbox -> Kafka (compacted) -> ingest-api in-memory registry.
    first = eventually(lambda: r if (r := send(key, [duplicate])).status_code == 202 else None)
    assert first.json() == {"accepted": 1, "clamped": 0}
    assert send(key, [duplicate]).status_code == 202  # client retry, must not double-count
    assert send(key, events).status_code == 202

    auth = {"X-Api-Key": key}

    def top_events() -> list[dict[str, Any]] | None:
        body: list[dict[str, Any]] = httpx.get(
            f"{ANALYTICS}/v1/analytics/top-events", headers=auth
        ).json()
        return body if sum(e["events"] for e in body) >= len(events) + 1 else None

    top = {e["name"]: (e["events"], e["users"]) for e in eventually(top_events)}
    assert top["visit"][1] == 10
    assert top["signup"] == (6, 6)
    assert top["pay"] == (3, 3)

    funnel_resp = httpx.post(
        f"{ANALYTICS}/v1/analytics/funnel",
        json={"steps": ["visit", "signup", "pay"], "window_seconds": 3600},
        headers=auth,
    )
    assert [s["users"] for s in funnel_resp.json()] == [10, 6, 3]


def test_rejects_unknown_key() -> None:
    assert send("pk_unknown", [{"name": "x", "user_id": "u"}]).status_code == 401


def test_revoked_key_stops_working(project: dict[str, Any]) -> None:
    pid = project["project"]["id"]
    admin = {"Authorization": f"Bearer {ADMIN_TOKEN}"}
    issued = httpx.post(f"{ADMIN}/v1/projects/{pid}/keys", headers=admin).json()
    event = [{"name": "x", "user_id": "u"}]
    eventually(lambda: True if send(issued["api_key"], event).status_code == 202 else None)

    httpx.delete(f"{ADMIN}/v1/projects/{pid}/keys/{issued['id']}", headers=admin).raise_for_status()

    eventually(lambda: True if send(issued["api_key"], event).status_code == 401 else None)

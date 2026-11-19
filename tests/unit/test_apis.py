from datetime import UTC, datetime
from uuid import uuid4

import pytest
from aiokafka.errors import KafkaTimeoutError
from fastapi.testclient import TestClient

from pulse.application.admin import ProjectService
from pulse.application.analytics import AnalyticsService
from pulse.application.ingest import IngestEvents
from pulse.entrypoints import admin_api, analytics_api, ingest_api
from tests.fakes import FakeAnalyticsRepository, FakePublisher, InMemoryStore, StaticResolver

KEY = "pk_test"
PROJECT = uuid4()


@pytest.fixture
def publisher() -> FakePublisher:
    return FakePublisher()


@pytest.fixture
def ingest_client(publisher: FakePublisher) -> TestClient:
    app = ingest_api.build_app(IngestEvents(publisher), StaticResolver({KEY: PROJECT}))
    return TestClient(app)


def test_ingest_accepts_batch(ingest_client: TestClient, publisher: FakePublisher) -> None:
    response = ingest_client.post(
        "/v1/events",
        headers={"X-Api-Key": KEY},
        json={"events": [{"name": "page_view", "user_id": "u1", "properties": {"path": "/"}}]},
    )
    assert response.status_code == 202
    assert response.json() == {"accepted": 1, "clamped": 0}
    assert publisher.published[0].project_id == PROJECT


@pytest.mark.parametrize("headers", [{}, {"X-Api-Key": "pk_wrong"}])
def test_ingest_requires_valid_key(ingest_client: TestClient, headers: dict[str, str]) -> None:
    response = ingest_client.post(
        "/v1/events", headers=headers, json={"events": [{"name": "e", "user_id": "u"}]}
    )
    assert response.status_code == 401


def test_ingest_validates_payload(ingest_client: TestClient) -> None:
    response = ingest_client.post(
        "/v1/events", headers={"X-Api-Key": KEY}, json={"events": [{"name": "bad name"}]}
    )
    assert response.status_code == 422


def test_ingest_returns_503_when_kafka_is_down() -> None:
    publisher = FakePublisher(error=KafkaTimeoutError())
    client = TestClient(
        ingest_api.build_app(IngestEvents(publisher), StaticResolver({KEY: PROJECT}))
    )
    response = client.post(
        "/v1/events", headers={"X-Api-Key": KEY}, json={"events": [{"name": "e", "user_id": "u"}]}
    )
    assert response.status_code == 503


def test_ingest_not_ready_until_registry_caught_up() -> None:
    resolver = StaticResolver({KEY: PROJECT}, ready=False)
    client = TestClient(ingest_api.build_app(IngestEvents(FakePublisher()), resolver))
    assert client.get("/healthz").status_code == 200
    assert client.get("/readyz").status_code == 503
    response = client.post(
        "/v1/events", headers={"X-Api-Key": KEY}, json={"events": [{"name": "e", "user_id": "u"}]}
    )
    assert response.status_code == 503


def test_metrics_endpoint(ingest_client: TestClient) -> None:
    response = ingest_client.get("/metrics/")
    assert response.status_code == 200
    assert "pulse_ingest_events_total" in response.text


@pytest.fixture
def analytics() -> tuple[TestClient, FakeAnalyticsRepository]:
    repo = FakeAnalyticsRepository()
    app = analytics_api.build_app(AnalyticsService(repo), StaticResolver({KEY: PROJECT}))
    return TestClient(app, headers={"X-Api-Key": KEY}), repo


def test_timeseries_endpoint(analytics: tuple[TestClient, FakeAnalyticsRepository]) -> None:
    client, repo = analytics
    response = client.get(
        "/v1/analytics/timeseries",
        params={"event": "signup", "granularity": "day", "from": "2026-01-01T00:00:00Z"},
    )
    assert response.status_code == 200
    assert response.json()[0]["events"] == 10
    assert repo.calls[0] == ("timeseries", (PROJECT, "signup", "day"))


@pytest.mark.parametrize("bounds", [{"from": "2026-01-01T00:00:00"}, {"to": "2026-12-01T00:00:00"}])
def test_bounds_without_offset_are_treated_as_utc(
    analytics: tuple[TestClient, FakeAnalyticsRepository], bounds: dict[str, str]
) -> None:
    client, _ = analytics
    params = {"granularity": "day", "from": "2026-11-01T00:00:00+03:00"} | bounds
    assert client.get("/v1/analytics/timeseries", params=params).status_code == 200


def test_admin_api_refuses_to_start_without_token() -> None:
    from pulse.config import Settings

    with pytest.raises(RuntimeError, match="PULSE_ADMIN_TOKEN"):
        admin_api.create_app(Settings(admin_token=""))


def test_timeseries_rejects_too_wide_range(
    analytics: tuple[TestClient, FakeAnalyticsRepository],
) -> None:
    client, _ = analytics
    response = client.get(
        "/v1/analytics/timeseries",
        params={
            "granularity": "minute",
            "from": "2026-01-01T00:00:00Z",
            "to": "2026-01-05T00:00:00Z",
        },
    )
    assert response.status_code == 422


def test_funnel_endpoint(analytics: tuple[TestClient, FakeAnalyticsRepository]) -> None:
    client, repo = analytics
    repo.levels = {1: 6, 2: 4}
    response = client.post(
        "/v1/analytics/funnel", json={"steps": ["visit", "signup"], "window_seconds": 600}
    )
    assert response.status_code == 200
    assert [s["users"] for s in response.json()] == [10, 4]


def test_retention_endpoint(analytics: tuple[TestClient, FakeAnalyticsRepository]) -> None:
    client, repo = analytics
    repo.retention = [10, 5]
    response = client.get(
        "/v1/analytics/retention",
        params={
            "start_event": "signup",
            "return_event": "open",
            "cohort_date": "2026-01-01",
            "days": 1,
        },
    )
    assert response.status_code == 200
    assert [p["rate"] for p in response.json()] == [1.0, 0.5]


@pytest.fixture
def admin() -> tuple[TestClient, InMemoryStore]:
    store = InMemoryStore()
    app = admin_api.build_app(ProjectService(store, "keys"), admin_token="secret")
    return TestClient(app, headers={"Authorization": "Bearer secret"}), store


def test_admin_requires_token(admin: tuple[TestClient, InMemoryStore]) -> None:
    client, _ = admin
    assert client.get("/v1/projects", headers={"Authorization": "Bearer nope"}).status_code == 401


def test_project_lifecycle_writes_outbox(admin: tuple[TestClient, InMemoryStore]) -> None:
    client, store = admin

    created = client.post("/v1/projects", json={"name": "shop"}).json()
    project_id = created["project"]["id"]
    assert created["key"]["api_key"].startswith("pk_")

    second = client.post(f"/v1/projects/{project_id}/keys").json()
    revoked = client.delete(f"/v1/projects/{project_id}/keys/{second['id']}").json()
    assert revoked["revoked_at"] is not None

    keys = client.get(f"/v1/projects/{project_id}/keys").json()
    assert len(keys) == 2
    assert "api_key" not in keys[0]
    assert [m.payload is None for m in store.outbox] == [False, False, True]


def test_unknown_project_returns_404(admin: tuple[TestClient, InMemoryStore]) -> None:
    client, _ = admin
    assert client.post(f"/v1/projects/{uuid4()}/keys").status_code == 404
    assert client.delete(f"/v1/projects/{uuid4()}/keys/{uuid4()}").status_code == 404


def test_now_is_timezone_aware() -> None:
    assert analytics_api._now().tzinfo is UTC
    assert datetime.now(UTC).tzinfo is UTC

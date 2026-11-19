from datetime import UTC, date, datetime, timedelta
from uuid import uuid4

import pytest

from pulse.adapters.clickhouse.repository import build_funnel_query, build_retention_query
from pulse.application.analytics import AnalyticsService
from pulse.domain.analytics import (
    Granularity,
    TimeRange,
    funnel_from_levels,
    retention_from_counts,
)
from pulse.domain.errors import InvalidQueryError
from tests.fakes import FakeAnalyticsRepository

T0 = datetime(2026, 1, 1, tzinfo=UTC)


def test_funnel_from_levels_accumulates_users_reaching_each_step() -> None:
    # 50 users stopped at step 1, 30 at step 2, 20 completed all 3.
    steps = funnel_from_levels(["visit", "signup", "pay"], {1: 50, 2: 30, 3: 20})

    assert [s.users for s in steps] == [100, 50, 20]
    assert [s.conversion_from_start for s in steps] == [1.0, 0.5, 0.2]
    assert [s.conversion_from_previous for s in steps] == [1.0, 0.5, 0.4]


def test_empty_funnel_has_zero_conversion() -> None:
    steps = funnel_from_levels(["a", "b"], {})
    assert [(s.users, s.conversion_from_start) for s in steps] == [(0, 0.0), (0, 0.0)]


def test_retention_rates_are_relative_to_cohort() -> None:
    points = retention_from_counts(date(2026, 1, 1), [200, 100, 50])
    assert [(p.day, p.date, p.rate) for p in points] == [
        (0, date(2026, 1, 1), 1.0),
        (1, date(2026, 1, 2), 0.5),
        (2, date(2026, 1, 3), 0.25),
    ]


def test_time_range_validation() -> None:
    with pytest.raises(InvalidQueryError):
        TimeRange(T0, T0)
    with pytest.raises(InvalidQueryError, match="too wide"):
        TimeRange(T0, T0 + timedelta(days=2)).ensure_fits(Granularity.MINUTE)
    TimeRange(T0, T0 + timedelta(days=2)).ensure_fits(Granularity.HOUR)


async def test_service_validates_funnel_input() -> None:
    service = AnalyticsService(FakeAnalyticsRepository())
    period = TimeRange(T0, T0 + timedelta(days=1))
    with pytest.raises(InvalidQueryError, match="steps"):
        await service.funnel(uuid4(), ["only-one"], 60, period)
    with pytest.raises(InvalidQueryError, match="window"):
        await service.funnel(uuid4(), ["a", "b"], 0, period)


async def test_service_rejects_too_long_retention() -> None:
    service = AnalyticsService(FakeAnalyticsRepository())
    with pytest.raises(InvalidQueryError):
        await service.retention(uuid4(), "a", "b", date(2026, 1, 1), 31)


def test_funnel_query_uses_bound_parameters_for_step_names() -> None:
    sql = build_funnel_query(3, 3600)
    assert "windowFunnel(3600000)" in sql
    assert "name = {s0:String}, name = {s1:String}, name = {s2:String}" in sql


def test_retention_query_has_one_condition_per_day() -> None:
    sql = build_retention_query(3)
    assert sql.count("{return_event:String} AND toDate") == 3
    assert "{cohort:Date} + 3" in sql

from collections.abc import AsyncIterator, Iterator
from typing import Any

import pytest
from pydantic import SecretStr
from testcontainers.community.clickhouse import ClickHouseContainer
from testcontainers.community.postgres import PostgresContainer

from pulse.adapters.clickhouse.client import create_client
from pulse.adapters.clickhouse.migrate import migrate_clickhouse
from pulse.config import ClickHouseSettings

pytestmark = pytest.mark.integration


@pytest.fixture(scope="session")
def clickhouse_settings() -> Iterator[ClickHouseSettings]:
    with ClickHouseContainer("clickhouse/clickhouse-server:25.8-alpine") as ch:
        yield ClickHouseSettings(
            host=ch.get_container_host_ip(),
            port=int(ch.get_exposed_port(8123)),
            user=ch.username,
            password=SecretStr(ch.password),
            database="pulse_test",
        )


@pytest.fixture
async def clickhouse(clickhouse_settings: ClickHouseSettings) -> AsyncIterator[Any]:
    await migrate_clickhouse(clickhouse_settings)
    client = await create_client(clickhouse_settings)
    for table in ("events", "events_by_minute"):
        await client.command(f"TRUNCATE TABLE {table}")
    yield client
    await client.close()


@pytest.fixture(scope="session")
def postgres_dsn() -> Iterator[str]:
    with PostgresContainer("postgres:17-alpine", driver="asyncpg") as pg:
        yield pg.get_connection_url()

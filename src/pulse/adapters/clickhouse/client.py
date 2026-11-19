from typing import Any

import clickhouse_connect

from pulse.config import ClickHouseSettings


async def create_client(cfg: ClickHouseSettings, with_database: bool = True) -> Any:
    return await clickhouse_connect.get_async_client(
        host=cfg.host,
        port=cfg.port,
        username=cfg.user,
        password=cfg.password.get_secret_value(),
        database=cfg.database if with_database else "default",
        compress="lz4",
    )

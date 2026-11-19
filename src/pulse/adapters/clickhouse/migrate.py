"""Tiny forward-only migration runner: applied versions are tracked in `schema_migrations`."""

from importlib import resources

import structlog

from pulse.adapters.clickhouse.client import create_client
from pulse.config import ClickHouseSettings

log = structlog.get_logger(__name__)


def _migrations() -> list[tuple[str, str]]:
    files = resources.files("pulse.adapters.clickhouse") / "migrations"
    return sorted(
        (f.name.removesuffix(".sql"), f.read_text())
        for f in files.iterdir()
        if f.name.endswith(".sql")
    )


def _statements(sql: str) -> list[str]:
    lines = [line for line in sql.splitlines() if not line.strip().startswith("--")]
    return [s.strip() for s in "\n".join(lines).split(";") if s.strip()]


async def migrate_clickhouse(cfg: ClickHouseSettings) -> list[str]:
    bootstrap = await create_client(cfg, with_database=False)
    await bootstrap.command(f"CREATE DATABASE IF NOT EXISTS {cfg.database}")
    await bootstrap.close()

    client = await create_client(cfg)
    try:
        await client.command(
            "CREATE TABLE IF NOT EXISTS schema_migrations "
            "(version String, applied_at DateTime DEFAULT now()) "
            "ENGINE = MergeTree ORDER BY version"
        )
        applied = {
            row[0]
            for row in (await client.query("SELECT version FROM schema_migrations")).result_rows
        }
        newly_applied = []
        for version, sql in _migrations():
            if version in applied:
                continue
            for statement in _statements(sql):
                await client.command(statement)
            await client.insert("schema_migrations", [[version]], column_names=["version"])
            newly_applied.append(version)
        log.info("clickhouse_migrated", applied=newly_applied)
        return newly_applied
    finally:
        await client.close()

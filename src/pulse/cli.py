import argparse
import asyncio

import uvicorn

from pulse.config import get_settings
from pulse.logging import configure_logging

APPS = {
    "ingest-api": "pulse.entrypoints.ingest_api:create_app",
    "analytics-api": "pulse.entrypoints.analytics_api:create_app",
    "admin-api": "pulse.entrypoints.admin_api:create_app",
}


def _migrate(target: str) -> None:
    from pulse.adapters.clickhouse.migrate import migrate_clickhouse
    from pulse.adapters.kafka.topics import ensure_topics
    from pulse.adapters.postgres.migrate import migrate_postgres

    settings = get_settings()
    if target in ("kafka", "all"):
        asyncio.run(ensure_topics(settings.kafka))
    if target in ("clickhouse", "all"):
        asyncio.run(migrate_clickhouse(settings.clickhouse))
    if target in ("postgres", "all"):
        migrate_postgres(settings.postgres.dsn.get_secret_value())


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="pulse", description="Pulse analytics platform")
    sub = parser.add_subparsers(dest="command", required=True)
    for name in APPS:
        sub.add_parser(name, help=f"run the {name} HTTP service")
    sub.add_parser("writer", help="run the Kafka -> ClickHouse writer")
    sub.add_parser("outbox-relay", help="run the Postgres outbox -> Kafka relay")
    migrate = sub.add_parser("migrate", help="create Kafka topics and apply DB migrations")
    migrate.add_argument("target", choices=["kafka", "clickhouse", "postgres", "all"])
    args = parser.parse_args(argv)

    settings = get_settings()
    configure_logging(settings.log_level, settings.log_json, service=args.command)

    if args.command in APPS:
        uvicorn.run(
            APPS[args.command],
            factory=True,
            host=settings.http_host,
            port=settings.http_port,
            log_config=None,
            access_log=False,
            timeout_graceful_shutdown=20,
        )
    elif args.command == "writer":
        from pulse.entrypoints.workers import run_writer

        asyncio.run(run_writer(settings))
    elif args.command == "outbox-relay":
        from pulse.entrypoints.workers import run_outbox_relay

        asyncio.run(run_outbox_relay(settings))
    elif args.command == "migrate":
        _migrate(args.target)


if __name__ == "__main__":
    main()

from pathlib import Path

from alembic import command
from alembic.config import Config


def migrate_postgres(dsn: str) -> None:
    cfg = Config()
    cfg.set_main_option("script_location", str(Path(__file__).parent / "alembic"))
    cfg.set_main_option("sqlalchemy.url", dsn.replace("%", "%%"))
    command.upgrade(cfg, "head")

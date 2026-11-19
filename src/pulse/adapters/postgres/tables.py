import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB, UUID

metadata = sa.MetaData()

projects = sa.Table(
    "projects",
    metadata,
    sa.Column("id", UUID(as_uuid=True), primary_key=True),
    sa.Column("name", sa.Text, nullable=False),
    sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
)

api_keys = sa.Table(
    "api_keys",
    metadata,
    sa.Column("id", UUID(as_uuid=True), primary_key=True),
    sa.Column(
        "project_id",
        UUID(as_uuid=True),
        sa.ForeignKey("projects.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    ),
    sa.Column("key_hash", sa.CHAR(64), nullable=False, unique=True),
    sa.Column("display_prefix", sa.Text, nullable=False),
    sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    sa.Column("revoked_at", sa.DateTime(timezone=True), nullable=True),
)

outbox = sa.Table(
    "outbox",
    metadata,
    sa.Column("id", sa.BigInteger, sa.Identity(), primary_key=True),
    sa.Column("topic", sa.Text, nullable=False),
    sa.Column("key", sa.Text, nullable=False),
    # NULL payload is a Kafka tombstone (deletes the key from the compacted topic).
    sa.Column("payload", JSONB(none_as_null=True), nullable=True),
    sa.Column(
        "created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
    ),
    sa.Column("published_at", sa.DateTime(timezone=True), nullable=True),
    sa.Index("ix_outbox_unpublished", "id", postgresql_where=sa.text("published_at IS NULL")),
)

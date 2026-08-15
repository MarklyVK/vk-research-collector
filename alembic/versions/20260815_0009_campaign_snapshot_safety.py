"""Materialized campaign snapshot and single-active-campaign safety.

Revision ID: 20260815_0009
"""

from collections.abc import Sequence

import sqlalchemy as sa
from sqlalchemy import inspect

from alembic import op

revision: str = "20260815_0009"
down_revision: str | None = "20260815_0008"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    inspector = inspect(op.get_bind())
    campaign_columns = {column["name"] for column in inspector.get_columns("collection_campaigns")}
    if "snapshot_user_count" not in campaign_columns:
        op.add_column(
            "collection_campaigns",
            sa.Column("snapshot_user_count", sa.BigInteger(), nullable=False, server_default="0"),
        )
    if not inspector.has_table("collection_campaign_users"):
        op.create_table(
            "collection_campaign_users",
            sa.Column("campaign_id", sa.UUID(), nullable=False),
            sa.Column("user_id", sa.BigInteger(), nullable=False),
            sa.Column(
                "created_at",
                sa.DateTime(timezone=True),
                nullable=False,
                server_default=sa.func.now(),
            ),
            sa.ForeignKeyConstraint(
                ["campaign_id"], ["collection_campaigns.id"], ondelete="CASCADE"
            ),
            sa.ForeignKeyConstraint(["user_id"], ["vk_users.vk_id"], ondelete="CASCADE"),
            sa.PrimaryKeyConstraint("campaign_id", "user_id"),
        )
        op.create_index(
            "ix_collection_campaign_users_plan",
            "collection_campaign_users",
            ["campaign_id", "user_id"],
        )
    op.execute("DROP INDEX IF EXISTS uq_collection_campaigns_active_configuration")
    op.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS uq_collection_campaigns_active_type "
        "ON collection_campaigns (campaign_type) "
        "WHERE status IN ('planned','running','paused','waiting_method_limit',"
        "'paused_capacity_limit')"
    )


def downgrade() -> None:
    raise RuntimeError("Downgrade campaign snapshot migration запрещён; восстановите backup")

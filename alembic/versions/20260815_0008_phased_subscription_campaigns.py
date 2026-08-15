"""Durable phased subscription campaigns and metadata enrichment state.

Revision ID: 20260815_0008
"""

from collections.abc import Sequence

import sqlalchemy as sa
from sqlalchemy import inspect

from alembic import op
from vk_collector.database import models

revision: str = "20260815_0008"
down_revision: str | None = "20260810_0007"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def _foreign_key_exists(table: str, column: str) -> bool:
    return any(
        item.get("constrained_columns") == [column]
        for item in inspect(op.get_bind()).get_foreign_keys(table)
    )


def upgrade() -> None:
    bind = op.get_bind()
    models.CollectionCampaign.__table__.create(bind, checkfirst=True)
    inspector = inspect(bind)

    run_columns = {column["name"] for column in inspector.get_columns("collection_runs")}
    if "campaign_id" not in run_columns:
        op.add_column(
            "collection_runs",
            sa.Column("campaign_id", sa.UUID(), nullable=True),
        )
    if not _foreign_key_exists("collection_runs", "campaign_id"):
        op.create_foreign_key(
            "fk_collection_runs_campaign",
            "collection_runs",
            "collection_campaigns",
            ["campaign_id"],
            ["id"],
            ondelete="SET NULL",
        )
    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_collection_runs_campaign_status "
        "ON collection_runs (campaign_id, status, created_at)"
    )

    state_columns = {column["name"] for column in inspector.get_columns("user_subscription_states")}
    if "is_truncated" not in state_columns:
        op.add_column(
            "user_subscription_states",
            sa.Column("is_truncated", sa.Boolean(), nullable=False, server_default="false"),
        )
    if "terminal_reason" not in state_columns:
        op.add_column("user_subscription_states", sa.Column("terminal_reason", sa.String(100)))
    if "last_campaign_id" not in state_columns:
        op.add_column("user_subscription_states", sa.Column("last_campaign_id", sa.UUID()))
    if not _foreign_key_exists("user_subscription_states", "last_campaign_id"):
        op.create_foreign_key(
            "fk_user_subscription_states_campaign",
            "user_subscription_states",
            "collection_campaigns",
            ["last_campaign_id"],
            ["id"],
            ondelete="SET NULL",
        )
    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_user_subscription_states_campaign "
        "ON user_subscription_states (last_campaign_id, last_success_at)"
    )

    method_columns = {column["name"] for column in inspector.get_columns("vk_token_method_states")}
    if "successful_requests" not in method_columns:
        op.add_column(
            "vk_token_method_states",
            sa.Column("successful_requests", sa.Integer(), nullable=False, server_default="0"),
        )
    if "cooldown_seconds" not in method_columns:
        op.add_column(
            "vk_token_method_states",
            sa.Column("cooldown_seconds", sa.BigInteger(), nullable=False, server_default="0"),
        )


def downgrade() -> None:
    raise RuntimeError("Downgrade phased campaign migration запрещён; восстановите backup")

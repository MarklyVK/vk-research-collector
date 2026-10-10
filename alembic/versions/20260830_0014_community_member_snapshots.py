"""Create community_member_snapshots for follower-count history (VK has no growth endpoint).

Revision ID: 20260830_0014
Revises: 20260820_0013
"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "20260830_0014"
down_revision: str | None = "20260820_0013"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    existing_tables = set(inspector.get_table_names())
    if "community_member_snapshots" not in existing_tables:
        op.create_table(
            "community_member_snapshots",
            sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
            sa.Column(
                "community_vk_id",
                sa.BigInteger(),
                sa.ForeignKey("vk_communities.vk_id", ondelete="CASCADE"),
                nullable=False,
            ),
            sa.Column("members_count", sa.Integer(), nullable=True),
            sa.Column("captured_at", sa.DateTime(timezone=True), nullable=False),
            sa.Column(
                "snapshot_date",
                sa.Date(),
                sa.Computed("(captured_at AT TIME ZONE 'UTC')::date", persisted=True),
                nullable=False,
            ),
        )
        op.create_unique_constraint(
            "uq_community_member_snapshots_community_date",
            "community_member_snapshots",
            ["community_vk_id", "snapshot_date"],
        )
        op.create_index(
            "ix_community_member_snapshots_community_captured",
            "community_member_snapshots",
            ["community_vk_id", "captured_at"],
        )


def downgrade() -> None:
    raise RuntimeError(
        "Downgrade community_member_snapshots migration запрещён; восстановите backup"
    )

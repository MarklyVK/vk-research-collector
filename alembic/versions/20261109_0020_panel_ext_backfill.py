"""VK panel: 12-month extended backfill marker for video/clip posts.

Revision ID: 20261109_0020
Revises: 20261108_0019
"""

from __future__ import annotations

import sqlalchemy as sa

from alembic import op

revision: str = "20261109_0020"
down_revision: str | None = "20261108_0019"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "panel_communities",
        sa.Column("ext_backfill_done_at", sa.DateTime(timezone=True), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("panel_communities", "ext_backfill_done_at")

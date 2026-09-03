"""Create vk_method_global_states for cross-token method pacing (VK flood control appears app/IP-scoped, not per-token).

Revision ID: 20260903_0015
Revises: 20260830_0014
"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "20260903_0015"
down_revision: str | None = "20260830_0014"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    existing_tables = set(inspector.get_table_names())
    if "vk_method_global_states" not in existing_tables:
        op.create_table(
            "vk_method_global_states",
            sa.Column("method", sa.String(length=100), primary_key=True),
            sa.Column("next_request_at", sa.DateTime(timezone=True), nullable=True),
            sa.Column(
                "updated_at",
                sa.DateTime(timezone=True),
                nullable=False,
                server_default=sa.func.now(),
            ),
        )


def downgrade() -> None:
    raise RuntimeError("Downgrade vk_method_global_states migration запрещён; восстановите backup")

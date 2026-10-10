"""Add city/country jsonb columns to vk_communities.

Mini App region filter needs real city data, previously not collected.

Revision ID: 20260904_0016
Revises: 20260903_0015
"""

from collections.abc import Sequence

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision: str = "20260904_0016"
down_revision: str | None = "20260903_0015"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    existing_columns = {col["name"] for col in inspector.get_columns("vk_communities")}
    if "city" not in existing_columns:
        op.add_column("vk_communities", sa.Column("city", postgresql.JSONB(), nullable=True))
    if "country" not in existing_columns:
        op.add_column("vk_communities", sa.Column("country", postgresql.JSONB(), nullable=True))


def downgrade() -> None:
    raise RuntimeError(
        "Downgrade vk_communities city/country migration запрещён; восстановите backup"
    )

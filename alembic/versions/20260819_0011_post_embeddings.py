"""Create post_embeddings table for multimodal recommendation system.

Revision ID: 20260819_0011
Revises: 20260815_0010
"""

from collections.abc import Sequence

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB

from alembic import op

revision: str = "20260819_0011"
down_revision: str | None = "20260815_0010"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "post_embeddings",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column(
            "post_id",
            sa.BigInteger(),
            sa.ForeignKey("group_posts.id", ondelete="CASCADE"),
            nullable=False,
            unique=True,
        ),
        sa.Column("run_id", sa.String(length=64), nullable=False),
        sa.Column("model_name", sa.String(length=255), nullable=False),
        sa.Column("embedding_dim", sa.Integer(), nullable=False, server_default="2048"),
        sa.Column("embedding_vector", JSONB(), nullable=False),
        sa.Column("modality_profile", sa.String(length=50), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
    )
    op.create_index("ix_post_embeddings_run_id", "post_embeddings", ["run_id"])
    op.create_index("ix_post_embeddings_model_name", "post_embeddings", ["model_name"])


def downgrade() -> None:
    raise RuntimeError("Downgrade post_embeddings migration запрещён; восстановите backup")

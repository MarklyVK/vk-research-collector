"""Create user_posts, user_post_attachments,
user_post_collection_states tables and vk_users demographics.

Revision ID: 20260819_0012
Revises: 20260819_0011
"""

from collections.abc import Sequence

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB

from alembic import op

revision: str = "20260819_0012"
down_revision: str | None = "20260819_0011"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # 1. Добавление демографических полей в vk_users
    op.add_column("vk_users", sa.Column("sex", sa.Integer(), nullable=True))
    op.add_column("vk_users", sa.Column("bdate", sa.String(length=50), nullable=True))
    op.add_column("vk_users", sa.Column("city", sa.String(length=255), nullable=True))
    op.add_column("vk_users", sa.Column("education", sa.String(length=512), nullable=True))
    op.add_column("vk_users", sa.Column("relation", sa.Integer(), nullable=True))
    op.add_column("vk_users", sa.Column("followers_count", sa.Integer(), nullable=True))
    op.add_column("vk_users", sa.Column("friends_count", sa.Integer(), nullable=True))
    op.add_column("vk_users", sa.Column("gifts_count", sa.Integer(), nullable=True))
    op.add_column(
        "vk_users",
        sa.Column(
            "demographics",
            JSONB(),
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
    )

    # 2. Создание таблицы user_posts
    op.create_table(
        "user_posts",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("vk_owner_id", sa.BigInteger(), nullable=False),
        sa.Column("vk_post_id", sa.BigInteger(), nullable=False),
        sa.Column(
            "user_id",
            sa.BigInteger(),
            sa.ForeignKey("vk_users.vk_id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("published_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("edited_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("text", sa.Text(), nullable=False, server_default=""),
        sa.Column("post_type", sa.String(length=50), nullable=False, server_default="post"),
        sa.Column("is_pinned", sa.Boolean(), nullable=False, server_default="false"),
        sa.Column("comments_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("likes_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("reposts_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("views_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("signer_vk_user_id", sa.BigInteger(), nullable=True),
        sa.Column("source_updated_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("first_seen_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_seen_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("content_hash", sa.String(length=64), nullable=False),
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
        sa.UniqueConstraint("vk_owner_id", "vk_post_id", name="uq_user_posts_owner_post"),
    )
    op.create_index("ix_user_posts_user_published", "user_posts", ["user_id", "published_at"])

    # 3. Создание таблицы user_post_attachments
    op.create_table(
        "user_post_attachments",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column(
            "post_id",
            sa.BigInteger(),
            sa.ForeignKey("user_posts.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("position", sa.Integer(), nullable=False),
        sa.Column("attachment_type", sa.String(length=50), nullable=False),
        sa.Column("vk_owner_id", sa.BigInteger(), nullable=True),
        sa.Column("vk_attachment_id", sa.BigInteger(), nullable=True),
        sa.Column("access_key", sa.String(length=255), nullable=True),
        sa.Column("duration", sa.Integer(), nullable=True),
        sa.Column("width", sa.Integer(), nullable=True),
        sa.Column("height", sa.Integer(), nullable=True),
        sa.Column("title", sa.String(length=1000), nullable=True),
        sa.Column("external_url", sa.Text(), nullable=True),
        sa.Column(
            "metadata",
            JSONB(),
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
        sa.UniqueConstraint("post_id", "position", name="uq_user_post_attachments_post_position"),
    )
    op.create_index("ix_user_post_attachments_type", "user_post_attachments", ["attachment_type"])

    # 4. Создание таблицы user_post_collection_states
    op.create_table(
        "user_post_collection_states",
        sa.Column(
            "user_id",
            sa.BigInteger(),
            sa.ForeignKey("vk_users.vk_id", ondelete="CASCADE"),
            primary_key=True,
        ),
        sa.Column("last_attempt_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_success_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("next_scheduled_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "last_run_id",
            sa.dialects.postgresql.UUID(as_uuid=True),
            sa.ForeignKey("collection_runs.id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column("last_error_code", sa.Integer(), nullable=True),
        sa.Column("last_error_reason", sa.String(length=255), nullable=True),
        sa.Column("collected_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("wall_private", sa.Boolean(), nullable=False, server_default="false"),
        sa.Column("unavailable", sa.Boolean(), nullable=False, server_default="false"),
    )
    op.create_index(
        "ix_user_post_collection_states_next",
        "user_post_collection_states",
        ["next_scheduled_at"],
    )


def downgrade() -> None:
    raise RuntimeError("Downgrade user_posts migration запрещён; восстановите backup")

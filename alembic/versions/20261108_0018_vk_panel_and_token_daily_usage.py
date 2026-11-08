"""VK panel tables (virality study) and per-token daily usage counter.

Revision ID: 20261108_0018
Revises: 20261011_0017
"""

from collections.abc import Sequence

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision: str = "20261108_0018"
down_revision: str | None = "20261011_0017"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "vk_token_daily_usage",
        sa.Column("day", sa.Date(), nullable=False),
        sa.Column("token_fingerprint", sa.String(length=64), nullable=False),
        sa.Column("method", sa.String(length=100), nullable=False),
        sa.Column("calls", sa.Integer(), server_default="0", nullable=False),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint(
            "day", "token_fingerprint", "method", name=op.f("pk_vk_token_daily_usage")
        ),
    )
    op.create_table(
        "panels",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("name", sa.String(length=100), nullable=False),
        sa.Column("status", sa.String(length=20), server_default="draft", nullable=False),
        sa.Column("config", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("frozen_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_panels")),
        sa.UniqueConstraint("name", name=op.f("uq_panels_name")),
    )
    op.create_table(
        "panel_communities",
        sa.Column("panel_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("community_vk_id", sa.BigInteger(), autoincrement=False, nullable=False),
        sa.Column("name", sa.String(length=512), server_default="", nullable=False),
        sa.Column("screen_name", sa.String(length=255), nullable=True),
        sa.Column("stratum", sa.String(length=40), nullable=False),
        sa.Column("size_bucket", sa.String(length=2), nullable=False),
        sa.Column("members_at_selection", sa.Integer(), nullable=False),
        sa.Column("selection_seed", sa.BigInteger(), nullable=False),
        sa.Column("poll_interval_seconds", sa.Integer(), nullable=False),
        sa.Column("next_poll_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_poll_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_seen_post_id", sa.BigInteger(), nullable=True),
        sa.Column("backfill_done_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("excluded_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("excluded_reason", sa.String(length=100), nullable=True),
        sa.ForeignKeyConstraint(
            ["panel_id"],
            ["panels.id"],
            name=op.f("fk_panel_communities_panel_id_panels"),
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("panel_id", "community_vk_id", name=op.f("pk_panel_communities")),
    )
    op.create_index(
        "ix_panel_communities_due",
        "panel_communities",
        ["panel_id", "next_poll_at"],
        unique=False,
        postgresql_where=sa.text("excluded_at IS NULL"),
    )
    op.create_table(
        "panel_posts",
        sa.Column("panel_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("vk_owner_id", sa.BigInteger(), autoincrement=False, nullable=False),
        sa.Column("vk_post_id", sa.BigInteger(), autoincrement=False, nullable=False),
        sa.Column("community_vk_id", sa.BigInteger(), nullable=False),
        sa.Column("published_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("first_seen_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("origin", sa.String(length=10), nullable=False),
        sa.Column("post_type", sa.String(length=20), nullable=True),
        sa.Column("is_pinned", sa.Boolean(), server_default="false", nullable=False),
        sa.Column("marked_as_ads", sa.Boolean(), server_default="false", nullable=False),
        sa.Column("is_repost", sa.Boolean(), server_default="false", nullable=False),
        sa.Column("repost_source_owner_id", sa.BigInteger(), nullable=True),
        sa.Column("repost_source_post_id", sa.BigInteger(), nullable=True),
        sa.Column("text", sa.Text(), server_default="", nullable=False),
        sa.Column("text_norm_hash", sa.String(length=64), nullable=True),
        sa.Column(
            "attachments",
            postgresql.JSONB(astext_type=sa.Text()),
            server_default=sa.text("'[]'::jsonb"),
            nullable=False,
        ),
        sa.Column("n_photo", sa.Integer(), server_default="0", nullable=False),
        sa.Column("n_video", sa.Integer(), server_default="0", nullable=False),
        sa.Column("next_target_index", sa.Integer(), nullable=True),
        sa.Column("next_due_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("missed_targets", sa.Integer(), server_default="0", nullable=False),
        sa.Column("deleted_at", sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(
            ["panel_id"],
            ["panels.id"],
            name=op.f("fk_panel_posts_panel_id_panels"),
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint(
            "panel_id", "vk_owner_id", "vk_post_id", name=op.f("pk_panel_posts")
        ),
    )
    op.create_index(
        "ix_panel_posts_community_published",
        "panel_posts",
        ["panel_id", "community_vk_id", "published_at"],
        unique=False,
    )
    op.create_index(
        "ix_panel_posts_due",
        "panel_posts",
        ["panel_id", "next_due_at"],
        unique=False,
        postgresql_where=sa.text("next_due_at IS NOT NULL"),
    )
    op.create_table(
        "panel_post_snapshots",
        sa.Column("id", sa.BigInteger(), nullable=False),
        sa.Column("panel_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("vk_owner_id", sa.BigInteger(), nullable=False),
        sa.Column("vk_post_id", sa.BigInteger(), nullable=False),
        sa.Column("measured_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("age_seconds", sa.Integer(), nullable=False),
        sa.Column("target_age_h", sa.Integer(), nullable=True),
        sa.Column("views", sa.Integer(), nullable=True),
        sa.Column("likes", sa.Integer(), nullable=True),
        sa.Column("reposts", sa.Integer(), nullable=True),
        sa.Column("comments", sa.Integer(), nullable=True),
        sa.Column("source", sa.String(length=10), nullable=False),
        sa.ForeignKeyConstraint(
            ["panel_id"],
            ["panels.id"],
            name=op.f("fk_panel_post_snapshots_panel_id_panels"),
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_panel_post_snapshots")),
    )
    op.create_index(
        "ix_panel_snapshots_post_time",
        "panel_post_snapshots",
        ["panel_id", "vk_owner_id", "vk_post_id", "measured_at"],
        unique=False,
    )
    op.create_index(
        "uq_panel_snapshot_target",
        "panel_post_snapshots",
        ["panel_id", "vk_owner_id", "vk_post_id", "target_age_h"],
        unique=True,
        postgresql_where=sa.text("target_age_h IS NOT NULL"),
    )
    op.create_table(
        "panel_media",
        sa.Column("panel_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("vk_owner_id", sa.BigInteger(), autoincrement=False, nullable=False),
        sa.Column("vk_post_id", sa.BigInteger(), autoincrement=False, nullable=False),
        sa.Column("position", sa.Integer(), autoincrement=False, nullable=False),
        sa.Column("kind", sa.String(length=20), nullable=False),
        sa.Column("url", sa.Text(), nullable=True),
        sa.Column("priority", sa.Integer(), server_default="0", nullable=False),
        sa.Column("status", sa.String(length=10), server_default="pending", nullable=False),
        sa.Column("http_status", sa.Integer(), nullable=True),
        sa.Column("phash", sa.String(length=16), nullable=True),
        sa.Column("sha256", sa.String(length=64), nullable=True),
        sa.Column("width", sa.Integer(), nullable=True),
        sa.Column("height", sa.Integer(), nullable=True),
        sa.Column("downloaded_at", sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(
            ["panel_id"],
            ["panels.id"],
            name=op.f("fk_panel_media_panel_id_panels"),
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint(
            "panel_id", "vk_owner_id", "vk_post_id", "position", "kind", name=op.f("pk_panel_media")
        ),
    )
    op.create_index(
        "ix_panel_media_pending", "panel_media", ["panel_id", "status", "priority"], unique=False
    )
    op.create_table(
        "panel_community_snapshots",
        sa.Column("panel_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("community_vk_id", sa.BigInteger(), autoincrement=False, nullable=False),
        sa.Column("measured_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("members_count", sa.Integer(), nullable=True),
        sa.Column("is_closed", sa.Integer(), nullable=True),
        sa.Column("deactivated", sa.String(length=50), nullable=True),
        sa.ForeignKeyConstraint(
            ["panel_id"],
            ["panels.id"],
            name=op.f("fk_panel_community_snapshots_panel_id_panels"),
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint(
            "panel_id", "community_vk_id", "measured_at", name=op.f("pk_panel_community_snapshots")
        ),
    )


def downgrade() -> None:
    op.drop_table("panel_community_snapshots")
    op.drop_index("ix_panel_media_pending", table_name="panel_media")
    op.drop_table("panel_media")
    op.drop_index("uq_panel_snapshot_target", table_name="panel_post_snapshots")
    op.drop_index("ix_panel_snapshots_post_time", table_name="panel_post_snapshots")
    op.drop_table("panel_post_snapshots")
    op.drop_index("ix_panel_posts_due", table_name="panel_posts")
    op.drop_index("ix_panel_posts_community_published", table_name="panel_posts")
    op.drop_table("panel_posts")
    op.drop_index("ix_panel_communities_due", table_name="panel_communities")
    op.drop_table("panel_communities")
    op.drop_table("panels")
    op.drop_table("vk_token_daily_usage")

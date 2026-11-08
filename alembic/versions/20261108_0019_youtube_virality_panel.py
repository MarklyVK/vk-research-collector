"""YouTube virality panel tables (IEEE Access TOTE study).

Revision ID: 20261108_0019
Revises: 20261108_0018
"""

from collections.abc import Sequence

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision: str = "20261108_0019"
down_revision: str | None = "20261108_0018"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # 1. Quota usage tracking
    op.create_table(
        "yt_key_daily_usage",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("key_fingerprint", sa.String(length=64), nullable=False),
        sa.Column("endpoint", sa.String(length=100), nullable=False),
        sa.Column("day_pt", sa.Date(), nullable=False),
        sa.Column("calls_count", sa.Integer(), server_default="0", nullable=False),
        sa.Column("units_spent", sa.Integer(), server_default="0", nullable=False),
        sa.Column("quota_exceeded_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_yt_key_daily_usage")),
        sa.UniqueConstraint(
            "key_fingerprint",
            "endpoint",
            "day_pt",
            name=op.f("uq_yt_key_daily_usage_key_endpoint_day"),
        ),
    )
    op.create_index("ix_yt_key_daily_usage_day", "yt_key_daily_usage", ["day_pt"], unique=False)

    # 2. Search query logs
    op.create_table(
        "yt_search_query_logs",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("domain", sa.String(length=100), nullable=False),
        sa.Column("query_text", sa.String(length=500), nullable=False),
        sa.Column(
            "executed_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("results_count", sa.Integer(), server_default="0", nullable=False),
        sa.Column("units_cost", sa.Integer(), server_default="100", nullable=False),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_yt_search_query_logs")),
    )
    op.create_index(
        "ix_yt_search_query_logs_domain", "yt_search_query_logs", ["domain"], unique=False
    )

    # 3. Discovered & stratified channels
    op.create_table(
        "yt_channels",
        sa.Column("channel_id", sa.String(length=100), nullable=False),
        sa.Column("title", sa.String(length=500), server_default="", nullable=False),
        sa.Column("description", sa.Text(), server_default="", nullable=False),
        sa.Column("country", sa.String(length=10), nullable=True),
        sa.Column("default_language", sa.String(length=20), nullable=True),
        sa.Column("subscribers_count", sa.BigInteger(), nullable=True),
        sa.Column("size_tier", sa.String(length=10), nullable=False),
        sa.Column("domain", sa.String(length=100), nullable=False),
        sa.Column("uploads_playlist_id", sa.String(length=100), server_default="", nullable=False),
        sa.Column("is_selected", sa.Boolean(), server_default="false", nullable=False),
        sa.Column("selection_seed", sa.BigInteger(), nullable=True),
        sa.Column("discovered_via_query", sa.String(length=500), nullable=True),
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
        sa.PrimaryKeyConstraint("channel_id", name=op.f("pk_yt_channels")),
    )
    op.create_index(
        "ix_yt_channels_domain_size", "yt_channels", ["domain", "size_tier"], unique=False
    )
    op.create_index("ix_yt_channels_selected", "yt_channels", ["is_selected"], unique=False)

    # 4. Posts (videos)
    op.create_table(
        "yt_posts",
        sa.Column("post_id", sa.String(length=100), nullable=False),
        sa.Column("url", sa.String(length=500), nullable=False),
        sa.Column("channel_id", sa.String(length=100), nullable=False),
        sa.Column("published_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("crawled_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("age_hours", sa.Float(), server_default="0.0", nullable=False),
        sa.Column("views", sa.BigInteger(), nullable=True),
        sa.Column("likes", sa.BigInteger(), nullable=True),
        sa.Column("comments", sa.BigInteger(), nullable=True),
        sa.Column("shares", sa.BigInteger(), nullable=True),
        sa.Column("duration_s", sa.Integer(), server_default="0", nullable=False),
        sa.Column("is_short", sa.Boolean(), server_default="false", nullable=False),
        sa.Column("title", sa.String(length=1000), server_default="", nullable=False),
        sa.Column("description", sa.Text(), server_default="", nullable=False),
        sa.Column(
            "tags",
            postgresql.JSONB(astext_type=sa.Text()),
            server_default=sa.text("'[]'::jsonb"),
            nullable=False,
        ),
        sa.Column("category_id", sa.String(length=50), nullable=True),
        sa.Column("default_language", sa.String(length=50), nullable=True),
        sa.Column("thumbnail_url", sa.Text(), server_default="", nullable=False),
        sa.Column("is_backfill", sa.Boolean(), server_default="false", nullable=False),
        sa.Column("media_downloaded", sa.Boolean(), server_default="false", nullable=False),
        sa.Column(
            "comments_status", sa.String(length=20), server_default="pending", nullable=False
        ),
        sa.ForeignKeyConstraint(
            ["channel_id"],
            ["yt_channels.channel_id"],
            name=op.f("fk_yt_posts_channel_id_yt_channels"),
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("post_id", name=op.f("pk_yt_posts")),
    )
    op.create_index("ix_yt_posts_channel", "yt_posts", ["channel_id"], unique=False)
    op.create_index("ix_yt_posts_published", "yt_posts", ["published_at"], unique=False)
    op.create_index("ix_yt_posts_backfill", "yt_posts", ["is_backfill"], unique=False)
    op.create_index("ix_yt_posts_comments_status", "yt_posts", ["comments_status"], unique=False)
    op.create_index("ix_yt_posts_media_downloaded", "yt_posts", ["media_downloaded"], unique=False)

    # 5. Timeseries measurements
    op.create_table(
        "yt_timeseries",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("post_id", sa.String(length=100), nullable=False),
        sa.Column("crawled_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("target_age_h", sa.Integer(), nullable=True),
        sa.Column("age_seconds", sa.Integer(), nullable=False),
        sa.Column("views", sa.BigInteger(), nullable=True),
        sa.Column("likes", sa.BigInteger(), nullable=True),
        sa.Column("comments", sa.BigInteger(), nullable=True),
        sa.ForeignKeyConstraint(
            ["post_id"],
            ["yt_posts.post_id"],
            name=op.f("fk_yt_timeseries_post_id_yt_posts"),
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_yt_timeseries")),
    )
    op.create_index("ix_yt_timeseries_post", "yt_timeseries", ["post_id"], unique=False)
    op.create_index(
        "uq_yt_timeseries_post_target",
        "yt_timeseries",
        ["post_id", "target_age_h"],
        unique=True,
        postgresql_where=sa.text("target_age_h IS NOT NULL"),
    )

    # 6. Scheduled measurements queue
    op.create_table(
        "yt_scheduled_measurements",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("post_id", sa.String(length=100), nullable=False),
        sa.Column("target_age_h", sa.Integer(), nullable=False),
        sa.Column("scheduled_for", sa.DateTime(timezone=True), nullable=False),
        sa.Column("status", sa.String(length=20), server_default="pending", nullable=False),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(
            ["post_id"],
            ["yt_posts.post_id"],
            name=op.f("fk_yt_scheduled_measurements_post_id_yt_posts"),
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_yt_scheduled_measurements")),
        sa.UniqueConstraint("post_id", "target_age_h", name=op.f("uq_yt_sched_post_target")),
    )
    op.create_index(
        "ix_yt_scheduled_measurements_due",
        "yt_scheduled_measurements",
        ["status", "scheduled_for"],
        unique=False,
        postgresql_where=sa.text("status = 'pending'"),
    )
    op.create_index(
        "ix_yt_scheduled_measurements_post",
        "yt_scheduled_measurements",
        ["post_id"],
        unique=False,
    )

    # 7. Sampled top comments
    op.create_table(
        "yt_comments",
        sa.Column("comment_id", sa.String(length=100), nullable=False),
        sa.Column("post_id", sa.String(length=100), nullable=False),
        sa.Column("author_hash", sa.String(length=64), nullable=False),
        sa.Column("text", sa.Text(), nullable=False),
        sa.Column("like_count", sa.Integer(), server_default="0", nullable=False),
        sa.Column("reply_count", sa.Integer(), server_default="0", nullable=False),
        sa.Column("published_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("crawled_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(
            ["post_id"],
            ["yt_posts.post_id"],
            name=op.f("fk_yt_comments_post_id_yt_posts"),
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("comment_id", name=op.f("pk_yt_comments")),
    )
    op.create_index("ix_yt_comments_post", "yt_comments", ["post_id"], unique=False)


def downgrade() -> None:
    op.drop_index("ix_yt_comments_post", table_name="yt_comments")
    op.drop_table("yt_comments")
    op.drop_index("ix_yt_scheduled_measurements_post", table_name="yt_scheduled_measurements")
    op.drop_index("ix_yt_scheduled_measurements_due", table_name="yt_scheduled_measurements")
    op.drop_table("yt_scheduled_measurements")
    op.drop_index("uq_yt_timeseries_post_target", table_name="yt_timeseries")
    op.drop_index("ix_yt_timeseries_post", table_name="yt_timeseries")
    op.drop_table("yt_timeseries")
    op.drop_index("ix_yt_posts_media_downloaded", table_name="yt_posts")
    op.drop_index("ix_yt_posts_comments_status", table_name="yt_posts")
    op.drop_index("ix_yt_posts_backfill", table_name="yt_posts")
    op.drop_index("ix_yt_posts_published", table_name="yt_posts")
    op.drop_index("ix_yt_posts_channel", table_name="yt_posts")
    op.drop_table("yt_posts")
    op.drop_index("ix_yt_channels_selected", table_name="yt_channels")
    op.drop_index("ix_yt_channels_domain_size", table_name="yt_channels")
    op.drop_table("yt_channels")
    op.drop_index("ix_yt_search_query_logs_domain", table_name="yt_search_query_logs")
    op.drop_table("yt_search_query_logs")
    op.drop_index("ix_yt_key_daily_usage_day", table_name="yt_key_daily_usage")
    op.drop_table("yt_key_daily_usage")

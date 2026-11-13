"""YouTube: duration_s and is_short in yt_posts & yt_timeseries, plus yt_top table.

Revision ID: 20261111_0021
Revises: 20261109_0020
"""

from __future__ import annotations

from alembic import op

revision: str = "20261111_0021"
down_revision: str | None = "20261109_0020"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # 1. Гарантировать наличие duration_s и is_short в yt_posts
    op.execute(
        "ALTER TABLE yt_posts ADD COLUMN IF NOT EXISTS duration_s INTEGER NOT NULL DEFAULT 0;"
    )
    op.execute(
        "ALTER TABLE yt_posts ADD COLUMN IF NOT EXISTS is_short BOOLEAN NOT NULL DEFAULT FALSE;"
    )

    # 2. Гарантировать наличие duration_s и is_short в yt_timeseries
    op.execute("ALTER TABLE yt_timeseries ADD COLUMN IF NOT EXISTS duration_s INTEGER;")
    op.execute("ALTER TABLE yt_timeseries ADD COLUMN IF NOT EXISTS is_short BOOLEAN;")

    # Бэкфилл существующих строк yt_timeseries из yt_posts
    op.execute(
        """
        UPDATE yt_timeseries t
        SET duration_s = p.duration_s,
            is_short = p.is_short
        FROM yt_posts p
        WHERE t.post_id = p.post_id
          AND t.duration_s IS NULL;
        """
    )

    # 3. Создание таблицы yt_top и её индексов
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS yt_top (
            id BIGSERIAL PRIMARY KEY,
            video_id VARCHAR(32) NOT NULL,
            url TEXT NOT NULL,
            title TEXT,
            description TEXT,
            channel_id VARCHAR(64),
            channel_title TEXT,
            channel_subscribers BIGINT,
            published_at TIMESTAMPTZ,
            crawled_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            views BIGINT,
            likes BIGINT,
            comments BIGINT,
            duration_s INTEGER,
            is_short BOOLEAN,
            source VARCHAR(128) NOT NULL,
            rank INTEGER,
            flag_news BOOLEAN DEFAULT FALSE,
            flag_infopovod BOOLEAN DEFAULT FALSE,
            UNIQUE (video_id, source)
        )
        """
    )
    op.execute("CREATE INDEX IF NOT EXISTS idx_yt_top_video_id ON yt_top(video_id)")
    op.execute("CREATE INDEX IF NOT EXISTS idx_yt_top_source ON yt_top(source)")


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS yt_top CASCADE;")
    op.execute("ALTER TABLE yt_timeseries DROP COLUMN IF EXISTS is_short;")
    op.execute("ALTER TABLE yt_timeseries DROP COLUMN IF EXISTS duration_s;")

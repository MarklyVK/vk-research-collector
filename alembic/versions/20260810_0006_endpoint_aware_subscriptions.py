"""Endpoint-aware tokens, canonical communities and subscriptions.

Revision ID: 20260810_0006
"""

from collections.abc import Sequence

import sqlalchemy as sa
from sqlalchemy import inspect

from alembic import op
from vk_collector.database import models

revision: str = "20260810_0006"
down_revision: str | None = "20260730_0005"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def _foreign_key_name(table: str, column: str) -> str | None:
    for foreign_key in inspect(op.get_bind()).get_foreign_keys(table):
        if foreign_key.get("constrained_columns") == [column]:
            name = foreign_key.get("name")
            return str(name) if name else None
    return None


def upgrade() -> None:
    bind = op.get_bind()
    inspector = inspect(bind)
    tables = set(inspector.get_table_names())

    op.execute("ALTER TYPE collection_run_status ADD VALUE IF NOT EXISTS 'waiting_method_limit'")
    run_columns = {column["name"] for column in inspector.get_columns("collection_runs")}
    if "next_wakeup_at" not in run_columns:
        op.add_column("collection_runs", sa.Column("next_wakeup_at", sa.DateTime(timezone=True)))

    for table in (
        models.VKTokenState.__table__,
        models.VKTokenMethodState.__table__,
        models.VKCommunity.__table__,
        models.UserSubscriptionState.__table__,
    ):
        table.create(bind, checkfirst=True)

    # Сначала создаём канонические строки для кандидатов и уже сохранённых подписок.
    op.execute(
        """
        INSERT INTO vk_communities
            (vk_id, name, description, status_text, screen_name,
             first_seen_at, last_seen_at, metadata_updated_at)
        SELECT vk_id, name, description, status_text, screen_name,
               first_seen_at, last_seen_at, last_seen_at
        FROM group_candidates
        ON CONFLICT (vk_id) DO UPDATE SET
            name = EXCLUDED.name,
            description = EXCLUDED.description,
            status_text = EXCLUDED.status_text,
            screen_name = COALESCE(EXCLUDED.screen_name, vk_communities.screen_name),
            last_seen_at = GREATEST(vk_communities.last_seen_at, EXCLUDED.last_seen_at)
        """
    )
    if "user_group_subscriptions" in tables:
        op.execute(
            """
            INSERT INTO vk_communities (vk_id, first_seen_at, last_seen_at)
            SELECT vk_group_id, MIN(first_seen_at), MAX(last_seen_at)
            FROM user_group_subscriptions GROUP BY vk_group_id
            ON CONFLICT (vk_id) DO NOTHING
            """
        )
        if _foreign_key_name("user_group_subscriptions", "vk_group_id") is None:
            op.create_foreign_key(
                "fk_user_group_subscriptions_community",
                "user_group_subscriptions",
                "vk_communities",
                ["vk_group_id"],
                ["vk_id"],
                ondelete="RESTRICT",
            )

    post_columns = {column["name"] for column in inspector.get_columns("group_posts")}
    if "community_vk_id" not in post_columns:
        op.add_column("group_posts", sa.Column("community_vk_id", sa.BigInteger()))
    op.execute(
        """
        DO $$
        DECLARE changed integer;
        BEGIN
          LOOP
            WITH batch AS (
              SELECT p.id, g.vk_id
              FROM group_posts p
              JOIN group_candidates g ON g.id = p.group_id
              WHERE p.community_vk_id IS NULL
              ORDER BY p.id LIMIT 10000
            )
            UPDATE group_posts p SET community_vk_id = batch.vk_id
            FROM batch WHERE p.id = batch.id;
            GET DIAGNOSTICS changed = ROW_COUNT;
            EXIT WHEN changed = 0;
          END LOOP;
        END $$
        """
    )
    missing = bind.scalar(sa.text("SELECT count(*) FROM group_posts WHERE community_vk_id IS NULL"))
    if missing:
        raise RuntimeError(f"Нельзя связать {missing} существующих постов с vk_communities")
    if _foreign_key_name("group_posts", "community_vk_id") is None:
        op.create_foreign_key(
            "fk_group_posts_community",
            "group_posts",
            "vk_communities",
            ["community_vk_id"],
            ["vk_id"],
            ondelete="RESTRICT",
        )
    op.alter_column("group_posts", "community_vk_id", nullable=False)
    old_fk = _foreign_key_name("group_posts", "group_id")
    if old_fk:
        op.drop_constraint(old_fk, "group_posts", type_="foreignkey")
    op.alter_column("group_posts", "group_id", nullable=True)
    op.create_foreign_key(
        "fk_group_posts_candidate",
        "group_posts",
        "group_candidates",
        ["group_id"],
        ["id"],
        ondelete="SET NULL",
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_group_posts_community_published "
        "ON group_posts (community_vk_id, published_at)"
    )


def downgrade() -> None:
    raise RuntimeError("Downgrade endpoint-aware migration запрещён; восстановите backup")

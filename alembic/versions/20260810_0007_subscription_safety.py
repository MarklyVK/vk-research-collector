"""Subscription gates, probe reservation support and canonical post TTL state.

Revision ID: 20260810_0007
"""

from collections.abc import Sequence

from alembic import op
from vk_collector.database import models

revision: str = "20260810_0007"
down_revision: str | None = "20260810_0006"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    bind = op.get_bind()
    models.CommunityPostCollectionState.__table__.create(bind, checkfirst=True)

    # Existing candidate post success is canonical evidence of a successful wall snapshot.
    op.execute(
        """
        INSERT INTO community_post_collection_states
            (community_vk_id, last_attempt_at, last_success_at, next_scheduled_at,
             collected_count, wall_private, unavailable)
        SELECT community_vk_id, MAX(last_seen_at), MAX(last_seen_at), MAX(last_seen_at),
               COUNT(*)::integer, false, false
        FROM group_posts
        GROUP BY community_vk_id
        ON CONFLICT (community_vk_id) DO NOTHING
        """
    )


def downgrade() -> None:
    raise RuntimeError("Downgrade safety migration запрещён; восстановите backup")

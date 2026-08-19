"""Allow the independent user-post campaign phase.

Revision ID: 20260820_0013
"""

from collections.abc import Sequence

from alembic import op

revision: str = "20260820_0013"
down_revision: str | None = "20260819_0012"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute(
        "ALTER TABLE collection_campaigns "
        "DROP CONSTRAINT IF EXISTS collection_campaign_phase_allowed"
    )
    op.execute(
        "ALTER TABLE collection_campaigns DROP CONSTRAINT IF EXISTS "
        "ck_collection_campaigns_collection_campaign_phase_allowed"
    )
    op.execute(
        "ALTER TABLE collection_campaigns ADD CONSTRAINT "
        "collection_campaign_phase_allowed CHECK (phase IN ("
        "'subscription_discovery','subscription_metadata','user_posts_collection',"
        "'waiting_method_limit','paused_capacity_limit','completed','failed','cancelled'))"
    )


def downgrade() -> None:
    raise RuntimeError("Downgrade user-post campaign migration запрещён; восстановите backup")

"""Drop the campaign-user index duplicated by the primary key.

Revision ID: 20260815_0010
"""

from collections.abc import Sequence

from alembic import op

revision: str = "20260815_0010"
down_revision: str | None = "20260815_0009"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute("DROP INDEX IF EXISTS ix_collection_campaign_users_plan")


def downgrade() -> None:
    raise RuntimeError("Downgrade campaign index migration запрещён; восстановите backup")

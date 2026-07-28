"""Убрать sequence у естественного VK user ID.

Revision ID: 20260728_0003
"""

from collections.abc import Sequence

from alembic import op

revision: str = "20260728_0003"
down_revision: str | None = "20260728_0002"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute("ALTER TABLE vk_users ALTER COLUMN vk_id DROP DEFAULT")
    op.execute("DROP SEQUENCE IF EXISTS vk_users_vk_id_seq")


def downgrade() -> None:
    # VK ID является внешним естественным ключом, поэтому автогенерацию намеренно
    # не восстанавливаем: это безопаснее, чем создать фиктивные VK ID.
    pass

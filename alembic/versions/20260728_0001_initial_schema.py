"""Начальная схема первого этапа.

Revision ID: 20260728_0001
"""

from collections.abc import Sequence

from alembic import op
from vk_collector.database import models  # noqa: F401
from vk_collector.database.base import Base

revision: str = "20260728_0001"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    Base.metadata.create_all(op.get_bind())

    # Состав экспортированного пакета неизменяем после создания.
    op.execute(
        """
        CREATE FUNCTION reject_batch_item_mutation() RETURNS trigger
        LANGUAGE plpgsql AS $$
        BEGIN
            RAISE EXCEPTION 'classification batch items are immutable';
        END;
        $$
        """
    )
    op.execute(
        """
        CREATE TRIGGER classification_batch_items_immutable
        BEFORE UPDATE OR DELETE ON classification_batch_items
        FOR EACH ROW EXECUTE FUNCTION reject_batch_item_mutation()
        """
    )

    # Пользователь создаётся инфраструктурой с секретным паролем; миграция лишь выдаёт права,
    # если роль уже существует. DDL приложения остаётся недоступным reader-пользователю.
    op.execute(
        """
        DO $$ BEGIN
          IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'vk_reader') THEN
            EXECUTE format('GRANT CONNECT ON DATABASE %I TO vk_reader', current_database());
            GRANT USAGE ON SCHEMA public TO vk_reader;
            GRANT SELECT ON ALL TABLES IN SCHEMA public TO vk_reader;
            ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO vk_reader;
          END IF;
        END $$
        """
    )


def downgrade() -> None:
    op.execute("DROP FUNCTION IF EXISTS reject_batch_item_mutation() CASCADE")
    Base.metadata.drop_all(op.get_bind())

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
    # Эта историческая миграция фиксирует только stage 1. CollectionJob намеренно
    # создаётся следующей миграцией: иначе динамический Base.metadata добавил бы stage 2
    # при разворачивании чистой БД до выполнения stage 2 revision.
    stage_one_tables = [
        table
        for table in Base.metadata.sorted_tables
        if table.name
        not in {
            "collection_jobs",
            "collection_runs",
            "group_collection_states",
            "group_posts",
            "post_attachments",
            "vk_users",
            "group_memberships",
            "user_group_subscriptions",
            "collection_job_errors",
            "vk_communities",
            "user_subscription_states",
            "vk_token_states",
            "vk_token_method_states",
        }
    ]
    Base.metadata.create_all(op.get_bind(), tables=stage_one_tables)

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
    stage_one_tables = [
        table
        for table in reversed(Base.metadata.sorted_tables)
        if table.name
        not in {
            "collection_jobs",
            "collection_runs",
            "group_collection_states",
            "group_posts",
            "post_attachments",
            "vk_users",
            "group_memberships",
            "user_group_subscriptions",
            "collection_job_errors",
            "vk_communities",
            "user_subscription_states",
            "vk_token_states",
            "vk_token_method_states",
        }
    ]
    Base.metadata.drop_all(op.get_bind(), tables=stage_one_tables)

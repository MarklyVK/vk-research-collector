"""Добавить food_service и аудит расширения предметных областей.

Revision ID: 20260730_0005
"""

from collections.abc import Sequence

import sqlalchemy as sa
from sqlalchemy import inspect

from alembic import op
from vk_collector.database import models

revision: str = "20260730_0005"
down_revision: str | None = "20260728_0004"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_ALLOWED_SQL = "('food_delivery', 'customer_acquisition', 'tender_support', 'food_service')"
_OLD_ALLOWED_SQL = "('food_delivery', 'customer_acquisition', 'tender_support')"


def _add_search_run_columns(bind: sa.Connection) -> None:
    columns = {column["name"] for column in inspect(bind).get_columns("search_runs")}
    additions: tuple[sa.Column[object], ...] = (
        sa.Column("configuration", models.JSONB(), server_default="{}", nullable=False),
        sa.Column("api_results_count", sa.Integer(), server_default="0", nullable=False),
        sa.Column("private_results_count", sa.Integer(), server_default="0", nullable=False),
        sa.Column("deleted_results_count", sa.Integer(), server_default="0", nullable=False),
        sa.Column("error_count", sa.Integer(), server_default="0", nullable=False),
    )
    for column in additions:
        if column.name not in columns:
            op.add_column("search_runs", column)


def upgrade() -> None:
    bind = op.get_bind()
    op.execute("ALTER TABLE group_labels DROP CONSTRAINT IF EXISTS label_allowed")
    op.execute(
        "ALTER TABLE group_labels DROP CONSTRAINT IF EXISTS ck_group_labels_label_allowed"
    )
    op.create_check_constraint("label_allowed", "group_labels", f"label IN {_ALLOWED_SQL}")
    op.execute("ALTER TABLE search_keywords DROP CONSTRAINT IF EXISTS subject_allowed")
    op.execute(
        "ALTER TABLE search_keywords "
        "DROP CONSTRAINT IF EXISTS ck_search_keywords_subject_allowed"
    )
    op.create_check_constraint(
        "subject_allowed", "search_keywords", f"subject IN {_ALLOWED_SQL}"
    )
    _add_search_run_columns(bind)
    models.SearchRunGroup.__table__.create(bind, checkfirst=True)
    models.ClassificationReview.__table__.create(bind, checkfirst=True)


def downgrade() -> None:
    bind = op.get_bind()
    has_food_service = bool(
        bind.scalar(
            sa.text(
                "SELECT EXISTS ("
                "SELECT 1 FROM group_labels WHERE label='food_service' "
                "UNION ALL SELECT 1 FROM search_keywords WHERE subject='food_service'"
                ")"
            )
        )
    )
    if has_food_service:
        raise RuntimeError(
            "Downgrade остановлен: food_service уже используется. "
            "Сначала сохраните и осознанно перенесите эти данные."
        )

    models.ClassificationReview.__table__.drop(bind, checkfirst=True)
    models.SearchRunGroup.__table__.drop(bind, checkfirst=True)
    columns = {column["name"] for column in inspect(bind).get_columns("search_runs")}
    for name in (
        "error_count",
        "deleted_results_count",
        "private_results_count",
        "api_results_count",
        "configuration",
    ):
        if name in columns:
            op.drop_column("search_runs", name)
    op.execute("ALTER TABLE search_keywords DROP CONSTRAINT IF EXISTS subject_allowed")
    op.execute(
        "ALTER TABLE search_keywords "
        "DROP CONSTRAINT IF EXISTS ck_search_keywords_subject_allowed"
    )
    op.create_check_constraint(
        "subject_allowed", "search_keywords", f"subject IN {_OLD_ALLOWED_SQL}"
    )
    op.execute("ALTER TABLE group_labels DROP CONSTRAINT IF EXISTS label_allowed")
    op.execute(
        "ALTER TABLE group_labels DROP CONSTRAINT IF EXISTS ck_group_labels_label_allowed"
    )
    op.create_check_constraint("label_allowed", "group_labels", f"label IN {_OLD_ALLOWED_SQL}")

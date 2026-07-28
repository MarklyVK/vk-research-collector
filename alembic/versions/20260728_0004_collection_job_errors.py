"""Привести имя истории ошибок к контракту collection_job_errors.

Revision ID: 20260728_0004
"""

from collections.abc import Sequence

from sqlalchemy import inspect

from alembic import op

revision: str = "20260728_0004"
down_revision: str | None = "20260728_0003"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    bind = op.get_bind()
    tables = set(inspect(bind).get_table_names())
    if "collection_errors" in tables and "collection_job_errors" not in tables:
        op.rename_table("collection_errors", "collection_job_errors")
        op.execute(
            "ALTER INDEX IF EXISTS ix_collection_errors_run_created "
            "RENAME TO ix_collection_job_errors_run_created"
        )
        op.execute(
            "ALTER INDEX IF EXISTS ix_collection_errors_category "
            "RENAME TO ix_collection_job_errors_category"
        )


def downgrade() -> None:
    bind = op.get_bind()
    tables = set(inspect(bind).get_table_names())
    if "collection_job_errors" in tables and "collection_errors" not in tables:
        op.rename_table("collection_job_errors", "collection_errors")
        op.execute(
            "ALTER INDEX IF EXISTS ix_collection_job_errors_run_created "
            "RENAME TO ix_collection_errors_run_created"
        )
        op.execute(
            "ALTER INDEX IF EXISTS ix_collection_job_errors_category "
            "RENAME TO ix_collection_errors_category"
        )

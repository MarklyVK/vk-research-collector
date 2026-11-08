"""Declare indexes that were created manually on production.

ix_collection_jobs_claim (claim query of the worker) and ix_group_posts_engagement
exist on production since September but were never part of the schema history, so
``alembic check`` in the deployment failed with "remove_index". IF NOT EXISTS makes
this a no-op on production and creates them on fresh databases.

Revision ID: 20261011_0017
Revises: 20260904_0016
"""

from collections.abc import Sequence

from alembic import op

revision: str = "20261011_0017"
down_revision: str | None = "20260904_0016"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_collection_jobs_claim ON collection_jobs "
        "(collection_run_id, job_type, priority, created_at) "
        "WHERE status = ANY (ARRAY['pending'::collection_job_status, "
        "'retry_wait'::collection_job_status])"
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_group_posts_engagement ON group_posts "
        "(((likes_count + reposts_count) + comments_count) DESC, group_id)"
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS ix_group_posts_engagement")
    op.execute("DROP INDEX IF EXISTS ix_collection_jobs_claim")

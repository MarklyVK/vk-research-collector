"""Схема возобновляемого сбора approved-данных.

Revision ID: 20260728_0002
"""

import uuid
from collections.abc import Sequence

import sqlalchemy as sa
from sqlalchemy import inspect

from alembic import op
from vk_collector.database import models

revision: str = "20260728_0002"
down_revision: str | None = "20260728_0001"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    bind = op.get_bind()
    inspector = inspect(bind)
    existing = set(inspector.get_table_names())

    # На уже развёрнутой stage 1 БД расширяем enum до добавления новых состояний.
    if "collection_jobs" in existing:
        for value in ("retry_wait", "skipped", "cancelled"):
            op.execute(f"ALTER TYPE collection_job_status ADD VALUE IF NOT EXISTS '{value}'")

    models.CollectionRun.__table__.create(bind, checkfirst=True)

    if "collection_jobs" not in existing:
        models.CollectionJob.__table__.create(bind, checkfirst=True)
    else:
        columns = {column["name"] for column in inspector.get_columns("collection_jobs")}
        additions: list[sa.Column[object]] = [
            sa.Column("collection_run_id", sa.UUID(), nullable=True),
            sa.Column("entity_type", sa.String(50), nullable=True),
            sa.Column("entity_id", sa.BigInteger(), nullable=True),
            sa.Column("priority", sa.Integer(), server_default="100", nullable=False),
            sa.Column("attempt_count", sa.Integer(), server_default="0", nullable=False),
            sa.Column("max_attempts", sa.Integer(), server_default="5", nullable=False),
            sa.Column("next_attempt_at", sa.DateTime(timezone=True)),
            sa.Column("locked_at", sa.DateTime(timezone=True)),
            sa.Column("locked_by", sa.String(255)),
            sa.Column("heartbeat_at", sa.DateTime(timezone=True)),
            sa.Column("checkpoint", models.JSONB(), server_default="{}", nullable=False),
            sa.Column("last_error_type", sa.String(100)),
            sa.Column("last_error_message", sa.Text()),
            sa.Column("api_requests", sa.Integer(), server_default="0", nullable=False),
            sa.Column("rows_inserted", sa.Integer(), server_default="0", nullable=False),
            sa.Column("rows_updated", sa.Integer(), server_default="0", nullable=False),
        ]
        for column in additions:
            if column.name not in columns:
                op.add_column("collection_jobs", column)

        legacy_run_id = uuid.uuid4()
        op.execute(
            sa.insert(models.CollectionRun.__table__).values(
                id=legacy_run_id,
                scope="legacy",
                status=models.CollectionRunStatus.CANCELLED.value,
                configuration={},
                error_message="Legacy stage 1 placeholder jobs",
            )
        )
        op.execute(
            sa.text(
                "UPDATE collection_jobs SET collection_run_id=:run_id, "
                "entity_type='legacy', entity_id=0 WHERE collection_run_id IS NULL"
            ).bindparams(run_id=legacy_run_id)
        )
        op.alter_column("collection_jobs", "collection_run_id", nullable=False)
        op.alter_column("collection_jobs", "entity_type", nullable=False)
        op.alter_column("collection_jobs", "entity_id", nullable=False)
        op.create_foreign_key(
            "fk_collection_jobs_run",
            "collection_jobs",
            "collection_runs",
            ["collection_run_id"],
            ["id"],
            ondelete="CASCADE",
        )
        op.create_unique_constraint(
            "uq_collection_jobs_run_type_entity",
            "collection_jobs",
            ["collection_run_id", "job_type", "entity_type", "entity_id"],
        )
        op.create_check_constraint(
            "collection_jobs_attempt_nonnegative", "collection_jobs", "attempt_count >= 0"
        )
        op.create_check_constraint(
            "collection_jobs_max_attempts_positive", "collection_jobs", "max_attempts > 0"
        )
        op.drop_index("ix_collection_jobs_status_created_at", table_name="collection_jobs")
        op.create_index(
            "ix_collection_jobs_queue",
            "collection_jobs",
            ["status", "next_attempt_at", "priority", "created_at"],
        )
        op.create_index("ix_collection_jobs_lease", "collection_jobs", ["status", "locked_at"])
        op.create_index(
            "ix_collection_jobs_run_status",
            "collection_jobs",
            ["collection_run_id", "status"],
        )

    for table in (
        models.GroupCollectionState.__table__,
        models.GroupPost.__table__,
        models.PostAttachment.__table__,
        models.VKUser.__table__,
        models.GroupMembership.__table__,
        models.UserGroupSubscription.__table__,
        models.CollectionJobError.__table__,
    ):
        table.create(bind, checkfirst=True)


def downgrade() -> None:
    bind = op.get_bind()
    for table in (
        models.CollectionJobError.__table__,
        models.UserGroupSubscription.__table__,
        models.GroupMembership.__table__,
        models.VKUser.__table__,
        models.PostAttachment.__table__,
        models.GroupPost.__table__,
        models.GroupCollectionState.__table__,
        models.CollectionJob.__table__,
        models.CollectionRun.__table__,
    ):
        table.drop(bind, checkfirst=True)

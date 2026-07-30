from __future__ import annotations

import uuid
from collections import Counter
from typing import Any

from sqlalchemy import distinct, func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from vk_collector.database.models import (
    ClassificationStatus,
    CollectionJob,
    CollectionJobError,
    CollectionRun,
    CollectionRunStatus,
    GroupCandidate,
    GroupMembership,
    GroupPost,
    JobStatus,
    PostAttachment,
    UserGroupSubscription,
    VKUser,
)


async def latest_run_id(
    sessions: async_sessionmaker[AsyncSession],
) -> uuid.UUID | None:
    async with sessions() as session:
        run_id: uuid.UUID | None = await session.scalar(
            select(CollectionRun.id).order_by(CollectionRun.created_at.desc()).limit(1)
        )
        return run_id


async def latest_runnable_run_id(
    sessions: async_sessionmaker[AsyncSession],
) -> uuid.UUID | None:
    """Найти последний разрешённый full/incremental run для автономного worker."""
    async with sessions() as session:
        run_id: uuid.UUID | None = await session.scalar(
            select(CollectionRun.id)
            .where(
                CollectionRun.scope.in_(["full", "incremental"]),
                CollectionRun.status.in_(
                    [CollectionRunStatus.PLANNED, CollectionRunStatus.RUNNING]
                ),
                CollectionRun.configuration["capacity_gate"].astext == "passed",
            )
            .order_by(CollectionRun.created_at.desc())
            .limit(1)
        )
        return run_id


async def run_summary(
    sessions: async_sessionmaker[AsyncSession], run_id: uuid.UUID | None = None
) -> dict[str, Any]:
    async with sessions() as session:
        target = run_id or await session.scalar(
            select(CollectionRun.id).order_by(CollectionRun.created_at.desc()).limit(1)
        )
        if target is None:
            return {"run_id": None, "status": "absent", "jobs": {}}
        run = await session.get(CollectionRun, target)
        if run is None:
            raise ValueError("Запуск не найден")
        jobs = (
            await session.execute(
                select(CollectionJob.status, func.count(CollectionJob.id))
                .where(CollectionJob.collection_run_id == target)
                .group_by(CollectionJob.status)
            )
        ).all()
        metrics = (
            await session.execute(
                select(
                    func.coalesce(func.sum(CollectionJob.api_requests), 0),
                    func.coalesce(func.sum(CollectionJob.rows_inserted), 0),
                    func.coalesce(func.sum(CollectionJob.rows_updated), 0),
                    func.coalesce(func.sum(func.greatest(CollectionJob.attempt_count - 1, 0)), 0),
                ).where(CollectionJob.collection_run_id == target)
            )
        ).one()
        return {
            "run_id": str(target),
            "scope": run.scope,
            "status": run.status.value,
            "jobs": {status.value: count for status, count in jobs},
            "api_requests": int(metrics[0]),
            "rows_inserted": int(metrics[1]),
            "rows_updated": int(metrics[2]),
            "retries": int(metrics[3]),
            "error_message": run.error_message,
        }


async def global_summary(sessions: async_sessionmaker[AsyncSession]) -> dict[str, int]:
    async with sessions() as session:
        models = {
            "groups": GroupCandidate,
            "posts": GroupPost,
            "attachments": PostAttachment,
            "memberships": GroupMembership,
            "users": VKUser,
            "subscriptions": UserGroupSubscription,
            "runs": CollectionRun,
            "jobs": CollectionJob,
            "errors": CollectionJobError,
        }
        result: dict[str, int] = {}
        for label, model in models.items():
            result[label] = int(await session.scalar(select(func.count()).select_from(model)) or 0)
        return result


async def verify_run(
    sessions: async_sessionmaker[AsyncSession], run_id: uuid.UUID
) -> dict[str, int]:
    async with sessions() as session:
        post_duplicates = int(
            await session.scalar(
                select(func.count()).select_from(
                    select(GroupPost.vk_owner_id, GroupPost.vk_post_id)
                    .group_by(GroupPost.vk_owner_id, GroupPost.vk_post_id)
                    .having(func.count() > 1)
                    .subquery()
                )
            )
            or 0
        )
        membership_duplicates = int(
            await session.scalar(
                select(func.count()).select_from(
                    select(GroupMembership.group_id, GroupMembership.user_id)
                    .group_by(GroupMembership.group_id, GroupMembership.user_id)
                    .having(func.count() > 1)
                    .subquery()
                )
            )
            or 0
        )
        subscription_duplicates = int(
            await session.scalar(
                select(func.count()).select_from(
                    select(UserGroupSubscription.user_id, UserGroupSubscription.vk_group_id)
                    .group_by(
                        UserGroupSubscription.user_id,
                        UserGroupSubscription.vk_group_id,
                    )
                    .having(func.count() > 1)
                    .subquery()
                )
            )
            or 0
        )
        rejected_jobs = int(
            await session.scalar(
                select(func.count(distinct(CollectionJob.id)))
                .join(
                    GroupCandidate,
                    (CollectionJob.entity_type == "group")
                    & (CollectionJob.entity_id == GroupCandidate.id),
                )
                .where(
                    CollectionJob.collection_run_id == run_id,
                    GroupCandidate.classification_status == ClassificationStatus.REJECTED,
                )
            )
            or 0
        )
        running_jobs = int(
            await session.scalar(
                select(func.count(CollectionJob.id)).where(
                    CollectionJob.collection_run_id == run_id,
                    CollectionJob.status == JobStatus.RUNNING,
                )
            )
            or 0
        )
        return {
            "post_duplicates": post_duplicates,
            "membership_duplicates": membership_duplicates,
            "subscription_duplicates": subscription_duplicates,
            "rejected_jobs": rejected_jobs,
            "running_jobs": running_jobs,
        }


async def database_metrics(
    sessions: async_sessionmaker[AsyncSession],
) -> dict[str, int]:
    async with sessions() as session:
        size = int(
            await session.scalar(select(func.pg_database_size(func.current_database()))) or 0
        )
        counts = await global_summary_from_session(session)
        return {"database_bytes": size, **counts}


async def global_summary_from_session(session: AsyncSession) -> dict[str, int]:
    rows = {
        "posts": GroupPost,
        "attachments": PostAttachment,
        "memberships": GroupMembership,
        "users": VKUser,
        "subscriptions": UserGroupSubscription,
    }
    result: dict[str, int] = {}
    for label, model in rows.items():
        result[label] = int(await session.scalar(select(func.count()).select_from(model)) or 0)
    return result


async def error_categories(
    sessions: async_sessionmaker[AsyncSession], run_id: uuid.UUID
) -> dict[str, int]:
    async with sessions() as session:
        rows = (
            await session.execute(
                select(CollectionJobError.error_category, func.count(CollectionJobError.id))
                .where(CollectionJobError.collection_run_id == run_id)
                .group_by(CollectionJobError.error_category)
            )
        ).all()
    return dict(Counter({category: count for category, count in rows}))


async def capacity_gate_passed(
    sessions: async_sessionmaker[AsyncSession], run_id: uuid.UUID
) -> bool:
    async with sessions() as session:
        run = await session.get(CollectionRun, run_id)
        return bool(
            run
            and run.scope in {"full", "incremental"}
            and run.configuration.get("capacity_gate") == "passed"
            and run.status
            in {
                CollectionRunStatus.PLANNED,
                CollectionRunStatus.RUNNING,
                CollectionRunStatus.PAUSED,
            }
        )

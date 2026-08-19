from __future__ import annotations

import uuid
from collections import Counter
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import distinct, func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from vk_collector.database.models import (
    ClassificationStatus,
    CollectionJob,
    CollectionJobError,
    CollectionRun,
    CollectionRunStatus,
    CommunityPostCollectionState,
    GroupCandidate,
    GroupMembership,
    GroupPost,
    JobStatus,
    PostAttachment,
    UserGroupSubscription,
    UserPost,
    UserPostAttachment,
    UserSubscriptionState,
    VKCommunity,
    VKTokenMethodState,
    VKTokenState,
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
    """Найти последний разрешённый production run для автономного worker."""
    async with sessions() as session:
        run_id: uuid.UUID | None = await session.scalar(
            select(CollectionRun.id)
            .where(
                CollectionRun.scope.in_(
                    [
                        "full",
                        "incremental",
                        "subscriptions",
                        "subscription_posts",
                        "subscription_discovery",
                        "subscription_metadata",
                        "light_repair",
                    ]
                ),
                CollectionRun.status.in_(
                    [
                        CollectionRunStatus.PLANNED,
                        CollectionRunStatus.RUNNING,
                        CollectionRunStatus.WAITING_METHOD_LIMIT,
                    ]
                ),
                CollectionRun.configuration["capacity_gate"].astext == "passed",
            )
            .order_by(CollectionRun.created_at.desc())
            .limit(1)
        )
        return run_id


async def runnable_run_ids(
    sessions: async_sessionmaker[AsyncSession],
) -> list[uuid.UUID]:
    """Return every operator-authorized non-pilot run for fair autonomous scheduling."""
    async with sessions() as session:
        return list(
            (
                await session.scalars(
                    select(CollectionRun.id)
                    .where(
                        CollectionRun.scope.in_(
                            [
                                "full",
                                "incremental",
                                "subscriptions",
                                "subscription_posts",
                                "subscription_discovery",
                                "subscription_metadata",
                                "light_repair",
                            ]
                        ),
                        CollectionRun.status.in_(
                            [
                                CollectionRunStatus.PLANNED,
                                CollectionRunStatus.RUNNING,
                                CollectionRunStatus.WAITING_METHOD_LIMIT,
                            ]
                        ),
                        CollectionRun.configuration["capacity_gate"].astext == "passed",
                    )
                    .order_by(CollectionRun.created_at, CollectionRun.id)
                )
            ).all()
        )


async def next_runnable_wakeup(
    sessions: async_sessionmaker[AsyncSession],
) -> datetime | None:
    """Return the nearest persisted run/job wakeup for authorized work."""
    async with sessions() as session:
        runnable = select(CollectionRun.id).where(
            CollectionRun.scope.in_(
                [
                    "full",
                    "incremental",
                    "subscriptions",
                    "subscription_posts",
                    "subscription_discovery",
                    "subscription_metadata",
                    "light_repair",
                ]
            ),
            CollectionRun.status.in_(
                [
                    CollectionRunStatus.PLANNED,
                    CollectionRunStatus.RUNNING,
                    CollectionRunStatus.WAITING_METHOD_LIMIT,
                ]
            ),
            CollectionRun.configuration["capacity_gate"].astext == "passed",
        )
        now = datetime.now(UTC)
        run_wakeup = await session.scalar(
            select(func.min(CollectionRun.next_wakeup_at)).where(
                CollectionRun.id.in_(runnable), CollectionRun.next_wakeup_at > now
            )
        )
        job_wakeup = await session.scalar(
            select(func.min(CollectionJob.next_attempt_at)).where(
                CollectionJob.collection_run_id.in_(runnable),
                CollectionJob.status == JobStatus.RETRY_WAIT,
                CollectionJob.next_attempt_at > now,
            )
        )
        values = [value for value in (run_wakeup, job_wakeup) if value is not None]
        return min(values) if values else None


def bounded_wakeup_delay(
    wakeup: datetime | None,
    *,
    now: datetime,
    idle_seconds: float,
    stop_check_seconds: float = 60.0,
) -> float:
    """Choose the durable wakeup while bounding stop-signal latency."""
    if wakeup is None:
        return idle_seconds
    return min(stop_check_seconds, max(0.1, (wakeup - now).total_seconds()))


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
        job_types = (
            await session.execute(
                select(CollectionJob.job_type, CollectionJob.status, func.count(CollectionJob.id))
                .where(CollectionJob.collection_run_id == target)
                .group_by(CollectionJob.job_type, CollectionJob.status)
            )
        ).all()
        method_limits = int(
            await session.scalar(
                select(func.count(VKTokenMethodState.id)).where(
                    VKTokenMethodState.blocked_until > func.now()
                )
            )
            or 0
        )
        next_method_retry = await session.scalar(
            select(func.min(VKTokenMethodState.blocked_until)).where(
                VKTokenMethodState.blocked_until > func.now()
            )
        )
        return {
            "run_id": str(target),
            "scope": run.scope,
            "status": run.status.value,
            "jobs": {status.value: count for status, count in jobs},
            "jobs_by_type": {
                job_type: {
                    status.value: count
                    for current_type, status, count in job_types
                    if current_type == job_type
                }
                for job_type in sorted({row[0] for row in job_types})
            },
            "api_requests": int(metrics[0]),
            "rows_inserted": int(metrics[1]),
            "rows_updated": int(metrics[2]),
            "retries": int(metrics[3]),
            "error_message": run.error_message,
            "next_wakeup_at": run.next_wakeup_at.isoformat() if run.next_wakeup_at else None,
            "blocked_token_methods": method_limits,
            "next_method_retry_at": next_method_retry.isoformat() if next_method_retry else None,
            "capacity_gate": run.configuration.get("capacity_gate", "not_required"),
            "phase": run.configuration.get("phase"),
            "processed_users": int(
                await session.scalar(
                    select(func.count(UserSubscriptionState.user_id)).where(
                        UserSubscriptionState.last_run_id == target,
                        UserSubscriptionState.last_success_at.is_not(None),
                    )
                )
                or 0
            ),
            "private_users": int(
                await session.scalar(
                    select(func.count(UserSubscriptionState.user_id)).where(
                        UserSubscriptionState.last_run_id == target,
                        UserSubscriptionState.privacy_denied.is_(True),
                    )
                )
                or 0
            ),
            "skipped_users": sum(
                count
                for job_type, status, count in job_types
                if job_type == "collect_user_subscriptions" and status == JobStatus.SKIPPED
            ),
            "skipped_walls": sum(
                count
                for job_type, status, count in job_types
                if job_type == "collect_subscription_group_posts" and status == JobStatus.SKIPPED
            ),
            "subscription_links": int(
                await session.scalar(
                    select(func.count(UserGroupSubscription.id)).where(
                        UserGroupSubscription.source_run_id == target
                    )
                )
                or 0
            ),
            "unique_communities": int(
                await session.scalar(
                    select(func.count(distinct(UserGroupSubscription.vk_group_id))).where(
                        UserGroupSubscription.source_run_id == target
                    )
                )
                or 0
            ),
            "post_ttl_states": int(
                await session.scalar(
                    select(func.count(CommunityPostCollectionState.community_vk_id)).where(
                        CommunityPostCollectionState.last_run_id == target
                    )
                )
                or 0
            ),
            "post_jobs": sum(
                count
                for job_type, _status, count in job_types
                if job_type == "collect_subscription_group_posts"
            ),
            "posts": int(
                await session.scalar(
                    select(func.count(GroupPost.id))
                    .join(
                        CommunityPostCollectionState,
                        CommunityPostCollectionState.community_vk_id == GroupPost.community_vk_id,
                    )
                    .where(CommunityPostCollectionState.last_run_id == target)
                )
                or 0
            ),
            "attachments": int(
                await session.scalar(
                    select(func.count(PostAttachment.id))
                    .join(GroupPost, GroupPost.id == PostAttachment.post_id)
                    .join(
                        CommunityPostCollectionState,
                        CommunityPostCollectionState.community_vk_id == GroupPost.community_vk_id,
                    )
                    .where(CommunityPostCollectionState.last_run_id == target)
                )
                or 0
            ),
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
            "communities": VKCommunity,
            "subscription_states": UserSubscriptionState,
            "community_post_states": CommunityPostCollectionState,
            "token_states": VKTokenState,
            "token_method_states": VKTokenMethodState,
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
        relations = {}
        for table in (
            "collection_runs",
            "collection_jobs",
            "collection_job_errors",
            "vk_communities",
            "user_group_subscriptions",
            "user_subscription_states",
            "community_post_collection_states",
            "group_posts",
            "post_attachments",
            "user_posts",
            "user_post_attachments",
            "user_post_collection_states",
        ):
            relations[f"relation_{table}_bytes"] = int(
                await session.scalar(select(func.pg_total_relation_size(table))) or 0
            )
        subscription_communities = int(
            await session.scalar(
                select(func.count(distinct(UserGroupSubscription.vk_group_id))).where(
                    UserGroupSubscription.is_current.is_(True)
                )
            )
            or 0
        )
        return {
            "database_bytes": size,
            "subscription_communities": subscription_communities,
            **counts,
            **relations,
        }


async def global_summary_from_session(session: AsyncSession) -> dict[str, int]:
    rows = {
        "posts": GroupPost,
        "attachments": PostAttachment,
        "user_posts": UserPost,
        "user_attachments": UserPostAttachment,
        "memberships": GroupMembership,
        "users": VKUser,
        "subscriptions": UserGroupSubscription,
        "communities": VKCommunity,
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
            and run.scope in {"full", "incremental", "subscriptions", "subscription_posts"}
            and run.configuration.get("capacity_gate") == "passed"
            and run.status
            in {
                CollectionRunStatus.PLANNED,
                CollectionRunStatus.RUNNING,
                CollectionRunStatus.PAUSED,
                CollectionRunStatus.WAITING_METHOD_LIMIT,
            }
        )

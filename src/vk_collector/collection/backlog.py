from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from sqlalchemy import and_, distinct, exists, func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from vk_collector.collection.queue import CollectionQueue
from vk_collector.config import Settings
from vk_collector.database.models import (
    CampaignStatus,
    ClassificationStatus,
    CollectionCampaign,
    CollectionJob,
    CollectionRun,
    CollectionRunStatus,
    CommunityPostCollectionState,
    GroupCandidate,
    GroupCollectionState,
    GroupMembership,
    JobStatus,
    UserGroupSubscription,
    UserSubscriptionState,
    VKCommunity,
    VKUser,
)


async def canonical_backlog(
    sessions: async_sessionmaker[AsyncSession], settings: Settings
) -> dict[str, Any]:
    """Build a read-only backlog from canonical state, never from raw job totals alone."""
    now = datetime.now(UTC)
    profile_stale = now - timedelta(days=settings.collection_user_profile_ttl_days)
    metadata_stale = now - timedelta(days=settings.collection_community_metadata_ttl_days)
    lease_stale = now - timedelta(seconds=settings.collection_job_lease_seconds)
    async with sessions() as session:
        approved = GroupCandidate.classification_status == ClassificationStatus.APPROVED
        groups = await session.execute(
            select(
                func.count(GroupCandidate.id),
                func.count(GroupCandidate.id).filter(GroupCollectionState.group_id.is_(None)),
                func.count(GroupCandidate.id).filter(
                    GroupCollectionState.last_group_success_at.is_(None),
                    func.coalesce(GroupCollectionState.unavailable, False).is_(False),
                ),
                func.count(GroupCandidate.id).filter(
                    GroupCollectionState.last_posts_success_at.is_(None),
                    func.coalesce(GroupCollectionState.unavailable, False).is_(False),
                ),
                func.count(GroupCandidate.id).filter(
                    GroupCollectionState.last_members_success_at.is_(None),
                    func.coalesce(GroupCollectionState.unavailable, False).is_(False),
                ),
                func.count(GroupCandidate.id).filter(
                    func.coalesce(GroupCollectionState.unavailable, False).is_(True)
                ),
            )
            .outerjoin(GroupCollectionState, GroupCollectionState.group_id == GroupCandidate.id)
            .where(approved)
        )
        users = await session.execute(
            select(
                func.count(VKUser.vk_id),
                func.count(VKUser.vk_id).filter(
                    VKUser.deactivated.is_(None),
                    ((VKUser.is_closed.is_(False)) | (VKUser.can_access_closed.is_(True))),
                ),
                func.count(VKUser.vk_id).filter(VKUser.profile_updated_at.is_(None)),
                func.count(VKUser.vk_id).filter(VKUser.profile_updated_at < profile_stale),
                func.count(VKUser.vk_id).filter(VKUser.deactivated.is_not(None)),
                func.count(VKUser.vk_id).filter(
                    VKUser.is_closed.is_(True), VKUser.can_access_closed.is_(False)
                ),
            )
        )
        eligible_user = and_(
            VKUser.deactivated.is_(None),
            or_(VKUser.is_closed.is_(False), VKUser.can_access_closed.is_(True)),
            exists(
                select(GroupMembership.id)
                .join(GroupCandidate, GroupCandidate.id == GroupMembership.group_id)
                .where(
                    GroupMembership.user_id == VKUser.vk_id,
                    GroupMembership.is_current.is_(True),
                    GroupCandidate.classification_status == ClassificationStatus.APPROVED,
                )
            ),
        )
        subscriptions = await session.execute(
            select(
                func.count(VKUser.vk_id),
                func.count(VKUser.vk_id).filter(UserSubscriptionState.last_success_at.is_not(None)),
                func.count(VKUser.vk_id).filter(
                    UserSubscriptionState.last_success_at.is_(None),
                    UserSubscriptionState.terminal_reason == "privacy_or_access",
                ),
                func.count(VKUser.vk_id).filter(
                    UserSubscriptionState.last_success_at.is_(None),
                    UserSubscriptionState.terminal_reason == "deleted_or_unavailable",
                ),
                func.count(VKUser.vk_id).filter(
                    UserSubscriptionState.last_success_at.is_(None),
                    UserSubscriptionState.terminal_reason.is_(None),
                    UserSubscriptionState.next_scheduled_at > now,
                ),
                func.count(VKUser.vk_id).filter(
                    UserSubscriptionState.last_success_at.is_(None),
                    UserSubscriptionState.terminal_reason.is_(None),
                    UserSubscriptionState.next_scheduled_at <= now,
                ),
                func.count(VKUser.vk_id).filter(UserSubscriptionState.is_truncated.is_(True)),
                func.count(VKUser.vk_id).filter(
                    UserSubscriptionState.last_success_at.is_(None),
                    UserSubscriptionState.terminal_reason.is_(None),
                ),
            )
            .outerjoin(UserSubscriptionState, UserSubscriptionState.user_id == VKUser.vk_id)
            .where(eligible_user)
        )
        communities = await session.execute(
            select(
                func.count(distinct(UserGroupSubscription.vk_group_id)),
                func.count(distinct(UserGroupSubscription.vk_group_id)).filter(
                    VKCommunity.metadata_updated_at.is_(None)
                ),
                func.count(distinct(UserGroupSubscription.vk_group_id)).filter(
                    VKCommunity.metadata_updated_at < metadata_stale
                ),
                func.count(distinct(UserGroupSubscription.vk_group_id)).filter(
                    VKCommunity.deactivated.is_not(None)
                ),
                func.count(distinct(UserGroupSubscription.vk_group_id)).filter(
                    VKCommunity.is_closed.is_(True)
                ),
            )
            .join(VKCommunity, VKCommunity.vk_id == UserGroupSubscription.vk_group_id)
            .where(UserGroupSubscription.is_current.is_(True))
        )
        posts = await session.execute(
            select(
                func.count(distinct(UserGroupSubscription.vk_group_id)),
                func.count(distinct(UserGroupSubscription.vk_group_id)).filter(
                    CommunityPostCollectionState.community_vk_id.is_(None)
                ),
                func.count(distinct(UserGroupSubscription.vk_group_id)).filter(
                    CommunityPostCollectionState.last_success_at.is_not(None)
                ),
                func.count(distinct(UserGroupSubscription.vk_group_id)).filter(
                    CommunityPostCollectionState.wall_private.is_(True)
                ),
                func.count(distinct(UserGroupSubscription.vk_group_id)).filter(
                    CommunityPostCollectionState.unavailable.is_(True)
                ),
            )
            .join(VKCommunity, VKCommunity.vk_id == UserGroupSubscription.vk_group_id)
            .outerjoin(
                CommunityPostCollectionState,
                CommunityPostCollectionState.community_vk_id == UserGroupSubscription.vk_group_id,
            )
            .where(
                UserGroupSubscription.is_current.is_(True),
                VKCommunity.deactivated.is_(None),
            )
        )
        job_rows = (
            await session.execute(
                select(
                    CollectionJob.job_type,
                    CollectionJob.status,
                    func.count(CollectionJob.id),
                    func.count(
                        distinct(func.row(CollectionJob.entity_type, CollectionJob.entity_id))
                    ),
                )
                .group_by(CollectionJob.job_type, CollectionJob.status)
                .order_by(CollectionJob.job_type, CollectionJob.status)
            )
        ).all()
        run_rows = (
            await session.execute(
                select(CollectionRun.scope, CollectionRun.status, func.count(CollectionRun.id))
                .group_by(CollectionRun.scope, CollectionRun.status)
                .order_by(CollectionRun.scope, CollectionRun.status)
            )
        ).all()
        active_run_models = list(
            (
                await session.scalars(
                    select(CollectionRun).where(
                        CollectionRun.status.in_(
                            [
                                CollectionRunStatus.PLANNED,
                                CollectionRunStatus.RUNNING,
                                CollectionRunStatus.WAITING_METHOD_LIMIT,
                            ]
                        )
                    )
                )
            ).all()
        )
        expected_configuration = CollectionQueue(sessions, settings).collection_configuration()
        runtime_configuration_mismatches = sum(
            run.configuration.get("collection") != expected_configuration
            for run in active_run_models
        )
        invalid_gate_artifacts = 0
        for run in active_run_models:
            if run.configuration.get("capacity_gate") != "passed":
                continue
            report_path = run.configuration.get("capacity_report")
            backup = run.configuration.get("verified_backup")
            report_valid = isinstance(report_path, str) and Path(report_path).is_file()
            requires_backup = run.scope in {
                "subscriptions",
                "subscription_discovery",
                "subscription_metadata",
                "subscription_posts",
            }
            backup_valid = not requires_backup and not isinstance(backup, dict)
            if isinstance(backup, dict):
                backup_path = backup.get("path")
                try:
                    stat = Path(str(backup_path)).stat()
                except OSError:
                    backup_valid = False
                else:
                    backup_valid = stat.st_size == backup.get(
                        "size_bytes"
                    ) and stat.st_mtime_ns == backup.get("modified_ns")
            if not report_valid or not backup_valid:
                invalid_gate_artifacts += 1
        stale_leases = int(
            await session.scalar(
                select(func.count(CollectionJob.id)).where(
                    CollectionJob.status == JobStatus.RUNNING,
                    CollectionJob.locked_at < lease_stale,
                )
            )
            or 0
        )
        active_campaigns = int(
            await session.scalar(
                select(func.count(CollectionCampaign.id)).where(
                    CollectionCampaign.status.in_(
                        [
                            CampaignStatus.PLANNED.value,
                            CampaignStatus.RUNNING.value,
                            CampaignStatus.PAUSED.value,
                            CampaignStatus.WAITING_METHOD_LIMIT.value,
                            CampaignStatus.PAUSED_CAPACITY_LIMIT.value,
                        ]
                    )
                )
            )
            or 0
        )
        campaign_snapshot_users = int(
            await session.scalar(
                select(func.coalesce(func.sum(CollectionCampaign.snapshot_user_count), 0)).where(
                    CollectionCampaign.status.in_(
                        [
                            CampaignStatus.PLANNED.value,
                            CampaignStatus.RUNNING.value,
                            CampaignStatus.PAUSED.value,
                            CampaignStatus.WAITING_METHOD_LIMIT.value,
                            CampaignStatus.PAUSED_CAPACITY_LIMIT.value,
                        ]
                    )
                )
            )
            or 0
        )
        nearest_transient_retry = await session.scalar(
            select(func.min(CollectionJob.next_attempt_at)).where(
                CollectionJob.job_type == "collect_user_subscriptions",
                CollectionJob.status == JobStatus.RETRY_WAIT,
                CollectionJob.next_attempt_at > now,
            )
        )
        duplicate_active_campaign_types = int(
            await session.scalar(
                select(func.count()).select_from(
                    select(CollectionCampaign.campaign_type)
                    .where(
                        CollectionCampaign.status.in_(
                            [
                                CampaignStatus.PLANNED.value,
                                CampaignStatus.RUNNING.value,
                                CampaignStatus.PAUSED.value,
                                CampaignStatus.WAITING_METHOD_LIMIT.value,
                                CampaignStatus.PAUSED_CAPACITY_LIMIT.value,
                            ]
                        )
                    )
                    .group_by(CollectionCampaign.campaign_type)
                    .having(func.count(CollectionCampaign.id) > 1)
                    .subquery()
                )
            )
            or 0
        )
        unfinished_pilots = int(
            await session.scalar(
                select(func.count(CollectionRun.id)).where(
                    CollectionRun.scope.like("%pilot%"),
                    CollectionRun.status.not_in(
                        [
                            CollectionRunStatus.COMPLETED,
                            CollectionRunStatus.COMPLETED_WITH_ERRORS,
                            CollectionRunStatus.FAILED,
                            CollectionRunStatus.CANCELLED,
                        ]
                    ),
                )
            )
            or 0
        )
        group_values = tuple(int(value or 0) for value in groups.one())
        user_values = tuple(int(value or 0) for value in users.one())
        subscription_values = tuple(int(value or 0) for value in subscriptions.one())
        eligible_count = subscription_values[0]
        resolved_count = sum(subscription_values[1:4])
        community_values = tuple(int(value or 0) for value in communities.one())
        post_values = tuple(int(value or 0) for value in posts.one())
        return {
            "generated_at": now.isoformat(),
            "approved_groups": dict(
                zip(
                    (
                        "total",
                        "without_state",
                        "metadata_missing",
                        "posts_missing",
                        "members_missing",
                        "unavailable",
                    ),
                    group_values,
                    strict=True,
                )
            ),
            "users": dict(
                zip(
                    (
                        "total",
                        "accessible",
                        "profile_missing",
                        "profile_stale",
                        "deactivated",
                        "closed_without_access",
                    ),
                    user_values,
                    strict=True,
                )
            ),
            "subscriptions": dict(
                zip(
                    (
                        "eligible_users",
                        "successful",
                        "terminal_privacy",
                        "terminal_deleted",
                        "transient_deferred",
                        "transient_due",
                        "truncated",
                        "unresolved",
                    ),
                    subscription_values,
                    strict=True,
                )
            ),
            "subscription_communities": dict(
                zip(
                    (
                        "current_unique",
                        "metadata_missing",
                        "metadata_stale",
                        "deactivated",
                        "closed",
                    ),
                    community_values,
                    strict=True,
                )
            ),
            "subscription_posts": dict(
                zip(
                    (
                        "available_unique",
                        "without_state",
                        "collected",
                        "wall_private",
                        "unavailable",
                    ),
                    post_values,
                    strict=True,
                )
            ),
            "jobs": [
                {
                    "job_type": job_type,
                    "status": status.value,
                    "rows": int(rows),
                    "distinct_entities": int(entities),
                }
                for job_type, status, rows, entities in job_rows
            ],
            "runs": [
                {"scope": scope, "status": status.value, "count": int(count)}
                for scope, status, count in run_rows
            ],
            "stale_running_leases": stale_leases,
            "unfinished_pilots": unfinished_pilots,
            "active_campaigns": active_campaigns,
            "active_campaign_snapshot_users": campaign_snapshot_users,
            "nearest_transient_retry": (
                nearest_transient_retry.isoformat() if nearest_transient_retry else None
            ),
            "next_light_backlog": (
                "approved_group_metadata"
                if group_values[2]
                else "user_profiles"
                if user_values[2] + user_values[3]
                else None
            ),
            "data_provenance": {
                "app_env": settings.app_env,
                "environment_is_production": settings.app_env == "production",
                "production_snapshot_verified": False,
                "verification_note": "APP_ENV является marker среды, а не доказательством среза",
            },
            "campaign_types_with_multiple_active": duplicate_active_campaign_types,
            "active_run_configuration_mismatches": runtime_configuration_mismatches,
            "active_run_invalid_gate_artifacts": invalid_gate_artifacts,
            "historical_jobs_are_not_canonical_backlog": True,
            "subscription_coverage_percent": (
                round(100.0 * resolved_count / eligible_count, 2) if eligible_count else 100.0
            ),
            "subscription_eta_healthy_minutes": None,
            "subscription_eta_cooldown_adjusted_minutes": None,
            "subscription_eta_null_reason": (
                "Нет достаточных измерений healthy throughput и cooldown duty cycle"
            ),
        }

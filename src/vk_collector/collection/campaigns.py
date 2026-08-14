from __future__ import annotations

import hashlib
import json
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import and_, exists, func, literal, or_, select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from vk_collector.collection.queue import CollectionQueue
from vk_collector.config import Settings
from vk_collector.database.models import (
    CampaignPhase,
    CampaignStatus,
    ClassificationStatus,
    CollectionCampaign,
    CollectionCampaignUser,
    CollectionJob,
    CollectionRun,
    CollectionRunStatus,
    GroupCandidate,
    GroupMembership,
    JobStatus,
    UserGroupSubscription,
    UserSubscriptionState,
    VKCommunity,
    VKUser,
)

ACTIVE_CAMPAIGN_STATUSES = (
    CampaignStatus.PLANNED.value,
    CampaignStatus.RUNNING.value,
    CampaignStatus.PAUSED.value,
    CampaignStatus.WAITING_METHOD_LIMIT.value,
    CampaignStatus.PAUSED_CAPACITY_LIMIT.value,
)
ACTIVE_JOB_STATUSES = (
    JobStatus.PENDING,
    JobStatus.RUNNING,
    JobStatus.RETRY_WAIT,
    JobStatus.PAUSED,
)


class CampaignManager:
    """Plan and reconcile a fixed-snapshot phased subscription campaign."""

    def __init__(self, sessions: async_sessionmaker[AsyncSession], settings: Settings) -> None:
        self._sessions = sessions
        self._settings = settings

    def configuration(self) -> dict[str, object]:
        queue = CollectionQueue(self._sessions, self._settings)
        return {
            "campaign_type": "subscription_enrichment",
            "cohort_users": self._settings.collection_campaign_cohort_users,
            "metadata_batch_size": self._settings.collection_community_metadata_batch_size,
            "metadata_ttl_days": self._settings.collection_community_metadata_ttl_days,
            "subscription_limit": self._settings.collection_subscriptions_max_per_user,
            "collection": queue.collection_configuration(),
        }

    async def plan_preview(self) -> dict[str, object]:
        """Estimate the narrow snapshot without materializing campaign state."""
        now = datetime.now(UTC)
        async with self._sessions() as session:
            users = int(
                await session.scalar(
                    select(func.count(VKUser.vk_id)).where(self._eligible_user_predicate(now))
                )
                or 0
            )
            active = await session.scalar(
                select(CollectionCampaign).where(
                    CollectionCampaign.campaign_type == "subscription_enrichment",
                    CollectionCampaign.status.in_(ACTIVE_CAMPAIGN_STATUSES),
                )
            )
        configuration = self.configuration()
        configuration_hash = hashlib.sha256(
            json.dumps(configuration, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()
        return {
            "apply": False,
            "campaign_type": "subscription_enrichment",
            "snapshot_users": users,
            "estimated_snapshot_bytes": users * 96,
            "cohort_users": self._settings.collection_campaign_cohort_users,
            "subscription_limit": self._settings.collection_subscriptions_max_per_user,
            "planning_configuration_hash": configuration_hash,
            "compatible_active_campaign": (
                active is not None and active.configuration_hash == configuration_hash
            ),
            "incompatible_active_campaign": (
                active is not None and active.configuration_hash != configuration_hash
            ),
            "initial_status": "paused_capacity_limit",
        }

    async def plan(self) -> uuid.UUID:
        """Create or reuse one active campaign for the exact configuration."""
        configuration = self.configuration()
        configuration_hash = hashlib.sha256(
            json.dumps(configuration, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()
        now = datetime.now(UTC)
        async with self._sessions() as session:
            await session.execute(
                select(func.pg_advisory_xact_lock(func.hashtext("subscription_enrichment")))
            )
            existing = await session.scalar(
                select(CollectionCampaign)
                .where(
                    CollectionCampaign.campaign_type == "subscription_enrichment",
                    CollectionCampaign.status.in_(ACTIVE_CAMPAIGN_STATUSES),
                )
                .order_by(CollectionCampaign.created_at.desc())
                .limit(1)
                .with_for_update()
            )
            if existing is not None:
                if existing.configuration_hash != configuration_hash:
                    raise ValueError(
                        "Уже существует активная subscription campaign с другой "
                        "immutable planning configuration"
                    )
                return existing.id
            snapshot_max = int(
                await session.scalar(
                    select(func.coalesce(func.max(VKUser.vk_id), 0)).where(
                        self._eligible_user_predicate(now)
                    )
                )
                or 0
            )
            campaign = CollectionCampaign(
                campaign_type="subscription_enrichment",
                status=CampaignStatus.PAUSED_CAPACITY_LIMIT.value,
                phase=CampaignPhase.SUBSCRIPTION_DISCOVERY.value,
                snapshot_at=now,
                snapshot_max_user_id=snapshot_max,
                configuration=configuration,
                configuration_hash=configuration_hash,
            )
            session.add(campaign)
            await session.flush()
            snapshot_select = select(literal(campaign.id), VKUser.vk_id).where(
                self._eligible_user_predicate(now),
                VKUser.vk_id <= snapshot_max,
            )
            await session.execute(
                insert(CollectionCampaignUser)
                .from_select(["campaign_id", "user_id"], snapshot_select)
                .on_conflict_do_nothing()
            )
            campaign.snapshot_user_count = int(
                await session.scalar(
                    select(func.count(CollectionCampaignUser.user_id)).where(
                        CollectionCampaignUser.campaign_id == campaign.id
                    )
                )
                or 0
            )
            if await self._plan_discovery_cohort(session, campaign) is None:
                campaign.status = CampaignStatus.COMPLETED.value
                campaign.phase = CampaignPhase.COMPLETED.value
                campaign.finished_at = now
            await session.commit()
            return campaign.id

    def _eligible_user_predicate(self, snapshot_at: datetime) -> Any:
        membership = exists(
            select(GroupMembership.id)
            .join(GroupCandidate, GroupCandidate.id == GroupMembership.group_id)
            .where(
                GroupMembership.user_id == VKUser.vk_id,
                GroupMembership.first_seen_at <= snapshot_at,
                GroupMembership.is_current.is_(True),
                GroupCandidate.classification_status == ClassificationStatus.APPROVED,
            )
        )
        return and_(
            VKUser.first_seen_at <= snapshot_at,
            VKUser.deactivated.is_(None),
            or_(VKUser.is_closed.is_(False), VKUser.can_access_closed.is_(True)),
            membership,
        )

    async def _plan_discovery_cohort(
        self, session: AsyncSession, campaign: CollectionCampaign
    ) -> uuid.UUID | None:
        resolved = self._resolved_user_ids(campaign)
        ids = list(
            (
                await session.scalars(
                    select(CollectionCampaignUser.user_id)
                    .where(
                        CollectionCampaignUser.campaign_id == campaign.id,
                        CollectionCampaignUser.user_id > campaign.last_planned_user_id,
                        CollectionCampaignUser.user_id.not_in(resolved),
                    )
                    .order_by(CollectionCampaignUser.user_id)
                    .limit(self._settings.collection_campaign_cohort_users)
                )
            ).all()
        )
        if not ids:
            return None
        gate_passed = campaign.configuration.get("capacity_gate") == "passed"
        run = CollectionRun(
            campaign_id=campaign.id,
            scope="subscription_discovery",
            status=(
                CollectionRunStatus.PLANNED
                if gate_passed
                else CollectionRunStatus.PAUSED_CAPACITY_LIMIT
            ),
            configuration={
                "campaign_id": str(campaign.id),
                "phase": CampaignPhase.SUBSCRIPTION_DISCOVERY.value,
                "cohort_first_user_id": ids[0],
                "cohort_last_user_id": ids[-1],
                "user_count": len(ids),
                "capacity_gate": "passed" if gate_passed else "pilot_required",
                "collection": CollectionQueue(
                    self._sessions, self._settings
                ).collection_configuration(),
                **self._inherited_gate(campaign),
            },
        )
        session.add(run)
        await session.flush()
        for start in range(0, len(ids), 1000):
            await session.execute(
                insert(CollectionJob)
                .values(
                    [
                        {
                            "collection_run_id": run.id,
                            "job_type": "collect_user_subscriptions",
                            "entity_type": "user",
                            "entity_id": user_id,
                            "priority": 10,
                        }
                        for user_id in ids[start : start + 1000]
                    ]
                )
                .on_conflict_do_nothing(constraint="uq_collection_jobs_run_type_entity")
            )
        run.total_jobs = len(ids)
        campaign.last_planned_user_id = ids[-1]
        if gate_passed:
            campaign.status = CampaignStatus.RUNNING.value
            campaign.next_wakeup_at = None
            campaign.error_message = None
        return run.id

    def _inherited_gate(self, campaign: CollectionCampaign) -> dict[str, object]:
        keys = ("capacity_report", "verified_backup", "projected_database_bytes")
        return {key: campaign.configuration[key] for key in keys if key in campaign.configuration}

    def _resolved_user_ids(self, campaign: CollectionCampaign) -> Any:
        """Reuse canonical state only while its explicit freshness window is valid."""
        return select(UserSubscriptionState.user_id).where(
            UserSubscriptionState.next_scheduled_at.is_not(None),
            UserSubscriptionState.next_scheduled_at > campaign.snapshot_at,
            or_(
                UserSubscriptionState.last_success_at.is_not(None),
                UserSubscriptionState.terminal_reason.is_not(None),
            ),
        )

    async def _plan_metadata_cohort(
        self, session: AsyncSession, campaign: CollectionCampaign
    ) -> uuid.UUID | None:
        stale_before = datetime.now(UTC) - timedelta(
            days=self._settings.collection_community_metadata_ttl_days
        )
        eligible_users = select(CollectionCampaignUser.user_id).where(
            CollectionCampaignUser.campaign_id == campaign.id
        )
        ids = list(
            (
                await session.scalars(
                    select(UserGroupSubscription.vk_group_id)
                    .join(VKCommunity, VKCommunity.vk_id == UserGroupSubscription.vk_group_id)
                    .where(
                        UserGroupSubscription.is_current.is_(True),
                        UserGroupSubscription.user_id.in_(eligible_users),
                        UserGroupSubscription.vk_group_id > campaign.last_metadata_vk_id,
                        VKCommunity.deactivated.is_(None),
                        or_(
                            VKCommunity.metadata_updated_at.is_(None),
                            VKCommunity.metadata_updated_at < stale_before,
                        ),
                    )
                    .distinct()
                    .order_by(UserGroupSubscription.vk_group_id)
                    .limit(self._settings.collection_campaign_cohort_users)
                )
            ).all()
        )
        if not ids:
            return None
        run = CollectionRun(
            campaign_id=campaign.id,
            scope="subscription_metadata",
            status=CollectionRunStatus.PLANNED,
            configuration={
                "campaign_id": str(campaign.id),
                "phase": CampaignPhase.SUBSCRIPTION_METADATA.value,
                "community_count": len(ids),
                "capacity_gate": "passed",
                "collection": CollectionQueue(
                    self._sessions, self._settings
                ).collection_configuration(),
                **self._inherited_gate(campaign),
            },
        )
        session.add(run)
        await session.flush()
        for start in range(0, len(ids), 1000):
            await session.execute(
                insert(CollectionJob)
                .values(
                    [
                        {
                            "collection_run_id": run.id,
                            "job_type": "refresh_community_metadata",
                            "entity_type": "vk_community",
                            "entity_id": community_id,
                            "priority": 10,
                        }
                        for community_id in ids[start : start + 1000]
                    ]
                )
                .on_conflict_do_nothing(constraint="uq_collection_jobs_run_type_entity")
            )
        run.total_jobs = len(ids)
        campaign.last_metadata_vk_id = ids[-1]
        campaign.status = CampaignStatus.RUNNING.value
        campaign.next_wakeup_at = None
        campaign.error_message = None
        return run.id

    async def reconcile(self, campaign_id: uuid.UUID) -> None:
        """Advance cohorts/phases only from canonical terminal state."""
        async with self._sessions() as session:
            campaign = await session.get(CollectionCampaign, campaign_id, with_for_update=True)
            if campaign is None or campaign.status not in ACTIVE_CAMPAIGN_STATUSES:
                return
            failed = int(
                await session.scalar(
                    select(func.count(CollectionJob.id))
                    .join(CollectionRun, CollectionRun.id == CollectionJob.collection_run_id)
                    .where(
                        CollectionRun.campaign_id == campaign.id,
                        CollectionJob.status == JobStatus.FAILED,
                    )
                )
                or 0
            )
            if failed:
                campaign.status = CampaignStatus.FAILED.value
                campaign.phase = CampaignPhase.FAILED.value
                campaign.error_message = f"Кампания содержит failed jobs: {failed}"
                campaign.finished_at = datetime.now(UTC)
                await session.commit()
                return
            active = int(
                await session.scalar(
                    select(func.count(CollectionJob.id))
                    .join(CollectionRun, CollectionRun.id == CollectionJob.collection_run_id)
                    .where(
                        CollectionRun.campaign_id == campaign.id,
                        CollectionJob.status.in_(ACTIVE_JOB_STATUSES),
                    )
                )
                or 0
            )
            if active:
                await session.commit()
                return
            if campaign.phase == CampaignPhase.SUBSCRIPTION_DISCOVERY.value:
                unresolved = await self._unresolved_count(session, campaign)
                if unresolved:
                    planned = await self._plan_discovery_cohort(session, campaign)
                    if planned is None:
                        campaign.status = CampaignStatus.FAILED.value
                        campaign.phase = CampaignPhase.FAILED.value
                        campaign.error_message = (
                            f"Остались unresolved пользователи без runnable jobs: {unresolved}"
                        )
                        campaign.finished_at = datetime.now(UTC)
                    await session.commit()
                    return
                campaign.phase = CampaignPhase.SUBSCRIPTION_METADATA.value
                campaign.last_metadata_vk_id = 0
                if await self._plan_metadata_cohort(session, campaign) is None:
                    campaign.status = CampaignStatus.COMPLETED.value
                    campaign.phase = CampaignPhase.COMPLETED.value
                    campaign.finished_at = datetime.now(UTC)
                    campaign.next_wakeup_at = None
                else:
                    campaign.status = CampaignStatus.RUNNING.value
            elif campaign.phase == CampaignPhase.SUBSCRIPTION_METADATA.value:
                if await self._plan_metadata_cohort(session, campaign) is None:
                    campaign.status = CampaignStatus.COMPLETED.value
                    campaign.phase = CampaignPhase.COMPLETED.value
                    campaign.finished_at = datetime.now(UTC)
                    campaign.next_wakeup_at = None
            await session.commit()

    async def _unresolved_count(self, session: AsyncSession, campaign: CollectionCampaign) -> int:
        resolved = self._resolved_user_ids(campaign)
        return int(
            await session.scalar(
                select(func.count(CollectionCampaignUser.user_id)).where(
                    CollectionCampaignUser.campaign_id == campaign.id,
                    CollectionCampaignUser.user_id.not_in(resolved),
                )
            )
            or 0
        )

    async def change_status(self, campaign_id: uuid.UUID, *, pause: bool) -> None:
        """Pause/resume a campaign without resetting checkpoints."""
        async with self._sessions() as session:
            campaign = await session.get(CollectionCampaign, campaign_id, with_for_update=True)
            if campaign is None:
                raise ValueError("Кампания не найдена")
            if not pause and campaign.configuration.get("capacity_gate") != "passed":
                raise ValueError("Сначала примените проверенный capacity gate к первому cohort")
            campaign.status = CampaignStatus.PAUSED.value if pause else CampaignStatus.RUNNING.value
            run_status = CollectionRunStatus.PAUSED if pause else CollectionRunStatus.RUNNING
            await session.execute(
                update(CollectionRun)
                .where(
                    CollectionRun.campaign_id == campaign_id,
                    CollectionRun.status.in_(
                        [
                            CollectionRunStatus.PLANNED,
                            CollectionRunStatus.RUNNING,
                            CollectionRunStatus.PAUSED,
                            CollectionRunStatus.WAITING_METHOD_LIMIT,
                        ]
                    ),
                )
                .values(status=run_status)
            )
            await session.execute(
                update(CollectionJob)
                .where(
                    CollectionJob.collection_run_id.in_(
                        select(CollectionRun.id).where(CollectionRun.campaign_id == campaign_id)
                    ),
                    CollectionJob.status.in_([JobStatus.PENDING, JobStatus.PAUSED]),
                )
                .values(status=JobStatus.PAUSED if pause else JobStatus.PENDING)
            )
            await session.commit()

    async def status(self, campaign_id: uuid.UUID | None = None) -> dict[str, Any]:
        """Return campaign coverage and phase using canonical state."""
        async with self._sessions() as session:
            campaign = (
                await session.get(CollectionCampaign, campaign_id)
                if campaign_id is not None
                else await session.scalar(
                    select(CollectionCampaign)
                    .order_by(CollectionCampaign.created_at.desc())
                    .limit(1)
                )
            )
            if campaign is None:
                return {"campaign_id": None, "status": "absent"}
            eligible = campaign.snapshot_user_count
            unresolved = await self._unresolved_count(session, campaign)
            truncated = int(
                await session.scalar(
                    select(func.count(UserSubscriptionState.user_id)).where(
                        UserSubscriptionState.last_campaign_id == campaign.id,
                        UserSubscriptionState.is_truncated.is_(True),
                    )
                )
                or 0
            )
            return {
                "campaign_id": str(campaign.id),
                "campaign_type": campaign.campaign_type,
                "status": campaign.status,
                "phase": campaign.phase,
                "snapshot_at": campaign.snapshot_at.isoformat(),
                "snapshot_users": eligible,
                "eligible_users": eligible,
                "resolved_users": max(0, eligible - unresolved),
                "unresolved_users": unresolved,
                "coverage_percent": round(100.0 * (eligible - unresolved) / eligible, 2)
                if eligible
                else 100.0,
                "truncated_users": truncated,
                "next_wakeup_at": (
                    campaign.next_wakeup_at.isoformat() if campaign.next_wakeup_at else None
                ),
                "error_message": campaign.error_message,
            }

    async def metadata_preview(self, campaign_id: uuid.UUID) -> dict[str, int]:
        """Read-only preview of distinct metadata work after discovery."""
        async with self._sessions() as session:
            campaign = await session.get(CollectionCampaign, campaign_id)
            if campaign is None:
                raise ValueError("Кампания не найдена")
            stale_before = datetime.now(UTC) - timedelta(
                days=self._settings.collection_community_metadata_ttl_days
            )
            eligible_users = select(CollectionCampaignUser.user_id).where(
                CollectionCampaignUser.campaign_id == campaign.id
            )
            total = int(
                await session.scalar(
                    select(func.count(func.distinct(UserGroupSubscription.vk_group_id))).where(
                        UserGroupSubscription.is_current.is_(True),
                        UserGroupSubscription.user_id.in_(eligible_users),
                    )
                )
                or 0
            )
            due = int(
                await session.scalar(
                    select(func.count(func.distinct(UserGroupSubscription.vk_group_id)))
                    .join(VKCommunity, VKCommunity.vk_id == UserGroupSubscription.vk_group_id)
                    .where(
                        UserGroupSubscription.is_current.is_(True),
                        UserGroupSubscription.user_id.in_(eligible_users),
                        VKCommunity.deactivated.is_(None),
                        or_(
                            VKCommunity.metadata_updated_at.is_(None),
                            VKCommunity.metadata_updated_at < stale_before,
                        ),
                    )
                )
                or 0
            )
            return {"unique_communities": total, "metadata_due": due}

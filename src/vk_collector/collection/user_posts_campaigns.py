from __future__ import annotations

import hashlib
import json
import math
import uuid
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import and_, exists, func, literal, or_, select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from vk_collector.collection.campaigns import (
    ACTIVE_CAMPAIGN_STATUSES,
    IMMEDIATE_JOB_STATUSES,
    MINIMUM_RESERVE_FACTOR,
    SNAPSHOT_HEAP_BYTES_PER_USER,
    SNAPSHOT_PRIMARY_KEY_BYTES_PER_USER,
)
from vk_collector.collection.capacity import SAFE_DISK_LIMIT_BYTES
from vk_collector.collection.queue import CollectionQueue
from vk_collector.collection.safety import DiskState, inspect_disk
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
    UserPost,
    UserPostAttachment,
    UserPostCollectionState,
    VKUser,
)

CAMPAIGN_TYPE = "user_posts_enrichment"
RUN_SCOPE = "user_posts"
PILOT_SCOPE = "user_posts_pilot"
PHASE = CampaignPhase.USER_POSTS_COLLECTION.value
STATE_BYTES_PER_USER = 256
JOB_BYTES_PER_USER = 768
INDEX_BYTES_PER_POST = 160
MINIMUM_POST_BYTES = 1024
MINIMUM_ATTACHMENT_BYTES = 384


def build_user_posts_capacity_projection(
    *,
    preview: dict[str, object],
    pilot: dict[str, object],
    database_bytes: int,
    disk: DiskState,
    warning_percent: int,
    safe_database_limit_bytes: int = SAFE_DISK_LIMIT_BYTES,
    min_free_bytes: int = 0,
) -> dict[str, object]:
    """Project the complete immutable snapshot from measured user-wall evidence."""
    reasons: list[str] = []
    snapshot_users = preview.get("snapshot_users")
    due_users = preview.get("due_users")
    measured_users = pilot.get("measured_users")
    measured_growth = pilot.get("database_growth_bytes")
    posts = pilot.get("user_posts")
    attachments = pilot.get("attachments")
    if not isinstance(snapshot_users, int) or snapshot_users < 0:
        reasons.append("snapshot_users некорректен")
        snapshot_users = 0
    if not isinstance(due_users, int) or due_users < 0 or due_users > snapshot_users:
        reasons.append("due_users некорректен")
        due_users = 0
    if not isinstance(measured_users, int) or measured_users <= 0:
        reasons.append("Pilot не содержит обработанных пользователей")
        measured_users = 0
    if not isinstance(measured_growth, int) or measured_growth < 0:
        reasons.append("Pilot не содержит корректный фактический рост БД")
        measured_growth = 0
    if not isinstance(posts, int) or posts < 0:
        reasons.append("Pilot user_posts некорректен")
        posts = 0
    if not isinstance(attachments, int) or attachments < 0:
        reasons.append("Pilot attachments некорректен")
        attachments = 0

    measured_per_user = math.ceil(measured_growth / measured_users) if measured_users else 0
    modeled_payload = (
        math.ceil(posts / measured_users) * (MINIMUM_POST_BYTES + INDEX_BYTES_PER_POST)
        + math.ceil(attachments / measured_users) * MINIMUM_ATTACHMENT_BYTES
        if measured_users
        else 0
    )
    per_due_user = (
        max(measured_per_user, modeled_payload) + STATE_BYTES_PER_USER + JOB_BYTES_PER_USER
    )
    snapshot_bytes = snapshot_users * (
        SNAPSHOT_HEAP_BYTES_PER_USER + SNAPSHOT_PRIMARY_KEY_BYTES_PER_USER
    )
    payload_growth = math.ceil(due_users * per_due_user * MINIMUM_RESERVE_FACTOR)
    snapshot_growth = math.ceil(snapshot_bytes * MINIMUM_RESERVE_FACTOR)
    aggregate_growth = payload_growth + snapshot_growth
    projected_database = database_bytes + aggregate_growth
    projected_used = (
        100.0 * (disk.total_bytes - disk.free_bytes + aggregate_growth) / disk.total_bytes
        if disk.total_bytes > 0
        else 100.0
    )
    available_disk_growth = max(0, disk.free_bytes - min_free_bytes)
    available_growth = min(
        max(0, safe_database_limit_bytes - database_bytes), available_disk_growth
    )
    if projected_database > safe_database_limit_bytes:
        reasons.append("projected final database превышает настроенный hard safe database limit")
    if aggregate_growth > available_disk_growth:
        reasons.append("projected growth превышает свободный диск")
    if disk.warning or disk.stop or projected_used >= warning_percent:
        reasons.append("projected disk usage достигает warning threshold")
    return {
        **preview,
        "pilot": pilot,
        "reserve_factor": MINIMUM_RESERVE_FACTOR,
        "measured_payload_bytes_per_user": measured_per_user,
        "modeled_payload_bytes_per_user": modeled_payload,
        "state_bytes_per_user": STATE_BYTES_PER_USER,
        "job_bytes_per_user": JOB_BYTES_PER_USER,
        "payload_projected_growth_bytes": payload_growth,
        "snapshot_projected_growth_bytes": snapshot_growth,
        "aggregate_projected_growth_bytes": aggregate_growth,
        "current_database_bytes": database_bytes,
        "projected_final_database_bytes": projected_database,
        "current_disk_free_bytes": disk.free_bytes,
        "projected_final_disk_used_percent": projected_used,
        "safe_database_limit_bytes": safe_database_limit_bytes,
        "minimum_disk_free_bytes": min_free_bytes,
        "available_growth_bytes": available_growth,
        "additional_disk_required_bytes": max(0, aggregate_growth - available_growth),
        "decision": "passed" if not reasons else "rejected",
        "rejection_reasons": reasons,
    }


class UserPostCampaignManager:
    """Durable fixed-snapshot campaign for authorized personal user walls."""

    def __init__(self, sessions: async_sessionmaker[AsyncSession], settings: Settings) -> None:
        self._sessions = sessions
        self._settings = settings

    def configuration(self) -> dict[str, object]:
        return {
            "campaign_type": CAMPAIGN_TYPE,
            "cohort_users": self._settings.collection_campaign_cohort_users,
            "snapshot_user_limit": self._settings.collection_user_posts_snapshot_user_limit,
            "minimum_disk_free_bytes": self._settings.collection_disk_min_free_bytes,
            "safe_database_limit_bytes": self._settings.collection_safe_database_limit_bytes,
            "maximum_posts_per_user": self._settings.collection_user_posts_max_per_user,
            "window_days": self._settings.collection_user_posts_window_days,
            "collection": CollectionQueue(
                self._sessions, self._settings
            ).collection_configuration(),
        }

    def _configuration_hash(self) -> str:
        return hashlib.sha256(
            json.dumps(self.configuration(), sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()

    @staticmethod
    def eligible_predicate(snapshot_at: datetime) -> Any:
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

    @staticmethod
    def resolved_ids(snapshot_at: datetime) -> Any:
        return select(UserPostCollectionState.user_id).where(
            UserPostCollectionState.next_scheduled_at.is_not(None),
            UserPostCollectionState.next_scheduled_at > snapshot_at,
            or_(
                UserPostCollectionState.last_success_at.is_not(None),
                UserPostCollectionState.wall_private.is_(True),
                UserPostCollectionState.unavailable.is_(True),
            ),
        )

    async def plan_preview(self) -> dict[str, object]:
        now = datetime.now(UTC)
        async with self._sessions() as session:
            eligible = self.eligible_predicate(now)
            resolved = self.resolved_ids(now)
            users = int(await session.scalar(select(func.count(VKUser.vk_id)).where(eligible)) or 0)
            fresh = int(
                await session.scalar(
                    select(func.count(VKUser.vk_id)).where(eligible, VKUser.vk_id.in_(resolved))
                )
                or 0
            )
            terminal = int(
                await session.scalar(
                    select(func.count(VKUser.vk_id)).where(
                        eligible,
                        VKUser.vk_id.in_(
                            select(UserPostCollectionState.user_id).where(
                                UserPostCollectionState.next_scheduled_at > now,
                                or_(
                                    UserPostCollectionState.wall_private.is_(True),
                                    UserPostCollectionState.unavailable.is_(True),
                                ),
                            )
                        ),
                    )
                )
                or 0
            )
        full_due = max(0, users - fresh)
        limit = self._settings.collection_user_posts_snapshot_user_limit
        bounded = limit is not None
        target_due = min(full_due, limit) if limit is not None else full_due
        return {
            "apply": False,
            "campaign_type": CAMPAIGN_TYPE,
            "snapshot_users": target_due if bounded else users,
            "fresh_users": 0 if bounded else fresh,
            "terminal_users": 0 if bounded else terminal,
            "due_users": target_due,
            "eligible_users": users,
            "eligible_fresh_users": fresh,
            "eligible_terminal_users": terminal,
            "eligible_due_users": full_due,
            "bounded_snapshot": bounded,
            "snapshot_user_limit": limit,
            "cohort_users": self._settings.collection_campaign_cohort_users,
            "maximum_posts_per_user": self._settings.collection_user_posts_max_per_user,
            "window_days": self._settings.collection_user_posts_window_days,
            "planning_configuration_hash": self._configuration_hash(),
        }

    async def pilot_preview(self) -> dict[str, object]:
        preview = await self.plan_preview()
        raw_due = preview.get("due_users")
        due_users = raw_due if isinstance(raw_due, int) else 0
        return {
            **preview,
            "pilot_users": min(due_users, self._settings.collection_user_posts_pilot_users),
            "maximum_pilot_users": 500,
            "estimated_wall_get_requests": min(
                due_users, self._settings.collection_user_posts_pilot_users
            ),
        }

    async def plan_pilot(self) -> uuid.UUID:
        now = datetime.now(UTC)
        configuration = self.configuration()
        async with self._sessions() as session:
            await session.execute(select(func.pg_advisory_xact_lock(func.hashtext(PILOT_SCOPE))))
            existing = await session.scalar(
                select(CollectionRun)
                .where(
                    CollectionRun.scope == PILOT_SCOPE,
                    CollectionRun.status.in_(
                        [
                            CollectionRunStatus.PLANNED,
                            CollectionRunStatus.RUNNING,
                            CollectionRunStatus.PAUSED,
                            CollectionRunStatus.PAUSED_NO_TOKENS,
                            CollectionRunStatus.WAITING_METHOD_LIMIT,
                        ]
                    ),
                )
                .order_by(CollectionRun.created_at)
                .limit(1)
                .with_for_update()
            )
            if existing is not None:
                if existing.configuration.get("collection") != configuration["collection"]:
                    raise ValueError("Существует несовместимый незавершённый user-post pilot")
                return existing.id
            ids = list(
                (
                    await session.scalars(
                        select(VKUser.vk_id)
                        .where(
                            self.eligible_predicate(now),
                            VKUser.vk_id.not_in(self.resolved_ids(now)),
                        )
                        .order_by(VKUser.vk_id)
                        .limit(self._settings.collection_user_posts_pilot_users)
                    )
                ).all()
            )
            plan_key = hashlib.sha256(
                json.dumps({"configuration": configuration, "users": ids}, sort_keys=True).encode()
            ).hexdigest()
            run = CollectionRun(
                scope=PILOT_SCOPE,
                status=CollectionRunStatus.PLANNED if ids else CollectionRunStatus.COMPLETED,
                finished_at=now if not ids else None,
                total_jobs=len(ids),
                configuration={
                    "plan_key": plan_key,
                    "pilot": True,
                    "phase": PHASE,
                    "capacity_gate": "pilot",
                    "user_count": len(ids),
                    "collection": configuration["collection"],
                },
            )
            session.add(run)
            await session.flush()
            if ids:
                await session.execute(
                    insert(CollectionJob)
                    .values(
                        [
                            {
                                "collection_run_id": run.id,
                                "job_type": "collect_user_posts",
                                "entity_type": "user",
                                "entity_id": user_id,
                                "priority": 45,
                            }
                            for user_id in ids
                        ]
                    )
                    .on_conflict_do_nothing(constraint="uq_collection_jobs_run_type_entity")
                )
            await session.commit()
            return run.id

    async def pilot_metrics(self, run_id: uuid.UUID, *, database_before: int) -> dict[str, object]:
        async with self._sessions() as session:
            run = await session.get(CollectionRun, run_id)
            if run is None or run.scope != PILOT_SCOPE:
                raise ValueError("User-post pilot не найден")
            database_after = int(
                await session.scalar(select(func.pg_database_size(func.current_database()))) or 0
            )
            jobs = (
                await session.execute(
                    select(CollectionJob.status, func.count(CollectionJob.id))
                    .where(CollectionJob.collection_run_id == run_id)
                    .group_by(CollectionJob.status)
                )
            ).all()
            state_count = int(
                await session.scalar(
                    select(func.count(UserPostCollectionState.user_id)).where(
                        UserPostCollectionState.last_run_id == run_id
                    )
                )
                or 0
            )
            posts = int(
                await session.scalar(
                    select(func.count(UserPost.id)).where(
                        UserPost.user_id.in_(
                            select(CollectionJob.entity_id).where(
                                CollectionJob.collection_run_id == run_id,
                                CollectionJob.job_type == "collect_user_posts",
                            )
                        )
                    )
                )
                or 0
            )
            attachments = int(
                await session.scalar(
                    select(func.count(UserPostAttachment.id))
                    .join(UserPost, UserPost.id == UserPostAttachment.post_id)
                    .where(
                        UserPost.user_id.in_(
                            select(CollectionJob.entity_id).where(
                                CollectionJob.collection_run_id == run_id
                            )
                        )
                    )
                )
                or 0
            )
            private = int(
                await session.scalar(
                    select(func.count(UserPostCollectionState.user_id)).where(
                        UserPostCollectionState.last_run_id == run_id,
                        UserPostCollectionState.wall_private.is_(True),
                    )
                )
                or 0
            )
            unavailable = int(
                await session.scalar(
                    select(func.count(UserPostCollectionState.user_id)).where(
                        UserPostCollectionState.last_run_id == run_id,
                        UserPostCollectionState.unavailable.is_(True),
                    )
                )
                or 0
            )
            requests = int(
                await session.scalar(
                    select(func.coalesce(func.sum(CollectionJob.api_requests), 0)).where(
                        CollectionJob.collection_run_id == run_id
                    )
                )
                or 0
            )
        counts = {status.value: int(count) for status, count in jobs}
        return {
            "run_id": str(run_id),
            "run_status": run.status.value,
            "planned_users": run.total_jobs,
            "measured_users": state_count,
            "completed_users": counts.get("completed", 0),
            "skipped_users": counts.get("skipped", 0),
            "failed_users": counts.get("failed", 0),
            "private_users": private,
            "unavailable_users": unavailable,
            "wall_get_requests": requests,
            "user_posts": posts,
            "attachments": attachments,
            "jobs": counts,
            "database_before_bytes": database_before,
            "database_after_bytes": database_after,
            "database_growth_bytes": max(0, database_after - database_before),
        }

    async def plan(self, *, gate_evidence: dict[str, object]) -> uuid.UUID:
        if gate_evidence.get("decision") != "passed":
            raise ValueError("User-post campaign требует passed aggregate capacity gate")
        if gate_evidence.get("planning_configuration_hash") != self._configuration_hash():
            raise ValueError("Capacity evidence относится к другой configuration")
        for key in (
            "current_database_bytes",
            "aggregate_projected_growth_bytes",
            "payload_projected_growth_bytes",
            "snapshot_projected_growth_bytes",
            "projected_final_database_bytes",
            "current_disk_free_bytes",
            "safe_database_limit_bytes",
        ):
            value = gate_evidence.get(key)
            if not isinstance(value, int) or value < 0:
                raise ValueError("Capacity evidence неполон")
        projected_final = gate_evidence.get("projected_final_database_bytes")
        safe_limit = gate_evidence.get("safe_database_limit_bytes")
        current_database = gate_evidence.get("current_database_bytes")
        aggregate_growth = gate_evidence.get("aggregate_projected_growth_bytes")
        payload_growth = gate_evidence.get("payload_projected_growth_bytes")
        snapshot_growth = gate_evidence.get("snapshot_projected_growth_bytes")
        disk_free = gate_evidence.get("current_disk_free_bytes")
        minimum_disk_free = gate_evidence.get("minimum_disk_free_bytes", 0)
        assert isinstance(projected_final, int)
        assert isinstance(safe_limit, int)
        assert isinstance(current_database, int)
        assert isinstance(aggregate_growth, int)
        assert isinstance(payload_growth, int)
        assert isinstance(snapshot_growth, int)
        assert isinstance(disk_free, int)
        if not isinstance(minimum_disk_free, int) or minimum_disk_free < 0:
            raise ValueError("Capacity evidence не содержит корректный disk reserve")
        if safe_limit != self._settings.collection_safe_database_limit_bytes:
            raise ValueError("Capacity evidence содержит неожиданный safe database limit")
        if minimum_disk_free != self._settings.collection_disk_min_free_bytes:
            raise ValueError("Capacity evidence содержит неожиданный disk reserve")
        if projected_final < current_database + aggregate_growth:
            raise ValueError("Capacity evidence занижает projected final database")
        if aggregate_growth < payload_growth + snapshot_growth:
            raise ValueError("Capacity evidence занижает payload/snapshot projection")
        if projected_final > safe_limit:
            raise ValueError("Capacity evidence превышает safe database limit")
        if aggregate_growth > max(0, disk_free - minimum_disk_free):
            raise ValueError("Capacity evidence превышает свободный диск")
        now = datetime.now(UTC)
        configuration = self.configuration()
        configuration_hash = self._configuration_hash()
        async with self._sessions() as session:
            await session.execute(select(func.pg_advisory_xact_lock(func.hashtext(CAMPAIGN_TYPE))))
            existing = await session.scalar(
                select(CollectionCampaign)
                .where(
                    CollectionCampaign.campaign_type == CAMPAIGN_TYPE,
                    CollectionCampaign.status.in_(ACTIVE_CAMPAIGN_STATUSES),
                )
                .limit(1)
                .with_for_update()
            )
            if existing is not None:
                if existing.configuration_hash != configuration_hash:
                    raise ValueError("Активная user-post campaign несовместима")
                return existing.id
            eligible_count = int(
                await session.scalar(
                    select(func.count(VKUser.vk_id)).where(self.eligible_predicate(now))
                )
                or 0
            )
            fresh_count = int(
                await session.scalar(
                    select(func.count(VKUser.vk_id)).where(
                        self.eligible_predicate(now), VKUser.vk_id.in_(self.resolved_ids(now))
                    )
                )
                or 0
            )
            eligible_due = max(0, eligible_count - fresh_count)
            snapshot_limit = self._settings.collection_user_posts_snapshot_user_limit
            bounded = snapshot_limit is not None
            snapshot_expected = (
                min(eligible_due, snapshot_limit) if snapshot_limit is not None else eligible_count
            )
            snapshot_fresh = 0 if bounded else fresh_count
            snapshot_due = snapshot_expected if bounded else eligible_due
            if (
                gate_evidence.get("eligible_users", eligible_count) != eligible_count
                or gate_evidence.get("eligible_fresh_users", fresh_count) != fresh_count
                or gate_evidence.get("eligible_due_users", eligible_due) != eligible_due
                or gate_evidence.get("snapshot_users") != snapshot_expected
                or gate_evidence.get("fresh_users") != snapshot_fresh
                or gate_evidence.get("due_users") != snapshot_due
            ):
                raise ValueError("Eligible/fresh counts изменились; повторите gate")
            campaign = CollectionCampaign(
                campaign_type=CAMPAIGN_TYPE,
                status=CampaignStatus.RUNNING.value,
                phase=PHASE,
                snapshot_at=now,
                snapshot_max_user_id=0,
                configuration={**configuration, **gate_evidence, "capacity_gate": "passed"},
                configuration_hash=configuration_hash,
            )
            session.add(campaign)
            await session.flush()
            if bounded:
                bounded_ids = (
                    select(VKUser.vk_id.label("user_id"))
                    .where(
                        self.eligible_predicate(now),
                        VKUser.vk_id.not_in(self.resolved_ids(now)),
                    )
                    .order_by(VKUser.vk_id)
                    .limit(snapshot_expected)
                    .subquery()
                )
                snapshot_select = select(literal(campaign.id), bounded_ids.c.user_id)
            else:
                snapshot_select = select(literal(campaign.id), VKUser.vk_id).where(
                    self.eligible_predicate(now)
                )
            await session.execute(
                insert(CollectionCampaignUser)
                .from_select(
                    ["campaign_id", "user_id"],
                    snapshot_select,
                )
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
            campaign.snapshot_max_user_id = int(
                await session.scalar(
                    select(func.coalesce(func.max(CollectionCampaignUser.user_id), 0)).where(
                        CollectionCampaignUser.campaign_id == campaign.id
                    )
                )
                or 0
            )
            if campaign.snapshot_user_count != snapshot_expected:
                raise ValueError("Snapshot изменился при materialization; transaction отменена")
            if await self._plan_cohort(session, campaign) is None:
                campaign.status = CampaignStatus.COMPLETED.value
                campaign.phase = CampaignPhase.COMPLETED.value
                campaign.finished_at = now
            await session.commit()
            return campaign.id

    async def _unresolved_count(self, session: AsyncSession, campaign: CollectionCampaign) -> int:
        return int(
            await session.scalar(
                select(func.count(CollectionCampaignUser.user_id)).where(
                    CollectionCampaignUser.campaign_id == campaign.id,
                    CollectionCampaignUser.user_id.not_in(self.resolved_ids(campaign.snapshot_at)),
                )
            )
            or 0
        )

    async def _budget_available(self, session: AsyncSession, campaign: CollectionCampaign) -> bool:
        initial = campaign.configuration.get("current_database_bytes")
        projected = campaign.configuration.get("projected_final_database_bytes")
        due = campaign.configuration.get("due_users")
        payload_growth = campaign.configuration.get("payload_projected_growth_bytes")
        if not all(
            isinstance(v, int) and v >= 0 for v in (initial, projected, due, payload_growth)
        ):
            self._pause_capacity(campaign, "Aggregate user-post evidence повреждён")
            return False
        assert isinstance(initial, int)
        assert isinstance(projected, int)
        assert isinstance(due, int)
        assert isinstance(payload_growth, int)
        current = int(
            await session.scalar(select(func.pg_database_size(func.current_database()))) or 0
        )
        unresolved = await self._unresolved_count(session, campaign)
        # Snapshot rows have already been materialized before this recheck. Scale only the
        # reserved post/state/job payload, otherwise every next cohort counts the snapshot
        # twice and can pause a campaign whose aggregate gate still has enough headroom.
        remaining = math.ceil(payload_growth * unresolved / due) if due else 0
        disk = inspect_disk(
            self._settings.collection_export_dir,
            self._settings.disk_warning_percent,
            self._settings.disk_stop_percent,
            min_free_bytes=self._settings.collection_disk_min_free_bytes,
        )
        required_final = current + remaining
        projected_used = (
            100.0 * (disk.total_bytes - disk.free_bytes + remaining) / disk.total_bytes
            if disk.total_bytes
            else 100.0
        )
        if (
            required_final > projected
            or required_final > self._settings.collection_safe_database_limit_bytes
            or remaining > max(0, disk.free_bytes - self._settings.collection_disk_min_free_bytes)
            or disk.warning
            or disk.stop
            or projected_used >= self._settings.disk_warning_percent
        ):
            self._pause_capacity(
                campaign,
                f"Следующий user-post cohort отклонён: required_final={required_final}, "
                f"evidence_limit={projected}, disk_free={disk.free_bytes}",
            )
            return False
        campaign.configuration = {
            **campaign.configuration,
            "last_capacity_recheck": {
                "checked_at": datetime.now(UTC).isoformat(),
                "remaining_users": unresolved,
                "remaining_projected_growth_bytes": remaining,
                "current_database_bytes": current,
                "disk_free_bytes": disk.free_bytes,
            },
        }
        return True

    @staticmethod
    def _pause_capacity(campaign: CollectionCampaign, message: str) -> None:
        campaign.status = CampaignStatus.PAUSED_CAPACITY_LIMIT.value
        campaign.error_message = message
        campaign.next_wakeup_at = None

    async def _plan_cohort(
        self, session: AsyncSession, campaign: CollectionCampaign
    ) -> uuid.UUID | None:
        if campaign.last_planned_user_id > 0 and not await self._budget_available(
            session, campaign
        ):
            return None
        ids = list(
            (
                await session.scalars(
                    select(CollectionCampaignUser.user_id)
                    .where(
                        CollectionCampaignUser.campaign_id == campaign.id,
                        CollectionCampaignUser.user_id > campaign.last_planned_user_id,
                        CollectionCampaignUser.user_id.not_in(
                            self.resolved_ids(campaign.snapshot_at)
                        ),
                    )
                    .order_by(CollectionCampaignUser.user_id)
                    .limit(self._settings.collection_campaign_cohort_users)
                )
            ).all()
        )
        if not ids:
            return None
        run = CollectionRun(
            campaign_id=campaign.id,
            scope=RUN_SCOPE,
            status=CollectionRunStatus.PLANNED,
            total_jobs=len(ids),
            configuration={
                "campaign_id": str(campaign.id),
                "phase": PHASE,
                "capacity_gate": "passed",
                "cohort_first_user_id": ids[0],
                "cohort_last_user_id": ids[-1],
                "user_count": len(ids),
                "collection": campaign.configuration["collection"],
                "capacity_evidence": {
                    key: campaign.configuration[key]
                    for key in (
                        "current_database_bytes",
                        "projected_final_database_bytes",
                        "aggregate_projected_growth_bytes",
                        "safe_database_limit_bytes",
                    )
                },
                **(
                    {"capacity_report": campaign.configuration["capacity_report"]}
                    if "capacity_report" in campaign.configuration
                    else {}
                ),
                **(
                    {"verified_backup": campaign.configuration["verified_backup"]}
                    if "verified_backup" in campaign.configuration
                    else {}
                ),
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
                            "job_type": "collect_user_posts",
                            "entity_type": "user",
                            "entity_id": user_id,
                            "priority": 45,
                        }
                        for user_id in ids[start : start + 1000]
                    ]
                )
                .on_conflict_do_nothing(constraint="uq_collection_jobs_run_type_entity")
            )
        campaign.last_planned_user_id = ids[-1]
        campaign.status = CampaignStatus.RUNNING.value
        campaign.error_message = None
        return run.id

    async def reconcile(self, campaign_id: uuid.UUID) -> None:
        async with self._sessions() as session:
            campaign = await session.get(CollectionCampaign, campaign_id, with_for_update=True)
            if campaign is None or campaign.campaign_type != CAMPAIGN_TYPE:
                return
            failed = int(
                await session.scalar(
                    select(func.count(CollectionJob.id))
                    .join(CollectionRun, CollectionRun.id == CollectionJob.collection_run_id)
                    .where(
                        CollectionRun.campaign_id == campaign_id,
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
            else:
                if campaign.status not in (
                    CampaignStatus.PLANNED.value,
                    CampaignStatus.RUNNING.value,
                ):
                    await session.commit()
                    return
                immediate = int(
                    await session.scalar(
                        select(func.count(CollectionJob.id))
                        .join(CollectionRun, CollectionRun.id == CollectionJob.collection_run_id)
                        .where(
                            CollectionRun.campaign_id == campaign_id,
                            CollectionJob.status.in_(IMMEDIATE_JOB_STATUSES),
                        )
                    )
                    or 0
                )
                if not immediate:
                    deferred = int(
                        await session.scalar(
                            select(func.count(CollectionJob.id))
                            .join(
                                CollectionRun,
                                CollectionRun.id == CollectionJob.collection_run_id,
                            )
                            .where(
                                CollectionRun.campaign_id == campaign_id,
                                CollectionJob.status == JobStatus.RETRY_WAIT,
                            )
                        )
                        or 0
                    )
                    unresolved = await self._unresolved_count(session, campaign)
                    planned = await self._plan_cohort(session, campaign) if unresolved else None
                    if planned is None and campaign.status != CampaignStatus.PAUSED_CAPACITY_LIMIT:
                        if deferred:
                            campaign.status = CampaignStatus.RUNNING.value
                            campaign.error_message = (
                                f"Ожидаются отложенные повторы: {deferred}; "
                                f"неразрешённых пользователей: {unresolved}"
                            )
                        elif unresolved:
                            campaign.status = CampaignStatus.FAILED.value
                            campaign.phase = CampaignPhase.FAILED.value
                            campaign.error_message = (
                                f"Остались unresolved пользователи без runnable jobs: {unresolved}"
                            )
                            campaign.finished_at = datetime.now(UTC)
                        else:
                            campaign.status = CampaignStatus.COMPLETED.value
                            campaign.phase = CampaignPhase.COMPLETED.value
                            campaign.finished_at = datetime.now(UTC)
                            campaign.next_wakeup_at = None
            await session.commit()

    async def change_status(self, campaign_id: uuid.UUID, *, pause: bool) -> None:
        async with self._sessions() as session:
            campaign = await session.get(CollectionCampaign, campaign_id, with_for_update=True)
            if campaign is None or campaign.campaign_type != CAMPAIGN_TYPE:
                raise ValueError("User-post campaign не найдена")
            campaign.status = CampaignStatus.PAUSED.value if pause else CampaignStatus.RUNNING.value
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
                .values(status=CollectionRunStatus.PAUSED if pause else CollectionRunStatus.RUNNING)
            )
            await session.execute(
                update(CollectionJob)
                .where(
                    CollectionJob.collection_run_id.in_(
                        select(CollectionRun.id).where(CollectionRun.campaign_id == campaign_id)
                    ),
                    CollectionJob.status.in_([JobStatus.PENDING, JobStatus.PAUSED]),
                    *(
                        (
                            or_(
                                CollectionJob.last_error_type.is_(None),
                                CollectionJob.last_error_type != "tokens_unavailable",
                            ),
                        )
                        if not pause
                        else ()
                    ),
                )
                .values(status=JobStatus.PAUSED if pause else JobStatus.PENDING)
            )
            await session.commit()
        if not pause:
            await self.reconcile(campaign_id)

    async def status(self, campaign_id: uuid.UUID | None = None) -> dict[str, object]:
        async with self._sessions() as session:
            campaign = (
                await session.get(CollectionCampaign, campaign_id)
                if campaign_id
                else await session.scalar(
                    select(CollectionCampaign)
                    .where(CollectionCampaign.campaign_type == CAMPAIGN_TYPE)
                    .order_by(CollectionCampaign.created_at.desc())
                    .limit(1)
                )
            )
            if campaign is None:
                return {"campaign_id": None, "status": "absent"}
            unresolved = await self._unresolved_count(session, campaign)
            jobs = (
                await session.execute(
                    select(CollectionJob.status, func.count(CollectionJob.id))
                    .join(CollectionRun, CollectionRun.id == CollectionJob.collection_run_id)
                    .where(CollectionRun.campaign_id == campaign.id)
                    .group_by(CollectionJob.status)
                )
            ).all()
            raw_recheck = campaign.configuration.get("last_capacity_recheck")
            recheck = raw_recheck if isinstance(raw_recheck, dict) else {}
            return {
                "campaign_id": str(campaign.id),
                "campaign_type": campaign.campaign_type,
                "status": campaign.status,
                "phase": campaign.phase,
                "snapshot_at": campaign.snapshot_at.isoformat(),
                "snapshot_users": campaign.snapshot_user_count,
                "resolved_users": max(0, campaign.snapshot_user_count - unresolved),
                "unresolved_users": unresolved,
                "coverage_percent": round(
                    100.0
                    * (campaign.snapshot_user_count - unresolved)
                    / campaign.snapshot_user_count,
                    2,
                )
                if campaign.snapshot_user_count
                else 100.0,
                "jobs": {status.value: int(count) for status, count in jobs},
                "remaining_projected_growth_bytes": recheck.get("remaining_projected_growth_bytes"),
                "rejection_reasons": campaign.configuration.get("rejection_reasons", []),
                "next_wakeup_at": campaign.next_wakeup_at.isoformat()
                if campaign.next_wakeup_at
                else None,
                "error_message": campaign.error_message,
            }

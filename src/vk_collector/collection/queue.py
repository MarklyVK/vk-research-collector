from __future__ import annotations

import hashlib
import json
import random
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import and_, exists, func, literal, or_, select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from vk_collector.collection.notifications import notify
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
    GroupLabel,
    GroupMembership,
    JobStatus,
    UserGroupSubscription,
    UserSubscriptionState,
    VKCommunity,
    VKUser,
)
from vk_collector.subjects import SUBJECT_NAMES


@dataclass(frozen=True, slots=True)
class PlanPreview:
    approved_groups: int
    selected_groups: int
    scopes: tuple[str, ...]
    jobs: int
    estimated_requests: int
    estimated_disk_growth_bytes: int = 0
    warnings: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class ClaimedJob:
    id: uuid.UUID
    run_id: uuid.UUID
    job_type: str
    entity_type: str
    entity_id: int
    checkpoint: dict[str, Any]
    attempt_count: int


class CollectionQueue:
    """PostgreSQL queue с lease, SKIP LOCKED и идемпотентным plan."""

    def __init__(self, sessions: async_sessionmaker[AsyncSession], settings: Settings) -> None:
        self._sessions = sessions
        self._settings = settings

    async def approved_group_ids(self) -> list[int]:
        async with self._sessions() as session:
            return list(
                (
                    await session.scalars(
                        select(GroupCandidate.id)
                        .where(
                            GroupCandidate.classification_status == ClassificationStatus.APPROVED
                        )
                        .order_by(GroupCandidate.id)
                    )
                ).all()
            )

    async def pilot_group_ids(self) -> list[int]:
        async with self._sessions() as session:
            rows = (
                await session.execute(
                    select(GroupCandidate.id, GroupLabel.label)
                    .join(GroupLabel, GroupLabel.group_id == GroupCandidate.id)
                    .where(GroupCandidate.classification_status == ClassificationStatus.APPROVED)
                    .order_by(GroupCandidate.id)
                )
            ).all()
        labels: dict[int, set[str]] = {}
        for group_id, label in rows:
            labels.setdefault(group_id, set()).add(label)
        rng = random.Random(self._settings.collection_pilot_seed)
        selected: set[int] = set()
        for label in SUBJECT_NAMES:
            candidates = sorted(group_id for group_id, values in labels.items() if label in values)
            rng.shuffle(candidates)
            selected.update(candidates[: self._settings.collection_pilot_groups_per_category])
        multi = sorted(group_id for group_id, values in labels.items() if len(values) > 1)
        rng.shuffle(multi)
        selected.update(multi[:5])
        return sorted(selected)

    def enabled_scopes(self) -> tuple[str, ...]:
        scopes = ["groups"]
        if self._settings.collection_posts_enabled:
            scopes.append("posts")
        if self._settings.collection_members_enabled:
            scopes.append("members")
        if self._settings.collection_users_enabled:
            scopes.append("users")
        return tuple(scopes)

    def collection_configuration(self) -> dict[str, object]:
        """Вернуть параметры, влияющие на объём и семантику сбора."""
        return {
            "scopes": list(self.enabled_scopes()),
            "posts_max_per_group": self._settings.collection_posts_max_per_group,
            "posts_page_size": self._settings.collection_posts_page_size,
            "posts_stop_at_date": self._settings.collection_posts_stop_at_date,
            "members_max_per_group": self._settings.collection_members_max_per_group,
            "members_page_size": self._settings.collection_members_page_size,
            "user_profile_ttl_days": self._settings.collection_user_profile_ttl_days,
            "user_batch_size": self._settings.collection_user_batch_size,
            "subscriptions_max_per_user": (self._settings.collection_subscriptions_max_per_user),
            "subscriptions_page_size": self._settings.collection_subscriptions_page_size,
            "subscriptions_users_per_run": self._settings.collection_subscriptions_users_per_run,
            "subscriptions_ttl_days": self._settings.collection_subscriptions_ttl_days,
            "campaign_cohort_users": self._settings.collection_campaign_cohort_users,
            "community_metadata_batch_size": (
                self._settings.collection_community_metadata_batch_size
            ),
            "community_metadata_ttl_days": (self._settings.collection_community_metadata_ttl_days),
            "subscription_pilot_users": self._settings.collection_subscription_pilot_users,
            "subscription_pilot_min_users": (
                self._settings.collection_subscription_pilot_min_users
            ),
            "subscriptions_enabled": self._settings.collection_subscriptions_enabled,
            "subscription_posts_enabled": (
                self._settings.collection_subscription_group_posts_enabled
            ),
            "subscription_posts_max": self._settings.collection_subscription_group_posts_max,
            "subscription_posts_ttl_days": (
                self._settings.collection_subscription_group_posts_ttl_days
            ),
            "subscription_posts_pilot_communities": (
                self._settings.collection_subscription_posts_pilot_communities
            ),
            "subscription_posts_pilot_min_communities": (
                self._settings.collection_subscription_posts_pilot_min_communities
            ),
        }

    async def incremental_group_ids(self, baseline_run_id: uuid.UUID) -> list[int]:
        """Вернуть approved-группы вне неизменяемого snapshot основного run."""
        async with self._sessions() as session:
            baseline = await session.get(CollectionRun, baseline_run_id)
            if baseline is None:
                raise ValueError("Базовый collection run не найден")
            baseline_ids = select(CollectionJob.entity_id).where(
                CollectionJob.collection_run_id == baseline_run_id,
                CollectionJob.entity_type == "group",
            )
            return list(
                (
                    await session.scalars(
                        select(GroupCandidate.id)
                        .where(
                            GroupCandidate.classification_status == ClassificationStatus.APPROVED,
                            GroupCandidate.id.not_in(baseline_ids),
                        )
                        .order_by(GroupCandidate.id)
                    )
                ).all()
            )

    async def _estimated_incremental_bytes(
        self, baseline_run_id: uuid.UUID, selected_groups: int
    ) -> int:
        async with self._sessions() as session:
            baseline = await session.get(CollectionRun, baseline_run_id)
            if baseline is None:
                raise ValueError("Базовый collection run не найден")
            projected = baseline.configuration.get("projected_database_bytes")
            group_count = baseline.configuration.get("group_count")
            if isinstance(projected, int) and isinstance(group_count, int) and group_count > 0:
                return int(projected / group_count * selected_groups)
        return selected_groups * 512 * 1024

    async def preview(
        self, *, pilot: bool = False, incremental_from: uuid.UUID | None = None
    ) -> PlanPreview:
        all_ids = await self.approved_group_ids()
        ids = (
            await self.incremental_group_ids(incremental_from)
            if incremental_from is not None
            else await self.pilot_group_ids()
            if pilot
            else all_ids
        )
        scopes = self.enabled_scopes()
        initial = sum(scope in scopes for scope in ("groups", "posts", "members"))
        jobs = len(ids) * initial
        post_pages = (
            (self._settings.collection_posts_max_per_group - 1)
            // self._settings.collection_posts_page_size
            + 1
            if "posts" in scopes
            else 0
        )
        member_limit = self._settings.collection_members_max_per_group or 1000
        member_pages = (
            (member_limit - 1) // self._settings.collection_members_page_size + 1
            if "members" in scopes
            else 0
        )
        requests = len(ids) * (int("groups" in scopes) + post_pages + member_pages)
        warnings = []
        if "subscriptions" in scopes:
            warnings.append("Подписки требуют отдельного capacity gate.")
        if not pilot:
            warnings.append("Full plan разрешён только после успешного pilot capacity gate.")
        estimated_disk = (
            await self._estimated_incremental_bytes(incremental_from, len(ids))
            if incremental_from is not None
            else 0
        )
        if incremental_from is not None:
            warnings = ["Incremental plan разрешён только после успешного аудита и capacity gate."]
        return PlanPreview(
            len(all_ids), len(ids), scopes, jobs, requests, estimated_disk, tuple(warnings)
        )

    async def plan(
        self,
        *,
        pilot: bool = False,
        incremental_from: uuid.UUID | None = None,
        reason: str | None = None,
        source: str | None = None,
        capacity_passed: bool = False,
        estimated_disk_growth_bytes: int = 0,
    ) -> uuid.UUID:
        ids = (
            await self.incremental_group_ids(incremental_from)
            if incremental_from is not None
            else await self.pilot_group_ids()
            if pilot
            else await self.approved_group_ids()
        )
        scopes = self.enabled_scopes()
        collection_configuration = self.collection_configuration()
        key_payload = {
            "pilot": pilot,
            "incremental_from": str(incremental_from) if incremental_from else None,
            "group_ids": ids,
            "collection": collection_configuration,
        }
        plan_key = hashlib.sha256(
            json.dumps(key_payload, sort_keys=True).encode("utf-8")
        ).hexdigest()
        async with self._sessions() as session:
            reusable_statuses = [
                CollectionRunStatus.PLANNED,
                CollectionRunStatus.PAUSED_CAPACITY_LIMIT,
            ]
            if incremental_from is not None:
                reusable_statuses.extend(
                    [
                        CollectionRunStatus.RUNNING,
                        CollectionRunStatus.COMPLETED,
                        CollectionRunStatus.COMPLETED_WITH_ERRORS,
                    ]
                )
            existing = await session.scalar(
                select(CollectionRun).where(
                    CollectionRun.status.in_(reusable_statuses),
                    CollectionRun.configuration["plan_key"].astext == plan_key,
                )
            )
            if existing is not None:
                return existing.id
            run = CollectionRun(
                scope=("incremental" if incremental_from else "pilot" if pilot else "full"),
                status=(
                    CollectionRunStatus.PLANNED
                    if incremental_from is None or capacity_passed
                    else CollectionRunStatus.PAUSED_CAPACITY_LIMIT
                ),
                configuration={
                    "plan_key": plan_key,
                    "pilot": pilot,
                    "scopes": list(scopes),
                    "group_count": len(ids),
                    "collection": collection_configuration,
                    **(
                        {
                            "baseline_run_id": str(incremental_from),
                            "reason": reason or "food_service_increment",
                            "source": source or "food_service_expansion",
                            "capacity_gate": "passed" if capacity_passed else "failed",
                            "estimated_disk_growth_bytes": estimated_disk_growth_bytes,
                        }
                        if incremental_from
                        else {}
                    ),
                },
            )
            session.add(run)
            await session.flush()
            specs = []
            mapping = {
                "groups": "refresh_group",
                "posts": "collect_group_posts",
                "members": "collect_group_members",
            }
            completed_specs: set[tuple[int, str]] = set()
            if incremental_from is not None and ids:
                completed_rows = (
                    await session.execute(
                        select(CollectionJob.entity_id, CollectionJob.job_type).where(
                            CollectionJob.entity_type == "group",
                            CollectionJob.entity_id.in_(ids),
                            CollectionJob.status == JobStatus.COMPLETED,
                        )
                    )
                ).all()
                completed_specs = {(entity_id, job_type) for entity_id, job_type in completed_rows}
            for group_id in ids:
                for scope, job_type in mapping.items():
                    if scope in scopes and (group_id, job_type) not in completed_specs:
                        specs.append(
                            {
                                "collection_run_id": run.id,
                                "job_type": job_type,
                                "entity_type": "group",
                                "entity_id": group_id,
                                "priority": {"groups": 10, "posts": 20, "members": 30}[scope],
                            }
                        )
            for start in range(0, len(specs), 1000):
                await session.execute(
                    insert(CollectionJob)
                    .values(specs[start : start + 1000])
                    .on_conflict_do_nothing(constraint="uq_collection_jobs_run_type_entity")
                )
            run.total_jobs = len(specs)
            await session.commit()
            return run.id

    async def light_repair_preview(self) -> dict[str, object]:
        """Read-only canonical gaps safe for users.get/groups.getById repair."""
        now = datetime.now(UTC)
        profile_stale = now - timedelta(days=self._settings.collection_user_profile_ttl_days)
        metadata_stale = now - timedelta(days=self._settings.collection_community_metadata_ttl_days)
        group_due = or_(
            VKCommunity.vk_id.is_(None),
            VKCommunity.metadata_updated_at.is_(None),
            VKCommunity.metadata_updated_at < metadata_stale,
        )
        user_due = and_(
            VKUser.deactivated.is_(None),
            or_(VKUser.is_closed.is_(False), VKUser.can_access_closed.is_(True)),
            or_(
                VKUser.profile_updated_at.is_(None),
                VKUser.profile_updated_at < profile_stale,
            ),
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
        async with self._sessions() as session:
            group_count = int(
                await session.scalar(
                    select(func.count(GroupCandidate.id))
                    .outerjoin(VKCommunity, VKCommunity.vk_id == GroupCandidate.vk_id)
                    .where(
                        GroupCandidate.classification_status == ClassificationStatus.APPROVED,
                        or_(VKCommunity.vk_id.is_(None), VKCommunity.deactivated.is_(None)),
                        group_due,
                    )
                )
                or 0
            )
            user_count = int(
                await session.scalar(select(func.count(VKUser.vk_id)).where(user_due)) or 0
            )
        group_requests = (group_count + 99) // 100
        user_requests = (user_count + self._settings.collection_user_batch_size - 1) // (
            self._settings.collection_user_batch_size
        )
        return {
            "approved_group_metadata_gaps": group_count,
            "user_profile_gaps": user_count,
            "distinct_entities": group_count + user_count,
            "estimated_api_requests": group_requests + user_requests,
            "estimated_database_growth_bytes": (group_count + user_count) * 512,
            "excluded_terminal_entities": True,
            "historical_job_rows_are_not_gaps": True,
        }

    async def plan_light_repair(self) -> uuid.UUID:
        """Create or reuse an immutable explicitly-authorized light repair run."""
        preview = await self.light_repair_preview()
        raw_group_count = preview["approved_group_metadata_gaps"]
        raw_user_count = preview["user_profile_gaps"]
        if not isinstance(raw_group_count, int) or not isinstance(raw_user_count, int):
            raise ValueError("Light-repair preview повреждён")
        group_count = raw_group_count
        user_count = raw_user_count
        payload = {
            "scope": "light_repair",
            "profile_ttl_days": self._settings.collection_user_profile_ttl_days,
            "metadata_ttl_days": self._settings.collection_community_metadata_ttl_days,
        }
        plan_key = hashlib.sha256(
            json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()
        async with self._sessions() as session:
            existing = await session.scalar(
                select(CollectionRun)
                .where(
                    CollectionRun.scope == "light_repair",
                    CollectionRun.status.in_(
                        [
                            CollectionRunStatus.PLANNED,
                            CollectionRunStatus.RUNNING,
                            CollectionRunStatus.WAITING_METHOD_LIMIT,
                            CollectionRunStatus.PAUSED,
                        ]
                    ),
                    CollectionRun.configuration["plan_key"].astext == plan_key,
                )
                .order_by(CollectionRun.created_at.desc())
                .limit(1)
            )
            if existing is not None:
                return existing.id
            run = CollectionRun(
                scope="light_repair",
                status=CollectionRunStatus.PLANNED,
                configuration={
                    "plan_key": plan_key,
                    "capacity_gate": "passed",
                    "light_repair": True,
                    "allowed_job_types": [
                        "refresh_community_metadata",
                        "refresh_user_profile",
                    ],
                    "collection": self.collection_configuration(),
                },
            )
            session.add(run)
            await session.flush()
            now = datetime.now(UTC)
            metadata_stale = now - timedelta(
                days=self._settings.collection_community_metadata_ttl_days
            )
            profile_stale = now - timedelta(days=self._settings.collection_user_profile_ttl_days)
            group_select = (
                select(
                    literal(run.id),
                    literal("refresh_community_metadata"),
                    literal("vk_community"),
                    GroupCandidate.vk_id,
                    literal(40),
                )
                .outerjoin(VKCommunity, VKCommunity.vk_id == GroupCandidate.vk_id)
                .where(
                    GroupCandidate.classification_status == ClassificationStatus.APPROVED,
                    or_(VKCommunity.vk_id.is_(None), VKCommunity.deactivated.is_(None)),
                    or_(
                        VKCommunity.vk_id.is_(None),
                        VKCommunity.metadata_updated_at.is_(None),
                        VKCommunity.metadata_updated_at < metadata_stale,
                    ),
                )
            )
            user_select = select(
                literal(run.id),
                literal("refresh_user_profile"),
                literal("user"),
                VKUser.vk_id,
                literal(50),
            ).where(
                VKUser.deactivated.is_(None),
                or_(VKUser.is_closed.is_(False), VKUser.can_access_closed.is_(True)),
                or_(
                    VKUser.profile_updated_at.is_(None),
                    VKUser.profile_updated_at < profile_stale,
                ),
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
            columns = [
                "collection_run_id",
                "job_type",
                "entity_type",
                "entity_id",
                "priority",
            ]
            await session.execute(
                insert(CollectionJob)
                .from_select(columns, group_select)
                .on_conflict_do_nothing(constraint="uq_collection_jobs_run_type_entity")
            )
            await session.execute(
                insert(CollectionJob)
                .from_select(columns, user_select)
                .on_conflict_do_nothing(constraint="uq_collection_jobs_run_type_entity")
            )
            run.total_jobs = group_count + user_count
            if not run.total_jobs:
                run.status = CollectionRunStatus.COMPLETED
                run.finished_at = datetime.now(UTC)
            await session.commit()
            return run.id

    async def recover_expired(self, run_id: uuid.UUID) -> int:
        cutoff = datetime.now(UTC) - timedelta(seconds=self._settings.collection_job_lease_seconds)
        async with self._sessions() as session:
            result = await session.execute(
                update(CollectionJob)
                .where(
                    CollectionJob.collection_run_id == run_id,
                    CollectionJob.status == JobStatus.RUNNING,
                    CollectionJob.locked_at < cutoff,
                )
                .values(
                    status=JobStatus.PENDING,
                    attempt_count=func.greatest(CollectionJob.attempt_count - 1, 0),
                    locked_at=None,
                    locked_by=None,
                    heartbeat_at=None,
                    last_error_type="lease_expired_recovered",
                    last_error_message="Истёк lease; job безопасно возвращён в pending",
                )
            )
            await session.commit()
            return int(result.rowcount or 0)  # type: ignore[attr-defined]

    async def claim(
        self,
        run_id: uuid.UUID,
        *,
        scope: str | None = None,
        job_type: str | None = None,
    ) -> ClaimedJob | None:
        now = datetime.now(UTC)
        selected_job_type = job_type or {
            "groups": "refresh_group",
            "posts": "collect_group_posts",
            "members": "collect_group_members",
            "users": "refresh_user_profile",
            "subscriptions": "collect_user_subscriptions",
            "metadata": "refresh_community_metadata",
            "subscription_posts": "collect_subscription_group_posts",
        }.get(scope or "")
        async with self._sessions() as session:
            query = (
                select(CollectionJob)
                .where(
                    CollectionJob.collection_run_id == run_id,
                    CollectionJob.status.in_([JobStatus.PENDING, JobStatus.RETRY_WAIT]),
                    or_(
                        CollectionJob.next_attempt_at.is_(None),
                        CollectionJob.next_attempt_at <= now,
                    ),
                )
                .order_by(CollectionJob.priority, CollectionJob.created_at)
                .limit(1)
                .with_for_update(skip_locked=True)
            )
            if selected_job_type:
                query = query.where(CollectionJob.job_type == selected_job_type)
            job = await session.scalar(query)
            if job is None:
                return None
            job.status = JobStatus.RUNNING
            job.locked_at = now
            job.heartbeat_at = now
            job.locked_by = self._settings.collection_worker_id
            job.started_at = job.started_at or now
            job.attempt_count += 1
            run = await session.get(CollectionRun, run_id)
            if run is not None and run.status in {
                CollectionRunStatus.PLANNED,
                CollectionRunStatus.PAUSED,
                CollectionRunStatus.PAUSED_NO_TOKENS,
                CollectionRunStatus.WAITING_METHOD_LIMIT,
            }:
                run.status = CollectionRunStatus.RUNNING
                run.started_at = run.started_at or now
            await session.commit()
            return ClaimedJob(
                job.id,
                job.collection_run_id,
                job.job_type,
                job.entity_type,
                job.entity_id,
                dict(job.checkpoint),
                job.attempt_count,
            )

    async def plan_subscriptions(self, *, pilot: bool = False) -> uuid.UUID:
        """Идемпотентно спланировать enrichment уже существующих доступных users."""
        now = datetime.now(UTC)
        cutoff = now - timedelta(days=self._settings.collection_subscriptions_ttl_days)
        limit = (
            self._settings.collection_subscription_pilot_users
            if pilot
            else self._settings.collection_subscriptions_users_per_run
        )
        async with self._sessions() as session:
            fresh = select(UserSubscriptionState.user_id).where(
                or_(
                    UserSubscriptionState.next_scheduled_at > now,
                    UserSubscriptionState.last_success_at >= cutoff,
                )
            )
            ids = list(
                (
                    await session.scalars(
                        select(VKUser.vk_id)
                        .join(GroupMembership, GroupMembership.user_id == VKUser.vk_id)
                        .join(GroupCandidate, GroupCandidate.id == GroupMembership.group_id)
                        .where(
                            GroupCandidate.classification_status == ClassificationStatus.APPROVED,
                            GroupMembership.is_current.is_(True),
                            VKUser.deactivated.is_(None),
                            or_(
                                VKUser.is_closed.is_(False),
                                VKUser.can_access_closed.is_(True),
                            ),
                            VKUser.vk_id.not_in(fresh),
                        )
                        .distinct()
                        .order_by(VKUser.vk_id)
                        .limit(limit)
                    )
                ).all()
            )
            id_hash = hashlib.sha256(
                ",".join(str(value) for value in ids).encode("ascii")
            ).hexdigest()
            collection_configuration = self.collection_configuration()
            configuration = {
                "plan_key": hashlib.sha256(
                    json.dumps(
                        {
                            "user_hash": id_hash,
                            "pilot": pilot,
                            "collection": collection_configuration,
                        },
                        sort_keys=True,
                    ).encode()
                ).hexdigest(),
                "pilot": pilot,
                "phase": "A",
                "user_ids_hash": id_hash,
                "user_count": len(ids),
                "collection": collection_configuration,
                "capacity_gate": "pilot_required" if not pilot else "pilot",
            }
            existing_query = select(CollectionRun).where(
                CollectionRun.configuration["plan_key"].astext == configuration["plan_key"]
            )
            if ids:
                existing_query = existing_query.where(
                    CollectionRun.status.in_(
                        [
                            CollectionRunStatus.PLANNED,
                            CollectionRunStatus.RUNNING,
                            CollectionRunStatus.PAUSED,
                            CollectionRunStatus.PAUSED_NO_TOKENS,
                            CollectionRunStatus.PAUSED_CAPACITY_LIMIT,
                            CollectionRunStatus.WAITING_METHOD_LIMIT,
                        ]
                    )
                )
            existing = await session.scalar(
                existing_query.order_by(CollectionRun.created_at.desc()).limit(1)
            )
            if existing is not None:
                return existing.id
            run = CollectionRun(
                scope="subscriptions_pilot" if pilot else "subscriptions",
                status=(
                    CollectionRunStatus.COMPLETED
                    if not ids
                    else CollectionRunStatus.PLANNED
                    if pilot
                    else CollectionRunStatus.PAUSED_CAPACITY_LIMIT
                ),
                configuration=configuration,
                finished_at=now if not ids else None,
                error_message="Нет подходящих пользователей; запуск завершён без API-вызовов"
                if not ids
                else None,
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
                                "priority": 50,
                            }
                            for user_id in ids[start : start + 1000]
                        ]
                    )
                    .on_conflict_do_nothing(constraint="uq_collection_jobs_run_type_entity")
                )
            run.total_jobs = len(ids)
            await session.commit()
            return run.id

    async def plan_subscription_posts(
        self,
        *,
        pilot: bool = False,
        source_run_id: uuid.UUID | None = None,
    ) -> uuid.UUID:
        """Создать отдельный неизменяемый run последних 20 постов communities."""
        now = datetime.now(UTC)
        async with self._sessions() as session:
            if pilot and source_run_id is None:
                source_run_id = await session.scalar(
                    select(CollectionRun.id)
                    .where(
                        CollectionRun.scope == "subscriptions_pilot",
                        CollectionRun.status == CollectionRunStatus.COMPLETED,
                    )
                    .order_by(CollectionRun.created_at.desc())
                    .limit(1)
                )
            if not pilot and source_run_id is None:
                raise ValueError(
                    "Для production posts нужен завершённый production subscriptions run"
                )
            if source_run_id is not None:
                source_run = await session.get(CollectionRun, source_run_id)
                expected_scope = "subscriptions_pilot" if pilot else "subscriptions"
                if (
                    source_run is None
                    or source_run.scope != expected_scope
                    or source_run.status != CollectionRunStatus.COMPLETED
                ):
                    label = "Pilot B" if pilot else "Production posts"
                    raise ValueError(
                        f"{label} принимает только завершённый run scope={expected_scope}"
                    )
            elif pilot:
                raise ValueError("Для Pilot B нужен завершённый Pilot A")
            fresh = select(CommunityPostCollectionState.community_vk_id).where(
                CommunityPostCollectionState.next_scheduled_at > now,
            )
            query = (
                select(VKCommunity.vk_id)
                .join(
                    UserGroupSubscription,
                    UserGroupSubscription.vk_group_id == VKCommunity.vk_id,
                )
                .where(
                    UserGroupSubscription.is_current.is_(True),
                    VKCommunity.deactivated.is_(None),
                    VKCommunity.is_closed.is_(False),
                    VKCommunity.vk_id.not_in(fresh),
                )
                .distinct()
                .order_by(VKCommunity.vk_id)
            )
            if source_run_id is not None:
                query = query.where(UserGroupSubscription.source_run_id == source_run_id)
            if pilot:
                query = query.limit(self._settings.collection_subscription_posts_pilot_communities)
            ids = list((await session.scalars(query)).all())
            configuration = {
                "pilot": pilot,
                "phase": "B",
                "source_run_id": str(source_run_id) if source_run_id else None,
                "community_count": len(ids),
                "collection": self.collection_configuration(),
                "capacity_gate": "pilot" if pilot else "pilot_required",
            }
            configuration["plan_key"] = hashlib.sha256(
                json.dumps({**configuration, "community_ids": ids}, sort_keys=True).encode("utf-8")
            ).hexdigest()
            existing_query = select(CollectionRun).where(
                CollectionRun.configuration["plan_key"].astext == configuration["plan_key"]
            )
            if ids:
                existing_query = existing_query.where(
                    CollectionRun.status.in_(
                        [
                            CollectionRunStatus.PLANNED,
                            CollectionRunStatus.RUNNING,
                            CollectionRunStatus.PAUSED,
                            CollectionRunStatus.PAUSED_NO_TOKENS,
                            CollectionRunStatus.PAUSED_CAPACITY_LIMIT,
                            CollectionRunStatus.WAITING_METHOD_LIMIT,
                        ]
                    )
                )
            existing = await session.scalar(
                existing_query.order_by(CollectionRun.created_at.desc()).limit(1)
            )
            if existing is not None:
                return existing.id
            run = CollectionRun(
                scope="subscription_posts_pilot" if pilot else "subscription_posts",
                status=(
                    CollectionRunStatus.COMPLETED
                    if not ids
                    else CollectionRunStatus.PLANNED
                    if pilot
                    else CollectionRunStatus.PAUSED_CAPACITY_LIMIT
                ),
                configuration=configuration,
                finished_at=now if not ids else None,
                error_message="Нет communities для сбора постов; запуск завершён без API-вызовов"
                if not ids
                else None,
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
                                "job_type": "collect_subscription_group_posts",
                                "entity_type": "community",
                                "entity_id": community_id,
                                "priority": 60,
                            }
                            for community_id in ids[start : start + 1000]
                        ]
                    )
                    .on_conflict_do_nothing(constraint="uq_collection_jobs_run_type_entity")
                )
            run.total_jobs = len(ids)
            await session.commit()
            return run.id

    async def claim_user_batch(self, run_id: uuid.UUID, *, limit: int) -> list[ClaimedJob]:
        """Атомарно захватить дополнительные profile jobs для одного users.get."""
        if limit <= 0:
            return []
        now = datetime.now(UTC)
        async with self._sessions() as session:
            jobs = list(
                (
                    await session.scalars(
                        select(CollectionJob)
                        .where(
                            CollectionJob.collection_run_id == run_id,
                            CollectionJob.job_type == "refresh_user_profile",
                            CollectionJob.status.in_([JobStatus.PENDING, JobStatus.RETRY_WAIT]),
                            or_(
                                CollectionJob.next_attempt_at.is_(None),
                                CollectionJob.next_attempt_at <= now,
                            ),
                        )
                        .order_by(CollectionJob.priority, CollectionJob.created_at)
                        .limit(limit)
                        .with_for_update(skip_locked=True)
                    )
                ).all()
            )
            claimed: list[ClaimedJob] = []
            for job in jobs:
                job.status = JobStatus.RUNNING
                job.locked_at = now
                job.heartbeat_at = now
                job.locked_by = self._settings.collection_worker_id
                job.started_at = job.started_at or now
                job.attempt_count += 1
                claimed.append(
                    ClaimedJob(
                        job.id,
                        job.collection_run_id,
                        job.job_type,
                        job.entity_type,
                        job.entity_id,
                        dict(job.checkpoint),
                        job.attempt_count,
                    )
                )
            await session.commit()
            return claimed

    async def claim_metadata_batch(self, run_id: uuid.UUID, *, limit: int) -> list[ClaimedJob]:
        """Claim extra community metadata jobs for one groups.getById request."""
        if limit <= 0:
            return []
        now = datetime.now(UTC)
        async with self._sessions() as session:
            jobs = list(
                (
                    await session.scalars(
                        select(CollectionJob)
                        .where(
                            CollectionJob.collection_run_id == run_id,
                            CollectionJob.job_type == "refresh_community_metadata",
                            CollectionJob.status.in_([JobStatus.PENDING, JobStatus.RETRY_WAIT]),
                            or_(
                                CollectionJob.next_attempt_at.is_(None),
                                CollectionJob.next_attempt_at <= now,
                            ),
                        )
                        .order_by(CollectionJob.entity_id)
                        .limit(limit)
                        .with_for_update(skip_locked=True)
                    )
                ).all()
            )
            claimed: list[ClaimedJob] = []
            for job in jobs:
                job.status = JobStatus.RUNNING
                job.locked_at = now
                job.heartbeat_at = now
                job.locked_by = self._settings.collection_worker_id
                job.started_at = job.started_at or now
                job.attempt_count += 1
                claimed.append(
                    ClaimedJob(
                        job.id,
                        job.collection_run_id,
                        job.job_type,
                        job.entity_type,
                        job.entity_id,
                        dict(job.checkpoint),
                        job.attempt_count,
                    )
                )
            await session.commit()
            return claimed

    async def release(self, jobs: list[ClaimedJob]) -> None:
        """Вернуть дополнительные batch jobs после неуспешного общего API-запроса."""
        if not jobs:
            return
        async with self._sessions() as session:
            await session.execute(
                update(CollectionJob)
                .where(CollectionJob.id.in_([job.id for job in jobs]))
                .values(
                    status=JobStatus.PENDING,
                    attempt_count=func.greatest(CollectionJob.attempt_count - 1, 0),
                    locked_at=None,
                    locked_by=None,
                    heartbeat_at=None,
                )
            )
            await session.commit()

    async def finish(
        self,
        job_id: uuid.UUID,
        status: JobStatus,
        *,
        error_type: str | None = None,
        error_message: str | None = None,
        retry_at: datetime | None = None,
    ) -> bool:
        """Finish/defer one job and reconcile only on the run's first terminal edge."""
        now = datetime.now(UTC)
        run_became_terminal = False
        campaign_id: uuid.UUID | None = None
        terminal_statuses = {JobStatus.COMPLETED, JobStatus.SKIPPED, JobStatus.FAILED}
        async with self._sessions() as session:
            job = await session.get(CollectionJob, job_id, with_for_update=True)
            if job is None:
                return False
            previous_status = job.status
            if previous_status in terminal_statuses:
                return False
            job.status = status
            job.last_error_type = error_type
            job.last_error_message = error_message
            job.error_message = error_message
            job.next_attempt_at = retry_at
            job.locked_at = None
            job.locked_by = None
            job.heartbeat_at = None
            if status in terminal_statuses:
                job.finished_at = now
                run = await session.get(CollectionRun, job.collection_run_id, with_for_update=True)
                if run is not None:
                    if status == JobStatus.COMPLETED:
                        run.completed_jobs += 1
                    elif status == JobStatus.SKIPPED:
                        run.skipped_jobs += 1
                    else:
                        run.failed_jobs += 1
                    await session.flush()
                    has_active = bool(
                        await session.scalar(
                            select(
                                exists().where(
                                    CollectionJob.collection_run_id == run.id,
                                    CollectionJob.status.in_(
                                        [
                                            JobStatus.PENDING,
                                            JobStatus.RUNNING,
                                            JobStatus.RETRY_WAIT,
                                            JobStatus.PAUSED,
                                        ]
                                    ),
                                )
                            )
                        )
                    )
                    if not has_active and run.total_jobs:
                        run.status = (
                            CollectionRunStatus.COMPLETED_WITH_ERRORS
                            if run.failed_jobs
                            else CollectionRunStatus.COMPLETED
                        )
                        run.finished_at = now
                        run.next_wakeup_at = None
                        run_became_terminal = True
                        campaign_id = run.campaign_id
            await session.commit()
        if run_became_terminal and campaign_id is not None:
            from vk_collector.collection.campaigns import CampaignManager

            await CampaignManager(self._sessions, self._settings).reconcile(campaign_id)
        return run_became_terminal

    async def defer_method(self, job: ClaimedJob, *, retry_at: datetime, message: str) -> None:
        """Отложить job из-за method limit, не расходуя обычную попытку."""
        async with self._sessions() as session:
            current = await session.get(CollectionJob, job.id, with_for_update=True)
            if current is None:
                return
            current.status = JobStatus.RETRY_WAIT
            current.next_attempt_at = retry_at
            current.attempt_count = max(0, current.attempt_count - 1)
            current.last_error_type = "method_limit"
            current.last_error_message = message
            current.locked_at = None
            current.locked_by = None
            current.heartbeat_at = None
            run = await session.get(CollectionRun, job.run_id, with_for_update=True)
            if run is not None:
                run.status = CollectionRunStatus.WAITING_METHOD_LIMIT
                run.next_wakeup_at = (
                    retry_at if run.next_wakeup_at is None else min(run.next_wakeup_at, retry_at)
                )
                run.error_message = message
                if run.campaign_id is not None:
                    campaign = await session.get(
                        CollectionCampaign, run.campaign_id, with_for_update=True
                    )
                    if campaign is not None:
                        campaign.status = CampaignStatus.WAITING_METHOD_LIMIT.value
                        campaign.next_wakeup_at = (
                            retry_at
                            if campaign.next_wakeup_at is None
                            else min(campaign.next_wakeup_at, retry_at)
                        )
                        campaign.error_message = message
            await session.commit()

    async def pending_job_types(self, run_id: uuid.UUID) -> set[str]:
        """Вернуть типы готовых pending/retry jobs без claim."""
        now = datetime.now(UTC)
        async with self._sessions() as session:
            return set(
                (
                    await session.scalars(
                        select(CollectionJob.job_type)
                        .where(
                            CollectionJob.collection_run_id == run_id,
                            CollectionJob.status.in_([JobStatus.PENDING, JobStatus.RETRY_WAIT]),
                            or_(
                                CollectionJob.next_attempt_at.is_(None),
                                CollectionJob.next_attempt_at <= now,
                            ),
                        )
                        .distinct()
                    )
                ).all()
            )

    async def wait_for_methods(self, run_id: uuid.UUID, retry_at: datetime) -> None:
        """Зафиксировать ближайшее автоматическое пробуждение run."""
        async with self._sessions() as session:
            run = await session.get(CollectionRun, run_id, with_for_update=True)
            if run is not None:
                run.status = CollectionRunStatus.WAITING_METHOD_LIMIT
                run.next_wakeup_at = retry_at
                run.error_message = "Все готовые VK methods временно ограничены"
            await session.commit()

    async def refresh_run(self, run_id: uuid.UUID) -> None:
        progress_notification: int | None = None
        async with self._sessions() as session:
            counts = (
                await session.execute(
                    select(CollectionJob.status, func.count(CollectionJob.id))
                    .where(CollectionJob.collection_run_id == run_id)
                    .group_by(CollectionJob.status)
                )
            ).all()
            by_status = {status: count for status, count in counts}
            run = await session.get(CollectionRun, run_id, with_for_update=True)
            if run is None:
                return
            run.total_jobs = sum(by_status.values())
            run.completed_jobs = by_status.get(JobStatus.COMPLETED, 0)
            run.failed_jobs = by_status.get(JobStatus.FAILED, 0)
            run.skipped_jobs = by_status.get(JobStatus.SKIPPED, 0)
            terminal = run.completed_jobs + run.failed_jobs + run.skipped_jobs
            progress = min(100, terminal * 100 // run.total_jobs) if run.total_jobs else 0
            progress_bucket = progress // 10 * 10
            raw_last_notified = run.configuration.get("last_notified_percent", 0)
            last_notified = raw_last_notified if isinstance(raw_last_notified, int) else 0
            if progress_bucket >= 10 and progress_bucket > last_notified:
                run.configuration = {
                    **run.configuration,
                    "last_notified_percent": progress_bucket,
                }
                progress_notification = progress_bucket
            active = sum(
                by_status.get(status, 0)
                for status in (
                    JobStatus.PENDING,
                    JobStatus.RUNNING,
                    JobStatus.RETRY_WAIT,
                    JobStatus.PAUSED,
                )
            )
            if active == 0 and run.total_jobs:
                run.finished_at = datetime.now(UTC)
                run.status = (
                    CollectionRunStatus.COMPLETED_WITH_ERRORS
                    if run.failed_jobs
                    else CollectionRunStatus.COMPLETED
                )
            elif run.status == CollectionRunStatus.WAITING_METHOD_LIMIT:
                next_retry = await session.scalar(
                    select(func.min(CollectionJob.next_attempt_at)).where(
                        CollectionJob.collection_run_id == run_id,
                        CollectionJob.status == JobStatus.RETRY_WAIT,
                    )
                )
                run.next_wakeup_at = next_retry
                if next_retry is not None and next_retry <= datetime.now(UTC):
                    run.status = CollectionRunStatus.RUNNING
                    run.next_wakeup_at = None
            await session.commit()
        if progress_notification is not None:
            await notify(
                self._settings,
                f"Collection run {run_id}: progress={progress_notification}%",
            )

    async def set_run_status(
        self, run_id: uuid.UUID, status: CollectionRunStatus, reason: str | None = None
    ) -> None:
        async with self._sessions() as session:
            run = await session.get(CollectionRun, run_id, with_for_update=True)
            if run is None:
                raise ValueError("Запуск не найден")
            run.status = status
            run.error_message = reason
            if run.campaign_id is not None:
                campaign = await session.get(
                    CollectionCampaign, run.campaign_id, with_for_update=True
                )
                if campaign is not None:
                    if status == CollectionRunStatus.PAUSED_CAPACITY_LIMIT:
                        campaign.status = CampaignStatus.PAUSED_CAPACITY_LIMIT.value
                    elif status == CollectionRunStatus.PAUSED:
                        campaign.status = CampaignStatus.PAUSED.value
                    elif status == CollectionRunStatus.WAITING_METHOD_LIMIT:
                        campaign.status = CampaignStatus.WAITING_METHOD_LIMIT.value
                    elif status == CollectionRunStatus.RUNNING:
                        campaign.status = CampaignStatus.RUNNING.value
                    campaign.error_message = reason
            await session.execute(
                update(CollectionJob)
                .where(
                    CollectionJob.collection_run_id == run_id,
                    CollectionJob.status == JobStatus.PAUSED,
                )
                .values(
                    status=JobStatus.PENDING
                    if status == CollectionRunStatus.RUNNING
                    else JobStatus.PAUSED
                )
            )
            if status == CollectionRunStatus.PAUSED:
                await session.execute(
                    update(CollectionJob)
                    .where(
                        CollectionJob.collection_run_id == run_id,
                        CollectionJob.status == JobStatus.PENDING,
                    )
                    .values(status=JobStatus.PAUSED)
                )
            await session.commit()

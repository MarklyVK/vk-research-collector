from __future__ import annotations

import hashlib
import json
import random
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import func, or_, select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from vk_collector.config import Settings
from vk_collector.database.models import (
    ClassificationStatus,
    CollectionJob,
    CollectionRun,
    CollectionRunStatus,
    GroupCandidate,
    GroupLabel,
    JobStatus,
)


@dataclass(frozen=True, slots=True)
class PlanPreview:
    approved_groups: int
    selected_groups: int
    scopes: tuple[str, ...]
    jobs: int
    estimated_requests: int
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
        for label in ("food_delivery", "customer_acquisition", "tender_support"):
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
        if self._settings.collection_subscriptions_enabled:
            scopes.append("subscriptions")
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
        }

    async def preview(self, *, pilot: bool = False) -> PlanPreview:
        all_ids = await self.approved_group_ids()
        ids = await self.pilot_group_ids() if pilot else all_ids
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
        return PlanPreview(len(all_ids), len(ids), scopes, jobs, requests, tuple(warnings))

    async def plan(self, *, pilot: bool = False) -> uuid.UUID:
        ids = await self.pilot_group_ids() if pilot else await self.approved_group_ids()
        scopes = self.enabled_scopes()
        collection_configuration = self.collection_configuration()
        key_payload = {
            "pilot": pilot,
            "group_ids": ids,
            "collection": collection_configuration,
        }
        plan_key = hashlib.sha256(
            json.dumps(key_payload, sort_keys=True).encode("utf-8")
        ).hexdigest()
        async with self._sessions() as session:
            existing = await session.scalar(
                select(CollectionRun).where(
                    CollectionRun.status.in_(
                        [
                            CollectionRunStatus.PLANNED,
                            CollectionRunStatus.PAUSED_CAPACITY_LIMIT,
                        ]
                    ),
                    CollectionRun.configuration["plan_key"].astext == plan_key,
                )
            )
            if existing is not None:
                return existing.id
            run = CollectionRun(
                scope="pilot" if pilot else "full",
                status=CollectionRunStatus.PLANNED,
                configuration={
                    "plan_key": plan_key,
                    "pilot": pilot,
                    "scopes": list(scopes),
                    "group_count": len(ids),
                    "collection": collection_configuration,
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
            for group_id in ids:
                for scope, job_type in mapping.items():
                    if scope in scopes:
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
                    locked_at=None,
                    locked_by=None,
                    heartbeat_at=None,
                )
            )
            await session.commit()
            return int(result.rowcount or 0)  # type: ignore[attr-defined]

    async def claim(self, run_id: uuid.UUID, *, scope: str | None = None) -> ClaimedJob | None:
        now = datetime.now(UTC)
        job_type = {
            "groups": "refresh_group",
            "posts": "collect_group_posts",
            "members": "collect_group_members",
            "users": "refresh_user_profile",
            "subscriptions": "collect_user_subscriptions",
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
            if job_type:
                query = query.where(CollectionJob.job_type == job_type)
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
    ) -> None:
        now = datetime.now(UTC)
        async with self._sessions() as session:
            job = await session.get(CollectionJob, job_id, with_for_update=True)
            if job is None:
                return
            job.status = status
            job.last_error_type = error_type
            job.last_error_message = error_message
            job.error_message = error_message
            job.next_attempt_at = retry_at
            job.locked_at = None
            job.locked_by = None
            job.heartbeat_at = None
            if status in {JobStatus.COMPLETED, JobStatus.SKIPPED, JobStatus.FAILED}:
                job.finished_at = now
            await session.commit()
        await self.refresh_run(job.collection_run_id)

    async def refresh_run(self, run_id: uuid.UUID) -> None:
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
            await session.commit()

    async def set_run_status(
        self, run_id: uuid.UUID, status: CollectionRunStatus, reason: str | None = None
    ) -> None:
        async with self._sessions() as session:
            run = await session.get(CollectionRun, run_id, with_for_update=True)
            if run is None:
                raise ValueError("Запуск не найден")
            run.status = status
            run.error_message = reason
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

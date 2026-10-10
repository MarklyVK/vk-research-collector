from __future__ import annotations

import asyncio
import contextlib
import hashlib
import logging
import random
import time
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any, cast

from sqlalchemy import Table, and_, delete, func, or_, select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from vk_collector.collection.notifications import notify
from vk_collector.collection.queue import ClaimedJob, CollectionQueue
from vk_collector.collection.safety import inspect_disk, sanitize_message
from vk_collector.config import Settings
from vk_collector.database.models import (
    CollectionJob,
    CollectionJobError,
    CollectionRun,
    CollectionRunStatus,
    CommunityMemberSnapshot,
    CommunityPostCollectionState,
    GroupCandidate,
    GroupCollectionState,
    GroupMembership,
    GroupPost,
    JobStatus,
    PostAttachment,
    UserGroupSubscription,
    UserPost,
    UserPostAttachment,
    UserPostCollectionState,
    UserSubscriptionState,
    VKCommunity,
    VKUser,
)
from vk_collector.vk import (
    VKAPIError,
    VKClient,
    VKError,
    VKMethodUnavailable,
    VKTokensUnavailable,
)

TERMINAL_VK_CODES = frozenset({7, 15, 18, 30, 260})
JOB_METHODS = {
    "refresh_group": "groups.getById",
    "collect_group_posts": "wall.get",
    "collect_group_members": "groups.getMembers",
    "refresh_user_profile": "users.get",
    "collect_user_posts": "wall.get",
    "collect_user_subscriptions": "groups.get",
    "refresh_community_metadata": "groups.getById",
    "collect_subscription_group_posts": "wall.get",
    "fast_top_up_members": "groups.getMembers",
}
FAIR_JOB_TYPES = tuple(JOB_METHODS)
RETRY_DELAYS = (60, 300, 900, 3600, 21600)
USER_ENRICHMENT_LONG_RETRY_MAX_SECONDS = 86400
METADATA_BATCH_MISS_LIMIT = 3
METADATA_SINGLE_MISS_LIMIT = 2
logger = logging.getLogger(__name__)


def _counter(value: object) -> int:
    if isinstance(value, dict):
        raw = value.get("count", 0)
        return int(raw) if isinstance(raw, (int, float)) else 0
    return 0


def _utc_from_timestamp(value: object) -> datetime | None:
    if not isinstance(value, (int, float)) or value <= 0:
        return None
    return datetime.fromtimestamp(value, UTC)


def normalize_attachment(raw: dict[str, Any], position: int) -> dict[str, Any]:
    """Оставить небольшой разрешённый набор метаданных вложения."""
    kind = str(raw.get("type", "unknown"))
    payload = raw.get(kind, {})
    value = payload if isinstance(payload, dict) else {}
    sizes = value.get("sizes", [])
    largest: dict[str, Any] = {}
    if isinstance(sizes, list):
        valid = [item for item in sizes if isinstance(item, dict)]
        largest = max(valid, key=lambda item: int(item.get("width", 0)), default={})
    external_url = value.get("url") if kind in {"link", "doc"} else None
    metadata: dict[str, object] = {}
    if kind == "link" and isinstance(value.get("caption"), str):
        metadata["caption"] = value["caption"]
    return {
        "position": position,
        "attachment_type": kind,
        "vk_owner_id": value.get("owner_id") if isinstance(value.get("owner_id"), int) else None,
        "vk_attachment_id": value.get("id") if isinstance(value.get("id"), int) else None,
        "access_key": value.get("access_key") if isinstance(value.get("access_key"), str) else None,
        "duration": value.get("duration") if isinstance(value.get("duration"), int) else None,
        "width": value.get("width", largest.get("width"))
        if isinstance(value.get("width", largest.get("width")), int)
        else None,
        "height": value.get("height", largest.get("height"))
        if isinstance(value.get("height", largest.get("height")), int)
        else None,
        "title": str(value["title"])[:1000] if value.get("title") else None,
        "external_url": str(external_url) if external_url else None,
        "attachment_metadata": metadata,
    }


class CollectionWorker:
    def __init__(
        self,
        sessions: async_sessionmaker[AsyncSession],
        client: VKClient,
        settings: Settings,
    ) -> None:
        self._sessions = sessions
        self._client = client
        self._settings = settings
        self._queue = CollectionQueue(sessions, settings)
        self._scheduler_cursor = 0

    async def run(
        self,
        run_id: uuid.UUID,
        *,
        scope: str | None = None,
        max_jobs: int | None = None,
        stop_event: asyncio.Event | None = None,
        until_idle: bool = True,
    ) -> int:
        """Обработать доступные jobs с ограниченной concurrency до idle."""
        await self._queue.recover_expired(run_id)
        tasks: set[asyncio.Task[None]] = set()
        claimed = 0
        disk_warning_sent = False
        recovery_interval = max(
            1.0,
            min(60.0, self._settings.collection_job_lease_seconds / 2),
        )
        effective_concurrency = self._settings.effective_collection_concurrency
        next_recovery_at = time.monotonic() + recovery_interval
        while stop_event is None or not stop_event.is_set():
            if time.monotonic() >= next_recovery_at:
                recovered = await self._queue.recover_expired(run_id)
                if recovered:
                    logger.warning("run=%s recovered_expired_jobs=%s", run_id, recovered)
                next_recovery_at = time.monotonic() + recovery_interval
            disk = inspect_disk(
                self._settings.collection_export_dir,
                self._settings.disk_warning_percent,
                self._settings.disk_stop_percent,
                min_free_bytes=self._settings.collection_disk_min_free_bytes,
            )
            if disk.stop:
                await self._queue.set_run_status(
                    run_id,
                    CollectionRunStatus.PAUSED_CAPACITY_LIMIT,
                    f"Диск заполнен на {disk.used_percent:.1f}%",
                )
                await notify(self._settings, f"Сбор {run_id} поставлен на паузу: диск >95%")
                break
            if disk.warning and not disk_warning_sent:
                logger.warning(
                    "run=%s disk_used_percent=%.1f threshold=warning", run_id, disk.used_percent
                )
                await notify(
                    self._settings,
                    f"Сбор {run_id}: предупреждение, диск заполнен на {disk.used_percent:.1f}%",
                )
                disk_warning_sent = True
            elif not disk.warning:
                disk_warning_sent = False
            while len(tasks) < effective_concurrency and (max_jobs is None or claimed < max_jobs):
                job = await self._claim_next(run_id, scope)
                if job is None:
                    break
                tasks.add(asyncio.create_task(self._process(job)))
                claimed += 1
            if not tasks:
                await self._queue.refresh_run(run_id)
                if until_idle:
                    break
                async with self._sessions() as session:
                    run = await session.get(CollectionRun, run_id)
                    if run is None or run.status not in {
                        CollectionRunStatus.PLANNED,
                        CollectionRunStatus.RUNNING,
                        CollectionRunStatus.WAITING_METHOD_LIMIT,
                    }:
                        break
                if stop_event is None:
                    await asyncio.sleep(self._settings.collection_idle_sleep_seconds)
                else:
                    with contextlib.suppress(TimeoutError):
                        await asyncio.wait_for(
                            stop_event.wait(),
                            timeout=self._settings.collection_idle_sleep_seconds,
                        )
                continue
            done, tasks = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                await task
            logger.info(
                "run=%s claimed=%s active=%s max_jobs=%s",
                run_id,
                claimed,
                len(tasks),
                max_jobs,
            )
            if max_jobs is not None and claimed >= max_jobs and not tasks:
                break
        if tasks:
            await asyncio.gather(*tasks)
        await self._queue.refresh_run(run_id)
        return claimed

    async def _claim_next(self, run_id: uuid.UUID, scope: str | None) -> ClaimedJob | None:
        if scope is not None:
            method = JOB_METHODS.get(
                {
                    "groups": "refresh_group",
                    "posts": "collect_group_posts",
                    "members": "collect_group_members",
                    "users": "refresh_user_profile",
                    "subscriptions": "collect_user_subscriptions",
                    "metadata": "refresh_community_metadata",
                    "subscription_posts": "collect_subscription_group_posts",
                    "user_posts": "collect_user_posts",
                }.get(scope, "")
            )
            if method is not None and not await self._method_available(method):
                return None
            return await self._queue.claim(run_id, scope=scope)
        for distance in range(len(FAIR_JOB_TYPES)):
            index = (self._scheduler_cursor + distance) % len(FAIR_JOB_TYPES)
            job_type = FAIR_JOB_TYPES[index]
            if not await self._method_available(JOB_METHODS[job_type]):
                continue
            job = await self._queue.claim(run_id, job_type=job_type)
            if job is not None:
                self._scheduler_cursor = (index + 1) % len(FAIR_JOB_TYPES)
                return job
        pending_types = await self._queue.pending_job_types(run_id)
        retry_times = [
            value
            for job_type in pending_types
            if job_type in JOB_METHODS
            for value in [await self._next_method_available_at(JOB_METHODS[job_type])]
            if value is not None
        ]
        if pending_types and retry_times:
            delay = max(1.0, min(retry_times) - time.monotonic())
            await self._queue.wait_for_methods(run_id, datetime.now(UTC) + timedelta(seconds=delay))
        return None

    async def _method_available(self, method: str) -> bool:
        """Считать method доступным для простых fake clients старого тестового контракта."""
        if not hasattr(self._client, "is_method_available"):
            return True
        return await self._client.is_method_available(method)

    async def _next_method_available_at(self, method: str) -> float | None:
        if not hasattr(self._client, "next_method_available_at"):
            return None
        return await self._client.next_method_available_at(method)

    async def _process(self, job: ClaimedJob) -> None:
        started = time.monotonic()
        try:
            if job.job_type == "refresh_group":
                if not await self._refresh_group(job):
                    return
            elif job.job_type == "collect_group_posts":
                await self._collect_posts(job)
            elif job.job_type == "collect_group_members":
                await self._collect_members(job)
            elif job.job_type == "refresh_user_profile":
                await self._refresh_user(job)
            elif job.job_type == "collect_user_posts":
                if await self._skip_fresh(job):
                    return
                await self._collect_user_posts(job)
            elif job.job_type == "collect_user_subscriptions":
                if await self._skip_fresh(job):
                    return
                await self._collect_subscriptions(job)
            elif job.job_type == "refresh_community_metadata":
                if not await self._refresh_community_metadata(job):
                    return
            elif job.job_type == "collect_subscription_group_posts":
                await self._collect_subscription_posts(job)
            elif job.job_type == "fast_top_up_members":
                await self._fast_top_up_members(job)
            else:
                raise ValueError(f"Неизвестный тип задания: {job.job_type}")
            await self._queue.finish(job.id, JobStatus.COMPLETED)
        except VKMethodUnavailable as exc:
            message = sanitize_message(str(exc))
            delay = max(1.0, (exc.retry_at or time.monotonic() + 60) - time.monotonic())
            await self._record_error(job, "method_limit", message, vk_code=exc.error_code)
            await self._queue.defer_method(
                job,
                retry_at=datetime.now(UTC) + timedelta(seconds=delay),
                message=message,
            )
        except VKTokensUnavailable as exc:
            message = sanitize_message(str(exc))
            await self._record_error(job, "tokens_unavailable", message)
            await self._queue.finish(
                job.id, JobStatus.PAUSED, error_type="tokens_unavailable", error_message=message
            )
            await self._queue.set_run_status(
                job.run_id, CollectionRunStatus.PAUSED_NO_TOKENS, message
            )
            await notify(self._settings, f"Сбор {job.run_id}: все VK-токены недоступны")
        except VKAPIError as exc:
            message = sanitize_message(str(exc))
            subscriptions_private = (
                exc.code in {15, 30, 260} and job.job_type == "collect_user_subscriptions"
            )
            category = (
                "subscriptions_private"
                if subscriptions_private
                else "permission_or_private"
                if exc.code in TERMINAL_VK_CODES
                else "vk_api"
            )
            await self._record_error(job, category, message, vk_code=exc.code)
            if subscriptions_private:
                await self._mark_subscriptions_terminal(job, exc.code)
                await self._queue.finish(
                    job.id, JobStatus.SKIPPED, error_type=category, error_message=message
                )
            elif exc.code in TERMINAL_VK_CODES:
                if job.job_type == "collect_user_subscriptions":
                    await self._mark_subscriptions_terminal(job, exc.code)
                if job.job_type == "collect_user_posts":
                    await self._mark_user_post_failure(job, exc.code, message, terminal=True)
                if job.job_type in {
                    "collect_group_posts",
                    "collect_subscription_group_posts",
                }:
                    await self._mark_post_failure(job, exc.code, message, terminal=True)
                await self._queue.finish(
                    job.id, JobStatus.SKIPPED, error_type=category, error_message=message
                )
            else:
                await self._retry_or_fail(job, category, message)
        except VKError as exc:
            message = sanitize_message(str(exc))
            await self._record_error(job, "transient", message)
            await self._retry_or_fail(job, "transient", message)
        except Exception as exc:
            message = sanitize_message(str(exc))
            await self._record_error(job, type(exc).__name__, message)
            await self._retry_or_fail(job, type(exc).__name__, message)
        finally:
            await self._log_job(job, time.monotonic() - started)

    async def _log_job(self, job: ClaimedJob, duration_seconds: float) -> None:
        async with self._sessions() as session:
            metrics = (
                await session.execute(
                    select(
                        CollectionJob.status,
                        CollectionJob.api_requests,
                        CollectionJob.rows_inserted,
                        CollectionJob.rows_updated,
                    ).where(CollectionJob.id == job.id)
                )
            ).one_or_none()
        if metrics is None:
            return
        status, requests, inserted, updated = metrics
        logger.info(
            "run=%s job=%s endpoint=%s entity=%s:%s attempt=%s status=%s "
            "api_requests=%s rows_inserted=%s rows_updated=%s duration_seconds=%.3f",
            job.run_id,
            job.id,
            job.job_type,
            job.entity_type,
            job.entity_id,
            job.attempt_count,
            status.value,
            requests,
            inserted,
            updated,
            duration_seconds,
        )

    async def _retry_or_fail(self, job: ClaimedJob, category: str, message: str) -> None:
        if job.job_type in {"collect_group_posts", "collect_subscription_group_posts"}:
            await self._mark_post_failure(job, None, message, terminal=False)
        if job.job_type == "collect_user_posts":
            await self._mark_user_post_failure(job, None, message, terminal=False)
        if job.job_type == "collect_user_subscriptions":
            await self._mark_subscriptions_transient(job)
        durable_user_enrichment = job.job_type in {
            "collect_user_posts",
            "collect_user_subscriptions",
        } and category in {"transient", "vk_api"}
        if job.attempt_count >= len(RETRY_DELAYS) and not durable_user_enrichment:
            await self._queue.finish(
                job.id, JobStatus.FAILED, error_type=category, error_message=message
            )
            return
        if job.attempt_count <= len(RETRY_DELAYS):
            delay = RETRY_DELAYS[job.attempt_count - 1]
        else:
            long_retry_step = min(2, job.attempt_count - len(RETRY_DELAYS))
            delay = min(
                USER_ENRICHMENT_LONG_RETRY_MAX_SECONDS,
                RETRY_DELAYS[-1] * (2**long_retry_step),
            )
        retry_at = datetime.now(UTC) + timedelta(seconds=delay)
        await self._queue.finish(
            job.id,
            JobStatus.RETRY_WAIT,
            error_type=category,
            error_message=message,
            retry_at=retry_at,
        )

    async def _mark_subscriptions_transient(self, job: ClaimedJob) -> None:
        """Отложить transient-user, чтобы он не блокировал каждую следующую cohort."""
        now = datetime.now(UTC)
        async with self._sessions() as session:
            campaign_id = await session.scalar(
                select(CollectionRun.campaign_id).where(CollectionRun.id == job.run_id)
            )
            await session.execute(
                insert(UserSubscriptionState)
                .values(
                    user_id=job.entity_id,
                    last_attempt_at=now,
                    collected_limit=self._settings.collection_subscriptions_max_per_user,
                    privacy_denied=False,
                    is_truncated=False,
                    terminal_reason=None,
                    last_error_code=None,
                    last_run_id=job.run_id,
                    last_campaign_id=campaign_id,
                    next_scheduled_at=now + timedelta(days=1),
                )
                .on_conflict_do_update(
                    index_elements=[UserSubscriptionState.user_id],
                    set_={
                        "last_attempt_at": now,
                        "privacy_denied": False,
                        "is_truncated": False,
                        "terminal_reason": None,
                        "last_error_code": None,
                        "last_run_id": job.run_id,
                        "last_campaign_id": campaign_id,
                        "next_scheduled_at": now + timedelta(days=1),
                    },
                )
            )
            await session.commit()

    async def _group(self, group_id: int) -> GroupCandidate:
        async with self._sessions() as session:
            group = await session.get(GroupCandidate, group_id)
            if group is None:
                raise ValueError("Approved-группа не найдена")
            return group

    async def _refresh_group(self, job: ClaimedJob) -> bool:
        """Process up to collection_community_metadata_batch_size groups per
        groups.getById request (verified live: VK accepts up to 500 IDs per call;
        this previously issued one request per group).
        """
        extras = await self._queue.claim_group_batch(
            job.run_id, limit=self._settings.collection_community_metadata_batch_size - 1
        )
        batch = [job, *extras]
        try:
            async with self._sessions() as session:
                groups = list(
                    (
                        await session.scalars(
                            select(GroupCandidate).where(
                                GroupCandidate.id.in_([item.entity_id for item in batch])
                            )
                        )
                    ).all()
                )
            by_group_id = {g.id: g for g in groups}
            missing_ids = [item.entity_id for item in batch if item.entity_id not in by_group_id]
            if missing_ids:
                raise ValueError(f"Группы исчезли во время обновления: {missing_ids}")
            rows = await self._client.get_groups([g.vk_id for g in groups])
        except Exception:
            await self._queue.release(extras)
            raise
        by_vk_id = {
            value["id"]: value
            for value in rows
            if isinstance(value.get("id"), int) and not isinstance(value.get("id"), bool)
        }
        now = datetime.now(UTC)
        unavailable: dict[int, str] = {}
        async with self._sessions() as session:
            for group in groups:
                value = by_vk_id.get(group.vk_id)
                if value is None:
                    unavailable[group.id] = "Группа удалена или недоступна"
                    continue
                if value.get("deactivated") or int(value.get("is_closed", 0)) != 0:
                    unavailable[group.id] = "Группа закрыта, удалена или заблокирована"
                    continue
                current = await session.get(GroupCandidate, group.id, with_for_update=True)
                if current is None:
                    unavailable[group.id] = "Группа исчезла во время обновления"
                    continue
                current.name = str(value.get("name", current.name))
                current.description = str(value.get("description", ""))
                current.status_text = str(value.get("status", ""))
                screen_name = value.get("screen_name")
                current.screen_name = str(screen_name) if screen_name else current.screen_name
                current.last_seen_at = now
                await session.execute(
                    insert(VKCommunity)
                    .values(
                        vk_id=current.vk_id,
                        name=current.name,
                        description=current.description,
                        status_text=current.status_text,
                        screen_name=current.screen_name,
                        community_type=str(value.get("type")) if value.get("type") else None,
                        is_closed=bool(value.get("is_closed", False)),
                        deactivated=str(value["deactivated"]) if value.get("deactivated") else None,
                        members_count=value.get("members_count")
                        if isinstance(value.get("members_count"), int)
                        else None,
                        city=value.get("city") if isinstance(value.get("city"), dict) else None,
                        country=value.get("country")
                        if isinstance(value.get("country"), dict)
                        else None,
                        first_seen_at=current.first_seen_at,
                        last_seen_at=now,
                        metadata_updated_at=now,
                    )
                    .on_conflict_do_update(
                        index_elements=[VKCommunity.vk_id],
                        set_={
                            "name": current.name,
                            "description": current.description,
                            "status_text": current.status_text,
                            "screen_name": current.screen_name,
                            "community_type": str(value.get("type")) if value.get("type") else None,
                            "is_closed": bool(value.get("is_closed", False)),
                            "deactivated": str(value["deactivated"])
                            if value.get("deactivated")
                            else None,
                            # COALESCE: не затирать уже известный members_count, если в
                            # этом конкретном ответе VK его не прислал (раньше ON CONFLICT
                            # вообще не трогал колонку, из-за чего она не заполнялась годами).
                            "members_count": func.coalesce(
                                value.get("members_count")
                                if isinstance(value.get("members_count"), int)
                                else None,
                                VKCommunity.members_count,
                            ),
                            "city": value.get("city")
                            if isinstance(value.get("city"), dict)
                            else None,
                            "country": value.get("country")
                            if isinstance(value.get("country"), dict)
                            else None,
                            "last_seen_at": now,
                            "metadata_updated_at": now,
                        },
                    )
                )
                await session.execute(
                    insert(GroupCollectionState)
                    .values(group_id=group.id, last_group_success_at=now)
                    .on_conflict_do_update(
                        index_elements=[GroupCollectionState.group_id],
                        set_={
                            "last_group_success_at": now,
                            "unavailable": False,
                            "skip_reason": None,
                        },
                    )
                )
            await self._update_metrics(
                session, job.id, requests=1, updated=len(groups) - len(unavailable)
            )
            await session.commit()

        for extra in extras:
            reason = unavailable.get(extra.entity_id)
            if reason is None:
                await self._queue.finish(extra.id, JobStatus.COMPLETED)
            else:
                await self._queue.finish(
                    extra.id,
                    JobStatus.SKIPPED,
                    error_type="permission_or_private",
                    error_message=reason,
                )
        if job.entity_id in unavailable:
            await self._queue.finish(
                job.id,
                JobStatus.SKIPPED,
                error_type="permission_or_private",
                error_message=unavailable[job.entity_id],
            )
            return False
        return True

    async def _collect_posts(self, job: ClaimedJob) -> None:
        group = await self._group(job.entity_id)
        offset = int(job.checkpoint.get("offset", 0))
        maximum = self._settings.collection_posts_max_per_group
        page_size = self._settings.collection_posts_page_size
        while offset < maximum:
            count = min(page_size, maximum - offset)
            response = await self._client.get_wall_page(group.vk_id, offset, count)
            raw_items = response.get("items", [])
            items = (
                [item for item in raw_items if isinstance(item, dict)]
                if isinstance(raw_items, list)
                else []
            )
            now = datetime.now(UTC)
            async with self._sessions() as session:
                changed = 0
                await session.execute(
                    insert(VKCommunity)
                    .values(
                        vk_id=group.vk_id,
                        name=group.name,
                        description=group.description,
                        status_text=group.status_text,
                        screen_name=group.screen_name,
                        first_seen_at=group.first_seen_at,
                        last_seen_at=group.last_seen_at,
                    )
                    .on_conflict_do_nothing(index_elements=[VKCommunity.vk_id])
                )
                for raw in items:
                    published = _utc_from_timestamp(raw.get("date"))
                    if published is None:
                        continue
                    text = str(raw.get("text", ""))
                    content_hash = hashlib.sha256(
                        f"{text}\0{raw.get('date')}\0{raw.get('edited')}".encode()
                    ).hexdigest()
                    post_id = await session.scalar(
                        insert(GroupPost)
                        .values(
                            vk_owner_id=int(raw.get("owner_id", -group.vk_id)),
                            vk_post_id=int(raw["id"]),
                            group_id=group.id,
                            community_vk_id=group.vk_id,
                            published_at=published,
                            edited_at=_utc_from_timestamp(raw.get("edited")),
                            text=text,
                            post_type=str(raw.get("post_type", "post")),
                            is_pinned=bool(raw.get("is_pinned", False)),
                            is_ad=bool(raw.get("is_ad", False)),
                            marked_as_ads=bool(raw.get("marked_as_ads", False)),
                            comments_count=_counter(raw.get("comments")),
                            likes_count=_counter(raw.get("likes")),
                            reposts_count=_counter(raw.get("reposts")),
                            views_count=_counter(raw.get("views")),
                            signer_vk_user_id=raw.get("signer_id")
                            if isinstance(raw.get("signer_id"), int)
                            else None,
                            source_updated_at=_utc_from_timestamp(raw.get("edited")) or published,
                            first_seen_at=now,
                            last_seen_at=now,
                            content_hash=content_hash,
                        )
                        .on_conflict_do_update(
                            constraint="uq_group_posts_owner_post",
                            set_={
                                "text": text,
                                "edited_at": _utc_from_timestamp(raw.get("edited")),
                                "comments_count": _counter(raw.get("comments")),
                                "likes_count": _counter(raw.get("likes")),
                                "reposts_count": _counter(raw.get("reposts")),
                                "views_count": _counter(raw.get("views")),
                                "last_seen_at": now,
                                "content_hash": content_hash,
                            },
                        )
                        .returning(GroupPost.id)
                    )
                    if post_id is None:
                        continue
                    await session.execute(
                        delete(PostAttachment).where(PostAttachment.post_id == post_id)
                    )
                    raw_attachments = raw.get("attachments", [])
                    attachments = (
                        [item for item in raw_attachments if isinstance(item, dict)]
                        if isinstance(raw_attachments, list)
                        else []
                    )
                    for position, attachment in enumerate(attachments):
                        normalized = normalize_attachment(attachment, position)
                        normalized["post_id"] = post_id
                        session.add(PostAttachment(**normalized))
                    changed += 1
                offset += len(items)
                checkpoint: dict[str, object] = {
                    "offset": offset,
                    "total": int(response.get("count", 0)),
                }
                await self._checkpoint(session, job.id, checkpoint, 1, changed)
                await session.commit()
            total = int(response.get("count", 0))
            if not items or offset >= total or len(items) < count:
                break
        async with self._sessions() as session:
            now = datetime.now(UTC)
            await session.execute(
                insert(GroupCollectionState)
                .values(
                    group_id=group.id,
                    posts_checkpoint={"offset": offset},
                    last_posts_success_at=now,
                )
                .on_conflict_do_update(
                    index_elements=[GroupCollectionState.group_id],
                    set_={
                        "posts_checkpoint": {"offset": offset},
                        "last_posts_success_at": now,
                    },
                )
            )
            await session.execute(
                insert(CommunityPostCollectionState)
                .values(
                    community_vk_id=group.vk_id,
                    last_attempt_at=now,
                    last_success_at=now,
                    next_scheduled_at=now
                    + timedelta(days=self._settings.collection_subscription_group_posts_ttl_days),
                    last_run_id=job.run_id,
                    last_error_code=None,
                    last_error_reason=None,
                    collected_count=offset,
                    wall_private=False,
                    unavailable=False,
                )
                .on_conflict_do_update(
                    index_elements=[CommunityPostCollectionState.community_vk_id],
                    set_={
                        "last_attempt_at": now,
                        "last_success_at": now,
                        "next_scheduled_at": now
                        + timedelta(
                            days=self._settings.collection_subscription_group_posts_ttl_days
                        ),
                        "last_run_id": job.run_id,
                        "last_error_code": None,
                        "last_error_reason": None,
                        "collected_count": offset,
                        "wall_private": False,
                        "unavailable": False,
                    },
                )
            )
            await session.commit()

    async def _collect_members(self, job: ClaimedJob) -> None:
        """Two-phase member collection: exhaustively enumerate member IDs (cheap,
        id-only pages), then draw a reproducible uniform random sample of
        collection_members_max_per_group IDs and only fully process those.

        Replaces the previous "first N members in VK's default id_asc order"
        behaviour, which was an undisclosed order-bias (systematically the
        oldest accounts in each group), not a representative sample.
        """
        group = await self._group(job.entity_id)
        offset = int(job.checkpoint.get("offset", 0))
        sample_size = self._settings.collection_members_max_per_group
        page_size = self._settings.collection_members_page_size
        total = 0
        discovered_user_ids: set[int] = set()

        # Phase 1: exhaustive id-only enumeration (cheap — same endpoint/quota
        # as before, just not stopped early at sample_size).
        while True:
            response = await self._client.get_members_page(group.vk_id, offset, page_size)
            raw_items = response.get("items", [])
            values = raw_items if isinstance(raw_items, list) else []
            for item in values:
                if isinstance(item, int):
                    discovered_user_ids.add(item)
                elif isinstance(item, dict):
                    raw_id = item.get("id")
                    if isinstance(raw_id, int):
                        discovered_user_ids.add(raw_id)
            total = int(response.get("count", 0))
            offset += len(values)
            async with self._sessions() as session:
                await self._checkpoint(
                    session,
                    job.id,
                    {"offset": offset, "total": total, "phase": "enumerate"},
                    1,
                    0,
                )
                await session.commit()
            if not values or offset >= total or len(values) < page_size:
                break
            # Small jitter between pages: smooths the aggregate request pattern
            # across concurrent workers on this method, on top of the token
            # pool's own per-method rate limit (see vk_members_method_rps).
            await asyncio.sleep(random.uniform(0.15, 0.35))

        # Phase 2: reproducible uniform random sample of the full membership.
        # Seed is derived per-group from a fixed base seed so re-running the
        # pipeline reproduces the same sample without storing all member IDs.
        rng = random.Random(f"{self._settings.collection_members_sample_seed}:{group.vk_id}")
        unique_ids = sorted(discovered_user_ids)
        sampled_ids = (
            unique_ids
            if sample_size is None or len(unique_ids) <= sample_size
            else rng.sample(unique_ids, sample_size)
        )

        now = datetime.now(UTC)
        async with self._sessions() as session:
            for user_id in sampled_ids:
                await session.execute(
                    insert(VKUser)
                    .values(vk_id=user_id, first_seen_at=now, last_seen_at=now)
                    .on_conflict_do_update(
                        index_elements=[VKUser.vk_id], set_={"last_seen_at": now}
                    )
                )
                await session.execute(
                    insert(GroupMembership)
                    .values(
                        group_id=group.id,
                        user_id=user_id,
                        first_seen_at=now,
                        last_seen_at=now,
                        source_run_id=job.run_id,
                        is_current=True,
                    )
                    .on_conflict_do_update(
                        constraint="uq_group_memberships_group_user",
                        set_={
                            "last_seen_at": now,
                            "source_run_id": job.run_id,
                            "is_current": True,
                        },
                    )
                )
            if self._settings.collection_users_enabled and sampled_ids:
                cutoff = now - timedelta(days=self._settings.collection_user_profile_ttl_days)
                stale_ids = list(
                    (
                        await session.scalars(
                            select(VKUser.vk_id).where(
                                VKUser.vk_id.in_(sampled_ids),
                                or_(
                                    VKUser.profile_updated_at.is_(None),
                                    VKUser.profile_updated_at < cutoff,
                                ),
                            )
                        )
                    ).all()
                )
                for user_id in stale_ids:
                    await session.execute(
                        insert(CollectionJob)
                        .values(
                            collection_run_id=job.run_id,
                            job_type="refresh_user_profile",
                            entity_type="user",
                            entity_id=user_id,
                            priority=40,
                        )
                        .on_conflict_do_nothing(constraint="uq_collection_jobs_run_type_entity")
                    )
            # Superseded sample members from a previous run of this group
            # (e.g. re-sampled after growth) are marked no longer current;
            # enumeration is now always exhaustive, so the sample is always
            # complete relative to the full membership known at this run.
            await session.execute(
                update(GroupMembership)
                .where(
                    GroupMembership.group_id == group.id,
                    GroupMembership.source_run_id != job.run_id,
                )
                .values(is_current=False)
            )
            await session.execute(
                insert(GroupCollectionState)
                .values(
                    group_id=group.id,
                    members_checkpoint={
                        "total_members": total,
                        "sample_size": len(sampled_ids),
                        "sample_seed": self._settings.collection_members_sample_seed,
                        "complete": True,
                    },
                    last_members_success_at=now,
                )
                .on_conflict_do_update(
                    index_elements=[GroupCollectionState.group_id],
                    set_={
                        "members_checkpoint": {
                            "total_members": total,
                            "sample_size": len(sampled_ids),
                            "sample_seed": self._settings.collection_members_sample_seed,
                            "complete": True,
                        },
                        "last_members_success_at": now,
                    },
                )
            )
            await self._update_metrics(session, job.id, requests=0, updated=len(sampled_ids))
            await session.commit()

    async def _fast_top_up_members(self, job: ClaimedJob) -> None:
        """Owner-authorized fast baseline pass (2026-09-07): one groups.getMembers
        call per group, VK's default order, no enumeration/random sampling —
        tops the group up to collection_fast_top_up_target members quickly.

        Groups with fewer real members than the target are marked unavailable
        with a skip_reason (excluded-group count for the article). Queues
        refresh_user_profile / collect_user_subscriptions / collect_user_posts
        for the returned members at elevated priority so the full profile
        (demography + subscriptions + posts) completes fast for this cohort,
        ahead of the much larger pre-existing general backlog.
        """
        group = await self._group(job.entity_id)
        target = self._settings.collection_fast_top_up_target
        response = await self._client.get_members_page(group.vk_id, 0, target)
        raw_items = response.get("items", [])
        values = raw_items if isinstance(raw_items, list) else []
        member_ids: list[int] = []
        for item in values:
            if isinstance(item, int):
                member_ids.append(item)
            elif isinstance(item, dict):
                raw_id = item.get("id")
                if isinstance(raw_id, int):
                    member_ids.append(raw_id)
        total = int(response.get("count", 0))
        now = datetime.now(UTC)

        async with self._sessions() as session:
            if total < target:
                reason = f"fast_top_up: в группе {total} участников, меньше цели {target}"
                await session.execute(
                    insert(GroupCollectionState)
                    .values(
                        group_id=group.id,
                        unavailable=True,
                        skip_reason=reason,
                        last_members_success_at=now,
                    )
                    .on_conflict_do_update(
                        index_elements=[GroupCollectionState.group_id],
                        set_={
                            "unavailable": True,
                            "skip_reason": reason,
                            "last_members_success_at": now,
                        },
                    )
                )
                await self._update_metrics(session, job.id, requests=1, updated=0)
                await session.commit()
                return

            for user_id in member_ids:
                await session.execute(
                    insert(VKUser)
                    .values(vk_id=user_id, first_seen_at=now, last_seen_at=now)
                    .on_conflict_do_update(
                        index_elements=[VKUser.vk_id], set_={"last_seen_at": now}
                    )
                )
                await session.execute(
                    insert(GroupMembership)
                    .values(
                        group_id=group.id,
                        user_id=user_id,
                        first_seen_at=now,
                        last_seen_at=now,
                        source_run_id=job.run_id,
                        is_current=True,
                    )
                    .on_conflict_do_update(
                        constraint="uq_group_memberships_group_user",
                        set_={
                            "last_seen_at": now,
                            "source_run_id": job.run_id,
                            "is_current": True,
                        },
                    )
                )
            for user_id in member_ids:
                await session.execute(
                    insert(CollectionJob)
                    .values(
                        collection_run_id=job.run_id,
                        job_type="refresh_user_profile",
                        entity_type="user",
                        entity_id=user_id,
                        priority=5,
                    )
                    .on_conflict_do_nothing(constraint="uq_collection_jobs_run_type_entity")
                )
                await session.execute(
                    insert(CollectionJob)
                    .values(
                        collection_run_id=job.run_id,
                        job_type="collect_user_subscriptions",
                        entity_type="user",
                        entity_id=user_id,
                        priority=5,
                    )
                    .on_conflict_do_nothing(constraint="uq_collection_jobs_run_type_entity")
                )
                await session.execute(
                    insert(CollectionJob)
                    .values(
                        collection_run_id=job.run_id,
                        job_type="collect_user_posts",
                        entity_type="user",
                        entity_id=user_id,
                        priority=5,
                    )
                    .on_conflict_do_nothing(constraint="uq_collection_jobs_run_type_entity")
                )
            await session.execute(
                insert(GroupCollectionState)
                .values(
                    group_id=group.id,
                    members_checkpoint={
                        "fast_top_up": True,
                        "target": target,
                        "collected": len(member_ids),
                    },
                    last_members_success_at=now,
                )
                .on_conflict_do_update(
                    index_elements=[GroupCollectionState.group_id],
                    set_={
                        "members_checkpoint": {
                            "fast_top_up": True,
                            "target": target,
                            "collected": len(member_ids),
                        },
                        "last_members_success_at": now,
                    },
                )
            )
            await self._update_metrics(session, job.id, requests=1, updated=len(member_ids))
            await session.commit()

    async def _refresh_user(self, job: ClaimedJob) -> None:
        extras = await self._queue.claim_user_batch(
            job.run_id, limit=self._settings.collection_user_batch_size - 1
        )
        batch = [job, *extras]
        try:
            rows = await self._client.get_users([item.entity_id for item in batch])
        except Exception:
            await self._queue.release(extras)
            raise
        by_id = {int(value["id"]): value for value in rows if isinstance(value.get("id"), int)}
        now = datetime.now(UTC)
        async with self._sessions() as session:
            for user_id, value in by_id.items():
                sex_val = int(value["sex"]) if isinstance(value.get("sex"), int) else None
                bdate_val = str(value["bdate"]) if value.get("bdate") else None
                city_raw = value.get("city")
                city_val = (
                    str(city_raw.get("title"))
                    if isinstance(city_raw, dict) and "title" in city_raw
                    else (str(city_raw) if isinstance(city_raw, str) else None)
                )
                edu_val = (
                    str(
                        value.get("university_name")
                        or value.get("faculty_name")
                        or value.get("education_form")
                        or ""
                    )
                    or None
                )
                rel_val = int(value["relation"]) if isinstance(value.get("relation"), int) else None
                followers_val = (
                    int(value["followers_count"])
                    if isinstance(value.get("followers_count"), int)
                    else None
                )
                counters_raw = value.get("counters")
                friends_val = (
                    int(counters_raw["friends"])
                    if isinstance(counters_raw, dict)
                    and isinstance(counters_raw.get("friends"), int)
                    else None
                )
                gifts_val = (
                    int(counters_raw["gifts"])
                    if isinstance(counters_raw, dict) and isinstance(counters_raw.get("gifts"), int)
                    else None
                )

                demographics_dict: dict[str, object] = {
                    k: v
                    for k, v in {
                        "sex": sex_val,
                        "bdate": bdate_val,
                        "city": city_val,
                        "education": edu_val,
                        "relation": rel_val,
                        "followers_count": followers_val,
                        "friends_count": friends_val,
                        "gifts_count": gifts_val,
                    }.items()
                    if v is not None
                }

                await session.execute(
                    insert(VKUser)
                    .values(
                        vk_id=user_id,
                        first_name=str(value.get("first_name", "")),
                        last_name=str(value.get("last_name", "")),
                        screen_name=str(value["screen_name"]) if value.get("screen_name") else None,
                        is_closed=bool(value.get("is_closed", False)),
                        can_access_closed=bool(value.get("can_access_closed", False)),
                        deactivated=str(value["deactivated"]) if value.get("deactivated") else None,
                        sex=sex_val,
                        bdate=bdate_val,
                        city=city_val,
                        education=edu_val,
                        relation=rel_val,
                        followers_count=followers_val,
                        friends_count=friends_val,
                        gifts_count=gifts_val,
                        demographics=demographics_dict,
                        first_seen_at=now,
                        last_seen_at=now,
                        profile_updated_at=now,
                    )
                    .on_conflict_do_update(
                        index_elements=[VKUser.vk_id],
                        set_={
                            "first_name": str(value.get("first_name", "")),
                            "last_name": str(value.get("last_name", "")),
                            "screen_name": str(value["screen_name"])
                            if value.get("screen_name")
                            else None,
                            "is_closed": bool(value.get("is_closed", False)),
                            "can_access_closed": bool(value.get("can_access_closed", False)),
                            "deactivated": str(value["deactivated"])
                            if value.get("deactivated")
                            else None,
                            "sex": sex_val,
                            "bdate": bdate_val,
                            "city": city_val,
                            "education": edu_val,
                            "relation": rel_val,
                            "followers_count": followers_val,
                            "friends_count": friends_val,
                            "gifts_count": gifts_val,
                            "demographics": demographics_dict,
                            "last_seen_at": now,
                            "profile_updated_at": now,
                        },
                    )
                )
                if (
                    self._settings.collection_subscriptions_enabled
                    and (not value.get("is_closed") or value.get("can_access_closed"))
                    and not value.get("deactivated")
                ):
                    await session.execute(
                        insert(CollectionJob)
                        .values(
                            collection_run_id=job.run_id,
                            job_type="collect_user_subscriptions",
                            entity_type="user",
                            entity_id=user_id,
                            priority=50,
                        )
                        .on_conflict_do_nothing(constraint="uq_collection_jobs_run_type_entity")
                    )
            await self._update_metrics(session, job.id, requests=1, updated=len(by_id))
            await session.commit()
        for extra in extras:
            available = extra.entity_id in by_id
            await self._queue.finish(
                extra.id,
                JobStatus.COMPLETED if available else JobStatus.SKIPPED,
                error_type=None if available else "user_unavailable",
                error_message=None if available else "Пользователь удалён или недоступен",
            )
        if job.entity_id not in by_id:
            raise VKAPIError(18, "Пользователь удалён или недоступен")

    async def _skip_fresh(self, job: ClaimedJob) -> bool:
        """Закрыть job без запроса к VK, если пользователь собран недавно.

        Состояние пользователя общее для всех runs и кампаний, поэтому тот же
        пользователь из другого run (или повтор после недавнего сбора) не
        запрашивается повторно. Job, начатый с ненулевого checkpoint, не
        пропускается: его страницы уже частично записаны. Явно наступивший
        next_scheduled_at (пересбор назначен системой) тоже отменяет пропуск.
        """
        days = self._settings.collection_skip_fresh_days
        if days <= 0 or int(job.checkpoint.get("offset", 0)) > 0:
            return False
        now = datetime.now(UTC)
        cutoff = now - timedelta(days=days)
        async with self._sessions() as session:
            if job.job_type == "collect_user_subscriptions":
                fresh = await session.scalar(
                    select(UserSubscriptionState.user_id).where(
                        UserSubscriptionState.user_id == job.entity_id,
                        or_(
                            UserSubscriptionState.next_scheduled_at.is_(None),
                            UserSubscriptionState.next_scheduled_at > now,
                        ),
                        or_(
                            UserSubscriptionState.last_success_at >= cutoff,
                            and_(
                                UserSubscriptionState.terminal_reason.is_not(None),
                                UserSubscriptionState.last_attempt_at >= cutoff,
                            ),
                        ),
                    )
                )
            else:
                fresh = await session.scalar(
                    select(UserPostCollectionState.user_id).where(
                        UserPostCollectionState.user_id == job.entity_id,
                        or_(
                            UserPostCollectionState.next_scheduled_at.is_(None),
                            UserPostCollectionState.next_scheduled_at > now,
                        ),
                        or_(
                            UserPostCollectionState.last_success_at >= cutoff,
                            and_(
                                or_(
                                    UserPostCollectionState.wall_private.is_(True),
                                    UserPostCollectionState.unavailable.is_(True),
                                ),
                                UserPostCollectionState.last_attempt_at >= cutoff,
                            ),
                        ),
                    )
                )
        if fresh is None:
            return False
        await self._queue.finish(
            job.id,
            JobStatus.SKIPPED,
            error_type="fresh_recently_collected",
            error_message=f"Данные пользователя собраны менее {days} дн. назад",
        )
        return True

    async def _collect_subscriptions(self, job: ClaimedJob) -> None:
        offset = int(job.checkpoint.get("offset", 0))
        maximum = self._settings.collection_subscriptions_max_per_user
        page_size = self._settings.collection_subscriptions_page_size
        total = 0
        async with self._sessions() as session:
            campaign_id = await session.scalar(
                select(CollectionRun.campaign_id).where(CollectionRun.id == job.run_id)
            )
        while offset < maximum:
            count = min(page_size, maximum - offset)
            page = await self._client.get_subscription_ids_page(job.entity_id, offset, count)
            group_ids = sorted(set(page.group_ids))
            total = page.total_reported
            now = datetime.now(UTC)
            next_offset = page.next_offset
            truncated = total > maximum
            last_page = (
                next_offset >= maximum or next_offset >= total or page.returned_count < count
            )
            full_snapshot = last_page and next_offset >= total and not truncated
            async with self._sessions() as session:
                if group_ids:
                    await session.execute(
                        insert(VKCommunity)
                        .values(
                            [
                                {
                                    "vk_id": group_id,
                                    "name": "",
                                    "description": "",
                                    "status_text": "",
                                    "first_seen_at": now,
                                    "last_seen_at": now,
                                    "metadata_updated_at": None,
                                }
                                for group_id in group_ids
                            ]
                        )
                        .on_conflict_do_update(
                            index_elements=[VKCommunity.vk_id],
                            set_={
                                "last_seen_at": func.greatest(VKCommunity.last_seen_at, now),
                            },
                        )
                    )
                    await session.execute(
                        insert(UserGroupSubscription)
                        .values(
                            [
                                {
                                    "user_id": job.entity_id,
                                    "vk_group_id": group_id,
                                    "first_seen_at": now,
                                    "last_seen_at": now,
                                    "is_current": True,
                                    "source_run_id": job.run_id,
                                }
                                for group_id in group_ids
                            ]
                        )
                        .on_conflict_do_update(
                            constraint="uq_user_group_subscriptions",
                            set_={
                                "last_seen_at": now,
                                "is_current": True,
                                "source_run_id": job.run_id,
                            },
                        )
                    )
                await session.execute(
                    insert(UserSubscriptionState)
                    .values(
                        user_id=job.entity_id,
                        last_attempt_at=now,
                        total_reported=total,
                        collected_count=next_offset,
                        collected_limit=maximum,
                        privacy_denied=False,
                        is_truncated=truncated,
                        terminal_reason=None,
                        last_error_code=None,
                        last_run_id=job.run_id,
                        last_campaign_id=campaign_id,
                        last_success_at=now if last_page else None,
                        next_scheduled_at=now
                        + timedelta(days=self._settings.collection_subscriptions_ttl_days)
                        if last_page
                        else None,
                    )
                    .on_conflict_do_update(
                        index_elements=[UserSubscriptionState.user_id],
                        set_={
                            "last_attempt_at": now,
                            "total_reported": total,
                            "collected_count": next_offset,
                            "collected_limit": maximum,
                            "privacy_denied": False,
                            "is_truncated": truncated,
                            "terminal_reason": None,
                            "last_error_code": None,
                            "last_run_id": job.run_id,
                            "last_campaign_id": campaign_id,
                            **({"last_success_at": now} if last_page else {}),
                            **(
                                {
                                    "next_scheduled_at": now
                                    + timedelta(
                                        days=self._settings.collection_subscriptions_ttl_days
                                    )
                                }
                                if last_page
                                else {}
                            ),
                        },
                    )
                )
                if full_snapshot:
                    await session.execute(
                        update(UserGroupSubscription)
                        .where(
                            UserGroupSubscription.user_id == job.entity_id,
                            or_(
                                UserGroupSubscription.source_run_id != job.run_id,
                                UserGroupSubscription.source_run_id.is_(None),
                            ),
                        )
                        .values(is_current=False)
                    )
                await self._checkpoint(
                    session,
                    job.id,
                    {"offset": next_offset, "total": total, "truncated": truncated},
                    1,
                    len(group_ids),
                )
                await session.commit()
            offset = next_offset
            if last_page:
                break

    async def _mark_subscriptions_terminal(self, job: ClaimedJob, error_code: int) -> None:
        """Не включать terminal-ошибку подписок в каждую следующую cohort."""
        now = datetime.now(UTC)
        privacy_denied = error_code in {15, 30, 260}
        async with self._sessions() as session:
            campaign_id = await session.scalar(
                select(CollectionRun.campaign_id).where(CollectionRun.id == job.run_id)
            )
            terminal_reason = "privacy_or_access" if privacy_denied else "deleted_or_unavailable"
            await session.execute(
                insert(UserSubscriptionState)
                .values(
                    user_id=job.entity_id,
                    last_attempt_at=now,
                    collected_limit=self._settings.collection_subscriptions_max_per_user,
                    privacy_denied=privacy_denied,
                    is_truncated=False,
                    terminal_reason=terminal_reason,
                    last_error_code=error_code,
                    last_run_id=job.run_id,
                    last_campaign_id=campaign_id,
                    next_scheduled_at=now
                    + timedelta(days=self._settings.collection_subscriptions_ttl_days),
                )
                .on_conflict_do_update(
                    index_elements=[UserSubscriptionState.user_id],
                    set_={
                        "last_attempt_at": now,
                        "privacy_denied": privacy_denied,
                        "is_truncated": False,
                        "terminal_reason": terminal_reason,
                        "last_error_code": error_code,
                        "last_run_id": job.run_id,
                        "last_campaign_id": campaign_id,
                        "next_scheduled_at": now
                        + timedelta(days=self._settings.collection_subscriptions_ttl_days),
                    },
                )
            )
            await session.commit()

    async def _refresh_community_metadata(self, job: ClaimedJob) -> bool:
        """Process several community jobs with one groups.getById request."""
        extras = (
            await self._queue.claim_metadata_batch(
                job.run_id,
                limit=self._settings.collection_community_metadata_batch_size - 1,
            )
            if int(job.checkpoint.get("metadata_miss_count", 0)) < METADATA_BATCH_MISS_LIMIT
            else []
        )
        batch = sorted([job, *extras], key=lambda item: item.entity_id)
        requested_ids = [item.entity_id for item in batch]
        try:
            rows = await self._client.get_groups(requested_ids)
        except Exception:
            await self._queue.release(extras)
            raise
        by_id: dict[int, dict[str, Any]] = {}
        requested = set(requested_ids)
        for value in rows:
            raw_id = value.get("id")
            if (
                isinstance(raw_id, int)
                and not isinstance(raw_id, bool)
                and raw_id > 0
                and raw_id in requested
            ):
                by_id[raw_id] = value
        now = datetime.now(UTC)
        normalized = []
        for community_id in sorted(by_id):
            value = by_id[community_id]
            normalized.append(
                {
                    "vk_id": community_id,
                    "name": str(value.get("name", "")),
                    "description": str(value.get("description", "")),
                    "status_text": str(value.get("status", "")),
                    "screen_name": str(value["screen_name"]) if value.get("screen_name") else None,
                    "community_type": str(value["type"]) if value.get("type") else None,
                    "is_closed": bool(value.get("is_closed", False)),
                    "deactivated": str(value["deactivated"]) if value.get("deactivated") else None,
                    "members_count": value.get("members_count")
                    if isinstance(value.get("members_count"), int)
                    and not isinstance(value.get("members_count"), bool)
                    else None,
                    "city": value.get("city") if isinstance(value.get("city"), dict) else None,
                    "country": value.get("country")
                    if isinstance(value.get("country"), dict)
                    else None,
                    "first_seen_at": now,
                    "last_seen_at": now,
                    "metadata_updated_at": now,
                }
            )
        async with self._sessions() as session:
            if normalized:
                statement = insert(VKCommunity).values(normalized)
                await session.execute(
                    statement.on_conflict_do_update(
                        index_elements=[VKCommunity.vk_id],
                        set_={
                            "name": statement.excluded.name,
                            "description": statement.excluded.description,
                            "status_text": statement.excluded.status_text,
                            "screen_name": statement.excluded.screen_name,
                            "community_type": statement.excluded.community_type,
                            "is_closed": statement.excluded.is_closed,
                            "deactivated": statement.excluded.deactivated,
                            "members_count": statement.excluded.members_count,
                            "city": statement.excluded.city,
                            "country": statement.excluded.country,
                            "last_seen_at": statement.excluded.last_seen_at,
                            "metadata_updated_at": statement.excluded.metadata_updated_at,
                        },
                    )
                )
                # VK не отдаёт прирост подписчиков напрямую (нет такого метода) —
                # копим один снимок members_count в день на сообщество, чтобы
                # считать day-over-day прирост для оценки эффективности групп.
                snapshot_statement = insert(CommunityMemberSnapshot).values(
                    [
                        {
                            "community_vk_id": row["vk_id"],
                            "members_count": row["members_count"],
                            "captured_at": now,
                        }
                        for row in normalized
                    ]
                )
                await session.execute(
                    snapshot_statement.on_conflict_do_update(
                        constraint="uq_community_member_snapshots_community_date",
                        set_={
                            "members_count": snapshot_statement.excluded.members_count,
                            "captured_at": snapshot_statement.excluded.captured_at,
                        },
                    )
                )
            candidates = list(
                (
                    await session.scalars(
                        select(GroupCandidate).where(GroupCandidate.vk_id.in_(by_id))
                    )
                ).all()
            )
            for candidate in candidates:
                value = by_id[candidate.vk_id]
                candidate.name = str(value.get("name", candidate.name))
                candidate.description = str(value.get("description", ""))
                candidate.status_text = str(value.get("status", ""))
                candidate.screen_name = (
                    str(value["screen_name"]) if value.get("screen_name") else candidate.screen_name
                )
                candidate.last_seen_at = now
                deactivated = value.get("deactivated")
                await session.execute(
                    insert(GroupCollectionState)
                    .values(
                        group_id=candidate.id,
                        last_group_success_at=None if deactivated else now,
                        next_scheduled_at=now
                        + timedelta(days=self._settings.collection_community_metadata_ttl_days),
                        unavailable=bool(deactivated),
                        skip_reason=(f"deactivated:{deactivated}" if deactivated else None),
                        last_error_type=("community_deactivated" if deactivated else None),
                        last_error_message=(
                            "VK явно пометил сообщество как deactivated" if deactivated else None
                        ),
                    )
                    .on_conflict_do_update(
                        index_elements=[GroupCollectionState.group_id],
                        set_={
                            "last_group_success_at": None if deactivated else now,
                            "next_scheduled_at": now
                            + timedelta(days=self._settings.collection_community_metadata_ttl_days),
                            "unavailable": bool(deactivated),
                            "skip_reason": (f"deactivated:{deactivated}" if deactivated else None),
                            "last_error_type": ("community_deactivated" if deactivated else None),
                            "last_error_message": (
                                "VK явно пометил сообщество как deactivated"
                                if deactivated
                                else None
                            ),
                        },
                    )
                )
            await self._update_metrics(session, job.id, requests=1, updated=len(normalized))
            await session.commit()
        for extra in extras:
            available = extra.entity_id in by_id
            if available:
                await self._queue.finish(extra.id, JobStatus.COMPLETED)
            else:
                await self._finish_metadata_miss(extra)
        if job.entity_id not in by_id:
            await self._finish_metadata_miss(job)
            return False
        return True

    async def _finish_metadata_miss(self, job: ClaimedJob) -> None:
        """Persist bounded batch-to-single diagnostics for an omitted community ID."""
        previous_misses = int(job.checkpoint.get("metadata_miss_count", 0))
        miss_count = previous_misses + 1
        single_attempt = max(0, miss_count - METADATA_BATCH_MISS_LIMIT)
        terminal = single_attempt >= METADATA_SINGLE_MISS_LIMIT
        stage = "single" if miss_count > METADATA_BATCH_MISS_LIMIT else "batch"
        checkpoint = {**job.checkpoint, "metadata_miss_count": miss_count, "stage": stage}
        message = (
            f"groups.getById не вернул ID; stage={stage}; "
            f"attempt={job.attempt_count}; miss={miss_count}; single_miss={single_attempt}"
        )
        await self._record_error(job, "community_partial_response", message)
        if terminal:
            now = datetime.now(UTC)
            async with self._sessions() as session:
                await session.execute(
                    insert(VKCommunity)
                    .values(
                        vk_id=job.entity_id,
                        name="",
                        description="",
                        status_text="",
                        deactivated="unavailable",
                        first_seen_at=now,
                        last_seen_at=now,
                        metadata_updated_at=now,
                    )
                    .on_conflict_do_update(
                        index_elements=[VKCommunity.vk_id],
                        set_={
                            "deactivated": "unavailable",
                            "last_seen_at": now,
                            "metadata_updated_at": now,
                        },
                    )
                )
                candidate_id = await session.scalar(
                    select(GroupCandidate.id).where(GroupCandidate.vk_id == job.entity_id)
                )
                if candidate_id is not None:
                    await session.execute(
                        insert(GroupCollectionState)
                        .values(
                            group_id=candidate_id,
                            next_scheduled_at=now
                            + timedelta(days=self._settings.collection_community_metadata_ttl_days),
                            unavailable=True,
                            skip_reason="missing_from_groups_get_by_id",
                            last_error_type="community_missing_terminal",
                            last_error_message=message,
                        )
                        .on_conflict_do_update(
                            index_elements=[GroupCollectionState.group_id],
                            set_={
                                "next_scheduled_at": now
                                + timedelta(
                                    days=self._settings.collection_community_metadata_ttl_days
                                ),
                                "unavailable": True,
                                "skip_reason": "missing_from_groups_get_by_id",
                                "last_error_type": "community_missing_terminal",
                                "last_error_message": message,
                            },
                        )
                    )
                await session.commit()
            await self._queue.finish(
                job.id,
                JobStatus.SKIPPED,
                error_type="community_missing_terminal",
                error_message=message,
                checkpoint=checkpoint,
            )
            return
        delay = RETRY_DELAYS[min(miss_count - 1, len(RETRY_DELAYS) - 1)]
        await self._queue.finish(
            job.id,
            JobStatus.RETRY_WAIT,
            error_type="community_partial_response",
            error_message=message,
            retry_at=datetime.now(UTC) + timedelta(seconds=delay),
            checkpoint=checkpoint,
        )

    async def _collect_subscription_posts(self, job: ClaimedJob) -> None:
        """Сохранить не более 20 последних постов канонического сообщества."""
        async with self._sessions() as session:
            community = await session.get(VKCommunity, job.entity_id)
            candidate_id = await session.scalar(
                select(GroupCandidate.id).where(GroupCandidate.vk_id == job.entity_id)
            )
        if community is None:
            raise ValueError("Сообщество подписки не найдено")
        maximum = self._settings.collection_subscription_group_posts_max
        response = await self._client.get_wall_page(community.vk_id, 0, maximum)
        raw_items = response.get("items", [])
        items = (
            [item for item in raw_items if isinstance(item, dict)][:maximum]
            if isinstance(raw_items, list)
            else []
        )
        now = datetime.now(UTC)
        async with self._sessions() as session:
            for raw in items:
                published = _utc_from_timestamp(raw.get("date"))
                if published is None or not isinstance(raw.get("id"), int):
                    continue
                text = str(raw.get("text", ""))
                content_hash = hashlib.sha256(
                    f"{text}\0{raw.get('date')}\0{raw.get('edited')}".encode()
                ).hexdigest()
                post_id = await session.scalar(
                    insert(GroupPost)
                    .values(
                        vk_owner_id=int(raw.get("owner_id", -community.vk_id)),
                        vk_post_id=int(raw["id"]),
                        group_id=candidate_id,
                        community_vk_id=community.vk_id,
                        published_at=published,
                        edited_at=_utc_from_timestamp(raw.get("edited")),
                        text=text,
                        post_type=str(raw.get("post_type", "post")),
                        is_pinned=bool(raw.get("is_pinned", False)),
                        is_ad=bool(raw.get("is_ad", False)),
                        marked_as_ads=bool(raw.get("marked_as_ads", False)),
                        comments_count=_counter(raw.get("comments")),
                        likes_count=_counter(raw.get("likes")),
                        reposts_count=_counter(raw.get("reposts")),
                        views_count=_counter(raw.get("views")),
                        signer_vk_user_id=raw.get("signer_id")
                        if isinstance(raw.get("signer_id"), int)
                        else None,
                        source_updated_at=_utc_from_timestamp(raw.get("edited")) or published,
                        first_seen_at=now,
                        last_seen_at=now,
                        content_hash=content_hash,
                    )
                    .on_conflict_do_update(
                        constraint="uq_group_posts_owner_post",
                        set_={
                            "group_id": candidate_id,
                            "community_vk_id": community.vk_id,
                            "text": text,
                            "edited_at": _utc_from_timestamp(raw.get("edited")),
                            "comments_count": _counter(raw.get("comments")),
                            "likes_count": _counter(raw.get("likes")),
                            "reposts_count": _counter(raw.get("reposts")),
                            "views_count": _counter(raw.get("views")),
                            "last_seen_at": now,
                            "content_hash": content_hash,
                        },
                    )
                    .returning(GroupPost.id)
                )
                if post_id is None:
                    continue
                await session.execute(
                    delete(PostAttachment).where(PostAttachment.post_id == post_id)
                )
                raw_attachments = raw.get("attachments", [])
                attachments = raw_attachments if isinstance(raw_attachments, list) else []
                for position, attachment in enumerate(attachments):
                    if not isinstance(attachment, dict):
                        continue
                    normalized = normalize_attachment(attachment, position)
                    normalized["post_id"] = post_id
                    session.add(PostAttachment(**normalized))
            await self._checkpoint(
                session,
                job.id,
                {"offset": len(items), "total": int(response.get("count", 0))},
                1,
                len(items),
            )
            await session.execute(
                insert(CommunityPostCollectionState)
                .values(
                    community_vk_id=community.vk_id,
                    last_attempt_at=now,
                    last_success_at=now,
                    next_scheduled_at=now
                    + timedelta(days=self._settings.collection_subscription_group_posts_ttl_days),
                    last_run_id=job.run_id,
                    last_error_code=None,
                    last_error_reason=None,
                    collected_count=len(items),
                    wall_private=False,
                    unavailable=False,
                )
                .on_conflict_do_update(
                    index_elements=[CommunityPostCollectionState.community_vk_id],
                    set_={
                        "last_attempt_at": now,
                        "last_success_at": now,
                        "next_scheduled_at": now
                        + timedelta(
                            days=self._settings.collection_subscription_group_posts_ttl_days
                        ),
                        "last_run_id": job.run_id,
                        "last_error_code": None,
                        "last_error_reason": None,
                        "collected_count": len(items),
                        "wall_private": False,
                        "unavailable": False,
                    },
                )
            )
            await session.commit()

    async def _collect_user_posts(self, job: ClaimedJob) -> None:
        """Сохранить не более 20 последних постов пользователя не старше полугода (180 дней)."""
        async with self._sessions() as session:
            user = await session.get(VKUser, job.entity_id)
        if user is None:
            raise VKAPIError(18, "Пользователь не найден")
        if user.deactivated:
            raise VKAPIError(18, f"Пользователь деактивирован: {user.deactivated}")
        if user.is_closed and not user.can_access_closed:
            raise VKAPIError(30, "Профиль пользователя закрыт")

        offset = int(job.checkpoint.get("offset", 0))
        maximum = self._settings.collection_user_posts_max_per_user
        page_size = min(self._settings.collection_user_posts_page_size, maximum)
        now = datetime.now(UTC)
        cutoff = now - timedelta(days=self._settings.collection_user_posts_window_days)
        if self._settings.collection_user_posts_stop_at_date:
            try:
                custom_cutoff = datetime.fromisoformat(
                    self._settings.collection_user_posts_stop_at_date
                )
                custom_cutoff = (
                    custom_cutoff.replace(tzinfo=UTC)
                    if custom_cutoff.tzinfo is None
                    else custom_cutoff.astimezone(UTC)
                )
                cutoff = max(cutoff, custom_cutoff)
            except ValueError:
                pass

        total_collected = int(job.checkpoint.get("collected", 0))
        while total_collected < maximum:
            count = min(page_size, maximum - total_collected)
            response = await self._client.get_user_wall_page(user.vk_id, offset, count)
            raw_items = response.get("items", [])
            items = (
                [item for item in raw_items if isinstance(item, dict)]
                if isinstance(raw_items, list)
                else []
            )
            if not items:
                break

            reached_cutoff = False
            async with self._sessions() as session:
                changed = 0
                for raw in items:
                    published = _utc_from_timestamp(raw.get("date"))
                    if published is None or not isinstance(raw.get("id"), int):
                        continue
                    if published < cutoff and bool(raw.get("is_pinned", False)):
                        # VK can put an old pinned post before newer wall entries. It is not
                        # evidence that the rest of the page is outside the 180-day window.
                        continue
                    if published < cutoff:
                        reached_cutoff = True
                        break

                    text = str(raw.get("text", ""))
                    content_hash = hashlib.sha256(
                        f"{text}\0{raw.get('date')}\0{raw.get('edited')}".encode()
                    ).hexdigest()

                    post_id = await session.scalar(
                        insert(UserPost)
                        .values(
                            vk_owner_id=int(raw.get("owner_id", user.vk_id)),
                            vk_post_id=int(raw["id"]),
                            user_id=user.vk_id,
                            published_at=published,
                            edited_at=_utc_from_timestamp(raw.get("edited")),
                            text=text,
                            post_type=str(raw.get("post_type", "post")),
                            is_pinned=bool(raw.get("is_pinned", False)),
                            comments_count=_counter(raw.get("comments")),
                            likes_count=_counter(raw.get("likes")),
                            reposts_count=_counter(raw.get("reposts")),
                            views_count=_counter(raw.get("views")),
                            signer_vk_user_id=raw.get("signer_id")
                            if isinstance(raw.get("signer_id"), int)
                            else None,
                            source_updated_at=_utc_from_timestamp(raw.get("edited")) or published,
                            first_seen_at=now,
                            last_seen_at=now,
                            content_hash=content_hash,
                        )
                        .on_conflict_do_update(
                            constraint="uq_user_posts_owner_post",
                            set_={
                                "user_id": user.vk_id,
                                "published_at": published,
                                "text": text,
                                "edited_at": _utc_from_timestamp(raw.get("edited")),
                                "post_type": str(raw.get("post_type", "post")),
                                "is_pinned": bool(raw.get("is_pinned", False)),
                                "comments_count": _counter(raw.get("comments")),
                                "likes_count": _counter(raw.get("likes")),
                                "reposts_count": _counter(raw.get("reposts")),
                                "views_count": _counter(raw.get("views")),
                                "signer_vk_user_id": raw.get("signer_id")
                                if isinstance(raw.get("signer_id"), int)
                                else None,
                                "source_updated_at": _utc_from_timestamp(raw.get("edited"))
                                or published,
                                "last_seen_at": now,
                                "content_hash": content_hash,
                            },
                        )
                        .returning(UserPost.id)
                    )
                    if post_id is None:
                        continue

                    raw_attachments = raw.get("attachments", [])
                    attachments = (
                        [item for item in raw_attachments if isinstance(item, dict)]
                        if isinstance(raw_attachments, list)
                        else []
                    )
                    for position, attachment in enumerate(attachments):
                        normalized = normalize_attachment(attachment, position)
                        normalized["post_id"] = post_id
                        attachment_values = {
                            ("metadata" if key == "attachment_metadata" else key): value
                            for key, value in normalized.items()
                        }
                        attachment_table = cast(Table, UserPostAttachment.__table__)
                        await session.execute(
                            insert(attachment_table)
                            .values(**attachment_values)
                            .on_conflict_do_update(
                                constraint="uq_user_post_attachments_post_position",
                                set_={
                                    attachment_table.c[key]: value
                                    for key, value in attachment_values.items()
                                    if key not in {"post_id", "position"}
                                },
                            )
                        )
                    stale_attachments = delete(UserPostAttachment).where(
                        UserPostAttachment.post_id == post_id
                    )
                    if attachments:
                        stale_attachments = stale_attachments.where(
                            UserPostAttachment.position.not_in(range(len(attachments)))
                        )
                    await session.execute(stale_attachments)
                    changed += 1
                    total_collected += 1
                    if total_collected >= maximum:
                        break

                offset += len(items)
                checkpoint: dict[str, object] = {
                    "offset": offset,
                    "total": int(response.get("count", 0)),
                    "collected": total_collected,
                }
                await self._checkpoint(session, job.id, checkpoint, 1, changed)
                await session.commit()

            total_vk = int(response.get("count", 0))
            if (
                reached_cutoff
                or total_collected >= maximum
                or offset >= total_vk
                or len(items) < count
            ):
                break

        async with self._sessions() as session:
            await session.execute(
                insert(UserPostCollectionState)
                .values(
                    user_id=user.vk_id,
                    last_attempt_at=now,
                    last_success_at=now,
                    next_scheduled_at=now
                    + timedelta(days=self._settings.collection_user_posts_ttl_days),
                    last_run_id=job.run_id,
                    last_error_code=None,
                    last_error_reason=None,
                    collected_count=total_collected,
                    wall_private=False,
                    unavailable=False,
                )
                .on_conflict_do_update(
                    index_elements=[UserPostCollectionState.user_id],
                    set_={
                        "last_attempt_at": now,
                        "last_success_at": now,
                        "next_scheduled_at": now
                        + timedelta(days=self._settings.collection_user_posts_ttl_days),
                        "last_run_id": job.run_id,
                        "last_error_code": None,
                        "last_error_reason": None,
                        "collected_count": total_collected,
                        "wall_private": False,
                        "unavailable": False,
                    },
                )
            )
            await session.commit()

    async def _mark_user_post_failure(
        self,
        job: ClaimedJob,
        error_code: int | None,
        reason: str,
        *,
        terminal: bool,
    ) -> None:
        user_id = job.entity_id
        now = datetime.now(UTC)
        async with self._sessions() as session:
            await session.execute(
                insert(UserPostCollectionState)
                .values(
                    user_id=user_id,
                    last_attempt_at=now,
                    next_scheduled_at=now
                    + timedelta(days=self._settings.collection_user_posts_ttl_days)
                    if terminal
                    else None,
                    last_run_id=job.run_id,
                    last_error_code=error_code,
                    last_error_reason=reason[:255],
                    wall_private=terminal and error_code in {7, 15, 30, 260},
                    unavailable=terminal and error_code in {18},
                )
                .on_conflict_do_update(
                    index_elements=[UserPostCollectionState.user_id],
                    set_={
                        "last_attempt_at": now,
                        "next_scheduled_at": now
                        + timedelta(days=self._settings.collection_user_posts_ttl_days)
                        if terminal
                        else None,
                        "last_run_id": job.run_id,
                        "last_error_code": error_code,
                        "last_error_reason": reason[:255],
                        "wall_private": terminal and error_code in {7, 15, 30, 260},
                        "unavailable": terminal and error_code in {18},
                    },
                )
            )
            await session.commit()

    async def _mark_post_failure(
        self,
        job: ClaimedJob,
        error_code: int | None,
        reason: str,
        *,
        terminal: bool,
    ) -> None:
        community_id = job.entity_id
        if job.job_type == "collect_group_posts":
            try:
                community_id = (await self._group(job.entity_id)).vk_id
            except ValueError:
                return
        now = datetime.now(UTC)
        async with self._sessions() as session:
            await session.execute(
                insert(CommunityPostCollectionState)
                .values(
                    community_vk_id=community_id,
                    last_attempt_at=now,
                    next_scheduled_at=now
                    + timedelta(days=self._settings.collection_subscription_group_posts_ttl_days)
                    if terminal
                    else None,
                    last_run_id=job.run_id,
                    last_error_code=error_code,
                    last_error_reason=reason[:255],
                    wall_private=terminal and error_code in {7, 15},
                    unavailable=terminal and error_code in {18, 30},
                )
                .on_conflict_do_update(
                    index_elements=[CommunityPostCollectionState.community_vk_id],
                    set_={
                        "last_attempt_at": now,
                        "next_scheduled_at": now
                        + timedelta(
                            days=self._settings.collection_subscription_group_posts_ttl_days
                        )
                        if terminal
                        else None,
                        "last_run_id": job.run_id,
                        "last_error_code": error_code,
                        "last_error_reason": reason[:255],
                        "wall_private": terminal and error_code in {7, 15},
                        "unavailable": terminal and error_code in {18, 30},
                    },
                )
            )
            await session.commit()

    async def _checkpoint(
        self,
        session: AsyncSession,
        job_id: uuid.UUID,
        checkpoint: dict[str, object],
        requests: int,
        rows: int,
    ) -> None:
        await session.execute(
            update(CollectionJob)
            .where(CollectionJob.id == job_id)
            .values(
                checkpoint=checkpoint,
                progress_offset=(
                    checkpoint["offset"] if isinstance(checkpoint.get("offset"), int) else 0
                ),
                heartbeat_at=datetime.now(UTC),
                api_requests=CollectionJob.api_requests + requests,
                rows_updated=CollectionJob.rows_updated + rows,
            )
        )

    async def _update_metrics(
        self, session: AsyncSession, job_id: uuid.UUID, *, requests: int, updated: int
    ) -> None:
        await session.execute(
            update(CollectionJob)
            .where(CollectionJob.id == job_id)
            .values(
                api_requests=CollectionJob.api_requests + requests,
                rows_updated=CollectionJob.rows_updated + updated,
                heartbeat_at=datetime.now(UTC),
            )
        )

    async def _record_error(
        self,
        job: ClaimedJob,
        category: str,
        message: str,
        *,
        vk_code: int | None = None,
    ) -> None:
        async with self._sessions() as session:
            session.add(
                CollectionJobError(
                    collection_run_id=job.run_id,
                    job_id=job.id,
                    endpoint=JOB_METHODS.get(job.job_type, job.job_type),
                    error_category=category,
                    vk_error_code=vk_code,
                    attempt=job.attempt_count,
                    sanitized_message=message,
                )
            )
            await session.commit()

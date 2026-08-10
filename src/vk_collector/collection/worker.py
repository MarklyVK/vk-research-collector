from __future__ import annotations

import asyncio
import contextlib
import hashlib
import logging
import time
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import delete, or_, select, update
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
    GroupCandidate,
    GroupCollectionState,
    GroupMembership,
    GroupPost,
    JobStatus,
    PostAttachment,
    UserGroupSubscription,
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

TERMINAL_VK_CODES = frozenset({7, 15, 18, 30})
JOB_METHODS = {
    "refresh_group": "groups.getById",
    "collect_group_posts": "wall.get",
    "collect_group_members": "groups.getMembers",
    "refresh_user_profile": "users.get",
    "collect_user_subscriptions": "groups.get",
    "collect_subscription_group_posts": "wall.get",
}
FAIR_JOB_TYPES = tuple(JOB_METHODS)
RETRY_DELAYS = (60, 300, 900, 3600, 21600)
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
            while len(tasks) < self._settings.collection_max_concurrency and (
                max_jobs is None or claimed < max_jobs
            ):
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
                    "subscription_posts": "collect_subscription_group_posts",
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
                await self._refresh_group(job)
            elif job.job_type == "collect_group_posts":
                await self._collect_posts(job)
            elif job.job_type == "collect_group_members":
                await self._collect_members(job)
            elif job.job_type == "refresh_user_profile":
                await self._refresh_user(job)
            elif job.job_type == "collect_user_subscriptions":
                await self._collect_subscriptions(job)
            elif job.job_type == "collect_subscription_group_posts":
                await self._collect_subscription_posts(job)
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
            subscriptions_private = exc.code == 260 and job.job_type == "collect_user_subscriptions"
            category = (
                "subscriptions_private"
                if subscriptions_private
                else "permission_or_private"
                if exc.code in TERMINAL_VK_CODES
                else "vk_api"
            )
            await self._record_error(job, category, message, vk_code=exc.code)
            if subscriptions_private:
                await self._mark_subscriptions_private(job)
                await self._queue.finish(
                    job.id, JobStatus.SKIPPED, error_type=category, error_message=message
                )
            elif exc.code in TERMINAL_VK_CODES:
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
        if job.attempt_count >= len(RETRY_DELAYS):
            await self._queue.finish(
                job.id, JobStatus.FAILED, error_type=category, error_message=message
            )
            return
        retry_at = datetime.now(UTC) + timedelta(seconds=RETRY_DELAYS[job.attempt_count - 1])
        await self._queue.finish(
            job.id,
            JobStatus.RETRY_WAIT,
            error_type=category,
            error_message=message,
            retry_at=retry_at,
        )

    async def _group(self, group_id: int) -> GroupCandidate:
        async with self._sessions() as session:
            group = await session.get(GroupCandidate, group_id)
            if group is None:
                raise ValueError("Approved-группа не найдена")
            return group

    async def _refresh_group(self, job: ClaimedJob) -> None:
        group = await self._group(job.entity_id)
        rows = await self._client.get_groups([group.vk_id])
        if not rows:
            raise VKAPIError(18, "Группа удалена или недоступна")
        value = rows[0]
        if value.get("deactivated") or int(value.get("is_closed", 0)) != 0:
            raise VKAPIError(15, "Группа закрыта, удалена или заблокирована")
        now = datetime.now(UTC)
        async with self._sessions() as session:
            current = await session.get(GroupCandidate, group.id, with_for_update=True)
            if current is None:
                raise ValueError("Группа исчезла во время обновления")
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
                    set_={"last_group_success_at": now, "unavailable": False, "skip_reason": None},
                )
            )
            await self._update_metrics(session, job.id, requests=1, updated=1)
            await session.commit()

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
            await session.execute(
                insert(GroupCollectionState)
                .values(
                    group_id=group.id,
                    posts_checkpoint={"offset": offset},
                    last_posts_success_at=datetime.now(UTC),
                )
                .on_conflict_do_update(
                    index_elements=[GroupCollectionState.group_id],
                    set_={
                        "posts_checkpoint": {"offset": offset},
                        "last_posts_success_at": datetime.now(UTC),
                    },
                )
            )
            await session.commit()

    async def _collect_members(self, job: ClaimedJob) -> None:
        group = await self._group(job.entity_id)
        offset = int(job.checkpoint.get("offset", 0))
        maximum = self._settings.collection_members_max_per_group
        page_size = self._settings.collection_members_page_size
        total = 0
        while maximum is None or offset < maximum:
            count = page_size if maximum is None else min(page_size, maximum - offset)
            response = await self._client.get_members_page(group.vk_id, offset, count)
            raw_items = response.get("items", [])
            values = raw_items if isinstance(raw_items, list) else []
            discovered_user_ids: set[int] = set()
            for item in values:
                if isinstance(item, int):
                    discovered_user_ids.add(item)
                elif isinstance(item, dict):
                    raw_id = item.get("id")
                    if isinstance(raw_id, int):
                        discovered_user_ids.add(raw_id)
            user_ids = sorted(discovered_user_ids)
            total = int(response.get("count", 0))
            now = datetime.now(UTC)
            async with self._sessions() as session:
                for user_id in user_ids:
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
                if self._settings.collection_users_enabled and user_ids:
                    cutoff = now - timedelta(days=self._settings.collection_user_profile_ttl_days)
                    stale_ids = list(
                        (
                            await session.scalars(
                                select(VKUser.vk_id).where(
                                    VKUser.vk_id.in_(user_ids),
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
                offset += len(values)
                await self._checkpoint(
                    session, job.id, {"offset": offset, "total": total}, 1, len(user_ids)
                )
                await session.commit()
            if not values or offset >= total or len(values) < count:
                break
        full_snapshot = offset >= total and (maximum is None or total <= maximum)
        async with self._sessions() as session:
            if full_snapshot:
                await session.execute(
                    update(GroupMembership)
                    .where(
                        GroupMembership.group_id == group.id,
                        GroupMembership.source_run_id != job.run_id,
                    )
                    .values(is_current=False)
                )
            now = datetime.now(UTC)
            await session.execute(
                insert(GroupCollectionState)
                .values(
                    group_id=group.id,
                    members_checkpoint={"offset": offset, "complete": full_snapshot},
                    last_members_success_at=now,
                )
                .on_conflict_do_update(
                    index_elements=[GroupCollectionState.group_id],
                    set_={
                        "members_checkpoint": {"offset": offset, "complete": full_snapshot},
                        "last_members_success_at": now,
                    },
                )
            )
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
                            "last_seen_at": now,
                            "profile_updated_at": now,
                        },
                    )
                )
                if self._settings.collection_subscriptions_enabled and not value.get("is_closed"):
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

    async def _collect_subscriptions(self, job: ClaimedJob) -> None:
        offset = int(job.checkpoint.get("offset", 0))
        maximum = self._settings.collection_subscriptions_max_per_user
        page_size = self._settings.collection_subscriptions_page_size
        total = 0
        while offset < maximum:
            count = min(page_size, maximum - offset)
            response = await self._client.get_subscriptions_page(job.entity_id, offset, count)
            raw_items = response.get("items", [])
            communities = (
                [
                    item
                    for item in raw_items
                    if isinstance(item, dict) and isinstance(item.get("id"), int)
                ]
                if isinstance(raw_items, list)
                else []
            )
            total = int(response.get("count", 0))
            now = datetime.now(UTC)
            async with self._sessions() as session:
                for value in communities:
                    group_id = int(value["id"])
                    await session.execute(
                        insert(VKCommunity)
                        .values(
                            vk_id=group_id,
                            name=str(value.get("name", "")),
                            description=str(value.get("description", "")),
                            status_text=str(value.get("status", "")),
                            screen_name=str(value["screen_name"])
                            if value.get("screen_name")
                            else None,
                            community_type=str(value["type"]) if value.get("type") else None,
                            is_closed=bool(value.get("is_closed", False)),
                            deactivated=str(value["deactivated"])
                            if value.get("deactivated")
                            else None,
                            members_count=value.get("members_count")
                            if isinstance(value.get("members_count"), int)
                            else None,
                            first_seen_at=now,
                            last_seen_at=now,
                            metadata_updated_at=now,
                        )
                        .on_conflict_do_update(
                            index_elements=[VKCommunity.vk_id],
                            set_={
                                "name": str(value.get("name", "")),
                                "description": str(value.get("description", "")),
                                "status_text": str(value.get("status", "")),
                                "screen_name": str(value["screen_name"])
                                if value.get("screen_name")
                                else None,
                                "community_type": str(value["type"]) if value.get("type") else None,
                                "is_closed": bool(value.get("is_closed", False)),
                                "deactivated": str(value["deactivated"])
                                if value.get("deactivated")
                                else None,
                                "members_count": value.get("members_count")
                                if isinstance(value.get("members_count"), int)
                                else None,
                                "last_seen_at": now,
                                "metadata_updated_at": now,
                            },
                        )
                    )
                    await session.execute(
                        insert(UserGroupSubscription)
                        .values(
                            user_id=job.entity_id,
                            vk_group_id=group_id,
                            first_seen_at=now,
                            last_seen_at=now,
                            is_current=True,
                            source_run_id=job.run_id,
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
                    if (
                        self._settings.collection_subscription_group_posts_enabled
                        and not value.get("deactivated")
                        and not bool(value.get("is_closed", False))
                    ):
                        await session.execute(
                            insert(CollectionJob)
                            .values(
                                collection_run_id=job.run_id,
                                job_type="collect_subscription_group_posts",
                                entity_type="community",
                                entity_id=group_id,
                                priority=60,
                            )
                            .on_conflict_do_nothing(constraint="uq_collection_jobs_run_type_entity")
                        )
                offset += len(raw_items) if isinstance(raw_items, list) else 0
                await session.execute(
                    insert(UserSubscriptionState)
                    .values(
                        user_id=job.entity_id,
                        last_attempt_at=now,
                        total_reported=total,
                        collected_count=offset,
                        collected_limit=maximum,
                        privacy_denied=False,
                        last_error_code=None,
                        last_run_id=job.run_id,
                    )
                    .on_conflict_do_update(
                        index_elements=[UserSubscriptionState.user_id],
                        set_={
                            "last_attempt_at": now,
                            "total_reported": total,
                            "collected_count": offset,
                            "collected_limit": maximum,
                            "privacy_denied": False,
                            "last_error_code": None,
                            "last_run_id": job.run_id,
                        },
                    )
                )
                await self._checkpoint(
                    session,
                    job.id,
                    {"offset": offset, "total": total, "truncated": total > maximum},
                    1,
                    len(communities),
                )
                await session.commit()
            if not raw_items or offset >= total or len(raw_items) < count:
                break
        full_snapshot = offset >= total and total <= maximum
        async with self._sessions() as session:
            if full_snapshot:
                await session.execute(
                    update(UserGroupSubscription)
                    .where(
                        UserGroupSubscription.user_id == job.entity_id,
                        UserGroupSubscription.source_run_id != job.run_id,
                    )
                    .values(is_current=False)
                )
            now = datetime.now(UTC)
            await session.execute(
                insert(UserSubscriptionState)
                .values(
                    user_id=job.entity_id,
                    last_attempt_at=now,
                    last_success_at=now,
                    total_reported=total,
                    collected_count=offset,
                    collected_limit=maximum,
                    privacy_denied=False,
                    last_run_id=job.run_id,
                    next_scheduled_at=now
                    + timedelta(days=self._settings.collection_subscriptions_ttl_days),
                )
                .on_conflict_do_update(
                    index_elements=[UserSubscriptionState.user_id],
                    set_={
                        "last_attempt_at": now,
                        "last_success_at": now,
                        "total_reported": total,
                        "collected_count": offset,
                        "collected_limit": maximum,
                        "privacy_denied": False,
                        "last_run_id": job.run_id,
                        "next_scheduled_at": now
                        + timedelta(days=self._settings.collection_subscriptions_ttl_days),
                    },
                )
            )
            await session.commit()

    async def _mark_subscriptions_private(self, job: ClaimedJob) -> None:
        now = datetime.now(UTC)
        async with self._sessions() as session:
            await session.execute(
                insert(UserSubscriptionState)
                .values(
                    user_id=job.entity_id,
                    last_attempt_at=now,
                    collected_limit=self._settings.collection_subscriptions_max_per_user,
                    privacy_denied=True,
                    last_error_code=260,
                    last_run_id=job.run_id,
                    next_scheduled_at=now
                    + timedelta(days=self._settings.collection_subscriptions_ttl_days),
                )
                .on_conflict_do_update(
                    index_elements=[UserSubscriptionState.user_id],
                    set_={
                        "last_attempt_at": now,
                        "privacy_denied": True,
                        "last_error_code": 260,
                        "last_run_id": job.run_id,
                        "next_scheduled_at": now
                        + timedelta(days=self._settings.collection_subscriptions_ttl_days),
                    },
                )
            )
            await session.commit()

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

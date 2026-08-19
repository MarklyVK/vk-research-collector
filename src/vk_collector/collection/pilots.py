from __future__ import annotations

import hashlib
import json
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import func, or_, select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from vk_collector.collection.queue import CollectionQueue
from vk_collector.config import Settings
from vk_collector.database.models import (
    CollectionJob,
    CollectionRun,
    CollectionRunStatus,
    JobStatus,
)

PILOT_SCOPES = ("subscriptions_pilot", "subscription_posts_pilot", "user_posts_pilot")
TERMINAL_RUN_STATUSES = (
    CollectionRunStatus.COMPLETED,
    CollectionRunStatus.COMPLETED_WITH_ERRORS,
    CollectionRunStatus.FAILED,
    CollectionRunStatus.CANCELLED,
)


def choose_pilot_control_action(rows: list[dict[str, Any]]) -> dict[str, Any]:
    """Choose a deterministic hourly-control action from classified pilot state."""
    unfinished = [row for row in rows if row["classification"] != "terminal"]
    if not unfinished:
        return {"action": "create", "run_id": None, "reason": "unfinished pilot отсутствует"}
    actionable = [
        row
        for row in unfinished
        if row["classification"] in {"compatible_recoverable", "stale_running_lease"}
    ]
    waiting = [row for row in unfinished if row["classification"] == "waiting"]
    if len(unfinished) == 1 and len(actionable) == 1:
        return {
            "action": "resume",
            "run_id": actionable[0]["run_id"],
            "scope": actionable[0].get("scope"),
            "reason": actionable[0]["classification"],
        }
    if len(unfinished) == 1 and len(waiting) == 1:
        return {
            "action": "wait",
            "run_id": waiting[0]["run_id"],
            "scope": waiting[0].get("scope"),
            "next_wakeup_at": waiting[0]["nearest_retry"],
            "reason": "persisted retry ещё не наступил",
        }
    return {
        "action": "operator_required",
        "run_id": None,
        "pilot_ids": [row["run_id"] for row in unfinished],
        "reason": "Незавершённые pilot неоднозначны или несовместимы",
    }


async def pilot_previews(
    sessions: async_sessionmaker[AsyncSession], settings: Settings
) -> list[dict[str, Any]]:
    """Return read-only classifications for every historical subscription pilot."""
    now = datetime.now(UTC)
    stale_before = now - timedelta(seconds=settings.collection_job_lease_seconds)
    expected = CollectionQueue(sessions, settings).collection_configuration()
    async with sessions() as session:
        runs = list(
            (
                await session.scalars(
                    select(CollectionRun)
                    .where(CollectionRun.scope.in_(PILOT_SCOPES))
                    .order_by(CollectionRun.created_at, CollectionRun.id)
                )
            ).all()
        )
        result: list[dict[str, Any]] = []
        for run in runs:
            counts = (
                await session.execute(
                    select(CollectionJob.status, func.count(CollectionJob.id))
                    .where(CollectionJob.collection_run_id == run.id)
                    .group_by(CollectionJob.status)
                )
            ).all()
            by_status = {status.value: int(count) for status, count in counts}
            stale = int(
                await session.scalar(
                    select(func.count(CollectionJob.id)).where(
                        CollectionJob.collection_run_id == run.id,
                        CollectionJob.status == JobStatus.RUNNING,
                        CollectionJob.locked_at < stale_before,
                    )
                )
                or 0
            )
            nearest_retry = await session.scalar(
                select(func.min(CollectionJob.next_attempt_at)).where(
                    CollectionJob.collection_run_id == run.id,
                    CollectionJob.status == JobStatus.RETRY_WAIT,
                )
            )
            ready = int(
                await session.scalar(
                    select(func.count(CollectionJob.id)).where(
                        CollectionJob.collection_run_id == run.id,
                        CollectionJob.status.in_([JobStatus.PENDING, JobStatus.RETRY_WAIT]),
                        or_(
                            CollectionJob.next_attempt_at.is_(None),
                            CollectionJob.next_attempt_at <= now,
                        ),
                    )
                )
                or 0
            )
            compatible = run.configuration.get("collection") == expected
            active_jobs = sum(
                by_status.get(status, 0)
                for status in ("pending", "running", "retry_wait", "paused")
            )
            if run.status in TERMINAL_RUN_STATUSES:
                classification = "terminal"
                recommendation = "history only"
            elif not compatible:
                classification = "incompatible_configuration"
                recommendation = (
                    f"collection subscriptions cancel-pilot --run-id {run.id} --confirm"
                )
            elif run.status == CollectionRunStatus.PAUSED_NO_TOKENS:
                classification = "paused_no_tokens"
                recommendation = "восстановить токены, затем явно resume по run ID"
            elif run.status == CollectionRunStatus.PAUSED:
                classification = "operator_paused"
                recommendation = "явно resume или cancel-pilot --confirm"
            elif stale:
                classification = "stale_running_lease"
                recommendation = f"collection subscriptions pilot --run-id {run.id}"
            elif nearest_retry is not None and nearest_retry > now and ready == 0:
                classification = "waiting"
                recommendation = f"не запускать раньше {nearest_retry.isoformat()}"
            elif active_jobs:
                classification = "compatible_recoverable"
                recommendation = f"collection subscriptions pilot --run-id {run.id}"
            else:
                classification = "obsolete"
                recommendation = (
                    f"collection subscriptions cancel-pilot --run-id {run.id} --confirm"
                )
            result.append(
                {
                    "run_id": str(run.id),
                    "scope": run.scope,
                    "status": run.status.value,
                    "plan_hash": run.configuration.get("plan_key"),
                    "user_ids_hash": run.configuration.get("user_ids_hash"),
                    "runtime_configuration_hash": hashlib.sha256(
                        json.dumps(
                            run.configuration.get("collection", {}),
                            sort_keys=True,
                            separators=(",", ":"),
                        ).encode()
                    ).hexdigest(),
                    "jobs": {
                        key: by_status.get(key, 0)
                        for key in ("pending", "running", "retry_wait", "paused")
                    },
                    "stale_leases": stale,
                    "nearest_retry": nearest_retry.isoformat() if nearest_retry else None,
                    "compatible": compatible,
                    "classification": classification,
                    "recommended_action": recommendation,
                }
            )
        return result


async def cancel_pilot(
    sessions: async_sessionmaker[AsyncSession], run_id: uuid.UUID, *, reason: str
) -> dict[str, Any]:
    """Cancel one explicit pilot without deleting jobs, checkpoints or collected data."""
    now = datetime.now(UTC)
    async with sessions() as session:
        run = await session.get(CollectionRun, run_id, with_for_update=True)
        if run is None or run.scope not in PILOT_SCOPES:
            raise ValueError("Subscription pilot не найден")
        if run.status in TERMINAL_RUN_STATUSES:
            raise ValueError("Pilot уже terminal")
        changed = await session.execute(
            update(CollectionJob)
            .where(
                CollectionJob.collection_run_id == run_id,
                CollectionJob.status.in_(
                    [JobStatus.PENDING, JobStatus.RUNNING, JobStatus.RETRY_WAIT, JobStatus.PAUSED]
                ),
            )
            .values(
                status=JobStatus.CANCELLED,
                locked_at=None,
                locked_by=None,
                heartbeat_at=None,
                finished_at=now,
                last_error_type="operator_cancelled_pilot",
                last_error_message=reason,
            )
        )
        run.status = CollectionRunStatus.CANCELLED
        run.finished_at = now
        run.next_wakeup_at = None
        run.error_message = reason
        await session.commit()
        return {
            "run_id": str(run_id),
            "status": "cancelled",
            "cancelled_jobs": int(changed.rowcount or 0),  # type: ignore[attr-defined]
            "history_deleted": False,
        }


async def finalize_deferred_subscription_pilot(
    sessions: async_sessionmaker[AsyncSession],
    settings: Settings,
    run_id: uuid.UUID,
    *,
    confirmation: str,
) -> dict[str, Any]:
    """Finish a representative Pilot A without waiting hours for isolated transient users.

    Deferred users are not treated as resolved: only their pilot jobs become skipped. A
    later production campaign snapshots the same due users and creates fresh durable jobs
    for them. No checkpoints, errors, states, subscriptions or other collected data are
    deleted.
    """
    expected_confirmation = "FINALIZE_DEFERRED_SUBSCRIPTION_PILOT"
    if confirmation != expected_confirmation:
        raise ValueError(f"Требуется точное confirmation {expected_confirmation}")
    now = datetime.now(UTC)
    expected = CollectionQueue(sessions, settings).collection_configuration()
    async with sessions() as session:
        run = await session.get(CollectionRun, run_id, with_for_update=True)
        if run is None or run.scope != "subscriptions_pilot":
            raise ValueError("Pilot A не найден")
        if run.status in TERMINAL_RUN_STATUSES:
            raise ValueError("Pilot A уже terminal")
        if run.configuration.get("collection") != expected:
            raise ValueError("Pilot A несовместим с текущей runtime-конфигурацией")
        counts = (
            await session.execute(
                select(CollectionJob.status, func.count(CollectionJob.id))
                .where(CollectionJob.collection_run_id == run_id)
                .group_by(CollectionJob.status)
            )
        ).all()
        by_status = {status: int(count) for status, count in counts}
        if by_status.get(JobStatus.FAILED, 0):
            raise ValueError("Pilot A содержит failed jobs и не может быть завершён как измеряемый")
        if any(
            by_status.get(status, 0)
            for status in (JobStatus.PENDING, JobStatus.RUNNING, JobStatus.PAUSED)
        ):
            raise ValueError("Pilot A ещё содержит готовые или выполняющиеся jobs")
        completed = by_status.get(JobStatus.COMPLETED, 0)
        if completed < settings.collection_subscription_pilot_min_users:
            raise ValueError("Pilot A не набрал минимальное число успешных измерений")
        deferred = by_status.get(JobStatus.RETRY_WAIT, 0)
        if deferred <= 0:
            raise ValueError("Pilot A не содержит deferred jobs")
        changed = await session.execute(
            update(CollectionJob)
            .where(
                CollectionJob.collection_run_id == run_id,
                CollectionJob.status == JobStatus.RETRY_WAIT,
            )
            .values(
                status=JobStatus.SKIPPED,
                next_attempt_at=None,
                locked_at=None,
                locked_by=None,
                heartbeat_at=None,
                finished_at=now,
                last_error_type="pilot_measurement_deferred",
                last_error_message=(
                    "Transient pilot job отложен до production campaign; "
                    "собранные данные и checkpoint сохранены"
                ),
                error_message=(
                    "Transient pilot job отложен до production campaign; "
                    "собранные данные и checkpoint сохранены"
                ),
            )
        )
        finalized = int(changed.rowcount or 0)  # type: ignore[attr-defined]
        if finalized != deferred:
            raise ValueError("Состав deferred jobs изменился; повторите preview")
        run.status = CollectionRunStatus.COMPLETED
        run.finished_at = now
        run.next_wakeup_at = None
        run.completed_jobs = completed
        run.failed_jobs = 0
        run.skipped_jobs = by_status.get(JobStatus.SKIPPED, 0) + finalized
        run.configuration = {
            **run.configuration,
            "deferred_pilot_jobs_finalized": finalized,
            "deferred_pilot_finalized_at": now.isoformat(),
        }
        run.error_message = None
        await session.commit()
        return {
            "run_id": str(run_id),
            "status": "completed",
            "completed_jobs": completed,
            "deferred_jobs_finalized": finalized,
            "minimum_successful_measurements": settings.collection_subscription_pilot_min_users,
            "jobs_checkpoints_data_deleted": False,
            "deferred_users_remain_due_for_production": True,
        }


async def quarantine_incompatible_pilots(
    sessions: async_sessionmaker[AsyncSession],
    settings: Settings,
    *,
    confirmation: str,
) -> dict[str, Any]:
    """Quarantine only incompatible/obsolete pilots while preserving durable history."""
    if confirmation != "QUARANTINE_INCOMPATIBLE_PILOTS":
        raise ValueError("Требуется точное confirmation QUARANTINE_INCOMPATIBLE_PILOTS")
    previews = await pilot_previews(sessions, settings)
    targets = [
        row
        for row in previews
        if row["classification"] in {"incompatible_configuration", "obsolete"}
    ]
    results = []
    for row in targets:
        results.append(
            await cancel_pilot(
                sessions,
                uuid.UUID(str(row["run_id"])),
                reason="Карантин несовместимого/устаревшего legacy pilot",
            )
        )
    return {
        "confirmation": confirmation,
        "quarantined": results,
        "quarantined_count": len(results),
        "jobs_checkpoints_data_deleted": False,
        "untouched_classifications": [
            "compatible_recoverable",
            "stale_running_lease",
            "waiting",
            "paused_no_tokens",
            "operator_paused",
            "terminal",
        ],
    }

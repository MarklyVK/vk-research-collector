from __future__ import annotations

import hashlib
import json
import math
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import and_, exists, func, literal, or_, select, text, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

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
IMMEDIATE_JOB_STATUSES = (
    JobStatus.PENDING,
    JobStatus.RUNNING,
    JobStatus.PAUSED,
)

MINIMUM_RESERVE_FACTOR = 1.30
SNAPSHOT_HEAP_BYTES_PER_USER = 64
SNAPSHOT_PRIMARY_KEY_BYTES_PER_USER = 48
HISTORICAL_JOB_BYTES_PER_USER = 768
METADATA_MINIMUM_BYTES_PER_JOB = 1024


def build_aggregate_capacity_projection(
    *,
    preview: dict[str, object],
    report: dict[str, Any],
    database_bytes: int,
    disk: DiskState,
    warning_percent: int,
    safe_database_limit_bytes: int = SAFE_DISK_LIMIT_BYTES,
    min_free_bytes: int = 0,
) -> dict[str, object]:
    """Scale validated Pilot A evidence to the complete immutable discovery snapshot."""
    reasons: list[str] = []
    snapshot_value = preview.get("snapshot_users", 0)
    resolved_value = preview.get("already_resolved_users", 0)
    due_value = preview.get("discovery_due_users", 0)
    snapshot_users = snapshot_value if isinstance(snapshot_value, int) else -1
    already_resolved = resolved_value if isinstance(resolved_value, int) else -1
    discovery_due = due_value if isinstance(due_value, int) else -1
    projected = report.get("projected")
    limits = report.get("limits")
    target_entities = limits.get("production_users") if isinstance(limits, dict) else None
    target_growth = projected.get("database_growth_bytes") if isinstance(projected, dict) else None
    report_reserve = projected.get("reserve_factor") if isinstance(projected, dict) else None
    reserve_factor = max(
        MINIMUM_RESERVE_FACTOR,
        float(report_reserve) if isinstance(report_reserve, (int, float)) else 0.0,
    )
    if snapshot_users < 0 or already_resolved < 0 or discovery_due < 0:
        reasons.append("snapshot counts не могут быть отрицательными")
    if already_resolved + discovery_due != snapshot_users:
        reasons.append("already_resolved_users + discovery_due_users не равны snapshot_users")
    if not isinstance(target_entities, int) or target_entities <= 0:
        reasons.append("Gate A не содержит положительный target_entities")
    if not isinstance(target_growth, int) or target_growth <= 0:
        reasons.append("Gate A не содержит положительный measured/projected growth")
    storage = preview.get("snapshot_storage_estimate")
    heap_bytes = int(storage.get("heap_bytes", 0)) if isinstance(storage, dict) else 0
    pk_bytes = int(storage.get("primary_key_bytes", 0)) if isinstance(storage, dict) else 0
    if heap_bytes < 0 or pk_bytes < 0:
        reasons.append("snapshot heap/PK projection не может быть отрицательной")
    discovery_growth = 0
    if isinstance(target_entities, int) and target_entities > 0 and isinstance(target_growth, int):
        discovery_growth = math.ceil(target_growth * discovery_due / target_entities)
    snapshot_growth = math.ceil((heap_bytes + pk_bytes) * reserve_factor)
    aggregate_growth = discovery_growth + snapshot_growth
    projected_database = database_bytes + aggregate_growth
    projected_used = (
        100.0 * (disk.total_bytes - disk.free_bytes + aggregate_growth) / disk.total_bytes
        if disk.total_bytes > 0
        else 100.0
    )
    available_database_growth = max(0, safe_database_limit_bytes - database_bytes)
    available_disk_growth = max(0, disk.free_bytes - min_free_bytes)
    available_growth = min(available_database_growth, available_disk_growth)
    if database_bytes < 0 or disk.free_bytes < 0 or disk.total_bytes <= 0:
        reasons.append("текущие database/disk измерения некорректны")
    if projected_database > safe_database_limit_bytes:
        reasons.append(
            f"projected final database {projected_database} bytes превышает "
            f"safe limit {safe_database_limit_bytes} bytes"
        )
    if aggregate_growth > available_disk_growth:
        reasons.append(
            f"aggregate growth {aggregate_growth} bytes превышает свободный диск "
            f"{available_disk_growth} bytes after reserve"
        )
    if disk.warning or disk.stop or projected_used >= warning_percent:
        reasons.append(
            f"projected disk usage {projected_used:.2f}% достигает warning {warning_percent}%"
        )
    return {
        **preview,
        "gate_a_target_entities": target_entities,
        "gate_a_projected_growth_bytes": target_growth,
        "aggregate_discovery_projected_growth_bytes": discovery_growth,
        "snapshot_heap_growth_bytes": heap_bytes,
        "snapshot_primary_key_growth_bytes": pk_bytes,
        "snapshot_projected_growth_bytes": snapshot_growth,
        "aggregate_projected_growth_bytes": aggregate_growth,
        "reserve_factor": reserve_factor,
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


def choose_campaign_control_action(rows: list[dict[str, object]]) -> dict[str, object]:
    """Choose a phase-aware production-control action without mutating campaign state."""
    if not rows:
        return {"action": "pilot_required", "reason": "active campaign отсутствует"}
    campaign_ids = {str(row["campaign_id"]) for row in rows}
    if len(campaign_ids) != 1:
        return {
            "action": "operator_required",
            "campaign_ids": sorted(campaign_ids),
            "reason": "обнаружено несколько active campaign",
        }
    if any(not bool(row.get("compatible")) for row in rows):
        return {
            "action": "operator_required",
            "campaign_ids": sorted(campaign_ids),
            "reason": "campaign несовместима с runtime configuration",
        }
    if any(row.get("campaign_status") == CampaignStatus.PAUSED.value for row in rows):
        return {
            "action": "operator_paused",
            "campaign_id": next(iter(campaign_ids)),
            "reason": "campaign поставлена оператором на паузу",
        }
    paused = [row for row in rows if row.get("run_status") == "paused_capacity_limit"]
    if len(paused) > 1:
        return {
            "action": "operator_required",
            "campaign_ids": sorted(campaign_ids),
            "run_ids": [str(row["run_id"]) for row in paused],
            "reason": "несколько capacity-paused runs",
        }
    if paused:
        row = paused[0]
        scope = str(row["scope"])
        return {
            "action": "renew_metadata" if scope == "subscription_metadata" else "renew_discovery",
            "campaign_id": next(iter(campaign_ids)),
            "run_id": str(row["run_id"]),
            "scope": scope,
            "reason": "существующий phase run требует свежие report/backup/disk evidence",
        }
    return {
        "action": "reuse_active",
        "campaign_id": next(iter(campaign_ids)),
        "reason": "active campaign уже имеет runnable или waiting work",
    }


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
            "snapshot_user_limit": self._settings.collection_subscription_snapshot_user_limit,
            "minimum_disk_free_bytes": self._settings.collection_disk_min_free_bytes,
            "safe_database_limit_bytes": self._settings.collection_safe_database_limit_bytes,
            "metadata_batch_size": self._settings.collection_community_metadata_batch_size,
            "metadata_ttl_days": self._settings.collection_community_metadata_ttl_days,
            "subscription_limit": self._settings.collection_subscriptions_max_per_user,
            "collection": queue.collection_configuration(),
        }

    async def plan_preview(self) -> dict[str, object]:
        """Estimate the narrow snapshot without materializing campaign state."""
        now = datetime.now(UTC)
        async with self._sessions() as session:
            eligible = self._eligible_user_predicate(now)
            resolved = self._resolved_user_ids_at(now)
            users = int(await session.scalar(select(func.count(VKUser.vk_id)).where(eligible)) or 0)
            already_resolved = int(
                await session.scalar(
                    select(func.count(VKUser.vk_id)).where(
                        eligible,
                        VKUser.vk_id.in_(resolved),
                    )
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
        full_due = max(0, users - already_resolved)
        limit = self._settings.collection_subscription_snapshot_user_limit
        bounded = limit is not None
        target_due = min(full_due, limit) if limit is not None else full_due
        snapshot_users = target_due if bounded else users
        snapshot_resolved = 0 if bounded else already_resolved
        return {
            "apply": False,
            "campaign_type": "subscription_enrichment",
            "snapshot_users": snapshot_users,
            "already_resolved_users": snapshot_resolved,
            "discovery_due_users": target_due,
            "eligible_users": users,
            "eligible_resolved_users": already_resolved,
            "eligible_due_users": full_due,
            "bounded_snapshot": bounded,
            "snapshot_user_limit": limit,
            "snapshot_storage_estimate": {
                "method": "local PostgreSQL 16 measurement with conservative rounding",
                "heap_bytes": snapshot_users * SNAPSHOT_HEAP_BYTES_PER_USER,
                "primary_key_bytes": snapshot_users * SNAPSHOT_PRIMARY_KEY_BYTES_PER_USER,
                "historical_job_bytes_per_discovery_user": HISTORICAL_JOB_BYTES_PER_USER,
                "reserve_factor": MINIMUM_RESERVE_FACTOR,
            },
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

    async def control_decision(self) -> dict[str, object]:
        """Return a read-only phase-aware decision for production hourly-control."""
        configuration_hash = hashlib.sha256(
            json.dumps(self.configuration(), sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()
        async with self._sessions() as session:
            rows = (
                await session.execute(
                    select(CollectionCampaign, CollectionRun)
                    .outerjoin(CollectionRun, CollectionRun.campaign_id == CollectionCampaign.id)
                    .where(
                        CollectionCampaign.campaign_type == "subscription_enrichment",
                        CollectionCampaign.status.in_(ACTIVE_CAMPAIGN_STATUSES),
                    )
                    .order_by(CollectionCampaign.created_at, CollectionRun.created_at)
                )
            ).all()
        payload = [
            {
                "campaign_id": campaign.id,
                "campaign_status": campaign.status,
                "compatible": campaign.configuration_hash == configuration_hash,
                "run_id": run.id if run is not None else None,
                "scope": run.scope if run is not None else None,
                "run_status": run.status.value if run is not None else None,
            }
            for campaign, run in rows
        ]
        return {"campaigns": payload, "decision": choose_campaign_control_action(payload)}

    async def plan(self, *, gate_evidence: dict[str, object]) -> uuid.UUID:
        """Create or reuse one active campaign for the exact configuration."""
        if gate_evidence.get("decision") != "passed":
            raise ValueError("Campaign materialization требует проверенный capacity gate")
        required_positive = (
            "projected_final_database_bytes",
            "current_disk_free_bytes",
            "safe_database_limit_bytes",
        )
        for key in required_positive:
            value = gate_evidence.get(key)
            if not isinstance(value, int) or value <= 0:
                raise ValueError("Capacity evidence не содержит полный aggregate projection")
        aggregate_value = gate_evidence.get("aggregate_projected_growth_bytes")
        current_database_value = gate_evidence.get("current_database_bytes")
        if not isinstance(aggregate_value, int) or aggregate_value < 0:
            raise ValueError("Capacity evidence не содержит полный aggregate projection")
        if not isinstance(current_database_value, int) or current_database_value < 0:
            raise ValueError("Capacity evidence не содержит current database bytes")
        projected_final = gate_evidence.get("projected_final_database_bytes")
        safe_limit = gate_evidence.get("safe_database_limit_bytes")
        aggregate_growth = aggregate_value
        disk_free = gate_evidence.get("current_disk_free_bytes")
        minimum_disk_free = gate_evidence.get("minimum_disk_free_bytes", 0)
        assert isinstance(projected_final, int)
        assert isinstance(safe_limit, int)
        assert isinstance(aggregate_growth, int)
        assert isinstance(disk_free, int)
        if not isinstance(minimum_disk_free, int) or minimum_disk_free < 0:
            raise ValueError("Aggregate evidence не содержит корректный disk reserve")
        if safe_limit != self._settings.collection_safe_database_limit_bytes:
            raise ValueError("Aggregate evidence содержит неожиданный safe database limit")
        if minimum_disk_free != self._settings.collection_disk_min_free_bytes:
            raise ValueError("Aggregate evidence содержит неожиданный disk reserve")
        if projected_final < current_database_value + aggregate_growth:
            raise ValueError("Aggregate evidence содержит заниженный projected final database")
        if projected_final > safe_limit:
            raise ValueError("Aggregate projection превышает safe database limit")
        if aggregate_growth > max(0, disk_free - minimum_disk_free):
            raise ValueError("Aggregate projection превышает свободный диск")
        configuration = self.configuration()
        configuration_hash = hashlib.sha256(
            json.dumps(configuration, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()
        if gate_evidence.get("planning_configuration_hash") != configuration_hash:
            raise ValueError("Capacity evidence относится к другой planning configuration")
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
            eligible_count = int(
                await session.scalar(
                    select(func.count(VKUser.vk_id)).where(self._eligible_user_predicate(now))
                )
                or 0
            )
            resolved_count = int(
                await session.scalar(
                    select(func.count(VKUser.vk_id)).where(
                        self._eligible_user_predicate(now),
                        VKUser.vk_id.in_(self._resolved_user_ids_at(now)),
                    )
                )
                or 0
            )
            eligible_due = max(0, eligible_count - resolved_count)
            snapshot_limit = self._settings.collection_subscription_snapshot_user_limit
            bounded = snapshot_limit is not None
            snapshot_expected = (
                min(eligible_due, snapshot_limit) if snapshot_limit is not None else eligible_count
            )
            snapshot_resolved = 0 if bounded else resolved_count
            snapshot_due = snapshot_expected if bounded else eligible_due
            if (
                gate_evidence.get("eligible_users", eligible_count) != eligible_count
                or gate_evidence.get("eligible_resolved_users", resolved_count) != resolved_count
                or gate_evidence.get("eligible_due_users", eligible_due) != eligible_due
                or gate_evidence.get("snapshot_users") != snapshot_expected
                or gate_evidence.get("already_resolved_users") != snapshot_resolved
                or gate_evidence.get("discovery_due_users") != snapshot_due
            ):
                raise ValueError(
                    "Eligible/resolved counts изменились после preview; повторите capacity checks"
                )
            campaign = CollectionCampaign(
                campaign_type="subscription_enrichment",
                status=CampaignStatus.PAUSED_CAPACITY_LIMIT.value,
                phase=CampaignPhase.SUBSCRIPTION_DISCOVERY.value,
                snapshot_at=now,
                snapshot_max_user_id=0,
                configuration=configuration,
                configuration_hash=configuration_hash,
            )
            campaign.configuration = {
                **campaign.configuration,
                "capacity_gate": "passed",
                **gate_evidence,
            }
            session.add(campaign)
            await session.flush()
            if bounded:
                bounded_ids = (
                    select(VKUser.vk_id.label("user_id"))
                    .where(
                        self._eligible_user_predicate(now),
                        VKUser.vk_id.not_in(self._resolved_user_ids_at(now)),
                    )
                    .order_by(VKUser.vk_id)
                    .limit(snapshot_expected)
                    .subquery()
                )
                snapshot_select = select(literal(campaign.id), bounded_ids.c.user_id)
            else:
                snapshot_select = select(literal(campaign.id), VKUser.vk_id).where(
                    self._eligible_user_predicate(now)
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
            campaign.snapshot_max_user_id = int(
                await session.scalar(
                    select(func.coalesce(func.max(CollectionCampaignUser.user_id), 0)).where(
                        CollectionCampaignUser.campaign_id == campaign.id
                    )
                )
                or 0
            )
            materialized_resolved_count = int(
                await session.scalar(
                    select(func.count(CollectionCampaignUser.user_id)).where(
                        CollectionCampaignUser.campaign_id == campaign.id,
                        CollectionCampaignUser.user_id.in_(
                            self._resolved_user_ids_at(campaign.snapshot_at)
                        ),
                    )
                )
                or 0
            )
            if (
                campaign.snapshot_user_count != snapshot_expected
                or materialized_resolved_count != snapshot_resolved
            ):
                raise ValueError(
                    "Eligible/resolved snapshot изменился во время materialization; "
                    "transaction отменена, повторите capacity checks"
                )
            if campaign.snapshot_user_count == 0:
                campaign.status = CampaignStatus.COMPLETED.value
                campaign.phase = CampaignPhase.COMPLETED.value
                campaign.finished_at = now
                campaign.error_message = "Snapshot пуст; campaign завершена без jobs"
            elif await self._plan_discovery_cohort(session, campaign) is None:
                campaign.phase = CampaignPhase.SUBSCRIPTION_METADATA.value
                campaign.last_metadata_vk_id = 0
                if await self._prepare_metadata_gate(session, campaign) is None:
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
        if campaign.last_planned_user_id > 0 and not await self._discovery_budget_available(
            session, campaign
        ):
            return None
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

    async def _discovery_budget_available(
        self, session: AsyncSession, campaign: CollectionCampaign
    ) -> bool:
        """Recheck live disk and the remaining durable discovery budget."""
        evidence = campaign.configuration
        initial_database = evidence.get("current_database_bytes")
        projected_final = evidence.get("projected_final_database_bytes")
        discovery_due = evidence.get("discovery_due_users")
        discovery_growth = evidence.get("aggregate_discovery_projected_growth_bytes")
        if not all(
            isinstance(value, int) and value >= 0
            for value in (initial_database, projected_final, discovery_due, discovery_growth)
        ):
            self._pause_capacity(
                campaign, "Aggregate discovery evidence повреждён; нужен новый preview"
            )
            return False
        assert isinstance(initial_database, int)
        assert isinstance(projected_final, int)
        assert isinstance(discovery_due, int)
        assert isinstance(discovery_growth, int)
        current_database = int(
            await session.scalar(select(func.pg_database_size(func.current_database()))) or 0
        )
        unresolved = await self._unresolved_count(session, campaign)
        remaining_growth = (
            math.ceil(discovery_growth * unresolved / discovery_due) if discovery_due > 0 else 0
        )
        disk = inspect_disk(
            self._settings.collection_export_dir,
            self._settings.disk_warning_percent,
            self._settings.disk_stop_percent,
            min_free_bytes=self._settings.collection_disk_min_free_bytes,
        )
        required_final = current_database + remaining_growth
        projected_used = (
            100.0 * (disk.total_bytes - disk.free_bytes + remaining_growth) / disk.total_bytes
            if disk.total_bytes > 0
            else 100.0
        )
        if (
            required_final > projected_final
            or required_final > self._settings.collection_safe_database_limit_bytes
            or remaining_growth
            > max(0, disk.free_bytes - self._settings.collection_disk_min_free_bytes)
            or disk.warning
            or disk.stop
            or projected_used >= self._settings.disk_warning_percent
        ):
            self._pause_capacity(
                campaign,
                "Следующий discovery cohort отклонён: "
                f"required_final={required_final} bytes, evidence_limit={projected_final} bytes, "
                f"disk_free={disk.free_bytes} bytes, projected_disk={projected_used:.2f}%",
            )
            return False
        campaign.configuration = {
            **campaign.configuration,
            "last_discovery_capacity_check": {
                "checked_at": datetime.now(UTC).isoformat(),
                "current_database_bytes": current_database,
                "unresolved_users": unresolved,
                "remaining_projected_growth_bytes": remaining_growth,
                "disk_free_bytes": disk.free_bytes,
                "projected_disk_used_percent": projected_used,
            },
        }
        return True

    @staticmethod
    def _pause_capacity(campaign: CollectionCampaign, message: str) -> None:
        campaign.status = CampaignStatus.PAUSED_CAPACITY_LIMIT.value
        campaign.next_wakeup_at = None
        campaign.error_message = message

    def _inherited_gate(self, campaign: CollectionCampaign) -> dict[str, object]:
        keys = ("capacity_report", "verified_backup", "projected_database_bytes")
        return {key: campaign.configuration[key] for key in keys if key in campaign.configuration}

    def _resolved_user_ids(self, campaign: CollectionCampaign) -> Any:
        """Reuse canonical state only while its explicit freshness window is valid."""
        return self._resolved_user_ids_at(campaign.snapshot_at)

    def _resolved_user_ids_at(self, snapshot_at: datetime) -> Any:
        return select(UserSubscriptionState.user_id).where(
            UserSubscriptionState.next_scheduled_at.is_not(None),
            UserSubscriptionState.next_scheduled_at > snapshot_at,
            or_(
                UserSubscriptionState.last_success_at.is_not(None),
                UserSubscriptionState.terminal_reason.is_not(None),
            ),
        )

    async def _plan_metadata_cohort(
        self,
        session: AsyncSession,
        campaign: CollectionCampaign,
        existing_run: CollectionRun | None = None,
    ) -> uuid.UUID | None:
        if campaign.configuration.get("metadata_capacity_gate") != "passed":
            self._pause_capacity(campaign, "Metadata capacity gate ещё не применён")
            return None
        if campaign.last_metadata_vk_id > 0 and not await self._metadata_budget_available(
            session, campaign
        ):
            return None
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
        run = existing_run or CollectionRun(campaign_id=campaign.id, scope="subscription_metadata")
        run.status = CollectionRunStatus.PLANNED
        run.configuration = {
            **run.configuration,
            "campaign_id": str(campaign.id),
            "phase": CampaignPhase.SUBSCRIPTION_METADATA.value,
            "community_count": len(ids),
            "capacity_gate": "passed",
            "metadata_gate_pending": False,
            "collection": CollectionQueue(
                self._sessions, self._settings
            ).collection_configuration(),
            **self._inherited_gate(campaign),
        }
        if existing_run is None:
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

    async def _metadata_counts(
        self, session: AsyncSession, campaign: CollectionCampaign
    ) -> tuple[int, int]:
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
        return total, due

    async def _prepare_metadata_gate(
        self, session: AsyncSession, campaign: CollectionCampaign
    ) -> uuid.UUID | None:
        """Persist the total metadata projection without creating runnable jobs."""
        if await self._unresolved_count(session, campaign) > 0:
            raise ValueError("Metadata gate запрещён при unresolved discovery users")
        total, due = await self._metadata_counts(session, campaign)
        if due == 0:
            return None
        average_row_bytes = int(
            await session.scalar(
                text("SELECT coalesce(avg(pg_column_size(v)), 0)::bigint FROM vk_communities AS v")
            )
            or 0
        )
        per_job_bytes = max(METADATA_MINIMUM_BYTES_PER_JOB, average_row_bytes + 512)
        projected_growth = math.ceil(due * per_job_bytes * MINIMUM_RESERVE_FACTOR)
        database_bytes = int(
            await session.scalar(select(func.pg_database_size(func.current_database()))) or 0
        )
        disk = inspect_disk(
            self._settings.collection_export_dir,
            self._settings.disk_warning_percent,
            self._settings.disk_stop_percent,
            min_free_bytes=self._settings.collection_disk_min_free_bytes,
        )
        projected_database = database_bytes + projected_growth
        projected_used = (
            100.0 * (disk.total_bytes - disk.free_bytes + projected_growth) / disk.total_bytes
            if disk.total_bytes > 0
            else 100.0
        )
        reasons: list[str] = []
        if projected_database > self._settings.collection_safe_database_limit_bytes:
            reasons.append("metadata projected database превышает safe limit")
        if projected_growth > max(
            0, disk.free_bytes - self._settings.collection_disk_min_free_bytes
        ):
            reasons.append("metadata projected growth превышает свободный диск")
        if disk.warning or disk.stop or projected_used >= self._settings.disk_warning_percent:
            reasons.append("metadata projected disk usage достигает warning")
        evidence = {
            "phase": CampaignPhase.SUBSCRIPTION_METADATA.value,
            "metadata_distinct_communities": total,
            "metadata_due": due,
            "measured_average_community_row_bytes": average_row_bytes,
            "conservative_per_job_bytes": per_job_bytes,
            "aggregate_projected_growth_bytes": projected_growth,
            "current_database_bytes": database_bytes,
            "projected_final_database_bytes": projected_database,
            "current_disk_free_bytes": disk.free_bytes,
            "projected_final_disk_used_percent": projected_used,
            "safe_database_limit_bytes": self._settings.collection_safe_database_limit_bytes,
            "minimum_disk_free_bytes": self._settings.collection_disk_min_free_bytes,
            "reserve_factor": MINIMUM_RESERVE_FACTOR,
            "decision": "passed" if not reasons else "rejected",
            "rejection_reasons": reasons,
            "verified_at": datetime.now(UTC).isoformat(),
        }
        campaign.configuration = {
            **campaign.configuration,
            "metadata_capacity_gate": "pending",
            "metadata_capacity_preview": evidence,
        }
        self._pause_capacity(
            campaign,
            "Metadata ожидает явный aggregate capacity gate" if not reasons else "; ".join(reasons),
        )
        run = CollectionRun(
            campaign_id=campaign.id,
            scope="subscription_metadata",
            status=CollectionRunStatus.PAUSED_CAPACITY_LIMIT,
            total_jobs=0,
            configuration={
                "campaign_id": str(campaign.id),
                "phase": CampaignPhase.SUBSCRIPTION_METADATA.value,
                "metadata_gate_pending": True,
                "metadata_capacity_preview": evidence,
                "capacity_gate": "pending",
                "collection": CollectionQueue(
                    self._sessions, self._settings
                ).collection_configuration(),
                **self._inherited_gate(campaign),
            },
            error_message=campaign.error_message,
        )
        session.add(run)
        await session.flush()
        return run.id

    async def activate_metadata_cohort(
        self, session: AsyncSession, campaign: CollectionCampaign, run: CollectionRun
    ) -> uuid.UUID | None:
        """Populate the first bounded metadata cohort only after an explicit gate."""
        return await self._plan_metadata_cohort(session, campaign, existing_run=run)

    async def _metadata_budget_available(
        self, session: AsyncSession, campaign: CollectionCampaign
    ) -> bool:
        evidence = campaign.configuration.get("metadata_capacity_evidence")
        if not isinstance(evidence, dict):
            self._pause_capacity(campaign, "Metadata aggregate evidence отсутствует")
            return False
        initial_database = evidence.get("current_database_bytes")
        projected_final = evidence.get("projected_final_database_bytes")
        total_due = evidence.get("metadata_due")
        aggregate_growth = evidence.get("aggregate_projected_growth_bytes")
        if not all(
            isinstance(value, int) and value >= 0
            for value in (initial_database, projected_final, total_due, aggregate_growth)
        ):
            self._pause_capacity(campaign, "Metadata aggregate evidence повреждён")
            return False
        assert isinstance(initial_database, int)
        assert isinstance(projected_final, int)
        assert isinstance(total_due, int)
        assert isinstance(aggregate_growth, int)
        _, remaining_due = await self._metadata_counts(session, campaign)
        remaining_growth = (
            math.ceil(aggregate_growth * remaining_due / total_due) if total_due > 0 else 0
        )
        current_database = int(
            await session.scalar(select(func.pg_database_size(func.current_database()))) or 0
        )
        disk = inspect_disk(
            self._settings.collection_export_dir,
            self._settings.disk_warning_percent,
            self._settings.disk_stop_percent,
            min_free_bytes=self._settings.collection_disk_min_free_bytes,
        )
        required_final = current_database + remaining_growth
        projected_used = (
            100.0 * (disk.total_bytes - disk.free_bytes + remaining_growth) / disk.total_bytes
            if disk.total_bytes > 0
            else 100.0
        )
        if (
            current_database < initial_database
            or required_final > projected_final
            or required_final > self._settings.collection_safe_database_limit_bytes
            or remaining_growth
            > max(0, disk.free_bytes - self._settings.collection_disk_min_free_bytes)
            or disk.warning
            or disk.stop
            or projected_used >= self._settings.disk_warning_percent
        ):
            self._pause_capacity(
                campaign,
                "Следующий metadata cohort отклонён: "
                f"required_final={required_final}, evidence_limit={projected_final}, "
                f"disk_free={disk.free_bytes}",
            )
            return False
        return True

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
                        CollectionRun.campaign_id == campaign.id,
                        CollectionJob.status.in_(IMMEDIATE_JOB_STATUSES),
                    )
                )
                or 0
            )
            if immediate:
                await session.commit()
                return
            deferred = int(
                await session.scalar(
                    select(func.count(CollectionJob.id))
                    .join(CollectionRun, CollectionRun.id == CollectionJob.collection_run_id)
                    .where(
                        CollectionRun.campaign_id == campaign.id,
                        CollectionJob.status == JobStatus.RETRY_WAIT,
                    )
                )
                or 0
            )
            if campaign.phase == CampaignPhase.SUBSCRIPTION_DISCOVERY.value:
                unresolved = await self._unresolved_count(session, campaign)
                if unresolved:
                    planned = await self._plan_discovery_cohort(session, campaign)
                    if (
                        planned is None
                        and campaign.status != CampaignStatus.PAUSED_CAPACITY_LIMIT.value
                    ):
                        if deferred:
                            campaign.status = CampaignStatus.RUNNING.value
                            campaign.error_message = (
                                f"Ожидаются отложенные повторы: {deferred}; "
                                f"неразрешённых пользователей: {unresolved}"
                            )
                        else:
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
                if await self._prepare_metadata_gate(session, campaign) is None:
                    campaign.status = CampaignStatus.COMPLETED.value
                    campaign.phase = CampaignPhase.COMPLETED.value
                    campaign.finished_at = datetime.now(UTC)
                    campaign.next_wakeup_at = None
            elif campaign.phase == CampaignPhase.SUBSCRIPTION_METADATA.value:
                if deferred:
                    await session.commit()
                    return
                if campaign.configuration.get("metadata_capacity_gate") != "passed":
                    await session.commit()
                    return
                if (
                    await self._plan_metadata_cohort(session, campaign) is None
                    and campaign.status != CampaignStatus.PAUSED_CAPACITY_LIMIT.value
                ):
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

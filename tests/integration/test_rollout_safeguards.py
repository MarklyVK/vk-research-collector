import asyncio
import os
import uuid
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import delete, func, select, text, update
from sqlalchemy.ext.asyncio import async_sessionmaker

from vk_collector.cli.app import _validate_autonomous_run
from vk_collector.collection.campaigns import CampaignManager
from vk_collector.collection.pilots import (
    cancel_pilot,
    finalize_deferred_subscription_pilot,
    pilot_previews,
)
from vk_collector.collection.queue import CollectionQueue
from vk_collector.collection.worker import CollectionWorker
from vk_collector.config import Settings
from vk_collector.database.models import (
    CampaignPhase,
    CampaignStatus,
    ClassificationStatus,
    CollectionCampaign,
    CollectionJob,
    CollectionRun,
    CollectionRunStatus,
    GroupCandidate,
    GroupCollectionState,
    GroupMembership,
    JobStatus,
    UserGroupSubscription,
    UserSubscriptionState,
    VKCommunity,
    VKTokenMethodState,
    VKUser,
)
from vk_collector.database.session import create_database_engine
from vk_collector.vk import TokenPool, VKMethodUnavailable


def database_url() -> str:
    explicit = os.getenv("DATABASE_URL")
    if explicit:
        return explicit
    return (
        "postgresql+asyncpg://"
        f"{os.getenv('POSTGRES_USER', 'vk_collector')}:"
        f"{os.getenv('POSTGRES_PASSWORD', 'local_development_only')}@"
        f"{os.getenv('POSTGRES_HOST', 'postgres')}:{os.getenv('POSTGRES_PORT', '5432')}/"
        f"{os.getenv('POSTGRES_DB', 'vk_research')}"
    )


pytestmark = pytest.mark.skipif(
    os.getenv("RUN_INTEGRATION_TESTS") != "1"
    or not database_url().rsplit("/", 1)[-1].endswith("_test"),
    reason="PostgreSQL integration test требует явного запуска на изолированной *_test БД",
)


class MetadataVK:
    def __init__(self, *, missing: bool = False) -> None:
        self.missing = missing
        self.calls: list[list[int]] = []

    async def get_groups(self, ids: list[int]) -> list[dict[str, object]]:
        self.calls.append(ids)
        if self.missing:
            return []
        return [
            {
                "id": value,
                "name": f"updated-{value}",
                "description": "public",
                "status": "ok",
                "screen_name": f"group{value}",
                "is_closed": 0,
            }
            for value in ids
        ]


@pytest.mark.asyncio
async def test_finalize_deferred_subscription_pilot_preserves_checkpoint() -> None:
    engine = create_database_engine(database_url())
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    settings = Settings(
        database_url=database_url(),
        collection_export_dir=".",
        collection_subscription_pilot_min_users=1,
    )
    try:
        async with sessions() as session:
            run = CollectionRun(
                scope="subscriptions_pilot",
                status=CollectionRunStatus.RUNNING,
                total_jobs=2,
                completed_jobs=1,
                configuration={
                    "collection": CollectionQueue(sessions, settings).collection_configuration()
                },
            )
            session.add(run)
            await session.flush()
            completed = CollectionJob(
                collection_run_id=run.id,
                job_type="collect_user_subscriptions",
                entity_type="user",
                entity_id=int(uuid.uuid4().hex[:10], 16),
                status=JobStatus.COMPLETED,
                finished_at=datetime.now(UTC),
            )
            deferred = CollectionJob(
                collection_run_id=run.id,
                job_type="collect_user_subscriptions",
                entity_type="user",
                entity_id=int(uuid.uuid4().hex[:10], 16),
                status=JobStatus.RETRY_WAIT,
                next_attempt_at=datetime.now(UTC) + timedelta(hours=6),
                checkpoint={"offset": 50, "collected": 50},
            )
            session.add_all([completed, deferred])
            await session.commit()
            run_id, deferred_id = run.id, deferred.id

        result = await finalize_deferred_subscription_pilot(
            sessions,
            settings,
            run_id,
            confirmation="FINALIZE_DEFERRED_SUBSCRIPTION_PILOT",
        )
        assert result["status"] == "completed"
        assert result["deferred_jobs_finalized"] == 1
        assert result["jobs_checkpoints_data_deleted"] is False
        async with sessions() as session:
            stored_run = await session.get(CollectionRun, run_id)
            stored_job = await session.get(CollectionJob, deferred_id)
            assert stored_run is not None
            assert stored_run.status == CollectionRunStatus.COMPLETED
            assert stored_run.completed_jobs == 1
            assert stored_run.skipped_jobs == 1
            assert stored_job is not None
            assert stored_job.status == JobStatus.SKIPPED
            assert stored_job.checkpoint == {"offset": 50, "collected": 50}
            assert stored_job.next_attempt_at is None
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_concurrent_light_repair_apply_reuses_one_active_run() -> None:
    engine = create_database_engine(database_url())
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    marker = int(uuid.uuid4().hex[:10], 16)
    settings = Settings(database_url=database_url(), collection_export_dir=".")
    run_ids: set[uuid.UUID] = set()
    try:
        async with sessions() as session:
            session.add(
                GroupCandidate(
                    id=marker,
                    vk_id=marker,
                    name="concurrent",
                    description="",
                    status_text="",
                    address=f"https://vk.com/{marker}",
                    first_seen_at=datetime.now(UTC),
                    last_seen_at=datetime.now(UTC),
                    classification_status=ClassificationStatus.APPROVED,
                )
            )
            await session.commit()
        queue = CollectionQueue(sessions, settings)
        preview = await queue.light_repair_preview()
        evidence = {"decision": "passed", "preview_hash": preview["preview_hash"]}
        values = await asyncio.gather(
            queue.plan_light_repair(capacity_evidence=evidence),
            queue.plan_light_repair(capacity_evidence=evidence),
        )
        run_ids.update(values)
        assert values[0] == values[1]
        async with sessions() as session:
            run = await session.get(CollectionRun, values[0], with_for_update=True)
            assert run is not None
            run.configuration = {**run.configuration, "plan_key": "incompatible"}
            await session.commit()
        with pytest.raises(ValueError, match="несовместимой immutable"):
            await queue.plan_light_repair(capacity_evidence=evidence)
    finally:
        async with sessions() as session:
            if run_ids:
                await session.execute(delete(CollectionRun).where(CollectionRun.id.in_(run_ids)))
            await session.execute(delete(GroupCandidate).where(GroupCandidate.id == marker))
            await session.commit()
        await engine.dispose()


@pytest.mark.asyncio
async def test_light_repair_is_canonical_bounded_and_idempotent() -> None:
    engine = create_database_engine(database_url())
    marker = int(uuid.uuid4().hex[:8], 16) * 100_000
    settings = Settings(
        database_url=database_url(),
        collection_light_repair_cohort_size=10_000,
        collection_max_concurrency=1,
        collection_export_dir=".",
    )
    try:
        async with engine.connect() as connection:
            outer = await connection.begin()
            sessions = async_sessionmaker(
                connection, expire_on_commit=False, join_transaction_mode="create_savepoint"
            )
            try:
                now = datetime.now(UTC)
                async with sessions() as session:
                    candidate = GroupCandidate(
                        id=marker - 1,
                        vk_id=marker - 1,
                        name="old",
                        description="",
                        status_text="",
                        address=f"https://vk.com/{marker - 1}",
                        first_seen_at=now,
                        last_seen_at=now,
                        classification_status=ClassificationStatus.APPROVED,
                    )
                    session.add(candidate)
                    await session.commit()
                queue = CollectionQueue(sessions, settings)
                first_preview = await queue.light_repair_preview()
                with pytest.raises(ValueError, match="disk/capacity gate"):
                    await queue.plan_light_repair(
                        capacity_evidence={
                            "decision": "rejected",
                            "preview_hash": first_preview["preview_hash"],
                        }
                    )
                async with sessions() as session:
                    assert (
                        int(
                            await session.scalar(
                                select(func.count(CollectionRun.id)).where(
                                    CollectionRun.scope == "light_repair"
                                )
                            )
                            or 0
                        )
                        == 0
                    )
                evidence = {
                    "decision": "passed",
                    "preview_hash": first_preview["preview_hash"],
                }
                run_id = await queue.plan_light_repair(capacity_evidence=evidence)
                fake = MetadataVK()
                await CollectionWorker(sessions, fake, settings).run(run_id, max_jobs=1)
                async with sessions() as session:
                    refreshed = await session.get(GroupCandidate, marker - 1)
                    community = await session.get(VKCommunity, marker - 1)
                    state = await session.get(GroupCollectionState, marker - 1)
                    assert refreshed is not None and refreshed.name == f"updated-{marker - 1}"
                    assert community is not None and community.metadata_updated_at is not None
                    assert state is not None and state.last_group_success_at is not None
                assert (await queue.light_repair_preview())["approved_group_metadata_gaps"] == 0

                async with sessions() as session:
                    await session.execute(
                        text(
                            """
                            INSERT INTO group_candidates
                              (id, vk_id, name, description, status_text, address,
                               first_seen_at, last_seen_at, classification_status)
                            SELECT CAST(:base AS bigint) + value,
                                   CAST(:base AS bigint) + value, 'bulk', '', '',
                                   'https://vk.com/bulk' || value, now(), now(), 'approved'
                              FROM generate_series(1, 25001) AS value
                            """
                        ),
                        {"base": marker},
                    )
                    await session.commit()
                preview = await queue.light_repair_preview()
                assert preview["approved_group_metadata_gaps"] == 25_001
                evidence = {"decision": "passed", "preview_hash": preview["preview_hash"]}
                first = await queue.plan_light_repair(capacity_evidence=evidence)
                assert await queue.plan_light_repair(capacity_evidence=evidence) == first
                async with sessions() as session:
                    total = int(
                        await session.scalar(
                            select(func.count(CollectionJob.id)).where(
                                CollectionJob.collection_run_id == first
                            )
                        )
                        or 0
                    )
                    assert total == 10_000
                    await session.execute(
                        text(
                            """
                            INSERT INTO group_collection_states
                              (group_id, last_group_success_at, unavailable)
                            SELECT gc.id, now(), false
                              FROM group_candidates gc
                              JOIN collection_jobs j ON j.entity_id = gc.vk_id
                             WHERE j.collection_run_id = :run_id
                            ON CONFLICT (group_id) DO UPDATE
                              SET last_group_success_at=excluded.last_group_success_at,
                                  unavailable=false
                            """
                        ),
                        {"run_id": first},
                    )
                    await session.execute(
                        update(CollectionJob)
                        .where(CollectionJob.collection_run_id == first)
                        .values(status=JobStatus.COMPLETED)
                    )
                    run = await session.get(CollectionRun, first)
                    assert run is not None
                    run.status = CollectionRunStatus.COMPLETED
                    run.completed_jobs = total
                    run.finished_at = datetime.now(UTC)
                    await session.commit()
                second_preview = await queue.light_repair_preview()
                second = await queue.plan_light_repair(
                    capacity_evidence={
                        "decision": "passed",
                        "preview_hash": second_preview["preview_hash"],
                    }
                )
                assert second != first
                async with sessions() as session:
                    second_total = int(
                        await session.scalar(
                            select(func.count(CollectionJob.id)).where(
                                CollectionJob.collection_run_id == second
                            )
                        )
                        or 0
                    )
                    assert second_total == 10_000
            finally:
                await outer.rollback()
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_partial_metadata_retry_becomes_single_then_terminal() -> None:
    engine = create_database_engine(database_url())
    settings = Settings(
        database_url=database_url(),
        collection_community_metadata_batch_size=100,
        collection_max_concurrency=1,
        collection_export_dir=".",
    )
    try:
        async with engine.connect() as connection:
            outer = await connection.begin()
            sessions = async_sessionmaker(
                connection, expire_on_commit=False, join_transaction_mode="create_savepoint"
            )
            try:
                async with sessions() as session:
                    run = CollectionRun(
                        scope="subscription_metadata", status=CollectionRunStatus.PLANNED
                    )
                    session.add(run)
                    await session.flush()
                    job = CollectionJob(
                        collection_run_id=run.id,
                        job_type="refresh_community_metadata",
                        entity_type="vk_community",
                        entity_id=int(uuid.uuid4().hex[:10], 16),
                    )
                    session.add(job)
                    run.total_jobs = 1
                    await session.commit()
                    run_id = run.id
                    job_id = job.id
                worker = CollectionWorker(sessions, MetadataVK(missing=True), settings)
                delays: list[float] = []
                for _ in range(5):
                    await worker.run(run_id, max_jobs=1)
                    async with sessions() as session:
                        current = await session.get(CollectionJob, job_id, with_for_update=True)
                        assert current is not None
                        if current.status == JobStatus.SKIPPED:
                            break
                        assert current.status == JobStatus.RETRY_WAIT
                        assert current.next_attempt_at is not None
                        delays.append((current.next_attempt_at - datetime.now(UTC)).total_seconds())
                        current.next_attempt_at = datetime.now(UTC) - timedelta(seconds=1)
                        run = await session.get(CollectionRun, run_id, with_for_update=True)
                        assert run is not None
                        run.status = CollectionRunStatus.PLANNED
                        await session.commit()
                async with sessions() as session:
                    current = await session.get(CollectionJob, job_id)
                    assert current is not None
                    assert current.status == JobStatus.SKIPPED
                    assert current.attempt_count == 5
                    assert current.last_error_type == "community_missing_terminal"
                    assert "single_miss=2" in str(current.last_error_message)
                assert delays == sorted(delays)
            finally:
                await outer.rollback()
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_pilot_preview_reuse_and_campaign_metadata_transition() -> None:
    engine = create_database_engine(database_url())
    marker = int(uuid.uuid4().hex[:10], 16)
    settings = Settings(database_url=database_url(), collection_export_dir=".")
    try:
        async with engine.connect() as connection:
            outer = await connection.begin()
            sessions = async_sessionmaker(
                connection, expire_on_commit=False, join_transaction_mode="create_savepoint"
            )
            try:
                queue = CollectionQueue(sessions, settings)
                now = datetime.now(UTC)
                async with sessions() as session:
                    group = GroupCandidate(
                        id=marker,
                        vk_id=marker,
                        name="approved",
                        description="",
                        status_text="",
                        address=f"https://vk.com/{marker}",
                        first_seen_at=now,
                        last_seen_at=now,
                        classification_status=ClassificationStatus.APPROVED,
                    )
                    user = VKUser(
                        vk_id=marker,
                        is_closed=False,
                        can_access_closed=True,
                        first_seen_at=now,
                        last_seen_at=now,
                    )
                    session.add_all([group, user])
                    await session.flush()
                    session.add(
                        GroupMembership(
                            group_id=group.id,
                            user_id=user.vk_id,
                            first_seen_at=now,
                            last_seen_at=now,
                            is_current=True,
                        )
                    )
                    await session.commit()
                pilot_id = await queue.plan_subscriptions(pilot=True)
                assert await queue.plan_subscriptions(pilot=True) == pilot_id
                rows = await pilot_previews(sessions, settings)
                preview = next(row for row in rows if row["run_id"] == str(pilot_id))
                assert preview["classification"] == "compatible_recoverable"

                async with sessions() as session:
                    pilot = await session.get(CollectionRun, pilot_id, with_for_update=True)
                    assert pilot is not None
                    pilot.status = CollectionRunStatus.WAITING_METHOD_LIMIT
                    await session.execute(
                        update(CollectionJob)
                        .where(CollectionJob.collection_run_id == pilot_id)
                        .values(
                            status=JobStatus.RETRY_WAIT,
                            next_attempt_at=now + timedelta(hours=1),
                        )
                    )
                    await session.commit()
                rows = await pilot_previews(sessions, settings)
                waiting = next(row for row in rows if row["run_id"] == str(pilot_id))
                assert waiting["classification"] == "waiting"
                async with sessions() as session:
                    await session.execute(
                        update(CollectionJob)
                        .where(CollectionJob.collection_run_id == pilot_id)
                        .values(
                            status=JobStatus.RUNNING,
                            locked_at=now - timedelta(hours=1),
                            next_attempt_at=None,
                        )
                    )
                    await session.commit()
                rows = await pilot_previews(sessions, settings)
                stale = next(row for row in rows if row["run_id"] == str(pilot_id))
                assert stale["classification"] == "stale_running_lease"

                cancelled = await cancel_pilot(sessions, pilot_id, reason="obsolete test pilot")
                assert cancelled["history_deleted"] is False
                async with sessions() as session:
                    session.add(
                        UserSubscriptionState(
                            user_id=user.vk_id,
                            last_success_at=now,
                            next_scheduled_at=now + timedelta(days=30),
                        )
                    )
                    session.add(
                        VKCommunity(
                            vk_id=marker + 1,
                            name="",
                            description="",
                            status_text="",
                            first_seen_at=now,
                            last_seen_at=now,
                        )
                    )
                    await session.flush()
                    session.add(
                        UserGroupSubscription(
                            user_id=user.vk_id,
                            vk_group_id=marker + 1,
                            first_seen_at=now,
                            last_seen_at=now,
                            is_current=True,
                        )
                    )
                    await session.commit()
                replacement_pilot = await queue.plan_subscriptions(pilot=True)
                assert replacement_pilot != pilot_id
                manager = CampaignManager(sessions, settings)
                campaign_preview = await manager.plan_preview()
                gate = {
                    "decision": "passed",
                    **{
                        key: campaign_preview[key]
                        for key in (
                            "planning_configuration_hash",
                            "snapshot_users",
                            "already_resolved_users",
                            "discovery_due_users",
                        )
                    },
                    "capacity_report": "test",
                    "current_database_bytes": 1,
                    "aggregate_discovery_projected_growth_bytes": 1_000_000_000,
                    "aggregate_projected_growth_bytes": 1_000_000_000,
                    "projected_final_database_bytes": 7 * 1024**3 - 1,
                    "current_disk_free_bytes": 8 * 1024**3,
                    "safe_database_limit_bytes": 7 * 1024**3,
                }
                with pytest.raises(ValueError, match="capacity gate"):
                    await manager.plan(gate_evidence={"decision": "rejected"})
                async with sessions() as session:
                    assert (
                        int(await session.scalar(select(func.count(CollectionCampaign.id))) or 0)
                        == 0
                    )
                campaign_id = await manager.plan(gate_evidence=gate)
                async with sessions() as session:
                    campaign = await session.get(CollectionCampaign, campaign_id)
                    assert campaign is not None
                    assert campaign.phase == CampaignPhase.SUBSCRIPTION_METADATA.value
                    metadata_runs = int(
                        await session.scalar(
                            select(func.count(CollectionRun.id)).where(
                                CollectionRun.campaign_id == campaign_id,
                                CollectionRun.scope == "subscription_metadata",
                            )
                        )
                        or 0
                    )
                    assert metadata_runs == 1
                    metadata_run = await session.scalar(
                        select(CollectionRun).where(
                            CollectionRun.campaign_id == campaign_id,
                            CollectionRun.scope == "subscription_metadata",
                        )
                    )
                    assert metadata_run is not None
                    assert metadata_run.status == CollectionRunStatus.PAUSED_CAPACITY_LIMIT
                    assert metadata_run.total_jobs == 0
                    assert metadata_run.configuration["metadata_gate_pending"] is True
                    assert campaign.status == CampaignStatus.PAUSED_CAPACITY_LIMIT.value
                    assert campaign.configuration["metadata_capacity_gate"] == "pending"
                    assert (
                        await session.scalar(
                            select(func.count(CollectionJob.id)).where(
                                CollectionJob.collection_run_id == metadata_run.id
                            )
                        )
                        == 0
                    )
                    campaign.configuration = {
                        **campaign.configuration,
                        "metadata_capacity_gate": "passed",
                        "metadata_capacity_evidence": {
                            "current_database_bytes": 1,
                            "projected_final_database_bytes": 7 * 1024**3 - 1,
                            "metadata_due": 1,
                            "aggregate_projected_growth_bytes": 1024,
                        },
                    }
                    assert (
                        await manager.activate_metadata_cohort(session, campaign, metadata_run)
                        == metadata_run.id
                    )
                    await session.commit()
                    assert metadata_run.total_jobs == 1
                    assert metadata_run.status == CollectionRunStatus.PLANNED
                    job_types = set(
                        (
                            await session.scalars(
                                select(CollectionJob.job_type).where(
                                    CollectionJob.collection_run_id == metadata_run.id
                                )
                            )
                        ).all()
                    )
                    assert job_types == {"refresh_community_metadata"}
                assert await manager.plan(gate_evidence=gate) == campaign_id
            finally:
                await outer.rollback()
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_claim_clears_due_method_wait_on_run_and_campaign() -> None:
    engine = create_database_engine(database_url())
    settings = Settings(database_url=database_url(), collection_export_dir=".")
    try:
        async with engine.connect() as connection:
            outer = await connection.begin()
            sessions = async_sessionmaker(
                connection, expire_on_commit=False, join_transaction_mode="create_savepoint"
            )
            try:
                old_wakeup = datetime.now(UTC) - timedelta(minutes=1)
                async with sessions() as session:
                    campaign = CollectionCampaign(
                        campaign_type=f"claim-sync-{uuid.uuid4()}",
                        status="waiting_method_limit",
                        phase="waiting_method_limit",
                        snapshot_at=datetime.now(UTC),
                        configuration={"capacity_gate": "passed"},
                        configuration_hash=uuid.uuid4().hex,
                        next_wakeup_at=old_wakeup,
                        error_message="old method wait",
                    )
                    session.add(campaign)
                    await session.flush()
                    run = CollectionRun(
                        campaign_id=campaign.id,
                        scope="subscription_metadata",
                        status=CollectionRunStatus.WAITING_METHOD_LIMIT,
                        configuration={"capacity_gate": "passed"},
                        total_jobs=1,
                        next_wakeup_at=old_wakeup,
                        error_message="old method wait",
                    )
                    session.add(run)
                    await session.flush()
                    session.add(
                        CollectionJob(
                            collection_run_id=run.id,
                            job_type="refresh_community_metadata",
                            entity_type="vk_community",
                            entity_id=int(uuid.uuid4().hex[:10], 16),
                            status=JobStatus.RETRY_WAIT,
                            next_attempt_at=old_wakeup,
                        )
                    )
                    await session.commit()
                    campaign_id = campaign.id
                    run_id = run.id
                claimed = await CollectionQueue(sessions, settings).claim(run_id)
                assert claimed is not None
                async with sessions() as session:
                    run = await session.get(CollectionRun, run_id)
                    campaign = await session.get(CollectionCampaign, campaign_id)
                    assert run is not None and run.status == CollectionRunStatus.RUNNING
                    assert run.next_wakeup_at is None and run.error_message is None
                    assert campaign is not None and campaign.status == "running"
                    assert campaign.next_wakeup_at is None and campaign.error_message is None
            finally:
                await outer.rollback()
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_autonomous_light_repair_rejects_forbidden_job_type() -> None:
    engine = create_database_engine(database_url())
    settings = Settings(database_url=database_url(), collection_export_dir=".")
    try:
        async with engine.connect() as connection:
            outer = await connection.begin()
            sessions = async_sessionmaker(
                connection, expire_on_commit=False, join_transaction_mode="create_savepoint"
            )
            try:
                configuration = CollectionQueue(sessions, settings).collection_configuration()
                async with sessions() as session:
                    run = CollectionRun(
                        scope="light_repair",
                        status=CollectionRunStatus.PLANNED,
                        configuration={
                            "capacity_gate": "passed",
                            "collection": configuration,
                        },
                        total_jobs=1,
                    )
                    session.add(run)
                    await session.flush()
                    session.add(
                        CollectionJob(
                            collection_run_id=run.id,
                            job_type="collect_group_posts",
                            entity_type="group",
                            entity_id=1,
                        )
                    )
                    await session.commit()
                    run_id = run.id
                with pytest.raises(ValueError, match="запрещённые job types"):
                    await _validate_autonomous_run(sessions, settings, run_id)
            finally:
                await outer.rollback()
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_mixed_method_codes_are_deterministic_after_restart() -> None:
    engine = create_database_engine(database_url())
    try:
        async with engine.connect() as connection:
            outer = await connection.begin()
            sessions = async_sessionmaker(
                connection, expire_on_commit=False, join_transaction_mode="create_savepoint"
            )
            try:
                tokens = (f"token-{uuid.uuid4()}", f"token-{uuid.uuid4()}")
                current = [100.0]

                async def no_sleep(_seconds: float) -> None:
                    return None

                pool = TokenPool(
                    tokens,
                    rps=100,
                    clock=lambda: current[0],
                    sleep=no_sleep,
                    sessions=sessions,
                    flood_initial_cooldown=1000,
                    quota_initial_cooldown=1000,
                )
                first = await pool.acquire("groups.get")
                await pool.method_cooldown(first, 9)
                current[0] += 1
                second = await pool.acquire("groups.get")
                await pool.method_cooldown(second, 29)
                async with sessions() as session:
                    await session.execute(
                        update(VKTokenMethodState)
                        .where(VKTokenMethodState.token_fingerprint == first.fingerprint)
                        .values(last_error_at=datetime.now(UTC) - timedelta(minutes=1))
                    )
                    await session.execute(
                        update(VKTokenMethodState)
                        .where(VKTokenMethodState.token_fingerprint == second.fingerprint)
                        .values(last_error_at=datetime.now(UTC))
                    )
                    await session.commit()
                with pytest.raises(VKMethodUnavailable) as current_error:
                    await pool.acquire("groups.get")
                assert current_error.value.error_code == 29
                restarted = TokenPool(
                    tokens,
                    rps=100,
                    clock=lambda: current[0],
                    sleep=no_sleep,
                    sessions=sessions,
                    flood_initial_cooldown=1000,
                    quota_initial_cooldown=1000,
                )
                with pytest.raises(VKMethodUnavailable) as restarted_error:
                    await restarted.acquire("groups.get")
                assert restarted_error.value.error_code == 29
            finally:
                await outer.rollback()
    finally:
        await engine.dispose()

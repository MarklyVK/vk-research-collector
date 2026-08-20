from __future__ import annotations

import asyncio
import os
import time
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from sqlalchemy import delete, event, func, select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import async_sessionmaker

from vk_collector.collection.campaigns import CampaignManager
from vk_collector.collection.queue import CollectionQueue
from vk_collector.collection.reporting import capacity_gate_passed, latest_runnable_run_id
from vk_collector.collection.safety import DiskState
from vk_collector.collection.worker import CollectionWorker
from vk_collector.config import Settings
from vk_collector.database.models import (
    ClassificationStatus,
    CollectionCampaign,
    CollectionCampaignUser,
    CollectionJob,
    CollectionRun,
    CollectionRunStatus,
    CommunityPostCollectionState,
    GroupCandidate,
    GroupCollectionState,
    GroupLabel,
    GroupMembership,
    GroupPost,
    JobStatus,
    UserGroupSubscription,
    UserSubscriptionState,
    VKCommunity,
    VKTokenMethodState,
    VKTokenState,
    VKUser,
)
from vk_collector.database.session import create_database_engine
from vk_collector.privacy import delete_user, inspect_user
from vk_collector.vk import (
    TokenPool,
    VKAPIError,
    VKMethodUnavailable,
    VKSubscriptionIDsPage,
)
from vk_collector.vk.errors import VKRetryExhausted


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


class FakeVK:
    async def get_groups(self, group_ids: list[int]) -> list[dict[str, Any]]:
        return [{"id": group_ids[0], "name": "Обновлённая группа", "screen_name": "fake"}]

    async def get_wall_page(self, group_vk_id: int, offset: int, count: int) -> dict[str, Any]:
        if offset:
            return {"count": 1, "items": []}
        return {
            "count": 1,
            "items": [
                {
                    "id": 1,
                    "owner_id": -group_vk_id,
                    "date": 1_700_000_000,
                    "text": "Публичный пост",
                    "attachments": [{"type": "link", "link": {"url": "https://example.test"}}],
                }
            ],
        }

    async def get_members_page(self, group_vk_id: int, offset: int, count: int) -> dict[str, Any]:
        return {"count": 1, "items": [9_000_000_001] if offset == 0 else []}

    async def get_users(self, user_ids: list[int]) -> list[dict[str, Any]]:
        return [{"id": user_ids[0], "first_name": "Иван", "last_name": "Тестов"}]

    async def get_subscription_ids_page(
        self, user_vk_id: int, offset: int, count: int
    ) -> VKSubscriptionIDsPage:
        ids = (777,) if offset == 0 else ()
        return VKSubscriptionIDsPage(1, ids, offset, count, len(ids), offset + len(ids))


class PrivateSubscriptionsVK(FakeVK):
    async def get_subscription_ids_page(
        self, user_vk_id: int, offset: int, count: int
    ) -> VKSubscriptionIDsPage:
        raise VKAPIError(260, "Доступ к подпискам ограничен")


class AccessDeniedSubscriptionsVK(FakeVK):
    async def get_subscription_ids_page(
        self, user_vk_id: int, offset: int, count: int
    ) -> VKSubscriptionIDsPage:
        raise VKAPIError(15, "Доступ запрещён")


class FailingSubscriptionsVK(FakeVK):
    async def get_subscription_ids_page(
        self, user_vk_id: int, offset: int, count: int
    ) -> VKSubscriptionIDsPage:
        raise VKRetryExhausted("VK API недоступен после повторов")


class FakeClock:
    def __init__(self) -> None:
        self.value = 1000.0

    def __call__(self) -> float:
        return self.value

    async def sleep(self, seconds: float) -> None:
        self.value += seconds


@pytest.mark.asyncio
async def test_endpoint_state_survives_new_pool_and_does_not_block_other_method() -> None:
    engine = create_database_engine(database_url())
    token = f"integration-secret-{uuid.uuid4()}"
    clock = FakeClock()
    try:
        async with engine.connect() as connection:
            outer = await connection.begin()
            sessions = async_sessionmaker(
                connection,
                expire_on_commit=False,
                join_transaction_mode="create_savepoint",
            )
            first_pool = TokenPool(
                [token],
                rps=100,
                clock=clock,
                sleep=clock.sleep,
                sessions=sessions,
                flood_initial_cooldown=60,
            )
            lease = await first_pool.acquire("groups.get")
            await first_pool.method_cooldown(lease, 9)

            restarted_pool = TokenPool(
                [token],
                rps=100,
                clock=clock,
                sleep=clock.sleep,
                sessions=sessions,
                flood_initial_cooldown=60,
                probe_seconds=10,
                escalation_methods=2,
            )
            with pytest.raises(VKMethodUnavailable):
                await restarted_pool.acquire("groups.get")
            wall_lease = await restarted_pool.acquire("wall.get")
            assert wall_lease.method == "wall.get"
            assert token not in repr(wall_lease)

            # PostgreSQL резервирует ровно одну probe-попытку между worker-процессами.
            async with sessions() as session:
                await session.execute(
                    update(VKTokenMethodState)
                    .where(
                        VKTokenMethodState.token_fingerprint == lease.fingerprint,
                        VKTokenMethodState.method == "groups.get",
                    )
                    .values(next_probe_at=datetime.now(UTC) - timedelta(seconds=1))
                )
                await session.commit()
            probe = await restarted_pool.acquire("groups.get")
            assert probe.is_probe
            competing_pool = TokenPool(
                [token],
                rps=100,
                clock=clock,
                sleep=clock.sleep,
                sessions=sessions,
                flood_initial_cooldown=60,
                probe_seconds=10,
                escalation_methods=2,
            )
            with pytest.raises(VKMethodUnavailable):
                await competing_pool.acquire("groups.get")
            await restarted_pool.mark_success(probe)

            # Два разных endpoint-limit события дают короткую persisted escalation.
            wall = await restarted_pool.acquire("wall.get")
            await restarted_pool.method_cooldown(wall, 9)
            members = await restarted_pool.acquire("groups.getMembers")
            await restarted_pool.method_cooldown(members, 29)
            async with sessions() as session:
                token_state = await session.get(VKTokenState, lease.fingerprint)
                assert token_state is not None
                assert token_state.global_blocked_until is not None
            await outer.rollback()
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_queue_recovery_fake_full_path_rerun_and_privacy_rollback() -> None:
    engine = create_database_engine(database_url())
    marker = int(uuid.uuid4().hex[:10], 16)
    group_vk_id = marker + 10_000_000_000
    settings = Settings(
        database_url=database_url(),
        collection_max_concurrency=1,
        collection_posts_max_per_group=2,
        collection_members_max_per_group=1,
        collection_subscriptions_enabled=True,
        collection_subscriptions_max_per_user=1,
        collection_export_dir="/tmp",
    )
    try:
        async with engine.connect() as connection:
            outer = await connection.begin()
            sessions = async_sessionmaker(
                connection,
                expire_on_commit=False,
                autoflush=False,
                join_transaction_mode="create_savepoint",
            )
            try:
                async with sessions() as session:
                    group = GroupCandidate(
                        vk_id=group_vk_id,
                        name="Fixture",
                        description="",
                        status_text="",
                        screen_name=f"stage2_{marker}",
                        address=f"https://vk.com/stage2_{marker}",
                        first_seen_at=datetime.now(UTC),
                        last_seen_at=datetime.now(UTC),
                        classification_status=ClassificationStatus.APPROVED,
                    )
                    session.add(group)
                    await session.flush()
                    run = CollectionRun(scope="pilot", status=CollectionRunStatus.PLANNED)
                    session.add(run)
                    await session.flush()
                    for priority, job_type in enumerate(
                        ("refresh_group", "collect_group_posts", "collect_group_members"), 10
                    ):
                        session.add(
                            CollectionJob(
                                collection_run_id=run.id,
                                job_type=job_type,
                                entity_type="group",
                                entity_id=group.id,
                                priority=priority,
                            )
                        )
                    await session.commit()
                    run_id = run.id
                    group_id = group.id

                queue = CollectionQueue(sessions, settings)
                async with sessions() as session:
                    runnable = CollectionRun(
                        scope="full",
                        status=CollectionRunStatus.PLANNED,
                        configuration={
                            "capacity_gate": "passed",
                            "collection": queue.collection_configuration(),
                        },
                    )
                    session.add(runnable)
                    await session.commit()
                    runnable_id = runnable.id
                assert await latest_runnable_run_id(sessions) == runnable_id
                async with sessions() as session:
                    waiting_run = await session.get(CollectionRun, runnable_id)
                    assert waiting_run is not None
                    waiting_run.status = CollectionRunStatus.WAITING_METHOD_LIMIT
                    await session.commit()
                assert await capacity_gate_passed(sessions, runnable_id)

                await queue.set_run_status(run_id, CollectionRunStatus.PAUSED)
                await queue.refresh_run(run_id)
                async with sessions() as session:
                    paused_run = await session.get(CollectionRun, run_id)
                    assert paused_run is not None
                    assert paused_run.status == CollectionRunStatus.PAUSED
                    assert (
                        await session.scalar(
                            select(func.count(CollectionJob.id)).where(
                                CollectionJob.collection_run_id == run_id,
                                CollectionJob.status == JobStatus.PAUSED,
                            )
                        )
                        == 3
                    )
                await queue.set_run_status(run_id, CollectionRunStatus.RUNNING)

                claimed = await queue.claim(run_id)
                assert claimed is not None
                second = await queue.claim(run_id)
                assert second is not None  # второй независимый job через SKIP LOCKED
                async with sessions() as session:
                    await session.execute(
                        update(CollectionJob)
                        .where(CollectionJob.id == claimed.id)
                        .values(locked_at=datetime.now(UTC) - timedelta(hours=1))
                    )
                    await session.execute(
                        update(CollectionJob)
                        .where(CollectionJob.id == second.id)
                        .values(status=JobStatus.PENDING, locked_at=None, locked_by=None)
                    )
                    await session.commit()
                assert await queue.recover_expired(run_id) == 1

                # Немедленный restart до истечения lease всё равно восстанавливает job
                # в процессе долгой работы, без необходимости перезапускать worker ещё раз.
                fast_settings = settings.model_copy(
                    update={
                        "collection_job_lease_seconds": 1,
                        "collection_idle_sleep_seconds": 0.05,
                    }
                )
                async with sessions() as session:
                    recovery_run = CollectionRun(scope="pilot", status=CollectionRunStatus.PLANNED)
                    session.add(recovery_run)
                    await session.flush()
                    session.add(
                        CollectionJob(
                            collection_run_id=recovery_run.id,
                            job_type="refresh_group",
                            entity_type="group",
                            entity_id=group_id,
                            priority=10,
                        )
                    )
                    await session.commit()
                    recovery_run_id = recovery_run.id
                fast_queue = CollectionQueue(sessions, fast_settings)
                assert await fast_queue.claim(recovery_run_id) is not None
                await asyncio.wait_for(
                    CollectionWorker(sessions, FakeVK(), fast_settings).run(
                        recovery_run_id, until_idle=False
                    ),
                    timeout=4,
                )  # type: ignore[arg-type]
                async with sessions() as session:
                    recovered_run = await session.get(CollectionRun, recovery_run_id)
                    assert recovered_run is not None
                    assert recovered_run.status == CollectionRunStatus.COMPLETED

                await CollectionWorker(sessions, FakeVK(), settings).run(run_id)  # type: ignore[arg-type]
                async with sessions() as session:
                    assert (
                        await session.scalar(
                            select(func.count(GroupPost.id)).where(GroupPost.group_id == group_id)
                        )
                        == 1
                    )
                    assert (
                        await session.scalar(
                            select(func.count(GroupMembership.id)).where(
                                GroupMembership.group_id == group_id
                            )
                        )
                        == 1
                    )
                    assert await session.scalar(select(func.count(VKUser.vk_id))) >= 1
                    assert (
                        await session.scalar(
                            select(func.count(UserGroupSubscription.id)).where(
                                UserGroupSubscription.user_id == 9_000_000_001
                            )
                        )
                        == 1
                    )

                # Posts планируются отдельным run и общий TTL исключает немедленный rerun.
                with pytest.raises(ValueError, match="production subscriptions run"):
                    await queue.plan_subscription_posts(pilot=False)
                async with sessions() as session:
                    pilot_a = CollectionRun(
                        scope="subscriptions_pilot",
                        status=CollectionRunStatus.COMPLETED,
                        finished_at=datetime.now(UTC),
                    )
                    session.add(pilot_a)
                    await session.flush()
                    await session.execute(
                        update(UserGroupSubscription)
                        .where(UserGroupSubscription.user_id == 9_000_000_001)
                        .values(source_run_id=pilot_a.id)
                    )
                    await session.commit()
                    pilot_a_run_id = pilot_a.id
                posts_run_id = await queue.plan_subscription_posts(
                    pilot=True, source_run_id=pilot_a_run_id
                )
                await CollectionWorker(sessions, FakeVK(), settings).run(posts_run_id)  # type: ignore[arg-type]
                async with sessions() as session:
                    post_state = await session.get(CommunityPostCollectionState, 777)
                    assert post_state is not None
                    assert post_state.collected_count == 1
                    assert post_state.next_scheduled_at is not None
                fresh_posts_run_id = await queue.plan_subscription_posts(
                    pilot=True, source_run_id=pilot_a_run_id
                )
                async with sessions() as session:
                    fresh_posts_run = await session.get(CollectionRun, fresh_posts_run_id)
                    assert fresh_posts_run is not None
                    assert fresh_posts_run.status == CollectionRunStatus.COMPLETED
                    assert fresh_posts_run.total_jobs == 0

                # Private state соблюдает next_scheduled_at, а completed plan после TTL
                # не блокирует создание нового run с тем же cohort.
                async with sessions() as session:
                    state = await session.get(UserSubscriptionState, 9_000_000_001)
                    assert state is not None
                    state.last_success_at = None
                    state.privacy_denied = True
                    state.next_scheduled_at = datetime.now(UTC) + timedelta(days=1)
                    await session.commit()
                private_fresh_run_id = await queue.plan_subscriptions(pilot=True)
                async with sessions() as session:
                    private_fresh_run = await session.get(CollectionRun, private_fresh_run_id)
                    assert private_fresh_run is not None
                    assert private_fresh_run.total_jobs == 0
                    state = await session.get(UserSubscriptionState, 9_000_000_001)
                    assert state is not None
                    state.next_scheduled_at = datetime.now(UTC) - timedelta(seconds=1)
                    await session.commit()
                due_run_id = await queue.plan_subscriptions(pilot=True)
                async with sessions() as session:
                    due_run = await session.get(CollectionRun, due_run_id)
                    assert due_run is not None
                    assert due_run.total_jobs == 1
                    due_run.status = CollectionRunStatus.COMPLETED
                    await session.execute(
                        update(CollectionJob)
                        .where(CollectionJob.collection_run_id == due_run_id)
                        .values(status=JobStatus.COMPLETED)
                    )
                    await session.commit()
                renewed_run_id = await queue.plan_subscriptions(pilot=True)
                assert renewed_run_id != due_run_id
                await CollectionWorker(sessions, PrivateSubscriptionsVK(), settings).run(
                    renewed_run_id
                )  # type: ignore[arg-type]
                async with sessions() as session:
                    renewed_job = await session.scalar(
                        select(CollectionJob).where(
                            CollectionJob.collection_run_id == renewed_run_id
                        )
                    )
                    private_state = await session.get(UserSubscriptionState, 9_000_000_001)
                    assert renewed_job is not None
                    assert renewed_job.status == JobStatus.SKIPPED
                    assert renewed_job.last_error_type == "subscriptions_private"
                    assert private_state is not None
                    assert private_state.privacy_denied
                    assert private_state.next_scheduled_at is not None

                    private_state.next_scheduled_at = datetime.now(UTC) - timedelta(seconds=1)
                    await session.commit()
                denied_run_id = await queue.plan_subscriptions(pilot=True)
                await CollectionWorker(sessions, AccessDeniedSubscriptionsVK(), settings).run(
                    denied_run_id
                )  # type: ignore[arg-type]
                async with sessions() as session:
                    denied_state = await session.get(UserSubscriptionState, 9_000_000_001)
                    assert denied_state is not None
                    assert denied_state.privacy_denied
                    assert denied_state.last_error_code == 15
                    assert denied_state.next_scheduled_at > datetime.now(UTC)

                # Transient-сбой также создаёт короткий cooldown и не блокирует
                # каждую следующую pilot cohort одним и тем же user.
                transient_user_id = 9_000_000_002
                async with sessions() as session:
                    transient_user = await session.get(VKUser, transient_user_id)
                    if transient_user is None:
                        session.add(
                            VKUser(
                                vk_id=transient_user_id,
                                first_name="Сетевой",
                                last_name="Сбой",
                                is_closed=False,
                                can_access_closed=True,
                                first_seen_at=datetime.now(UTC),
                                last_seen_at=datetime.now(UTC),
                            )
                        )
                        await session.commit()
                transient_run = CollectionRun(
                    scope="subscriptions_pilot", status=CollectionRunStatus.PLANNED
                )
                async with sessions() as session:
                    session.add(transient_run)
                    await session.flush()
                    transient_job = CollectionJob(
                        collection_run_id=transient_run.id,
                        job_type="collect_user_subscriptions",
                        entity_type="user",
                        entity_id=transient_user_id,
                    )
                    session.add(transient_job)
                    transient_run.total_jobs = 1
                    await session.commit()
                await CollectionWorker(sessions, FailingSubscriptionsVK(), settings).run(
                    transient_run.id
                )  # type: ignore[arg-type]
                async with sessions() as session:
                    transient_state = await session.get(UserSubscriptionState, transient_user_id)
                    assert transient_state is not None
                    assert transient_state.next_scheduled_at > datetime.now(UTC)

                # Новый run с теми же сущностями обновляет строки, но не создаёт дублей.
                async with sessions() as session:
                    rerun = CollectionRun(scope="pilot", status=CollectionRunStatus.PLANNED)
                    session.add(rerun)
                    await session.flush()
                    for priority, job_type in enumerate(
                        ("collect_group_posts", "collect_group_members"), 10
                    ):
                        session.add(
                            CollectionJob(
                                collection_run_id=rerun.id,
                                job_type=job_type,
                                entity_type="group",
                                entity_id=group_id,
                                priority=priority,
                            )
                        )
                    await session.commit()
                    rerun_id = rerun.id
                await CollectionWorker(sessions, FakeVK(), settings).run(rerun_id)  # type: ignore[arg-type]
                async with sessions() as session:
                    assert (
                        await session.scalar(
                            select(func.count(GroupPost.id)).where(GroupPost.group_id == group_id)
                        )
                        == 1
                    )
                    before = await inspect_user(session, 9_000_000_001)
                    nested = await session.begin_nested()
                    await delete_user(session, 9_000_000_001)
                    await nested.rollback()
                    after = await inspect_user(session, 9_000_000_001)
                    assert before == after
            finally:
                await outer.rollback()
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_discovery_bulk_writes_overlapping_communities_without_duplicates() -> None:
    engine = create_database_engine(database_url())
    marker = int(uuid.uuid4().hex[:10], 16)
    settings = Settings(
        database_url=database_url(),
        collection_max_concurrency=2,
        collection_subscriptions_max_per_user=50,
        collection_subscriptions_page_size=50,
    )
    run_id: uuid.UUID | None = None
    campaign_id: uuid.UUID | None = None
    user_ids = [marker + 30_000_000_000 + index for index in range(500)]
    community_ids = [marker + 31_000_000_000 + index for index in range(50)]

    class OverlappingSubscriptionsVK(FakeVK):
        async def get_subscription_ids_page(
            self, user_vk_id: int, offset: int, count: int
        ) -> VKSubscriptionIDsPage:
            values = community_ids if user_vk_id % 2 else list(reversed(community_ids))
            page = tuple(values[offset : offset + count])
            return VKSubscriptionIDsPage(50, page, offset, count, len(page), offset + len(page))

    sessions = async_sessionmaker(engine, expire_on_commit=False)
    storage_statements: list[str] = []
    campaign_count_statements: list[str] = []

    def count_storage_statement(
        _connection: object,
        _cursor: object,
        statement: str,
        _parameters: object,
        _context: object,
        _executemany: bool,
    ) -> None:
        normalized = statement.upper()
        if any(
            table in normalized
            for table in (
                "VK_COMMUNITIES",
                "USER_GROUP_SUBSCRIPTIONS",
                "USER_SUBSCRIPTION_STATES",
            )
        ):
            storage_statements.append(normalized)
        if "COUNT(" in normalized and "COLLECTION_RUNS.CAMPAIGN_ID" in normalized:
            campaign_count_statements.append(normalized)

    event.listen(engine.sync_engine, "before_cursor_execute", count_storage_statement)
    try:
        async with sessions() as session:
            now = datetime.now(UTC)
            session.add_all(
                [
                    VKUser(
                        vk_id=user_id,
                        is_closed=False,
                        can_access_closed=True,
                        first_seen_at=now,
                        last_seen_at=now,
                    )
                    for user_id in user_ids
                ]
            )
            campaign = CollectionCampaign(
                campaign_type=f"benchmark_{uuid.uuid4().hex}",
                status="running",
                phase="subscription_discovery",
                snapshot_at=now,
                snapshot_max_user_id=max(user_ids),
                snapshot_user_count=len(user_ids),
                configuration={"capacity_gate": "passed"},
                configuration_hash=uuid.uuid4().hex,
                last_planned_user_id=max(user_ids),
            )
            session.add(campaign)
            await session.flush()
            session.add_all(
                [
                    CollectionCampaignUser(campaign_id=campaign.id, user_id=user_id)
                    for user_id in user_ids
                ]
            )
            run = CollectionRun(
                campaign_id=campaign.id,
                scope="subscription_discovery",
                status=CollectionRunStatus.PLANNED,
            )
            session.add(run)
            await session.flush()
            session.add_all(
                [
                    CollectionJob(
                        collection_run_id=run.id,
                        job_type="collect_user_subscriptions",
                        entity_type="user",
                        entity_id=user_id,
                    )
                    for user_id in reversed(user_ids)
                ]
            )
            run.total_jobs = len(user_ids)
            await session.commit()
            run_id = run.id
            campaign_id = campaign.id
        started_at = time.monotonic()
        await CollectionWorker(sessions, OverlappingSubscriptionsVK(), settings).run(  # type: ignore[arg-type]
            run_id
        )
        duration_seconds = time.monotonic() - started_at
        async with sessions() as session:
            assert (
                await session.scalar(
                    select(func.count(UserGroupSubscription.id)).where(
                        UserGroupSubscription.user_id.in_(user_ids),
                        UserGroupSubscription.vk_group_id.in_(community_ids),
                    )
                )
                == len(user_ids) * 50
            )
            assert (
                await session.scalar(
                    select(VKCommunity.metadata_updated_at).where(
                        VKCommunity.vk_id == community_ids[0]
                    )
                )
                is None
            )
            assert (
                await session.scalar(
                    select(func.count(CollectionRun.id)).where(
                        CollectionRun.campaign_id == campaign_id,
                        CollectionRun.scope == "subscription_metadata",
                    )
                )
                == 1
            )
        row_by_row_baseline = len(user_ids) * 50 * 2
        assert len(storage_statements) <= len(user_ids) * 10
        assert len(storage_statements) < row_by_row_baseline
        assert len(campaign_count_statements) <= 4
        assert settings.collection_subscriptions_page_size == 50
        print(
            "campaign_benchmark "
            f"duration_seconds={duration_seconds:.3f} "
            f"storage_statements={len(storage_statements)} "
            f"campaign_counts={len(campaign_count_statements)}"
        )
    finally:
        event.remove(engine.sync_engine, "before_cursor_execute", count_storage_statement)
        async with sessions() as session:
            if campaign_id is not None:
                await session.execute(
                    delete(CollectionCampaign).where(CollectionCampaign.id == campaign_id)
                )
            await session.execute(delete(VKUser).where(VKUser.vk_id.in_(user_ids)))
            await session.execute(delete(VKCommunity).where(VKCommunity.vk_id.in_(community_ids)))
            await session.commit()
        await engine.dispose()


@pytest.mark.asyncio
async def test_campaign_is_idempotent_and_metadata_waits_for_discovery() -> None:
    engine = create_database_engine(database_url())
    marker = int(uuid.uuid4().hex[:10], 16)
    settings = Settings(
        database_url=database_url(),
        collection_campaign_cohort_users=2,
        collection_community_metadata_ttl_days=31,
        collection_subscription_snapshot_user_limit=1,
    )
    try:
        async with engine.connect() as connection:
            outer = await connection.begin()
            sessions = async_sessionmaker(
                connection,
                expire_on_commit=False,
                join_transaction_mode="create_savepoint",
            )
            try:
                now = datetime.now(UTC)
                async with sessions() as session:
                    group = GroupCandidate(
                        vk_id=marker + 40_000_000_000,
                        name="Campaign group",
                        description="",
                        status_text="",
                        address=f"https://vk.com/{marker}",
                        first_seen_at=now,
                        last_seen_at=now,
                        classification_status=ClassificationStatus.APPROVED,
                    )
                    session.add(group)
                    await session.flush()
                    users = [
                        VKUser(
                            vk_id=marker + 41_000_000_000 + index,
                            is_closed=False,
                            can_access_closed=True,
                            first_seen_at=now,
                            last_seen_at=now,
                        )
                        for index in range(2)
                    ]
                    session.add_all(users)
                    await session.flush()
                    session.add_all(
                        [
                            GroupMembership(
                                group_id=group.id,
                                user_id=user.vk_id,
                                first_seen_at=now,
                                last_seen_at=now,
                                is_current=True,
                            )
                            for user in users
                        ]
                    )
                    await session.commit()
                manager = CampaignManager(sessions, settings)
                campaign_preview = await manager.plan_preview()
                assert campaign_preview["snapshot_users"] == 1
                assert campaign_preview["discovery_due_users"] == 1
                assert campaign_preview["eligible_users"] == 2
                assert campaign_preview["eligible_due_users"] == 2
                assert campaign_preview["bounded_snapshot"] is True
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
                campaign_id = await manager.plan(gate_evidence=gate)
                assert await manager.plan(gate_evidence=gate) == campaign_id
                incompatible = CampaignManager(
                    sessions,
                    settings.model_copy(update={"collection_community_metadata_ttl_days": 32}),
                )
                with pytest.raises(ValueError, match=r"другой.*planning"):
                    await incompatible.plan(gate_evidence=gate)
                async with sessions() as session:
                    campaign = await session.get(CollectionCampaign, campaign_id)
                    assert campaign is not None
                    assert campaign.phase == "subscription_discovery"
                    nested = await session.begin_nested()
                    session.add(
                        CollectionCampaign(
                            campaign_type="subscription_enrichment",
                            status="planned",
                            phase="subscription_discovery",
                            snapshot_at=now,
                            snapshot_max_user_id=0,
                            configuration={},
                            configuration_hash=uuid.uuid4().hex,
                        )
                    )
                    with pytest.raises(IntegrityError):
                        await session.flush()
                    await nested.rollback()
                    assert (
                        await session.scalar(
                            select(func.count(CollectionJob.id)).where(
                                CollectionJob.job_type == "refresh_community_metadata",
                                CollectionJob.collection_run_id.in_(
                                    select(CollectionRun.id).where(
                                        CollectionRun.campaign_id == campaign_id
                                    )
                                ),
                            )
                        )
                        == 0
                    )
                    discovery_jobs = list(
                        (
                            await session.scalars(
                                select(CollectionJob)
                                .join(
                                    CollectionRun,
                                    CollectionRun.id == CollectionJob.collection_run_id,
                                )
                                .where(CollectionRun.campaign_id == campaign_id)
                            )
                        ).all()
                    )
                    for job in discovery_jobs:
                        session.add(
                            UserSubscriptionState(
                                user_id=job.entity_id,
                                last_success_at=now,
                                next_scheduled_at=now + timedelta(days=30),
                                collected_limit=50,
                                last_run_id=job.collection_run_id,
                                last_campaign_id=campaign_id,
                            )
                        )
                        job.status = JobStatus.COMPLETED
                    session.add(
                        VKCommunity(
                            vk_id=marker + 42_000_000_000,
                            first_seen_at=now,
                            last_seen_at=now,
                        )
                    )
                    await session.flush()
                    for user in users:
                        session.add(
                            UserGroupSubscription(
                                user_id=user.vk_id,
                                vk_group_id=marker + 42_000_000_000,
                                first_seen_at=now,
                                last_seen_at=now,
                                is_current=True,
                            )
                        )
                    await session.commit()
                async with sessions() as session:
                    snapshot_before = int(
                        await session.scalar(
                            select(func.count(CollectionCampaignUser.user_id)).where(
                                CollectionCampaignUser.campaign_id == campaign_id
                            )
                        )
                        or 0
                    )
                    memberships = list(
                        (
                            await session.scalars(
                                select(GroupMembership).where(
                                    GroupMembership.user_id.in_([user.vk_id for user in users])
                                )
                            )
                        ).all()
                    )
                    for membership in memberships:
                        membership.is_current = False
                    await session.commit()
                assert snapshot_before == 1
                assert (await manager.status(campaign_id))["eligible_users"] == 1
                await manager.reconcile(campaign_id)
                async with sessions() as session:
                    campaign = await session.get(CollectionCampaign, campaign_id)
                    assert campaign is not None
                    assert campaign.phase == "subscription_metadata"
                    assert campaign.status == "paused_capacity_limit"
                    assert (
                        await session.scalar(
                            select(func.count(CollectionJob.id))
                            .join(
                                CollectionRun,
                                CollectionRun.id == CollectionJob.collection_run_id,
                            )
                            .where(
                                CollectionRun.campaign_id == campaign_id,
                                CollectionJob.job_type == "refresh_community_metadata",
                            )
                        )
                        == 0
                    )
                    metadata_run = await session.scalar(
                        select(CollectionRun).where(
                            CollectionRun.campaign_id == campaign_id,
                            CollectionRun.scope == "subscription_metadata",
                        )
                    )
                    assert metadata_run is not None
                    assert metadata_run.status == CollectionRunStatus.PAUSED_CAPACITY_LIMIT
                    assert metadata_run.total_jobs == 0
            finally:
                await outer.rollback()
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_campaign_apply_rolls_back_when_eligible_or_resolved_counts_change() -> None:
    engine = create_database_engine(database_url())
    marker = int(uuid.uuid4().hex[:10], 16) + 60_000_000_000
    settings = Settings(database_url=database_url(), collection_campaign_cohort_users=2)
    try:
        async with engine.connect() as connection:
            outer = await connection.begin()
            sessions = async_sessionmaker(
                connection, expire_on_commit=False, join_transaction_mode="create_savepoint"
            )
            try:
                now = datetime.now(UTC)
                async with sessions() as session:
                    group = GroupCandidate(
                        vk_id=marker,
                        name="Aggregate race group",
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
                manager = CampaignManager(sessions, settings)
                preview = await manager.plan_preview()
                gate = {
                    "decision": "passed",
                    **{
                        key: preview[key]
                        for key in (
                            "planning_configuration_hash",
                            "snapshot_users",
                            "already_resolved_users",
                            "discovery_due_users",
                        )
                    },
                    "current_database_bytes": 1,
                    "aggregate_discovery_projected_growth_bytes": 1_000_000,
                    "aggregate_projected_growth_bytes": 1_000_000,
                    "projected_final_database_bytes": 2_000_000_000,
                    "current_disk_free_bytes": 8 * 1024**3,
                    "safe_database_limit_bytes": 7 * 1024**3,
                }
                async with sessions() as session:
                    second = VKUser(
                        vk_id=marker + 1,
                        is_closed=False,
                        can_access_closed=True,
                        first_seen_at=now,
                        last_seen_at=now,
                    )
                    session.add(second)
                    await session.flush()
                    session.add(
                        GroupMembership(
                            group_id=group.id,
                            user_id=second.vk_id,
                            first_seen_at=now,
                            last_seen_at=now,
                            is_current=True,
                        )
                    )
                    await session.commit()
                with pytest.raises(ValueError, match="Eligible/resolved counts"):
                    await manager.plan(gate_evidence=gate)
                async with sessions() as session:
                    assert (
                        int(await session.scalar(select(func.count(CollectionCampaign.id))) or 0)
                        == 0
                    )
                    assert (
                        int(
                            await session.scalar(select(func.count(CollectionCampaignUser.user_id)))
                            or 0
                        )
                        == 0
                    )
            finally:
                await outer.rollback()
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_next_discovery_cohort_pauses_when_durable_budget_is_exceeded(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine = create_database_engine(database_url())
    marker = int(uuid.uuid4().hex[:10], 16) + 70_000_000_000
    settings = Settings(database_url=database_url(), collection_campaign_cohort_users=2)
    try:
        async with engine.connect() as connection:
            outer = await connection.begin()
            sessions = async_sessionmaker(
                connection, expire_on_commit=False, join_transaction_mode="create_savepoint"
            )
            try:
                now = datetime.now(UTC)
                async with sessions() as session:
                    group = GroupCandidate(
                        vk_id=marker,
                        name="Durable budget group",
                        description="",
                        status_text="",
                        address=f"https://vk.com/{marker}",
                        first_seen_at=now,
                        last_seen_at=now,
                        classification_status=ClassificationStatus.APPROVED,
                    )
                    session.add(group)
                    await session.flush()
                    users = [
                        VKUser(
                            vk_id=marker + index,
                            is_closed=False,
                            can_access_closed=True,
                            first_seen_at=now,
                            last_seen_at=now,
                        )
                        for index in range(3)
                    ]
                    session.add_all(users)
                    await session.flush()
                    session.add_all(
                        [
                            GroupMembership(
                                group_id=group.id,
                                user_id=user.vk_id,
                                first_seen_at=now,
                                last_seen_at=now,
                                is_current=True,
                            )
                            for user in users
                        ]
                    )
                    initial_database = int(
                        await session.scalar(select(func.pg_database_size(func.current_database())))
                        or 0
                    )
                    await session.commit()
                manager = CampaignManager(sessions, settings)
                preview = await manager.plan_preview()
                gate = {
                    "decision": "passed",
                    **{
                        key: preview[key]
                        for key in (
                            "planning_configuration_hash",
                            "snapshot_users",
                            "already_resolved_users",
                            "discovery_due_users",
                        )
                    },
                    "current_database_bytes": initial_database,
                    "aggregate_discovery_projected_growth_bytes": 100,
                    "aggregate_projected_growth_bytes": 100,
                    "projected_final_database_bytes": initial_database + 100,
                    "current_disk_free_bytes": 8 * 1024**3,
                    "safe_database_limit_bytes": 7 * 1024**3,
                }
                campaign_id = await manager.plan(gate_evidence=gate)
                async with sessions() as session:
                    first_run = await session.scalar(
                        select(CollectionRun).where(CollectionRun.campaign_id == campaign_id)
                    )
                    assert first_run is not None and first_run.total_jobs == 2
                    jobs = list(
                        (
                            await session.scalars(
                                select(CollectionJob).where(
                                    CollectionJob.collection_run_id == first_run.id
                                )
                            )
                        ).all()
                    )
                    for job in jobs:
                        job.status = JobStatus.COMPLETED
                        session.add(
                            UserSubscriptionState(
                                user_id=job.entity_id,
                                last_success_at=now,
                                next_scheduled_at=now + timedelta(days=30),
                                last_run_id=first_run.id,
                                last_campaign_id=campaign_id,
                            )
                        )
                    await session.commit()
                monkeypatch.setattr(
                    "vk_collector.collection.campaigns.inspect_disk",
                    lambda *_args, **_kwargs: DiskState(
                        used_percent=85.0,
                        warning=True,
                        stop=False,
                        total_bytes=10 * 1024**3,
                        free_bytes=1,
                    ),
                )
                await manager.reconcile(campaign_id)
                async with sessions() as session:
                    campaign = await session.get(CollectionCampaign, campaign_id)
                    assert campaign is not None
                    assert campaign.status == "paused_capacity_limit"
                    assert campaign.last_planned_user_id == jobs[-1].entity_id
                    assert "Следующий discovery cohort отклонён" in str(campaign.error_message)
                    assert (
                        int(
                            await session.scalar(
                                select(func.count(CollectionRun.id)).where(
                                    CollectionRun.campaign_id == campaign_id
                                )
                            )
                            or 0
                        )
                        == 1
                    )
            finally:
                await outer.rollback()
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_campaign_transient_subscription_remains_durable_after_fast_retries() -> None:
    engine = create_database_engine(database_url())
    settings = Settings(database_url=database_url())
    marker = int(uuid.uuid4().hex[:10], 16) + 50_000_000_000
    try:
        async with engine.connect() as connection:
            outer = await connection.begin()
            sessions = async_sessionmaker(
                connection,
                expire_on_commit=False,
                join_transaction_mode="create_savepoint",
            )
            try:
                now = datetime.now(UTC)
                async with sessions() as session:
                    user = VKUser(
                        vk_id=marker,
                        is_closed=False,
                        can_access_closed=True,
                        first_seen_at=now,
                        last_seen_at=now,
                    )
                    campaign = CollectionCampaign(
                        campaign_type=f"test_transient_{uuid.uuid4().hex}",
                        status="running",
                        phase="subscription_discovery",
                        snapshot_at=now,
                        snapshot_max_user_id=marker,
                        snapshot_user_count=1,
                        configuration={},
                        configuration_hash=uuid.uuid4().hex,
                    )
                    session.add_all([user, campaign])
                    await session.flush()
                    session.add(CollectionCampaignUser(campaign_id=campaign.id, user_id=marker))
                    run = CollectionRun(
                        campaign_id=campaign.id,
                        scope="subscription_discovery",
                        status=CollectionRunStatus.PLANNED,
                    )
                    session.add(run)
                    await session.flush()
                    session.add(
                        CollectionJob(
                            collection_run_id=run.id,
                            job_type="collect_user_subscriptions",
                            entity_type="user",
                            entity_id=marker,
                            attempt_count=5,
                        )
                    )
                    run.total_jobs = 1
                    await session.commit()
                    campaign_id = campaign.id
                    run_id = run.id
                await CollectionWorker(sessions, FailingSubscriptionsVK(), settings).run(
                    run_id, max_jobs=1
                )  # type: ignore[arg-type]
                async with sessions() as session:
                    job = await session.scalar(
                        select(CollectionJob).where(CollectionJob.collection_run_id == run_id)
                    )
                    campaign = await session.get(CollectionCampaign, campaign_id)
                    assert job is not None
                    assert job.status == JobStatus.RETRY_WAIT
                    assert job.attempt_count == 6
                    assert job.next_attempt_at is not None
                    assert job.next_attempt_at > datetime.now(UTC) + timedelta(hours=10)
                    assert campaign is not None
                    assert campaign.status == "running"
            finally:
                await outer.rollback()
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_deferred_subscription_does_not_block_next_cohort() -> None:
    engine = create_database_engine(database_url())
    settings = Settings(
        database_url=database_url(),
        collection_export_dir=".",
        collection_campaign_cohort_users=2,
        collection_safe_database_limit_bytes=100 * 1024**3,
        disk_warning_percent=100,
        disk_stop_percent=100,
    )
    marker = int(uuid.uuid4().hex[:10], 16) + 55_000_000_000
    try:
        async with engine.connect() as connection:
            outer = await connection.begin()
            sessions = async_sessionmaker(
                connection,
                expire_on_commit=False,
                join_transaction_mode="create_savepoint",
            )
            try:
                now = datetime.now(UTC)
                async with sessions() as session:
                    database_bytes = int(
                        await session.scalar(select(func.pg_database_size(func.current_database())))
                        or 0
                    )
                    users = [
                        VKUser(
                            vk_id=marker + index,
                            is_closed=False,
                            can_access_closed=True,
                            first_seen_at=now,
                            last_seen_at=now,
                        )
                        for index in range(3)
                    ]
                    campaign = CollectionCampaign(
                        campaign_type="subscription_enrichment",
                        status="running",
                        phase="subscription_discovery",
                        snapshot_at=now,
                        snapshot_max_user_id=users[-1].vk_id,
                        snapshot_user_count=3,
                        last_planned_user_id=users[1].vk_id,
                        configuration={
                            "capacity_gate": "passed",
                            "current_database_bytes": database_bytes,
                            "projected_final_database_bytes": database_bytes + 1024**2,
                            "discovery_due_users": 3,
                            "aggregate_discovery_projected_growth_bytes": 0,
                            "safe_database_limit_bytes": 100 * 1024**3,
                        },
                        configuration_hash=uuid.uuid4().hex,
                    )
                    session.add_all([*users, campaign])
                    await session.flush()
                    session.add_all(
                        [
                            CollectionCampaignUser(
                                campaign_id=campaign.id,
                                user_id=user.vk_id,
                            )
                            for user in users
                        ]
                    )
                    run = CollectionRun(
                        campaign_id=campaign.id,
                        scope="subscription_discovery",
                        status=CollectionRunStatus.RUNNING,
                        total_jobs=2,
                    )
                    session.add(run)
                    await session.flush()
                    deferred = CollectionJob(
                        collection_run_id=run.id,
                        job_type="collect_user_subscriptions",
                        entity_type="user",
                        entity_id=users[0].vk_id,
                        status=JobStatus.RETRY_WAIT,
                        next_attempt_at=now + timedelta(hours=6),
                    )
                    finishing = CollectionJob(
                        collection_run_id=run.id,
                        job_type="collect_user_subscriptions",
                        entity_type="user",
                        entity_id=users[1].vk_id,
                        status=JobStatus.RUNNING,
                    )
                    session.add_all([deferred, finishing])
                    await session.commit()
                    campaign_id = campaign.id
                    finishing_id = finishing.id

                became_terminal = await CollectionQueue(sessions, settings).finish(
                    finishing_id,
                    JobStatus.COMPLETED,
                )
                assert became_terminal is False

                async with sessions() as session:
                    jobs = list(
                        (
                            await session.execute(
                                select(CollectionJob.entity_id, CollectionJob.status)
                                .join(
                                    CollectionRun,
                                    CollectionRun.id == CollectionJob.collection_run_id,
                                )
                                .where(CollectionRun.campaign_id == campaign_id)
                            )
                        ).all()
                    )
                    campaign = await session.get(CollectionCampaign, campaign_id)
                    assert campaign is not None and campaign.status == "running"
                    assert len(jobs) == 3
                    assert (users[0].vk_id, JobStatus.RETRY_WAIT) in jobs
                    assert (users[2].vk_id, JobStatus.PENDING) in jobs
            finally:
                await outer.rollback()
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_subscription_control_ignores_active_user_post_campaign() -> None:
    engine = create_database_engine(database_url())
    settings = Settings(database_url=database_url())
    try:
        async with engine.connect() as connection:
            outer = await connection.begin()
            sessions = async_sessionmaker(
                connection,
                expire_on_commit=False,
                join_transaction_mode="create_savepoint",
            )
            try:
                now = datetime.now(UTC)
                async with sessions() as session:
                    session.add(
                        CollectionCampaign(
                            campaign_type="user_posts_enrichment",
                            status="waiting_method_limit",
                            phase="user_posts_collection",
                            snapshot_at=now,
                            snapshot_max_user_id=0,
                            snapshot_user_count=0,
                            configuration={},
                            configuration_hash=uuid.uuid4().hex,
                        )
                    )
                    await session.commit()

                result = await CampaignManager(sessions, settings).control_decision()
                assert result["campaigns"] == []
                assert result["decision"] == {
                    "action": "pilot_required",
                    "reason": "active campaign отсутствует",
                }
            finally:
                await outer.rollback()
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_metadata_batch_partial_deactivated_and_transient_are_safe() -> None:
    engine = create_database_engine(database_url())
    settings = Settings(database_url=database_url(), collection_community_metadata_batch_size=100)
    marker = int(uuid.uuid4().hex[:10], 16) + 60_000_000_000

    class PartialMetadataVK(FakeVK):
        async def get_groups(self, group_ids: list[int]) -> list[dict[str, Any]]:
            return [
                {"id": group_ids[0], "name": "Доступно"},
                {"id": group_ids[1], "name": "Удалено", "deactivated": "deleted"},
            ]

    class TransientMetadataVK(FakeVK):
        async def get_groups(self, group_ids: list[int]) -> list[dict[str, Any]]:
            raise VKRetryExhausted("metadata transport failure")

    try:
        async with engine.connect() as connection:
            outer = await connection.begin()
            sessions = async_sessionmaker(
                connection,
                expire_on_commit=False,
                join_transaction_mode="create_savepoint",
            )
            try:
                async with sessions() as session:
                    now = datetime.now(UTC)
                    session.add_all(
                        [
                            VKCommunity(vk_id=marker + index, first_seen_at=now, last_seen_at=now)
                            for index in range(5)
                        ]
                    )
                    deactivated_candidate = GroupCandidate(
                        id=marker + 10_000,
                        vk_id=marker + 1,
                        name="Удалено",
                        description="",
                        status_text="",
                        address=f"https://vk.com/{marker + 1}",
                        first_seen_at=now,
                        last_seen_at=now,
                        classification_status=ClassificationStatus.APPROVED,
                    )
                    session.add(deactivated_candidate)
                    partial_run = CollectionRun(
                        scope="subscription_metadata", status=CollectionRunStatus.PLANNED
                    )
                    session.add(partial_run)
                    await session.flush()
                    session.add_all(
                        [
                            CollectionJob(
                                collection_run_id=partial_run.id,
                                job_type="refresh_community_metadata",
                                entity_type="vk_community",
                                entity_id=marker + index,
                                priority=index,
                            )
                            for index in range(3)
                        ]
                    )
                    partial_run.total_jobs = 3
                    await session.commit()
                    partial_run_id = partial_run.id
                await CollectionWorker(sessions, PartialMetadataVK(), settings).run(
                    partial_run_id, max_jobs=1
                )  # type: ignore[arg-type]
                async with sessions() as session:
                    statuses = dict(
                        (
                            await session.execute(
                                select(CollectionJob.entity_id, CollectionJob.status).where(
                                    CollectionJob.collection_run_id == partial_run_id
                                )
                            )
                        ).all()
                    )
                    deleted = await session.get(VKCommunity, marker + 1)
                    missing = await session.get(VKCommunity, marker + 2)
                    deactivated_state = await session.get(GroupCollectionState, marker + 10_000)
                    assert statuses[marker] == JobStatus.COMPLETED
                    assert statuses[marker + 1] == JobStatus.COMPLETED
                    assert statuses[marker + 2] == JobStatus.RETRY_WAIT
                    assert deleted is not None and deleted.deactivated == "deleted"
                    assert deactivated_state is not None and deactivated_state.unavailable
                    assert missing is not None and missing.deactivated is None

                    transient_run = CollectionRun(
                        scope="subscription_metadata", status=CollectionRunStatus.PLANNED
                    )
                    session.add(transient_run)
                    await session.flush()
                    session.add_all(
                        [
                            CollectionJob(
                                collection_run_id=transient_run.id,
                                job_type="refresh_community_metadata",
                                entity_type="vk_community",
                                entity_id=marker + index,
                                priority=index,
                            )
                            for index in (3, 4)
                        ]
                    )
                    transient_run.total_jobs = 2
                    await session.commit()
                    transient_run_id = transient_run.id
                await CollectionWorker(sessions, TransientMetadataVK(), settings).run(
                    transient_run_id, max_jobs=1
                )  # type: ignore[arg-type]
                async with sessions() as session:
                    transient_statuses = set(
                        (
                            await session.scalars(
                                select(CollectionJob.status).where(
                                    CollectionJob.collection_run_id == transient_run_id
                                )
                            )
                        ).all()
                    )
                    assert transient_statuses == {JobStatus.PENDING, JobStatus.RETRY_WAIT}
                    transient_checkpoints = list(
                        (
                            await session.scalars(
                                select(CollectionJob.checkpoint).where(
                                    CollectionJob.collection_run_id == transient_run_id
                                )
                            )
                        ).all()
                    )
                    assert all(
                        "metadata_miss_count" not in value for value in transient_checkpoints
                    )
            finally:
                await outer.rollback()
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_food_service_incremental_plan_is_snapshot_safe_and_idempotent() -> None:
    if not database_url().rsplit("/", 1)[-1].endswith("_test"):
        pytest.skip("Incremental integration test требует изолированную test DB")
    engine = create_database_engine(database_url())
    marker = int(uuid.uuid4().hex[:10], 16)
    settings = Settings(
        database_url=database_url(),
        collection_posts_max_per_group=100,
        collection_members_max_per_group=200,
        collection_subscriptions_enabled=False,
    )
    try:
        async with engine.connect() as connection:
            outer = await connection.begin()
            sessions = async_sessionmaker(
                connection,
                expire_on_commit=False,
                autoflush=False,
                join_transaction_mode="create_savepoint",
            )
            try:
                async with sessions() as session:
                    old_group = GroupCandidate(
                        vk_id=marker + 20_000_000_000,
                        name="Старая approved-группа",
                        description="",
                        status_text="",
                        address="https://vk.com/old",
                        first_seen_at=datetime.now(UTC),
                        last_seen_at=datetime.now(UTC),
                        classification_status=ClassificationStatus.APPROVED,
                    )
                    new_group = GroupCandidate(
                        vk_id=marker + 30_000_000_000,
                        name="Новое кафе",
                        description="Действующее кафе",
                        status_text="",
                        address="https://vk.com/new",
                        first_seen_at=datetime.now(UTC),
                        last_seen_at=datetime.now(UTC),
                        classification_status=ClassificationStatus.APPROVED,
                    )
                    session.add_all([old_group, new_group])
                    await session.flush()
                    session.add_all(
                        [
                            GroupLabel(group_id=new_group.id, label="food_service"),
                            GroupLabel(group_id=new_group.id, label="food_delivery"),
                        ]
                    )
                    baseline = CollectionRun(
                        scope="full",
                        status=CollectionRunStatus.RUNNING,
                        configuration={
                            "group_count": 1,
                            "projected_database_bytes": 512 * 1024,
                        },
                    )
                    session.add(baseline)
                    await session.flush()
                    session.add(
                        CollectionJob(
                            collection_run_id=baseline.id,
                            job_type="refresh_group",
                            entity_type="group",
                            entity_id=old_group.id,
                        )
                    )
                    await session.commit()
                    baseline_id = baseline.id
                    new_group_id = new_group.id

                queue = CollectionQueue(sessions, settings)
                assert await queue.incremental_group_ids(baseline_id) == [new_group_id]
                preview = await queue.preview(incremental_from=baseline_id)
                assert preview.selected_groups == 1
                first = await queue.plan(
                    incremental_from=baseline_id,
                    reason="food_service_increment",
                    source="food_service_expansion",
                    capacity_passed=True,
                    estimated_disk_growth_bytes=preview.estimated_disk_growth_bytes,
                )
                second = await queue.plan(
                    incremental_from=baseline_id,
                    reason="food_service_increment",
                    source="food_service_expansion",
                    capacity_passed=True,
                    estimated_disk_growth_bytes=preview.estimated_disk_growth_bytes,
                )
                assert first == second
                async with sessions() as session:
                    jobs = list(
                        (
                            await session.scalars(
                                select(CollectionJob).where(
                                    CollectionJob.collection_run_id == first
                                )
                            )
                        ).all()
                    )
                    assert len(jobs) == 3
                    assert {job.entity_id for job in jobs} == {new_group_id}
                    run = await session.get(CollectionRun, first)
                    assert run is not None
                    assert run.scope == "incremental"
                    assert run.configuration["reason"] == "food_service_increment"
                    assert run.configuration["source"] == "food_service_expansion"
            finally:
                await outer.rollback()
    finally:
        await engine.dispose()

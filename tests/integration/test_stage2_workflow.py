from __future__ import annotations

import asyncio
import os
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import async_sessionmaker

from vk_collector.collection.queue import CollectionQueue
from vk_collector.collection.reporting import capacity_gate_passed, latest_runnable_run_id
from vk_collector.collection.worker import CollectionWorker
from vk_collector.config import Settings
from vk_collector.database.models import (
    ClassificationStatus,
    CollectionJob,
    CollectionRun,
    CollectionRunStatus,
    CommunityPostCollectionState,
    GroupCandidate,
    GroupLabel,
    GroupMembership,
    GroupPost,
    JobStatus,
    UserGroupSubscription,
    UserSubscriptionState,
    VKTokenMethodState,
    VKTokenState,
    VKUser,
)
from vk_collector.database.session import create_database_engine
from vk_collector.privacy import delete_user, inspect_user
from vk_collector.vk import TokenPool, VKAPIError, VKMethodUnavailable


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

    async def get_subscriptions_page(
        self, user_vk_id: int, offset: int, count: int
    ) -> dict[str, Any]:
        return {
            "count": 1,
            "items": [
                {
                    "id": 777,
                    "name": "Подписка",
                    "description": "Публичное описание",
                    "screen_name": "subscription_fixture",
                    "type": "group",
                    "is_closed": 0,
                }
            ]
            if offset == 0
            else [],
        }


class PrivateSubscriptionsVK(FakeVK):
    async def get_subscriptions_page(
        self, user_vk_id: int, offset: int, count: int
    ) -> dict[str, Any]:
        raise VKAPIError(260, "Доступ к подпискам ограничен")


class AccessDeniedSubscriptionsVK(FakeVK):
    async def get_subscriptions_page(
        self, user_vk_id: int, offset: int, count: int
    ) -> dict[str, Any]:
        raise VKAPIError(15, "Доступ запрещён")


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

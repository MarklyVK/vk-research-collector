from __future__ import annotations

import os
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import async_sessionmaker

from vk_collector.collection.queue import CollectionQueue
from vk_collector.collection.reporting import latest_runnable_run_id
from vk_collector.collection.worker import CollectionWorker
from vk_collector.config import Settings
from vk_collector.database.models import (
    ClassificationStatus,
    CollectionJob,
    CollectionRun,
    CollectionRunStatus,
    GroupCandidate,
    GroupMembership,
    GroupPost,
    JobStatus,
    UserGroupSubscription,
    VKUser,
)
from vk_collector.database.session import create_database_engine
from vk_collector.privacy import delete_user, inspect_user

pytestmark = pytest.mark.skipif(
    os.getenv("RUN_INTEGRATION_TESTS") != "1" and os.getenv("APP_ENV") != "test",
    reason="PostgreSQL integration test запускается только в CI/Docker",
)


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
        return {"count": 1, "items": [777] if offset == 0 else []}


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

import os
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from vk_collector.collection.worker import CollectionWorker
from vk_collector.config import Settings
from vk_collector.database.models import (
    CollectionJob,
    CollectionRun,
    JobStatus,
    UserPostCollectionState,
    UserSubscriptionState,
    VKUser,
)
from vk_collector.database.session import create_database_engine
from vk_collector.vk.models import VKSubscriptionIDsPage


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


class CountingVK:
    """Fake VK, считающий вызовы groups.get и wall.get."""

    def __init__(self) -> None:
        self.subscription_calls: list[int] = []
        self.wall_calls: list[int] = []

    async def get_subscription_ids_page(
        self, user_vk_id: int, offset: int, count: int
    ) -> VKSubscriptionIDsPage:
        self.subscription_calls.append(user_vk_id)
        return VKSubscriptionIDsPage(
            total_reported=1,
            group_ids=(777,),
            offset=offset,
            requested_count=count,
            returned_count=1,
            next_offset=offset + 1,
        )

    async def get_user_wall_page(self, user_vk_id: int, offset: int, count: int) -> dict[str, Any]:
        self.wall_calls.append(user_vk_id)
        return {"count": 0, "items": []}


async def _user(session: AsyncSession, vk_id: int, now: datetime) -> None:
    session.add(
        VKUser(
            vk_id=vk_id,
            is_closed=False,
            can_access_closed=False,
            first_seen_at=now,
            last_seen_at=now,
        )
    )


async def _run_jobs(
    sessions: async_sessionmaker[AsyncSession],
    settings: Settings,
    jobs: list[tuple[str, int, dict[str, object]]],
) -> tuple[CountingVK, dict[int, CollectionJob]]:
    async with sessions() as session:
        run = CollectionRun(scope="user_posts_pilot", total_jobs=len(jobs))
        session.add(run)
        await session.flush()
        created = [
            CollectionJob(
                collection_run_id=run.id,
                job_type=job_type,
                entity_type="user",
                entity_id=entity_id,
                checkpoint=checkpoint,
            )
            for job_type, entity_id, checkpoint in jobs
        ]
        session.add_all(created)
        await session.commit()
        run_id = run.id
        job_ids = {job.entity_id: job.id for job in created}
    client = CountingVK()
    await CollectionWorker(sessions, client, settings).run(run_id)  # type: ignore[arg-type]
    result: dict[int, CollectionJob] = {}
    async with sessions() as session:
        for entity_id, job_id in job_ids.items():
            job = await session.get(CollectionJob, job_id)
            assert job is not None
            result[entity_id] = job
    return client, result


@pytest.mark.asyncio
@pytest.mark.parametrize("skip_days", [30, 0])
async def test_fresh_users_are_skipped_without_vk_request(skip_days: int) -> None:
    engine = create_database_engine(database_url())
    settings = Settings(
        database_url=database_url(),
        collection_export_dir=".",
        collection_max_concurrency=1,
        collection_skip_fresh_days=skip_days,
    )
    marker = int(uuid.uuid4().hex[:10], 16)
    now = datetime.now(UTC)
    fresh_subs, stale_subs, private_subs, recent_subs = (marker + i for i in range(4))
    fresh_posts, stale_posts, private_wall = (marker + i for i in range(4, 7))
    try:
        async with engine.connect() as connection:
            outer = await connection.begin()
            sessions = async_sessionmaker(
                connection, expire_on_commit=False, join_transaction_mode="create_savepoint"
            )
            try:
                async with sessions() as session:
                    for vk_id in range(marker, marker + 7):
                        await _user(session, vk_id, now)
                    await session.flush()
                    session.add_all(
                        [
                            UserSubscriptionState(
                                user_id=fresh_subs,
                                last_attempt_at=now - timedelta(days=5),
                                last_success_at=now - timedelta(days=5),
                                collected_limit=50,
                            ),
                            UserSubscriptionState(
                                user_id=stale_subs,
                                last_attempt_at=now - timedelta(days=40),
                                last_success_at=now - timedelta(days=40),
                                collected_limit=50,
                            ),
                            UserSubscriptionState(
                                user_id=private_subs,
                                last_attempt_at=now - timedelta(days=5),
                                collected_limit=50,
                                privacy_denied=True,
                                terminal_reason="privacy_or_access",
                                last_error_code=30,
                            ),
                            UserSubscriptionState(
                                user_id=recent_subs,
                                last_attempt_at=now - timedelta(days=1),
                                last_success_at=now - timedelta(days=1),
                                collected_limit=50,
                            ),
                            UserPostCollectionState(
                                user_id=fresh_posts,
                                last_attempt_at=now - timedelta(days=5),
                                last_success_at=now - timedelta(days=5),
                            ),
                            UserPostCollectionState(
                                user_id=stale_posts,
                                last_attempt_at=now - timedelta(days=40),
                                last_success_at=now - timedelta(days=40),
                            ),
                            UserPostCollectionState(
                                user_id=private_wall,
                                last_attempt_at=now - timedelta(days=5),
                                wall_private=True,
                                last_error_code=30,
                            ),
                        ]
                    )
                    await session.commit()

                client, jobs = await _run_jobs(
                    sessions,
                    settings,
                    [
                        ("collect_user_subscriptions", fresh_subs, {}),
                        ("collect_user_subscriptions", stale_subs, {}),
                        ("collect_user_subscriptions", private_subs, {}),
                        ("collect_user_subscriptions", recent_subs, {}),
                        ("collect_user_posts", fresh_posts, {}),
                        ("collect_user_posts", stale_posts, {}),
                        ("collect_user_posts", private_wall, {}),
                    ],
                )

                if skip_days == 0:
                    assert sorted(client.subscription_calls) == [
                        fresh_subs,
                        stale_subs,
                        private_subs,
                        recent_subs,
                    ]
                    assert sorted(client.wall_calls) == [fresh_posts, stale_posts, private_wall]
                    assert all(job.status == JobStatus.COMPLETED for job in jobs.values())
                    return

                assert sorted(client.subscription_calls) == [stale_subs]
                assert client.wall_calls == [stale_posts]
                for skipped in (fresh_subs, private_subs, recent_subs, fresh_posts, private_wall):
                    assert jobs[skipped].status == JobStatus.SKIPPED
                    assert jobs[skipped].last_error_type == "fresh_recently_collected"
                    assert jobs[skipped].api_requests == 0
                assert jobs[stale_subs].status == JobStatus.COMPLETED
                assert jobs[stale_posts].status == JobStatus.COMPLETED
                async with sessions() as session:
                    # Пропуск не трогает canonical state пользователя.
                    state = await session.get(UserSubscriptionState, fresh_subs)
                    assert state is not None and state.last_run_id is None
            finally:
                await outer.rollback()
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_resumed_job_with_checkpoint_offset_is_not_skipped() -> None:
    engine = create_database_engine(database_url())
    settings = Settings(
        database_url=database_url(), collection_export_dir=".", collection_max_concurrency=1
    )
    marker = int(uuid.uuid4().hex[:10], 16)
    now = datetime.now(UTC)
    try:
        async with engine.connect() as connection:
            outer = await connection.begin()
            sessions = async_sessionmaker(
                connection, expire_on_commit=False, join_transaction_mode="create_savepoint"
            )
            try:
                async with sessions() as session:
                    await _user(session, marker, now)
                    await session.flush()
                    session.add(
                        UserPostCollectionState(
                            user_id=marker,
                            last_attempt_at=now - timedelta(days=1),
                            last_success_at=now - timedelta(days=1),
                        )
                    )
                    await session.commit()
                client, jobs = await _run_jobs(
                    sessions,
                    settings,
                    [("collect_user_posts", marker, {"offset": 10, "collected": 10})],
                )
                assert client.wall_calls == [marker]
                assert jobs[marker].status == JobStatus.COMPLETED
            finally:
                await outer.rollback()
    finally:
        await engine.dispose()

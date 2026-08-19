import os
import uuid
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import async_sessionmaker

from vk_collector.collection.pilots import quarantine_incompatible_pilots
from vk_collector.collection.queue import CollectionQueue
from vk_collector.collection.reporting import runnable_run_ids
from vk_collector.collection.user_posts_campaigns import UserPostCampaignManager
from vk_collector.collection.worker import CollectionWorker
from vk_collector.config import Settings
from vk_collector.database.models import (
    CampaignPhase,
    ClassificationStatus,
    CollectionCampaign,
    CollectionCampaignUser,
    CollectionJob,
    CollectionRun,
    CollectionRunStatus,
    GroupCandidate,
    GroupMembership,
    JobStatus,
    UserPost,
    UserPostAttachment,
    UserPostCollectionState,
    VKTokenState,
    VKUser,
)
from vk_collector.database.session import create_database_engine
from vk_collector.vk import token_fingerprint
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


@pytest.mark.asyncio
async def test_user_posts_campaign_is_direct_idempotent_and_immutable() -> None:
    engine = create_database_engine(database_url())
    settings = Settings(
        database_url=database_url(),
        collection_export_dir=".",
        collection_campaign_cohort_users=10,
    )
    marker = int(uuid.uuid4().hex[:10], 16)
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
                        is_closed=True,
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

                manager = UserPostCampaignManager(sessions, settings)
                with pytest.raises(ValueError, match="capacity gate"):
                    await manager.plan(gate_evidence={"decision": "rejected"})
                async with sessions() as session:
                    assert (
                        int(await session.scalar(select(func.count(CollectionCampaign.id))) or 0)
                        == 0
                    )

                preview = await manager.plan_preview()
                assert preview["snapshot_users"] == 1
                assert preview["due_users"] == 1
                gate = {
                    **preview,
                    "decision": "passed",
                    "current_database_bytes": 1,
                    "aggregate_projected_growth_bytes": 1024,
                    "projected_final_database_bytes": 2048,
                    "current_disk_free_bytes": 10 * 1024**3,
                    "safe_database_limit_bytes": 7 * 1024**3,
                }
                campaign_id = await manager.plan(gate_evidence=gate)
                assert await manager.plan(gate_evidence=gate) == campaign_id
                async with sessions() as session:
                    campaign = await session.get(CollectionCampaign, campaign_id)
                    assert campaign is not None
                    assert campaign.phase == CampaignPhase.USER_POSTS_COLLECTION.value
                    assert campaign.snapshot_user_count == 1
                    run = await session.scalar(
                        select(CollectionRun).where(CollectionRun.campaign_id == campaign_id)
                    )
                    assert run is not None and run.scope == "user_posts"
                    job_types = set(
                        (
                            await session.scalars(
                                select(CollectionJob.job_type).where(
                                    CollectionJob.collection_run_id == run.id
                                )
                            )
                        ).all()
                    )
                    assert job_types == {"collect_user_posts"}

                    late_user = VKUser(
                        vk_id=marker + 1,
                        is_closed=False,
                        can_access_closed=False,
                        first_seen_at=datetime.now(UTC),
                        last_seen_at=datetime.now(UTC),
                    )
                    session.add(late_user)
                    await session.flush()
                    session.add(
                        GroupMembership(
                            group_id=marker,
                            user_id=late_user.vk_id,
                            first_seen_at=datetime.now(UTC),
                            last_seen_at=datetime.now(UTC),
                            is_current=True,
                        )
                    )
                    await session.commit()
                    snapshot_count = int(
                        await session.scalar(
                            select(func.count(CollectionCampaignUser.user_id)).where(
                                CollectionCampaignUser.campaign_id == campaign_id
                            )
                        )
                        or 0
                    )
                    assert snapshot_count == 1
            finally:
                await outer.rollback()
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_paused_no_tokens_resumes_only_with_usable_fingerprint() -> None:
    engine = create_database_engine(database_url())
    settings = Settings(database_url=database_url(), collection_export_dir=".")
    try:
        async with engine.connect() as connection:
            outer = await connection.begin()
            sessions = async_sessionmaker(
                connection, expire_on_commit=False, join_transaction_mode="create_savepoint"
            )
            try:
                disabled = token_fingerprint(f"disabled-{uuid.uuid4()}")
                usable = token_fingerprint(f"usable-{uuid.uuid4()}")
                async with sessions() as session:
                    session.add_all(
                        [
                            VKTokenState(token_fingerprint=disabled, disabled=True),
                            VKTokenState(token_fingerprint=usable, disabled=False),
                        ]
                    )
                    run = CollectionRun(
                        scope="user_posts",
                        status=CollectionRunStatus.PAUSED_NO_TOKENS,
                        configuration={"capacity_gate": "passed"},
                        total_jobs=1,
                    )
                    session.add(run)
                    await session.flush()
                    job = CollectionJob(
                        collection_run_id=run.id,
                        job_type="collect_user_posts",
                        entity_type="user",
                        entity_id=int(uuid.uuid4().hex[:10], 16),
                        status=JobStatus.PAUSED,
                        last_error_type="tokens_unavailable",
                    )
                    session.add(job)
                    await session.commit()
                    run_id = run.id
                    job_id = job.id
                queue = CollectionQueue(sessions, settings)
                assert await queue.resume_paused_no_tokens({disabled}) == 0
                assert await queue.resume_paused_no_tokens({disabled, usable}) == 1
                async with sessions() as session:
                    run = await session.get(CollectionRun, run_id)
                    job = await session.get(CollectionJob, job_id)
                    disabled_row = await session.get(VKTokenState, disabled)
                    assert run is not None and run.status == CollectionRunStatus.RUNNING
                    assert job is not None and job.status == JobStatus.PENDING
                    assert disabled_row is not None and disabled_row.disabled is True
            finally:
                await outer.rollback()
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_live_capacity_recheck_stops_next_user_post_cohort() -> None:
    engine = create_database_engine(database_url())
    settings = Settings(
        database_url=database_url(),
        collection_export_dir=".",
        collection_campaign_cohort_users=1,
    )
    marker = int(uuid.uuid4().hex[:10], 16)
    try:
        async with engine.connect() as connection:
            outer = await connection.begin()
            sessions = async_sessionmaker(
                connection, expire_on_commit=False, join_transaction_mode="create_savepoint"
            )
            try:
                now = datetime.now(UTC)
                async with sessions() as session:
                    session.add_all(
                        [
                            VKUser(
                                vk_id=marker,
                                is_closed=False,
                                can_access_closed=False,
                                first_seen_at=now,
                                last_seen_at=now,
                            ),
                            VKUser(
                                vk_id=marker + 1,
                                is_closed=False,
                                can_access_closed=False,
                                first_seen_at=now,
                                last_seen_at=now,
                            ),
                        ]
                    )
                    campaign = CollectionCampaign(
                        campaign_type="user_posts_enrichment",
                        status="running",
                        phase=CampaignPhase.USER_POSTS_COLLECTION.value,
                        snapshot_at=now,
                        snapshot_max_user_id=marker + 1,
                        snapshot_user_count=2,
                        last_planned_user_id=marker,
                        configuration={
                            "current_database_bytes": 0,
                            "projected_final_database_bytes": 0,
                            "due_users": 2,
                            "aggregate_projected_growth_bytes": 0,
                            "safe_database_limit_bytes": 7 * 1024**3,
                            "collection": CollectionQueue(
                                sessions, settings
                            ).collection_configuration(),
                        },
                        configuration_hash=uuid.uuid4().hex,
                    )
                    session.add(campaign)
                    await session.flush()
                    session.add_all(
                        [
                            CollectionCampaignUser(campaign_id=campaign.id, user_id=marker),
                            CollectionCampaignUser(campaign_id=campaign.id, user_id=marker + 1),
                        ]
                    )
                    await session.commit()
                    campaign_id = campaign.id

                await UserPostCampaignManager(sessions, settings).reconcile(campaign_id)
                async with sessions() as session:
                    campaign = await session.get(CollectionCampaign, campaign_id)
                    assert campaign is not None
                    assert campaign.status == "paused_capacity_limit"
                    assert "отклонён" in (campaign.error_message or "")
                    assert (
                        int(
                            await session.scalar(
                                select(func.count(CollectionRun.id)).where(
                                    CollectionRun.campaign_id == campaign_id
                                )
                            )
                            or 0
                        )
                        == 0
                    )
            finally:
                await outer.rollback()
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_method_waiting_run_does_not_hide_other_runnable_run() -> None:
    engine = create_database_engine(database_url())
    try:
        async with engine.connect() as connection:
            outer = await connection.begin()
            sessions = async_sessionmaker(
                connection, expire_on_commit=False, join_transaction_mode="create_savepoint"
            )
            try:
                async with sessions() as session:
                    blocked = CollectionRun(
                        scope="subscriptions",
                        status=CollectionRunStatus.WAITING_METHOD_LIMIT,
                        configuration={"capacity_gate": "passed"},
                        next_wakeup_at=datetime.now(UTC) + timedelta(hours=1),
                    )
                    runnable = CollectionRun(
                        scope="user_posts",
                        status=CollectionRunStatus.RUNNING,
                        configuration={"capacity_gate": "passed"},
                    )
                    session.add_all([blocked, runnable])
                    await session.commit()
                    blocked_id, runnable_id = blocked.id, runnable.id
                ids = await runnable_run_ids(sessions)
                assert blocked_id in ids
                assert runnable_id in ids
            finally:
                await outer.rollback()
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_legacy_quarantine_preserves_job_checkpoint_and_collected_data() -> None:
    engine = create_database_engine(database_url())
    settings = Settings(database_url=database_url(), collection_export_dir=".")
    marker = int(uuid.uuid4().hex[:10], 16)
    try:
        async with engine.connect() as connection:
            outer = await connection.begin()
            sessions = async_sessionmaker(
                connection, expire_on_commit=False, join_transaction_mode="create_savepoint"
            )
            try:
                now = datetime.now(UTC)
                async with sessions() as session:
                    user = VKUser(
                        vk_id=marker,
                        is_closed=False,
                        can_access_closed=False,
                        first_seen_at=now,
                        last_seen_at=now,
                    )
                    run = CollectionRun(
                        scope="user_posts_pilot",
                        status=CollectionRunStatus.RUNNING,
                        configuration={"collection": {"legacy": True}},
                        total_jobs=1,
                    )
                    session.add_all([user, run])
                    await session.flush()
                    post = UserPost(
                        vk_owner_id=marker,
                        vk_post_id=1,
                        user_id=marker,
                        published_at=now,
                        text="собранные данные",
                        post_type="post",
                        is_pinned=False,
                        first_seen_at=now,
                        last_seen_at=now,
                        content_hash="a" * 64,
                    )
                    job = CollectionJob(
                        collection_run_id=run.id,
                        job_type="collect_user_posts",
                        entity_type="user",
                        entity_id=marker,
                        checkpoint={"offset": 7, "collected": 7},
                    )
                    session.add_all([post, job])
                    await session.commit()
                    run_id, job_id, post_id = run.id, job.id, post.id

                result = await quarantine_incompatible_pilots(
                    sessions,
                    settings,
                    confirmation="QUARANTINE_INCOMPATIBLE_PILOTS",
                )
                assert result["quarantined_count"] == 1
                async with sessions() as session:
                    run = await session.get(CollectionRun, run_id)
                    job = await session.get(CollectionJob, job_id)
                    assert run is not None and run.status == CollectionRunStatus.CANCELLED
                    assert job is not None and job.status == JobStatus.CANCELLED
                    assert job.checkpoint == {"offset": 7, "collected": 7}
                    assert await session.get(UserPost, post_id) is not None
            finally:
                await outer.rollback()
    finally:
        await engine.dispose()


class WallVK:
    def __init__(self, items: list[dict[str, object]], *, fail_second_once: bool = False) -> None:
        self.items = items
        self.fail_second_once = fail_second_once
        self.calls: list[tuple[int, int]] = []

    async def get_user_wall_page(self, _user_id: int, offset: int, count: int) -> dict[str, object]:
        self.calls.append((offset, count))
        if self.fail_second_once and offset > 0:
            self.fail_second_once = False
            raise VKRetryExhausted("временная ошибка")
        return {"count": len(self.items), "items": self.items[offset : offset + count]}


@pytest.mark.asyncio
async def test_user_wall_checkpoint_cutoff_zero_and_idempotency() -> None:
    engine = create_database_engine(database_url())
    settings = Settings(
        database_url=database_url(),
        collection_export_dir=".",
        collection_user_posts_page_size=10,
        collection_user_posts_max_per_user=20,
        collection_user_posts_window_days=180,
    )
    marker = int(uuid.uuid4().hex[:10], 16)
    now = datetime.now(UTC)
    recent = [
        {
            "id": index + 1,
            "owner_id": marker,
            "date": int((now - timedelta(days=index)).timestamp()),
            "text": f"post-{index}",
            "attachments": [{"type": "photo", "photo": {"id": index + 1, "owner_id": marker}}],
        }
        for index in range(20)
    ]
    old = {
        "id": 999,
        "owner_id": marker,
        "date": int((now - timedelta(days=181)).timestamp()),
        "text": "old",
    }
    try:
        async with engine.connect() as connection:
            outer = await connection.begin()
            sessions = async_sessionmaker(
                connection, expire_on_commit=False, join_transaction_mode="rollback_only"
            )
            try:
                async with sessions() as session:
                    session.add_all(
                        [
                            VKUser(
                                vk_id=marker,
                                is_closed=False,
                                can_access_closed=False,
                                first_seen_at=now,
                                last_seen_at=now,
                            ),
                            VKUser(
                                vk_id=marker + 1,
                                is_closed=False,
                                can_access_closed=False,
                                first_seen_at=now,
                                last_seen_at=now,
                            ),
                            VKUser(
                                vk_id=marker + 2,
                                is_closed=False,
                                can_access_closed=False,
                                first_seen_at=now,
                                last_seen_at=now,
                            ),
                            VKUser(
                                vk_id=marker + 3,
                                is_closed=True,
                                can_access_closed=False,
                                first_seen_at=now,
                                last_seen_at=now,
                            ),
                            VKUser(
                                vk_id=marker + 4,
                                is_closed=False,
                                can_access_closed=False,
                                deactivated="deleted",
                                first_seen_at=now,
                                last_seen_at=now,
                            ),
                        ]
                    )
                    run = CollectionRun(scope="user_posts_pilot", total_jobs=1)
                    session.add(run)
                    await session.flush()
                    job = CollectionJob(
                        collection_run_id=run.id,
                        job_type="collect_user_posts",
                        entity_type="user",
                        entity_id=marker,
                    )
                    session.add(job)
                    await session.commit()
                    run_id = run.id
                    job_id = job.id
                client = WallVK([*recent, old], fail_second_once=True)
                worker = CollectionWorker(sessions, client, settings)  # type: ignore[arg-type]
                await worker.run(run_id, max_jobs=1)
                async with sessions() as session:
                    job = await session.get(CollectionJob, job_id)
                    assert job is not None and job.status == JobStatus.RETRY_WAIT, (
                        job.last_error_message if job is not None else "job missing"
                    )
                    assert "offset" in job.checkpoint, job.last_error_message
                    assert job.checkpoint["offset"] == 10
                    assert job.checkpoint["collected"] == 10
                    await session.execute(
                        update(CollectionJob)
                        .where(CollectionJob.id == job_id)
                        .values(next_attempt_at=datetime.now(UTC) - timedelta(seconds=1))
                    )
                    await session.commit()
                await worker.run(run_id, max_jobs=1)
                async with sessions() as session:
                    assert int(await session.scalar(select(func.count(UserPost.id))) or 0) == 20
                    assert (
                        int(await session.scalar(select(func.count(UserPostAttachment.id))) or 0)
                        == 20
                    )
                    state = await session.get(UserPostCollectionState, marker)
                    assert state is not None and state.collected_count == 20
                    assert state.last_success_at is not None

                    repeat = CollectionRun(scope="user_posts_pilot", total_jobs=1)
                    session.add(repeat)
                    await session.flush()
                    session.add(
                        CollectionJob(
                            collection_run_id=repeat.id,
                            job_type="collect_user_posts",
                            entity_type="user",
                            entity_id=marker,
                        )
                    )
                    await session.commit()
                    repeat_id = repeat.id
                repeat_client = WallVK([*recent, old])
                await CollectionWorker(  # type: ignore[arg-type]
                    sessions, repeat_client, settings
                ).run(repeat_id)
                async with sessions() as session:
                    assert int(await session.scalar(select(func.count(UserPost.id))) or 0) == 20
                    zero_run = CollectionRun(scope="user_posts_pilot", total_jobs=1)
                    session.add(zero_run)
                    await session.flush()
                    session.add(
                        CollectionJob(
                            collection_run_id=zero_run.id,
                            job_type="collect_user_posts",
                            entity_type="user",
                            entity_id=marker + 1,
                        )
                    )
                    await session.commit()
                    zero_run_id = zero_run.id
                await CollectionWorker(  # type: ignore[arg-type]
                    sessions, WallVK([]), settings
                ).run(zero_run_id)
                async with sessions() as session:
                    zero = await session.get(UserPostCollectionState, marker + 1)
                    assert zero is not None and zero.collected_count == 0
                    assert zero.last_success_at is not None
                    cutoff_run = CollectionRun(scope="user_posts_pilot", total_jobs=1)
                    session.add(cutoff_run)
                    await session.flush()
                    session.add(
                        CollectionJob(
                            collection_run_id=cutoff_run.id,
                            job_type="collect_user_posts",
                            entity_type="user",
                            entity_id=marker + 2,
                        )
                    )
                    await session.commit()
                    cutoff_run_id = cutoff_run.id
                cutoff_items: list[dict[str, object]] = [
                    {
                        "id": 100 + index,
                        "owner_id": marker + 2,
                        "date": int((now - timedelta(days=index)).timestamp()),
                        "text": f"cutoff-{index}",
                    }
                    for index in range(5)
                ]
                cutoff_items.append(
                    {
                        "id": 199,
                        "owner_id": marker + 2,
                        "date": int((now - timedelta(days=181)).timestamp()),
                        "text": "too-old",
                    }
                )
                await CollectionWorker(  # type: ignore[arg-type]
                    sessions, WallVK(cutoff_items), settings
                ).run(cutoff_run_id)
                async with sessions() as session:
                    cutoff_state = await session.get(UserPostCollectionState, marker + 2)
                    assert cutoff_state is not None and cutoff_state.collected_count == 5
                    assert int(await session.scalar(select(func.count(UserPost.id))) or 0) == 25
                    terminal_run = CollectionRun(scope="user_posts_pilot", total_jobs=2)
                    session.add(terminal_run)
                    await session.flush()
                    session.add_all(
                        [
                            CollectionJob(
                                collection_run_id=terminal_run.id,
                                job_type="collect_user_posts",
                                entity_type="user",
                                entity_id=marker + 3,
                            ),
                            CollectionJob(
                                collection_run_id=terminal_run.id,
                                job_type="collect_user_posts",
                                entity_type="user",
                                entity_id=marker + 4,
                            ),
                        ]
                    )
                    await session.commit()
                    terminal_run_id = terminal_run.id
                await CollectionWorker(  # type: ignore[arg-type]
                    sessions, WallVK([]), settings
                ).run(terminal_run_id)
                async with sessions() as session:
                    private = await session.get(UserPostCollectionState, marker + 3)
                    unavailable = await session.get(UserPostCollectionState, marker + 4)
                    assert private is not None and private.wall_private is True
                    assert unavailable is not None and unavailable.unavailable is True
            finally:
                await outer.rollback()
    finally:
        await engine.dispose()

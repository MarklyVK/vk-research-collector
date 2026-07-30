from __future__ import annotations

import json
import os
import uuid
from datetime import UTC, datetime
from pathlib import Path

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker

from vk_collector.classification.reclassification import (
    import_reclassification,
    prepare_reclassification,
)
from vk_collector.classification.service import export_batch, import_classification
from vk_collector.database.models import (
    ClassificationStatus,
    GroupCandidate,
    GroupKeywordMatch,
    GroupLabel,
    SearchKeyword,
    SearchRun,
)
from vk_collector.database.session import create_database_engine
from vk_collector.search.postgres import PostgresSearchPersistence, search_run_summary
from vk_collector.search.service import Keyword
from vk_collector.vk import VKGroup, VKSearchPage

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


@pytest.mark.asyncio
async def test_export_and_minimal_import_are_transactional(tmp_path: Path) -> None:
    engine = create_database_engine(database_url())
    marker = uuid.uuid4().hex
    vk_ids = (int(marker[:8], 16) + 1_000_000_000, int(marker[8:16], 16) + 2_000_000_000)
    now = datetime.now(UTC)
    try:
        async with engine.connect() as connection:
            transaction = await connection.begin()
            sessions = async_sessionmaker(
                connection,
                expire_on_commit=False,
                autoflush=False,
                join_transaction_mode="create_savepoint",
            )
            try:
                await _exercise_classification_workflow(sessions, tmp_path, marker, vk_ids, now)
            finally:
                await transaction.rollback()
    finally:
        await engine.dispose()


async def _exercise_classification_workflow(
    sessions: async_sessionmaker,
    tmp_path: Path,
    marker: str,
    vk_ids: tuple[int, int],
    now: datetime,
) -> None:
    async with sessions() as session:
        run = SearchRun()
        keyword = SearchKeyword(subject="food_delivery", keyword=f"доставка-{marker}")
        food_service_keyword = SearchKeyword(subject="food_service", keyword=f"кафе-{marker}")
        session.add_all([run, keyword, food_service_keyword])
        await session.flush()
        groups = [
            GroupCandidate(
                vk_id=vk_id,
                name=f"Группа {vk_id}",
                description="Описание",
                status_text="Статус",
                screen_name=f"test_{vk_id}",
                address=f"https://vk.com/test_{vk_id}",
                first_seen_at=now,
                last_seen_at=now,
            )
            for vk_id in vk_ids
        ]
        session.add_all(groups)
        await session.flush()
        session.add_all(
            GroupKeywordMatch(
                group_id=group.id,
                keyword_id=keyword.id,
                first_search_run_id=run.id,
            )
            for group in groups
        )
        session.add(
            GroupKeywordMatch(
                group_id=groups[0].id,
                keyword_id=food_service_keyword.id,
                first_search_run_id=run.id,
            )
        )
        await session.commit()

    async with sessions() as session:
        exported = await export_batch(session, tmp_path, 2)
        assert exported is not None
        payload = json.loads(exported.read_text(encoding="utf-8"))
        assert {item["vk_id"] for item in payload["groups"]} == set(vk_ids)

    invalid = tmp_path / "invalid.json"
    invalid.write_text(
        json.dumps({"batch_id": payload["batch_id"], "approved_group_ids": [999]}),
        encoding="utf-8",
    )
    async with sessions() as session:
        with pytest.raises(ValueError, match="не содержит"):
            await import_classification(session, invalid)
        await session.rollback()
        statuses = list(
            (
                await session.scalars(
                    select(GroupCandidate.classification_status).where(
                        GroupCandidate.vk_id.in_(vk_ids)
                    )
                )
            ).all()
        )
        assert statuses == [ClassificationStatus.PENDING] * 2

    valid = tmp_path / "valid.json"
    valid.write_text(
        json.dumps({"batch_id": payload["batch_id"], "approved_group_ids": [vk_ids[0]]}),
        encoding="utf-8",
    )
    async with sessions() as session:
        assert await import_classification(session, valid) == 2
        status_rows = (
            await session.execute(
                select(GroupCandidate.vk_id, GroupCandidate.classification_status).where(
                    GroupCandidate.vk_id.in_(vk_ids)
                )
            )
        ).all()
        assert dict(status_rows) == {
            vk_ids[0]: ClassificationStatus.APPROVED,
            vk_ids[1]: ClassificationStatus.REJECTED,
        }
        labels = list((await session.scalars(select(GroupLabel.label))).all())
        assert "food_delivery" in labels
        assert "food_service" in labels

    async with sessions() as session:
        assert await import_classification(session, valid) == 0

    if not database_url().rsplit("/", 1)[-1].endswith("_test"):
        return

    reclassification_dir = tmp_path / "food-service-reclassification"
    async with sessions() as session:
        progress = await prepare_reclassification(session, reclassification_dir)
        assert progress["total"] == 2
    decisions_path = reclassification_dir / "decisions.json"
    decisions = json.loads(decisions_path.read_text(encoding="utf-8"))
    for item in decisions["decisions"]:
        item["food_service"] = True
        item["final_approved"] = True
        item["final_labels"] = sorted({*item["previous_labels"], "food_service"})
        item["confidence"] = 0.95
        item["reason"] = "Описание независимо подтверждает действующее заведение общепита"
    decisions_path.write_text(json.dumps(decisions, ensure_ascii=False), encoding="utf-8")
    async with sessions() as session:
        result = await import_reclassification(session, decisions_path)
        assert result["processed"] == 2
        assert result["rejected_to_approved"] == 1
    async with sessions() as session:
        assert (await import_reclassification(session, decisions_path))["processed"] == 0


@pytest.mark.asyncio
async def test_food_service_search_deduplicates_known_group_and_keeps_new_pending() -> None:
    if not database_url().rsplit("/", 1)[-1].endswith("_test"):
        pytest.skip("Search integration test требует изолированную test DB")
    engine = create_database_engine(database_url())
    marker = int(uuid.uuid4().hex[:10], 16)
    now = datetime.now(UTC)
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
                known_vk_id = marker + 40_000_000_000
                new_vk_id = marker + 50_000_000_000
                async with sessions() as session:
                    session.add(
                        GroupCandidate(
                            vk_id=known_vk_id,
                            name="Известное кафе",
                            description="Кафе с залом",
                            status_text="",
                            address="https://vk.com/known",
                            first_seen_at=now,
                            last_seen_at=now,
                            classification_status=ClassificationStatus.APPROVED,
                        )
                    )
                    await session.commit()

                persistence = PostgresSearchPersistence(sessions)
                keyword = Keyword("кафе", "food_service")
                run_id = await persistence.start_or_resume_run((keyword,))
                await persistence.save_page(
                    run_id,
                    keyword,
                    "group",
                    VKSearchPage(
                        total=3,
                        items=(
                            VKGroup(
                                known_vk_id,
                                "Известное кафе",
                                "Кафе с залом",
                                "",
                                "known",
                                "https://vk.com/known",
                            ),
                            VKGroup(
                                new_vk_id,
                                "Новое кафе",
                                "Действующее кафе",
                                "",
                                "new",
                                "https://vk.com/new",
                            ),
                        ),
                        raw_count=3,
                        private_count=1,
                    ),
                    3,
                )
                await persistence.mark_keyword_complete(run_id, keyword, "group")
                await persistence.mark_run_complete(run_id)
                async with sessions() as session:
                    rows = (
                        await session.execute(
                            select(
                                GroupCandidate.vk_id,
                                GroupCandidate.classification_status,
                            ).where(GroupCandidate.vk_id.in_([known_vk_id, new_vk_id]))
                        )
                    ).all()
                    statuses = dict(rows)
                    assert statuses[known_vk_id] == ClassificationStatus.APPROVED
                    assert statuses[new_vk_id] == ClassificationStatus.PENDING
                    summary = await search_run_summary(session, uuid.UUID(run_id))
                    assert summary["unique_vk_groups"] == 2
                    assert summary["already_known_groups"] == 1
                    assert summary["new_groups"] == 1
                    assert summary["private_results"] == 1
            finally:
                await outer.rollback()
    finally:
        await engine.dispose()

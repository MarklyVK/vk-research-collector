from __future__ import annotations

import json
import os
import uuid
from datetime import UTC, datetime
from pathlib import Path

import pytest
from sqlalchemy import select

from vk_collector.classification.service import export_batch, import_classification
from vk_collector.database.models import (
    ClassificationStatus,
    GroupCandidate,
    GroupKeywordMatch,
    GroupLabel,
    SearchKeyword,
    SearchRun,
)
from vk_collector.database.session import create_database_engine, create_session_factory

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
    sessions = create_session_factory(engine)
    marker = uuid.uuid4().hex
    vk_ids = (int(marker[:8], 16) + 1_000_000_000, int(marker[8:16], 16) + 2_000_000_000)
    now = datetime.now(UTC)
    try:
        async with sessions() as session:
            run = SearchRun()
            keyword = SearchKeyword(subject="food_delivery", keyword=f"доставка-{marker}")
            session.add_all([run, keyword])
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
    finally:
        await engine.dispose()

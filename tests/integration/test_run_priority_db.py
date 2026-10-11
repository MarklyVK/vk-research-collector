import os
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker

from vk_collector.collection.reporting import (
    SCHEDULER_PRIORITY_KEY,
    high_priority_run_ids,
    scheduler_priority,
)
from vk_collector.database.models import CollectionRun
from vk_collector.database.session import create_database_engine


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
async def test_only_runs_with_active_priority_are_selected() -> None:
    engine = create_database_engine(database_url())
    now = datetime.now(UTC)
    try:
        async with engine.connect() as connection:
            outer = await connection.begin()
            sessions = async_sessionmaker(
                connection, expire_on_commit=False, join_transaction_mode="create_savepoint"
            )
            try:
                async with sessions() as session:
                    plain = CollectionRun(scope="full", configuration={"capacity_gate": "passed"})
                    active = CollectionRun(
                        scope="full",
                        configuration={
                            "capacity_gate": "passed",
                            SCHEDULER_PRIORITY_KEY: scheduler_priority(24, now=now),
                        },
                    )
                    expired = CollectionRun(
                        scope="full",
                        configuration={
                            "capacity_gate": "passed",
                            SCHEDULER_PRIORITY_KEY: scheduler_priority(
                                1, now=now - timedelta(hours=2)
                            ),
                        },
                    )
                    outside = CollectionRun(
                        scope="full",
                        configuration={
                            "capacity_gate": "passed",
                            SCHEDULER_PRIORITY_KEY: scheduler_priority(24, now=now),
                        },
                    )
                    session.add_all([plain, active, expired, outside])
                    await session.commit()
                    targets = [plain.id, expired.id, active.id]
                    active_id = active.id
                assert await high_priority_run_ids(sessions, targets, now=now) == [active_id]
                assert await high_priority_run_ids(sessions, [], now=now) == []
            finally:
                await outer.rollback()
    finally:
        await engine.dispose()

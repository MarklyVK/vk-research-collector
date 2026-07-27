from __future__ import annotations

import uuid
from datetime import UTC, datetime

from sqlalchemy import select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from vk_collector.database.models import (
    GroupCandidate,
    GroupKeywordMatch,
    RunStatus,
    SearchKeyword,
    SearchRun,
    SearchRunKeyword,
)
from vk_collector.search.service import Keyword
from vk_collector.vk import VKGroup


class PostgresSearchPersistence:
    """PostgreSQL checkpoints and idempotent candidate writes."""

    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._sessions = session_factory

    async def _keyword_id(self, session: AsyncSession, keyword: Keyword) -> int:
        statement = (
            insert(SearchKeyword)
            .values(subject=keyword.subject, keyword=keyword.value)
            .on_conflict_do_update(
                index_elements=[SearchKeyword.subject, SearchKeyword.keyword],
                set_={"enabled": True},
            )
            .returning(SearchKeyword.id)
        )
        return int(await session.scalar(statement))

    async def start_or_resume_run(self, keywords: tuple[Keyword, ...]) -> str:
        async with self._sessions() as session:
            run = await session.scalar(
                select(SearchRun)
                .where(SearchRun.status.in_([RunStatus.RUNNING, RunStatus.PAUSED]))
                .order_by(SearchRun.created_at.desc())
                .limit(1)
                .with_for_update(skip_locked=True)
            )
            if run is None:
                run = SearchRun(status=RunStatus.RUNNING)
                session.add(run)
                await session.flush()
            else:
                run.status = RunStatus.RUNNING
                run.error_message = None
            await session.commit()
            return str(run.id)

    async def _progress(
        self, session: AsyncSession, run_id: str, keyword: Keyword, group_type: str
    ) -> SearchRunKeyword:
        keyword_id = await self._keyword_id(session, keyword)
        statement = (
            insert(SearchRunKeyword)
            .values(
                search_run_id=uuid.UUID(run_id),
                keyword_id=keyword_id,
                community_type=group_type,
                status=RunStatus.RUNNING,
            )
            .on_conflict_do_nothing(
                index_elements=["search_run_id", "keyword_id", "community_type"]
            )
        )
        await session.execute(statement)
        progress = await session.scalar(
            select(SearchRunKeyword).where(
                SearchRunKeyword.search_run_id == uuid.UUID(run_id),
                SearchRunKeyword.keyword_id == keyword_id,
                SearchRunKeyword.community_type == group_type,
            )
        )
        if progress is None:
            raise RuntimeError("Не удалось создать checkpoint поиска")
        return progress

    async def get_offset(self, run_id: str, keyword: Keyword, group_type: str) -> int:
        async with self._sessions() as session:
            progress = await self._progress(session, run_id, keyword, group_type)
            await session.commit()
            return progress.next_offset

    async def is_keyword_complete(self, run_id: str, keyword: Keyword, group_type: str) -> bool:
        async with self._sessions() as session:
            progress = await self._progress(session, run_id, keyword, group_type)
            await session.commit()
            return progress.status == RunStatus.COMPLETED

    async def save_page(
        self,
        run_id: str,
        keyword: Keyword,
        group_type: str,
        groups: tuple[VKGroup, ...],
        next_offset: int,
    ) -> None:
        now = datetime.now(UTC)
        async with self._sessions() as session:
            keyword_id = await self._keyword_id(session, keyword)
            for group in groups:
                group_id = await session.scalar(
                    insert(GroupCandidate)
                    .values(
                        vk_id=group.vk_id,
                        name=group.name,
                        description=group.description,
                        status_text=group.status,
                        screen_name=group.screen_name,
                        address=group.address,
                        first_seen_at=now,
                        last_seen_at=now,
                    )
                    .on_conflict_do_update(
                        index_elements=[GroupCandidate.vk_id],
                        set_={
                            "name": group.name,
                            "description": group.description,
                            "status_text": group.status,
                            "screen_name": group.screen_name,
                            "address": group.address,
                            "last_seen_at": now,
                        },
                    )
                    .returning(GroupCandidate.id)
                )
                await session.execute(
                    insert(GroupKeywordMatch)
                    .values(
                        group_id=group_id,
                        keyword_id=keyword_id,
                        first_search_run_id=uuid.UUID(run_id),
                        first_matched_at=now,
                        last_matched_at=now,
                    )
                    .on_conflict_do_update(
                        index_elements=["group_id", "keyword_id"],
                        set_={"last_matched_at": now},
                    )
                )
            await session.execute(
                update(SearchRunKeyword)
                .where(
                    SearchRunKeyword.search_run_id == uuid.UUID(run_id),
                    SearchRunKeyword.keyword_id == keyword_id,
                    SearchRunKeyword.community_type == group_type,
                )
                .values(next_offset=next_offset)
            )
            await session.commit()

    async def mark_keyword_complete(self, run_id: str, keyword: Keyword, group_type: str) -> None:
        await self._set_progress(run_id, keyword, group_type, RunStatus.COMPLETED)

    async def _set_progress(
        self,
        run_id: str,
        keyword: Keyword,
        group_type: str,
        status: RunStatus,
        error: str | None = None,
    ) -> None:
        async with self._sessions() as session:
            progress = await self._progress(session, run_id, keyword, group_type)
            progress.status = status
            progress.error_message = error
            if status == RunStatus.COMPLETED:
                progress.completed_at = datetime.now(UTC)
            await session.commit()

    async def mark_run_complete(self, run_id: str) -> None:
        await self._set_run(run_id, RunStatus.COMPLETED)

    async def pause_run(self, run_id: str, reason: str) -> None:
        await self._set_run(run_id, RunStatus.PAUSED, reason)

    async def _set_run(self, run_id: str, status: RunStatus, error: str | None = None) -> None:
        async with self._sessions() as session:
            values: dict[str, object] = {"status": status, "error_message": error}
            if status == RunStatus.COMPLETED:
                values["finished_at"] = datetime.now(UTC)
            await session.execute(
                update(SearchRun).where(SearchRun.id == uuid.UUID(run_id)).values(**values)
            )
            await session.commit()

    async def record_keyword_error(
        self, run_id: str, keyword: Keyword, group_type: str, error: str
    ) -> None:
        await self._set_progress(run_id, keyword, group_type, RunStatus.FAILED, error)

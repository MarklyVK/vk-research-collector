"""Оркестрация поиска, не связанная с конкретной реализацией БД."""

from dataclasses import dataclass
from typing import Protocol

from vk_collector.vk import VKClient, VKError, VKGroup, VKTokensUnavailable


@dataclass(frozen=True, slots=True)
class Keyword:
    value: str
    subject: str


class SearchPersistence(Protocol):
    async def start_or_resume_run(self, keywords: tuple[Keyword, ...]) -> str: ...

    async def get_offset(self, run_id: str, keyword: Keyword, group_type: str) -> int: ...

    async def is_keyword_complete(self, run_id: str, keyword: Keyword, group_type: str) -> bool: ...

    async def save_page(
        self,
        run_id: str,
        keyword: Keyword,
        group_type: str,
        groups: tuple[VKGroup, ...],
        next_offset: int,
    ) -> None: ...

    async def mark_keyword_complete(
        self, run_id: str, keyword: Keyword, group_type: str
    ) -> None: ...

    async def mark_run_complete(self, run_id: str) -> None: ...

    async def pause_run(self, run_id: str, reason: str) -> None: ...

    async def record_keyword_error(
        self, run_id: str, keyword: Keyword, group_type: str, error: str
    ) -> None: ...


class SearchService:
    def __init__(
        self,
        client: VKClient,
        persistence: SearchPersistence,
        *,
        page_size: int = 1000,
        group_types: tuple[str, ...] = ("group",),
    ) -> None:
        self._client = client
        self._persistence = persistence
        self._page_size = page_size
        self._group_types = group_types

    async def run(self, keywords: tuple[Keyword, ...]) -> str:
        """Запустить либо продолжить поиск; каждая сохранённая страница является checkpoint."""
        run_id = await self._persistence.start_or_resume_run(keywords)
        try:
            for keyword in keywords:
                for group_type in self._group_types:
                    if await self._persistence.is_keyword_complete(run_id, keyword, group_type):
                        continue
                    offset = await self._persistence.get_offset(run_id, keyword, group_type)
                    try:
                        async for next_offset, page in self._client.iter_search(
                            keyword.value,
                            start_offset=offset,
                            page_size=self._page_size,
                            group_type=group_type,
                        ):
                            await self._persistence.save_page(
                                run_id, keyword, group_type, page.items, next_offset
                            )
                        await self._persistence.mark_keyword_complete(run_id, keyword, group_type)
                    except VKTokensUnavailable:
                        raise
                    except VKError as exc:
                        await self._persistence.record_keyword_error(
                            run_id, keyword, group_type, str(exc)
                        )
            await self._persistence.mark_run_complete(run_id)
        except VKTokensUnavailable as exc:
            await self._persistence.pause_run(run_id, str(exc))
        return run_id

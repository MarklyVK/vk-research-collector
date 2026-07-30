import asyncio
from collections.abc import Callable, Coroutine
from typing import Any, TypeVar

from vk_collector.search import Keyword, SearchService
from vk_collector.vk import VKGroup, VKSearchPage, VKTokensUnavailable

T = TypeVar("T")


def async_test(function: Callable[[], Coroutine[Any, Any, T]]) -> Callable[[], T]:
    def run() -> T:
        return asyncio.run(function())

    return run


class FakeClient:
    async def iter_search(
        self, query: str, *, start_offset: int, page_size: int, group_type: str
    ) -> Any:
        yield (
            2,
            VKSearchPage(2, (VKGroup(1, "One", "", "", None, "https://vk.com/club1"),)),
        )


class MemoryPersistence:
    def __init__(self, offset: int = 0) -> None:
        self.offset = offset
        self.saved: list[int] = []
        self.complete = False
        self.paused = False

    async def start_or_resume_run(self, keywords: tuple[Keyword, ...]) -> str:
        return "run-id"

    async def get_offset(self, run_id: str, keyword: Keyword, group_type: str) -> int:
        return self.offset

    async def is_keyword_complete(self, run_id: str, keyword: Keyword, group_type: str) -> bool:
        return False

    async def save_page(
        self,
        run_id: str,
        keyword: Keyword,
        group_type: str,
        page: VKSearchPage,
        next_offset: int,
    ) -> None:
        self.saved.append(next_offset)
        self.offset = next_offset

    async def mark_keyword_complete(self, run_id: str, keyword: Keyword, group_type: str) -> None:
        pass

    async def mark_run_complete(self, run_id: str) -> None:
        self.complete = True

    async def pause_run(self, run_id: str, reason: str) -> None:
        self.paused = True

    async def record_keyword_error(
        self, run_id: str, keyword: Keyword, group_type: str, error: str
    ) -> None:
        pass


@async_test
async def test_page_is_checkpointed_and_run_completed() -> None:
    persistence = MemoryPersistence(offset=1)
    service = SearchService(FakeClient(), persistence, page_size=2)  # type: ignore[arg-type]
    assert await service.run((Keyword("еда", "food"),)) == "run-id"
    assert persistence.saved == [2]
    assert persistence.complete


class EmptyPoolClient:
    async def iter_search(
        self, query: str, *, start_offset: int, page_size: int, group_type: str
    ) -> Any:
        raise VKTokensUnavailable("empty")
        yield


@async_test
async def test_run_is_paused_when_all_tokens_are_unavailable() -> None:
    persistence = MemoryPersistence()
    service = SearchService(EmptyPoolClient(), persistence)  # type: ignore[arg-type]
    await service.run((Keyword("еда", "food"),))
    assert persistence.paused
    assert not persistence.complete

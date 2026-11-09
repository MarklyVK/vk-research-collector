"""Ретроспектива панели: одна страница на execute, откат count на ошибке VK 13, очередь."""

from __future__ import annotations

from collections.abc import Sequence
from types import SimpleNamespace
from typing import Any

import pytest

from vk_collector.panel.service import PanelService
from vk_collector.vk.errors import VKAPIError


class TooBigVK:
    """Стена, где страница больше max_ok постов не помещается в ответ execute (ошибка 13)."""

    def __init__(self, max_ok: int) -> None:
        self.max_ok = max_ok
        self.calls: list[tuple[int, list[int], int]] = []

    async def wall_pages(self, community_id: int, offsets: Sequence[int], count: int) -> list[Any]:
        self.calls.append((community_id, list(offsets), count))
        if count > self.max_ok:
            raise VKAPIError(13, "response size is too big")
        return [{"count": 1000, "items": [{"id": i} for i in range(count)]} for _ in offsets]


def _service(vk: Any) -> PanelService:
    return PanelService(sessions=None, vk=vk)  # type: ignore[arg-type]


@pytest.mark.asyncio
async def test_page_falls_back_100_50_20_on_error_13() -> None:
    vk = TooBigVK(max_ok=20)
    service = _service(vk)
    page, size = await service._backfill_page(
        7, 300, service._page_sizes({"backfill_page_size": 100})
    )
    assert size == 20
    assert len(page["items"]) == 20
    # всегда одна страница на execute
    assert vk.calls == [(7, [300], 100), (7, [300], 50), (7, [300], 20)]


@pytest.mark.asyncio
async def test_page_raises_when_even_20_is_too_big() -> None:
    vk = TooBigVK(max_ok=10)
    service = _service(vk)
    with pytest.raises(VKAPIError) as info:
        await service._backfill_page(7, 0, [100, 50, 20])
    assert info.value.code == 13


@pytest.mark.asyncio
async def test_other_vk_errors_are_not_retried_with_smaller_count() -> None:
    class Denied(TooBigVK):
        async def wall_pages(
            self, community_id: int, offsets: Sequence[int], count: int
        ) -> list[Any]:
            self.calls.append((community_id, list(offsets), count))
            raise VKAPIError(15, "access denied")

    vk = Denied(max_ok=100)
    with pytest.raises(VKAPIError):
        await _service(vk)._backfill_page(7, 0, [100, 50, 20])
    assert len(vk.calls) == 1


def test_deferred_communities_go_to_end_of_queue() -> None:
    service = _service(TooBigVK(100))
    todo = [SimpleNamespace(community_vk_id=c) for c in (1, 2, 3, 4)]
    service._bf_deferred = [3, 1]
    order = [c.community_vk_id for c in service._backfill_order(todo)]  # type: ignore[arg-type]
    assert order == [2, 4, 3, 1]


def test_page_sizes_respect_configured_first_size() -> None:
    service = _service(TooBigVK(100))
    assert service._page_sizes({"backfill_page_size": 100}) == [100, 50, 20]
    assert service._page_sizes({"backfill_page_size": 50}) == [50, 20]

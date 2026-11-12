"""Доступ панели к VK только через ``execute`` (до 25 вызовов API внутри одного запроса).

Ответы облегчаются прямо в VKScript, где это возможно (замеры счётчиков через ``@.``), чтобы
не упираться в размер ответа. Каждому ``execute`` передаётся ``inner_calls`` — для суточного
бюджета токена (вызовы внутри ``execute`` считаются отдельно по методам).
"""

from __future__ import annotations

import json
from collections.abc import Sequence
from typing import Any, Protocol

from vk_collector.vk.client import VKClient

MAX_EXECUTE_CALLS = 25


class PanelVK(Protocol):
    async def wall_latest(self, community_ids: Sequence[int], count: int) -> dict[int, Any]: ...

    async def wall_pages(
        self, community_id: int, offsets: Sequence[int], count: int
    ) -> list[Any]: ...

    async def posts_counters(self, keys: Sequence[str]) -> list[dict[str, Any]]: ...

    async def groups_info(self, community_ids: Sequence[int]) -> list[dict[str, Any]]: ...


class ExecutePanelVK:
    """Реальный адаптер поверх VKClient."""

    def __init__(
        self,
        client: VKClient,
        *,
        communities_per_execute: int = 10,
        pages_per_execute: int = 5,
        getbyid_per_execute: int = 10,
    ) -> None:
        if max(communities_per_execute, pages_per_execute, getbyid_per_execute) > MAX_EXECUTE_CALLS:
            raise ValueError("execute допускает не больше 25 вызовов API")
        self._client = client
        self.communities_per_execute = communities_per_execute
        self.pages_per_execute = pages_per_execute
        self.getbyid_per_execute = getbyid_per_execute

    async def _execute(self, code: str, inner: dict[str, int]) -> Any:
        # токен выбирается по внутреннему методу: ошибка 9/29 на нём закрывает токен
        # только для таких execute (см. VKClient._inner_errors)
        result, _ = await self._execute_checked(code, inner)
        return result

    async def _execute_checked(self, code: str, inner: dict[str, int]) -> tuple[Any, list[Any]]:
        """execute + список ошибок внутренних вызовов (execute_errors)."""
        key = "execute:" + "+".join(sorted(inner))
        out = await self._client.call(
            "execute", {"code": code}, inner_calls=inner, lease_key=key, return_errors=True
        )
        if isinstance(out, tuple) and len(out) == 2:
            return out[0], list(out[1] or [])
        return out, []  # клиент без return_errors (тестовые подмены)

    def take_token_calls(self) -> dict[str, int]:
        take = getattr(self._client, "take_token_calls", None)
        return take() if callable(take) else {}

    async def wall_latest(self, community_ids: Sequence[int], count: int) -> dict[int, Any]:
        """Последние count постов каждой стены (полные объекты).

        Возвращает {community_id: {"count", "items"} | None (ошибка)}.
        """
        out: dict[int, Any] = {}
        step = self.communities_per_execute
        for i in range(0, len(community_ids), step):
            part = [int(x) for x in community_ids[i : i + step]]
            code = (
                f"var ids={json.dumps(part)};var res=[];var i=0;"
                "while(i<ids.length){"
                f'res.push(API.wall.get({{"owner_id":-ids[i],"count":{int(count)}}}));i=i+1;}}'
                "return res;"
            )
            result = await self._execute(code, {"wall.get": len(part)})
            for cid, item in zip(part, result or [], strict=False):
                out[cid] = item if isinstance(item, dict) else None
        return out

    async def wall_pages(self, community_id: int, offsets: Sequence[int], count: int) -> list[Any]:
        """Страницы стены одного сообщества (offset, count), до pages_per_execute за execute."""
        pages: list[Any] = []
        step = self.pages_per_execute
        for i in range(0, len(offsets), step):
            part = [int(x) for x in offsets[i : i + step]]
            code = (
                f"var o={json.dumps(part)};var res=[];var i=0;"
                "while(i<o.length){"
                f'res.push(API.wall.get({{"owner_id":-{int(community_id)},'
                f'"offset":o[i],"count":{int(count)}}}));i=i+1;}}'
                "return res;"
            )
            result = await self._execute(code, {"wall.get": len(part)})
            pages.extend(result or [])
        return pages

    async def posts_counters(self, keys: Sequence[str]) -> list[dict[str, Any]]:
        """Счётчики постов по ключам "owner_post" (≤100 в wall.getById) — облегчённо через @.

        Если wall.getById внутри execute вернул ошибку (VK отдаёт false, например ошибка 9),
        для каждого ключа этой порции возвращается {"failed": key}: пост НЕ замерен, но и не
        удалён. Удалённым считается только пост, которого нет в успешном ответе VK.
        """
        rows: list[dict[str, Any]] = []
        chunks = [list(keys[i : i + 100]) for i in range(0, len(keys), 100)]
        step = self.getbyid_per_execute
        for i in range(0, len(chunks), step):
            part = [",".join(c) for c in chunks[i : i + step]]
            code = (
                f"var p={json.dumps(part)};var res=[];var i=0;"
                'while(i<p.length){var r=API.wall.getById({"posts":p[i]});'
                'if(!r){res.push({"ok":0});}else{'
                "var it=r.items;if(!it){it=r;}"
                'res.push({"ok":1,"id":it@.id,"owner_id":it@.owner_id,"date":it@.date,'
                '"views":it@.views,"likes":it@.likes,"reposts":it@.reposts,"comments":it@.comments});}'
                "i=i+1;}return res;"
            )
            group = chunks[i : i + step]
            result, errors = await self._execute_checked(code, {"wall.getById": len(part)})
            # «не получено» — только если VK сообщил об ошибке wall.getById в этом execute;
            # иначе пустой ответ = все посты порции удалены (явный признак VK, как раньше)
            getbyid_failed = any(
                isinstance(e, dict) and str(e.get("method", "")).endswith("wall.getById")
                for e in errors
            )
            blocks = result if isinstance(result, list) else []
            for n, chunk in enumerate(group):
                block = blocks[n] if n < len(blocks) else None
                bad = not isinstance(block, dict) or block.get("ok") == 0
                if bad and (getbyid_failed or not isinstance(result, list)):
                    rows.extend({"failed": key} for key in chunk)
                    continue
                if bad or not isinstance(block, dict):
                    continue  # успешный пустой ответ: постов порции у VK нет
                ids = block.get("id") or []
                for j, post_id in enumerate(ids):
                    rows.append(
                        {
                            "id": post_id,
                            "owner_id": _at(block.get("owner_id"), j),
                            "date": _at(block.get("date"), j),
                            "views": _count(_at(block.get("views"), j)),
                            "likes": _count(_at(block.get("likes"), j)),
                            "reposts": _count(_at(block.get("reposts"), j)),
                            "comments": _count(_at(block.get("comments"), j)),
                        }
                    )
        return rows

    async def groups_info(self, community_ids: Sequence[int]) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        for i in range(0, len(community_ids), 500):
            part = ",".join(str(int(x)) for x in community_ids[i : i + 500])
            result = await self._client.call(
                "groups.getById", {"group_ids": part, "fields": "members_count"}
            )
            groups = result.get("groups", []) if isinstance(result, dict) else result
            out.extend(groups or [])
        return out


def _at(values: Any, index: int) -> Any:
    if isinstance(values, list) and index < len(values):
        return values[index]
    return None


def _count(value: Any) -> int | None:
    if isinstance(value, dict):
        raw = value.get("count")
        return int(raw) if isinstance(raw, int) else None
    return int(value) if isinstance(value, int) else None

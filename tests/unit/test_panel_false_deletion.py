"""Пустой ответ wall.getById при ошибке VK не считается удалением (решение 12.11.2026)."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import httpx
import pytest

from vk_collector.panel.vk_adapter import ExecutePanelVK
from vk_collector.vk import TokenPool, VKClient
from vk_collector.vk.budget import DailyBudget


class FakeClient:
    def __init__(self, result: Any, errors: list[Any]) -> None:
        self.result, self.errors = result, errors
        self.calls: list[dict[str, Any]] = []

    async def call(self, method: str, params: dict[str, Any], **kw: Any) -> Any:
        self.calls.append(kw)
        return (self.result, self.errors) if kw.get("return_errors") else self.result


def _ok_block(ids: list[int], owner: int) -> dict[str, Any]:
    n = len(ids)
    return {
        "ok": 1,
        "id": ids,
        "owner_id": [owner] * n,
        "date": [1] * n,
        "views": [{"count": 10}] * n,
        "likes": [{"count": 1}] * n,
        "reposts": [{"count": 0}] * n,
        "comments": [{"count": 0}] * n,
    }


@pytest.mark.asyncio
async def test_failed_chunk_is_not_deletion_when_vk_reports_error() -> None:
    client = FakeClient([{"ok": 0}], [{"method": "wall.getById", "error_code": 9}])
    vk = ExecutePanelVK(client)  # type: ignore[arg-type]
    rows = await vk.posts_counters(["-1_10", "-1_11"])
    assert rows == [{"failed": "-1_10"}, {"failed": "-1_11"}]
    assert client.calls[0]["lease_key"] == "execute:wall.getById"


@pytest.mark.asyncio
async def test_empty_chunk_without_error_means_deleted_as_before() -> None:
    client = FakeClient([{"ok": 0}], [])  # VK не сообщил об ошибке: постов порции нет
    vk = ExecutePanelVK(client)  # type: ignore[arg-type]
    assert await vk.posts_counters(["-1_10"]) == []


@pytest.mark.asyncio
async def test_ok_chunk_returns_counters_and_missing_post_is_absent() -> None:
    client = FakeClient([_ok_block([10], -1)], [])
    vk = ExecutePanelVK(client)  # type: ignore[arg-type]
    rows = await vk.posts_counters(["-1_10", "-1_11"])
    assert [(r["owner_id"], r["id"], r["views"]) for r in rows] == [(-1, 10, 10)]


@pytest.mark.asyncio
async def test_inner_error_9_blocks_token_only_for_that_lease_key(
    caplog: pytest.LogCaptureFixture,
) -> None:
    seen: list[tuple[str, str]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        params = dict(httpx.QueryParams(request.content.decode()))
        code = params.get("code", "")
        seen.append((code, params["access_token"]))
        if params["access_token"] == "token-a" and code == "getbyid":
            return httpx.Response(
                200,
                json={
                    "response": [False],
                    "execute_errors": [
                        {"method": "wall.getById", "error_code": 9, "error_msg": "flood"}
                    ],
                },
            )
        return httpx.Response(200, json={"response": [1]})

    t = {"v": 0.0}

    async def sleep(d: float) -> None:
        t["v"] += d

    pool = TokenPool(["token-a", "token-b"], rps=1000, clock=lambda: t["v"], sleep=sleep)
    budget = DailyBudget({}, now=lambda: datetime(2026, 11, 12, 9, 0, tzinfo=UTC))
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler), base_url="https://api.vk.test/"
    ) as http:
        client = VKClient(pool, http_client=http, sleep=sleep, budget=budget)
        with caplog.at_level("WARNING"):
            res, errs = await client.call(
                "execute",
                {"code": "getbyid"},
                lease_key="execute:wall.getById",
                return_errors=True,
            )
        assert res == [False] and errs[0]["error_code"] == 9
        # тот же lease_key — уже только token-b; другой (wall.get) — token-a снова доступен
        for _ in range(3):
            await client.call("execute", {"code": "getbyid"}, lease_key="execute:wall.getById")
        await client.call("execute", {"code": "wallget"}, lease_key="execute:wall.get")
        await client.call("execute", {"code": "wallget"}, lease_key="execute:wall.get")
    getbyid_tokens = [tok for code, tok in seen[1:] if code == "getbyid"]
    assert set(getbyid_tokens) == {"token-b"}
    assert "token-a" in {tok for code, tok in seen if code == "wallget"}
    assert "vk_execute_inner_error" in caplog.text and "vk_error=9" in caplog.text
    assert "vk_token_method_stopped_until_next_day" in caplog.text
    assert "token-a" not in caplog.text  # в логе только отпечаток, не токен
    calls = client.take_token_calls()
    assert sum(calls.values()) == len(seen) and all(len(k) == 6 for k in calls)
    assert client.take_token_calls() == {}

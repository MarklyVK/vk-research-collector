"""Суточный бюджет VK на токен: учёт вызовов внутри execute и остановка при 80 % лимита."""

from __future__ import annotations

from datetime import UTC, datetime

import httpx
import pytest

from vk_collector.config import Settings
from vk_collector.vk import TokenPool, VKClient, VKMethodUnavailable
from vk_collector.vk.budget import DailyBudget


class FakeTime:
    def __init__(self) -> None:
        self.value = 0.0

    def clock(self) -> float:
        return self.value

    async def sleep(self, delay: float) -> None:
        self.value += delay


def _now() -> datetime:
    return datetime(2026, 11, 9, 9, 0, tzinfo=UTC)


@pytest.mark.asyncio
async def test_budget_counts_inner_calls_and_caps_at_fraction() -> None:
    budget = DailyBudget({"wall.get": 100}, fraction=0.8, now=_now)
    assert budget.cap("wall.get") == 80
    assert await budget.exceeded("fp", {"execute": 1, "wall.get": 25}) == []
    for _ in range(3):
        await budget.record("fp", {"execute": 1, "wall.get": 25})
    assert (await budget.usage("fp", ["wall.get", "execute"])) == {"wall.get": 75, "execute": 3}
    assert await budget.exceeded("fp", {"execute": 1, "wall.get": 25}) == ["wall.get"]
    assert await budget.exceeded("fp", {"execute": 1, "wall.get": 5}) == []
    assert await budget.exceeded("other", {"wall.get": 25}) == []
    # методы без лимита только учитываются
    assert await budget.exceeded("fp", {"execute": 10_000}) == []
    assert budget.next_reset().isoformat() == "2026-11-10T00:00:00+03:00"


@pytest.mark.asyncio
async def test_client_moves_to_next_token_then_stops_at_budget() -> None:
    seen: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        params = dict(httpx.QueryParams(request.content.decode()))
        seen.append(params["access_token"])
        return httpx.Response(200, json={"response": [{"count": 0, "items": []}]})

    time = FakeTime()
    pool = TokenPool(["token-a", "token-b"], rps=1000, clock=time.clock, sleep=time.sleep)
    budget = DailyBudget({"wall.get": 125}, fraction=0.8, now=_now)  # cap 100 = 4 execute × 25
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler), base_url="https://api.vk.test/"
    ) as http:
        client = VKClient(pool, http_client=http, sleep=time.sleep, budget=budget)
        for _ in range(8):
            await client.call("execute", {"code": "return 1;"}, inner_calls={"wall.get": 25})
        with pytest.raises(VKMethodUnavailable):
            await client.call("execute", {"code": "return 1;"}, inner_calls={"wall.get": 25})
    assert sorted(seen) == ["token-a"] * 4 + ["token-b"] * 4
    usage_a = await budget.usage(pool._states[0].fingerprint, ["wall.get", "execute"])
    assert usage_a == {"wall.get": 100, "execute": 4}


def test_settings_parse_daily_limits() -> None:
    settings = Settings(vk_method_daily_limits="wall.get=20000, wall.getById=15000")
    assert settings.method_daily_limits == {"wall.get": 20000, "wall.getById": 15000}
    with pytest.raises(ValueError):
        _ = Settings(vk_method_daily_limits="wall.get=abc").method_daily_limits

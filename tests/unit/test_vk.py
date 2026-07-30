import asyncio
import json
from collections.abc import Callable, Coroutine
from pathlib import Path
from typing import Any, TypeVar

import httpx
import pytest

from vk_collector.vk import (
    TokenPool,
    VKAPIError,
    VKClient,
    VKTokensUnavailable,
    load_tokens,
)

T = TypeVar("T")


def async_test(function: Callable[[], Coroutine[Any, Any, T]]) -> Callable[[], T]:
    def run() -> T:
        return asyncio.run(function())

    return run


class FakeTime:
    def __init__(self) -> None:
        self.value = 0.0
        self.sleeps: list[float] = []

    def clock(self) -> float:
        return self.value

    async def sleep(self, delay: float) -> None:
        self.sleeps.append(delay)
        self.value += delay


def test_loads_unlimited_nonempty_tokens(tmp_path: Path) -> None:
    path = tmp_path / "tokens.txt"
    path.write_text(" first\n\nsecond \n third\n", encoding="utf-8")
    assert load_tokens(path) == ("first", "second", "third")


@async_test
async def test_pool_rate_limit_cooldown_disable_and_redacted_repr() -> None:
    time = FakeTime()
    pool = TokenPool(["secret-a", "secret-b"], rps=2, clock=time.clock, sleep=time.sleep)
    assert await pool.acquire() == "secret-a"
    await pool.cooldown("secret-b", 10)
    assert await pool.acquire() == "secret-a"
    assert time.sleeps == [0.5]
    await pool.disable("secret-a")
    assert await pool.acquire() == "secret-b"
    assert time.value == 10
    await pool.disable("secret-b")
    with pytest.raises(VKTokensUnavailable):
        await pool.acquire()
    assert "secret" not in repr(pool)


@async_test
async def test_auth_error_switches_token_without_leaking_it() -> None:
    seen: list[str] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        form = dict(item.split("=") for item in request.content.decode().split("&"))
        seen.append(form["access_token"])
        if len(seen) == 1:
            return httpx.Response(200, json={"error": {"error_code": 5, "error_msg": "auth"}})
        return httpx.Response(200, json={"response": {"count": 0, "items": []}})

    time = FakeTime()
    pool = TokenPool(["bad", "good"], rps=100, clock=time.clock, sleep=time.sleep)
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler), base_url="https://api.vk.test/"
    ) as http:
        client = VKClient(pool, http_client=http, sleep=time.sleep, retry_delays=())
        assert (await client.search_page("query")).total == 0
    assert seen == ["bad", "good"]


@async_test
async def test_pagination_filters_unavailable_groups() -> None:
    requests: list[dict[str, str]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        params = dict(httpx.QueryParams(request.content.decode()))
        requests.append(params)
        offset = int(params["offset"])
        items = (
            [
                {"id": 1, "name": "One", "screen_name": "one", "description": "d"},
                {"id": 2, "is_closed": 1},
            ]
            if offset == 0
            else [{"id": 3, "name": "Three", "deactivated": "deleted"}]
        )
        return httpx.Response(200, json={"response": {"count": 3, "items": items}})

    time = FakeTime()
    pool = TokenPool(["fake"], rps=1000, clock=time.clock, sleep=time.sleep)
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler), base_url="https://api.vk.test/"
    ) as http:
        client = VKClient(pool, http_client=http, sleep=time.sleep)
        pages = [page async for page in client.iter_search("еда", page_size=2)]
    assert [offset for offset, _ in pages] == [2, 3]
    assert pages[0][1].items[0].address == "https://vk.com/one"
    assert pages[0][1].raw_count == 2
    assert pages[0][1].private_count == 1
    assert pages[1][1].deleted_count == 1
    assert [request["offset"] for request in requests] == ["0", "2"]


@async_test
async def test_invalid_params_is_not_retried() -> None:
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(
            200, content=json.dumps({"error": {"error_code": 100, "error_msg": "bad"}})
        )

    time = FakeTime()
    pool = TokenPool(["fake"], rps=10, clock=time.clock, sleep=time.sleep)
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler), base_url="https://api.vk.test/"
    ) as http:
        with pytest.raises(VKAPIError):
            await VKClient(pool, http_client=http, sleep=time.sleep).call("groups.search", {})
    assert calls == 1
    assert time.sleeps == []


@pytest.mark.asyncio
async def test_transient_retry_uses_injected_jitter_without_real_wait() -> None:
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        if calls == 1:
            return httpx.Response(503, request=request)
        return httpx.Response(200, json={"response": []}, request=request)

    time = FakeTime()
    pool = TokenPool(["fake"], rps=1000, clock=time.clock, sleep=time.sleep)
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler), base_url="https://api.vk.test/"
    ) as http:
        client = VKClient(
            pool,
            http_client=http,
            sleep=time.sleep,
            retry_delays=(10,),
            jitter=lambda delay: delay + 1,
        )
        assert await client.call("groups.getById", {}) == []
    assert time.sleeps == [11]

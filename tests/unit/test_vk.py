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
    VKMethodUnavailable,
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
    first = await pool.acquire("groups.get")
    assert first.token == "secret-a"
    second = await pool.acquire("groups.get")
    await pool.global_cooldown(second, 10)
    assert (await pool.acquire("groups.get")).token == "secret-a"
    assert time.sleeps == [0.5]
    await pool.disable(first)
    assert (await pool.acquire("groups.get")).token == "secret-b"
    assert time.value == 10
    await pool.disable(second)
    with pytest.raises(VKTokensUnavailable):
        await pool.acquire("groups.get")
    assert "secret" not in repr(pool)
    assert "secret" not in repr(first)


@pytest.mark.asyncio
@pytest.mark.parametrize("error_code", [9, 29])
async def test_method_limit_switches_token_and_keeps_other_methods(error_code: int) -> None:
    seen: list[tuple[str, str]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        params = dict(httpx.QueryParams(request.content.decode()))
        token = params["access_token"]
        method = request.url.path.rsplit("/", 1)[-1]
        seen.append((method, token))
        if method == "groups.get" and token == "token-a":
            return httpx.Response(
                200,
                json={"error": {"error_code": error_code, "error_msg": "limited"}},
            )
        return httpx.Response(200, json={"response": {"count": 0, "items": []}})

    time = FakeTime()
    pool = TokenPool(
        ["token-a", "token-b"],
        rps=1000,
        clock=time.clock,
        sleep=time.sleep,
        flood_initial_cooldown=10,
        quota_initial_cooldown=10,
    )
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler), base_url="https://api.vk.test/"
    ) as http:
        client = VKClient(pool, http_client=http, sleep=time.sleep)
        await client.call("groups.get", {})
        await client.call("wall.get", {})
    assert seen[:2] == [("groups.get", "token-a"), ("groups.get", "token-b")]
    assert seen[2] == ("wall.get", "token-a")


@pytest.mark.asyncio
async def test_all_tokens_limited_for_exact_method_raise_unavailable() -> None:
    time = FakeTime()
    pool = TokenPool(
        ["a", "b"],
        rps=1000,
        clock=time.clock,
        sleep=time.sleep,
        flood_initial_cooldown=10,
    )
    first = await pool.acquire("groups.get")
    await pool.method_cooldown(first, 9)
    second = await pool.acquire("groups.get")
    await pool.method_cooldown(second, 9)
    with pytest.raises(VKMethodUnavailable):
        await pool.acquire("groups.get")
    assert (await pool.acquire("wall.get")).fingerprint in {
        first.fingerprint,
        second.fingerprint,
    }


@pytest.mark.asyncio
async def test_method_cooldown_grows_and_is_capped() -> None:
    time = FakeTime()
    pool = TokenPool(
        ["a"],
        rps=1000,
        clock=time.clock,
        sleep=time.sleep,
        flood_initial_cooldown=10,
        max_method_cooldown=25,
    )
    lease = await pool.acquire("groups.get")
    assert await pool.method_cooldown(lease, 9) == 10
    time.value = 10
    lease = await pool.acquire("groups.get")
    assert await pool.method_cooldown(lease, 9) == 30
    time.value = 30
    lease = await pool.acquire("groups.get")
    assert await pool.method_cooldown(lease, 9) == 55


@pytest.mark.asyncio
async def test_next_probe_allows_exactly_one_attempt_before_long_block_expires() -> None:
    time = FakeTime()
    pool = TokenPool(
        ["a"],
        rps=1000,
        clock=time.clock,
        sleep=time.sleep,
        flood_initial_cooldown=100,
        probe_seconds=10,
    )
    lease = await pool.acquire("groups.get")
    await pool.method_cooldown(lease, 9)
    with pytest.raises(VKMethodUnavailable):
        await pool.acquire("groups.get")
    time.value = 10
    probe = await pool.acquire("groups.get")
    assert probe.is_probe
    with pytest.raises(VKMethodUnavailable):
        await pool.acquire("groups.get")
    await pool.mark_success(probe)
    assert not (await pool.acquire("groups.get")).is_probe


@pytest.mark.asyncio
async def test_code_6_uses_pool_configured_17_second_cooldown() -> None:
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        if calls == 1:
            return httpx.Response(
                200, json={"error": {"error_code": 6, "error_msg": "too many requests"}}
            )
        return httpx.Response(200, json={"response": []})

    time = FakeTime()
    pool = TokenPool(
        ["a"],
        rps=1000,
        clock=time.clock,
        sleep=time.sleep,
        global_rps_cooldown=17,
    )
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler), base_url="https://api.vk.test/"
    ) as http:
        await VKClient(pool, http_client=http).call("users.get", {})
    assert time.sleeps == [17]


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


@pytest.mark.asyncio
async def test_subscriptions_request_uses_extended_objects_and_requested_limit() -> None:
    captured: dict[str, str] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured.update(dict(httpx.QueryParams(request.content.decode())))
        return httpx.Response(
            200,
            json={"response": {"count": 1, "items": [{"id": 7, "name": "Группа"}]}},
        )

    time = FakeTime()
    pool = TokenPool(["fake"], rps=1000, clock=time.clock, sleep=time.sleep)
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler), base_url="https://api.vk.test/"
    ) as http:
        response = await VKClient(pool, http_client=http).get_subscriptions_page(42, 0, 100)
    assert response["items"] == [{"id": 7, "name": "Группа"}]
    assert captured["extended"] == "1"
    assert captured["count"] == "100"
    assert "description" in captured["fields"]

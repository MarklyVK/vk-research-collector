"""Асинхронный клиент минимальной части VK API."""

import asyncio
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping, Sequence
from typing import Any

import httpx

from .errors import VKAPIError, VKRetryExhausted
from .models import VKGroup, VKSearchPage
from .tokens import TokenPool

Sleep = Callable[[float], Awaitable[None]]

AUTH_ERRORS = frozenset({5, 27, 28})
RATE_LIMIT_ERRORS = frozenset({6, 29})
FLOOD_ERRORS = frozenset({9})
RETRYABLE_ERRORS = frozenset({1, 10})
PERMISSION_ERROR = 7
INVALID_PARAMS_ERROR = 100


class VKClient:
    def __init__(
        self,
        token_pool: TokenPool,
        *,
        api_version: str = "5.199",
        timeout: float = 30.0,
        http_client: httpx.AsyncClient | None = None,
        sleep: Sleep = asyncio.sleep,
        retry_delays: Sequence[float] = (60, 300, 900, 3600, 21600),
        cooldown_seconds: float = 60.0,
    ) -> None:
        self._pool = token_pool
        self._api_version = api_version
        self._sleep = sleep
        self._retry_delays = tuple(retry_delays)
        self._cooldown_seconds = cooldown_seconds
        self._http = http_client or httpx.AsyncClient(
            base_url="https://api.vk.com/method/", timeout=timeout
        )
        self._owns_http = http_client is None

    async def aclose(self) -> None:
        if self._owns_http:
            await self._http.aclose()

    async def call(
        self, method: str, params: Mapping[str, str | int]
    ) -> dict[str, Any]:
        """Вызвать метод, применяя политику ошибок без утечки токена."""
        retry_index = 0
        while True:
            token = await self._pool.acquire()
            try:
                response = await self._http.post(
                    method,
                    data={**params, "access_token": token, "v": self._api_version},
                )
                response.raise_for_status()
                body: dict[str, Any] = response.json()
            except (
                httpx.TimeoutException,
                httpx.NetworkError,
                httpx.HTTPStatusError,
            ) as exc:
                if retry_index >= len(self._retry_delays):
                    raise VKRetryExhausted("VK API недоступен после повторов") from exc
                await self._sleep(self._retry_delays[retry_index])
                retry_index += 1
                continue

            error = body.get("error")
            if not isinstance(error, dict):
                result = body.get("response")
                if not isinstance(result, dict):
                    raise VKAPIError(-1, "Некорректная структура ответа")
                return result
            code = int(error.get("error_code", -1))
            message = str(error.get("error_msg", "неизвестная ошибка"))
            if code in AUTH_ERRORS:
                await self._pool.disable(token)
                continue
            if code in RATE_LIMIT_ERRORS:
                await self._pool.cooldown(token, self._cooldown_seconds)
                continue
            if code in FLOOD_ERRORS:
                await self._pool.cooldown(token, self._cooldown_seconds * 2)
                continue
            if code in RETRYABLE_ERRORS:
                if retry_index >= len(self._retry_delays):
                    raise VKRetryExhausted(
                        f"VK API продолжает возвращать ошибку {code}"
                    )
                await self._sleep(self._retry_delays[retry_index])
                retry_index += 1
                continue
            if code in {PERMISSION_ERROR, INVALID_PARAMS_ERROR}:
                raise VKAPIError(code, message)
            raise VKAPIError(code, message)

    async def search_page(
        self,
        query: str,
        *,
        offset: int = 0,
        count: int = 1000,
        group_type: str = "group",
    ) -> VKSearchPage:
        response = await self.call(
            "groups.search",
            {
                "q": query,
                "offset": offset,
                "count": count,
                "type": group_type,
                "fields": "description,status",
            },
        )
        raw_items = response.get("items", [])
        if not isinstance(raw_items, list):
            raise VKAPIError(-1, "Некорректный список групп")
        items = tuple(
            VKGroup.from_api(item)
            for item in raw_items
            if isinstance(item, dict)
            and not item.get("is_closed")
            and not item.get("deactivated")
        )
        return VKSearchPage(total=int(response.get("count", 0)), items=items)

    async def iter_search(
        self,
        query: str,
        *,
        start_offset: int = 0,
        page_size: int = 1000,
        group_type: str = "group",
    ) -> AsyncIterator[tuple[int, VKSearchPage]]:
        """Получить все доступные страницы, возвращая offset следующей страницы."""
        offset = start_offset
        while True:
            page = await self.search_page(
                query, offset=offset, count=page_size, group_type=group_type
            )
            consumed = min(page_size, max(0, page.total - offset))
            next_offset = offset + consumed
            yield next_offset, page
            if consumed == 0 or next_offset >= page.total:
                break
            offset = next_offset

"""Асинхронный клиент минимальной части VK API."""

import asyncio
import random
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping, Sequence
from typing import Any

import httpx

from .errors import VKAPIError, VKRetryExhausted
from .models import VKGroup, VKSearchPage, VKSubscriptionIDsPage
from .tokens import TokenPool

Sleep = Callable[[float], Awaitable[None]]
Jitter = Callable[[float], float]

AUTH_ERRORS = frozenset({5, 27, 28})
GLOBAL_RATE_LIMIT_ERRORS = frozenset({6})
METHOD_LIMIT_ERRORS = frozenset({9, 29})
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
        retry_delays: Sequence[float] = (1, 3, 10),
        jitter: Jitter | None = None,
    ) -> None:
        self._pool = token_pool
        self._api_version = api_version
        self._sleep = sleep
        self._retry_delays = tuple(retry_delays)
        self._jitter = jitter or (lambda delay: random.uniform(0.9, 1.1) * delay)
        self._http = http_client or httpx.AsyncClient(
            base_url="https://api.vk.com/method/", timeout=timeout
        )
        self._owns_http = http_client is None

    async def aclose(self) -> None:
        if self._owns_http:
            await self._http.aclose()

    async def is_method_available(self, method: str) -> bool:
        """Проверить endpoint-aware доступность без выполнения запроса."""
        return await self._pool.is_method_available(method)

    async def next_method_available_at(self, method: str) -> float | None:
        """Вернуть ближайший момент доступности метода по clock пула."""
        return await self._pool.next_available_at(method)

    async def call(self, method: str, params: Mapping[str, str | int]) -> Any:
        """Вызвать метод, применяя политику ошибок без утечки токена."""
        retry_index = 0
        while True:
            lease = await self._pool.acquire(method)
            try:
                response = await self._http.post(
                    method,
                    data={**params, "access_token": lease.token, "v": self._api_version},
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
                await self._sleep(self._jitter(self._retry_delays[retry_index]))
                retry_index += 1
                continue

            error = body.get("error")
            if not isinstance(error, dict):
                result = body.get("response")
                if not isinstance(result, (dict, list)):
                    raise VKAPIError(-1, "Некорректная структура ответа")
                await self._pool.mark_success(lease)
                return result
            code = int(error.get("error_code", -1))
            message = str(error.get("error_msg", "неизвестная ошибка"))
            if code in AUTH_ERRORS:
                await self._pool.disable(lease, f"VK auth error {code}")
                continue
            if code in GLOBAL_RATE_LIMIT_ERRORS:
                await self._pool.global_cooldown(lease)
                continue
            if code in METHOD_LIMIT_ERRORS:
                await self._pool.method_cooldown(lease, code)
                continue
            if code in RETRYABLE_ERRORS:
                if retry_index >= len(self._retry_delays):
                    raise VKRetryExhausted(f"VK API продолжает возвращать ошибку {code}")
                await self._sleep(self._jitter(self._retry_delays[retry_index]))
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
        if not isinstance(response, dict):
            raise VKAPIError(-1, "Некорректный ответ поиска групп")
        raw_items = response.get("items", [])
        if not isinstance(raw_items, list):
            raise VKAPIError(-1, "Некорректный список групп")
        typed_items = [item for item in raw_items if isinstance(item, dict)]
        deleted_count = sum(bool(item.get("deactivated")) for item in typed_items)
        private_count = sum(
            bool(item.get("is_closed")) and not bool(item.get("deactivated"))
            for item in typed_items
        )
        items = tuple(
            VKGroup.from_api(item)
            for item in typed_items
            if not item.get("is_closed") and not item.get("deactivated")
        )
        return VKSearchPage(
            total=int(response.get("count", 0)),
            items=items,
            raw_count=len(typed_items),
            private_count=private_count,
            deleted_count=deleted_count,
        )

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

    async def get_groups(self, group_ids: Sequence[int]) -> list[dict[str, Any]]:
        """Получить только публичные поля групп."""
        response = await self.call(
            "groups.getById",
            {
                "group_ids": ",".join(str(value) for value in group_ids),
                "fields": "description,status,screen_name,members_count,is_closed,deactivated",
            },
        )
        if isinstance(response, list):
            return [item for item in response if isinstance(item, dict)]
        if isinstance(response, dict):
            raw = response.get("groups", response.get("items", []))
            if isinstance(raw, list):
                return [item for item in raw if isinstance(item, dict)]
        raise VKAPIError(-1, "Некорректный ответ groups.getById")

    async def get_wall_page(self, group_vk_id: int, offset: int, count: int) -> dict[str, Any]:
        response = await self.call(
            "wall.get", {"owner_id": -group_vk_id, "offset": offset, "count": count}
        )
        if not isinstance(response, dict):
            raise VKAPIError(-1, "Некорректный ответ wall.get")
        return response

    async def get_user_wall_page(self, user_vk_id: int, offset: int, count: int) -> dict[str, Any]:
        """Получить страницу постов со стены пользователя (owner_id положительный)."""
        response = await self.call(
            "wall.get", {"owner_id": user_vk_id, "offset": offset, "count": count}
        )
        if not isinstance(response, dict):
            raise VKAPIError(-1, "Некорректный ответ wall.get")
        return response

    async def get_members_page(self, group_vk_id: int, offset: int, count: int) -> dict[str, Any]:
        response = await self.call(
            "groups.getMembers", {"group_id": group_vk_id, "offset": offset, "count": count}
        )
        if not isinstance(response, dict):
            raise VKAPIError(-1, "Некорректный ответ groups.getMembers")
        return response

    async def get_users(self, user_ids: Sequence[int]) -> list[dict[str, Any]]:
        response = await self.call(
            "users.get",
            {
                "user_ids": ",".join(str(value) for value in user_ids),
                "fields": (
                    "screen_name,is_closed,can_access_closed,deactivated,"
                    "sex,bdate,city,education,universities,relation,followers_count,counters"
                ),
            },
        )
        if not isinstance(response, list):
            raise VKAPIError(-1, "Некорректный ответ users.get")
        return [item for item in response if isinstance(item, dict)]

    async def get_subscriptions_page(
        self, user_vk_id: int, offset: int, count: int
    ) -> dict[str, Any]:
        response = await self.call(
            "groups.get",
            {
                "user_id": user_vk_id,
                "offset": offset,
                "count": count,
                "extended": 1,
                "fields": "description,status,screen_name,members_count",
            },
        )
        if not isinstance(response, dict):
            raise VKAPIError(-1, "Некорректный ответ groups.get")
        return response

    async def get_subscription_ids_page(
        self, user_vk_id: int, offset: int, count: int
    ) -> VKSubscriptionIDsPage:
        """Получить только ID сообществ без расширенных group-объектов."""
        response = await self.call(
            "groups.get",
            {
                "user_id": user_vk_id,
                "offset": offset,
                "count": count,
                "extended": 0,
            },
        )
        if not isinstance(response, dict):
            raise VKAPIError(-1, "Некорректный ответ groups.get: ожидался объект")
        raw_total = response.get("count")
        raw_items = response.get("items")
        if (
            not isinstance(raw_total, int)
            or isinstance(raw_total, bool)
            or raw_total < 0
            or not isinstance(raw_items, list)
        ):
            raise VKAPIError(-1, "Некорректный ответ groups.get: count/items")
        group_ids: set[int] = set()
        for value in raw_items:
            if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
                raise VKAPIError(-1, "Некорректный ID сообщества в groups.get")
            group_ids.add(value)
        returned_count = len(raw_items)
        return VKSubscriptionIDsPage(
            total_reported=raw_total,
            group_ids=tuple(sorted(group_ids)),
            offset=offset,
            requested_count=count,
            returned_count=returned_count,
            next_offset=offset + returned_count,
        )

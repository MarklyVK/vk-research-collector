"""Асинхронный клиент минимальной части VK API."""

import asyncio
import logging
import random
from collections import Counter
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping, Sequence
from datetime import datetime, time, timedelta
from typing import Any
from zoneinfo import ZoneInfo

import httpx

from .budget import DailyBudget
from .errors import VKAPIError, VKRetryExhausted
from .models import VKGroup, VKSearchPage, VKSubscriptionIDsPage
from .tokens import TokenPool

logger = logging.getLogger(__name__)

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
        budget: DailyBudget | None = None,
        stop_token_on_quota_error: bool = False,
    ) -> None:
        self._pool = token_pool
        self._budget = budget
        self._stop_token_on_quota = stop_token_on_quota_error
        self._api_version = api_version
        self._sleep = sleep
        self._retry_delays = tuple(retry_delays)
        self._jitter = jitter or (lambda delay: random.uniform(0.9, 1.1) * delay)
        self._http = http_client or httpx.AsyncClient(
            base_url="https://api.vk.com/method/", timeout=timeout
        )
        self._owns_http = http_client is None
        # вызовы по токенам (первые 6 символов отпечатка) — для лога такта панели
        self._token_calls: Counter[str] = Counter()

    def take_token_calls(self) -> dict[str, int]:
        """Вернуть и обнулить число вызовов по токенам (отпечаток[:6]) с прошлого вызова."""
        out = dict(self._token_calls)
        self._token_calls.clear()
        return out

    async def aclose(self) -> None:
        if self._owns_http:
            await self._http.aclose()

    async def is_method_available(self, method: str) -> bool:
        """Проверить endpoint-aware доступность без выполнения запроса."""
        return await self._pool.is_method_available(method)

    async def next_method_available_at(self, method: str) -> float | None:
        """Вернуть ближайший момент доступности метода по clock пула."""
        return await self._pool.next_available_at(method)

    async def call(
        self,
        method: str,
        params: Mapping[str, str | int],
        *,
        inner_calls: Mapping[str, int] | None = None,
        lease_key: str | None = None,
        return_errors: bool = False,
    ) -> Any:
        """Вызвать метод, применяя политику ошибок без утечки токена.

        ``inner_calls`` — вызовы методов внутри ``execute`` (для суточного бюджета токена).
        ``lease_key`` — ключ выбора токена в пуле вместо имени метода (например
        ``execute:wall.getById``): так ошибка 9/29 внутреннего метода закрывает токен только
        для таких ``execute``, а не для всех.
        ``return_errors`` — вернуть пару (response, execute_errors) вместо response.
        """
        calls = {method: 1, **dict(inner_calls or {})}
        retry_index = 0
        while True:
            lease = await self._pool.acquire(lease_key or method)
            self._token_calls[lease.fingerprint[:6]] += 1
            if self._budget is not None:
                over = await self._budget.exceeded(lease.fingerprint, calls)
                if over:
                    # Доля суточного лимита исчерпана на этом токене — закрыть метод до
                    # следующих суток, пул выберет другой токен или сообщит о недоступности.
                    reset = self._budget.next_reset()
                    seconds = (reset - datetime.now(reset.tzinfo)).total_seconds()
                    await self._pool.block_method_for(lease, seconds)
                    continue
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
                if self._budget is not None:
                    await self._budget.record(lease.fingerprint, calls)
                inner_errors = body.get("execute_errors") if method == "execute" else None
                if inner_errors:
                    await self._inner_errors(lease, inner_errors)
                if return_errors:
                    return result, inner_errors if isinstance(inner_errors, list) else []
                return result
            if self._budget is not None:
                await self._budget.record(lease.fingerprint, calls)
            code = int(error.get("error_code", -1))
            message = str(error.get("error_msg", "неизвестная ошибка"))
            if code in AUTH_ERRORS:
                await self._pool.disable(lease, f"VK auth error {code}")
                continue
            if code in GLOBAL_RATE_LIMIT_ERRORS:
                await self._pool.global_cooldown(lease)
                continue
            if code == 29 and self._stop_token_on_quota:
                # Решение 08.11: первая ошибка 29 — токен целиком стоп до следующих суток (МСК).
                if self._budget is not None:
                    # одни и те же часы для «сейчас» и для границы суток
                    reset = self._budget.next_reset()
                    now = self._budget.now()
                else:
                    reset = _next_moscow_midnight()
                    now = datetime.now(reset.tzinfo)
                seconds = (reset - now).total_seconds()
                logger.warning(
                    "vk_token_stopped_until_next_day token=%s method=%s until=%s vk_error=29",
                    lease.fingerprint[:6],
                    method,
                    reset.isoformat(),
                )
                await self._pool.global_cooldown(lease, max(60.0, seconds))
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

    async def _inner_errors(self, lease: Any, errors: Any) -> None:
        """Ошибки вызовов внутри execute (VK кладёт их в execute_errors, сам execute успешен).

        Каждая логируется с отпечатком токена. Ошибка 9 (flood control) или 29 (лимит) на
        внутреннем методе закрывает этот токен до следующих суток (МСК) для данного
        ``lease_key`` — пул выберет другой токен (решение Андрея 12.11.2026).
        """
        if not isinstance(errors, list):
            return
        stop: set[int] = set()
        for err in errors:
            if not isinstance(err, dict):
                continue
            code = int(err.get("error_code", -1))
            logger.warning(
                "vk_execute_inner_error token=%s lease=%s method=%s vk_error=%s",
                lease.fingerprint[:6],
                lease.method,
                err.get("method", "?"),
                code,
            )
            if code in (9, 29):
                stop.add(code)
        if stop:
            if self._budget is not None:
                reset, now = self._budget.next_reset(), self._budget.now()
            else:
                reset = _next_moscow_midnight()
                now = datetime.now(reset.tzinfo)
            seconds = max(60.0, (reset - now).total_seconds())
            logger.warning(
                "vk_token_method_stopped_until_next_day token=%s lease=%s until=%s vk_error=%s",
                lease.fingerprint[:6],
                lease.method,
                reset.isoformat(),
                ",".join(str(c) for c in sorted(stop)),
            )
            await self._pool.block_method_for(lease, seconds)

    async def search_page(
        self,
        query: str,
        *,
        offset: int = 0,
        count: int = 1000,
        group_type: str = "group",
        city_id: int | None = None,
    ) -> VKSearchPage:
        params: dict[str, str | int] = {
            "q": query,
            "offset": offset,
            "count": count,
            "type": group_type,
            "fields": "description,status",
        }
        if city_id is not None:
            params["city_id"] = city_id
        response = await self.call("groups.search", params)
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
        city_id: int | None = None,
    ) -> AsyncIterator[tuple[int, VKSearchPage]]:
        """Получить все доступные страницы, возвращая offset следующей страницы."""
        offset = start_offset
        while True:
            page = await self.search_page(
                query, offset=offset, count=page_size, group_type=group_type, city_id=city_id
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
                "fields": (
                    "description,status,screen_name,members_count,is_closed,deactivated,"
                    "city,country"
                ),
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


def _next_moscow_midnight() -> datetime:
    tz = ZoneInfo("Europe/Moscow")
    tomorrow = datetime.now(tz).date() + timedelta(days=1)
    return datetime.combine(tomorrow, time(0, 0), tzinfo=tz)

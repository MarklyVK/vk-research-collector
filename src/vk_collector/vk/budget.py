"""Суточный учёт вызовов VK по каждому токену и остановка метода при доле от лимита.

Считается каждый метод отдельно, включая вызовы внутри ``execute`` (вызывающий передаёт их
в ``inner_calls``): ``execute`` с 25 × ``wall.get`` = 1 вызов ``execute`` + 25 вызовов ``wall.get``.
Сутки — по ``vk_budget_timezone`` (Москва). Когда ``calls + n`` превысил бы
``fraction × limit`` — метод на этом токене закрывается до начала следующих суток.
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Mapping
from datetime import date, datetime, time, timedelta
from typing import TYPE_CHECKING
from zoneinfo import ZoneInfo

from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert

from vk_collector.database.models import VKTokenDailyUsage

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker


class DailyBudget:
    def __init__(
        self,
        limits: Mapping[str, int],
        *,
        fraction: float = 0.8,
        timezone: str = "Europe/Moscow",
        sessions: async_sessionmaker[AsyncSession] | None = None,
        now: object | None = None,
    ) -> None:
        if not 0 < fraction <= 1:
            raise ValueError("fraction должна быть в (0, 1]")
        self._limits = dict(limits)
        self._fraction = fraction
        self._tz = ZoneInfo(timezone)
        self._sessions = sessions
        self._now = now  # для тестов: callable -> aware datetime
        self._memory: dict[tuple[date, str, str], int] = defaultdict(int)

    def _current(self) -> datetime:
        if callable(self._now):
            value = self._now()
            assert isinstance(value, datetime)
            return value.astimezone(self._tz)
        return datetime.now(self._tz)

    def now(self) -> datetime:
        """Текущее время по часам бюджета (в тестах — подставные часы)."""
        return self._current()

    def today(self) -> date:
        return self._current().date()

    def next_reset(self) -> datetime:
        """Начало следующих суток в часовом поясе бюджета (aware)."""
        tomorrow = self.today() + timedelta(days=1)
        return datetime.combine(tomorrow, time(0, 0), tzinfo=self._tz)

    def cap(self, method: str) -> int | None:
        limit = self._limits.get(method)
        return None if limit is None else int(limit * self._fraction)

    async def usage(self, fingerprint: str, methods: list[str]) -> dict[str, int]:
        day = self.today()
        if self._sessions is None:
            return {m: self._memory[(day, fingerprint, m)] for m in methods}
        async with self._sessions() as session:
            rows = (
                await session.execute(
                    select(VKTokenDailyUsage.method, VKTokenDailyUsage.calls).where(
                        VKTokenDailyUsage.day == day,
                        VKTokenDailyUsage.token_fingerprint == fingerprint,
                        VKTokenDailyUsage.method.in_(methods),
                    )
                )
            ).all()
        found = {method: calls for method, calls in rows}
        return {m: int(found.get(m, 0)) for m in methods}

    async def exceeded(self, fingerprint: str, calls: Mapping[str, int]) -> list[str]:
        """Методы, которые после этих вызовов превысили бы долю лимита на этом токене."""
        limited = [m for m in calls if self.cap(m) is not None]
        if not limited:
            return []
        used = await self.usage(fingerprint, limited)
        return [m for m in limited if used[m] + calls[m] > (self.cap(m) or 0)]

    async def record(self, fingerprint: str, calls: Mapping[str, int]) -> None:
        day = self.today()
        if self._sessions is None:
            for method, n in calls.items():
                self._memory[(day, fingerprint, method)] += n
            return
        async with self._sessions() as session:
            for method, n in calls.items():
                stmt = insert(VKTokenDailyUsage).values(
                    day=day, token_fingerprint=fingerprint, method=method, calls=n
                )
                await session.execute(
                    stmt.on_conflict_do_update(
                        index_elements=["day", "token_fingerprint", "method"],
                        set_={
                            "calls": VKTokenDailyUsage.calls + stmt.excluded.calls,
                            # иначе updated_at = момент создания строки суток, а не последний вызов
                            "updated_at": func.now(),
                        },
                    )
                )
            await session.commit()

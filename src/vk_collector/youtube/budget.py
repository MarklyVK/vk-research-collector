"""Суточный учёт квоты YouTube Data API v3 в часовом поясе America/Los_Angeles."""

from __future__ import annotations

import hashlib
import logging
from collections import defaultdict
from collections.abc import Callable, Sequence
from datetime import UTC, date, datetime, time, timedelta
from typing import TYPE_CHECKING
from zoneinfo import ZoneInfo

from sqlalchemy import func, select, update
from sqlalchemy.dialects.postgresql import insert

from vk_collector.database.models import YTKeyDailyUsage

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

logger = logging.getLogger(__name__)

PT_TIMEZONE = ZoneInfo("America/Los_Angeles")

ENDPOINT_QUOTA_COSTS: dict[str, int] = {
    "search.list": 100,
    "channels.list": 1,
    "playlistItems.list": 1,
    "videos.list": 1,
    "commentThreads.list": 1,
}


def compute_key_fingerprint(api_key: str) -> str:
    """Вычислить 16-символьный SHA-256 fingerprint ключа без сохранения сырого ключа."""
    return hashlib.sha256(api_key.encode("utf-8")).hexdigest()[:16]


class YouTubeQuotaExhaustedError(RuntimeError):
    """Исключение при исчерпании суточной квоты для всех доступных API-ключей YouTube."""


class YouTubeQuotaBudget:
    """Менеджер суточного бюджета квот YouTube API v3 с ротацией ключей."""

    def __init__(
        self,
        api_keys: Sequence[str],
        *,
        daily_limit: int = 10_000,
        safety_margin: int = 200,
        sessions: async_sessionmaker[AsyncSession] | None = None,
        now: Callable[[], datetime] | None = None,
    ) -> None:
        self._api_keys = [k.strip() for k in api_keys if k.strip()]
        self._fingerprints = [compute_key_fingerprint(k) for k in self._api_keys]
        self._key_by_fp = dict(zip(self._fingerprints, self._api_keys, strict=False))
        self._daily_limit = daily_limit
        self._safety_margin = safety_margin
        self._sessions = sessions
        self._now = now
        # In-memory хранилище для тестов / работы без сессий:
        # (fingerprint, endpoint, day_pt) -> units
        self._memory_usage: dict[tuple[str, str, date], int] = defaultdict(int)
        # (fingerprint, endpoint, day_pt) -> calls
        self._memory_calls: dict[tuple[str, str, date], int] = defaultdict(int)
        # (fingerprint, day_pt) -> quota_exceeded_at
        self._memory_blocked: dict[tuple[str, date], datetime] = {}

    @property
    def key_count(self) -> int:
        return len(self._api_keys)

    def current_pt_datetime(self) -> datetime:
        """Текущее время в часовом поясе America/Los_Angeles (aware)."""
        if callable(self._now):
            val = self._now()
            return val.astimezone(PT_TIMEZONE)
        return datetime.now(PT_TIMEZONE)

    def today_pt(self) -> date:
        """Текущая учетная дата квоты YouTube (Pacific Time)."""
        return self.current_pt_datetime().date()

    def next_reset_pt(self) -> datetime:
        """Начало следующих суток (00:00:00 PT) в часовом поясе America/Los_Angeles."""
        tomorrow = self.today_pt() + timedelta(days=1)
        return datetime.combine(tomorrow, time(0, 0, 0), tzinfo=PT_TIMEZONE)

    def seconds_until_reset(self) -> float:
        """Секунды до следующего сброса квоты (до 00:01 PT)."""
        reset_time = self.next_reset_pt() + timedelta(minutes=1)
        diff = (reset_time - self.current_pt_datetime()).total_seconds()
        return max(0.0, diff)

    async def get_key_spent(self, fingerprint: str, day_pt: date) -> int:
        """Получить суммарное число израсходованных единиц квоты по ключу за день."""
        if self._sessions is None:
            return sum(
                units
                for (fp, _, day), units in self._memory_usage.items()
                if fp == fingerprint and day == day_pt
            )
        async with self._sessions() as session:
            stmt = select(func.coalesce(func.sum(YTKeyDailyUsage.units_spent), 0)).where(
                YTKeyDailyUsage.key_fingerprint == fingerprint,
                YTKeyDailyUsage.day_pt == day_pt,
            )
            spent = await session.scalar(stmt)
            return int(spent or 0)

    async def is_key_blocked(self, fingerprint: str, day_pt: date) -> bool:
        """Проверить, заблокирован ли ключ из-за HTTP 403 quotaExceeded за этот день."""
        if (fingerprint, day_pt) in self._memory_blocked:
            return True
        if self._sessions is None:
            return False
        async with self._sessions() as session:
            stmt = select(YTKeyDailyUsage.quota_exceeded_at).where(
                YTKeyDailyUsage.key_fingerprint == fingerprint,
                YTKeyDailyUsage.day_pt == day_pt,
                YTKeyDailyUsage.quota_exceeded_at.is_not(None),
            )
            exceeded = await session.scalar(stmt)
            return exceeded is not None

    async def acquire_key(self, endpoint: str, cost: int | None = None) -> tuple[str, str]:
        """Выбрать первый ключ из пула с достаточным запасом квоты.

        Возвращает:
            tuple[api_key, fingerprint]

        Исключения:
            YouTubeQuotaExhaustedError: если все ключи исчерпаны или заблокированы.
        """
        if not self._api_keys:
            raise YouTubeQuotaExhaustedError(
                "Пул API-ключей YouTube пуст. Задайте YOUTUBE_API_KEY или YOUTUBE_API_KEYS."
            )

        call_cost = cost if cost is not None else ENDPOINT_QUOTA_COSTS.get(endpoint, 1)
        day_pt = self.today_pt()
        max_allowed = self._daily_limit - self._safety_margin

        for fp in self._fingerprints:
            if await self.is_key_blocked(fp, day_pt):
                continue
            spent = await self.get_key_spent(fp, day_pt)
            if spent + call_cost <= max_allowed:
                return self._key_by_fp[fp], fp

        reset_iso = self.next_reset_pt().isoformat()
        raise YouTubeQuotaExhaustedError(
            f"Суточный лимит квоты YouTube исчерпан для всех {len(self._api_keys)} ключей "
            f"на {day_pt} (сброс в {reset_iso})."
        )

    async def record_usage(
        self,
        fingerprint: str,
        endpoint: str,
        units: int,
        calls: int = 1,
    ) -> None:
        """Зафиксировать расход единиц квоты по ключу и эндпоинту."""
        day_pt = self.today_pt()
        if self._sessions is None:
            self._memory_usage[(fingerprint, endpoint, day_pt)] += units
            self._memory_calls[(fingerprint, endpoint, day_pt)] += calls
            return

        async with self._sessions() as session:
            stmt = insert(YTKeyDailyUsage).values(
                key_fingerprint=fingerprint,
                endpoint=endpoint,
                day_pt=day_pt,
                calls_count=calls,
                units_spent=units,
            )
            upsert_stmt = stmt.on_conflict_do_update(
                index_elements=["key_fingerprint", "endpoint", "day_pt"],
                set_={
                    "calls_count": YTKeyDailyUsage.calls_count + stmt.excluded.calls_count,
                    "units_spent": YTKeyDailyUsage.units_spent + stmt.excluded.units_spent,
                    "updated_at": func.now(),
                },
            )
            await session.execute(upsert_stmt)
            await session.commit()

    async def mark_quota_exceeded(self, fingerprint: str) -> None:
        """Немедленно заблокировать ключ до конца суток PT при получении ошибки quotaExceeded."""
        day_pt = self.today_pt()
        now_utc = datetime.now(UTC)
        self._memory_blocked[(fingerprint, day_pt)] = now_utc
        logger.warning(
            "Ключ YouTube %s... получил 403 quotaExceeded и заблокирован до %s",
            fingerprint[:8],
            self.next_reset_pt().isoformat(),
        )

        if self._sessions is None:
            return

        async with self._sessions() as session:
            stmt = insert(YTKeyDailyUsage).values(
                key_fingerprint=fingerprint,
                endpoint="quota_exceeded",
                day_pt=day_pt,
                calls_count=0,
                units_spent=0,
                quota_exceeded_at=now_utc,
            )
            upsert_stmt = stmt.on_conflict_do_update(
                index_elements=["key_fingerprint", "endpoint", "day_pt"],
                set_={
                    "quota_exceeded_at": now_utc,
                    "updated_at": func.now(),
                },
            )
            await session.execute(upsert_stmt)
            # Также обновим quota_exceeded_at во всех существующих строках этого ключа за сегодня
            await session.execute(
                update(YTKeyDailyUsage)
                .where(
                    YTKeyDailyUsage.key_fingerprint == fingerprint,
                    YTKeyDailyUsage.day_pt == day_pt,
                )
                .values(quota_exceeded_at=now_utc)
            )
            await session.commit()

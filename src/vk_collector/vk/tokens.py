"""Endpoint-aware пул VK-токенов без раскрытия секретов."""

import asyncio
import hashlib
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING

from sqlalchemy import func, select, update
from sqlalchemy.dialects.postgresql import insert

from vk_collector.database.models import VKTokenMethodState, VKTokenState

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from .errors import VKMethodUnavailable, VKTokensUnavailable

Clock = Callable[[], float]
Sleep = Callable[[float], Awaitable[None]]


def load_tokens(path: str | Path) -> tuple[str, ...]:
    """Прочитать произвольное число непустых токенов, по одному на строку."""
    tokens = tuple(
        line.strip() for line in Path(path).read_text(encoding="utf-8").splitlines() if line.strip()
    )
    if not tokens:
        raise VKTokensUnavailable("Файл токенов не содержит рабочих токенов")
    return tokens


def token_fingerprint(token: str) -> str:
    """Вернуть стабильный односторонний идентификатор токена."""
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


@dataclass(frozen=True, slots=True)
class TokenLease:
    """Краткоживущая выдача; секрет исключён из repr."""

    token: str = field(repr=False)
    fingerprint: str
    method: str
    issued_at: float
    is_probe: bool = False


@dataclass(slots=True)
class _MethodState:
    blocked_until: float = 0.0
    consecutive_flood_hits: int = 0
    consecutive_quota_hits: int = 0
    last_error_code: int | None = None
    last_success_at: float | None = None
    next_probe_at: float = 0.0


@dataclass(slots=True)
class _TokenState:
    value: str = field(repr=False)
    fingerprint: str
    next_request_at: float = 0.0
    global_blocked_until: float = 0.0
    disabled: bool = False
    disabled_reason: str | None = None
    methods: dict[str, _MethodState] = field(default_factory=dict)
    limit_events: list[tuple[float, str]] = field(default_factory=list)


class TokenPool:
    """Конкурентно-безопасный round-robin пул с точными method cooldown."""

    def __init__(
        self,
        tokens: Sequence[str],
        *,
        rps: float,
        clock: Clock,
        sleep: Sleep = asyncio.sleep,
        flood_initial_cooldown: float = 3600,
        quota_initial_cooldown: float = 3600,
        max_method_cooldown: float = 86400,
        probe_seconds: float = 900,
        global_rps_cooldown: float = 60,
        escalation_window: float = 60,
        escalation_methods: int = 3,
        sessions: "async_sessionmaker[AsyncSession] | None" = None,
    ) -> None:
        clean = tuple(dict.fromkeys(token.strip() for token in tokens if token.strip()))
        if not clean:
            raise VKTokensUnavailable("Не задано ни одного токена")
        if rps <= 0:
            raise ValueError("rps должен быть больше нуля")
        if min(flood_initial_cooldown, quota_initial_cooldown, max_method_cooldown) <= 0:
            raise ValueError("cooldown должен быть больше нуля")
        self._states = [
            _TokenState(value=token, fingerprint=token_fingerprint(token)) for token in clean
        ]
        self._interval = 1.0 / rps
        self._clock = clock
        self._sleep = sleep
        self._flood_initial = flood_initial_cooldown
        self._quota_initial = quota_initial_cooldown
        self._method_max = max_method_cooldown
        self._probe_seconds = probe_seconds
        self._global_rps_cooldown = global_rps_cooldown
        self._escalation_window = escalation_window
        self._escalation_methods = escalation_methods
        self._sessions = sessions
        self._db_initialized = False
        self._lock = asyncio.Lock()
        self._cursor = 0

    def __repr__(self) -> str:
        enabled = sum(not state.disabled for state in self._states)
        return f"TokenPool(tokens={len(self._states)}, enabled={enabled})"

    async def acquire(self, method: str) -> TokenLease:
        """Получить lease токена, доступного для точного VK method."""
        if not method:
            raise ValueError("Имя VK method не может быть пустым")
        if self._sessions is not None:
            return await self._acquire_postgres(method)
        while True:
            async with self._lock:
                enabled = [state for state in self._states if not state.disabled]
                if not enabled:
                    raise VKTokensUnavailable("Все VK-токены отключены")
                now = self._clock()
                method_retry: list[float] = []
                global_retry: list[float] = []
                for distance in range(len(self._states)):
                    index = (self._cursor + distance) % len(self._states)
                    state = self._states[index]
                    if state.disabled:
                        continue
                    method_state = state.methods.get(method)
                    blocked_until = method_state.blocked_until if method_state else 0.0
                    if blocked_until > now:
                        if method_state is not None and method_state.next_probe_at <= now:
                            ready_at = max(state.next_request_at, state.global_blocked_until)
                            if ready_at <= now:
                                method_state.next_probe_at = now + self._probe_seconds
                                state.next_request_at = now + self._interval
                                self._cursor = (index + 1) % len(self._states)
                                return TokenLease(
                                    state.value, state.fingerprint, method, now, is_probe=True
                                )
                            global_retry.append(ready_at)
                            continue
                        method_retry.append(
                            min(blocked_until, method_state.next_probe_at)
                            if method_state is not None and method_state.next_probe_at > now
                            else blocked_until
                        )
                        continue
                    ready_at = max(state.next_request_at, state.global_blocked_until)
                    if ready_at <= now:
                        state.next_request_at = now + self._interval
                        self._cursor = (index + 1) % len(self._states)
                        return TokenLease(state.value, state.fingerprint, method, now)
                    global_retry.append(ready_at)
                # Если хотя бы один токен годится для method, ждём только короткий global/RPS slot.
                method_capable = any(
                    state.methods.get(method, _MethodState()).blocked_until <= now
                    or state.methods.get(method, _MethodState()).next_probe_at <= now
                    for state in enabled
                )
                if not method_capable:
                    retry_at = min(method_retry) if method_retry else None
                    raise VKMethodUnavailable(method, retry_at, None)
                wait_until = min(global_retry) if global_retry else now + self._probe_seconds
            await self._sleep(max(0.0, wait_until - self._clock()))

    async def disable(self, lease: TokenLease, reason: str = "ошибка авторизации") -> None:
        """Глобально отключить токен, сохранив только очищенную причину в state."""
        async with self._lock:
            state = self._find_lease(lease)
            state.disabled = True
            state.disabled_reason = reason[:255]
        if self._sessions is not None:
            async with self._sessions() as session:
                await session.execute(
                    update(VKTokenState)
                    .where(VKTokenState.token_fingerprint == lease.fingerprint)
                    .values(
                        disabled=True,
                        disabled_reason=reason[:255],
                        last_error_at=datetime.now(UTC),
                    )
                )
                await session.commit()

    async def global_cooldown(self, lease: TokenLease, seconds: float | None = None) -> None:
        """Применить короткий глобальный cooldown (VK code 6)."""
        async with self._lock:
            state = self._find_lease(lease)
            duration = self._global_rps_cooldown if seconds is None else seconds
            state.global_blocked_until = max(state.global_blocked_until, self._clock() + duration)
        if self._sessions is not None:
            async with self._sessions() as session:
                row = await session.get(VKTokenState, lease.fingerprint, with_for_update=True)
                if row is not None:
                    blocked = datetime.now(UTC) + timedelta(seconds=duration)
                    row.global_blocked_until = (
                        blocked
                        if row.global_blocked_until is None
                        else max(row.global_blocked_until, blocked)
                    )
                    row.last_error_at = datetime.now(UTC)
                await session.commit()

    async def method_cooldown(self, lease: TokenLease, error_code: int) -> float:
        """Заблокировать только пару token+method и вернуть blocked_until."""
        if error_code not in {9, 29}:
            raise ValueError("Method cooldown разрешён только для VK codes 9/29")
        async with self._lock:
            state = self._find_lease(lease)
            method_state = state.methods.setdefault(lease.method, _MethodState())
            if error_code == 9:
                method_state.consecutive_flood_hits += 1
                hits = method_state.consecutive_flood_hits
                initial = self._flood_initial
            else:
                method_state.consecutive_quota_hits += 1
                hits = method_state.consecutive_quota_hits
                initial = self._quota_initial
            duration = min(self._method_max, initial * (2 ** (hits - 1)))
            now = self._clock()
            method_state.blocked_until = max(method_state.blocked_until, now + duration)
            method_state.next_probe_at = min(method_state.blocked_until, now + self._probe_seconds)
            method_state.last_error_code = error_code
            state.limit_events = [
                event for event in state.limit_events if event[0] >= now - self._escalation_window
            ]
            state.limit_events.append((now, lease.method))
            if len({method for _, method in state.limit_events}) >= self._escalation_methods:
                state.global_blocked_until = max(
                    state.global_blocked_until, now + self._global_rps_cooldown
                )
            blocked_until = method_state.blocked_until
        if self._sessions is not None:
            async with self._sessions() as session:
                now_dt = datetime.now(UTC)
                token_row = await session.get(VKTokenState, lease.fingerprint, with_for_update=True)
                row = await session.scalar(
                    select(VKTokenMethodState)
                    .where(
                        VKTokenMethodState.token_fingerprint == lease.fingerprint,
                        VKTokenMethodState.method == lease.method,
                    )
                    .with_for_update()
                )
                previous_hits = (
                    row.consecutive_limit_hits
                    if row is not None and row.last_error_code == error_code
                    else 0
                )
                hits = previous_hits + 1
                initial = self._flood_initial if error_code == 9 else self._quota_initial
                duration = min(self._method_max, initial * (2 ** (hits - 1)))
                blocked_dt = now_dt + timedelta(seconds=duration)
                await session.execute(
                    insert(VKTokenMethodState)
                    .values(
                        token_fingerprint=lease.fingerprint,
                        method=lease.method,
                        blocked_until=blocked_dt,
                        next_probe_at=min(
                            blocked_dt, now_dt + timedelta(seconds=self._probe_seconds)
                        ),
                        consecutive_limit_hits=hits,
                        last_error_code=error_code,
                        last_error_at=now_dt,
                    )
                    .on_conflict_do_update(
                        constraint="uq_vk_token_method_state",
                        set_={
                            "blocked_until": blocked_dt,
                            "next_probe_at": min(
                                blocked_dt, now_dt + timedelta(seconds=self._probe_seconds)
                            ),
                            "consecutive_limit_hits": hits,
                            "last_error_code": error_code,
                            "last_error_at": now_dt,
                        },
                    )
                )
                distinct_methods = int(
                    await session.scalar(
                        select(func.count(func.distinct(VKTokenMethodState.method))).where(
                            VKTokenMethodState.token_fingerprint == lease.fingerprint,
                            VKTokenMethodState.last_error_at
                            >= now_dt - timedelta(seconds=self._escalation_window),
                            VKTokenMethodState.last_error_code.in_([9, 29]),
                        )
                    )
                    or 0
                )
                if token_row is not None and distinct_methods >= self._escalation_methods:
                    escalated_until = now_dt + timedelta(seconds=self._global_rps_cooldown)
                    token_row.global_blocked_until = (
                        escalated_until
                        if token_row.global_blocked_until is None
                        else max(token_row.global_blocked_until, escalated_until)
                    )
                    token_row.last_error_at = now_dt
                await session.commit()
                blocked_until = self._clock() + duration
        return blocked_until

    async def mark_success(self, lease: TokenLease) -> None:
        """Сбросить последовательные limit hits успешного метода."""
        async with self._lock:
            state = self._find_lease(lease)
            method_state = state.methods.setdefault(lease.method, _MethodState())
            method_state.consecutive_flood_hits = 0
            method_state.consecutive_quota_hits = 0
            method_state.last_error_code = None
            method_state.last_success_at = self._clock()
            method_state.blocked_until = 0.0
            method_state.next_probe_at = 0.0
        if self._sessions is not None:
            async with self._sessions() as session:
                now_dt = datetime.now(UTC)
                await session.execute(
                    update(VKTokenState)
                    .where(VKTokenState.token_fingerprint == lease.fingerprint)
                    .values(last_success_at=now_dt)
                )
                await session.execute(
                    update(VKTokenMethodState)
                    .where(
                        VKTokenMethodState.token_fingerprint == lease.fingerprint,
                        VKTokenMethodState.method == lease.method,
                    )
                    .values(
                        consecutive_limit_hits=0,
                        last_error_code=None,
                        blocked_until=None,
                        next_probe_at=None,
                        last_success_at=now_dt,
                    )
                )
                await session.commit()

    async def is_method_available(self, method: str) -> bool:
        """Проверить наличие хотя бы одного доступного токена без ожидания."""
        if self._sessions is not None:
            await self._ensure_postgres_states()
            async with self._sessions() as session:
                now = datetime.now(UTC)
                blocked = select(VKTokenMethodState.token_fingerprint).where(
                    VKTokenMethodState.method == method,
                    VKTokenMethodState.blocked_until > now,
                    (VKTokenMethodState.next_probe_at.is_(None))
                    | (VKTokenMethodState.next_probe_at > now),
                )
                count = await session.scalar(
                    select(VKTokenState.token_fingerprint)
                    .where(
                        VKTokenState.token_fingerprint.in_(
                            [state.fingerprint for state in self._states]
                        ),
                        VKTokenState.disabled.is_(False),
                        (VKTokenState.global_blocked_until.is_(None))
                        | (VKTokenState.global_blocked_until <= now),
                        VKTokenState.token_fingerprint.not_in(blocked),
                    )
                    .limit(1)
                )
                return count is not None
        async with self._lock:
            now_clock = self._clock()
            return any(
                not state.disabled
                and state.global_blocked_until <= now_clock
                and (
                    state.methods.get(method, _MethodState()).blocked_until <= now_clock
                    or state.methods.get(method, _MethodState()).next_probe_at <= now_clock
                )
                for state in self._states
            )

    async def next_available_at(self, method: str) -> float | None:
        """Вернуть ближайшее время доступности method или None при auth exhaustion."""
        if self._sessions is not None:
            await self._ensure_postgres_states()
            async with self._sessions() as session:
                now_dt = datetime.now(UTC)
                rows = (
                    await session.execute(
                        select(
                            VKTokenState.next_request_at,
                            VKTokenState.global_blocked_until,
                            func.least(
                                VKTokenMethodState.blocked_until,
                                VKTokenMethodState.next_probe_at,
                            ),
                        )
                        .outerjoin(
                            VKTokenMethodState,
                            (VKTokenMethodState.token_fingerprint == VKTokenState.token_fingerprint)
                            & (VKTokenMethodState.method == method),
                        )
                        .where(
                            VKTokenState.token_fingerprint.in_(
                                [state.fingerprint for state in self._states]
                            ),
                            VKTokenState.disabled.is_(False),
                        )
                    )
                ).all()
                if not rows:
                    return None
                ready = min(
                    max(value for value in row if value is not None)
                    if any(value is not None for value in row)
                    else now_dt
                    for row in rows
                )
                return self._clock() + max(0.0, (ready - now_dt).total_seconds())
        async with self._lock:
            values = [
                max(
                    state.next_request_at,
                    state.global_blocked_until,
                    min(
                        state.methods.get(method, _MethodState()).blocked_until,
                        state.methods.get(method, _MethodState()).next_probe_at,
                    )
                    if state.methods.get(method, _MethodState()).blocked_until > self._clock()
                    and state.methods.get(method, _MethodState()).next_probe_at > self._clock()
                    else state.methods.get(method, _MethodState()).blocked_until,
                )
                for state in self._states
                if not state.disabled
            ]
            return min(values) if values else None

    async def reset_method(self, method: str) -> int:
        """Сбросить только cooldown указанного метода для всех токенов."""
        async with self._lock:
            changed = 0
            for state in self._states:
                if method in state.methods:
                    state.methods.pop(method)
                    changed += 1
        if self._sessions is not None:
            async with self._sessions() as session:
                result = await session.execute(
                    update(VKTokenMethodState)
                    .where(VKTokenMethodState.method == method)
                    .values(
                        blocked_until=None,
                        next_probe_at=None,
                        consecutive_limit_hits=0,
                        last_error_code=None,
                    )
                )
                await session.commit()
                changed = max(changed, int(result.rowcount or 0))  # type: ignore[attr-defined]
        return changed

    def _find_lease(self, lease: TokenLease) -> _TokenState:
        for state in self._states:
            if state.fingerprint == lease.fingerprint and lease.method:
                return state
        raise ValueError("Lease не принадлежит пулу")

    async def _ensure_postgres_states(self) -> None:
        if self._db_initialized or self._sessions is None:
            return
        async with self._sessions() as session:
            await session.execute(
                insert(VKTokenState)
                .values([{"token_fingerprint": state.fingerprint} for state in self._states])
                .on_conflict_do_nothing(index_elements=[VKTokenState.token_fingerprint])
            )
            await session.commit()
        self._db_initialized = True

    async def _acquire_postgres(self, method: str) -> TokenLease:
        """Транзакционно зарезервировать общий per-token RPS slot в PostgreSQL."""
        if self._sessions is None:
            raise RuntimeError("PostgreSQL backend не настроен")
        await self._ensure_postgres_states()
        fingerprints = [state.fingerprint for state in self._states]
        while True:
            async with self._sessions() as session:
                rows = list(
                    (
                        await session.scalars(
                            select(VKTokenState)
                            .where(VKTokenState.token_fingerprint.in_(fingerprints))
                            .order_by(VKTokenState.token_fingerprint)
                            .with_for_update()
                        )
                    ).all()
                )
                by_fingerprint = {row.token_fingerprint: row for row in rows}
                method_rows = list(
                    (
                        await session.scalars(
                            select(VKTokenMethodState).where(
                                VKTokenMethodState.token_fingerprint.in_(fingerprints),
                                VKTokenMethodState.method == method,
                            )
                        )
                    ).all()
                )
                methods = {row.token_fingerprint: row for row in method_rows}
                now_dt = datetime.now(UTC)
                enabled = [row for row in rows if not row.disabled]
                if not enabled:
                    raise VKTokensUnavailable("Все VK-токены отключены")
                method_retry: list[datetime] = []
                global_retry: list[datetime] = []
                method_capable = False
                for distance in range(len(self._states)):
                    index = (self._cursor + distance) % len(self._states)
                    local = self._states[index]
                    row = by_fingerprint.get(local.fingerprint)
                    if row is None or row.disabled:
                        continue
                    method_row = methods.get(local.fingerprint)
                    if (
                        method_row is not None
                        and method_row.blocked_until is not None
                        and method_row.blocked_until > now_dt
                    ):
                        if (
                            method_row.next_probe_at is not None
                            and method_row.next_probe_at <= now_dt
                        ):
                            method_capable = True
                            ready_at = max(
                                value
                                for value in (row.next_request_at, row.global_blocked_until, now_dt)
                                if value is not None
                            )
                            if ready_at <= now_dt:
                                method_row.next_probe_at = now_dt + timedelta(
                                    seconds=self._probe_seconds
                                )
                                row.next_request_at = now_dt + timedelta(seconds=self._interval)
                                self._cursor = (index + 1) % len(self._states)
                                await session.commit()
                                return TokenLease(
                                    local.value,
                                    local.fingerprint,
                                    method,
                                    self._clock(),
                                    is_probe=True,
                                )
                            global_retry.append(ready_at)
                            continue
                        method_retry.append(
                            min(method_row.blocked_until, method_row.next_probe_at)
                            if method_row.next_probe_at is not None
                            else method_row.blocked_until
                        )
                        continue
                    method_capable = True
                    ready_at = max(
                        value
                        for value in (row.next_request_at, row.global_blocked_until, now_dt)
                        if value is not None
                    )
                    if ready_at <= now_dt:
                        row.next_request_at = now_dt + timedelta(seconds=self._interval)
                        self._cursor = (index + 1) % len(self._states)
                        await session.commit()
                        return TokenLease(local.value, local.fingerprint, method, self._clock())
                    global_retry.append(ready_at)
                await session.commit()
                if not method_capable:
                    delay = max(0.0, (min(method_retry) - now_dt).total_seconds())
                    raise VKMethodUnavailable(method, self._clock() + delay, None)
                wait_for = (
                    max(0.0, (min(global_retry) - now_dt).total_seconds())
                    if global_retry
                    else self._probe_seconds
                )
            await self._sleep(wait_for)

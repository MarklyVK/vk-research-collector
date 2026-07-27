"""Пул токенов с rate limit, cooldown и отключением."""

import asyncio
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from pathlib import Path

from .errors import VKTokensUnavailable

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


@dataclass(slots=True)
class _TokenState:
    value: str
    next_request_at: float = 0.0
    cooldown_until: float = 0.0
    disabled: bool = False


class TokenPool:
    """Конкурентно-безопасный round-robin пул; значения токенов не представляются в repr."""

    def __init__(
        self,
        tokens: Sequence[str],
        *,
        rps: float,
        clock: Clock,
        sleep: Sleep = asyncio.sleep,
    ) -> None:
        clean = tuple(token.strip() for token in tokens if token.strip())
        if not clean:
            raise VKTokensUnavailable("Не задано ни одного токена")
        if rps <= 0:
            raise ValueError("rps должен быть больше нуля")
        self._states = [_TokenState(value=token) for token in clean]
        self._interval = 1.0 / rps
        self._clock = clock
        self._sleep = sleep
        self._lock = asyncio.Lock()
        self._cursor = 0

    def __repr__(self) -> str:
        enabled = sum(not state.disabled for state in self._states)
        return f"TokenPool(tokens={len(self._states)}, enabled={enabled})"

    async def acquire(self) -> str:
        """Получить токен, ожидая ближайший rate-limit/cooldown при необходимости."""
        while True:
            async with self._lock:
                enabled = [state for state in self._states if not state.disabled]
                if not enabled:
                    raise VKTokensUnavailable("Все VK-токены отключены")
                now = self._clock()
                for distance in range(len(self._states)):
                    index = (self._cursor + distance) % len(self._states)
                    state = self._states[index]
                    ready_at = max(state.next_request_at, state.cooldown_until)
                    if not state.disabled and ready_at <= now:
                        state.next_request_at = now + self._interval
                        self._cursor = (index + 1) % len(self._states)
                        return state.value
                wait_for = (
                    min(max(state.next_request_at, state.cooldown_until) for state in enabled) - now
                )
            await self._sleep(max(0.0, wait_for))

    async def disable(self, token: str) -> None:
        async with self._lock:
            self._find(token).disabled = True

    async def cooldown(self, token: str, seconds: float) -> None:
        async with self._lock:
            state = self._find(token)
            state.cooldown_until = max(state.cooldown_until, self._clock() + seconds)

    def _find(self, token: str) -> _TokenState:
        for state in self._states:
            if state.value == token:
                return state
        raise ValueError("Токен не принадлежит пулу")

from __future__ import annotations

import json
import logging
import re
import time
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Callable, Mapping
from typing import Any

LOGGER = logging.getLogger(__name__)
TELEGRAM_MESSAGE_LIMIT = 4000
_TOKEN_PATTERN = re.compile(r"\b\d{5,}:[A-Za-z0-9_-]{20,}\b")
_BOT_URL_PATTERN = re.compile(r"https://api\.telegram\.org/bot[^/\s]+")

UrlOpen = Callable[..., Any]
Sleeper = Callable[[float], None]


class TelegramAPIError(RuntimeError):
    """Безопасная ошибка Telegram API, не содержащая URL с token."""


def redact_secrets(value: str) -> str:
    """Удалить Telegram token и bot URL из диагностического текста."""
    return _TOKEN_PATTERN.sub("[СКРЫТО]", _BOT_URL_PATTERN.sub("[СКРЫТО]", value))


def request_json(
    token: str,
    method: str,
    fields: Mapping[str, str] | None = None,
    *,
    timeout: float = 10.0,
    attempts: int = 3,
    urlopen: UrlOpen = urllib.request.urlopen,
    sleeper: Sleeper = time.sleep,
) -> dict[str, Any]:
    """Вызвать официальный Bot API с retry/backoff и безопасными ошибками."""
    if not token:
        raise TelegramAPIError("Telegram token не задан")
    if attempts < 1:
        raise ValueError("attempts должен быть положительным")
    body = urllib.parse.urlencode(dict(fields or {})).encode("utf-8")
    request = urllib.request.Request(
        f"https://api.telegram.org/bot{token}/{method}",
        data=body,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        method="POST",
    )
    last_error = "неизвестная ошибка"
    for attempt in range(1, attempts + 1):
        retry_after = min(2 ** (attempt - 1), 30)
        try:
            with urlopen(request, timeout=timeout) as response:
                raw = response.read(256 * 1024)
                status = int(getattr(response, "status", 200))
            payload = json.loads(raw.decode("utf-8"))
            if not isinstance(payload, dict):
                raise TelegramAPIError("Telegram API вернул некорректный JSON")
            if status == 429 or not payload.get("ok", False):
                parameters = payload.get("parameters")
                if isinstance(parameters, dict):
                    candidate = parameters.get("retry_after")
                    if isinstance(candidate, int):
                        retry_after = min(max(candidate, 1), 60)
                description = str(payload.get("description", "ok=false"))
                last_error = f"HTTP {status}, {redact_secrets(description)[:200]}"
                if status != 429 and status < 500:
                    raise TelegramAPIError(f"Telegram API отклонил запрос: {last_error}")
            else:
                return payload
        except urllib.error.HTTPError as exc:
            status = exc.code
            retry_after = min(2 ** (attempt - 1), 30)
            try:
                payload = json.loads(exc.read(64 * 1024).decode("utf-8"))
            except (json.JSONDecodeError, UnicodeDecodeError):
                payload = {}
            if isinstance(payload, dict):
                parameters = payload.get("parameters")
                if isinstance(parameters, dict) and isinstance(parameters.get("retry_after"), int):
                    retry_after = min(max(int(parameters["retry_after"]), 1), 60)
            last_error = f"HTTP {status}"
            if status < 500 and status != 429:
                raise TelegramAPIError(f"Telegram API отклонил запрос: {last_error}") from None
        except (TimeoutError, urllib.error.URLError) as exc:
            reason = getattr(exc, "reason", exc)
            last_error = redact_secrets(type(reason).__name__)
        except (json.JSONDecodeError, UnicodeDecodeError):
            last_error = "некорректный JSON"
        if attempt < attempts:
            LOGGER.warning(
                "Telegram send failed: %s, retry in %s seconds",
                last_error,
                retry_after,
            )
            sleeper(float(retry_after))
    raise TelegramAPIError(f"Telegram API недоступен после {attempts} попыток: {last_error}")


def send_message(
    token: str,
    chat_id: str,
    message: str,
    *,
    parse_mode: str = "HTML",
    timeout: float = 10.0,
    attempts: int = 3,
    urlopen: UrlOpen = urllib.request.urlopen,
    sleeper: Sleeper = time.sleep,
) -> bool:
    """Отправить ограниченное по длине сообщение и проверить поле ok."""
    if not chat_id:
        raise TelegramAPIError("Telegram chat ID не задан")
    request_json(
        token,
        "sendMessage",
        {
            "chat_id": chat_id,
            "text": message[:TELEGRAM_MESSAGE_LIMIT],
            "parse_mode": parse_mode,
            "disable_web_page_preview": "true",
        },
        timeout=timeout,
        attempts=attempts,
        urlopen=urlopen,
        sleeper=sleeper,
    )
    return True

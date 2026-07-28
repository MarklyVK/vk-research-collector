from __future__ import annotations

import httpx

from vk_collector.config import Settings


async def notify(settings: Settings, message: str) -> bool:
    """Отправить best-effort Telegram notification без влияния на сбор."""
    token = settings.telegram_bot_token.get_secret_value()
    if not settings.telegram_enabled or not token or not settings.telegram_chat_id:
        return False
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            response = await client.post(
                f"https://api.telegram.org/bot{token}/sendMessage",
                data={"chat_id": settings.telegram_chat_id, "text": message[:4000]},
            )
            response.raise_for_status()
    except httpx.HTTPError:
        return False
    return True

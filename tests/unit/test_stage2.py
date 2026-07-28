from pathlib import Path

import httpx
import pytest
from pydantic import SecretStr

from vk_collector.collection.notifications import notify
from vk_collector.collection.safety import inspect_disk, sanitize_message
from vk_collector.collection.worker import normalize_attachment
from vk_collector.config import Settings


def test_blank_optional_limits_are_supported() -> None:
    settings = Settings(
        collection_members_max_per_group="",  # type: ignore[arg-type]
        collection_subscriptions_max_per_user="",  # type: ignore[arg-type]
    )
    assert settings.collection_members_max_per_group is None
    assert settings.collection_subscriptions_max_per_user is None


def test_secret_masking_removes_tokens_and_database_urls() -> None:
    message = (
        "access_token=vk1.secret&x=1 "
        "postgresql+asyncpg://collector:password@example/db password=top-secret"
    )
    sanitized = sanitize_message(message)
    assert "vk1.secret" not in sanitized
    assert "top-secret" not in sanitized
    assert "collector:password" not in sanitized


def test_attachment_normalization_keeps_metadata_not_binary() -> None:
    normalized = normalize_attachment(
        {
            "type": "photo",
            "photo": {
                "id": 12,
                "owner_id": -3,
                "access_key": "allowed-key",
                "sizes": [{"width": 100, "height": 50, "url": "https://binary"}],
                "raw": "must-not-survive",
            },
        },
        0,
    )
    assert normalized["vk_attachment_id"] == 12
    assert normalized["width"] == 100
    assert "raw" not in str(normalized)
    assert "https://binary" not in str(normalized)


def test_disk_thresholds_are_monotonic(tmp_path: Path) -> None:
    state = inspect_disk(tmp_path, warning_percent=0, stop_percent=0)
    assert state.warning
    assert state.stop


@pytest.mark.asyncio
async def test_telegram_failure_does_not_escape(monkeypatch: pytest.MonkeyPatch) -> None:
    class FailingClient:
        async def __aenter__(self) -> "FailingClient":
            return self

        async def __aexit__(self, *args: object) -> None:
            return None

        async def post(self, *args: object, **kwargs: object) -> httpx.Response:
            request = httpx.Request("POST", "https://api.telegram.test")
            raise httpx.ConnectError("offline", request=request)

    monkeypatch.setattr(httpx, "AsyncClient", lambda **kwargs: FailingClient())
    settings = Settings(
        telegram_enabled=True,
        telegram_bot_token=SecretStr("fake"),
        telegram_chat_id="123",
    )
    assert not await notify(settings, "test")

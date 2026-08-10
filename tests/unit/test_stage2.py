import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path

import httpx
import pytest
import yaml
from pydantic import SecretStr, ValidationError

from vk_collector.cli.app import _validated_backup_metadata
from vk_collector.collection.capacity import (
    build_capacity_report,
    validate_capacity_report,
    write_capacity_report,
)
from vk_collector.collection.notifications import notify
from vk_collector.collection.queue import CollectionQueue
from vk_collector.collection.safety import inspect_disk, sanitize_message
from vk_collector.collection.worker import normalize_attachment
from vk_collector.config import Settings


def test_blank_optional_limits_are_supported() -> None:
    settings = Settings(
        collection_members_max_per_group="",  # type: ignore[arg-type]
    )
    assert settings.collection_members_max_per_group is None
    assert settings.collection_subscriptions_max_per_user == 50


def test_subscription_limit_accepts_100_but_rejects_more() -> None:
    assert (
        Settings(collection_subscriptions_max_per_user=100).collection_subscriptions_max_per_user
        == 100
    )
    with pytest.raises(ValidationError):
        Settings(collection_subscriptions_max_per_user=101)


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


def test_collection_configuration_captures_capacity_limits() -> None:
    small = CollectionQueue(  # type: ignore[arg-type]
        None,
        Settings(collection_posts_max_per_group=100, collection_members_max_per_group=200),
    )
    large = CollectionQueue(  # type: ignore[arg-type]
        None,
        Settings(collection_posts_max_per_group=200, collection_members_max_per_group=1000),
    )
    assert small.collection_configuration() != large.collection_configuration()
    assert small.collection_configuration()["posts_max_per_group"] == 100
    assert small.collection_configuration()["members_max_per_group"] == 200
    assert small.collection_configuration()["subscription_posts_ttl_days"] == 30


def test_capacity_report_is_atomic_and_bound_to_configuration(tmp_path: Path) -> None:
    configuration: dict[str, object] = {
        "subscriptions_max_per_user": 50,
        "subscriptions_users_per_run": 10_000,
        "subscription_pilot_users": 500,
        "subscription_pilot_min_users": 100,
    }
    report = build_capacity_report(
        phase="A",
        run_id=uuid.uuid4(),
        configuration=configuration,
        limits={
            "pilot_users": 500,
            "minimum_pilot_users": 100,
            "subscriptions_per_user": 50,
            "subscriptions_preview_limit": 100,
            "production_users": 10_000,
        },
        measured={
            "duration_seconds": 1.0,
            "api_requests": 500,
            "processed_jobs": 500,
            "planned_entities": 500,
            "observed_entities": 500,
            "completed_entities": 490,
            "skipped_entities": 10,
            "failed_entities": 0,
            "database_bytes_before": 1024,
            "database_bytes_after": 2048,
            "database_growth_bytes": 1024,
            "relation_growth_bytes": 1024,
            "disk_free_bytes_after": 10_000,
        },
        projected={"database_bytes": 2048, "database_growth_bytes": 1024},
        production_allowed=True,
    )
    target = tmp_path / "gate-a.json"
    write_capacity_report(target, report)
    assert not list(tmp_path.glob("*.tmp"))
    assert (
        validate_capacity_report(target, phase="A", configuration=configuration, max_age_days=30)
        == report
    )
    with pytest.raises(ValueError, match="другой конфигурации"):
        validate_capacity_report(
            target,
            phase="A",
            configuration={
                "subscriptions_max_per_user": 100,
                "subscriptions_users_per_run": 10_000,
                "subscription_pilot_users": 500,
                "subscription_pilot_min_users": 100,
            },
            max_age_days=30,
        )


def test_capacity_report_rejects_stale_corrupt_and_theoretical_preview(tmp_path: Path) -> None:
    configuration: dict[str, object] = {
        "subscriptions_max_per_user": 50,
        "subscriptions_users_per_run": 10_000,
        "subscription_pilot_users": 500,
        "subscription_pilot_min_users": 100,
    }
    limits = {
        "subscriptions_per_user": 50,
        "pilot_users": 500,
        "minimum_pilot_users": 100,
        "subscriptions_preview_limit": 100,
        "production_users": 10_000,
    }
    measured = {
        "duration_seconds": 1,
        "api_requests": 1,
        "processed_jobs": 1,
        "planned_entities": 100,
        "observed_entities": 100,
        "completed_entities": 100,
        "skipped_entities": 0,
        "failed_entities": 0,
        "database_bytes_before": 1,
        "database_bytes_after": 2,
        "database_growth_bytes": 1,
        "relation_growth_bytes": 1,
        "disk_free_bytes_after": 10_000,
    }
    target = tmp_path / "gate-a.json"
    stale = build_capacity_report(
        phase="A",
        run_id=uuid.uuid4(),
        configuration=configuration,
        limits=limits,
        measured=measured,
        projected={"database_bytes": 1024, "database_growth_bytes": 1},
        production_allowed=True,
        measured_at=datetime.now(UTC) - timedelta(days=31),
    )
    write_capacity_report(target, stale)
    with pytest.raises(ValueError, match="устарел"):
        validate_capacity_report(target, phase="A", configuration=configuration, max_age_days=30)
    target.write_text("{broken", encoding="utf-8")
    with pytest.raises(ValueError, match="не читается"):
        validate_capacity_report(target, phase="A", configuration=configuration, max_age_days=30)
    preview = build_capacity_report(
        phase="A",
        run_id=uuid.uuid4(),
        configuration=configuration,
        limits=limits,
        measured=measured,
        projected={"database_bytes": 1024, "database_growth_bytes": 1},
        production_allowed=False,
    )
    write_capacity_report(target, preview)
    with pytest.raises(ValueError, match="не разрешает"):
        validate_capacity_report(target, phase="A", configuration=configuration, max_age_days=30)

    zero_growth = build_capacity_report(
        phase="A",
        run_id=uuid.uuid4(),
        configuration=configuration,
        limits=limits,
        measured={**measured, "database_growth_bytes": 0, "relation_growth_bytes": 0},
        projected={"database_bytes": 1024, "database_growth_bytes": 1},
        production_allowed=True,
    )
    write_capacity_report(target, zero_growth)
    with pytest.raises(ValueError, match="не разрешает"):
        validate_capacity_report(target, phase="A", configuration=configuration, max_age_days=30)

    insufficient_sample = build_capacity_report(
        phase="A",
        run_id=uuid.uuid4(),
        configuration=configuration,
        limits=limits,
        measured={
            **measured,
            "planned_entities": 99,
            "observed_entities": 99,
            "completed_entities": 99,
        },
        projected={"database_bytes": 1024, "database_growth_bytes": 1},
        production_allowed=True,
    )
    write_capacity_report(target, insufficient_sample)
    with pytest.raises(ValueError, match="не разрешает"):
        validate_capacity_report(target, phase="A", configuration=configuration, max_age_days=30)

    insufficient_disk = build_capacity_report(
        phase="A",
        run_id=uuid.uuid4(),
        configuration=configuration,
        limits=limits,
        measured={**measured, "disk_free_bytes_after": 1},
        projected={"database_bytes": 1024, "database_growth_bytes": 2},
        production_allowed=True,
    )
    write_capacity_report(target, insufficient_disk)
    with pytest.raises(ValueError, match="не разрешает"):
        validate_capacity_report(target, phase="A", configuration=configuration, max_age_days=30)


def test_verified_backup_must_remain_the_same_pg_dump(tmp_path: Path) -> None:
    backup = tmp_path / "before-subscriptions.dump"
    backup.write_bytes(b"PGDMP\x01safe-test")
    metadata = _validated_backup_metadata(backup)
    assert len(str(metadata["sha256"])) == 64
    assert _validated_backup_metadata(backup, expected=metadata) == metadata
    backup.write_bytes(b"PGDMP\x01changed-test")
    with pytest.raises(ValueError, match="изменился"):
        _validated_backup_metadata(backup, expected=metadata)


def test_compose_defines_restartable_autonomous_worker() -> None:
    compose = yaml.safe_load(Path("compose.yaml").read_text(encoding="utf-8"))
    worker = compose["services"]["collector-worker"]
    assert worker["command"] == ["collection", "worker"]
    assert worker["restart"] == "unless-stopped"
    assert any(
        str(volume).endswith(":/run/secrets/vk_tokens.txt:ro") for volume in worker["volumes"]
    )


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

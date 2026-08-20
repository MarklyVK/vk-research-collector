import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest
import yaml
from pydantic import SecretStr, ValidationError

from vk_collector.cli.app import _validate_autonomous_run, _validated_backup_metadata
from vk_collector.collection.backup import BackupVerifier
from vk_collector.collection.campaigns import (
    build_aggregate_capacity_projection,
    choose_campaign_control_action,
)
from vk_collector.collection.capacity import (
    build_capacity_report,
    validate_capacity_report,
    write_capacity_report,
)
from vk_collector.collection.notifications import notify
from vk_collector.collection.pilots import (
    choose_pilot_control_action,
    quarantine_incompatible_pilots,
    supersede_paused_capacity_campaigns,
)
from vk_collector.collection.queue import CollectionQueue
from vk_collector.collection.reporting import bounded_wakeup_delay
from vk_collector.collection.safety import DiskState, inspect_disk, sanitize_message
from vk_collector.collection.worker import normalize_attachment
from vk_collector.config import Settings
from vk_collector.vk import TokenPool, VKMethodUnavailable


def test_blank_optional_limits_are_supported() -> None:
    settings = Settings(
        collection_members_max_per_group="",  # type: ignore[arg-type]
    )
    assert settings.collection_members_max_per_group is None
    assert settings.collection_subscriptions_max_per_user == 50


@pytest.mark.asyncio
async def test_legacy_quarantine_requires_exact_confirmation() -> None:
    with pytest.raises(ValueError, match="QUARANTINE_INCOMPATIBLE_PILOTS"):
        await quarantine_incompatible_pilots(  # type: ignore[arg-type]
            None, Settings(), confirmation="yes"
        )


@pytest.mark.asyncio
async def test_capacity_campaign_supersede_requires_exact_confirmation() -> None:
    with pytest.raises(ValueError, match="SUPERSEDE_PAUSED_CAPACITY_CAMPAIGNS"):
        await supersede_paused_capacity_campaigns(None, confirmation="yes")  # type: ignore[arg-type]


def test_subscription_limit_accepts_50_but_rejects_more() -> None:
    assert (
        Settings(collection_subscriptions_max_per_user=50).collection_subscriptions_max_per_user
        == 50
    )
    with pytest.raises(ValidationError):
        Settings(collection_subscriptions_max_per_user=51)


def test_aggregate_capacity_scales_gate_a_to_full_discovery_snapshot() -> None:
    preview = {
        "snapshot_users": 20_000,
        "already_resolved_users": 0,
        "discovery_due_users": 20_000,
        "snapshot_storage_estimate": {
            "heap_bytes": 20_000 * 64,
            "primary_key_bytes": 20_000 * 48,
        },
    }
    report = {
        "limits": {"production_users": 10_000},
        "projected": {"database_growth_bytes": 200_000_000, "reserve_factor": 1.30},
    }
    rejected = build_aggregate_capacity_projection(
        preview=preview,
        report=report,
        database_bytes=7 * 1024**3 - 300_000_000,
        disk=DiskState(
            used_percent=70.0,
            warning=False,
            stop=False,
            total_bytes=10 * 1024**3,
            free_bytes=3 * 1024**3,
        ),
        warning_percent=85,
    )
    assert rejected["gate_a_target_entities"] == 10_000
    assert rejected["gate_a_projected_growth_bytes"] == 200_000_000
    assert rejected["aggregate_discovery_projected_growth_bytes"] == 400_000_000
    assert rejected["decision"] == "rejected"
    assert rejected["rejection_reasons"]

    passed = build_aggregate_capacity_projection(
        preview=preview,
        report=report,
        database_bytes=1_000_000_000,
        disk=DiskState(
            used_percent=20.0,
            warning=False,
            stop=False,
            total_bytes=10 * 1024**3,
            free_bytes=8 * 1024**3,
        ),
        warning_percent=85,
    )
    assert passed["decision"] == "passed"
    assert passed["snapshot_projected_growth_bytes"] == 2_912_000
    assert passed["aggregate_projected_growth_bytes"] == 402_912_000


def test_aggregate_capacity_never_consumes_absolute_disk_reserve() -> None:
    result = build_aggregate_capacity_projection(
        preview={
            "snapshot_users": 10_000,
            "already_resolved_users": 0,
            "discovery_due_users": 10_000,
            "snapshot_storage_estimate": {"heap_bytes": 640_000, "primary_key_bytes": 480_000},
        },
        report={
            "limits": {"production_users": 10_000},
            "projected": {"database_growth_bytes": 2 * 1024**3, "reserve_factor": 1.30},
        },
        database_bytes=2 * 1024**3,
        disk=DiskState(70.0, False, False, 10 * 1024**3, 3 * 1024**3),
        warning_percent=95,
        safe_database_limit_bytes=8 * 1024**3,
        min_free_bytes=2 * 1024**3,
    )
    assert result["available_growth_bytes"] == 1024**3
    assert result["decision"] == "rejected"
    assert result["additional_disk_required_bytes"] > 0


def test_pilot_control_decision_is_run_id_specific_and_terminal_safe() -> None:
    base = {
        "run_id": str(uuid.uuid4()),
        "classification": "compatible_recoverable",
        "nearest_retry": None,
        "scope": "subscriptions_pilot",
    }
    assert choose_pilot_control_action([base]) == {
        "action": "resume",
        "run_id": base["run_id"],
        "scope": "subscriptions_pilot",
        "reason": "compatible_recoverable",
    }
    waiting = {
        **base,
        "classification": "waiting",
        "nearest_retry": "2026-08-15T12:00:00+00:00",
    }
    assert choose_pilot_control_action([waiting])["action"] == "wait"
    stale = {**base, "classification": "stale_running_lease"}
    assert choose_pilot_control_action([stale])["action"] == "resume"
    incompatible = {**base, "classification": "incompatible_configuration"}
    assert (
        choose_pilot_control_action([incompatible, {**incompatible, "run_id": "other"}])["action"]
        == "operator_required"
    )
    terminal = {**base, "classification": "terminal"}
    assert choose_pilot_control_action([terminal])["action"] == "create"


def test_campaign_control_renews_existing_metadata_run_not_discovery() -> None:
    campaign_id = str(uuid.uuid4())
    discovery_id = str(uuid.uuid4())
    metadata_id = str(uuid.uuid4())
    rows: list[dict[str, object]] = [
        {
            "campaign_id": campaign_id,
            "campaign_status": "paused_capacity_limit",
            "compatible": True,
            "run_id": discovery_id,
            "scope": "subscription_discovery",
            "run_status": "completed",
        },
        {
            "campaign_id": campaign_id,
            "campaign_status": "paused_capacity_limit",
            "compatible": True,
            "run_id": metadata_id,
            "scope": "subscription_metadata",
            "run_status": "paused_capacity_limit",
        },
    ]
    decision = choose_campaign_control_action(rows)
    assert decision["action"] == "renew_metadata"
    assert decision["run_id"] == metadata_id
    assert decision["scope"] == "subscription_metadata"


@pytest.mark.asyncio
async def test_mixed_method_codes_choose_latest_causal_event() -> None:
    current = [100.0]

    async def no_sleep(_seconds: float) -> None:
        return None

    pool = TokenPool(
        ("token-a", "token-b"),
        rps=100,
        clock=lambda: current[0],
        sleep=no_sleep,
        flood_initial_cooldown=1000,
        quota_initial_cooldown=1000,
    )
    first = await pool.acquire("groups.get")
    await pool.method_cooldown(first, 9)
    current[0] += 1
    second = await pool.acquire("groups.get")
    await pool.method_cooldown(second, 29)
    with pytest.raises(VKMethodUnavailable) as captured:
        await pool.acquire("groups.get")
    assert captured.value.error_code == 29


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


def test_nearest_durable_wakeup_uses_bound_without_sleep() -> None:
    now = datetime(2026, 8, 15, tzinfo=UTC)
    assert bounded_wakeup_delay(now + timedelta(seconds=17), now=now, idle_seconds=5) == 17
    assert bounded_wakeup_delay(now + timedelta(hours=2), now=now, idle_seconds=5) == 60


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
    assert small.collection_configuration()["subscription_posts_enabled"] is False


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
        "deferred_entities": 0,
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


def test_backup_sha_is_read_once_per_worker_process(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    backup = tmp_path / "before-campaign.dump"
    backup.write_bytes(b"PGDMP\x01safe-test-content")
    expected = BackupVerifier().fingerprint(backup)
    verifier = BackupVerifier()
    original_open = Path.open
    reads = 0

    def counted_open(path: Path, *args: object, **kwargs: object):  # type: ignore[no-untyped-def]
        nonlocal reads
        if path == backup.resolve():
            reads += 1
        return original_open(path, *args, **kwargs)

    monkeypatch.setattr(Path, "open", counted_open)
    assert verifier.verify(backup, expected) == expected
    assert verifier.verify(backup, expected) == expected
    assert verifier.verify(backup, expected) == expected
    assert reads == 1


def test_backup_stat_change_stops_before_cached_use(tmp_path: Path) -> None:
    backup = tmp_path / "before-campaign.dump"
    backup.write_bytes(b"PGDMP\x01safe-test-content")
    expected = BackupVerifier().fingerprint(backup)
    verifier = BackupVerifier()
    verifier.verify(backup, expected)
    backup.write_bytes(b"PGDMP\x01changed-size-content")
    with pytest.raises(ValueError, match="изменился"):
        verifier.verify(backup, expected)


def test_backup_mismatch_is_not_cached_as_verified(tmp_path: Path) -> None:
    backup = tmp_path / "mismatch.dump"
    backup.write_bytes(b"PGDMP\x01same-stat-content")
    expected = BackupVerifier().fingerprint(backup)
    mismatched = {**expected, "sha256": "0" * 64}
    verifier = BackupVerifier()
    with pytest.raises(ValueError, match="изменился"):
        verifier.verify(backup, mismatched)
    assert not verifier._verified  # mismatch не должен становиться успешным cache entry
    assert verifier.verify(backup, expected) == expected


@pytest.mark.asyncio
async def test_three_scheduler_quantum_hash_backup_once(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    backup = tmp_path / "scheduler.dump"
    backup.write_bytes(b"PGDMP\x01scheduler-content")
    expected_backup = BackupVerifier().fingerprint(backup)
    settings = Settings()
    configuration = CollectionQueue(None, settings).collection_configuration()  # type: ignore[arg-type]
    run = SimpleNamespace(
        scope="subscription_discovery",
        configuration={
            "capacity_gate": "passed",
            "collection": configuration,
            "capacity_report": str(tmp_path / "gate.json"),
            "verified_backup": expected_backup,
        },
    )

    class FakeSession:
        async def __aenter__(self) -> "FakeSession":
            return self

        async def __aexit__(self, *args: object) -> None:
            return None

        async def get(self, *args: object) -> object:
            return run

    class FakeSessions:
        def __call__(self) -> FakeSession:
            return FakeSession()

    monkeypatch.setattr("vk_collector.cli.app.validate_capacity_report", lambda *a, **k: {})
    original_open = Path.open
    reads = 0

    def counted_open(path: Path, *args: object, **kwargs: object):  # type: ignore[no-untyped-def]
        nonlocal reads
        if path == backup.resolve():
            reads += 1
        return original_open(path, *args, **kwargs)

    monkeypatch.setattr(Path, "open", counted_open)
    verifier = BackupVerifier()
    claim_scopes = []
    for _ in range(3):
        claim_scopes.append(
            await _validate_autonomous_run(  # type: ignore[arg-type]
                FakeSessions(), settings, uuid.uuid4(), backup_verifier=verifier
            )
        )
    assert reads == 1
    assert claim_scopes == ["subscriptions"] * 3


def test_collection_concurrency_respects_both_worker_and_vk_caps() -> None:
    assert (
        Settings(
            collection_max_concurrency=6, vk_max_concurrency=6
        ).effective_collection_concurrency
        == 6
    )
    assert (
        Settings(
            collection_max_concurrency=8, vk_max_concurrency=4
        ).effective_collection_concurrency
        == 4
    )


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

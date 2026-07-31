from __future__ import annotations

import io
import json
import os
import urllib.error
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

from vk_collector.monitoring.telegram_api import (
    TelegramAPIError,
    redact_secrets,
    request_json,
    send_message,
)
from vk_collector.monitoring.telegram_monitor import (
    Issue,
    MonitorSettings,
    _deliver,
    _host_resources,
    evaluate_snapshot,
    format_alert,
    format_daily_report,
    load_state,
    process_alerts,
    process_daily,
)


class FakeResponse:
    def __init__(self, payload: dict[str, Any], status: int = 200) -> None:
        self.payload = payload
        self.status = status

    def __enter__(self) -> FakeResponse:
        return self

    def __exit__(self, *args: object) -> None:
        return None

    def read(self, limit: int) -> bytes:
        return json.dumps(self.payload).encode("utf-8")[:limit]


def settings(tmp_path: Path, **changes: object) -> MonitorSettings:
    defaults: dict[str, object] = {
        "root": tmp_path,
        "enabled": True,
        "token_file": tmp_path / "telegram-token",
        "chat_id": "123",
        "state_dir": tmp_path / "state",
    }
    defaults.update(changes)
    return MonitorSettings(**defaults)  # type: ignore[arg-type]


def healthy_snapshot(now: datetime) -> dict[str, Any]:
    started = (now - timedelta(days=2)).isoformat()
    return {
        "observed_at": now.isoformat(),
        "postgres": {
            "present": True,
            "service": "postgres",
            "status": "running",
            "health": "healthy",
            "restarts": 0,
            "oom_killed": False,
            "started_at": started,
        },
        "worker": {
            "present": True,
            "service": "collector-worker",
            "status": "running",
            "health": "healthy",
            "restarts": 0,
            "oom_killed": False,
            "started_at": started,
            "image": "ghcr.io/example/collector:sha-abc",
            "revision": "abc",
        },
        "database": {
            "available": True,
            "run_id": "61cf249b-aaa6-4784-ae4d-0d6aed174379",
            "run_status": "running",
            "run_error": None,
            "counts": {
                "completed": 100,
                "pending": 10,
                "running": 3,
                "skipped": 2,
                "retry_wait": 0,
            },
            "api_requests": 200,
            "rows_inserted": 5,
            "rows_updated": 1000,
            "retries": 2,
            "stale_running": 0,
            "completed_24h": 50,
            "api_24h": 80,
            "inserted_24h": 2,
            "updated_24h": 500,
            "errors_24h": 0,
            "auth_24h": 0,
            "rate_24h": 0,
            "uniqueness_constraints": 3,
            "rejected_jobs": 0,
            "active_runs": 1,
            "database_bytes": 1024**3,
            "alembic_revision": "head",
        },
        "resources": {
            "disk_percent": 50.0,
            "disk_free": 5 * 1024**3,
            "memory_total": 1024**3,
            "memory_available": 300 * 1024**2,
            "swap_total": 1024**3,
            "swap_used": 100 * 1024**2,
            "swap_percent": 10.0,
        },
        "runner": {"active": True, "enabled": True, "main_pid": 42},
        "deployment": {
            "status": "success",
            "commit_sha": "abc",
            "report_mtime": now.isoformat(),
            "backup_verified": True,
            "latest_backup_at": now.isoformat(),
            "latest_backup_size": 1000,
        },
        "tokens": {"present": True, "readable": True, "count": 1},
        "vk_tokens": {"present": True, "readable": True, "count": 3},
        "git_sha": "abc",
        "expected_alembic_head": "head",
    }


def test_formatting_escapes_dynamic_html_and_contains_daily_metrics(tmp_path: Path) -> None:
    now = datetime(2026, 7, 31, 6, 0, tzinfo=UTC)
    snapshot = healthy_snapshot(now)
    issue = Issue("test", "critical", "<ошибка & test>", "проверить <logs>")
    alert = format_alert(issue, snapshot, now)
    daily = format_daily_report(snapshot, [issue], now)
    assert "&lt;ошибка &amp; test&gt;" in alert
    assert "проверить &lt;logs&gt;" in alert
    assert "09:00 МСК" in alert
    assert "API requests за 24ч: 80" in daily
    assert "Рост DB за 24ч: недоступен" in daily
    assert "<ошибка" not in daily


@pytest.mark.parametrize(
    ("disk", "ram", "expected"),
    [
        (85.0, 300, ("resources.disk", "warning")),
        (95.0, 300, ("resources.disk", "critical")),
        (50.0, 99, ("resources.ram", "warning")),
        (50.0, 49, ("resources.ram", "critical")),
    ],
)
def test_resource_thresholds(
    tmp_path: Path,
    disk: float,
    ram: int,
    expected: tuple[str, str],
) -> None:
    now = datetime(2026, 7, 31, 6, 0, tzinfo=UTC)
    snapshot = healthy_snapshot(now)
    snapshot["resources"]["disk_percent"] = disk
    snapshot["resources"]["memory_available"] = ram * 1024**2
    issues = evaluate_snapshot(snapshot, {"alerts": {}, "metrics": {}}, settings(tmp_path), now=now)
    assert expected in {(issue.key, issue.severity) for issue in issues}


def test_disk_usage_matches_df_semantics_with_reserved_blocks(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class FileSystem:
        f_frsize = 1024
        f_bsize = 1024
        f_blocks = 1000
        f_bfree = 200
        f_bavail = 100

    monkeypatch.setattr(os, "statvfs", lambda _: FileSystem(), raising=False)
    resources = _host_resources(tmp_path)
    assert resources["disk_used"] == 800 * 1024
    assert resources["disk_free"] == 100 * 1024
    assert resources["disk_percent"] == 88.9


def test_stalled_collection_uses_persisted_progress_and_ignores_active_running_jobs(
    tmp_path: Path,
) -> None:
    now = datetime(2026, 7, 31, 6, 0, tzinfo=UTC)
    snapshot = healthy_snapshot(now)
    state = {
        "alerts": {},
        "metrics": {
            "run_id": snapshot["database"]["run_id"],
            "completed": 100,
            "api_requests": 200,
            "retries": 2,
            "progress_at": (now - timedelta(minutes=31)).isoformat(),
        },
    }
    issues = evaluate_snapshot(snapshot, state, settings(tmp_path), now=now)
    keys = {issue.key for issue in issues}
    assert "collection.stalled" in keys
    assert "collection.verify" not in keys

    snapshot["database"]["next_retry_at"] = (now + timedelta(minutes=10)).isoformat()
    snapshot["database"]["counts"]["retry_wait"] = 1
    issues = evaluate_snapshot(snapshot, state, settings(tmp_path), now=now)
    assert "collection.stalled" not in {issue.key for issue in issues}


def test_verify_errors_missing_container_database_and_runner_are_reported(tmp_path: Path) -> None:
    now = datetime(2026, 7, 31, 6, 0, tzinfo=UTC)
    snapshot = healthy_snapshot(now)
    snapshot["worker"] = {"present": False, "service": "collector-worker"}
    snapshot["database"]["rejected_jobs"] = 1
    snapshot["runner"] = {"active": False, "enabled": False, "main_pid": 0}
    issues = evaluate_snapshot(snapshot, {"alerts": {}, "metrics": {}}, settings(tmp_path), now=now)
    keys = {issue.key for issue in issues}
    assert {
        "collector-worker.absent",
        "collection.verify",
        "runner.inactive",
        "runner.disabled",
    }.issubset(keys)


def test_missing_postgres_and_database_are_reported(tmp_path: Path) -> None:
    now = datetime(2026, 7, 31, 6, 0, tzinfo=UTC)
    snapshot = healthy_snapshot(now)
    snapshot["postgres"] = {"present": False, "service": "postgres"}
    snapshot["database"] = {"available": False}
    issues = evaluate_snapshot(snapshot, {"alerts": {}, "metrics": {}}, settings(tmp_path), now=now)
    keys = {issue.key for issue in issues}
    assert {"postgres.absent", "database.unavailable"}.issubset(keys)


def test_deploy_in_progress_suppresses_only_transient_worker_and_revision_alerts(
    tmp_path: Path,
) -> None:
    now = datetime(2026, 7, 31, 6, 0, tzinfo=UTC)
    snapshot = healthy_snapshot(now)
    snapshot["worker"] = {
        "present": True,
        "service": "collector-worker",
        "status": "exited",
        "health": "unhealthy",
        "revision": "old",
    }
    snapshot["git_sha"] = "new"
    snapshot["deployment"].update(
        {"in_progress": True, "status": "failed", "backup_verified": False}
    )
    issues = evaluate_snapshot(snapshot, {"alerts": {}, "metrics": {}}, settings(tmp_path), now=now)
    keys = {issue.key for issue in issues}
    assert "collector-worker.stopped" not in keys
    assert "deployment.failed" not in keys
    assert "deployment.revision_mismatch" not in keys
    assert "backup.unverified" not in keys


def test_dedup_repeat_severity_change_and_recovery(tmp_path: Path) -> None:
    now = datetime(2026, 7, 31, 6, 0, tzinfo=UTC)
    snapshot = healthy_snapshot(now)
    state: dict[str, Any] = {"alerts": {}, "metrics": {}}
    sent: list[str] = []

    def deliver(message: str) -> bool:
        sent.append(message)
        return True

    warning = Issue("disk", "warning", "Мало места", "проверить диск")
    assert process_alerts(
        settings(tmp_path), snapshot, [warning], state, now=now, deliver=deliver
    ) == (1, 0)
    process_alerts(
        settings(tmp_path),
        snapshot,
        [warning],
        state,
        now=now + timedelta(minutes=5),
        deliver=deliver,
    )
    assert len(sent) == 1
    critical = Issue("disk", "critical", "Диск заполнен", "освободить место")
    process_alerts(
        settings(tmp_path),
        snapshot,
        [critical],
        state,
        now=now + timedelta(minutes=6),
        deliver=deliver,
    )
    assert len(sent) == 2
    process_alerts(
        settings(tmp_path),
        snapshot,
        [critical],
        state,
        now=now + timedelta(hours=3, minutes=7),
        deliver=deliver,
    )
    assert len(sent) == 3
    process_alerts(
        settings(tmp_path),
        snapshot,
        [],
        state,
        now=now + timedelta(hours=3, minutes=8),
        deliver=deliver,
    )
    assert len(sent) == 4
    assert "восстановлен" in sent[-1]
    assert state["alerts"] == {}


def test_failed_delivery_stays_pending_for_next_timer(tmp_path: Path) -> None:
    now = datetime(2026, 7, 31, 6, 0, tzinfo=UTC)
    snapshot = healthy_snapshot(now)
    state: dict[str, Any] = {"alerts": {}, "metrics": {}}
    issue = Issue("worker", "critical", "Worker недоступен", "проверить logs")
    assert process_alerts(
        settings(tmp_path),
        snapshot,
        [issue],
        state,
        now=now,
        deliver=lambda _: False,
    ) == (0, 1)
    assert state["alerts"]["worker"]["pending"] is True
    assert "last_sent" not in state["alerts"]["worker"]


def test_daily_report_only_once_per_moscow_date(tmp_path: Path) -> None:
    first = datetime(2026, 7, 30, 21, 30, tzinfo=UTC)
    second = datetime(2026, 7, 31, 20, 0, tzinfo=UTC)
    state: dict[str, Any] = {"alerts": {}, "metrics": {}}
    messages: list[str] = []

    def deliver(message: str) -> bool:
        messages.append(message)
        return True

    assert process_daily(
        settings(tmp_path),
        healthy_snapshot(first),
        [],
        state,
        now=first,
        deliver=deliver,
    )
    assert not process_daily(
        settings(tmp_path),
        healthy_snapshot(second),
        [],
        state,
        now=second,
        deliver=deliver,
    )
    assert len(messages) == 1
    assert state["last_daily_date"] == "2026-07-31"


def test_corrupt_state_is_safe(tmp_path: Path) -> None:
    path = tmp_path / "state.json"
    path.write_text("{broken", encoding="utf-8")
    state, corrupt = load_state(path)
    assert corrupt
    assert state == {"version": 1, "alerts": {}, "metrics": {}}


def test_telegram_api_success_and_ok_false() -> None:
    calls: list[str] = []

    def success(request: object, timeout: float) -> FakeResponse:
        calls.append(str(timeout))
        return FakeResponse({"ok": True, "result": {"message_id": 1}})

    assert send_message("123456:abcdefghijklmnopqrstuvwxyz", "42", "test", urlopen=success)
    assert calls == ["10.0"]

    def rejected(request: object, timeout: float) -> FakeResponse:
        return FakeResponse({"ok": False, "description": "chat not found"})

    with pytest.raises(TelegramAPIError, match="chat not found"):
        request_json(
            "123456:abcdefghijklmnopqrstuvwxyz",
            "sendMessage",
            urlopen=rejected,
        )


def test_telegram_api_429_and_timeout_retry_without_real_wait() -> None:
    attempts = 0
    sleeps: list[float] = []

    def limited(request: object, timeout: float) -> FakeResponse:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            payload = json.dumps({"ok": False, "parameters": {"retry_after": 7}}).encode()
            raise urllib.error.HTTPError(
                "https://redacted.invalid",
                429,
                "limited",
                {},
                io.BytesIO(payload),
            )
        return FakeResponse({"ok": True, "result": {}})

    assert request_json(
        "123456:abcdefghijklmnopqrstuvwxyz",
        "getMe",
        urlopen=limited,
        sleeper=sleeps.append,
    )["ok"]
    assert attempts == 2
    assert sleeps == [7.0]

    def timeout(request: object, timeout: float) -> FakeResponse:
        raise TimeoutError

    with pytest.raises(TelegramAPIError, match="3 попыток"):
        request_json(
            "123456:abcdefghijklmnopqrstuvwxyz",
            "getMe",
            urlopen=timeout,
            sleeper=sleeps.append,
        )


def test_secret_redaction() -> None:
    token = "123456:abcdefghijklmnopqrstuvwxyz"
    value = f"https://api.telegram.org/bot{token}/sendMessage token={token}"
    redacted = redact_secrets(value)
    assert token not in redacted
    assert "api.telegram.org/bot" not in redacted


def test_missing_telegram_token_or_chat_id_never_attempts_send(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    attempts: list[str] = []
    monkeypatch.setattr(
        "vk_collector.monitoring.telegram_monitor.send_message",
        lambda *args, **kwargs: attempts.append("sent") or True,
    )
    assert not _deliver(settings(tmp_path), "test")
    token_file = tmp_path / "telegram-token"
    token_file.write_text("fake-token\n", encoding="utf-8")
    assert not _deliver(settings(tmp_path, chat_id=""), "test")
    assert attempts == []

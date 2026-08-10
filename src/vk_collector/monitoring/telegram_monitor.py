from __future__ import annotations

import argparse
import html
import json
import logging
import os
import re
import shutil
import stat
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Literal
from zoneinfo import ZoneInfo

from vk_collector.monitoring.telegram_api import (
    TelegramAPIError,
    request_json,
    send_message,
)

LOGGER = logging.getLogger("vk_collector.telegram_monitor")
MOSCOW = ZoneInfo("Europe/Moscow")
RUNNER_UNIT = "actions.runner.MarklyVK-vk-research-collector.vk-collector-production-01.service"
Severity = Literal["info", "warning", "critical"]
SEVERITY_ORDER: dict[Severity, int] = {"info": 0, "warning": 1, "critical": 2}
ENV_LINE = re.compile(r"^([A-Z][A-Z0-9_]*)=(.*)$")
UUID_PATTERN = re.compile(
    r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-5][0-9a-fA-F]{3}-"
    r"[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}$"
)


@dataclass(frozen=True, slots=True)
class MonitorSettings:
    """Безопасная конфигурация production monitor."""

    root: Path
    enabled: bool
    token_file: Path
    chat_id: str
    state_dir: Path
    timezone: str = "Europe/Moscow"
    repeat_seconds: int = 10_800
    stall_minutes: int = 30
    disk_warning_percent: int = 85
    disk_critical_percent: int = 95
    disk_min_free_bytes: int = 1024**3
    ram_warning_available_mb: int = 100
    swap_warning_percent: int = 90
    collection_lease_seconds: int = 300
    collection_run_id: str = ""
    postgres_user: str = "vk_collector"
    postgres_db: str = "vk_research"

    @classmethod
    def load(cls, root: Path) -> MonitorSettings:
        """Загрузить только нужные monitor-параметры из production `.env`."""
        values = read_env(root / ".env")
        token_file = Path(
            values.get(
                "TELEGRAM_BOT_TOKEN_FILE",
                str(root / "secrets" / "telegram_bot_token.txt"),
            )
        )
        if not token_file.is_absolute():
            token_file = root / token_file
        state_dir = Path(
            values.get(
                "TELEGRAM_STATE_DIR",
                str(root / ".deploy" / "telegram-monitor"),
            )
        )
        if not state_dir.is_absolute():
            state_dir = root / state_dir
        explicit_run = values.get("TELEGRAM_COLLECTION_RUN_ID", "")
        if explicit_run and not UUID_PATTERN.fullmatch(explicit_run):
            explicit_run = ""
        return cls(
            root=root,
            enabled=parse_bool(values.get("TELEGRAM_ENABLED", "false")),
            token_file=token_file,
            chat_id=values.get("TELEGRAM_CHAT_ID", "").strip(),
            state_dir=state_dir,
            timezone=values.get("TELEGRAM_TIMEZONE", "Europe/Moscow"),
            repeat_seconds=parse_int(values, "TELEGRAM_ALERT_REPEAT_SECONDS", 10_800, 60),
            stall_minutes=parse_int(values, "TELEGRAM_STALL_MINUTES", 30, 5),
            disk_warning_percent=parse_int(
                values,
                "TELEGRAM_DISK_WARNING_PERCENT",
                parse_int(values, "DISK_WARNING_PERCENT", 85, 1),
                1,
            ),
            disk_critical_percent=parse_int(
                values,
                "TELEGRAM_DISK_CRITICAL_PERCENT",
                parse_int(values, "DISK_STOP_PERCENT", 95, 1),
                1,
            ),
            disk_min_free_bytes=parse_int(
                values,
                "TELEGRAM_DISK_MIN_FREE_MB",
                1024,
                1,
            )
            * 1024**2,
            ram_warning_available_mb=parse_int(values, "TELEGRAM_RAM_WARNING_AVAILABLE_MB", 100, 1),
            swap_warning_percent=parse_int(values, "TELEGRAM_SWAP_WARNING_PERCENT", 90, 1),
            collection_lease_seconds=parse_int(values, "COLLECTION_JOB_LEASE_SECONDS", 300, 30),
            collection_run_id=explicit_run,
            postgres_user=values.get("POSTGRES_USER", "vk_collector"),
            postgres_db=values.get("POSTGRES_DB", "vk_research"),
        )


@dataclass(frozen=True, slots=True)
class Issue:
    """Одна существенная production-проблема."""

    key: str
    severity: Severity
    problem: str
    action: str
    details: tuple[str, ...] = ()


@dataclass(slots=True)
class CommandRunner:
    """Запуск только заранее сформированных read-only команд."""

    root: Path
    timeout: float = 30.0

    def run(
        self, args: list[str], *, timeout: float | None = None
    ) -> subprocess.CompletedProcess[str]:
        """Выполнить команду без shell и вернуть очищаемый результат."""
        try:
            return subprocess.run(
                args,
                cwd=self.root,
                check=False,
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
                timeout=timeout or self.timeout,
            )
        except subprocess.TimeoutExpired:
            return subprocess.CompletedProcess(args, 124, "", "command timed out")
        except OSError:
            return subprocess.CompletedProcess(args, 127, "", "command unavailable")

    def compose(
        self, args: list[str], *, timeout: float | None = None
    ) -> subprocess.CompletedProcess[str]:
        """Выполнить Docker Compose с production-файлами."""
        command = [
            "docker",
            "compose",
            "--env-file",
            str(self.root / ".env"),
            "-f",
            str(self.root / "compose.yaml"),
            "-f",
            str(self.root / "compose.production.yaml"),
            *args,
        ]
        return self.run(command, timeout=timeout)


def read_env(path: Path) -> dict[str, str]:
    """Прочитать dotenv без исполнения shell-конструкций."""
    if not path.is_file():
        return {}
    result: dict[str, str] = {}
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        match = ENV_LINE.match(raw_line.strip())
        if not match:
            continue
        value = match.group(2).strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
            value = value[1:-1]
        result[match.group(1)] = value
    return result


def parse_bool(value: str) -> bool:
    """Разобрать безопасное логическое значение."""
    return value.strip().casefold() in {"1", "true", "yes", "on"}


def parse_int(values: dict[str, str], key: str, default: int, minimum: int) -> int:
    """Получить ограниченное целое значение либо default."""
    try:
        value = int(values.get(key, str(default)))
    except ValueError:
        return default
    return value if value >= minimum else default


def utc_now() -> datetime:
    """Вернуть текущее UTC-время с timezone."""
    return datetime.now(UTC)


def iso_utc(value: datetime) -> str:
    """Сериализовать UTC timestamp."""
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def parse_timestamp(value: object) -> datetime | None:
    """Разобрать ISO timestamp из state или Docker."""
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed.replace(tzinfo=UTC) if parsed.tzinfo is None else parsed.astimezone(UTC)


def load_state(path: Path) -> tuple[dict[str, Any], bool]:
    """Прочитать state; повреждённый JSON заменить пустым состоянием."""
    if not path.exists():
        return {"version": 1, "alerts": {}, "metrics": {}}, False
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError, UnicodeDecodeError):
        return {"version": 1, "alerts": {}, "metrics": {}}, True
    if not isinstance(payload, dict):
        return {"version": 1, "alerts": {}, "metrics": {}}, True
    alerts = payload.get("alerts")
    metrics = payload.get("metrics")
    if not isinstance(alerts, dict) or not isinstance(metrics, dict):
        return {"version": 1, "alerts": {}, "metrics": {}}, True
    return payload, False


def atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    """Атомарно сохранить state с mode 600."""
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    descriptor, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            json.dump(payload, stream, ensure_ascii=False, sort_keys=True, indent=2)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.chmod(temporary, 0o600)
        os.replace(temporary, path)
    finally:
        if temporary.exists():
            temporary.unlink()


def _safe_json(output: str) -> dict[str, Any]:
    try:
        payload = json.loads(output.strip())
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def _container_snapshot(runner: CommandRunner, service: str) -> dict[str, Any]:
    result = runner.compose(["ps", "-q", service])
    container_id = result.stdout.strip()
    if result.returncode != 0 or not container_id:
        return {"present": False, "service": service}
    inspected = runner.run(["docker", "inspect", container_id])
    if inspected.returncode != 0:
        return {"present": False, "service": service}
    try:
        rows = json.loads(inspected.stdout)
    except json.JSONDecodeError:
        return {"present": False, "service": service}
    if not isinstance(rows, list) or not rows or not isinstance(rows[0], dict):
        return {"present": False, "service": service}
    row: dict[str, Any] = rows[0]
    state_raw = row.get("State")
    config_raw = row.get("Config")
    state: dict[str, Any] = state_raw if isinstance(state_raw, dict) else {}
    config: dict[str, Any] = config_raw if isinstance(config_raw, dict) else {}
    labels_raw = config.get("Labels")
    health_raw = state.get("Health")
    labels: dict[str, Any] = labels_raw if isinstance(labels_raw, dict) else {}
    health: dict[str, Any] = health_raw if isinstance(health_raw, dict) else {}
    return {
        "present": True,
        "service": service,
        "id": str(row.get("Id", "")),
        "status": str(state.get("Status", "unknown")),
        "health": str(health.get("Status", "")),
        "restarts": int(row.get("RestartCount", 0) or 0),
        "oom_killed": bool(state.get("OOMKilled", False)),
        "started_at": str(state.get("StartedAt", "")),
        "image": str(config.get("Image", "")),
        "revision": str(labels.get("org.opencontainers.image.revision", "")),
    }


def _database_query(settings: MonitorSettings, runner: CommandRunner) -> dict[str, Any]:
    explicit = settings.collection_run_id
    explicit_clause = (
        f"WHEN r.id = '{explicit}'::uuid THEN 0 " if UUID_PATTERN.fullmatch(explicit) else ""
    )
    lease = settings.collection_lease_seconds
    sql = f"""
WITH chosen AS (
  SELECT r.*
  FROM collection_runs r
  ORDER BY CASE
    {explicit_clause}
    WHEN r.status = 'running' THEN 1
    WHEN EXISTS (
      SELECT 1 FROM collection_jobs j
      WHERE j.collection_run_id = r.id AND j.status IN ('pending', 'retry_wait')
    ) THEN 2 ELSE 3 END,
    r.created_at DESC
  LIMIT 1
), counts AS (
  SELECT j.status::text AS status, count(*)::bigint AS count
  FROM collection_jobs j JOIN chosen r ON r.id = j.collection_run_id
  GROUP BY j.status
), totals AS (
  SELECT
    coalesce(sum(j.api_requests), 0)::bigint AS api_requests,
    coalesce(sum(j.rows_inserted), 0)::bigint AS rows_inserted,
    coalesce(sum(j.rows_updated), 0)::bigint AS rows_updated,
    coalesce(sum(greatest(j.attempt_count - 1, 0)), 0)::bigint AS retries,
    max(j.finished_at) FILTER (WHERE j.status = 'completed') AS last_completed_at,
    max(j.updated_at) FILTER (WHERE j.api_requests > 0) AS last_api_at,
    min(j.next_attempt_at) FILTER (WHERE j.status = 'retry_wait') AS next_retry_at,
    count(*) FILTER (
      WHERE j.status = 'running'
      AND coalesce(j.heartbeat_at, j.locked_at, j.updated_at)
        < now() - interval '{lease} seconds'
    )::bigint AS stale_running,
    count(*) FILTER (WHERE j.finished_at >= now() - interval '24 hours'
      AND j.status = 'completed')::bigint AS completed_24h,
    coalesce(sum(j.api_requests) FILTER (
      WHERE j.finished_at >= now() - interval '24 hours'), 0)::bigint AS api_24h,
    coalesce(sum(j.rows_inserted) FILTER (
      WHERE j.finished_at >= now() - interval '24 hours'), 0)::bigint AS inserted_24h,
    coalesce(sum(j.rows_updated) FILTER (
      WHERE j.finished_at >= now() - interval '24 hours'), 0)::bigint AS updated_24h
  FROM collection_jobs j JOIN chosen r ON r.id = j.collection_run_id
), errors AS (
  SELECT
    count(*) FILTER (WHERE e.created_at >= now() - interval '24 hours')::bigint AS errors_24h,
    count(*) FILTER (WHERE e.created_at >= now() - interval '24 hours'
      AND e.error_category ~* '(auth|token)')::bigint AS auth_24h,
    count(*) FILTER (WHERE e.created_at >= now() - interval '24 hours'
      AND e.error_category ~* '(rate|limit|flood)')::bigint AS rate_24h
  FROM collection_job_errors e JOIN chosen r ON r.id = e.collection_run_id
), verify AS (
  SELECT
    (SELECT count(*) FROM pg_constraint
      WHERE convalidated
      AND conname IN (
        'uq_group_posts_owner_post',
        'uq_group_memberships_group_user',
        'uq_user_group_subscriptions'
      ))::bigint AS uniqueness_constraints,
    (SELECT count(DISTINCT j.id)
      FROM collection_jobs j
      JOIN chosen r ON r.id = j.collection_run_id
      JOIN group_candidates g ON j.entity_type = 'group' AND j.entity_id = g.id
      WHERE g.classification_status = 'rejected')::bigint AS rejected_jobs
), active AS (
  SELECT count(*)::bigint AS active_runs FROM collection_runs
  WHERE status IN ('planned', 'running', 'waiting_method_limit')
), endpoint AS (
  SELECT
    (SELECT count(*) FROM vk_token_method_states
      WHERE blocked_until > now())::bigint AS blocked_token_methods,
    (SELECT min(blocked_until) FROM vk_token_method_states
      WHERE blocked_until > now()) AS next_method_retry_at,
    (SELECT count(*) FROM user_subscription_states
      WHERE privacy_denied)::bigint AS private_subscriptions,
    (SELECT count(*) FROM user_subscription_states
      WHERE last_success_at IS NOT NULL)::bigint AS subscription_users,
    (SELECT count(*) FROM user_group_subscriptions)::bigint AS subscription_links,
    (SELECT count(*) FROM vk_communities)::bigint AS communities,
    (SELECT count(DISTINCT community_vk_id) FROM group_posts)::bigint AS communities_with_posts,
    (SELECT count(*) FROM collection_jobs
      WHERE job_type = 'collect_subscription_group_posts'
      AND status IN ('pending', 'retry_wait'))::bigint AS subscription_post_jobs_pending,
    (SELECT count(*) FROM collection_jobs
      WHERE job_type = 'collect_subscription_group_posts'
      AND status = 'completed')::bigint AS subscription_post_jobs_completed,
    (SELECT count(*) FROM collection_jobs
      WHERE job_type = 'collect_subscription_group_posts'
      AND status = 'skipped')::bigint AS subscription_post_jobs_skipped
)
SELECT json_build_object(
  'run_id', r.id::text,
  'run_status', r.status::text,
  'run_error', r.error_message,
  'run_created_at', r.created_at,
  'counts', coalesce((SELECT json_object_agg(status, count) FROM counts), '{{}}'::json),
  'api_requests', t.api_requests,
  'rows_inserted', t.rows_inserted,
  'rows_updated', t.rows_updated,
  'retries', t.retries,
  'last_completed_at', t.last_completed_at,
  'last_api_at', t.last_api_at,
  'next_retry_at', t.next_retry_at,
  'stale_running', t.stale_running,
  'completed_24h', t.completed_24h,
  'api_24h', t.api_24h,
  'inserted_24h', t.inserted_24h,
  'updated_24h', t.updated_24h,
  'errors_24h', e.errors_24h,
  'auth_24h', e.auth_24h,
  'rate_24h', e.rate_24h,
  'uniqueness_constraints', v.uniqueness_constraints,
  'rejected_jobs', v.rejected_jobs,
  'active_runs', a.active_runs,
  'blocked_token_methods', x.blocked_token_methods,
  'next_method_retry_at', x.next_method_retry_at,
  'private_subscriptions', x.private_subscriptions,
  'subscription_users', x.subscription_users,
  'subscription_links', x.subscription_links,
  'communities', x.communities,
  'communities_with_posts', x.communities_with_posts,
  'subscription_post_jobs_pending', x.subscription_post_jobs_pending,
  'subscription_post_jobs_completed', x.subscription_post_jobs_completed,
  'subscription_post_jobs_skipped', x.subscription_post_jobs_skipped,
  'database_bytes', pg_database_size(current_database()),
  'alembic_revision', (SELECT version_num FROM alembic_version LIMIT 1)
)
FROM chosen r CROSS JOIN totals t CROSS JOIN errors e CROSS JOIN verify v
CROSS JOIN active a CROSS JOIN endpoint x;
"""
    result = runner.compose(
        [
            "exec",
            "-T",
            "postgres",
            "psql",
            "-X",
            "-A",
            "-t",
            "-v",
            "ON_ERROR_STOP=1",
            "-U",
            settings.postgres_user,
            "-d",
            settings.postgres_db,
            "-c",
            " ".join(sql.split()),
        ],
        timeout=45,
    )
    if result.returncode != 0:
        return {"available": False, "error": "PostgreSQL query failed"}
    payload = _safe_json(result.stdout)
    payload["available"] = bool(payload)
    return payload


def _expected_alembic_head(root: Path) -> str:
    revisions: list[str] = []
    for path in (root / "alembic" / "versions").glob("*.py"):
        match = re.search(
            r'^revision:\s*str\s*=\s*["\']([^"\']+)["\']',
            path.read_text(encoding="utf-8"),
            re.MULTILINE,
        )
        if match:
            revisions.append(match.group(1))
    return sorted(revisions)[-1] if revisions else ""


def _host_resources(root: Path) -> dict[str, Any]:
    disk = shutil.disk_usage(root)
    disk_total = disk.total
    disk_used = disk.used
    disk_free = disk.free
    disk_percent = round(disk.used * 100 / disk.total, 1) if disk.total else 0.0
    statvfs = getattr(os, "statvfs", None)
    if callable(statvfs):
        filesystem = statvfs(root)
        fragment_size = int(filesystem.f_frsize or filesystem.f_bsize)
        disk_total = int(filesystem.f_blocks) * fragment_size
        disk_used = (int(filesystem.f_blocks) - int(filesystem.f_bfree)) * fragment_size
        disk_free = int(filesystem.f_bavail) * fragment_size
        usable = disk_used + disk_free
        disk_percent = round(disk_used * 100 / usable, 1) if usable else 0.0
    memory: dict[str, int] = {}
    try:
        for line in Path("/proc/meminfo").read_text(encoding="utf-8").splitlines():
            key, raw = line.split(":", 1)
            value = raw.strip().split()[0]
            memory[key] = int(value) * 1024
    except (OSError, ValueError, IndexError):
        pass
    swap_total = memory.get("SwapTotal", 0)
    swap_free = memory.get("SwapFree", 0)
    return {
        "disk_total": disk_total,
        "disk_used": disk_used,
        "disk_free": disk_free,
        "disk_percent": disk_percent,
        "memory_total": memory.get("MemTotal", 0),
        "memory_available": memory.get("MemAvailable", 0),
        "swap_total": swap_total,
        "swap_used": max(0, swap_total - swap_free),
        "swap_percent": (
            round((swap_total - swap_free) * 100 / swap_total, 1) if swap_total else 0.0
        ),
    }


def _runner_snapshot(runner: CommandRunner) -> dict[str, Any]:
    active = runner.run(["systemctl", "is-active", RUNNER_UNIT])
    enabled = runner.run(["systemctl", "is-enabled", RUNNER_UNIT])
    properties = runner.run(
        [
            "systemctl",
            "show",
            RUNNER_UNIT,
            "--property=MainPID",
            "--property=ActiveEnterTimestamp",
        ]
    )
    parsed: dict[str, str] = {}
    for line in properties.stdout.splitlines():
        if "=" in line:
            key, value = line.split("=", 1)
            parsed[key] = value
    pid = parsed.get("MainPID", "0")
    return {
        "active": active.stdout.strip() == "active",
        "enabled": enabled.stdout.strip() in {"enabled", "static"},
        "main_pid": int(pid) if pid.isdigit() else 0,
        "active_since": parsed.get("ActiveEnterTimestamp", ""),
    }


def _deployment_snapshot(root: Path, runner: CommandRunner) -> dict[str, Any]:
    allowed = {
        "STATUS",
        "COMMIT_SHA",
        "IMAGE",
        "IMAGE_DIGEST",
        "BACKUP_PATH",
        "ALEMBIC_REVISION",
        "DURATION_SECONDS",
    }
    raw = read_env(root / ".deploy" / "last-deployment.env")
    result: dict[str, Any] = {key.casefold(): value for key, value in raw.items() if key in allowed}
    lock_path = root / ".deploy" / "deploy.lock"
    if lock_path.exists():
        try:
            lock_check = runner.run(
                ["flock", "--nonblock", str(lock_path), "true"],
                timeout=5,
            )
            result["in_progress"] = lock_check.returncode != 0
        except (OSError, subprocess.SubprocessError):
            result["in_progress"] = False
    else:
        result["in_progress"] = False
    report = root / ".deploy" / "last-deployment.env"
    result["report_mtime"] = (
        iso_utc(datetime.fromtimestamp(report.stat().st_mtime, UTC)) if report.exists() else ""
    )
    successful = root / ".deploy" / "last-successful-deployment.env"
    result["last_successful_at"] = (
        iso_utc(datetime.fromtimestamp(successful.stat().st_mtime, UTC))
        if successful.exists()
        else (result["report_mtime"] if result.get("status") == "success" else "")
    )
    backups = sorted(
        (root / "backups").glob("predeploy-*.dump"),
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )
    if backups:
        latest = backups[0]
        result["latest_backup"] = str(latest)
        result["latest_backup_size"] = latest.stat().st_size
        result["latest_backup_at"] = iso_utc(datetime.fromtimestamp(latest.stat().st_mtime, UTC))
        result["backup_verified"] = (
            result.get("status") == "success"
            and Path(str(result.get("backup_path", ""))).name == latest.name
            and latest.stat().st_size > 0
        )
    else:
        result.update(
            {
                "latest_backup": "",
                "latest_backup_size": 0,
                "latest_backup_at": "",
                "backup_verified": False,
            }
        )
    return result


def _token_snapshot(settings: MonitorSettings) -> dict[str, Any]:
    if not settings.token_file.is_file():
        return {"present": False, "readable": False, "count": 0}
    try:
        token = settings.token_file.read_text(encoding="utf-8").strip()
    except (OSError, UnicodeDecodeError):
        return {"present": True, "readable": False, "count": 0}
    return {"present": True, "readable": True, "count": 1 if token else 0}


def _vk_token_snapshot(root: Path, runner: CommandRunner) -> dict[str, Any]:
    token_file = root / "secrets" / "vk_tokens.txt"
    if not token_file.is_file():
        return {"present": False, "readable": False, "count": 0}
    result = runner.compose(
        [
            "exec",
            "-T",
            "collector-worker",
            "python",
            "-c",
            (
                "from pathlib import Path; "
                "p=Path('/run/secrets/vk_tokens.txt'); "
                "print(sum(bool(x.strip()) for x in p.read_text(encoding='utf-8').splitlines()))"
            ),
        ]
    )
    if result.returncode != 0 or not result.stdout.strip().isdigit():
        return {"present": True, "readable": False, "count": 0}
    return {"present": True, "readable": True, "count": int(result.stdout.strip())}


def collect_snapshot(
    settings: MonitorSettings,
    *,
    runner: CommandRunner | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Собрать read-only production snapshot."""
    current = now or utc_now()
    command_runner = runner or CommandRunner(settings.root)
    postgres = _container_snapshot(command_runner, "postgres")
    worker = _container_snapshot(command_runner, "collector-worker")
    database = (
        _database_query(settings, command_runner)
        if postgres.get("status") == "running"
        else {"available": False, "error": "PostgreSQL container is not running"}
    )
    try:
        git = command_runner.run(["git", "rev-parse", "HEAD"]).stdout.strip()
    except (OSError, subprocess.SubprocessError):
        git = ""
    snapshot = {
        "observed_at": iso_utc(current),
        "postgres": postgres,
        "worker": worker,
        "database": database,
        "resources": _host_resources(settings.root),
        "runner": _runner_snapshot(command_runner),
        "deployment": _deployment_snapshot(settings.root, command_runner),
        "tokens": _token_snapshot(settings),
        "vk_tokens": _vk_token_snapshot(settings.root, command_runner),
        "git_sha": git,
        "expected_alembic_head": _expected_alembic_head(settings.root),
    }
    return snapshot


def _counts(database: dict[str, Any]) -> dict[str, int]:
    raw = database.get("counts")
    if not isinstance(raw, dict):
        return {}
    result: dict[str, int] = {}
    for key, value in raw.items():
        try:
            result[str(key)] = int(value)
        except (TypeError, ValueError):
            continue
    return result


def _add_service_issues(issues: list[Issue], service: dict[str, Any], title: str) -> None:
    key = str(service.get("service", title))
    if not service.get("present"):
        issues.append(
            Issue(
                f"{key}.absent", "critical", f"{title} container отсутствует", "проверить Compose"
            )
        )
        return
    status = str(service.get("status", "unknown"))
    health = str(service.get("health", ""))
    if status != "running":
        issues.append(
            Issue(
                f"{key}.stopped",
                "critical",
                f"{title} не запущен: {status}",
                "проверить container logs и безопасно восстановить сервис",
            )
        )
    elif health and health != "healthy":
        issues.append(
            Issue(
                f"{key}.unhealthy",
                "critical",
                f"{title} unhealthy: {health}",
                "проверить healthcheck и container logs",
            )
        )
    if bool(service.get("oom_killed")):
        issues.append(
            Issue(
                f"{key}.oom",
                "critical",
                f"{title} завершался по OOM",
                "проверить RAM/swap и container limit",
            )
        )
    restarts = int(service.get("restarts", 0) or 0)
    if restarts >= 3:
        severity: Severity = "critical" if restarts >= 5 else "warning"
        issues.append(
            Issue(
                f"{key}.restarts",
                severity,
                f"{title}: restart loop ({restarts})",
                "проверить container logs и причину перезапусков",
            )
        )


def evaluate_snapshot(
    snapshot: dict[str, Any],
    state: dict[str, Any],
    settings: MonitorSettings,
    *,
    now: datetime | None = None,
    state_corrupt: bool = False,
) -> list[Issue]:
    """Определить существенные проблемы и обновить progress baseline."""
    current = now or utc_now()
    issues: list[Issue] = []
    postgres = dict(snapshot.get("postgres", {}))
    worker = dict(snapshot.get("worker", {}))
    database = dict(snapshot.get("database", {}))
    resources = dict(snapshot.get("resources", {}))
    runner = dict(snapshot.get("runner", {}))
    deploy = dict(snapshot.get("deployment", {}))
    deploy_in_progress = bool(deploy.get("in_progress"))
    tokens = dict(snapshot.get("tokens", {}))
    vk_tokens = dict(snapshot.get("vk_tokens", {}))
    _add_service_issues(issues, postgres, "PostgreSQL")
    if not deploy_in_progress:
        _add_service_issues(issues, worker, "Collector worker")

    if not database.get("available"):
        issues.append(
            Issue(
                "database.unavailable",
                "critical",
                "PostgreSQL недоступен для read-only проверки",
                "проверить pg_isready и PostgreSQL logs",
            )
        )
    else:
        counts = _counts(database)
        status = str(database.get("run_status", "absent"))
        run_id = str(database.get("run_id", "не выбран"))
        if status in {"failed", "cancelled"}:
            issues.append(
                Issue(
                    "collection.status",
                    "critical",
                    f"Collection run имеет status={status}",
                    "проверить run error и worker logs",
                    (f"Run: {run_id}",),
                )
            )
        elif status in {"paused_no_tokens", "paused_capacity_limit"}:
            issues.append(
                Issue(
                    "collection.status",
                    "critical",
                    f"Collection run имеет status={status}",
                    "восстановить tokens или capacity gate",
                    (f"Run: {run_id}",),
                )
            )
        elif status == "paused":
            issues.append(
                Issue(
                    "collection.status",
                    "warning",
                    "Collection run поставлен на паузу",
                    "подтвердить, что пауза ожидаема",
                    (f"Run: {run_id}",),
                )
            )
        elif status == "waiting_method_limit":
            issues.append(
                Issue(
                    "collection.method_limit_wait",
                    "warning",
                    "Collection run ожидает снятия endpoint limit",
                    "проверить collection method-limits и next retry",
                    (f"Run: {run_id}",),
                )
            )
        if database.get("run_error"):
            issues.append(
                Issue(
                    "collection.error",
                    "critical",
                    "Collection run содержит error_message",
                    "проверить очищенную ошибку через collection status",
                    (f"Run: {run_id}",),
                )
            )
        stale = int(database.get("stale_running", 0) or 0)
        if stale:
            issues.append(
                Issue(
                    "collection.stale_leases",
                    "critical",
                    f"Зависли jobs дольше lease: {stale}",
                    "проверить heartbeat, lease и worker logs",
                    (f"Run: {run_id}",),
                )
            )
        verify_metrics = ("rejected_jobs",)
        broken = {
            metric: int(database.get(metric, 0) or 0)
            for metric in verify_metrics
            if int(database.get(metric, 0) or 0) > 0
        }
        if broken:
            details = tuple(f"{key}: {value}" for key, value in broken.items())
            issues.append(
                Issue(
                    "collection.verify",
                    "critical",
                    "Нарушены collection invariants",
                    "запустить collection verify и остановить новые destructive действия",
                    details,
                )
            )
        uniqueness_constraints = int(database.get("uniqueness_constraints", 0) or 0)
        if uniqueness_constraints != 3:
            issues.append(
                Issue(
                    "database.uniqueness_constraints",
                    "critical",
                    "PostgreSQL uniqueness constraints неполны",
                    "проверить Alembic schema; не удалять данные автоматически",
                    (f"Найдено: {uniqueness_constraints}/3",),
                )
            )
        if int(database.get("active_runs", 0) or 0) > 1:
            issues.append(
                Issue(
                    "collection.multiple_active",
                    "warning",
                    "Одновременно найдено несколько активных collection run",
                    "проверить plan/capacity и оставить ожидаемый run",
                )
            )
        expected = str(snapshot.get("expected_alembic_head", ""))
        actual = str(database.get("alembic_revision", ""))
        if expected and actual != expected:
            issues.append(
                Issue(
                    "database.alembic",
                    "critical",
                    "Alembic revision не соответствует repository head",
                    "проверить deployment report и выполнить только forward migration",
                    (f"DB: {actual or 'неизвестно'}", f"Head: {expected}"),
                )
            )
        errors_24h = int(database.get("errors_24h", 0) or 0)
        auth_24h = int(database.get("auth_24h", 0) or 0)
        rate_24h = int(database.get("rate_24h", 0) or 0)
        if auth_24h >= 5:
            severity: Severity = "critical" if auth_24h >= 20 else "warning"
            issues.append(
                Issue(
                    "vk.auth_errors",
                    severity,
                    f"Ошибки авторизации/VK token за 24ч: {auth_24h}",
                    "проверить валидность и права VK tokens",
                )
            )
        if rate_24h >= 50:
            issues.append(
                Issue(
                    "vk.rate_limit",
                    "warning",
                    f"Длительный VK rate-limit за 24ч: {rate_24h}",
                    "проверить token cooldown и допустимый RPS",
                )
            )
        if errors_24h >= 100:
            issues.append(
                Issue(
                    "collection.errors",
                    "warning",
                    f"Много collection errors за 24ч: {errors_24h}",
                    "проверить категории ошибок и worker logs",
                )
            )

        metrics = state.setdefault("metrics", {})
        previous_database_bytes = int(metrics.get("daily_database_bytes", 0) or 0)
        previous_database_at = parse_timestamp(metrics.get("daily_snapshot_at"))
        database_bytes = int(database.get("database_bytes", 0) or 0)
        if previous_database_at is not None:
            snapshot_age = current - previous_database_at
            if timedelta(hours=20) <= snapshot_age <= timedelta(hours=72):
                growth = database_bytes - previous_database_bytes
                database["database_growth_24h"] = growth
                snapshot["database"] = database
                if growth > max(512 * 1024**2, previous_database_bytes // 4):
                    issues.append(
                        Issue(
                            "database.growth",
                            "warning",
                            f"Размер БД резко вырос: +{format_bytes(growth)}",
                            "проверить объёмы новых данных и свободное место",
                        )
                    )

        previous_completed = int(metrics.get("completed", -1))
        previous_api = int(metrics.get("api_requests", -1))
        completed = counts.get("completed", 0)
        api_requests = int(database.get("api_requests", 0) or 0)
        progress_at = parse_timestamp(metrics.get("progress_at"))
        run_changed = metrics.get("run_id") != run_id
        if (
            run_changed
            or completed > previous_completed
            or api_requests > previous_api
            or progress_at is None
        ):
            progress_at = current
        pending = counts.get("pending", 0)
        retry_wait = counts.get("retry_wait", 0)
        next_retry = parse_timestamp(database.get("next_retry_at"))
        expected_backoff = retry_wait > 0 and next_retry is not None and next_retry > current
        worker_healthy = worker.get("status") == "running" and worker.get("health") in {
            "",
            "healthy",
        }
        stalled_for = current - progress_at
        if (
            status == "running"
            and pending > 0
            and worker_healthy
            and not deploy_in_progress
            and not expected_backoff
            and stalled_for >= timedelta(minutes=settings.stall_minutes)
        ):
            issues.append(
                Issue(
                    "collection.stalled",
                    "critical",
                    f"Сбор не показывает progress {int(stalled_for.total_seconds() // 60)} мин",
                    "проверить worker logs, VK tokens, lease и retries",
                    (
                        f"Run: {run_id}",
                        f"Pending: {pending}",
                        f"Running: {counts.get('running', 0)}",
                    ),
                )
            )
        previous_retries = int(metrics.get("retries", int(database.get("retries", 0) or 0)))
        retries = int(database.get("retries", 0) or 0)
        if not run_changed and retries - previous_retries >= 25:
            issues.append(
                Issue(
                    "collection.retry_spike",
                    "warning",
                    f"Retries выросли на {retries - previous_retries} с прошлой проверки",
                    "проверить VK/API error categories",
                )
            )
        metrics.update(
            {
                "run_id": run_id,
                "completed": completed,
                "api_requests": api_requests,
                "retries": retries,
                "rejected_jobs": int(database.get("rejected_jobs", 0) or 0),
                "progress_at": iso_utc(progress_at),
                "observed_at": iso_utc(current),
            }
        )

    if not tokens.get("present"):
        issues.append(
            Issue(
                "telegram.token_missing",
                "critical",
                "Telegram token file отсутствует",
                "запустить безопасный setup-скрипт",
            )
        )
    elif not tokens.get("readable"):
        issues.append(
            Issue(
                "telegram.token_permission",
                "critical",
                "Нет прав чтения Telegram token file",
                "проверить owner=deploy и mode 600",
            )
        )
    elif int(tokens.get("count", 0) or 0) == 0:
        issues.append(
            Issue(
                "telegram.token_empty",
                "critical",
                "Telegram token file пуст",
                "повторить безопасную настройку",
            )
        )

    if not vk_tokens.get("present"):
        issues.append(
            Issue(
                "vk.tokens_missing",
                "critical",
                "VK token file отсутствует",
                "восстановить production secrets/vk_tokens.txt",
            )
        )
    elif not vk_tokens.get("readable"):
        issues.append(
            Issue(
                "vk.tokens_permission",
                "critical",
                "Monitor не может прочитать VK token file",
                "проверить безопасные owner/group/mode без вывода содержимого",
            )
        )
    elif int(vk_tokens.get("count", 0) or 0) == 0:
        issues.append(
            Issue(
                "vk.tokens_empty",
                "critical",
                "Нет доступных VK tokens",
                "безопасно добавить рабочий VK token",
            )
        )

    disk_percent = float(resources.get("disk_percent", 0) or 0)
    disk_free = int(resources.get("disk_free", 0) or 0)
    if disk_percent >= settings.disk_critical_percent:
        issues.append(
            Issue(
                "resources.disk",
                "critical",
                f"Диск заполнен на {disk_percent:.1f}%",
                "освободить место без удаления PostgreSQL volume/backups вслепую",
            )
        )
    elif disk_percent >= settings.disk_warning_percent or (
        disk_free and disk_free < settings.disk_min_free_bytes
    ):
        issues.append(
            Issue(
                "resources.disk",
                "warning",
                f"Мало места: {disk_percent:.1f}%, свободно {format_bytes(disk_free)}",
                "увеличить диск или безопасно проверить крупные runtime-файлы",
            )
        )
    available = int(resources.get("memory_available", 0) or 0)
    if available and available < settings.ram_warning_available_mb * 1024**2:
        severity_ram: Severity = (
            "critical"
            if available < settings.ram_warning_available_mb * 1024**2 // 2
            else "warning"
        )
        issues.append(
            Issue(
                "resources.ram",
                severity_ram,
                f"Доступно RAM: {format_bytes(available)}",
                "проверить процессы, container limits и swap",
            )
        )
    swap_percent = float(resources.get("swap_percent", 0) or 0)
    if swap_percent >= settings.swap_warning_percent:
        issues.append(
            Issue(
                "resources.swap",
                "warning",
                f"Swap заполнен на {swap_percent:.1f}%",
                "проверить memory pressure и OOM events",
            )
        )

    if not runner.get("active"):
        issues.append(
            Issue(
                "runner.inactive",
                "critical",
                "GitHub Actions Runner inactive",
                "проверить systemd service и runner diagnostics",
            )
        )
    if not runner.get("enabled"):
        issues.append(
            Issue(
                "runner.disabled",
                "critical",
                "GitHub Actions Runner disabled",
                "включить runner systemd service",
            )
        )
    if runner.get("active") and int(runner.get("main_pid", 0) or 0) <= 0:
        issues.append(
            Issue(
                "runner.not_listening",
                "critical",
                "Runner service active, но listener process отсутствует",
                "проверить runner journal и регистрацию",
            )
        )

    if not deploy_in_progress and deploy.get("status") and deploy.get("status") != "success":
        issues.append(
            Issue(
                "deployment.failed",
                "critical",
                "Последний production deployment неуспешен",
                "проверить GitHub Actions и last-deployment report",
            )
        )
    git_sha = str(snapshot.get("git_sha", ""))
    worker_revision = str(worker.get("revision", ""))
    deploy_sha = str(deploy.get("commit_sha", ""))
    if not deploy_in_progress and git_sha and worker_revision and git_sha != worker_revision:
        issues.append(
            Issue(
                "deployment.revision_mismatch",
                "critical",
                "OCI revision worker не совпадает с production Git SHA",
                "проверить deployment/rollback и immutable image",
                (f"Git: {git_sha[:12]}", f"OCI: {worker_revision[:12]}"),
            )
        )
    if not deploy_in_progress and deploy_sha and worker_revision and deploy_sha != worker_revision:
        issues.append(
            Issue(
                "deployment.report_mismatch",
                "critical",
                "Worker revision не совпадает с deployment report",
                "проверить незавершённый deploy или rollback",
            )
        )
    if not deploy_in_progress and not bool(deploy.get("backup_verified")):
        issues.append(
            Issue(
                "backup.unverified",
                "warning",
                "Последний predeploy backup не подтверждён report-файлом",
                "проверить backup и pg_restore --list",
            )
        )

    if state_corrupt:
        issues.append(
            Issue(
                "monitor.state_corrupt",
                "warning",
                "State Telegram monitor повреждён; дедупликация начата заново",
                "проверить диск и права state directory",
            )
        )
    return sorted(issues, key=lambda issue: (-SEVERITY_ORDER[issue.severity], issue.key))


def format_bytes(value: int) -> str:
    """Сформировать компактный размер."""
    number = float(max(0, value))
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if number < 1024 or unit == "TiB":
            precision = 0 if unit in {"B", "KiB", "MiB"} else 1
            return f"{number:.{precision}f} {unit}"
        number /= 1024
    return f"{number:.1f} TiB"


def format_moscow_time(value: datetime) -> str:
    """Сформировать время по Москве."""
    return value.astimezone(MOSCOW).strftime("%d.%m.%Y %H:%M МСК")


def format_alert(issue: Issue, snapshot: dict[str, Any], now: datetime) -> str:
    """Сформировать короткий HTML alert."""
    icon = "🔴" if issue.severity == "critical" else "⚠️"
    database = dict(snapshot.get("database", {}))
    resources = dict(snapshot.get("resources", {}))
    counts = _counts(database)
    alert_title = "критическая проблема" if issue.severity == "critical" else "предупреждение"
    lines = [
        f"<b>{icon} VK Collector — {alert_title}</b>",
        f"Время: {html.escape(format_moscow_time(now))}",
        f"Проблема: {html.escape(issue.problem)}",
    ]
    run_id = str(database.get("run_id", ""))
    if run_id:
        lines.append(f"Run: <code>{html.escape(run_id)}</code>")
    if counts:
        lines.append(
            "Jobs: "
            f"completed {counts.get('completed', 0):,}; "
            f"pending {counts.get('pending', 0):,}; "
            f"running {counts.get('running', 0):,}"
        )
    lines.append(
        f"Диск: {float(resources.get('disk_percent', 0) or 0):.1f}%, "
        f"свободно {format_bytes(int(resources.get('disk_free', 0) or 0))}"
    )
    lines.extend(html.escape(detail) for detail in issue.details)
    lines.append(f"Действие: {html.escape(issue.action)}")
    return "\n".join(lines)


def format_recovery(issue_key: str, previous: dict[str, Any], now: datetime) -> str:
    """Сформировать отдельное recovery-сообщение."""
    problem = html.escape(str(previous.get("problem", issue_key)))
    return (
        f"<b>✅ VK Collector восстановлен</b>\n"
        f"Время: {html.escape(format_moscow_time(now))}\n"
        f"Проблема устранена: {problem}"
    )


def overall_status(issues: list[Issue]) -> str:
    """Определить общий статус snapshot."""
    if any(issue.severity == "critical" for issue in issues):
        return "CRITICAL"
    if any(issue.severity == "warning" for issue in issues):
        return "WARNING"
    return "OK"


def _uptime(started_at: object, now: datetime) -> str:
    started = parse_timestamp(started_at)
    if started is None:
        return "н/д"
    seconds = max(0, int((now - started).total_seconds()))
    days, remainder = divmod(seconds, 86_400)
    hours, _ = divmod(remainder, 3600)
    return f"{days}д {hours}ч"


def format_daily_report(
    snapshot: dict[str, Any],
    issues: list[Issue],
    now: datetime,
) -> str:
    """Сформировать полную ежедневную HTML-сводку."""
    database = dict(snapshot.get("database", {}))
    resources = dict(snapshot.get("resources", {}))
    worker = dict(snapshot.get("worker", {}))
    postgres = dict(snapshot.get("postgres", {}))
    runner = dict(snapshot.get("runner", {}))
    deploy = dict(snapshot.get("deployment", {}))
    counts = _counts(database)
    status = overall_status(issues)
    icon = {"OK": "✅", "WARNING": "⚠️", "CRITICAL": "🔴"}[status]
    memory_total = int(resources.get("memory_total", 0) or 0)
    memory_available = int(resources.get("memory_available", 0) or 0)
    memory_used = max(0, memory_total - memory_available)
    swap_total = int(resources.get("swap_total", 0) or 0)
    swap_used = int(resources.get("swap_used", 0) or 0)
    commit = str(snapshot.get("git_sha", ""))
    image = str(worker.get("image", "н/д"))
    revision = str(worker.get("revision", ""))
    warning_text = "; ".join(issue.problem for issue in issues[:5]) if issues else "нет"
    action = issues[0].action if issues else "действий не требуется"
    backup_at = str(deploy.get("latest_backup_at", "н/д"))
    backup_size = format_bytes(int(deploy.get("latest_backup_size", 0) or 0))
    completed_24h = int(database.get("completed_24h", 0) or 0)
    api_24h = int(database.get("api_24h", 0) or 0)
    retries = int(database.get("retries", 0) or 0)
    errors_24h = int(database.get("errors_24h", 0) or 0)
    inserted_24h = int(database.get("inserted_24h", 0) or 0)
    updated_24h = int(database.get("updated_24h", 0) or 0)
    disk_percent = float(resources.get("disk_percent", 0) or 0)
    disk_free = int(resources.get("disk_free", 0) or 0)
    database_bytes = int(database.get("database_bytes", 0) or 0)
    postgres_status = html.escape(str(postgres.get("status", "absent")))
    postgres_health = html.escape(str(postgres.get("health", "")) or "без health")
    worker_status = html.escape(str(worker.get("status", "absent")))
    worker_health = html.escape(str(worker.get("health", "")) or "без health")
    runner_status = (
        "active/online" if runner.get("active") and runner.get("main_pid") else "недоступен"
    )
    runner_enabled = "yes" if runner.get("enabled") else "no"
    deploy_status = html.escape(str(deploy.get("status", "н/д")))
    deploy_time = html.escape(str(deploy.get("report_mtime", "н/д")))
    successful_time = html.escape(str(deploy.get("last_successful_at", "н/д")))
    backup_verified = "yes" if deploy.get("backup_verified") else "no"
    lines = [
        f"<b>{icon} VK Collector — ежедневная сводка</b>",
        f"Время: {html.escape(format_moscow_time(now))}",
        f"Общий статус: <b>{status}</b>",
        "",
        "<b>Сбор</b>",
        f"Run: <code>{html.escape(str(database.get('run_id', 'н/д')))}</code>",
        f"Статус: {html.escape(str(database.get('run_status', 'н/д')))}",
        f"Completed: {counts.get('completed', 0):,} (+{completed_24h:,} за 24ч)",
        "Pending / Running / Skipped: "
        f"{counts.get('pending', 0):,} / {counts.get('running', 0):,} / "
        f"{counts.get('skipped', 0):,}",
        f"API requests за 24ч: {api_24h:,}",
        f"Retries всего / errors за 24ч: {retries:,} / {errors_24h:,}",
        "Rows inserted / updated всего: "
        f"{int(database.get('rows_inserted', 0) or 0):,} / "
        f"{int(database.get('rows_updated', 0) or 0):,}",
        f"Rows inserted / updated за 24ч: {inserted_24h:,} / {updated_24h:,}",
        "",
        "<b>Сервер</b>",
        f"Диск: {disk_percent:.1f}%, свободно {format_bytes(disk_free)}",
        f"RAM: {format_bytes(memory_used)} / {format_bytes(memory_total)}; "
        f"доступно {format_bytes(memory_available)}",
        f"Swap: {format_bytes(swap_used)} / {format_bytes(swap_total)}",
        f"PostgreSQL DB: {format_bytes(database_bytes)}",
        "Рост DB за 24ч: "
        + (
            format_bytes(int(database.get("database_growth_24h", 0) or 0))
            if "database_growth_24h" in database
            else "недоступен"
        ),
        "",
        "<b>Сервисы</b>",
        f"PostgreSQL: {postgres_status}/{postgres_health}; "
        f"uptime {_uptime(postgres.get('started_at'), now)}",
        f"Worker: {worker_status}/{worker_health}; uptime {_uptime(worker.get('started_at'), now)}",
        f"Runner: {runner_status}; enabled={runner_enabled}",
        "",
        "<b>Deployment</b>",
        f"Commit: <code>{html.escape(commit[:12] or 'н/д')}</code>",
        f"Image: <code>{html.escape(image[-100:])}</code>",
        f"OCI revision: <code>{html.escape(revision[:12] or 'н/д')}</code>",
        f"Последний deploy: {deploy_status}, {deploy_time}",
        f"Последний успешный deploy: {successful_time}",
        f"Backup: {html.escape(backup_at)}, {backup_size}, verified={backup_verified}",
        "",
        f"Предупреждения: {html.escape(warning_text)}",
        f"Действие: {html.escape(action)}",
    ]
    return "\n".join(lines)


def _read_token(settings: MonitorSettings) -> str:
    try:
        return settings.token_file.read_text(encoding="utf-8").strip()
    except (OSError, UnicodeDecodeError):
        return ""


def _effective_uid() -> int:
    getter = getattr(os, "geteuid", None)
    return int(getter()) if callable(getter) else -1


def _change_owner(path: Path, user_id: int, group_id: int) -> None:
    chown = getattr(os, "chown", None)
    if not callable(chown):
        raise RuntimeError("Смена владельца поддерживается только на POSIX")
    chown(path, user_id, group_id)


def _deliver(settings: MonitorSettings, message: str) -> bool:
    token = _read_token(settings)
    if not settings.enabled or not token or not settings.chat_id:
        LOGGER.warning("Telegram monitor отключён или не настроен")
        return False
    try:
        return send_message(token, settings.chat_id, message)
    except TelegramAPIError as exc:
        LOGGER.error("Telegram send failed: %s", exc)
        return False


def process_alerts(
    settings: MonitorSettings,
    snapshot: dict[str, Any],
    issues: list[Issue],
    state: dict[str, Any],
    *,
    now: datetime | None = None,
    deliver: Any = None,
) -> tuple[int, int]:
    """Применить dedup/repeat/recovery и вернуть sent/failed."""
    current = now or utc_now()
    sender = deliver or (lambda message: _deliver(settings, message))
    raw_alerts = state.setdefault("alerts", {})
    alerts: dict[str, Any] = raw_alerts if isinstance(raw_alerts, dict) else {}
    state["alerts"] = alerts
    active = {issue.key: issue for issue in issues}
    sent = 0
    failed = 0
    for issue in issues:
        previous_raw = alerts.get(issue.key)
        previous = previous_raw if isinstance(previous_raw, dict) else {}
        last_sent = parse_timestamp(previous.get("last_sent"))
        due = (
            not previous.get("sent")
            or previous.get("severity") != issue.severity
            or last_sent is None
            or (current - last_sent).total_seconds() >= settings.repeat_seconds
        )
        entry = {
            **previous,
            "severity": issue.severity,
            "problem": issue.problem,
            "first_seen": previous.get("first_seen", iso_utc(current)),
            "last_seen": iso_utc(current),
            "pending": bool(due),
        }
        if due:
            if bool(sender(format_alert(issue, snapshot, current))):
                entry.update(
                    {
                        "sent": True,
                        "pending": False,
                        "last_sent": iso_utc(current),
                    }
                )
                sent += 1
            else:
                failed += 1
        alerts[issue.key] = entry
    for key in list(alerts):
        if key in active:
            continue
        previous_raw = alerts.get(key)
        previous = previous_raw if isinstance(previous_raw, dict) else {}
        if previous.get("sent") and not previous.get("recovered"):
            if bool(sender(format_recovery(key, previous, current))):
                sent += 1
                del alerts[key]
            else:
                previous["pending_recovery"] = True
                failed += 1
        else:
            del alerts[key]
    return sent, failed


def process_daily(
    settings: MonitorSettings,
    snapshot: dict[str, Any],
    issues: list[Issue],
    state: dict[str, Any],
    *,
    now: datetime | None = None,
    deliver: Any = None,
) -> bool:
    """Отправить не более одной daily-сводки за московскую дату."""
    current = now or utc_now()
    date_key = current.astimezone(MOSCOW).date().isoformat()
    if state.get("last_daily_date") == date_key:
        return False
    message = format_daily_report(snapshot, issues, current)
    state["pending_daily"] = {"date": date_key, "message": message}
    sender = deliver or (lambda text: _deliver(settings, text))
    if bool(sender(message)):
        state["last_daily_date"] = date_key
        state.pop("pending_daily", None)
        metrics = state.setdefault("metrics", {})
        database = dict(snapshot.get("database", {}))
        metrics["daily_database_bytes"] = int(database.get("database_bytes", 0) or 0)
        metrics["daily_snapshot_at"] = iso_utc(current)
        return True
    return False


def retry_pending_daily(
    settings: MonitorSettings,
    state: dict[str, Any],
    *,
    deliver: Any = None,
) -> bool:
    """Повторить неотправленную daily-сводку при следующем health timer."""
    pending = state.get("pending_daily")
    if not isinstance(pending, dict):
        return False
    message = pending.get("message")
    date_key = pending.get("date")
    if not isinstance(message, str) or not isinstance(date_key, str):
        state.pop("pending_daily", None)
        return False
    sender = deliver or (lambda text: _deliver(settings, text))
    if bool(sender(message)):
        state["last_daily_date"] = date_key
        state.pop("pending_daily", None)
        return True
    return False


def run_monitor(
    mode: Literal["health", "daily"],
    settings: MonitorSettings,
    *,
    dry_run: bool = False,
) -> int:
    """Выполнить один monitor cycle."""
    current = utc_now()
    state_path = settings.state_dir / "state.json"
    state, corrupt = load_state(state_path)
    snapshot = collect_snapshot(settings, now=current)
    issues = evaluate_snapshot(snapshot, state, settings, now=current, state_corrupt=corrupt)
    if dry_run:
        print(
            json.dumps(
                {
                    "status": overall_status(issues),
                    "issues": [
                        {"key": issue.key, "severity": issue.severity, "problem": issue.problem}
                        for issue in issues
                    ],
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        return 0
    if mode == "daily":
        process_daily(settings, snapshot, issues, state, now=current)
    else:
        retry_pending_daily(settings, state)
        process_alerts(settings, snapshot, issues, state, now=current)
    atomic_write_json(state_path, state)
    LOGGER.info(
        "Monitor cycle: mode=%s status=%s issues=%s", mode, overall_status(issues), len(issues)
    )
    return 0


def _atomic_secret(path: Path, value: str, owner: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            stream.write(value.strip() + "\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.chmod(temporary, 0o600)
        if _effective_uid() == 0:
            user_id = int(subprocess.check_output(["id", "-u", owner], text=True, encoding="utf-8"))
            group_id = int(
                subprocess.check_output(["id", "-g", owner], text=True, encoding="utf-8")
            )
            _change_owner(temporary, user_id, group_id)
        os.replace(temporary, path)
    finally:
        if temporary.exists():
            temporary.unlink()


def _update_env(path: Path, updates: dict[str, str]) -> None:
    original = path.read_text(encoding="utf-8") if path.exists() else ""
    remaining = dict(updates)
    lines: list[str] = []
    for line in original.splitlines():
        match = ENV_LINE.match(line.strip())
        if match and match.group(1) in remaining:
            key = match.group(1)
            lines.append(f"{key}={remaining.pop(key)}")
        else:
            lines.append(line)
    if remaining:
        if lines and lines[-1]:
            lines.append("")
        lines.extend(f"{key}={value}" for key, value in remaining.items())
    metadata = path.stat() if path.exists() else None
    descriptor, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            stream.write("\n".join(lines).rstrip() + "\n")
            stream.flush()
            os.fsync(stream.fileno())
        if metadata:
            os.chmod(temporary, stat.S_IMODE(metadata.st_mode))
            if _effective_uid() == 0:
                _change_owner(temporary, metadata.st_uid, metadata.st_gid)
        else:
            os.chmod(temporary, 0o600)
        os.replace(temporary, path)
    finally:
        if temporary.exists():
            temporary.unlink()


def _tty_input(prompt: str) -> str:
    with Path("/dev/tty").open("r+", encoding="utf-8") as terminal:
        terminal.write(prompt)
        terminal.flush()
        return terminal.readline().strip()


def setup_from_stdin(root: Path, owner: str, *, dry_run: bool = False) -> int:
    """Безопасно проверить token, найти chat ID и настроить production."""
    token = sys.stdin.readline().strip()
    if not token:
        print("ОШИБКА: Telegram token пуст.", file=sys.stderr)
        return 2
    try:
        me = request_json(token, "getMe")
    except TelegramAPIError as exc:
        print(f"ОШИБКА: token не принят Bot API: {exc}", file=sys.stderr)
        return 2
    result = me.get("result")
    username = str(result.get("username", "")) if isinstance(result, dict) else ""
    print(f"Bot API доступен. Username: @{username or 'не указан'}")
    _tty_input("Откройте бота, отправьте /start и нажмите Enter...")
    chats: dict[str, tuple[str, str]] = {}
    for _ in range(12):
        try:
            updates = request_json(token, "getUpdates", {"timeout": "0"})
        except TelegramAPIError as exc:
            print(f"ОШИБКА getUpdates: {exc}", file=sys.stderr)
            return 2
        rows = updates.get("result")
        if isinstance(rows, list):
            for row in rows:
                if not isinstance(row, dict):
                    continue
                message = row.get("message") or row.get("my_chat_member")
                if not isinstance(message, dict):
                    continue
                chat = message.get("chat")
                if not isinstance(chat, dict) and isinstance(message.get("chat"), dict):
                    chat = message["chat"]
                if not isinstance(chat, dict):
                    continue
                chat_id = str(chat.get("id", ""))
                if not re.fullmatch(r"-?\d+", chat_id):
                    continue
                label = str(chat.get("username") or chat.get("title") or "")
                chats[chat_id] = (str(chat.get("type", "unknown")), label)
        if chats:
            break
        time.sleep(5)
    if not chats:
        print("ОШИБКА: chat не найден. Отправьте /start и повторите setup.", file=sys.stderr)
        return 2
    print("Найдены chats:")
    for chat_id, (chat_type, label) in sorted(chats.items()):
        print(f"- {chat_id}; type={chat_type}; name={label or 'не указано'}")
    if len(chats) == 1:
        chat_id = next(iter(chats))
    else:
        chat_id = _tty_input("Введите нужный chat ID: ")
        if chat_id not in chats:
            print("ОШИБКА: выбран неизвестный chat ID.", file=sys.stderr)
            return 2
    if dry_run:
        print(f"DRY-RUN: token рабочий, chat ID {chat_id}; файлы не изменялись.")
        return 0
    token_path = root / "secrets" / "telegram_bot_token.txt"
    _atomic_secret(token_path, token, owner)
    _update_env(
        root / ".env",
        {
            "TELEGRAM_ENABLED": "true",
            "TELEGRAM_BOT_TOKEN": "",
            "TELEGRAM_BOT_TOKEN_FILE": str(token_path),
            "TELEGRAM_CHAT_ID": chat_id,
        },
    )
    try:
        send_message(
            token,
            chat_id,
            "✅ VK Collector: Telegram-уведомления успешно настроены.",
        )
    except TelegramAPIError as exc:
        print(f"ОШИБКА тестового сообщения: {exc}", file=sys.stderr)
        return 2
    token = ""
    print(f"Настройка сохранена. Chat ID: {chat_id}. Тестовое сообщение отправлено.")
    return 0


def send_test_alert(settings: MonitorSettings) -> int:
    """Отправить явно маркированный безопасный TEST alert."""
    message = "<b>🧪 TEST — это проверка Telegram-оповещений, реальной проблемы нет.</b>"
    return 0 if _deliver(settings, message) else 2


def send_workflow_failure() -> int:
    """Отправить failure/cancelled alert с GitHub-hosted runner."""
    token = os.environ.get("TELEGRAM_BOT_TOKEN", "")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID", "")
    if not token or not chat_id:
        LOGGER.warning("GitHub Telegram secrets не настроены; failure alert пропущен")
        return 0
    jobs = os.environ.get("FAILED_JOBS", "неизвестно")
    repository = os.environ.get("GITHUB_REPOSITORY", "unknown")
    sha = os.environ.get("GITHUB_SHA", "")
    branch = os.environ.get("GITHUB_REF_NAME", "")
    run_id = os.environ.get("GITHUB_RUN_ID", "")
    server_url = os.environ.get("GITHUB_SERVER_URL", "https://github.com")
    run_url = f"{server_url}/{repository}/actions/runs/{run_id}"
    message = "\n".join(
        (
            "<b>🔴 VK Collector — production workflow failed</b>",
            f"Repository: {html.escape(repository)}",
            f"Branch: {html.escape(branch)}",
            f"Commit: <code>{html.escape(sha)}</code>",
            f"Jobs: {html.escape(jobs)}",
            f'Run: <a href="{html.escape(run_url, quote=True)}">{html.escape(run_id)}</a>',
        )
    )
    try:
        send_message(token, chat_id, message)
    except TelegramAPIError as exc:
        LOGGER.error("Telegram workflow notification failed: %s", exc)
        return 1
    finally:
        os.environ.pop("TELEGRAM_BOT_TOKEN", None)
    return 0


def build_parser() -> argparse.ArgumentParser:
    """Собрать CLI parser."""
    parser = argparse.ArgumentParser(description="Telegram production monitor VK Collector")
    modes = parser.add_mutually_exclusive_group(required=True)
    modes.add_argument("--health", action="store_true", help="проверить production")
    modes.add_argument("--daily", action="store_true", help="отправить ежедневную сводку")
    modes.add_argument("--test-alert", action="store_true", help="отправить TEST alert")
    modes.add_argument("--setup-token-stdin", action="store_true", help=argparse.SUPPRESS)
    modes.add_argument("--workflow-failure", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument(
        "--project-root",
        type=Path,
        default=Path("/opt/vk-research-collector"),
    )
    parser.add_argument("--owner", default="deploy", help=argparse.SUPPRESS)
    parser.add_argument("--dry-run", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    """Точка входа production monitor."""
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    args = build_parser().parse_args(argv)
    root = args.project_root.resolve()
    if args.workflow_failure:
        return send_workflow_failure()
    if args.setup_token_stdin:
        return setup_from_stdin(root, str(args.owner), dry_run=bool(args.dry_run))
    settings = MonitorSettings.load(root)
    if args.test_alert:
        return send_test_alert(settings)
    mode: Literal["health", "daily"] = "daily" if args.daily else "health"
    return run_monitor(mode, settings, dry_run=bool(args.dry_run))


if __name__ == "__main__":
    raise SystemExit(main())

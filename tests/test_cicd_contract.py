from __future__ import annotations

import os
import re
import shutil
import stat
import subprocess
from pathlib import Path

import pytest
import yaml

try:
    import fcntl
except ImportError:  # pragma: no cover - Windows выполняет статические проверки
    fcntl = None  # type: ignore[assignment]


ROOT = Path(__file__).resolve().parents[1]
DEPLOY_SCRIPT = ROOT / "scripts" / "deploy-production.sh"
CLEANUP_SCRIPT = ROOT / "scripts" / "cleanup-production-storage.sh"
COLLECTION_CONTROL_SCRIPT = ROOT / "scripts" / "production-collection-control.sh"
FULL_SHA = "0123456789abcdef0123456789abcdef01234567"
IMAGE = f"ghcr.io/marklyvk/vk-research-collector/collector:sha-{FULL_SHA}"
DIGEST = f"sha256:{'1' * 64}"

pytestmark = pytest.mark.skipif(
    not DEPLOY_SCRIPT.exists(),
    reason="Contract-тесты выполняются из checkout; deployment files не входят в runtime image",
)


def load_yaml(path: Path) -> dict[str, object]:
    return yaml.load(path.read_text(encoding="utf-8"), Loader=yaml.BaseLoader)


def test_workflows_have_safe_triggers_runners_permissions_and_pinned_actions() -> None:
    ci_text = (ROOT / ".github/workflows/ci.yml").read_text(encoding="utf-8")
    deploy_text = (ROOT / ".github/workflows/deploy-production.yml").read_text(encoding="utf-8")
    cleanup_text = (ROOT / ".github/workflows/cleanup-production-storage.yml").read_text(
        encoding="utf-8"
    )
    collection_control_text = (
        ROOT / ".github/workflows/production-collection-control.yml"
    ).read_text(encoding="utf-8")
    ci = load_yaml(ROOT / ".github/workflows/ci.yml")
    deploy = load_yaml(ROOT / ".github/workflows/deploy-production.yml")

    assert set(ci["on"]) == {"pull_request", "push", "workflow_dispatch"}  # type: ignore[arg-type]
    assert all(job["runs-on"] == "ubuntu-latest" for job in ci["jobs"].values())  # type: ignore[union-attr]
    assert "pull_request" not in deploy["on"]  # type: ignore[operator]
    assert deploy["concurrency"] == {  # type: ignore[index]
        "group": "production-deployment",
        "cancel-in-progress": "false",
    }
    jobs = deploy["jobs"]  # type: ignore[index]
    assert set(jobs) == {"quality", "build-image", "deploy", "verify", "notify-failure"}
    assert set(jobs["deploy"]["needs"]) == {"quality", "build-image"}  # type: ignore[index]
    assert jobs["deploy"]["runs-on"] == [  # type: ignore[index]
        "self-hosted",
        "linux",
        "x64",
        "production",
        "vk-collector",
    ]
    assert jobs["quality"]["runs-on"] == jobs["build-image"]["runs-on"] == "ubuntu-latest"  # type: ignore[index]
    assert jobs["verify"]["runs-on"] == "ubuntu-latest"  # type: ignore[index]
    assert jobs["notify-failure"]["runs-on"] == "ubuntu-latest"  # type: ignore[index]
    assert set(jobs["notify-failure"]["needs"]) == {  # type: ignore[index]
        "quality",
        "build-image",
        "deploy",
        "verify",
    }
    assert jobs["notify-failure"]["permissions"] == {"contents": "read"}  # type: ignore[index]
    assert jobs["deploy"]["permissions"] == {"contents": "read", "packages": "read"}  # type: ignore[index]
    assert jobs["build-image"]["permissions"] == {  # type: ignore[index]
        "contents": "read",
        "packages": "write",
    }
    assert jobs["build-image"]["outputs"]["digest"]  # type: ignore[index]
    assert "--image-digest" in deploy_text
    assert "TELEGRAM_BOT_TOKEN" in deploy_text
    assert "--workflow-failure" in deploy_text
    assert "ref=ghcr.io/${repository}/collector:sha-${GITHUB_SHA}" in deploy_text
    workflow_text = ci_text + deploy_text + cleanup_text + collection_control_text
    assert "pull_request_target" not in workflow_text
    action_refs = re.findall(r"uses:\s+[^@\s]+@([^\s#]+)", workflow_text)
    assert action_refs and all(re.fullmatch(r"[0-9a-f]{40}", ref) for ref in action_refs)


def test_production_compose_uses_sha_image_and_stable_volume_without_build() -> None:
    text = (ROOT / "compose.production.yaml").read_text(encoding="utf-8")
    compose = load_yaml(ROOT / "compose.production.yaml")
    services = compose["services"]  # type: ignore[index]
    assert "build" not in services["collector"]  # type: ignore[index]
    assert "build" not in services["collector-worker"]  # type: ignore[index]
    assert services["collector"]["image"].startswith("${COLLECTOR_IMAGE:?")  # type: ignore[index]
    assert services["collector-worker"]["restart"] == "unless-stopped"  # type: ignore[index]
    base = load_yaml(ROOT / "compose.yaml")
    assert base["services"]["collector-worker"]["stop_grace_period"] == "6m"  # type: ignore[index]
    assert "healthcheck" in base["services"]["postgres"]  # type: ignore[index]
    assert "healthcheck" in services["collector-worker"]  # type: ignore[operator]
    assert "vk_research_postgres_data" in text
    assert "down -v" not in text


def test_deploy_contract_has_all_failure_guards_and_no_destructive_volume_action() -> None:
    text = DEPLOY_SCRIPT.read_text(encoding="utf-8")
    required = (
        "flock -n",
        "DISK_WARNING",
        "DISK_STOP",
        "pg_dump",
        "pg_restore --list",
        "alembic upgrade head",
        "alembic check",
        "Healthcheck не пройден",
        "rollback_image",
        "collection verify",
        "collection status",
        'compose stop -t "$WORKER_STOP_TIMEOUT" collector-worker',
        "compose ps -aq collector-worker",
        "compose_cli",
        'docker image inspect "$IMAGE"',
        "compose run --rm --no-deps collector",
        "compose up -d --no-deps --no-build collector-worker",
        "Production PostgreSQL volume",
        "Не найден $DEPLOY_DIR/.env",
        "Не найден secrets/vk_tokens.txt",
        "fast_forward_checkout",
        "merge --ff-only",
        "verify_local_image",
        "org.opencontainers.image.revision",
        "EXPECTED_IMAGE_DIGEST",
        'BACKUP_KEEP="${PREDEPLOY_BACKUP_KEEP:-1}"',
        "load_protected_backups",
        "grant_collector_protected_backup_read",
        "is_protected_backup",
        "configuration #>> '{verified_backup,path}'",
        "paused_capacity_limit",
        "setfacl -m u:10001:rx",
        "setfacl -m u:10001:r",
        "stop_worker_on_critical_disk",
        "DISK_AFTER_BACKUP",
        "DISK_AFTER_PULL",
        "install_telegram_monitor_units",
        "systemctl --user enable --now",
    )
    assert all(item in text for item in required)
    assert "alembic downgrade" not in text
    assert "down -v" not in text
    assert "docker volume rm" not in text
    assert "docker compose build" not in text
    assert "compose run --rm --no-deps --no-build collector" not in text
    assert "docker system prune" not in text
    stop = text.index('compose stop -t "$WORKER_STOP_TIMEOUT" collector-worker')
    preflight = text.index("compose_cli alembic current", stop)
    upgrade = text.index("compose_cli alembic upgrade head", preflight)
    baseline = text.index("BASELINE_STATUS_JSON=$(compose_cli collection status", upgrade)
    worker_start = text.index("compose up -d --no-deps --no-build collector-worker", baseline)
    assert text.index("ROLLBACK_ALLOWED=1", stop) < preflight
    assert preflight < text.index("ROLLBACK_ALLOWED=0", preflight) < upgrade
    assert upgrade < baseline < worker_start
    assert "compose up -d --remove-orphans postgres" not in text
    assert "git reset --hard" not in text
    assert "git clean" not in text
    assert "eval " not in text
    assert "set -x" not in text


def test_runtime_secrets_archives_and_runner_are_not_tracked() -> None:
    tracked = subprocess.check_output(
        ["git", "ls-files"], cwd=ROOT, text=True, encoding="utf-8"
    ).splitlines()
    forbidden = re.compile(
        r"(^|/)(\.env|secrets|backups|runner)(/|$)|\.dump$|\.manifest\.json$|\.zip$"
    )
    assert not [path for path in tracked if forbidden.search(path)]


def test_endpoint_migration_uses_one_set_based_post_backfill() -> None:
    migration = (ROOT / "alembic/versions/20260810_0006_endpoint_aware_subscriptions.py").read_text(
        encoding="utf-8"
    )
    assert "UPDATE group_posts AS p" in migration
    assert "SET community_vk_id = g.vk_id" in migration
    assert "ORDER BY p.id LIMIT 10000" not in migration
    assert "GET DIAGNOSTICS changed" not in migration


def test_storage_cleanup_is_manual_allowlist_only_and_preserves_critical_data() -> None:
    workflow_path = ROOT / ".github/workflows/cleanup-production-storage.yml"
    workflow = load_yaml(workflow_path)
    workflow_text = workflow_path.read_text(encoding="utf-8")
    script = CLEANUP_SCRIPT.read_text(encoding="utf-8")

    assert set(workflow["on"]) == {"workflow_dispatch"}  # type: ignore[arg-type]
    assert workflow["concurrency"] == {  # type: ignore[index]
        "group": "production-deployment",
        "cancel-in-progress": "false",
    }
    job = workflow["jobs"]["cleanup"]  # type: ignore[index]
    assert job["runs-on"] == ["self-hosted", "linux", "x64", "production", "vk-collector"]
    assert job["environment"] == {"name": "production"}
    assert "DELETE_OLD_BACKUPS" in workflow_text
    assert "cleanup-production-storage.sh" in workflow_text

    required = (
        'EXPECTED_DEPLOY_DIR="${CLEANUP_EXPECTED_DEPLOY_DIR:-/opt/vk-research-collector}"',
        "pg_restore --list",
        "LATEST_BACKUP",
        "PROTECTED_BACKUPS",
        "configuration #>> '{verified_backup,path}'",
        "CURRENT_IMAGE_ID",
        "PREVIOUS_IMAGE_ID",
        "docker builder prune -af",
        "docker image prune -f",
        "PostgreSQL не healthy после cleanup",
        "Worker не healthy после cleanup",
        "20260815_0010",
    )
    assert all(item in script for item in required)
    forbidden = (
        "docker system prune",
        "docker volume rm",
        "compose down",
        "down -v",
        "rm -rf",
        'rm -r "$DEPLOY_DIR"',
    )
    assert not any(item in script for item in forbidden)


def test_collection_control_is_scheduled_gated_and_preserves_capacity_guards() -> None:
    workflow_path = ROOT / ".github/workflows/production-collection-control.yml"
    workflow = load_yaml(workflow_path)
    workflow_text = workflow_path.read_text(encoding="utf-8")
    script = COLLECTION_CONTROL_SCRIPT.read_text(encoding="utf-8")

    assert set(workflow["on"]) == {"schedule", "workflow_dispatch"}  # type: ignore[arg-type]
    assert workflow["on"]["schedule"] == [{"cron": "17 * * * *"}]  # type: ignore[index]
    assert workflow["concurrency"] == {  # type: ignore[index]
        "group": "production-deployment",
        "cancel-in-progress": "false",
    }
    job = workflow["jobs"]["control"]  # type: ignore[index]
    assert job["runs-on"] == ["self-hosted", "linux", "x64", "production", "vk-collector"]
    assert job["environment"] == {"name": "production"}
    assert "START_SUBSCRIPTIONS" in workflow_text
    assert "github.event_name == 'schedule'" in workflow_text
    assert '"$GITHUB_EVENT_NAME" != schedule' in workflow_text

    required = (
        "flock -n",
        "20260815_0010",
        "collection subscriptions pilot",
        "subscription-gate-a.json",
        "production_allowed",
        "collection campaign plan --apply",
        "collection capacity-apply",
        "pilot-control-decision",
        "campaign control-decision",
        "pilot --run-id",
        "cancel-pilot --run-id ID --confirm",
        "--backup",
        "setfacl -m u:10001:r",
        "setfacl -m u:10001:rx",
        'chmod 0700 "$backup_dir"',
        "Collector UID не может прочитать PGDMP header",
        "Пробую renewal Gate A",
        "COLLECTION_SUBSCRIPTIONS_MAX_PER_USER 50",
        "compose stop -t 360 collector-worker",
        "compose up -d --no-deps --no-build collector-worker",
        "group_keyword_matches",
        "vk_token_method_states",
        "Дубли не создаются",
        "Подходящих пользователей для новой cohort сейчас нет",
        "collection_campaigns",
        "subscription_discovery",
        "distinct_entities",
        "stale_running_leases",
        "sanitized_message",
        "j.status = 'failed'",
        "for pilot_attempt in 1 2 3",
        "retryable_pilot",
        "terminal-состояний",
        "ensure_worker_healthy",
        "безопасный self-heal",
        "'subscription_discovery','subscription_metadata'",
        "deferred_pilot",
        "следующий hourly-control выберет тот же run ID",
    )
    assert all(item in script for item in required)
    active_run_query = script.split("active_runs=$(", 1)[1].split(
        "paused_capacity_campaigns=$(", 1
    )[0]
    assert "'full'" not in active_run_query
    assert "'incremental'" not in active_run_query
    assert "subscriptions_pilot" not in active_run_query
    assert "paused_capacity_limit" not in active_run_query
    assert "Переиспользую paused-capacity campaign" in script
    assert 'collection subscriptions pilot --run-id "$pilot_run_id"' in script
    assert '--run-id "$renew_run_id"' in script
    forbidden = (
        "capacity_gate = 'passed'",
        "UPDATE collection_runs",
        "docker volume rm",
        "down -v",
    )
    assert not any(item in script for item in forbidden)


def test_operational_shell_scripts_are_executable_in_git() -> None:
    scripts = (
        "scripts/deploy-production.sh",
        "scripts/cleanup-production-storage.sh",
        "scripts/production-collection-control.sh",
        "scripts/install-github-runner.sh",
        "scripts/import-server-handoff.sh",
        "scripts/setup-telegram-monitor.sh",
        "scripts/telegram-monitor.py",
    )
    output = subprocess.check_output(
        ["git", "ls-files", "--stage", "--", *scripts],
        cwd=ROOT,
        text=True,
        encoding="utf-8",
    )
    modes = {line.split(maxsplit=1)[0] for line in output.splitlines()}
    assert modes == {"100755"}


def test_runner_installer_uses_production_runner_name_and_labels() -> None:
    text = (ROOT / "scripts/install-github-runner.sh").read_text(encoding="utf-8")
    assert "RUNNER_NAME=${RUNNER_NAME:-vk-collector-production-01}" in text
    assert "RUNNER_LABELS=production,vk-collector" in text


def test_telegram_systemd_and_secret_contract() -> None:
    units = {
        path.name: path.read_text(encoding="utf-8")
        for path in (ROOT / "deploy" / "systemd").glob("vk-collector-telegram-*")
    }
    assert {
        "vk-collector-telegram-health.service",
        "vk-collector-telegram-health.timer",
        "vk-collector-telegram-daily.service",
        "vk-collector-telegram-daily.timer",
    } == set(units)
    assert "OnBootSec=2min" in units["vk-collector-telegram-health.timer"]
    assert "OnUnitActiveSec=5min" in units["vk-collector-telegram-health.timer"]
    assert "OnCalendar=*-*-* 09:00:00 Europe/Moscow" in units["vk-collector-telegram-daily.timer"]
    assert all(
        "Persistent=true" in units[name]
        for name in (
            "vk-collector-telegram-health.timer",
            "vk-collector-telegram-daily.timer",
        )
    )
    assert not any("TELEGRAM_BOT_TOKEN=" in text for text in units.values())
    assert not any("/var/run/docker.sock" in text for text in units.values())
    setup = (ROOT / "scripts/setup-telegram-monitor.sh").read_text(encoding="utf-8")
    assert "read -rsp" in setup
    assert "unset TELEGRAM_BOT_TOKEN" in setup


def test_handoff_scripts_require_checksum_explicit_replace_and_keep_workers_exclusive() -> None:
    export = (ROOT / "scripts/export-server-handoff.ps1").read_text(encoding="utf-8")
    import_ = (ROOT / "scripts/import-server-handoff.sh").read_text(encoding="utf-8")
    assert "stop collector-worker" in export
    assert "Get-FileHash" in export and "SHA256" in export
    assert "ConvertTo-Json" in export and "scp" in export
    assert "up -d collector-worker" not in export
    assert "--confirm-replace-database" in import_
    assert "sha256sum --check" in import_
    assert "pg_restore --list" in import_
    assert "server-before-handoff" in import_
    assert "alembic upgrade head" in import_
    assert "collection verify" in import_
    assert import_.index("collection verify") < import_.index("compose up -d collector-worker")
    assert "down -v" not in import_
    assert "docker volume rm" not in import_


@pytest.fixture
def dry_run_tree(tmp_path: Path) -> tuple[Path, Path, dict[str, str]]:
    if os.name == "nt":
        pytest.skip("Поведенческие shell-тесты выполняются в Linux/Docker CI")
    runtime = tmp_path / "runtime"
    fake_bin = tmp_path / "bin"
    runtime.mkdir()
    fake_bin.mkdir()
    for relative in (
        "compose.yaml",
        "compose.production.yaml",
        "config/keywords.yml",
        "scripts/deploy-production.sh",
        "scripts/postgres-init-readonly.sh",
        "scripts/telegram-monitor.py",
        "deploy/systemd/vk-collector-telegram-health.service",
        "deploy/systemd/vk-collector-telegram-health.timer",
        "deploy/systemd/vk-collector-telegram-daily.service",
        "deploy/systemd/vk-collector-telegram-daily.timer",
    ):
        target = runtime / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(ROOT / relative, target)
    (runtime / "secrets").mkdir()
    (runtime / "exports").mkdir()
    (runtime / ".env").write_text(
        "\n".join(
            (
                "POSTGRES_DB=vk_research",
                "POSTGRES_USER=vk_collector",
                "POSTGRES_PASSWORD=fake-test-password",
                "POSTGRES_READER_PASSWORD=fake-reader-password",
                "POSTGRES_VOLUME_NAME=vk_research_postgres_data",
                "DISK_WARNING_PERCENT=85",
                "DISK_STOP_PERCENT=95",
                f"COLLECTION_RUN_ID={FULL_SHA[:8]}-0000-4000-8000-000000000000",
            )
        ),
        encoding="utf-8",
    )
    token_file = runtime / "secrets/vk_tokens.txt"
    token_file.write_text("fake-token-not-used\n", encoding="utf-8")
    (runtime / ".env").chmod(stat.S_IRUSR | stat.S_IWUSR)
    token_file.chmod(stat.S_IRUSR | stat.S_IWUSR)
    trace = tmp_path / "docker.trace"
    docker = fake_bin / "docker"
    docker.write_text(
        """#!/usr/bin/env bash
set -eu
printf '%s\\n' "$*" >> "$FAKE_DOCKER_TRACE"
case "$*" in
  "compose version") exit 0 ;;
  "volume inspect "*) exit 0 ;;
  *" config --quiet") exit 0 ;;
  *" exec -T postgres pg_isready "*) exit 0 ;;
  *" ps -q collector-worker") exit 0 ;;
  *" collector collection status"*)
    printf '%s' '{"run_id":"test","status":"running","jobs":'
    printf '%s\\n' '{"completed":10,"pending":2,"running":1,"retry_wait":0,"failed":0}}'
    exit 0 ;;
esac
exit 0
""",
        encoding="utf-8",
    )
    docker.chmod(0o755)
    env = os.environ.copy()
    env.update(
        {
            "PATH": f"{fake_bin}:{env['PATH']}",
            "FAKE_DOCKER_TRACE": str(trace),
            "DEPLOY_USER": subprocess.check_output(["id", "-un"], text=True).strip(),
        }
    )
    return runtime, trace, env


def run_dry(runtime: Path, env: dict[str, str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [
            "bash",
            str(DEPLOY_SCRIPT),
            "--dry-run",
            "--source-dir",
            str(ROOT),
            "--deploy-dir",
            str(runtime),
            "--image",
            IMAGE,
            "--image-digest",
            DIGEST,
            "--git-sha",
            FULL_SHA,
        ],
        check=False,
        capture_output=True,
        text=True,
        env=env,
    )


def test_dry_run_is_non_mutating(dry_run_tree: tuple[Path, Path, dict[str, str]]) -> None:
    runtime, trace, env = dry_run_tree
    result = run_dry(runtime, env)
    assert result.returncode == 0, result.stderr
    calls = trace.read_text(encoding="utf-8")
    assert all(word not in calls for word in (" stop ", "pg_dump", "alembic upgrade", " up -d"))
    assert not (runtime / "backups").exists()


@pytest.mark.parametrize("missing", [".env", "secrets/vk_tokens.txt"])
def test_dry_run_rejects_missing_runtime_secrets(
    dry_run_tree: tuple[Path, Path, dict[str, str]], missing: str
) -> None:
    runtime, _, env = dry_run_tree
    (runtime / missing).unlink()
    result = run_dry(runtime, env)
    assert result.returncode != 0
    assert "Не найден" in result.stderr


@pytest.mark.parametrize("usage,expected", [(85, "warning=85"), (95, "Критическое")])
def test_dry_run_rejects_disk_thresholds(
    dry_run_tree: tuple[Path, Path, dict[str, str]], usage: int, expected: str
) -> None:
    runtime, _, env = dry_run_tree
    env["DEPLOY_DISK_USED_PERCENT_OVERRIDE"] = str(usage)
    result = run_dry(runtime, env)
    assert result.returncode != 0
    assert expected in result.stderr


def test_process_lock_rejects_second_deployment(
    dry_run_tree: tuple[Path, Path, dict[str, str]],
) -> None:
    runtime, _, env = dry_run_tree
    deploy_state = runtime / ".deploy"
    deploy_state.mkdir(mode=0o700)
    lock_path = deploy_state / "deploy.lock"
    with lock_path.open("w", encoding="utf-8") as lock:
        assert fcntl is not None
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        result = run_dry(runtime, env)
    assert result.returncode != 0
    assert "уже выполняется" in result.stderr

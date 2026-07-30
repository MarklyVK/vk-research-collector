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
FULL_SHA = "0123456789abcdef0123456789abcdef01234567"
IMAGE = f"ghcr.io/marklyvk/vk-research-collector/collector:sha-{FULL_SHA}"


def load_yaml(path: Path) -> dict[str, object]:
    return yaml.load(path.read_text(encoding="utf-8"), Loader=yaml.BaseLoader)


def test_workflows_have_safe_triggers_runners_permissions_and_pinned_actions() -> None:
    ci_text = (ROOT / ".github/workflows/ci.yml").read_text(encoding="utf-8")
    deploy_text = (ROOT / ".github/workflows/deploy-production.yml").read_text(encoding="utf-8")
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
    assert set(jobs) == {"quality", "build-image", "deploy", "verify"}
    assert jobs["deploy"]["runs-on"] == [  # type: ignore[index]
        "self-hosted",
        "linux",
        "x64",
        "production",
        "vk-collector",
    ]
    assert jobs["quality"]["runs-on"] == jobs["build-image"]["runs-on"] == "ubuntu-latest"  # type: ignore[index]
    assert jobs["verify"]["runs-on"] == "ubuntu-latest"  # type: ignore[index]
    assert jobs["deploy"]["permissions"] == {"contents": "read", "packages": "read"}  # type: ignore[index]
    assert "pull_request_target" not in ci_text + deploy_text
    action_refs = re.findall(r"uses:\s+[^@\s]+@([^\s#]+)", ci_text + deploy_text)
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
        "compose stop collector-worker",
        "compose up -d --remove-orphans postgres collector-worker",
        "Production PostgreSQL volume",
        "Не найден $DEPLOY_DIR/.env",
        "Не найден secrets/vk_tokens.txt",
    )
    assert all(item in text for item in required)
    assert "alembic downgrade" not in text
    assert "down -v" not in text
    assert "docker volume rm" not in text
    assert "eval " not in text
    assert "set -x" not in text


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
    ):
        target = runtime / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(ROOT / relative, target)
    (runtime / "secrets").mkdir()
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
  *" run --rm collector collection status"*)
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

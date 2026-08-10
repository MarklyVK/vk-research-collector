"""Измеренные capacity reports для независимых gates подписок."""

from __future__ import annotations

import hashlib
import json
import os
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Literal

REPORT_SCHEMA_VERSION = 2
SAFE_DISK_LIMIT_BYTES = 7 * 1024**3
CapacityPhase = Literal["A", "B"]


def configuration_hash(configuration: dict[str, object]) -> str:
    """Вернуть стабильный hash всех параметров capacity-конфигурации."""
    encoded = json.dumps(configuration, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def build_capacity_report(
    *,
    phase: CapacityPhase,
    run_id: uuid.UUID,
    configuration: dict[str, object],
    limits: dict[str, int],
    measured: dict[str, int | float],
    projected: dict[str, int | float | None],
    production_allowed: bool,
    measured_at: datetime | None = None,
) -> dict[str, Any]:
    """Собрать версионированный отчёт только из переданных измерений."""
    return {
        "schema_version": REPORT_SCHEMA_VERSION,
        "phase": phase,
        "run_id": str(run_id),
        "measured_at": (measured_at or datetime.now(UTC)).isoformat(),
        "configuration_hash": configuration_hash(configuration),
        "configuration": configuration,
        "limits": limits,
        "measured": measured,
        "projected": projected,
        "safe_disk_limit_bytes": SAFE_DISK_LIMIT_BYTES,
        "production_allowed": production_allowed,
    }


def write_capacity_report(target: Path, payload: dict[str, Any]) -> None:
    """Атомарно записать JSON report в том же каталоге."""
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_name(f".{target.name}.{os.getpid()}.tmp")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    temporary.replace(target)


def validate_capacity_report(
    source: Path,
    *,
    phase: CapacityPhase,
    configuration: dict[str, object],
    max_age_days: int,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Прочитать report и безопасно отклонить повреждённый/чужой/устаревший."""
    try:
        payload = json.loads(source.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"Capacity report не читается: {exc}") from exc
    if not isinstance(payload, dict):
        raise ValueError("Capacity report должен содержать JSON-объект")
    if payload.get("schema_version") != REPORT_SCHEMA_VERSION or payload.get("phase") != phase:
        raise ValueError(f"Capacity report не соответствует фазе {phase} или версии схемы")
    if payload.get("configuration_hash") != configuration_hash(configuration):
        raise ValueError("Capacity report относится к другой конфигурации")
    if payload.get("configuration") != configuration:
        raise ValueError("Конфигурация capacity report повреждена")
    try:
        uuid.UUID(str(payload["run_id"]))
    except (KeyError, ValueError) as exc:
        raise ValueError("Capacity report не содержит корректный run ID") from exc
    try:
        measured_at = datetime.fromisoformat(str(payload["measured_at"]))
    except (KeyError, TypeError, ValueError) as exc:
        raise ValueError("Capacity report не содержит корректное время измерения") from exc
    if measured_at.tzinfo is None:
        raise ValueError("Время capacity report должно содержать UTC offset")
    current = now or datetime.now(UTC)
    if measured_at > current + timedelta(minutes=5):
        raise ValueError("Capacity report датирован будущим временем")
    if measured_at < current - timedelta(days=max_age_days):
        raise ValueError("Capacity report устарел")
    projected = payload.get("projected")
    measured = payload.get("measured")
    limits = payload.get("limits")
    required_measured = {
        "duration_seconds",
        "api_requests",
        "processed_jobs",
        "completed_entities",
        "skipped_entities",
        "failed_entities",
        "database_bytes_before",
        "database_bytes_after",
        "database_growth_bytes",
        "relation_growth_bytes",
        "planned_entities",
        "observed_entities",
        "disk_free_bytes_after",
    }
    if not isinstance(measured, dict) or not required_measured.issubset(measured):
        raise ValueError("Capacity report не содержит обязательные measured показатели")
    if not isinstance(limits, dict):
        raise ValueError("Capacity report не содержит точные limits")
    if phase == "A" and (
        limits.get("subscriptions_per_user") != configuration.get("subscriptions_max_per_user")
        or limits.get("production_users") != configuration.get("subscriptions_users_per_run")
        or limits.get("pilot_users") != configuration.get("subscription_pilot_users")
        or limits.get("minimum_pilot_users") != configuration.get("subscription_pilot_min_users")
        or limits.get("subscriptions_preview_limit") != 100
    ):
        raise ValueError("Capacity Gate A содержит несовпадающие limits")
    if phase == "B" and (
        limits.get("posts_per_community") != configuration.get("subscription_posts_max")
        or limits.get("post_ttl_days") != configuration.get("subscription_posts_ttl_days")
        or limits.get("pilot_communities")
        != configuration.get("subscription_posts_pilot_communities")
        or limits.get("minimum_pilot_communities")
        != configuration.get("subscription_posts_pilot_min_communities")
    ):
        raise ValueError("Capacity Gate B содержит несовпадающие limits")
    projected_bytes = projected.get("database_bytes") if isinstance(projected, dict) else None
    projected_growth = (
        projected.get("database_growth_bytes") if isinstance(projected, dict) else None
    )
    safe_limit = payload.get("safe_disk_limit_bytes")
    database_growth = measured.get("database_growth_bytes")
    relation_growth = measured.get("relation_growth_bytes")
    observed_entities = measured.get("observed_entities")
    planned_entities = measured.get("planned_entities")
    completed_entities = measured.get("completed_entities")
    skipped_entities = measured.get("skipped_entities")
    failed_entities = measured.get("failed_entities")
    disk_free = measured.get("disk_free_bytes_after")
    minimum_entities = (
        limits.get("minimum_pilot_users")
        if phase == "A"
        else limits.get("minimum_pilot_communities")
    )
    if (
        payload.get("production_allowed") is not True
        or not isinstance(projected_bytes, int)
        or not isinstance(safe_limit, int)
        or safe_limit != SAFE_DISK_LIMIT_BYTES
        or projected_bytes > safe_limit
        or not isinstance(projected_growth, int)
        or projected_growth <= 0
        or not isinstance(disk_free, int)
        or projected_growth > disk_free
        or not isinstance(database_growth, int)
        or not isinstance(relation_growth, int)
        or max(database_growth, relation_growth) <= 0
        or not isinstance(planned_entities, int)
        or not isinstance(observed_entities, int)
        or not isinstance(completed_entities, int)
        or not isinstance(skipped_entities, int)
        or not isinstance(failed_entities, int)
        or min(
            planned_entities,
            observed_entities,
            completed_entities,
            skipped_entities,
            failed_entities,
        )
        < 0
        or completed_entities + skipped_entities != observed_entities
        or observed_entities + failed_entities > planned_entities
        or failed_entities != 0
        or not isinstance(minimum_entities, int)
        or observed_entities < minimum_entities
    ):
        raise ValueError(f"Capacity Gate {phase} не разрешает production run")
    return payload

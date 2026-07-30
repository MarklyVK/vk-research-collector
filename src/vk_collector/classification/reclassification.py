from __future__ import annotations

import hashlib
import json
import uuid
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, ValidationError, model_validator
from sqlalchemy import func, select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from vk_collector.classification.schemas import AllowedLabel, Confidence
from vk_collector.database.models import (
    ClassificationReview,
    ClassificationStatus,
    GroupCandidate,
    GroupKeywordMatch,
    GroupLabel,
    SearchKeyword,
)

RECLASSIFICATION_SOURCE = "food_service_expansion"


class ReclassificationDecision(BaseModel):
    """Завершённое экспертное решение по одной существующей группе."""

    model_config = ConfigDict(extra="ignore")

    vk_id: int = Field(gt=0)
    previous_approved: bool
    previous_labels: list[AllowedLabel]
    food_service: bool
    final_approved: bool
    final_labels: list[AllowedLabel]
    confidence: Confidence
    reason: str = Field(min_length=10, max_length=2000)

    @model_validator(mode="after")
    def decision_is_safe(self) -> ReclassificationDecision:
        if len(self.previous_labels) != len(set(self.previous_labels)):
            raise ValueError("Предыдущие метки не должны повторяться")
        if len(self.final_labels) != len(set(self.final_labels)):
            raise ValueError("Итоговые метки не должны повторяться")
        if not set(self.previous_labels).issubset(self.final_labels):
            raise ValueError("Reclassification не может удалять прежние метки")
        if self.final_approved != bool(self.final_labels):
            raise ValueError("Approved требует меток, rejected требует пустой список")
        if self.food_service != ("food_service" in self.final_labels):
            raise ValueError("food_service должен совпадать с итоговой меткой")
        if not self.food_service and (
            self.final_approved != self.previous_approved
            or set(self.final_labels) != set(self.previous_labels)
        ):
            raise ValueError("Отрицательное решение не должно менять прежнюю классификацию")
        return self


class ReclassificationDocument(BaseModel):
    model_config = ConfigDict(extra="forbid")

    operation_id: str = Field(min_length=10, max_length=100)
    source: str
    snapshot_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    expected_count: int = Field(gt=0)
    decisions: list[ReclassificationDecision]

    @model_validator(mode="after")
    def decisions_are_unique(self) -> ReclassificationDocument:
        ids = [item.vk_id for item in self.decisions]
        if len(ids) != len(set(ids)):
            raise ValueError("VK ID в reclassification не должны повторяться")
        if self.source != RECLASSIFICATION_SOURCE:
            raise ValueError("Некорректный source reclassification")
        if len(self.decisions) != self.expected_count:
            raise ValueError("Документ должен содержать решение для каждой группы snapshot")
        return self


def _write_text_atomic(target: Path, value: str) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_suffix(target.suffix + ".tmp")
    temporary.write_text(value, encoding="utf-8")
    temporary.replace(target)


def _canonical_hash(payload: list[dict[str, Any]]) -> str:
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


async def prepare_reclassification(
    session: AsyncSession,
    output_dir: Path,
    *,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Создать полный воспроизводимый snapshot для семантической reclassification."""
    groups = list(
        (await session.scalars(select(GroupCandidate).order_by(GroupCandidate.vk_id))).all()
    )
    label_rows = (
        await session.execute(select(GroupLabel.group_id, GroupLabel.label).order_by(GroupLabel.id))
    ).all()
    keyword_rows = (
        await session.execute(
            select(GroupKeywordMatch.group_id, SearchKeyword.subject, SearchKeyword.keyword)
            .join(SearchKeyword, SearchKeyword.id == GroupKeywordMatch.keyword_id)
            .order_by(GroupKeywordMatch.group_id, SearchKeyword.id)
        )
    ).all()
    labels: dict[int, list[str]] = defaultdict(list)
    for group_id, label in label_rows:
        labels[group_id].append(label)
    keywords: dict[int, list[dict[str, str]]] = defaultdict(list)
    for group_id, subject, keyword in keyword_rows:
        keywords[group_id].append({"subject": subject, "keyword": keyword})

    draft_rows: list[dict[str, Any]] = []
    snapshot_rows: list[dict[str, Any]] = []
    for group in groups:
        previous_labels = sorted(labels[group.id])
        context = {
            "vk_id": group.vk_id,
            "name": group.name,
            "description": group.description,
            "status": group.status_text,
            "matched_keywords": keywords[group.id],
            "previous_approved": (group.classification_status == ClassificationStatus.APPROVED),
            "previous_labels": previous_labels,
        }
        snapshot_rows.append(context)
        draft_rows.append(
            {
                **context,
                "food_service": None,
                "final_approved": None,
                "final_labels": None,
                "confidence": None,
                "reason": "",
            }
        )

    created_at = now or datetime.now(UTC)
    snapshot_sha256 = _canonical_hash(snapshot_rows)
    operation_id = f"food-service-{created_at:%Y%m%d}-{uuid.uuid4().hex[:12]}"
    decisions_payload = {
        "operation_id": operation_id,
        "source": RECLASSIFICATION_SOURCE,
        "snapshot_sha256": snapshot_sha256,
        "expected_count": len(groups),
        "decisions": draft_rows,
    }
    progress = {
        "operation_id": operation_id,
        "snapshot_sha256": snapshot_sha256,
        "total": len(groups),
        "completed": 0,
        "remaining": len(groups),
        "status": "awaiting_semantic_review",
        "updated_at": created_at.isoformat(),
    }
    validation = {
        "operation_id": operation_id,
        "valid": False,
        "status": "not_validated",
        "expected_decisions": len(groups),
        "validated_decisions": 0,
    }
    report = (
        "# Отчёт повторной классификации food_service\n\n"
        f"Operation ID: `{operation_id}`.\n\n"
        f"В snapshot включено групп: {len(groups)}. Решений завершено: 0.\n\n"
        "Статус: ожидается семантическая проверка по description, name, status и только "
        "затем matched_keywords. Массовый импорт до полной валидации запрещён.\n"
    )
    _write_text_atomic(
        output_dir / "decisions.json",
        json.dumps(decisions_payload, ensure_ascii=False, indent=2),
    )
    _write_text_atomic(
        output_dir / "progress.json", json.dumps(progress, ensure_ascii=False, indent=2)
    )
    _write_text_atomic(
        output_dir / "validation-summary.json",
        json.dumps(validation, ensure_ascii=False, indent=2),
    )
    _write_text_atomic(output_dir / "RECLASSIFICATION_REPORT.md", report)
    return progress


def load_reclassification(source: Path) -> ReclassificationDocument:
    """Прочитать и полностью проверить завершённый документ решений."""
    try:
        raw: Any = json.loads(source.read_text(encoding="utf-8"))
        document = ReclassificationDocument.model_validate(raw)
        if not isinstance(raw, dict) or not isinstance(raw.get("decisions"), list):
            raise ValueError("Некорректная структура reclassification")
        context_keys = (
            "vk_id",
            "name",
            "description",
            "status",
            "matched_keywords",
            "previous_approved",
            "previous_labels",
        )
        snapshot_rows: list[dict[str, Any]] = []
        for row in raw["decisions"]:
            if not isinstance(row, dict) or any(key not in row for key in context_keys):
                raise ValueError("В решениях отсутствует исходный контекст snapshot")
            snapshot_rows.append({key: row[key] for key in context_keys})
        if _canonical_hash(snapshot_rows) != document.snapshot_sha256:
            raise ValueError("Контекст snapshot изменён после экспорта")
        return document
    except (OSError, ValidationError, json.JSONDecodeError) as exc:
        raise ValueError(f"Некорректный файл reclassification: {exc}") from exc


async def import_reclassification(session: AsyncSession, source: Path) -> dict[str, int]:
    """Идемпотентно добавить решения, не удаляя прежние корректные labels."""
    document = load_reclassification(source)
    await session.execute(select(func.pg_advisory_xact_lock(2_026_073_000_1)))
    all_vk_ids = set((await session.scalars(select(GroupCandidate.vk_id))).all())
    decision_ids = {item.vk_id for item in document.decisions}
    if decision_ids != all_vk_ids:
        missing = all_vk_ids - decision_ids
        unknown = decision_ids - all_vk_ids
        details = []
        if missing:
            details.append(f"нет решений для {len(missing)} существующих групп")
        if unknown:
            details.append(f"неизвестных VK ID: {len(unknown)}")
        raise ValueError("Неполный snapshot reclassification: " + "; ".join(details))
    existing_reviews = int(
        await session.scalar(
            select(func.count(ClassificationReview.id)).where(
                ClassificationReview.operation_id == document.operation_id
            )
        )
        or 0
    )
    if existing_reviews:
        if existing_reviews == len(document.decisions):
            return {"processed": 0, "food_service_added": 0, "rejected_to_approved": 0}
        raise ValueError("Найден неполный ранее применённый operation_id")

    groups = list(
        (
            await session.scalars(
                select(GroupCandidate)
                .where(GroupCandidate.vk_id.in_([item.vk_id for item in document.decisions]))
                .with_for_update()
            )
        ).all()
    )
    by_vk = {group.vk_id: group for group in groups}
    unknown = {item.vk_id for item in document.decisions} - set(by_vk)
    if unknown:
        raise ValueError(f"Неизвестные VK ID: {sorted(unknown)}")
    label_rows = (
        await session.execute(
            select(GroupLabel.group_id, GroupLabel.label).where(
                GroupLabel.group_id.in_([group.id for group in groups])
            )
        )
    ).all()
    current_labels: dict[int, set[str]] = defaultdict(set)
    for group_id, label in label_rows:
        current_labels[group_id].add(label)

    food_service_added = 0
    rejected_to_approved = 0
    for decision in document.decisions:
        group = by_vk[decision.vk_id]
        actual_approved = group.classification_status == ClassificationStatus.APPROVED
        actual_labels = current_labels[group.id]
        if decision.previous_approved != actual_approved:
            raise ValueError(f"Изменился предыдущий статус VK ID {decision.vk_id}")
        if set(decision.previous_labels) != actual_labels:
            raise ValueError(f"Изменились предыдущие метки VK ID {decision.vk_id}")
        if decision.food_service and "food_service" not in actual_labels:
            food_service_added += 1
        if decision.food_service and not actual_approved:
            rejected_to_approved += 1

        await session.execute(
            update(GroupCandidate)
            .where(GroupCandidate.id == group.id)
            .values(
                classification_status=(
                    ClassificationStatus.APPROVED
                    if decision.final_approved
                    else ClassificationStatus.REJECTED
                ),
                confidence=decision.confidence,
            )
        )
        for label in decision.final_labels:
            await session.execute(
                insert(GroupLabel)
                .values(group_id=group.id, label=label)
                .on_conflict_do_nothing(index_elements=[GroupLabel.group_id, GroupLabel.label])
            )
        session.add(
            ClassificationReview(
                operation_id=document.operation_id,
                group_id=group.id,
                previous_approved=decision.previous_approved,
                previous_labels=sorted(decision.previous_labels),
                food_service=decision.food_service,
                final_approved=decision.final_approved,
                final_labels=sorted(decision.final_labels),
                confidence=decision.confidence,
                reason=decision.reason,
                source=document.source,
            )
        )
    await session.commit()
    return {
        "processed": len(document.decisions),
        "food_service_added": food_service_added,
        "rejected_to_approved": rejected_to_approved,
    }

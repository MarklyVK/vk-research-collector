from __future__ import annotations

import json
import uuid
from collections import Counter, defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from pydantic import ValidationError
from sqlalchemy import delete, exists, func, or_, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from vk_collector.classification.schemas import (
    AllowedLabel,
    ClassificationImport,
    ExportBatch,
    ExportGroup,
    MatchedKeyword,
    build_decisions,
)
from vk_collector.database.models import (
    ClassificationBatch,
    ClassificationBatchItem,
    ClassificationStatus,
    GroupCandidate,
    GroupCollectionState,
    GroupKeywordMatch,
    GroupLabel,
    SearchKeyword,
)
from vk_collector.subjects import SubjectName


def _new_batch_id(now: datetime) -> str:
    return f"{now.astimezone(UTC):%Y-%m-%d}-{uuid.uuid4().hex[:12]}"


async def export_batch(
    session: AsyncSession,
    export_dir: Path,
    batch_size: int,
    *,
    now: datetime | None = None,
) -> Path | None:
    """Создать фиксированный пакет pending-групп и атомарно записать JSON."""
    exported_group_ids = select(ClassificationBatchItem.group_id)
    candidates = list(
        (
            await session.scalars(
                select(GroupCandidate)
                .where(
                    GroupCandidate.classification_status == ClassificationStatus.PENDING,
                    GroupCandidate.id.not_in(exported_group_ids),
                )
                .order_by(GroupCandidate.id)
                .limit(batch_size)
                .with_for_update(skip_locked=True)
            )
        ).all()
    )
    if not candidates:
        return None

    created_at = now or datetime.now(UTC)
    public_id = _new_batch_id(created_at)
    batch = ClassificationBatch(batch_id=public_id, created_at=created_at)
    session.add(batch)
    await session.flush()
    for position, candidate in enumerate(candidates):
        session.add(
            ClassificationBatchItem(
                batch_id=batch.id,
                group_id=candidate.id,
                position=position,
            )
        )

    matches = (
        await session.execute(
            select(GroupKeywordMatch.group_id, SearchKeyword.keyword, SearchKeyword.subject)
            .join(SearchKeyword, SearchKeyword.id == GroupKeywordMatch.keyword_id)
            .where(GroupKeywordMatch.group_id.in_([item.id for item in candidates]))
            .order_by(GroupKeywordMatch.group_id, SearchKeyword.id)
        )
    ).all()
    keywords: dict[int, list[MatchedKeyword]] = defaultdict(list)
    for group_id, keyword, subject in matches:
        keywords[group_id].append(MatchedKeyword(keyword=keyword, subject=subject))

    payload = ExportBatch(
        batch_id=public_id,
        groups=[
            ExportGroup(
                vk_id=item.vk_id,
                name=item.name,
                description=item.description,
                status=item.status_text,
                address=item.address,
                matched_keywords=keywords[item.id],
            )
            for item in candidates
        ],
    )
    export_dir.mkdir(parents=True, exist_ok=True)
    target = export_dir / f"{public_id}.json"
    temporary = export_dir / f".{public_id}.tmp"
    temporary.write_text(payload.model_dump_json(indent=2), encoding="utf-8")
    temporary.replace(target)
    await session.commit()
    return target


async def import_classification(session: AsyncSession, source: Path) -> int:
    """Проверить и применить файл классификации одной транзакцией."""
    try:
        payload = ClassificationImport.model_validate_json(source.read_text(encoding="utf-8"))
    except (OSError, ValidationError, json.JSONDecodeError) as exc:
        raise ValueError(f"Некорректный файл классификации: {exc}") from exc

    await session.execute(select(func.pg_advisory_xact_lock(2_026_073_000_1)))
    batch = await session.scalar(
        select(ClassificationBatch)
        .where(ClassificationBatch.batch_id == payload.batch_id)
        .with_for_update()
    )
    if batch is None:
        raise ValueError(f"Пакет {payload.batch_id!r} не найден")
    rows = (
        await session.execute(
            select(ClassificationBatchItem.group_id, GroupCandidate.vk_id)
            .join(GroupCandidate, GroupCandidate.id == ClassificationBatchItem.group_id)
            .where(ClassificationBatchItem.batch_id == batch.id)
        )
    ).all()
    internal_by_vk = {vk_id: group_id for group_id, vk_id in rows}
    subjects_rows = (
        await session.execute(
            select(GroupCandidate.vk_id, SearchKeyword.subject)
            .join(GroupKeywordMatch, GroupKeywordMatch.group_id == GroupCandidate.id)
            .join(SearchKeyword, SearchKeyword.id == GroupKeywordMatch.keyword_id)
            .where(GroupCandidate.vk_id.in_(internal_by_vk))
        )
    ).all()
    subjects: dict[int, set[AllowedLabel]] = defaultdict(set)
    for vk_id, subject in subjects_rows:
        subjects[vk_id].add(subject)

    decisions = build_decisions(payload, set(internal_by_vk), subjects)
    if batch.imported_at is not None:
        actual_label_rows = (
            await session.execute(
                select(GroupCandidate.vk_id, GroupLabel.label)
                .join(GroupLabel, GroupLabel.group_id == GroupCandidate.id)
                .where(GroupCandidate.vk_id.in_(internal_by_vk))
            )
        ).all()
        actual_labels: dict[int, set[str]] = defaultdict(set)
        for vk_id, label in actual_label_rows:
            actual_labels[vk_id].add(label)
        actual_status_rows = (
            await session.execute(
                select(GroupCandidate.vk_id, GroupCandidate.classification_status).where(
                    GroupCandidate.vk_id.in_(internal_by_vk)
                )
            )
        ).all()
        actual_statuses = {vk_id: status for vk_id, status in actual_status_rows}
        for decision in decisions:
            expected_status = (
                ClassificationStatus.APPROVED
                if decision.approved
                else ClassificationStatus.REJECTED
            )
            if actual_statuses[decision.vk_id] != expected_status or actual_labels[
                decision.vk_id
            ] != set(decision.labels):
                raise ValueError(
                    f"Повторный импорт не совпадает с сохранённым решением VK ID {decision.vk_id}"
                )
        return 0

    for decision in decisions:
        group_id = internal_by_vk[decision.vk_id]
        await session.execute(
            update(GroupCandidate)
            .where(GroupCandidate.id == group_id)
            .values(
                classification_status=(
                    ClassificationStatus.APPROVED
                    if decision.approved
                    else ClassificationStatus.REJECTED
                ),
                confidence=decision.confidence,
            )
        )
        await session.execute(delete(GroupLabel).where(GroupLabel.group_id == group_id))
        for label in sorted(decision.labels if decision.approved else set()):
            session.add(GroupLabel(group_id=group_id, label=label))
    batch.imported_at = datetime.now(UTC)
    await session.commit()
    return len(decisions)


async def classification_summary(
    session: AsyncSession, subject: SubjectName | None = None
) -> dict[str, Any]:
    """Вернуть количества статусов и распределение approved по меткам."""
    status_rows = (
        await session.execute(
            select(GroupCandidate.classification_status, func.count(GroupCandidate.id)).group_by(
                GroupCandidate.classification_status
            )
        )
    ).all()
    label_rows = (
        await session.execute(
            select(GroupLabel.label, func.count(GroupLabel.group_id))
            .join(GroupCandidate, GroupCandidate.id == GroupLabel.group_id)
            .where(GroupCandidate.classification_status == ClassificationStatus.APPROVED)
            .group_by(GroupLabel.label)
        )
    ).all()
    statuses = Counter({status.value: count for status, count in status_rows})
    result: dict[str, Any] = {
        "pending": statuses["pending"],
        "approved": statuses["approved"],
        "rejected": statuses["rejected"],
        "approved_by_label": {label: count for label, count in label_rows},
    }
    if subject is None:
        return result

    has_subject_match = exists(
        select(GroupKeywordMatch.id)
        .join(SearchKeyword, SearchKeyword.id == GroupKeywordMatch.keyword_id)
        .where(
            GroupKeywordMatch.group_id == GroupCandidate.id,
            SearchKeyword.subject == subject,
        )
    )
    has_subject_label = exists(
        select(GroupLabel.id).where(
            GroupLabel.group_id == GroupCandidate.id,
            GroupLabel.label == subject,
        )
    )
    candidate_predicate = or_(has_subject_match, has_subject_label)
    candidate_rows = (
        await session.execute(
            select(GroupCandidate.classification_status, func.count(GroupCandidate.id))
            .where(candidate_predicate)
            .group_by(GroupCandidate.classification_status)
        )
    ).all()
    candidates = Counter({status.value: count for status, count in candidate_rows})
    label_counts = (
        select(GroupLabel.group_id, func.count(GroupLabel.id).label("label_count"))
        .group_by(GroupLabel.group_id)
        .subquery()
    )
    multi_label = int(
        await session.scalar(
            select(func.count(GroupCandidate.id))
            .join(GroupLabel, GroupLabel.group_id == GroupCandidate.id)
            .join(label_counts, label_counts.c.group_id == GroupCandidate.id)
            .where(
                GroupCandidate.classification_status == ClassificationStatus.APPROVED,
                GroupLabel.label == subject,
                label_counts.c.label_count > 1,
            )
        )
        or 0
    )
    collected = int(
        await session.scalar(
            select(func.count(GroupCandidate.id))
            .join(GroupLabel, GroupLabel.group_id == GroupCandidate.id)
            .join(GroupCollectionState, GroupCollectionState.group_id == GroupCandidate.id)
            .where(
                GroupCandidate.classification_status == ClassificationStatus.APPROVED,
                GroupLabel.label == subject,
                or_(
                    GroupCollectionState.last_group_success_at.is_not(None),
                    GroupCollectionState.last_posts_success_at.is_not(None),
                    GroupCollectionState.last_members_success_at.is_not(None),
                ),
            )
        )
        or 0
    )
    approved_with_label = result["approved_by_label"].get(subject, 0)
    result["subject"] = {
        "name": subject,
        "candidates": sum(candidates.values()),
        "candidate_pending": candidates["pending"],
        "candidate_approved": candidates["approved"],
        "candidate_rejected": candidates["rejected"],
        "approved_with_label": approved_with_label,
        "multi_label": multi_label,
        "collected_groups": collected,
    }
    return result

from __future__ import annotations

from typing import Annotated

from pydantic import BaseModel, ConfigDict, Field, model_validator

from vk_collector.subjects import SubjectName

AllowedLabel = SubjectName
Confidence = Annotated[float, Field(ge=0, le=1)]


class MatchedKeyword(BaseModel):
    keyword: str
    subject: AllowedLabel


class ExportGroup(BaseModel):
    vk_id: int
    name: str
    description: str
    status: str
    address: str
    matched_keywords: list[MatchedKeyword]


class ExportBatch(BaseModel):
    batch_id: str
    groups: list[ExportGroup]


class FullResult(BaseModel):
    model_config = ConfigDict(extra="forbid")

    vk_id: int
    approved: bool
    labels: list[AllowedLabel]
    confidence: Confidence

    @model_validator(mode="after")
    def labels_are_unique(self) -> FullResult:
        if len(self.labels) != len(set(self.labels)):
            raise ValueError("Метки одной группы не должны повторяться")
        if self.approved and not self.labels:
            raise ValueError("У approved-группы должна быть хотя бы одна метка")
        if not self.approved and self.labels:
            raise ValueError("У rejected-группы список меток должен быть пустым")
        return self


class ClassificationImport(BaseModel):
    model_config = ConfigDict(extra="forbid")

    batch_id: str
    approved_group_ids: list[int] | None = None
    results: list[FullResult] | None = None

    @model_validator(mode="after")
    def exactly_one_format(self) -> ClassificationImport:
        if (self.approved_group_ids is None) == (self.results is None):
            raise ValueError("Укажите ровно один формат: approved_group_ids или results")
        ids = self.approved_group_ids or [item.vk_id for item in self.results or []]
        if len(ids) != len(set(ids)):
            raise ValueError("VK ID не должны повторяться")
        return self


class ClassificationDecision(BaseModel):
    vk_id: int
    approved: bool
    labels: set[AllowedLabel]
    confidence: Confidence


def build_decisions(
    payload: ClassificationImport,
    batch_group_ids: set[int],
    subjects_by_group: dict[int, set[AllowedLabel]],
) -> list[ClassificationDecision]:
    """Полностью проверить импорт и вернуть нормализованные решения."""
    if payload.approved_group_ids is not None:
        supplied = set(payload.approved_group_ids)
        unknown = supplied - batch_group_ids
        if unknown:
            raise ValueError(f"Пакет не содержит VK ID: {sorted(unknown)}")
        return [
            ClassificationDecision(
                vk_id=vk_id,
                approved=vk_id in supplied,
                labels=subjects_by_group.get(vk_id, set()) if vk_id in supplied else set(),
                confidence=1.0,
            )
            for vk_id in sorted(batch_group_ids)
        ]

    results = payload.results or []
    supplied = {item.vk_id for item in results}
    if supplied != batch_group_ids:
        missing = batch_group_ids - supplied
        unknown = supplied - batch_group_ids
        details = []
        if missing:
            details.append(f"пропущены VK ID: {sorted(missing)}")
        if unknown:
            details.append(f"неизвестные VK ID: {sorted(unknown)}")
        raise ValueError("Некорректный состав полного импорта: " + "; ".join(details))
    return [
        ClassificationDecision(
            vk_id=item.vk_id,
            approved=item.approved,
            labels=set(item.labels),
            confidence=item.confidence,
        )
        for item in results
    ]

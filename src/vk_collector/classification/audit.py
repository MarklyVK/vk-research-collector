from __future__ import annotations

import json
import random
from collections import Counter
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from vk_collector.classification.reclassification import (
    _write_text_atomic,
    load_reclassification,
)
from vk_collector.classification.schemas import AllowedLabel, Confidence

AUDIT_SEED = 20260730
AuditError = Literal[
    "correct",
    "false_positive",
    "false_negative",
    "wrong_category",
    "ambiguous",
]

_STRATA: tuple[tuple[str, int], ...] = (
    ("food_service_and_delivery", 100),
    ("rejected_to_approved", 50),
    ("approved_added_food_service", 50),
    ("approved_food_service", 100),
    ("rejected_food_service_candidate", 100),
    ("borderline", 100),
)


class AuditResult(BaseModel):
    model_config = ConfigDict(extra="forbid")

    vk_id: int = Field(gt=0)
    stratum: str
    original_decision: bool
    original_labels: list[AllowedLabel]
    audited_decision: bool
    audited_labels: list[AllowedLabel]
    confidence: Confidence
    reason: str = Field(min_length=10, max_length=2000)
    error_type: AuditError
    name: str = ""
    description: str = ""

    @model_validator(mode="after")
    def audited_invariants(self) -> AuditResult:
        if self.audited_decision != bool(self.audited_labels):
            raise ValueError("Audited approved требует меток, rejected — пустой список")
        if len(self.audited_labels) != len(set(self.audited_labels)):
            raise ValueError("Audited labels не должны повторяться")
        return self


class AuditDocument(BaseModel):
    model_config = ConfigDict(extra="forbid")

    seed: int
    results: list[AuditResult]

    @model_validator(mode="after")
    def ids_are_unique(self) -> AuditDocument:
        if self.seed != AUDIT_SEED:
            raise ValueError(f"Аудит должен использовать seed {AUDIT_SEED}")
        ids = [item.vk_id for item in self.results]
        if len(ids) != len(set(ids)):
            raise ValueError("Группы в аудите должны быть уникальными")
        return self


def _matches_stratum(row: dict[str, Any], stratum: str) -> bool:
    labels = set(row.get("final_labels") or [])
    previous_labels = set(row.get("previous_labels") or [])
    if stratum == "food_service_and_delivery":
        return {"food_service", "food_delivery"}.issubset(labels)
    if stratum == "rejected_to_approved":
        return not row.get("previous_approved") and bool(row.get("final_approved"))
    if stratum == "approved_added_food_service":
        return (
            bool(row.get("previous_approved"))
            and "food_service" in labels
            and "food_service" not in previous_labels
        )
    if stratum == "approved_food_service":
        return bool(row.get("final_approved")) and "food_service" in labels
    if stratum == "rejected_food_service_candidate":
        return not bool(row.get("food_service"))
    if stratum == "borderline":
        text = f"{row.get('name', '')} {row.get('description', '')}".casefold()
        ambiguous = any(word in text for word in ("кафе", "ресторан", "бар"))
        confidence = row.get("confidence")
        return ambiguous or (isinstance(confidence, (int, float)) and confidence < 0.85)
    return False


def prepare_audit(decisions_source: Path, output_dir: Path) -> dict[str, Any]:
    """Создать непересекающуюся стратифицированную выборку с фиксированным seed."""
    load_reclassification(decisions_source)
    raw = json.loads(decisions_source.read_text(encoding="utf-8"))
    decisions: list[dict[str, Any]] = raw["decisions"]
    rng = random.Random(AUDIT_SEED)
    selected_ids: set[int] = set()
    sample: list[dict[str, Any]] = []
    shortages: dict[str, int] = {}
    for stratum, required in _STRATA:
        candidates = [
            row
            for row in decisions
            if row["vk_id"] not in selected_ids and _matches_stratum(row, stratum)
        ]
        candidates.sort(key=lambda row: row["vk_id"])
        rng.shuffle(candidates)
        chosen = candidates[:required]
        if len(chosen) < required:
            shortages[stratum] = required - len(chosen)
        for row in chosen:
            selected_ids.add(row["vk_id"])
            sample.append(
                {
                    "vk_id": row["vk_id"],
                    "stratum": stratum,
                    "name": row.get("name", ""),
                    "description": row.get("description", ""),
                    "status": row.get("status", ""),
                    "matched_keywords": row.get("matched_keywords", []),
                    "original_decision": row["final_approved"],
                    "original_labels": row["final_labels"],
                }
            )
    sample_payload = {
        "seed": AUDIT_SEED,
        "required": dict(_STRATA),
        "shortages": shortages,
        "items": sample,
    }
    result_template = {"seed": AUDIT_SEED, "results": []}
    summary = {
        "seed": AUDIT_SEED,
        "decision": "not_ready",
        "required_results": sum(size for _, size in _STRATA),
        "sampled_results": len(sample),
        "shortages": shortages,
    }
    report = (
        "# Независимый аудит food_service\n\n"
        f"Seed: `{AUDIT_SEED}`. В выборке: {len(sample)} из 500.\n\n"
        "Статус: ожидаются независимые audited decisions. Импорт и incremental run "
        "до решения `passed` запрещены.\n"
    )
    _write_text_atomic(
        output_dir / "sample.json", json.dumps(sample_payload, ensure_ascii=False, indent=2)
    )
    _write_text_atomic(
        output_dir / "audit-results.json",
        json.dumps(result_template, ensure_ascii=False, indent=2),
    )
    _write_text_atomic(
        output_dir / "summary.json", json.dumps(summary, ensure_ascii=False, indent=2)
    )
    _write_text_atomic(output_dir / "AUDIT_REPORT.md", report)
    return summary


def evaluate_audit(source: Path) -> dict[str, Any]:
    """Рассчитать quality gates и вернуть машиночитаемое решение."""
    document = AuditDocument.model_validate_json(source.read_text(encoding="utf-8"))
    results = document.results
    predicted_positive = [item for item in results if "food_service" in item.original_labels]
    audited_positive = [item for item in results if "food_service" in item.audited_labels]
    true_positive = sum("food_service" in item.audited_labels for item in predicted_positive)
    false_negative = sum("food_service" not in item.original_labels for item in audited_positive)
    precision = true_positive / len(predicted_positive) if predicted_positive else 0.0
    false_negative_rate = false_negative / len(audited_positive) if audited_positive else 0.0
    multi = [item for item in results if item.stratum == "food_service_and_delivery"]
    multi_exact = (
        sum(set(item.original_labels) == set(item.audited_labels) for item in multi) / len(multi)
        if multi
        else 0.0
    )
    systematic: dict[str, dict[str, float | int]] = {}
    for word in ("кафе", "ресторан", "бар"):
        rows = [item for item in results if word in f"{item.name} {item.description}".casefold()]
        errors = sum(item.error_type not in {"correct", "ambiguous"} for item in rows)
        systematic[word] = {
            "sample": len(rows),
            "errors": errors,
            "error_rate": errors / len(rows) if rows else 0.0,
        }
    systematic_passed = all(
        values["sample"] < 10 or values["error_rate"] <= 0.10 for values in systematic.values()
    )
    required = sum(size for _, size in _STRATA)
    counts = Counter(item.error_type for item in results)
    passed = (
        len(results) >= required
        and precision >= 0.90
        and false_negative_rate <= 0.10
        and multi_exact >= 0.85
        and systematic_passed
    )
    return {
        "seed": AUDIT_SEED,
        "decision": "passed" if passed else "failed",
        "results": len(results),
        "precision": round(precision, 4),
        "false_negative_rate": round(false_negative_rate, 4),
        "multi_label_exact_match": round(multi_exact, 4),
        "systematic_keyword_checks": systematic,
        "error_types": dict(counts),
        "thresholds": {
            "precision_min": 0.90,
            "false_negative_rate_max": 0.10,
            "multi_label_exact_match_min": 0.85,
        },
    }

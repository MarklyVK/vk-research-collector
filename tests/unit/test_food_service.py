import pytest
from pydantic import ValidationError

from vk_collector.classification.audit import AUDIT_SEED, AuditDocument, AuditResult
from vk_collector.classification.reclassification import ReclassificationDecision


def test_existing_labels_are_preserved_when_food_service_is_added() -> None:
    decision = ReclassificationDecision(
        vk_id=1,
        previous_approved=True,
        previous_labels=["food_delivery"],
        food_service=True,
        final_approved=True,
        final_labels=["food_delivery", "food_service"],
        confidence=0.95,
        reason="Описание подтверждает действующий ресторан с собственной доставкой",
    )
    assert set(decision.final_labels) == {"food_delivery", "food_service"}


def test_rejected_group_can_become_food_service_approved() -> None:
    decision = ReclassificationDecision(
        vk_id=2,
        previous_approved=False,
        previous_labels=[],
        food_service=True,
        final_approved=True,
        final_labels=["food_service"],
        confidence=0.94,
        reason="Описание прямо указывает на действующее кафе с залом для посетителей",
    )
    assert decision.final_approved


def test_reclassification_cannot_remove_old_label() -> None:
    with pytest.raises(ValidationError, match="не может удалять"):
        ReclassificationDecision(
            vk_id=3,
            previous_approved=True,
            previous_labels=["food_delivery"],
            food_service=True,
            final_approved=True,
            final_labels=["food_service"],
            confidence=0.9,
            reason="Описание подтверждает заведение общественного питания",
        )


def test_audit_requires_fixed_seed_and_unique_groups() -> None:
    result = AuditResult(
        vk_id=1,
        stratum="approved_food_service",
        original_decision=True,
        original_labels=["food_service"],
        audited_decision=True,
        audited_labels=["food_service"],
        confidence=0.95,
        reason="Независимый аудитор подтвердил действующее кафе по описанию",
        error_type="correct",
    )
    with pytest.raises(ValidationError):
        AuditDocument(seed=AUDIT_SEED + 1, results=[result])
    with pytest.raises(ValidationError):
        AuditDocument(seed=AUDIT_SEED, results=[result, result])

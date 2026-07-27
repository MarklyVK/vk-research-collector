import pytest
from pydantic import ValidationError

from vk_collector.classification.schemas import (
    ClassificationImport,
    FullResult,
    build_decisions,
)


def test_minimal_import_rejects_rest_and_derives_labels() -> None:
    payload = ClassificationImport(batch_id="batch", approved_group_ids=[1])
    decisions = build_decisions(payload, {1, 2}, {1: {"food_delivery"}, 2: {"tender_support"}})
    assert [(item.vk_id, item.approved) for item in decisions] == [(1, True), (2, False)]
    assert decisions[0].labels == {"food_delivery"}
    assert decisions[1].labels == set()


def test_unknown_and_duplicate_ids_are_rejected() -> None:
    with pytest.raises(ValidationError):
        ClassificationImport(batch_id="batch", approved_group_ids=[1, 1])
    with pytest.raises(ValueError, match="не содержит"):
        build_decisions(
            ClassificationImport(batch_id="batch", approved_group_ids=[3]),
            {1, 2},
            {},
        )


def test_full_import_requires_every_group() -> None:
    payload = ClassificationImport(
        batch_id="batch",
        results=[FullResult(vk_id=1, approved=True, labels=["food_delivery"], confidence=0.9)],
    )
    with pytest.raises(ValueError, match="пропущены"):
        build_decisions(payload, {1, 2}, {})


def test_full_import_supports_multiple_labels() -> None:
    payload = ClassificationImport(
        batch_id="batch",
        results=[
            FullResult(
                vk_id=1,
                approved=True,
                labels=["food_delivery", "customer_acquisition"],
                confidence=0.94,
            )
        ],
    )
    [decision] = build_decisions(payload, {1}, {})
    assert decision.labels == {"food_delivery", "customer_acquisition"}


def test_confidence_range_is_enforced() -> None:
    with pytest.raises(ValidationError):
        FullResult(vk_id=1, approved=True, labels=[], confidence=1.1)

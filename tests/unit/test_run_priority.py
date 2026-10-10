import uuid
from datetime import UTC, datetime, timedelta

import pytest

from vk_collector.collection.reporting import (
    SCHEDULER_PRIORITY_KEY,
    priority_active,
    scheduler_priority,
    scheduling_order,
)

NOW = datetime(2026, 10, 11, 9, 0, tzinfo=UTC)


def test_priority_is_time_boxed_and_expires_by_itself() -> None:
    value = scheduler_priority(24, now=NOW)
    configuration = {SCHEDULER_PRIORITY_KEY: value}
    assert value["level"] == "high"
    assert priority_active(configuration, now=NOW + timedelta(hours=23, minutes=59))
    assert not priority_active(configuration, now=NOW + timedelta(hours=24))
    assert not priority_active({}, now=NOW)
    assert not priority_active({SCHEDULER_PRIORITY_KEY: {"level": "high", "until": "x"}}, now=NOW)
    # Naive until не принимается: все даты в UTC.
    assert not priority_active(
        {SCHEDULER_PRIORITY_KEY: {"level": "high", "until": "2026-10-12T09:00:00"}}, now=NOW
    )


@pytest.mark.parametrize("hours", [0, -1, 24.5, 48])
def test_priority_duration_is_bounded(hours: float) -> None:
    with pytest.raises(ValueError):
        scheduler_priority(hours, now=NOW)


def test_scheduling_order_puts_high_first_and_keeps_fair_rotation() -> None:
    a, b, c, perm = (uuid.uuid4() for _ in range(4))
    targets = [a, b, perm, c]
    assert scheduling_order(targets, [], 1) == [b, perm, c, a]
    assert scheduling_order(targets, [perm], 0) == [perm, a, b, c]
    assert scheduling_order(targets, [perm], 1) == [perm, b, c, a]
    assert scheduling_order(targets, [perm], 5) == [perm, c, a, b]
    assert scheduling_order([perm], [perm], 3) == [perm]

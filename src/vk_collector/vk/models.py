"""Типизированные объекты, извлекаемые из ответов VK."""

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True, slots=True)
class VKGroup:
    vk_id: int
    name: str
    description: str
    status: str
    screen_name: str | None
    address: str

    @classmethod
    def from_api(cls, value: dict[str, Any]) -> "VKGroup":
        group_id = int(value["id"])
        screen_name_value = value.get("screen_name")
        screen_name = str(screen_name_value) if screen_name_value else None
        address_part = screen_name or f"club{group_id}"
        return cls(
            vk_id=group_id,
            name=str(value.get("name", "")),
            description=str(value.get("description", "")),
            status=str(value.get("status", "")),
            screen_name=screen_name,
            address=f"https://vk.com/{address_part}",
        )


@dataclass(frozen=True, slots=True)
class VKSearchPage:
    total: int
    items: tuple[VKGroup, ...]
    raw_count: int = 0
    private_count: int = 0
    deleted_count: int = 0


@dataclass(frozen=True, slots=True)
class VKSubscriptionIDsPage:
    """Normalized ID-only page returned by ``groups.get``."""

    total_reported: int
    group_ids: tuple[int, ...]
    offset: int
    requested_count: int
    returned_count: int
    next_offset: int

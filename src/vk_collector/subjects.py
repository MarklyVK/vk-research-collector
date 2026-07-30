"""Единый реестр поддерживаемых предметных областей."""

from enum import StrEnum
from typing import Literal, TypeAlias

SubjectName: TypeAlias = Literal[
    "food_delivery",
    "customer_acquisition",
    "tender_support",
    "food_service",
]


class Subject(StrEnum):
    FOOD_DELIVERY = "food_delivery"
    CUSTOMER_ACQUISITION = "customer_acquisition"
    TENDER_SUPPORT = "tender_support"
    FOOD_SERVICE = "food_service"


SUBJECT_NAMES: tuple[SubjectName, ...] = (
    "food_delivery",
    "customer_acquisition",
    "tender_support",
    "food_service",
)

SUBJECT_TITLES: dict[SubjectName, str] = {
    "food_delivery": "Доставка еды",
    "customer_acquisition": "Привлечение клиентов",
    "tender_support": "Тендеры и торги",
    "food_service": "Общепит",
}

SUBJECT_DESCRIPTIONS: dict[SubjectName, str] = {
    "food_delivery": "Доставка готовой еды и продуктов.",
    "customer_acquisition": "Привлечение клиентов, маркетинг и продажи.",
    "tender_support": "Тендеры, закупки и сопровождение торгов.",
    "food_service": (
        "Кафе, рестораны и другие заведения, которые непосредственно готовят "
        "и продают готовую еду или напитки посетителям."
    ),
}


def ensure_subject(value: str) -> SubjectName:
    """Проверить машинное имя и вернуть его с точным типом."""
    if value not in SUBJECT_NAMES:
        allowed = ", ".join(SUBJECT_NAMES)
        raise ValueError(f"Неизвестная предметная область {value!r}; разрешены: {allowed}")
    return value  # type: ignore[return-value]

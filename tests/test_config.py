from pathlib import Path

import pytest
from pydantic import SecretStr

from vk_collector.config import Settings, load_keyword_config
from vk_collector.subjects import SUBJECT_NAMES


def test_database_password_is_url_encoded_and_hidden() -> None:
    settings = Settings(
        postgres_password=SecretStr("secret:@/# value"),
        database_url=None,
    )
    assert "secret%3A%40%2F%23%20value" in settings.sqlalchemy_url
    assert "secret:@/# value" not in repr(settings)


def test_existing_subscription_page_size_500_remains_compatible() -> None:
    settings = Settings(
        collection_subscriptions_page_size=500,
        collection_subscriptions_max_per_user=100,
    )
    assert settings.collection_subscriptions_page_size == 500
    assert settings.collection_subscriptions_max_per_user == 100


def test_all_four_subjects_and_food_service_keywords_are_loaded() -> None:
    config = load_keyword_config()
    assert config.subjects == SUBJECT_NAMES
    food_service = [item.keyword for item in config.keywords if item.subject == "food_service"]
    assert len(food_service) == 28
    assert food_service[:4] == ["ресторан", "кафе", "кофейня", "столовая"]
    assert not {"еда", "кухня", "вкусно", "обед", "кофе", "бар", "доставка"} & set(food_service)


def test_normalized_keyword_duplicate_is_rejected(tmp_path: Path) -> None:
    source = Path("config/keywords.yml").read_text(encoding="utf-8")
    duplicate = source.replace('      - "кафе"', '      - "кафе"\n      - "КАФЕ"')
    target = tmp_path / "keywords.yml"
    target.write_text(duplicate, encoding="utf-8")
    with pytest.raises(ValueError, match="Дублирующееся"):
        load_keyword_config(target)


def test_unknown_subject_is_rejected(tmp_path: Path) -> None:
    source = Path("config/keywords.yml").read_text(encoding="utf-8")
    target = tmp_path / "keywords.yml"
    target.write_text(source.replace("  food_service:", "  unknown_subject:"), encoding="utf-8")
    with pytest.raises(ValueError, match="Неизвестная"):
        load_keyword_config(target)

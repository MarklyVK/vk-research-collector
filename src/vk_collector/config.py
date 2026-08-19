from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Any
from urllib.parse import quote

import yaml
from pydantic import BaseModel, Field, SecretStr, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from vk_collector.subjects import SUBJECT_NAMES, SUBJECT_TITLES, SubjectName, ensure_subject


class Settings(BaseSettings):
    """Настройки приложения, изменяемые через переменные окружения."""

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    app_env: str = "development"
    log_level: str = "INFO"
    vk_api_version: str = "5.199"
    vk_tokens_file: Path = Path("/run/secrets/vk_tokens.txt")
    vk_request_timeout_seconds: float = 30
    vk_max_concurrency: int = Field(default=3, ge=1)
    vk_per_token_rps: float = Field(default=2.5, gt=0)
    vk_method_flood_initial_cooldown_seconds: int = Field(default=3600, ge=1)
    vk_method_quota_initial_cooldown_seconds: int = Field(default=3600, ge=1)
    vk_method_limit_max_cooldown_seconds: int = Field(default=86400, ge=1)
    vk_method_limit_probe_seconds: int = Field(default=900, ge=1)
    vk_global_rps_cooldown_seconds: int = Field(default=60, ge=1)
    vk_limit_escalation_window_seconds: int = Field(default=60, ge=1)
    vk_limit_escalation_distinct_methods: int = Field(default=3, ge=2)
    classification_batch_size: int = Field(default=100, ge=1)
    export_dir: Path = Path("/app/exports/classification")
    postgres_db: str = "vk_research"
    postgres_user: str = "vk_collector"
    postgres_password: SecretStr = SecretStr("change_me")
    postgres_host: str = "postgres"
    postgres_port: int = 5432
    database_url: str | None = None
    telegram_enabled: bool = False
    telegram_bot_token: SecretStr = SecretStr("")
    telegram_bot_token_file: Path = Path("/run/secrets/telegram_bot_token.txt")
    telegram_chat_id: str = ""
    telegram_timezone: str = "Europe/Moscow"
    telegram_daily_time: str = "09:00"
    telegram_health_interval_seconds: int = Field(default=300, ge=60)
    telegram_alert_repeat_seconds: int = Field(default=10_800, ge=60)
    telegram_stall_minutes: int = Field(default=30, ge=5)
    telegram_disk_warning_percent: int = Field(default=85, ge=1, le=100)
    telegram_disk_critical_percent: int = Field(default=95, ge=1, le=100)
    telegram_ram_warning_available_mb: int = Field(default=100, ge=1)
    disk_warning_percent: int = 85
    disk_stop_percent: int = 95
    collection_worker_id: str = "collector-1"
    collection_max_concurrency: int = Field(default=3, ge=1, le=10)
    collection_job_lease_seconds: int = Field(default=300, ge=30)
    collection_job_heartbeat_seconds: int = Field(default=60, ge=10)
    collection_idle_sleep_seconds: float = Field(default=5, ge=0.1)
    collection_posts_enabled: bool = True
    collection_posts_max_per_group: int = Field(default=100, ge=1)
    collection_posts_page_size: int = Field(default=100, ge=1, le=100)
    collection_posts_include_pinned: bool = True
    collection_posts_stop_at_date: str = ""
    collection_members_enabled: bool = True
    collection_members_max_per_group: int | None = Field(default=200, ge=1)
    collection_members_page_size: int = Field(default=1000, ge=1, le=1000)
    collection_users_enabled: bool = True
    collection_user_profile_ttl_days: int = Field(default=30, ge=1)
    collection_user_batch_size: int = Field(default=1000, ge=1, le=1000)
    collection_user_posts_enabled: bool = True
    collection_user_posts_max_per_user: int = Field(default=20, ge=1, le=100)
    collection_user_posts_page_size: int = Field(default=20, ge=1, le=100)
    collection_user_posts_window_days: int = Field(default=180, ge=1)
    collection_user_posts_ttl_days: int = Field(default=30, ge=1)
    collection_user_posts_stop_at_date: str = ""
    collection_subscriptions_enabled: bool = False
    collection_subscriptions_max_per_user: int = Field(default=50, ge=1, le=50)
    collection_subscriptions_page_size: int = Field(default=50, ge=1, le=1000)
    collection_subscriptions_users_per_run: int = Field(default=10000, ge=1)
    collection_subscriptions_ttl_days: int = Field(default=30, ge=1)
    collection_campaign_cohort_users: int = Field(default=10_000, ge=1, le=10_000)
    collection_light_repair_cohort_size: int = Field(default=10_000, ge=1, le=10_000)
    collection_community_metadata_batch_size: int = Field(default=100, ge=1, le=100)
    collection_community_metadata_ttl_days: int = Field(default=30, ge=1)
    collection_scheduler_quantum: int = Field(default=10, ge=1, le=100)
    collection_subscription_pilot_users: int = Field(default=500, ge=1, le=500)
    collection_subscription_pilot_min_users: int = Field(default=100, ge=1, le=500)
    collection_subscription_group_posts_enabled: bool = False
    collection_subscription_group_posts_max: int = Field(default=20, ge=1, le=20)
    collection_subscription_group_posts_ttl_days: int = Field(default=30, ge=1)
    collection_subscription_posts_pilot_communities: int = Field(default=500, ge=1, le=5000)
    collection_subscription_posts_pilot_min_communities: int = Field(default=50, ge=1, le=500)
    collection_capacity_report_max_age_days: int = Field(default=30, ge=1)
    collection_pilot_seed: int = 20260728
    collection_pilot_groups_per_category: int = Field(default=10, ge=1)
    collection_export_dir: Path = Path("/app/exports/stage2-pilot")

    @field_validator(
        "collection_members_max_per_group",
        mode="before",
    )
    @classmethod
    def blank_limit_is_none(cls, value: object) -> object:
        return None if value == "" else value

    @field_validator("vk_method_limit_max_cooldown_seconds")
    @classmethod
    def method_max_not_shorter_than_initial(cls, value: int, info: Any) -> int:
        flood = info.data.get("vk_method_flood_initial_cooldown_seconds", 3600)
        quota = info.data.get("vk_method_quota_initial_cooldown_seconds", 3600)
        if value < max(int(flood), int(quota)):
            raise ValueError("max cooldown не может быть короче начального cooldown")
        return value

    @model_validator(mode="after")
    def pilot_minimums_fit_samples(self) -> Settings:
        """Не разрешать minimum, который невозможно набрать заданным pilot."""
        if self.collection_subscription_pilot_min_users > self.collection_subscription_pilot_users:
            raise ValueError("minimum Pilot A не может превышать размер Pilot A")
        if (
            self.collection_subscription_posts_pilot_min_communities
            > self.collection_subscription_posts_pilot_communities
        ):
            raise ValueError("minimum Pilot B не может превышать размер Pilot B")
        return self

    @property
    def sqlalchemy_url(self) -> str:
        """Вернуть URL SQLAlchemy, не предназначенный для логирования."""
        if self.database_url:
            return self.database_url
        password = quote(self.postgres_password.get_secret_value(), safe="")
        return (
            f"postgresql+asyncpg://{self.postgres_user}:{password}@"
            f"{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )

    @property
    def telegram_token(self) -> str:
        """Вернуть runtime token из env или отдельного файла, не логируя его."""
        inline = self.telegram_bot_token.get_secret_value().strip()
        if inline:
            return inline
        try:
            return self.telegram_bot_token_file.read_text(encoding="utf-8").strip()
        except (OSError, UnicodeDecodeError):
            return ""


class Keyword(BaseModel):
    subject: SubjectName
    keyword: str


class KeywordConfig(BaseModel):
    community_types: list[str]
    subjects: tuple[SubjectName, ...]
    keywords: list[Keyword]


def _keyword_identity(value: str) -> str:
    """Нормализовать значение только для поиска конфигурационных дублей."""
    return " ".join(value.strip().casefold().replace("ё", "е").split())


def load_keyword_config(path: Path = Path("config/keywords.yml")) -> KeywordConfig:
    """Загрузить и нормализовать предметные области и ключевые слова."""
    raw_payload: Any = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(raw_payload, dict):
        raise ValueError("Конфигурация ключевых слов должна быть объектом YAML")
    payload: dict[str, Any] = raw_payload
    raw_subjects = payload.get("subjects", {})
    if not isinstance(raw_subjects, dict):
        raise ValueError("Раздел subjects должен быть объектом YAML")
    subject_names = tuple(ensure_subject(str(value)) for value in raw_subjects)
    if subject_names != SUBJECT_NAMES:
        raise ValueError(
            "Раздел subjects должен содержать четыре области в стабильном порядке: "
            + ", ".join(SUBJECT_NAMES)
        )

    keywords: list[Keyword] = []
    seen: dict[str, SubjectName] = {}
    for subject in subject_names:
        details = raw_subjects[subject]
        if not isinstance(details, dict):
            raise ValueError(f"Настройки {subject} должны быть объектом YAML")
        if details.get("title") != SUBJECT_TITLES[subject]:
            raise ValueError(f"Некорректное русское название предметной области {subject}")
        values = details.get("keywords", [])
        if not isinstance(values, list) or not values:
            raise ValueError(f"Для {subject} нужен непустой список ключевых слов")
        for raw_value in values:
            if not isinstance(raw_value, str) or not raw_value.strip():
                raise ValueError(f"Пустое или некорректное ключевое слово в {subject}")
            value = raw_value.strip()
            identity = _keyword_identity(value)
            previous = seen.get(identity)
            if previous is not None:
                raise ValueError(f"Дублирующееся ключевое слово {value!r}: {previous} и {subject}")
            seen[identity] = subject
            keywords.append(Keyword(subject=subject, keyword=value))

    raw_types = payload.get("search", {}).get("community_types", ["group"])
    if not isinstance(raw_types, list) or not raw_types:
        raise ValueError("search.community_types должен быть непустым списком")
    return KeywordConfig(
        community_types=[str(value) for value in raw_types],
        subjects=subject_names,
        keywords=keywords,
    )


@lru_cache
def get_settings() -> Settings:
    """Вернуть кэшированные настройки процесса."""
    return Settings()

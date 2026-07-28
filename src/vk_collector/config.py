from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Any
from urllib.parse import quote

import yaml
from pydantic import BaseModel, Field, SecretStr, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


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
    telegram_chat_id: str = ""
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
    collection_subscriptions_enabled: bool = False
    collection_subscriptions_max_per_user: int | None = Field(default=None, ge=1)
    collection_subscriptions_page_size: int = Field(default=500, ge=1, le=1000)
    collection_pilot_seed: int = 20260728
    collection_pilot_groups_per_category: int = Field(default=10, ge=1)
    collection_export_dir: Path = Path("/app/exports/stage2-pilot")

    @field_validator(
        "collection_members_max_per_group",
        "collection_subscriptions_max_per_user",
        mode="before",
    )
    @classmethod
    def blank_limit_is_none(cls, value: object) -> object:
        return None if value == "" else value

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


class Keyword(BaseModel):
    subject: str
    keyword: str


class KeywordConfig(BaseModel):
    community_types: list[str]
    keywords: list[Keyword]


def load_keyword_config(path: Path = Path("config/keywords.yml")) -> KeywordConfig:
    """Загрузить и нормализовать предметные области и ключевые слова."""
    payload: dict[str, Any] = yaml.safe_load(path.read_text(encoding="utf-8"))
    subjects = payload.get("subjects", {})
    keywords = [
        Keyword(subject=subject, keyword=value)
        for subject, details in subjects.items()
        for value in details.get("keywords", [])
    ]
    return KeywordConfig(
        community_types=payload.get("search", {}).get("community_types", ["group"]),
        keywords=keywords,
    )


@lru_cache
def get_settings() -> Settings:
    """Вернуть кэшированные настройки процесса."""
    return Settings()

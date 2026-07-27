from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Any
from urllib.parse import quote

import yaml
from pydantic import BaseModel, Field, SecretStr
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
    disk_warning_percent: int = 85
    disk_stop_percent: int = 95

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

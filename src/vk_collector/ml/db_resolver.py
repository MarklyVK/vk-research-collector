"""Безопасный резолвер подключений к локальной и удаленной базе данных PostgreSQL."""

from __future__ import annotations

import logging
import ssl
from pathlib import Path
from urllib.parse import quote

from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from vk_collector.ml.config import MLSettings

logger = logging.getLogger(__name__)


def resolve_database_password(settings: MLSettings) -> str:
    """Извлечь пароль БД из защищенного файла секрета или конфигурации."""
    if settings.remote_postgres_password_file:
        try:
            return settings.remote_postgres_password_file.read_text(encoding="utf-8").strip()
        except OSError as e:
            logger.warning(
                "Не удалось прочитать файл пароля %s: %s",
                settings.remote_postgres_password_file,
                e,
            )

    return settings.remote_postgres_password


def build_ml_database_url(settings: MLSettings, mask_password: bool = False) -> str:
    """Собрать URL подключения asyncpg с безопасной маскировкой или шифрованием."""
    if settings.database_url:
        if mask_password and "@" in settings.database_url:
            # Маскируем пароль в строке вида postgresql+asyncpg://user:pass@host:port/db
            prefix, rest = settings.database_url.split("@", 1)
            if "://" in prefix and ":" in prefix.split("://", 1)[1]:
                scheme_user, _ = prefix.split("://", 1)[1].split(":", 1)
                scheme = prefix.split("://", 1)[0]
                return f"{scheme}://{scheme_user}:***@{rest}"
        return settings.database_url

    host = settings.remote_postgres_host or "127.0.0.1"
    port = settings.remote_postgres_port
    db = settings.remote_postgres_db
    user = settings.remote_postgres_user

    if mask_password:
        return f"postgresql+asyncpg://{user}:***@{host}:{port}/{db}"

    raw_pass = resolve_database_password(settings)
    quoted_pass = quote(raw_pass, safe="")
    return f"postgresql+asyncpg://{user}:{quoted_pass}@{host}:{port}/{db}"


def create_ml_database_engine(
    url: str,
    ssl_mode: str = "prefer",
    ssl_ca_file: Path | None = None,
    pool_size: int = 5,
    max_overflow: int = 10,
    connect_timeout: int = 30,
) -> AsyncEngine:
    """Создать асинхронный движок SQLAlchemy с пулом соединений и SSL-контекстом."""
    connect_args: dict[str, object] = {
        "timeout": connect_timeout,
        "command_timeout": 120,
    }

    if ssl_mode in ("require", "verify-ca", "verify-full"):
        ctx = ssl.create_default_context()
        if ssl_mode == "require":
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE
        elif ssl_ca_file and ssl_ca_file.exists():
            ctx.load_verify_locations(str(ssl_ca_file))
        connect_args["ssl"] = ctx
    elif ssl_mode == "disable":
        connect_args["ssl"] = False

    engine = create_async_engine(
        url,
        pool_size=pool_size,
        max_overflow=max_overflow,
        pool_pre_ping=True,
        pool_recycle=1800,
        connect_args=connect_args,
    )
    return engine


def create_ml_session_factory(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    """Создать фабрику асинхронных сессий SQLAlchemy."""
    return async_sessionmaker(
        bind=engine,
        class_=AsyncSession,
        expire_on_commit=False,
    )

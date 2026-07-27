"""Создание async engine и фабрики транзакционных сессий."""

from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)


def create_database_engine(database_url: str, *, echo: bool = False) -> AsyncEngine:
    """Создать экономный engine, подходящий серверу с 1 GB RAM."""

    return create_async_engine(
        database_url,
        echo=echo,
        pool_size=3,
        max_overflow=2,
        pool_pre_ping=True,
    )


def create_session_factory(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    """Создать фабрику async-сессий без неявного истечения объектов."""

    return async_sessionmaker(engine, expire_on_commit=False, autoflush=False)

from pathlib import Path

from vk_collector.ml.config import MLSettings
from vk_collector.ml.db_resolver import (
    build_ml_database_url,
    create_ml_database_engine,
    create_ml_session_factory,
    resolve_database_password,
)


def test_resolve_password_from_file(tmp_path: Path) -> None:
    secret_file = tmp_path / "secret.txt"
    secret_file.write_text("super_secret_pass_123\n", encoding="utf-8")

    settings = MLSettings(
        remote_postgres_password="fallback_pass",
        remote_postgres_password_file=secret_file,
    )

    pwd = resolve_database_password(settings)
    assert pwd == "super_secret_pass_123"


def test_resolve_password_from_config() -> None:
    settings = MLSettings(
        remote_postgres_password="direct_pass",
        remote_postgres_password_file=None,
    )
    pwd = resolve_database_password(settings)
    assert pwd == "direct_pass"


def test_build_ml_database_url_masked_and_raw() -> None:
    settings = MLSettings(
        remote_postgres_host="db.cloud.ru",
        remote_postgres_port=5432,
        remote_postgres_db="vk_research",
        remote_postgres_user="vk_collector",
        remote_postgres_password="secret_pass_#1!",
    )

    url_raw = build_ml_database_url(settings, mask_password=False)
    assert "secret_pass_%231%21" in url_raw
    assert "db.cloud.ru:5432/vk_research" in url_raw

    url_masked = build_ml_database_url(settings, mask_password=True)
    assert "secret_pass" not in url_masked
    assert "vk_collector:***@db.cloud.ru:5432/vk_research" in url_masked


def test_build_ml_database_url_with_custom_url() -> None:
    settings = MLSettings(database_url="postgresql+asyncpg://myuser:mypass@remote.host:5432/testdb")
    url_masked = build_ml_database_url(settings, mask_password=True)
    assert "mypass" not in url_masked
    assert "myuser:***@remote.host:5432/testdb" in url_masked


def test_create_engine_and_session_factory() -> None:
    engine = create_ml_database_engine(
        "postgresql+asyncpg://user:pass@localhost:5432/db",
        ssl_mode="disable",
    )
    session_factory = create_ml_session_factory(engine)
    assert session_factory is not None
    assert engine.pool is not None

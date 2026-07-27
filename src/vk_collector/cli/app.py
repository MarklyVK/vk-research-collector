from __future__ import annotations

import asyncio
import time
from pathlib import Path
from typing import Annotated, Any

import typer
from sqlalchemy import func, select

from vk_collector.classification.service import (
    classification_summary,
    export_batch,
    import_classification,
)
from vk_collector.config import get_settings, load_keyword_config
from vk_collector.database.models import GroupCandidate, GroupKeywordMatch, SearchRun
from vk_collector.database.session import create_database_engine, create_session_factory
from vk_collector.search.postgres import PostgresSearchPersistence
from vk_collector.search.service import Keyword, SearchService
from vk_collector.vk import TokenPool, VKClient, load_tokens

app = typer.Typer(help="Поиск и ручная классификация сообществ VK.")
groups_app = typer.Typer(help="Поиск и статистика групп.")
classification_app = typer.Typer(help="Пакеты ручной классификации.")
collection_app = typer.Typer(help="Будущий основной сбор данных.")
app.add_typer(groups_app, name="groups")
app.add_typer(classification_app, name="classification")
app.add_typer(collection_app, name="collection")


async def _run_search() -> str:
    settings = get_settings()
    keyword_config = load_keyword_config()
    engine = create_database_engine(settings.sqlalchemy_url)
    sessions = create_session_factory(engine)
    pool = TokenPool(
        load_tokens(settings.vk_tokens_file),
        rps=settings.vk_per_token_rps,
        clock=time.monotonic,
    )
    client = VKClient(
        pool,
        api_version=settings.vk_api_version,
        timeout=settings.vk_request_timeout_seconds,
    )
    try:
        service = SearchService(
            client,
            PostgresSearchPersistence(sessions),
            group_types=tuple(keyword_config.community_types),
        )
        return await service.run(
            tuple(
                Keyword(value=item.keyword, subject=item.subject)
                for item in keyword_config.keywords
            )
        )
    finally:
        await client.aclose()
        await engine.dispose()


@groups_app.command("search")
def search_groups() -> None:
    """Запустить новый поиск или продолжить незавершённый."""
    run_id = asyncio.run(_run_search())
    typer.echo(f"Поиск завершён или приостановлен. ID запуска: {run_id}")


async def _group_summary() -> dict[str, int]:
    settings = get_settings()
    engine = create_database_engine(settings.sqlalchemy_url)
    sessions = create_session_factory(engine)
    try:
        async with sessions() as session:
            groups = await session.scalar(select(func.count(GroupCandidate.id))) or 0
            matches = await session.scalar(select(func.count(GroupKeywordMatch.id))) or 0
            runs = await session.scalar(select(func.count(SearchRun.id))) or 0
            return {"groups": groups, "matches": matches, "runs": runs}
    finally:
        await engine.dispose()


@groups_app.command("summary")
def groups_summary() -> None:
    """Показать статистику поиска кандидатов."""
    summary = asyncio.run(_group_summary())
    typer.echo(f"Уникальных групп: {summary['groups']}")
    typer.echo(f"Совпадений с ключевыми словами: {summary['matches']}")
    typer.echo(f"Запусков поиска: {summary['runs']}")


async def _export() -> Path | None:
    settings = get_settings()
    engine = create_database_engine(settings.sqlalchemy_url)
    sessions = create_session_factory(engine)
    try:
        async with sessions() as session:
            return await export_batch(
                session, settings.export_dir, settings.classification_batch_size
            )
    finally:
        await engine.dispose()


@classification_app.command("export")
def export_classification() -> None:
    """Экспортировать следующий фиксированный пакет pending-групп."""
    target = asyncio.run(_export())
    typer.echo(f"Пакет сохранён: {target}" if target else "Нет новых групп для экспорта.")


async def _import(source: Path) -> int:
    settings = get_settings()
    engine = create_database_engine(settings.sqlalchemy_url)
    sessions = create_session_factory(engine)
    try:
        async with sessions() as session:
            try:
                return await import_classification(session, source)
            except Exception:
                await session.rollback()
                raise
    finally:
        await engine.dispose()


@classification_app.command("import")
def import_results(
    source: Annotated[Path, typer.Argument(exists=True, dir_okay=False)],
) -> None:
    """Импортировать JSON ручной классификации транзакционно."""
    try:
        count = asyncio.run(_import(source))
    except ValueError as exc:
        typer.echo(f"Импорт отклонён: {exc}", err=True)
        raise typer.Exit(code=2) from exc
    typer.echo(f"Классификация применена к {count} группам.")


async def _classification_summary() -> dict[str, Any]:
    settings = get_settings()
    engine = create_database_engine(settings.sqlalchemy_url)
    sessions = create_session_factory(engine)
    try:
        async with sessions() as session:
            return await classification_summary(session)
    finally:
        await engine.dispose()


def _print_classification_summary(summary: dict[str, Any]) -> None:
    typer.echo(f"Pending: {summary['pending']}")
    typer.echo(f"Approved: {summary['approved']}")
    typer.echo(f"Rejected: {summary['rejected']}")
    typer.echo("Approved по областям:")
    for label, count in sorted(summary["approved_by_label"].items()):
        typer.echo(f"  {label}: {count}")


@classification_app.command("summary")
def show_classification_summary() -> None:
    """Показать результаты ручной классификации."""
    _print_classification_summary(asyncio.run(_classification_summary()))


@collection_app.command("start")
def start_collection() -> None:
    """Показать будущий набор групп, не запуская основной сбор."""
    summary = asyncio.run(_classification_summary())
    _print_classification_summary(summary)
    typer.echo(f"На будущем этапе будут использованы approved-группы: {summary['approved']}.")
    typer.echo("Второй этап ещё не реализован; посты и пользователи не загружались.")

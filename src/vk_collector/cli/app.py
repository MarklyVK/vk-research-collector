from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import signal
import time
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Annotated, Any, Literal

import typer
from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from vk_collector.classification.audit import evaluate_audit, prepare_audit
from vk_collector.classification.reclassification import (
    import_reclassification,
    load_reclassification,
    prepare_reclassification,
)
from vk_collector.classification.service import (
    classification_summary,
    export_batch,
    import_classification,
)
from vk_collector.collection import CollectionQueue, CollectionWorker
from vk_collector.collection.backlog import canonical_backlog
from vk_collector.collection.backup import BackupVerifier
from vk_collector.collection.campaigns import CampaignManager
from vk_collector.collection.capacity import (
    build_capacity_report,
    validate_capacity_report,
    write_capacity_report,
)
from vk_collector.collection.notifications import notify
from vk_collector.collection.reporting import (
    bounded_wakeup_delay,
    capacity_gate_passed,
    database_metrics,
    global_summary,
    latest_run_id,
    next_runnable_wakeup,
    run_summary,
    runnable_run_ids,
    verify_run,
)
from vk_collector.collection.safety import inspect_disk
from vk_collector.config import Settings, get_settings, load_keyword_config
from vk_collector.database.models import (
    CampaignStatus,
    CollectionCampaign,
    CollectionJob,
    CollectionRun,
    CollectionRunStatus,
    GroupCandidate,
    GroupKeywordMatch,
    JobStatus,
    SearchRun,
    UserGroupSubscription,
    UserSubscriptionState,
    VKCommunity,
    VKTokenMethodState,
)
from vk_collector.database.session import create_database_engine, create_session_factory
from vk_collector.privacy import delete_user, inspect_group, inspect_user
from vk_collector.search.postgres import PostgresSearchPersistence, search_run_summary
from vk_collector.search.service import Keyword, SearchService
from vk_collector.subjects import SubjectName, ensure_subject
from vk_collector.vk import TokenPool, VKClient, VKTokensUnavailable, load_tokens

logger = logging.getLogger(__name__)
app = typer.Typer(help="Поиск и ручная классификация сообществ VK.")
groups_app = typer.Typer(help="Поиск и статистика групп.")
classification_app = typer.Typer(help="Пакеты ручной классификации.")
collection_app = typer.Typer(help="Возобновляемый сбор публичных approved-данных.")
subscriptions_app = typer.Typer(help="Обогащение существующих пользователей подписками.")
campaign_app = typer.Typer(help="Многофазная кампания подписок с фиксированным snapshot.")
privacy_app = typer.Typer(help="Проверка и минимизация персональных данных.")
app.add_typer(groups_app, name="groups")
app.add_typer(classification_app, name="classification")
app.add_typer(collection_app, name="collection")
collection_app.add_typer(subscriptions_app, name="subscriptions")
collection_app.add_typer(campaign_app, name="campaign")
app.add_typer(privacy_app, name="privacy")


@app.callback()
def configure_logging() -> None:
    """Настроить безопасные UTC-логи CLI и автономного worker."""
    settings = get_settings()
    level = getattr(logging, settings.log_level.upper(), logging.INFO)
    logging.basicConfig(
        level=level,
        format="%(asctime)sZ level=%(levelname)s logger=%(name)s %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)


def _token_pool(
    settings: Settings,
    tokens: tuple[str, ...],
    sessions: async_sessionmaker[AsyncSession],
) -> TokenPool:
    """Создать endpoint-aware pool из валидированной конфигурации."""
    return TokenPool(
        tokens,
        rps=settings.vk_per_token_rps,
        clock=time.monotonic,
        flood_initial_cooldown=settings.vk_method_flood_initial_cooldown_seconds,
        quota_initial_cooldown=settings.vk_method_quota_initial_cooldown_seconds,
        max_method_cooldown=settings.vk_method_limit_max_cooldown_seconds,
        probe_seconds=settings.vk_method_limit_probe_seconds,
        global_rps_cooldown=settings.vk_global_rps_cooldown_seconds,
        escalation_window=settings.vk_limit_escalation_window_seconds,
        escalation_methods=settings.vk_limit_escalation_distinct_methods,
        sessions=sessions,
    )


async def _run_search(subject: SubjectName | None) -> tuple[str, dict[str, Any]]:
    settings = get_settings()
    keyword_config = load_keyword_config()
    engine = create_database_engine(settings.sqlalchemy_url)
    sessions = create_session_factory(engine)
    pool = _token_pool(settings, load_tokens(settings.vk_tokens_file), sessions)
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
        keywords = tuple(
            Keyword(value=item.keyword, subject=item.subject)
            for item in keyword_config.keywords
            if subject is None or item.subject == subject
        )
        run_id = await service.run(keywords)
        async with sessions() as session:
            summary = await search_run_summary(session, uuid.UUID(run_id))
        return run_id, summary
    finally:
        await client.aclose()
        await engine.dispose()


@groups_app.command("search")
def search_groups(
    subject: Annotated[
        str | None,
        typer.Option("--subject", help="Искать только одну предметную область."),
    ] = None,
) -> None:
    """Запустить новый поиск или продолжить незавершённый."""
    try:
        validated_subject = ensure_subject(subject) if subject is not None else None
        run_id, summary = asyncio.run(_run_search(validated_subject))
    except ValueError as exc:
        typer.echo(f"Поиск отклонён: {exc}", err=True)
        raise typer.Exit(code=2) from exc
    typer.echo(f"Поиск завершён или приостановлен. ID запуска: {run_id}")
    typer.echo(json.dumps(summary, ensure_ascii=False, indent=2))


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


async def _prepare_reclassification(output_dir: Path) -> dict[str, Any]:
    settings = get_settings()
    engine = create_database_engine(settings.sqlalchemy_url)
    sessions = create_session_factory(engine)
    try:
        async with sessions() as session:
            return await prepare_reclassification(session, output_dir)
    finally:
        await engine.dispose()


@classification_app.command("reclassification-prepare")
def reclassification_prepare(
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="Каталог артефактов повторной классификации."),
    ] = Path("/app/exports/food-service-reclassification"),
) -> None:
    """Создать полный snapshot для семантической проверки food_service."""
    payload = asyncio.run(_prepare_reclassification(output_dir))
    typer.echo(json.dumps(payload, ensure_ascii=False, indent=2))


@classification_app.command("reclassification-validate")
def reclassification_validate(
    source: Annotated[Path, typer.Argument(exists=True, dir_okay=False)],
) -> None:
    """Проверить формат завершённых решений без изменения базы."""
    try:
        document = load_reclassification(source)
    except ValueError as exc:
        typer.echo(f"Валидация отклонена: {exc}", err=True)
        raise typer.Exit(code=2) from exc
    typer.echo(
        json.dumps(
            {
                "valid": True,
                "operation_id": document.operation_id,
                "decisions": len(document.decisions),
            },
            ensure_ascii=False,
            indent=2,
        )
    )


async def _import_reclassification(source: Path) -> dict[str, int]:
    settings = get_settings()
    engine = create_database_engine(settings.sqlalchemy_url)
    sessions = create_session_factory(engine)
    try:
        async with sessions() as session:
            try:
                return await import_reclassification(session, source)
            except Exception:
                await session.rollback()
                raise
    finally:
        await engine.dispose()


@classification_app.command("reclassification-import")
def reclassification_import(
    source: Annotated[Path, typer.Argument(exists=True, dir_okay=False)],
    backup: Annotated[
        Path,
        typer.Option(
            "--backup",
            exists=True,
            dir_okay=False,
            help="Проверенный PostgreSQL backup, созданный непосредственно перед импортом.",
        ),
    ],
) -> None:
    """Импортировать проверенную reclassification с обязательным backup."""
    if backup.stat().st_size <= 0:
        typer.echo("Импорт отклонён: backup пуст.", err=True)
        raise typer.Exit(code=2)
    try:
        result = asyncio.run(_import_reclassification(source))
    except ValueError as exc:
        typer.echo(f"Импорт отклонён: {exc}", err=True)
        raise typer.Exit(code=2) from exc
    typer.echo(json.dumps(result, ensure_ascii=False, indent=2))


@classification_app.command("audit-prepare")
def audit_prepare(
    decisions: Annotated[Path, typer.Argument(exists=True, dir_okay=False)],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="Каталог независимого аудита."),
    ] = Path("/app/exports/food-service-audit"),
) -> None:
    """Создать фиксированную независимую выборку food_service."""
    try:
        summary = prepare_audit(decisions, output_dir)
    except ValueError as exc:
        typer.echo(f"Подготовка аудита отклонена: {exc}", err=True)
        raise typer.Exit(code=2) from exc
    typer.echo(json.dumps(summary, ensure_ascii=False, indent=2))


@classification_app.command("audit-validate")
def audit_validate(
    source: Annotated[Path, typer.Argument(exists=True, dir_okay=False)],
    output: Annotated[
        Path | None,
        typer.Option("--output", help="Путь для машиночитаемого summary.json."),
    ] = None,
) -> None:
    """Проверить независимый аудит и вычислить quality gates."""
    try:
        summary = evaluate_audit(source)
    except (OSError, ValueError) as exc:
        typer.echo(f"Аудит отклонён: {exc}", err=True)
        raise typer.Exit(code=2) from exc
    target = output or source.parent / "summary.json"
    _write_json(target, summary)
    typer.echo(json.dumps(summary, ensure_ascii=False, indent=2))
    if summary["decision"] != "passed":
        raise typer.Exit(code=1)


async def _classification_summary(subject: SubjectName | None = None) -> dict[str, Any]:
    settings = get_settings()
    engine = create_database_engine(settings.sqlalchemy_url)
    sessions = create_session_factory(engine)
    try:
        async with sessions() as session:
            return await classification_summary(session, subject)
    finally:
        await engine.dispose()


def _print_classification_summary(summary: dict[str, Any]) -> None:
    typer.echo(f"Pending: {summary['pending']}")
    typer.echo(f"Approved: {summary['approved']}")
    typer.echo(f"Rejected: {summary['rejected']}")
    typer.echo("Approved по областям:")
    for label, count in sorted(summary["approved_by_label"].items()):
        typer.echo(f"  {label}: {count}")
    if "subject" in summary:
        typer.echo("Статистика выбранной области:")
        for key, value in summary["subject"].items():
            typer.echo(f"  {key}: {value}")


@classification_app.command("summary")
def show_classification_summary(
    subject: Annotated[
        str | None,
        typer.Option("--subject", help="Фильтр по предметной области."),
    ] = None,
) -> None:
    """Показать результаты ручной классификации."""
    try:
        validated_subject = ensure_subject(subject) if subject is not None else None
        summary = asyncio.run(_classification_summary(validated_subject))
    except ValueError as exc:
        typer.echo(f"Статистика отклонена: {exc}", err=True)
        raise typer.Exit(code=2) from exc
    _print_classification_summary(summary)


@collection_app.command("start")
def start_collection() -> None:
    """Совместимая безопасная команда: показать план без создания jobs."""
    collection_plan(apply=False, pilot=False)


def _validate_audit_gate(source: Path) -> None:
    try:
        payload = json.loads(source.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"Отчёт аудита не читается: {exc}") from exc
    if payload.get("seed") != 20260730 or payload.get("decision") != "passed":
        raise ValueError("Incremental plan требует успешный аудит с seed 20260730")


async def _plan_collection(
    *,
    apply: bool,
    pilot: bool,
    incremental_from: uuid.UUID | None = None,
    reason: str = "food_service_increment",
    source: str = "food_service_expansion",
    audit_summary: Path | None = None,
) -> tuple[Any, uuid.UUID | None, float, float]:
    settings = get_settings()
    engine = create_database_engine(settings.sqlalchemy_url)
    sessions = create_session_factory(engine)
    try:
        queue = CollectionQueue(sessions, settings)
        if pilot and incremental_from is not None:
            raise ValueError("Нельзя одновременно планировать pilot и incremental run")
        preview = await queue.preview(pilot=pilot, incremental_from=incremental_from)
        disk = inspect_disk(
            settings.collection_export_dir,
            settings.disk_warning_percent,
            settings.disk_stop_percent,
        )
        run_id = None
        used_bytes = disk.total_bytes - disk.free_bytes
        projected_used_percent = (
            100.0 * (used_bytes + preview.estimated_disk_growth_bytes) / disk.total_bytes
        )
        if apply:
            if incremental_from is not None:
                if audit_summary is None:
                    raise ValueError("Для incremental plan укажите --audit-summary")
                _validate_audit_gate(audit_summary)
                capacity_passed = projected_used_percent < settings.disk_warning_percent
                run_id = await queue.plan(
                    incremental_from=incremental_from,
                    reason=reason,
                    source=source,
                    capacity_passed=capacity_passed,
                    estimated_disk_growth_bytes=preview.estimated_disk_growth_bytes,
                )
            elif disk.warning:
                raise ValueError(
                    f"Создание тяжёлых заданий запрещено: диск заполнен на {disk.used_percent:.1f}%"
                )
            else:
                run_id = await queue.plan(pilot=pilot)
        return preview, run_id, disk.used_percent, projected_used_percent
    finally:
        await engine.dispose()


@collection_app.command("plan")
def collection_plan(
    apply: Annotated[bool, typer.Option("--apply", help="Создать run и jobs.")] = False,
    pilot: Annotated[bool, typer.Option("--pilot", help="Планировать pilot-выборку.")] = False,
    incremental_from: Annotated[
        uuid.UUID | None,
        typer.Option("--incremental-from", help="Исключить snapshot базового run."),
    ] = None,
    reason: Annotated[str, typer.Option("--reason")] = "food_service_increment",
    source: Annotated[str, typer.Option("--source")] = "food_service_expansion",
    audit_summary: Annotated[
        Path | None,
        typer.Option("--audit-summary", help="Успешный summary независимого аудита."),
    ] = None,
) -> None:
    """Показать безопасный план; без --apply база не изменяется."""
    try:
        preview, run_id, disk, projected_disk = asyncio.run(
            _plan_collection(
                apply=apply,
                pilot=pilot,
                incremental_from=incremental_from,
                reason=reason,
                source=source,
                audit_summary=audit_summary,
            )
        )
    except ValueError as exc:
        typer.echo(f"Планирование отклонено: {exc}", err=True)
        raise typer.Exit(code=2) from exc
    typer.echo(f"Approved-групп: {preview.approved_groups}")
    typer.echo(f"Выбрано групп: {preview.selected_groups}")
    typer.echo(f"Scopes: {', '.join(preview.scopes)}")
    typer.echo(f"Jobs: {preview.jobs}; оценка VK-запросов: {preview.estimated_requests}")
    typer.echo(f"Заполнение диска: {disk:.1f}%")
    if incremental_from is not None:
        typer.echo(
            "Прогноз дополнительного объёма: "
            f"{preview.estimated_disk_growth_bytes} байт; прогноз заполнения: "
            f"{projected_disk:.1f}%"
        )
    for warning in preview.warnings:
        typer.echo(f"Предупреждение: {warning}")
    if run_id:
        typer.echo(f"Run создан или переиспользован: {run_id}")
    else:
        typer.echo("Задания не создавались. Для применения добавьте --apply.")


async def _execute_collection(
    run_id: uuid.UUID | None,
    scope: str | None,
    max_jobs: int | None,
    *,
    until_idle: bool,
    stop_event: asyncio.Event | None = None,
    explicit_pilot: bool = False,
) -> tuple[uuid.UUID, int]:
    settings = get_settings()
    engine = create_database_engine(settings.sqlalchemy_url)
    sessions = create_session_factory(engine)
    client: VKClient | None = None
    try:
        target = run_id or await latest_run_id(sessions)
        if target is None:
            raise ValueError("Нет спланированного запуска")
        queue = CollectionQueue(sessions, settings)
        async with sessions() as session:
            run = await session.get(CollectionRun, target)
            if run is None:
                raise ValueError("Запуск не найден")
            if run.scope in {"full", "incremental"} and not await capacity_gate_passed(
                sessions, target
            ):
                raise ValueError("Full/incremental run запрещён до успешного capacity gate")
            if run.scope in {"subscriptions_pilot", "subscription_posts_pilot"}:
                if not explicit_pilot:
                    raise ValueError(
                        "Pilot запускается только явной командой subscriptions pilot/posts-pilot"
                    )
            elif run.scope in {"subscriptions", "subscription_discovery", "subscription_metadata"}:
                if not settings.collection_subscriptions_enabled:
                    raise ValueError("Сбор подписок выключен COLLECTION_SUBSCRIPTIONS_ENABLED")
                report_path = run.configuration.get("capacity_report")
                if run.configuration.get("capacity_gate") != "passed" or not isinstance(
                    report_path, str
                ):
                    raise ValueError("Subscription run запрещён до успешного Gate A")
                expected = run.configuration.get("collection")
                if not isinstance(expected, dict):
                    raise ValueError("Subscription run содержит повреждённую конфигурацию")
                validate_capacity_report(
                    Path(report_path),
                    phase="A",
                    configuration=expected,
                    max_age_days=settings.collection_capacity_report_max_age_days,
                )
                backup = run.configuration.get("verified_backup")
                if not isinstance(backup, dict) or not isinstance(backup.get("path"), str):
                    raise ValueError("Subscription run запрещён без проверенного backup")
                _validated_backup_metadata(Path(backup["path"]), expected=backup)
            elif run.scope == "subscription_posts":
                if not settings.collection_subscription_group_posts_enabled:
                    raise ValueError(
                        "Сбор постов подписок выключен COLLECTION_SUBSCRIPTION_GROUP_POSTS_ENABLED"
                    )
                report_path = run.configuration.get("capacity_report")
                if run.configuration.get("capacity_gate") != "passed" or not isinstance(
                    report_path, str
                ):
                    raise ValueError("Subscription posts run запрещён до успешного Gate B")
                expected = run.configuration.get("collection")
                if not isinstance(expected, dict):
                    raise ValueError("Subscription posts run содержит повреждённую конфигурацию")
                validate_capacity_report(
                    Path(report_path),
                    phase="B",
                    configuration=expected,
                    max_age_days=settings.collection_capacity_report_max_age_days,
                )
                backup = run.configuration.get("verified_backup")
                if not isinstance(backup, dict) or not isinstance(backup.get("path"), str):
                    raise ValueError("Subscription posts run запрещён без проверенного backup")
                _validated_backup_metadata(Path(backup["path"]), expected=backup)
            expected_configuration = run.configuration.get("collection")
            if expected_configuration != queue.collection_configuration():
                raise ValueError(
                    "Runtime-настройки сбора не совпадают с проверенным plan/capacity report"
                )
        try:
            tokens = load_tokens(settings.vk_tokens_file)
        except VKTokensUnavailable as exc:
            await queue.set_run_status(target, CollectionRunStatus.PAUSED_NO_TOKENS, str(exc))
            return target, 0
        pool = _token_pool(settings, tokens, sessions)
        client = VKClient(
            pool,
            api_version=settings.vk_api_version,
            timeout=settings.vk_request_timeout_seconds,
        )
        stop_event = stop_event or asyncio.Event()
        loop = asyncio.get_running_loop()
        for handled_signal in (signal.SIGTERM, signal.SIGINT):
            with contextlib.suppress(NotImplementedError, RuntimeError):
                loop.add_signal_handler(handled_signal, stop_event.set)
        await notify(settings, f"Collection run {target} started")
        processed = await CollectionWorker(sessions, client, settings).run(
            target,
            scope=scope,
            max_jobs=max_jobs,
            stop_event=stop_event,
            until_idle=until_idle,
        )
        await notify(settings, f"Collection run {target} reached idle; processed={processed}")
        return target, processed
    finally:
        if client is not None:
            await client.aclose()
        await engine.dispose()


@collection_app.command("run")
def run_collection(
    run_id: Annotated[uuid.UUID | None, typer.Option("--run-id")] = None,
    scope: Annotated[str | None, typer.Option("--scope")] = None,
    max_jobs: Annotated[int | None, typer.Option("--max-jobs", min=1)] = None,
    until_idle: Annotated[bool, typer.Option("--until-idle")] = False,
) -> None:
    """Запустить foreground worker; результат всегда сохраняется в PostgreSQL."""
    if scope not in {
        None,
        "groups",
        "posts",
        "members",
        "users",
        "subscriptions",
        "metadata",
        "subscription_posts",
    }:
        typer.echo("Неизвестный scope.", err=True)
        raise typer.Exit(code=2)
    try:
        target, processed = asyncio.run(
            _execute_collection(run_id, scope, max_jobs, until_idle=until_idle)
        )
    except ValueError as exc:
        typer.echo(f"Запуск отклонён: {exc}", err=True)
        raise typer.Exit(code=2) from exc
    typer.echo(f"Run: {target}; обработано jobs: {processed}")


async def _collection_worker_service() -> None:
    """Fairly process all authorized runs with one shared VK client and token pool."""
    settings = get_settings()
    engine = create_database_engine(settings.sqlalchemy_url)
    sessions = create_session_factory(engine)
    stop_event = asyncio.Event()
    client: VKClient | None = None
    worker: CollectionWorker | None = None
    run_cursor = 0
    backup_verifier = BackupVerifier()
    loop = asyncio.get_running_loop()
    for handled_signal in (signal.SIGTERM, signal.SIGINT):
        with contextlib.suppress(NotImplementedError, RuntimeError):
            loop.add_signal_handler(handled_signal, stop_event.set)
    try:
        recovery_queue = CollectionQueue(sessions, settings)
        async with sessions() as session:
            recovery_rows = (
                await session.execute(
                    select(CollectionRun.id, CollectionRun.campaign_id).where(
                        CollectionRun.campaign_id.is_not(None),
                        CollectionRun.status.in_(
                            [
                                CollectionRunStatus.PLANNED,
                                CollectionRunStatus.RUNNING,
                                CollectionRunStatus.WAITING_METHOD_LIMIT,
                            ]
                        ),
                    )
                )
            ).all()
        recovery_campaigns: set[uuid.UUID] = set()
        for recovery_run_id, recovery_campaign_id in recovery_rows:
            await recovery_queue.recover_expired(recovery_run_id)
            if recovery_campaign_id is not None:
                recovery_campaigns.add(recovery_campaign_id)
        recovery_manager = CampaignManager(sessions, settings)
        for recovery_campaign_id in recovery_campaigns:
            await recovery_manager.reconcile(recovery_campaign_id)
        while not stop_event.is_set():
            targets = await runnable_run_ids(sessions)
            if not targets:
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(
                        stop_event.wait(), timeout=settings.collection_idle_sleep_seconds
                    )
                continue
            if client is None:
                try:
                    tokens = load_tokens(settings.vk_tokens_file)
                except VKTokensUnavailable as exc:
                    logger.warning("VK-токены пока недоступны: %s", exc)
                    with contextlib.suppress(TimeoutError):
                        await asyncio.wait_for(
                            stop_event.wait(), timeout=settings.collection_idle_sleep_seconds
                        )
                    continue
                pool = _token_pool(settings, tokens, sessions)
                client = VKClient(
                    pool,
                    api_version=settings.vk_api_version,
                    timeout=settings.vk_request_timeout_seconds,
                )
                worker = CollectionWorker(sessions, client, settings)
            ordered = targets[run_cursor:] + targets[:run_cursor]
            run_cursor = (run_cursor + 1) % len(targets)
            processed = 0
            for target in ordered:
                if stop_event.is_set():
                    break
                try:
                    await _validate_autonomous_run(
                        sessions, settings, target, backup_verifier=backup_verifier
                    )
                    assert worker is not None
                    processed += await worker.run(
                        target,
                        max_jobs=settings.collection_scheduler_quantum,
                        stop_event=stop_event,
                        until_idle=True,
                    )
                except ValueError as exc:
                    queue = CollectionQueue(sessions, settings)
                    await queue.set_run_status(
                        target, CollectionRunStatus.PAUSED_CAPACITY_LIMIT, str(exc)
                    )
                    logger.error("Автономный run %s безопасно отклонён: %s", target, exc)
            if processed == 0:
                wakeup = await next_runnable_wakeup(sessions)
                timeout = bounded_wakeup_delay(
                    wakeup,
                    now=datetime.now(UTC),
                    idle_seconds=settings.collection_idle_sleep_seconds,
                )
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(stop_event.wait(), timeout=timeout)
    finally:
        if client is not None:
            await client.aclose()
        await engine.dispose()


async def _validate_autonomous_run(
    sessions: async_sessionmaker[AsyncSession],
    settings: Settings,
    run_id: uuid.UUID,
    *,
    backup_verifier: BackupVerifier | None = None,
) -> None:
    """Revalidate immutable configuration, report and backup before autonomous claim."""
    queue = CollectionQueue(sessions, settings)
    async with sessions() as session:
        run = await session.get(CollectionRun, run_id)
        if run is None or run.configuration.get("capacity_gate") != "passed":
            raise ValueError("Run не имеет разрешающего capacity gate")
        if run.configuration.get("collection") != queue.collection_configuration():
            raise ValueError("Runtime-конфигурация не совпадает с immutable run")
        if run.scope in {
            "subscriptions",
            "subscription_discovery",
            "subscription_metadata",
            "subscription_posts",
        }:
            report_path = run.configuration.get("capacity_report")
            backup = run.configuration.get("verified_backup")
            expected = run.configuration.get("collection")
            if (
                not isinstance(report_path, str)
                or not isinstance(backup, dict)
                or not isinstance(backup.get("path"), str)
                or not isinstance(expected, dict)
            ):
                raise ValueError("Run не содержит проверенные report/backup")
            phase: Literal["A", "B"] = "B" if run.scope == "subscription_posts" else "A"
            validate_capacity_report(
                Path(report_path),
                phase=phase,
                configuration=expected,
                max_age_days=settings.collection_capacity_report_max_age_days,
            )
            (backup_verifier or BackupVerifier()).verify(Path(str(backup["path"])), expected=backup)


@collection_app.command("worker")
def collection_worker() -> None:
    """Запустить автономный worker, ожидающий разрешённые full runs."""
    try:
        asyncio.run(_collection_worker_service())
    except ValueError as exc:
        typer.echo(f"Worker остановлен: {exc}", err=True)
        raise typer.Exit(code=2) from exc


async def _show_status(run_id: uuid.UUID | None) -> dict[str, Any]:
    settings = get_settings()
    engine = create_database_engine(settings.sqlalchemy_url)
    sessions = create_session_factory(engine)
    try:
        payload = await run_summary(sessions, run_id)
        disk = inspect_disk(
            settings.collection_export_dir,
            settings.disk_warning_percent,
            settings.disk_stop_percent,
        )
        jobs = payload.get("jobs", {})
        if isinstance(jobs, dict):
            payload["estimated_remaining_jobs"] = sum(
                int(jobs.get(status, 0)) for status in ("pending", "running", "retry_wait")
            )
        payload["disk_used_percent"] = round(disk.used_percent, 1)
        return payload
    finally:
        await engine.dispose()


@collection_app.command("status")
def collection_status(
    run_id: Annotated[uuid.UUID | None, typer.Option("--run-id")] = None,
) -> None:
    """Показать состояние и накопленные метрики запуска."""
    payload = asyncio.run(_show_status(run_id))
    typer.echo(json.dumps(payload, ensure_ascii=False, indent=2))


async def _plan_subscriptions(pilot: bool) -> uuid.UUID:
    settings = get_settings()
    engine = create_database_engine(settings.sqlalchemy_url)
    sessions = create_session_factory(engine)
    try:
        return await CollectionQueue(sessions, settings).plan_subscriptions(pilot=pilot)
    finally:
        await engine.dispose()


async def _campaign_operation(
    action: Literal["plan", "status", "pause", "resume", "metadata-preview"],
    campaign_id: uuid.UUID | None = None,
) -> dict[str, Any]:
    settings = get_settings()
    engine = create_database_engine(settings.sqlalchemy_url)
    sessions = create_session_factory(engine)
    try:
        manager = CampaignManager(sessions, settings)
        if action == "plan":
            return {"campaign_id": str(await manager.plan())}
        if action == "status":
            return await manager.status(campaign_id)
        if campaign_id is None:
            raise ValueError("Укажите --campaign-id")
        if action == "metadata-preview":
            return await manager.metadata_preview(campaign_id)
        await manager.change_status(campaign_id, pause=action == "pause")
        return await manager.status(campaign_id)
    finally:
        await engine.dispose()


def _show_campaign_operation(
    action: Literal["plan", "status", "pause", "resume", "metadata-preview"],
    campaign_id: uuid.UUID | None = None,
) -> None:
    try:
        payload = asyncio.run(_campaign_operation(action, campaign_id))
    except ValueError as exc:
        typer.echo(f"Операция с кампанией отклонена: {exc}", err=True)
        raise typer.Exit(code=2) from exc
    typer.echo(json.dumps(payload, ensure_ascii=False, indent=2))


@campaign_app.command("plan")
def campaign_plan(
    apply: Annotated[
        bool,
        typer.Option("--apply", help="Создать или переиспользовать кампанию и первый cohort."),
    ] = False,
) -> None:
    """Создать campaign только явно; без --apply показать неизменяющий preview."""
    if not apply:

        async def preview() -> dict[str, object]:
            settings = get_settings()
            engine = create_database_engine(settings.sqlalchemy_url)
            sessions = create_session_factory(engine)
            try:
                return await CampaignManager(sessions, settings).plan_preview()
            finally:
                await engine.dispose()

        typer.echo(json.dumps(asyncio.run(preview()), ensure_ascii=False, indent=2))
        return
    _show_campaign_operation("plan")


@campaign_app.command("status")
def campaign_status(
    campaign_id: Annotated[uuid.UUID | None, typer.Option("--campaign-id")] = None,
) -> None:
    """Показать фазу, coverage, усечения и следующее пробуждение."""
    _show_campaign_operation("status", campaign_id)


@campaign_app.command("pause")
def campaign_pause(campaign_id: Annotated[uuid.UUID, typer.Option("--campaign-id")]) -> None:
    """Поставить кампанию на паузу без удаления jobs и checkpoint."""
    _show_campaign_operation("pause", campaign_id)


@campaign_app.command("resume")
def campaign_resume(campaign_id: Annotated[uuid.UUID, typer.Option("--campaign-id")]) -> None:
    """Возобновить кампанию с сохранённого checkpoint."""
    _show_campaign_operation("resume", campaign_id)


@campaign_app.command("metadata-preview")
def campaign_metadata_preview(
    campaign_id: Annotated[uuid.UUID, typer.Option("--campaign-id")],
) -> None:
    """Read-only preview DISTINCT communities будущей metadata-фазы."""
    _show_campaign_operation("metadata-preview", campaign_id)


@subscriptions_app.command("plan")
def subscriptions_plan() -> None:
    """Создать cohort-plan существующих публичных пользователей."""
    run_id = asyncio.run(_plan_subscriptions(False))
    typer.echo(f"План подписок создан или переиспользован: {run_id}")


@subscriptions_app.command("pilot")
def subscriptions_pilot(
    max_jobs: Annotated[int | None, typer.Option("--max-jobs", min=1)] = None,
) -> None:
    """Явно выполнить Pilot A максимум на 500 пользователей и записать report."""
    try:
        payload = asyncio.run(_run_subscription_pilot("A", max_jobs=max_jobs))
    except ValueError as exc:
        typer.echo(f"Pilot A отклонён: {exc}", err=True)
        raise typer.Exit(code=2) from exc
    typer.echo(json.dumps(payload, ensure_ascii=False, indent=2))


async def _plan_subscription_posts(pilot: bool, source_run_id: uuid.UUID | None) -> uuid.UUID:
    settings = get_settings()
    engine = create_database_engine(settings.sqlalchemy_url)
    sessions = create_session_factory(engine)
    try:
        return await CollectionQueue(sessions, settings).plan_subscription_posts(
            pilot=pilot, source_run_id=source_run_id
        )
    finally:
        await engine.dispose()


@subscriptions_app.command("posts-plan")
def subscription_posts_plan(
    source_run_id: Annotated[uuid.UUID, typer.Option("--source-run-id")],
) -> None:
    """Создать отдельный production-plan постов после Gate B."""
    try:
        run_id = asyncio.run(_plan_subscription_posts(False, source_run_id))
    except ValueError as exc:
        typer.echo(f"Production plan постов отклонён: {exc}", err=True)
        raise typer.Exit(code=2) from exc
    typer.echo(f"План постов подписок создан или переиспользован: {run_id}")


@subscriptions_app.command("posts-pilot")
def subscription_posts_pilot(
    source_run_id: Annotated[uuid.UUID | None, typer.Option("--source-run-id")] = None,
    max_jobs: Annotated[int | None, typer.Option("--max-jobs", min=1)] = None,
) -> None:
    """Явно выполнить Pilot B по communities завершённого Pilot A."""
    try:
        payload = asyncio.run(
            _run_subscription_pilot("B", source_run_id=source_run_id, max_jobs=max_jobs)
        )
    except ValueError as exc:
        typer.echo(f"Pilot B отклонён: {exc}", err=True)
        raise typer.Exit(code=2) from exc
    typer.echo(json.dumps(payload, ensure_ascii=False, indent=2))


@subscriptions_app.command("run")
def subscriptions_run(
    run_id: Annotated[uuid.UUID, typer.Option("--run-id")],
    max_jobs: Annotated[int | None, typer.Option("--max-jobs")] = None,
) -> None:
    """Выполнить подготовленный subscription run до idle."""
    try:
        _, count = asyncio.run(_execute_collection(run_id, None, max_jobs, until_idle=False))
    except ValueError as exc:
        typer.echo(f"Запуск подписок отклонён: {exc}", err=True)
        raise typer.Exit(code=2) from exc
    typer.echo(f"Обработано jobs: {count}")


@subscriptions_app.command("status")
def subscriptions_status(
    run_id: Annotated[uuid.UUID | None, typer.Option("--run-id")] = None,
) -> None:
    """Показать состояние subscription run."""
    typer.echo(json.dumps(asyncio.run(_show_status(run_id)), ensure_ascii=False, indent=2))


async def _subscription_totals() -> dict[str, int]:
    settings = get_settings()
    engine = create_database_engine(settings.sqlalchemy_url)
    sessions = create_session_factory(engine)
    try:
        async with sessions() as session:
            return {
                "processed_users": int(
                    await session.scalar(
                        select(func.count(UserSubscriptionState.user_id)).where(
                            UserSubscriptionState.last_success_at.is_not(None)
                        )
                    )
                    or 0
                ),
                "private_subscriptions": int(
                    await session.scalar(
                        select(func.count(UserSubscriptionState.user_id)).where(
                            UserSubscriptionState.privacy_denied.is_(True)
                        )
                    )
                    or 0
                ),
                "subscription_links": int(
                    await session.scalar(select(func.count(UserGroupSubscription.id))) or 0
                ),
                "communities": int(
                    await session.scalar(select(func.count(VKCommunity.vk_id))) or 0
                ),
            }
    finally:
        await engine.dispose()


@subscriptions_app.command("totals")
def subscriptions_totals() -> None:
    """Показать накопленные subscription counters."""
    typer.echo(json.dumps(asyncio.run(_subscription_totals()), ensure_ascii=False, indent=2))


@subscriptions_app.command("capacity-preview")
def subscriptions_capacity_preview() -> None:
    """Сформировать консервативный JSON preview независимых Gate A и Gate B."""
    settings = get_settings()
    users = settings.collection_subscriptions_users_per_run
    links = users * settings.collection_subscriptions_max_per_user
    unique_communities_upper = links
    posts = unique_communities_upper * settings.collection_subscription_group_posts_max
    gate_a_bytes = links * 256 + unique_communities_upper * 1024
    gate_b_bytes = posts * 2048 + posts * 256
    payload = {
        "kind": "theoretical_preview",
        "requires_real_pilot": True,
        "gate_a": {
            "users": users,
            "subscription_links_upper": links,
            "unique_communities_upper": unique_communities_upper,
            "estimated_bytes_upper": gate_a_bytes,
        },
        "gate_b": {
            "communities_upper": unique_communities_upper,
            "posts_upper": posts,
            "estimated_bytes_upper": gate_b_bytes,
        },
        "safe_disk_limit_bytes": 7 * 1024**3,
        "production_allowed": False,
    }
    target = settings.collection_export_dir / "subscription-capacity-preview.json"
    _write_json(target, payload)
    typer.echo(json.dumps({**payload, "path": str(target)}, ensure_ascii=False, indent=2))


async def _method_limits(method: str | None = None) -> list[dict[str, object]]:
    settings = get_settings()
    engine = create_database_engine(settings.sqlalchemy_url)
    sessions = create_session_factory(engine)
    try:
        async with sessions() as session:
            query = select(VKTokenMethodState).order_by(
                VKTokenMethodState.method, VKTokenMethodState.token_fingerprint
            )
            if method:
                query = query.where(VKTokenMethodState.method == method)
            rows = list((await session.scalars(query)).all())
            now = datetime.now(UTC)
            return [
                {
                    "token_fingerprint": row.token_fingerprint[:12],
                    "method": row.method,
                    "state": (
                        "blocked"
                        if row.blocked_until is not None and row.blocked_until > now
                        else "enabled"
                    ),
                    "blocked_until": row.blocked_until.isoformat() if row.blocked_until else None,
                    "next_probe_at": row.next_probe_at.isoformat() if row.next_probe_at else None,
                    "consecutive_limit_hits": row.consecutive_limit_hits,
                    "last_error_code": row.last_error_code,
                    "last_error_at": row.last_error_at.isoformat() if row.last_error_at else None,
                    "last_success_at": (
                        row.last_success_at.isoformat() if row.last_success_at else None
                    ),
                    "successful_requests": row.successful_requests,
                    "cooldown_seconds": row.cooldown_seconds,
                }
                for row in rows
            ]
    finally:
        await engine.dispose()


@collection_app.command("method-limits")
def method_limits(
    method: Annotated[str | None, typer.Option("--method")] = None,
) -> None:
    """Показать method-specific cooldown без исходных токенов."""
    typer.echo(json.dumps(asyncio.run(_method_limits(method)), ensure_ascii=False, indent=2))


async def _backlog() -> dict[str, Any]:
    settings = get_settings()
    engine = create_database_engine(settings.sqlalchemy_url)
    sessions = create_session_factory(engine)
    try:
        payload = await canonical_backlog(sessions, settings)
        disk = inspect_disk(
            settings.collection_export_dir,
            settings.disk_warning_percent,
            settings.disk_stop_percent,
        )
        payload["disk"] = {
            "used_percent": disk.used_percent,
            "free_bytes": disk.free_bytes,
            "warning": disk.warning,
            "stop": disk.stop,
        }
        return payload
    finally:
        await engine.dispose()


@collection_app.command("backlog")
def collection_backlog(
    as_json: Annotated[bool, typer.Option("--json", help="Вывести полный JSON.")] = False,
) -> None:
    """Показать read-only backlog по каноническим state-таблицам."""
    payload = asyncio.run(_backlog())
    if as_json:
        typer.echo(json.dumps(payload, ensure_ascii=False, indent=2))
        return
    typer.echo(f"Сформировано: {payload['generated_at']}")
    for title, key in (
        ("Approved-группы", "approved_groups"),
        ("Пользователи", "users"),
        ("Подписки", "subscriptions"),
        ("Сообщества подписок", "subscription_communities"),
        ("Посты сообществ подписок", "subscription_posts"),
    ):
        typer.echo(f"{title}: {json.dumps(payload[key], ensure_ascii=False)}")
    typer.echo(
        "Jobs показаны отдельно как история: rows и distinct_entities не являются "
        "каноническим backlog."
    )
    typer.echo(
        f"Зависшие lease: {payload['stale_running_leases']}; "
        f"незавершённые pilot: {payload['unfinished_pilots']}; "
        f"активные кампании: {payload['active_campaigns']}"
    )
    typer.echo(f"Диск: {json.dumps(payload['disk'], ensure_ascii=False)}")


async def _repair_stale_leases(*, confirm: bool) -> dict[str, Any]:
    settings = get_settings()
    engine = create_database_engine(settings.sqlalchemy_url)
    sessions = create_session_factory(engine)
    cutoff = datetime.now(UTC) - timedelta(seconds=settings.collection_job_lease_seconds)
    try:
        async with sessions() as session:
            rows = (
                await session.execute(
                    select(CollectionJob.collection_run_id, func.count(CollectionJob.id))
                    .where(
                        CollectionJob.status == JobStatus.RUNNING,
                        CollectionJob.locked_at < cutoff,
                    )
                    .group_by(CollectionJob.collection_run_id)
                    .order_by(CollectionJob.collection_run_id)
                )
            ).all()
        preview = {str(run_id): int(count) for run_id, count in rows}
        recovered = 0
        if confirm:
            queue = CollectionQueue(sessions, settings)
            for run_id, _count in rows:
                recovered += await queue.recover_expired(run_id)
        return {
            "mode": "confirm" if confirm else "preview",
            "reason": "lease_expired_recovered",
            "runs": preview,
            "stale_jobs": sum(preview.values()),
            "recovered_jobs": recovered,
            "history_deleted": False,
        }
    finally:
        await engine.dispose()


@collection_app.command("repair-stale-leases")
def repair_stale_leases(
    confirm: Annotated[
        bool,
        typer.Option("--confirm", help="Вернуть только истёкшие running lease в pending."),
    ] = False,
) -> None:
    """Сначала показать preview; изменять state только с явным --confirm."""
    typer.echo(
        json.dumps(
            asyncio.run(_repair_stale_leases(confirm=confirm)),
            ensure_ascii=False,
            indent=2,
        )
    )


@collection_app.command("light-repair")
def light_repair(
    apply: Annotated[
        bool,
        typer.Option("--apply", help="Явно создать безопасный light-repair run."),
    ] = False,
) -> None:
    """Показать canonical gaps; с --apply создать только metadata/profile jobs."""

    async def operation() -> dict[str, object]:
        settings = get_settings()
        engine = create_database_engine(settings.sqlalchemy_url)
        sessions = create_session_factory(engine)
        try:
            queue = CollectionQueue(sessions, settings)
            preview = await queue.light_repair_preview()
            if not apply:
                return {"apply": False, **preview}
            run_id = await queue.plan_light_repair()
            return {
                "apply": True,
                "run_id": str(run_id),
                "allowed_methods": ["groups.getById", "users.get"],
                **preview,
            }
        finally:
            await engine.dispose()

    typer.echo(json.dumps(asyncio.run(operation()), ensure_ascii=False, indent=2))


async def _change_run_status(run_id: uuid.UUID, status: CollectionRunStatus) -> None:
    settings = get_settings()
    engine = create_database_engine(settings.sqlalchemy_url)
    sessions = create_session_factory(engine)
    try:
        await CollectionQueue(sessions, settings).set_run_status(run_id, status)
        await notify(settings, f"Collection run {run_id}: status={status.value}")
    finally:
        await engine.dispose()


@collection_app.command("pause")
def pause_collection(run_id: Annotated[uuid.UUID, typer.Option("--run-id")]) -> None:
    """Поставить pending jobs на паузу."""
    asyncio.run(_change_run_status(run_id, CollectionRunStatus.PAUSED))
    typer.echo(f"Run {run_id} поставлен на паузу.")


@collection_app.command("resume")
def resume_collection(run_id: Annotated[uuid.UUID, typer.Option("--run-id")]) -> None:
    """Вернуть paused jobs в очередь без сброса checkpoint."""
    asyncio.run(_change_run_status(run_id, CollectionRunStatus.RUNNING))
    typer.echo(f"Run {run_id} возобновлён.")


async def _retry_failed(run_id: uuid.UUID) -> int:
    settings = get_settings()
    engine = create_database_engine(settings.sqlalchemy_url)
    sessions = create_session_factory(engine)
    try:
        async with sessions() as session:
            result = await session.execute(
                update(CollectionJob)
                .where(
                    CollectionJob.collection_run_id == run_id,
                    CollectionJob.status == JobStatus.FAILED,
                )
                .values(
                    status=JobStatus.PENDING,
                    next_attempt_at=None,
                    finished_at=None,
                    last_error_type=None,
                    last_error_message=None,
                )
            )
            await session.commit()
            return int(result.rowcount or 0)  # type: ignore[attr-defined]
    finally:
        await engine.dispose()


@collection_app.command("retry-failed")
def retry_failed(run_id: Annotated[uuid.UUID, typer.Option("--run-id")]) -> None:
    """Вернуть failed jobs в pending, сохранив checkpoint."""
    count = asyncio.run(_retry_failed(run_id))
    typer.echo(f"Возвращено jobs: {count}")


async def _verify(run_id: uuid.UUID) -> dict[str, int]:
    settings = get_settings()
    engine = create_database_engine(settings.sqlalchemy_url)
    sessions = create_session_factory(engine)
    try:
        return await verify_run(sessions, run_id)
    finally:
        await engine.dispose()


@collection_app.command("verify")
def verify_collection(run_id: Annotated[uuid.UUID, typer.Option("--run-id")]) -> None:
    """Проверить дубли, rejected jobs и зависшие locks."""
    payload = asyncio.run(_verify(run_id))
    typer.echo(json.dumps(payload, ensure_ascii=False, indent=2))
    if any(payload.values()):
        raise typer.Exit(code=1)


async def _global_summary() -> dict[str, int]:
    settings = get_settings()
    engine = create_database_engine(settings.sqlalchemy_url)
    sessions = create_session_factory(engine)
    try:
        return await global_summary(sessions)
    finally:
        await engine.dispose()


@collection_app.command("summary")
def collection_summary_command() -> None:
    """Показать глобальные количества stage 2."""
    typer.echo(json.dumps(asyncio.run(_global_summary()), ensure_ascii=False, indent=2))


def _write_json(target: Path, payload: dict[str, Any]) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_suffix(target.suffix + ".tmp")
    temporary.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    temporary.replace(target)


def _validated_backup_metadata(
    backup: Path,
    *,
    expected: dict[str, object] | None = None,
) -> dict[str, object]:
    """Проверить формат и fingerprint PostgreSQL custom-format backup."""
    verifier = BackupVerifier()
    return verifier.fingerprint(backup) if expected is None else verifier.verify(backup, expected)


async def _run_subscription_pilot(
    phase: Literal["A", "B"],
    *,
    source_run_id: uuid.UUID | None = None,
    max_jobs: int | None = None,
) -> dict[str, Any]:
    """Выполнить измеряемый Pilot A/B; незавершённый pilot оставляет gate закрытым."""
    settings = get_settings()
    engine = create_database_engine(settings.sqlalchemy_url)
    sessions = create_session_factory(engine)
    try:
        queue = CollectionQueue(sessions, settings)
        before = await database_metrics(sessions)
        run_id = (
            await queue.plan_subscriptions(pilot=True)
            if phase == "A"
            else await queue.plan_subscription_posts(pilot=True, source_run_id=source_run_id)
        )
        started_at = time.monotonic()
        _, processed = await _execute_collection(
            run_id,
            None,
            max_jobs,
            until_idle=True,
            explicit_pilot=True,
        )
        duration_seconds = time.monotonic() - started_at
        after = await database_metrics(sessions)
        summary = await run_summary(sessions, run_id)
        database_growth = max(0, after["database_bytes"] - before["database_bytes"])
        relation_growth = sum(
            max(0, after[key] - before[key])
            for key in before
            if key.startswith("relation_") and key in after
        )
        measured_growth = max(database_growth, relation_growth)
        configuration = queue.collection_configuration()
        selected = int(
            summary.get("jobs_by_type", {})
            .get(
                "collect_user_subscriptions"
                if phase == "A"
                else "collect_subscription_group_posts",
                {},
            )
            .get("completed", 0)
        )
        target_entities = (
            settings.collection_subscriptions_users_per_run
            if phase == "A"
            else int(after.get("subscription_communities", 0))
        )
        projected_growth = (
            int(measured_growth / selected * target_entities * 1.30)
            if selected > 0 and measured_growth > 0
            else None
        )
        projected_database = (
            after["database_bytes"] + projected_growth if projected_growth is not None else None
        )
        completed = summary["status"] == "completed"
        jobs = summary.get("jobs", {})
        planned_entities = sum(int(value) for value in jobs.values())
        skipped_entities = int(jobs.get("skipped", 0))
        failed_entities = int(jobs.get("failed", 0))
        observed_entities = selected + skipped_entities
        deferred_entities = max(
            0,
            planned_entities - observed_entities - failed_entities,
        )
        minimum_entities = (
            settings.collection_subscription_pilot_min_users
            if phase == "A"
            else settings.collection_subscription_posts_pilot_min_communities
        )
        disk = inspect_disk(
            settings.collection_export_dir,
            settings.disk_warning_percent,
            settings.disk_stop_percent,
        )
        production_allowed = bool(
            completed
            and max_jobs is None
            and failed_entities == 0
            and observed_entities >= minimum_entities
            and measured_growth > 0
            and isinstance(projected_growth, int)
            and isinstance(projected_database, int)
            and projected_database <= 7 * 1024**3
            and projected_growth <= disk.free_bytes
        )
        measured: dict[str, int | float] = {
            "duration_seconds": duration_seconds,
            "api_requests": int(summary.get("api_requests", 0)),
            "processed_jobs": processed,
            "planned_entities": planned_entities,
            "observed_entities": observed_entities,
            "completed_entities": selected,
            "skipped_entities": skipped_entities,
            "failed_entities": failed_entities,
            "deferred_entities": deferred_entities,
            "private_entities": int(summary.get("private_users", 0)),
            "database_bytes_before": before["database_bytes"],
            "database_bytes_after": after["database_bytes"],
            "database_growth_bytes": database_growth,
            "relation_growth_bytes": relation_growth,
            "disk_free_bytes_after": disk.free_bytes,
            "subscription_links_before": before.get("subscriptions", 0),
            "subscription_links_after": after.get("subscriptions", 0),
            "unique_communities_before": before.get("communities", 0),
            "unique_communities_after": after.get("communities", 0),
            "posts_before": before.get("posts", 0),
            "posts_after": after.get("posts", 0),
            "attachments_before": before.get("attachments", 0),
            "attachments_after": after.get("attachments", 0),
        }
        for key in sorted(name for name in before if name.startswith("relation_")):
            measured[f"{key}_before"] = before[key]
            measured[f"{key}_after"] = after[key]
        projected: dict[str, int | float | None] = {
            "target_entities": target_entities,
            "database_growth_bytes": projected_growth,
            "database_bytes": projected_database,
            "reserve_factor": 1.30,
        }
        if phase == "A" and projected_growth is not None:
            projected["database_bytes_limit_100"] = before["database_bytes"] + int(
                projected_growth * 100 / settings.collection_subscriptions_max_per_user
            )
        limits = (
            {
                "pilot_users": settings.collection_subscription_pilot_users,
                "minimum_pilot_users": settings.collection_subscription_pilot_min_users,
                "subscriptions_per_user": settings.collection_subscriptions_max_per_user,
                "subscriptions_preview_limit": 100,
                "production_users": settings.collection_subscriptions_users_per_run,
            }
            if phase == "A"
            else {
                "pilot_communities": (settings.collection_subscription_posts_pilot_communities),
                "minimum_pilot_communities": (
                    settings.collection_subscription_posts_pilot_min_communities
                ),
                "posts_per_community": settings.collection_subscription_group_posts_max,
                "post_ttl_days": settings.collection_subscription_group_posts_ttl_days,
            }
        )
        report = build_capacity_report(
            phase=phase,
            run_id=run_id,
            configuration=configuration,
            limits=limits,
            measured=measured,
            projected=projected,
            production_allowed=production_allowed,
        )
        target = settings.collection_export_dir / f"subscription-gate-{phase.lower()}.json"
        write_capacity_report(target, report)
        return {"path": str(target), **report}
    finally:
        await engine.dispose()


async def _pilot() -> tuple[uuid.UUID, dict[str, Any], dict[str, Any]]:
    settings = get_settings()
    engine = create_database_engine(settings.sqlalchemy_url)
    sessions = create_session_factory(engine)
    client: VKClient | None = None
    try:
        before = await database_metrics(sessions)
        queue = CollectionQueue(sessions, settings)
        preview = await queue.preview(pilot=True)
        run_id = await queue.plan(pilot=True)
        await notify(settings, f"Pilot {run_id} started")
        try:
            tokens = load_tokens(settings.vk_tokens_file)
        except VKTokensUnavailable as exc:
            await queue.set_run_status(run_id, CollectionRunStatus.PAUSED_NO_TOKENS, str(exc))
            processed = 0
        else:
            pool = _token_pool(settings, tokens, sessions)
            client = VKClient(
                pool,
                api_version=settings.vk_api_version,
                timeout=settings.vk_request_timeout_seconds,
            )
            processed = await CollectionWorker(sessions, client, settings).run(run_id)
        after = await database_metrics(sessions)
        summary = await run_summary(sessions, run_id)
        delta = max(0, after["database_bytes"] - before["database_bytes"])
        projected = (
            before["database_bytes"]
            + int(delta / preview.selected_groups * preview.approved_groups * 1.30)
            if preview.selected_groups and delta
            else None
        )
        completed = summary["status"] in {"completed", "completed_with_errors"}
        decision = (
            "passed"
            if completed and projected is not None and projected <= 7 * 1024**3
            else "failed"
            if completed and projected is not None
            else "paused_no_tokens"
            if summary["status"] == "paused_no_tokens"
            else "insufficient_measurement"
        )
        pilot_summary = {
            "run_id": str(run_id),
            "seed": settings.collection_pilot_seed,
            "selected_groups": preview.selected_groups,
            "processed_jobs": processed,
            "before": before,
            "after": after,
            "run": summary,
            "measured_at": datetime.now(UTC).isoformat(),
        }
        capacity = {
            "run_id": str(run_id),
            "database_delta_bytes": delta,
            "projected_database_bytes": projected,
            "safe_limit_bytes": 7 * 1024**3,
            "reserve_factor": 1.30,
            "decision": decision,
            "configuration": queue.collection_configuration(),
        }
        _write_json(settings.collection_export_dir / "pilot-summary.json", pilot_summary)
        _write_json(settings.collection_export_dir / "capacity-estimate.json", capacity)
        await notify(settings, f"Pilot {run_id} completed; capacity={decision}")
        return run_id, pilot_summary, capacity
    finally:
        if client is not None:
            await client.aclose()
        await engine.dispose()


@collection_app.command("pilot")
def collection_pilot() -> None:
    """Создать и выполнить детерминированный безопасный pilot."""
    run_id, summary, capacity = asyncio.run(_pilot())
    typer.echo(f"Pilot run: {run_id}; status: {summary['run']['status']}")
    typer.echo(f"Capacity gate: {capacity['decision']}")


async def _apply_capacity(
    run_id: uuid.UUID,
    source: Path,
    backup: Path | None = None,
) -> dict[str, Any]:
    try:
        payload = json.loads(source.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"Capacity report не читается: {exc}") from exc
    settings = get_settings()
    engine = create_database_engine(settings.sqlalchemy_url)
    sessions = create_session_factory(engine)
    try:
        async with sessions() as session:
            run = await session.get(CollectionRun, run_id, with_for_update=True)
            if run is None:
                raise ValueError("Collection run не найден")
            if run.status != CollectionRunStatus.PAUSED_CAPACITY_LIMIT or run.total_jobs <= 0:
                raise ValueError(
                    "Capacity gate можно применить только к непустому run на capacity-паузе"
                )
            expected = run.configuration.get("collection")
            if not isinstance(expected, dict):
                raise ValueError("Collection run содержит повреждённую конфигурацию")
            backup_metadata: dict[str, object] | None = None
            if run.scope in {
                "subscriptions",
                "subscription_discovery",
                "subscription_metadata",
                "subscription_posts",
            }:
                if backup is None:
                    raise ValueError("Для subscription capacity gate обязателен --backup")
                backup_metadata = _validated_backup_metadata(backup)
                phase: Literal["A", "B"] = (
                    "A"
                    if run.scope
                    in {"subscriptions", "subscription_discovery", "subscription_metadata"}
                    else "B"
                )
                payload = validate_capacity_report(
                    source,
                    phase=phase,
                    configuration=expected,
                    max_age_days=settings.collection_capacity_report_max_age_days,
                )
                pilot_id = payload.get("run_id")
                try:
                    pilot_run = await session.get(CollectionRun, uuid.UUID(str(pilot_id)))
                except ValueError as exc:
                    raise ValueError("Capacity report содержит некорректный pilot run ID") from exc
                expected_scope = (
                    "subscriptions_pilot" if phase == "A" else "subscription_posts_pilot"
                )
                if (
                    pilot_run is None
                    or pilot_run.scope != expected_scope
                    or pilot_run.status != CollectionRunStatus.COMPLETED
                ):
                    raise ValueError("Capacity report не связан с завершённым pilot run")
                projected = payload["projected"]["database_bytes"]
                safe_limit = payload["safe_disk_limit_bytes"]
            elif run.scope == "full":
                projected = payload.get("projected_database_bytes")
                safe_limit = payload.get("safe_limit_bytes")
                if (
                    payload.get("decision") != "passed"
                    or not isinstance(projected, int)
                    or not isinstance(safe_limit, int)
                    or projected > safe_limit
                ):
                    raise ValueError("Capacity report не разрешает full run")
                if payload.get("configuration") != expected:
                    raise ValueError("Capacity report относится к другой конфигурации сбора")
            else:
                raise ValueError("Capacity report нельзя применить к этому scope")
            run.configuration = {
                **run.configuration,
                "capacity_gate": "passed",
                "projected_database_bytes": projected,
                "safe_limit_bytes": safe_limit,
                "capacity_report": str(source.resolve()),
                **({"verified_backup": backup_metadata} if backup_metadata is not None else {}),
            }
            operator_paused = False
            if run.campaign_id is not None:
                current_campaign = await session.get(CollectionCampaign, run.campaign_id)
                operator_paused = (
                    current_campaign is not None
                    and current_campaign.status == CampaignStatus.PAUSED.value
                )
            run.status = (
                CollectionRunStatus.PAUSED if operator_paused else CollectionRunStatus.PLANNED
            )
            run.error_message = None
            if run.campaign_id is not None:
                campaign = await session.get(
                    CollectionCampaign, run.campaign_id, with_for_update=True
                )
                if campaign is None:
                    raise ValueError("Campaign, связанная с run, не найдена")
                campaign.configuration = {
                    **campaign.configuration,
                    "capacity_gate": "passed",
                    "projected_database_bytes": projected,
                    "safe_limit_bytes": safe_limit,
                    "capacity_report": str(source.resolve()),
                    **({"verified_backup": backup_metadata} if backup_metadata is not None else {}),
                }
                campaign.status = (
                    CampaignStatus.PAUSED.value if operator_paused else CampaignStatus.RUNNING.value
                )
                campaign.started_at = campaign.started_at or datetime.now(UTC)
                campaign.error_message = None
            await session.commit()
            return {"run_id": str(run_id), "status": "planned", "capacity_gate": "passed"}
    finally:
        await engine.dispose()


@collection_app.command("capacity-apply")
def apply_capacity_gate(
    run_id: Annotated[uuid.UUID, typer.Option("--run-id")],
    source: Annotated[
        Path | None,
        typer.Option("--source", help="Проверенный capacity-estimate.json."),
    ] = None,
    backup: Annotated[
        Path | None,
        typer.Option(
            "--backup",
            help="Проверенный pg_dump -Fc; обязателен для subscription Gate A/B.",
        ),
    ] = None,
) -> None:
    """Разрешить full run только по измеренному успешному capacity report."""
    settings = get_settings()
    target = source or settings.collection_export_dir / "capacity-estimate.json"
    try:
        payload = asyncio.run(_apply_capacity(run_id, target, backup))
    except ValueError as exc:
        typer.echo(f"Capacity gate отклонён: {exc}", err=True)
        raise typer.Exit(code=2) from exc
    typer.echo(json.dumps(payload, ensure_ascii=False, indent=2))


async def _privacy_operation(
    kind: str, vk_id: int, *, commit_delete: bool = False
) -> dict[str, Any]:
    settings = get_settings()
    engine = create_database_engine(settings.sqlalchemy_url)
    sessions = create_session_factory(engine)
    try:
        async with sessions() as session:
            if kind == "user":
                payload = await (
                    delete_user(session, vk_id) if commit_delete else inspect_user(session, vk_id)
                )
                if commit_delete:
                    await session.commit()
                return payload
            return await inspect_group(session, vk_id)
    finally:
        await engine.dispose()


@privacy_app.command("inspect-user")
def privacy_inspect_user(vk_id: int) -> None:
    """Показать агрегаты пользователя без чувствительных полей."""
    typer.echo(
        json.dumps(asyncio.run(_privacy_operation("user", vk_id)), ensure_ascii=False, indent=2)
    )


@privacy_app.command("delete-user")
def privacy_delete_user(
    vk_id: int,
    confirm: Annotated[bool, typer.Option("--confirm", help="Подтвердить удаление.")] = False,
) -> None:
    """Транзакционно удалить пользователя и связанные данные."""
    if not confirm:
        typer.echo("Удаление не выполнено: требуется --confirm.", err=True)
        raise typer.Exit(code=2)
    payload = asyncio.run(_privacy_operation("user", vk_id, commit_delete=True))
    typer.echo(json.dumps(payload, ensure_ascii=False, indent=2))


@privacy_app.command("inspect-group")
def privacy_inspect_group(vk_id: int) -> None:
    """Показать агрегаты группы без изменения данных."""
    typer.echo(
        json.dumps(asyncio.run(_privacy_operation("group", vk_id)), ensure_ascii=False, indent=2)
    )

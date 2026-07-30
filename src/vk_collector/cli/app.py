from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import signal
import time
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Annotated, Any

import typer
from sqlalchemy import func, select, update

from vk_collector.classification.service import (
    classification_summary,
    export_batch,
    import_classification,
)
from vk_collector.collection import CollectionQueue, CollectionWorker
from vk_collector.collection.notifications import notify
from vk_collector.collection.reporting import (
    capacity_gate_passed,
    database_metrics,
    global_summary,
    latest_run_id,
    latest_runnable_run_id,
    run_summary,
    verify_run,
)
from vk_collector.collection.safety import inspect_disk
from vk_collector.config import get_settings, load_keyword_config
from vk_collector.database.models import (
    CollectionJob,
    CollectionRun,
    CollectionRunStatus,
    GroupCandidate,
    GroupKeywordMatch,
    JobStatus,
    SearchRun,
)
from vk_collector.database.session import create_database_engine, create_session_factory
from vk_collector.privacy import delete_user, inspect_group, inspect_user
from vk_collector.search.postgres import PostgresSearchPersistence, search_run_summary
from vk_collector.search.service import Keyword, SearchService
from vk_collector.subjects import SubjectName, ensure_subject
from vk_collector.vk import TokenPool, VKClient, VKTokensUnavailable, load_tokens

app = typer.Typer(help="Поиск и ручная классификация сообществ VK.")
groups_app = typer.Typer(help="Поиск и статистика групп.")
classification_app = typer.Typer(help="Пакеты ручной классификации.")
collection_app = typer.Typer(help="Возобновляемый сбор публичных approved-данных.")
privacy_app = typer.Typer(help="Проверка и минимизация персональных данных.")
app.add_typer(groups_app, name="groups")
app.add_typer(classification_app, name="classification")
app.add_typer(collection_app, name="collection")
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


async def _run_search(subject: SubjectName | None) -> tuple[str, dict[str, Any]]:
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
    """Совместимая безопасная команда: показать план без создания jobs."""
    collection_plan(apply=False, pilot=False)


async def _plan_collection(*, apply: bool, pilot: bool) -> tuple[Any, uuid.UUID | None, float]:
    settings = get_settings()
    engine = create_database_engine(settings.sqlalchemy_url)
    sessions = create_session_factory(engine)
    try:
        queue = CollectionQueue(sessions, settings)
        preview = await queue.preview(pilot=pilot)
        disk = inspect_disk(
            settings.collection_export_dir,
            settings.disk_warning_percent,
            settings.disk_stop_percent,
        )
        run_id = None
        if apply:
            if disk.warning:
                raise ValueError(
                    f"Создание тяжёлых заданий запрещено: диск заполнен на {disk.used_percent:.1f}%"
                )
            run_id = await queue.plan(pilot=pilot)
        return preview, run_id, disk.used_percent
    finally:
        await engine.dispose()


@collection_app.command("plan")
def collection_plan(
    apply: Annotated[bool, typer.Option("--apply", help="Создать run и jobs.")] = False,
    pilot: Annotated[bool, typer.Option("--pilot", help="Планировать pilot-выборку.")] = False,
) -> None:
    """Показать безопасный план; без --apply база не изменяется."""
    try:
        preview, run_id, disk = asyncio.run(_plan_collection(apply=apply, pilot=pilot))
    except ValueError as exc:
        typer.echo(f"Планирование отклонено: {exc}", err=True)
        raise typer.Exit(code=2) from exc
    typer.echo(f"Approved-групп: {preview.approved_groups}")
    typer.echo(f"Выбрано групп: {preview.selected_groups}")
    typer.echo(f"Scopes: {', '.join(preview.scopes)}")
    typer.echo(f"Jobs: {preview.jobs}; оценка VK-запросов: {preview.estimated_requests}")
    typer.echo(f"Заполнение диска: {disk:.1f}%")
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
            if run.scope == "full" and not await capacity_gate_passed(sessions, target):
                raise ValueError("Full run запрещён до успешного capacity gate")
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
        pool = TokenPool(tokens, rps=settings.vk_per_token_rps, clock=time.monotonic)
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
    if scope not in {None, "groups", "posts", "members", "users", "subscriptions"}:
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
    """Ожидать разрешённый full run и выполнять его независимо от CLI-сессии."""
    settings = get_settings()
    engine = create_database_engine(settings.sqlalchemy_url)
    sessions = create_session_factory(engine)
    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for handled_signal in (signal.SIGTERM, signal.SIGINT):
        with contextlib.suppress(NotImplementedError, RuntimeError):
            loop.add_signal_handler(handled_signal, stop_event.set)
    try:
        while not stop_event.is_set():
            target = await latest_runnable_run_id(sessions)
            if target is None:
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(
                        stop_event.wait(), timeout=settings.collection_idle_sleep_seconds
                    )
                continue
            await _execute_collection(
                target,
                None,
                None,
                until_idle=False,
                stop_event=stop_event,
            )
    finally:
        await engine.dispose()


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
            pool = TokenPool(tokens, rps=settings.vk_per_token_rps, clock=time.monotonic)
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


async def _apply_capacity(run_id: uuid.UUID, source: Path) -> dict[str, Any]:
    try:
        payload = json.loads(source.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"Capacity report не читается: {exc}") from exc
    projected = payload.get("projected_database_bytes")
    safe_limit = payload.get("safe_limit_bytes")
    if (
        payload.get("decision") != "passed"
        or not isinstance(projected, int)
        or not isinstance(safe_limit, int)
        or projected > safe_limit
    ):
        raise ValueError("Capacity report не разрешает full run")
    settings = get_settings()
    engine = create_database_engine(settings.sqlalchemy_url)
    sessions = create_session_factory(engine)
    try:
        async with sessions() as session:
            run = await session.get(CollectionRun, run_id, with_for_update=True)
            if run is None or run.scope != "full":
                raise ValueError("Full run не найден")
            if payload.get("configuration") != run.configuration.get("collection"):
                raise ValueError("Capacity report относится к другой конфигурации сбора")
            run.configuration = {
                **run.configuration,
                "capacity_gate": "passed",
                "projected_database_bytes": projected,
                "safe_limit_bytes": safe_limit,
                "capacity_report": str(source),
            }
            run.status = CollectionRunStatus.PLANNED
            run.error_message = None
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
) -> None:
    """Разрешить full run только по измеренному успешному capacity report."""
    settings = get_settings()
    target = source or settings.collection_export_dir / "capacity-estimate.json"
    try:
        payload = asyncio.run(_apply_capacity(run_id, target))
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

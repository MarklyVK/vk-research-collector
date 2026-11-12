"""CLI VK-панели: panel freeze | backfill | tick | run | export | status."""

from __future__ import annotations

import asyncio
import csv
import json
import logging
import signal
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager, suppress
from pathlib import Path
from typing import Annotated, Any

import typer
from sqlalchemy import func, select

from vk_collector.config import get_settings
from vk_collector.database.models import (
    PanelCommunity,
    PanelPost,
    PanelPostSnapshot,
    VKTokenDailyUsage,
    VKTokenState,
)
from vk_collector.database.session import create_database_engine, create_session_factory
from vk_collector.panel.service import PanelService
from vk_collector.panel.vk_adapter import ExecutePanelVK
from vk_collector.vk import VKClient
from vk_collector.vk.tokens import load_tokens

logger = logging.getLogger(__name__)

panel_app = typer.Typer(help="Исследовательская VK-панель (виральность). По умолчанию выключена.")
DEFAULT_EXPORT_DIR = Path("/app/exports/vk_panel")


@asynccontextmanager
async def _service(export_dir: Path = DEFAULT_EXPORT_DIR) -> AsyncIterator[PanelService]:
    from vk_collector.cli.app import _daily_budget, _token_pool

    settings = get_settings()
    engine = create_database_engine(settings.sqlalchemy_url)
    sessions = create_session_factory(engine)
    pool = _token_pool(settings, load_tokens(settings.vk_tokens_file), sessions)
    client = VKClient(
        pool,
        api_version=settings.vk_api_version,
        timeout=settings.vk_request_timeout_seconds,
        budget=_daily_budget(settings, sessions),
        stop_token_on_quota_error=settings.vk_stop_token_on_quota_error,
    )
    try:
        yield PanelService(sessions, ExecutePanelVK(client), export_dir=export_dir)
    finally:
        await client.aclose()
        await engine.dispose()


@panel_app.command("freeze")
def panel_freeze(
    name: Annotated[str, typer.Option(help="Имя панели, например virality-2026-11")],
    selection_csv: Annotated[Path, typer.Option(help="CSV отбора (vk_id, stratum, size, ...)")],
    seed: Annotated[int, typer.Option(help="Зерно отбора")],
    config_json: Annotated[str, typer.Option(help="JSON-переопределения конфигурации")] = "{}",
) -> None:
    """Создать панель (status=draft) и зафиксировать выборку role=main. Сбор не запускает."""
    with selection_csv.open(encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    async def go() -> str:
        async with _service() as service:
            return str(
                await service.freeze_selection(
                    name, rows, seed=seed, config=json.loads(config_json)
                )
            )

    typer.echo(asyncio.run(go()))


@panel_app.command("activate")
def panel_activate(name: Annotated[str, typer.Option()]) -> None:
    """Перевести панель в active (started_at = сейчас). Только после «да» руководителя."""

    async def go() -> None:
        async with _service() as service:
            panel = await service.panel_by_name(name)
            await service.set_status(panel.id, "active")

    asyncio.run(go())


@panel_app.command("backfill")
def panel_backfill(
    name: Annotated[str, typer.Option()],
    limit: Annotated[int | None, typer.Option(help="Не больше N сообществ за запуск")] = None,
) -> None:
    """Ретроспектива: все посты за backfill_days + снимок счётчиков."""

    async def go() -> dict[str, int]:
        async with _service() as service:
            return await service.backfill(name, limit=limit)

    typer.echo(json.dumps(asyncio.run(go()), ensure_ascii=False))


@panel_app.command("tick")
def panel_tick(name: Annotated[str, typer.Option()]) -> None:
    """Один цикл: поиск новых постов, замеры по отметкам возраста, подписчики, выгрузки."""

    async def go() -> dict[str, Any]:
        async with _service() as service:
            return (await service.tick(name)).__dict__

    typer.echo(json.dumps(asyncio.run(go()), ensure_ascii=False))


@panel_app.command("run")
def panel_run(
    name: Annotated[str, typer.Option()],
    interval_seconds: Annotated[int, typer.Option()] = 600,
    backfill_first: Annotated[bool, typer.Option()] = True,
) -> None:
    """Автономный цикл панели до заморозки (stop_at). Ошибки тика логируются, цикл продолжается."""

    async def go() -> None:
        stop = asyncio.Event()
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, stop.set)
        async with _service() as service:
            while not stop.is_set():
                cycle_started = loop.time()
                try:
                    result = await service.tick(name)
                    logger.info("panel_tick %s", json.dumps(result.__dict__, ensure_ascii=False))
                    if result.frozen:
                        return
                    if backfill_first:
                        # ретроспектива — небольшими порциями между тиками, живая панель важнее
                        stats = await service.backfill(name, limit=5)
                        if stats["communities"] == 0 and stats["failed"] == 0:
                            # 90 дней готовы — 12 месяцев для video/clip (без медиа)
                            ext = await service.ext_backfill(name, limit=5)
                            logger.info("panel_ext_backfill %s", json.dumps(ext))
                except Exception:
                    logger.exception("panel_tick_failed")
                # интервал считается от начала тика: ретроспектива не сдвигает расписание замеров
                elapsed = loop.time() - cycle_started
                with suppress(TimeoutError):
                    await asyncio.wait_for(
                        stop.wait(), timeout=max(30.0, interval_seconds - elapsed)
                    )

    asyncio.run(go())


@panel_app.command("export")
def panel_export(
    name: Annotated[str, typer.Option()],
    label: Annotated[str, typer.Option(help="interim | final | manual")] = "manual",
) -> None:
    """Parquet-выгрузка панели в /app/exports/vk_panel/<label>/<время>/."""

    async def go() -> str:
        async with _service() as service:
            return await service.export(name, label)

    typer.echo(asyncio.run(go()))


@panel_app.command("restore-deleted")
def panel_restore_deleted(
    name: Annotated[str, typer.Option()],
    since: Annotated[str, typer.Option(help="ISO-время, например 2026-11-12T08:00:00+00:00")],
    apply: Annotated[bool, typer.Option("--apply", help="без флага — сухой прогон")] = False,
) -> None:
    """Снять ложные «удаления» (такты без единого замера) с момента since и вернуть посты в план."""
    from datetime import datetime

    moment = datetime.fromisoformat(since)
    if moment.tzinfo is None:
        raise typer.BadParameter("since должен быть с часовым поясом")

    async def go() -> dict[str, Any]:
        async with _service() as service:
            return await service.restore_false_deletions(name, moment, apply=apply)

    typer.echo(json.dumps(asyncio.run(go()), ensure_ascii=False, indent=1))


@panel_app.command("status")
def panel_status(name: Annotated[str, typer.Option()]) -> None:
    """Сводка: сообщества, посты, замеры по отметкам, пропуски, токены и квоты за сегодня."""
    settings = get_settings()

    def budget_today() -> Any:
        from vk_collector.vk.budget import DailyBudget

        return DailyBudget({}, timezone=settings.vk_budget_timezone).today()

    async def go() -> dict[str, Any]:
        async with _service() as service:
            panel = await service.panel_by_name(name)
            async with service._sessions() as session:
                communities = await session.scalar(
                    select(func.count()).where(PanelCommunity.panel_id == panel.id)
                )
                posts = dict(
                    (
                        await session.execute(
                            select(PanelPost.origin, func.count())
                            .where(PanelPost.panel_id == panel.id)
                            .group_by(PanelPost.origin)
                        )
                    ).all()
                )
                by_target = dict(
                    (
                        await session.execute(
                            select(PanelPostSnapshot.target_age_h, func.count())
                            .where(PanelPostSnapshot.panel_id == panel.id)
                            .group_by(PanelPostSnapshot.target_age_h)
                        )
                    ).all()
                )
                missed = await session.scalar(
                    select(func.sum(PanelPost.missed_targets)).where(PanelPost.panel_id == panel.id)
                )
                today = budget_today()
                usage = [
                    {"token": fp[:6], "method": m, "calls": c}
                    for fp, m, c in (
                        await session.execute(
                            select(
                                VKTokenDailyUsage.token_fingerprint,
                                VKTokenDailyUsage.method,
                                VKTokenDailyUsage.calls,
                            ).where(VKTokenDailyUsage.day == today)
                        )
                    ).all()
                ]
                stopped = [
                    {"token": fp[:6], "until": str(until)}
                    for fp, until in (
                        await session.execute(
                            select(
                                VKTokenState.token_fingerprint, VKTokenState.global_blocked_until
                            ).where(VKTokenState.global_blocked_until > func.now())
                        )
                    ).all()
                ]
            return {
                "tokens_stopped_until_next_day": stopped,
                "vk_usage_today": usage,
                "status": panel.status,
                "communities": communities,
                "posts": posts,
                "snapshots_by_target_h": {str(k): v for k, v in by_target.items()},
                "missed_targets": missed or 0,
            }

    typer.echo(json.dumps(asyncio.run(go()), ensure_ascii=False, indent=1))


__all__ = ["panel_app"]

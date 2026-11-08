"""CLI для подсистемы YouTube Virality Panel (IEEE Access, TOTE)."""

from __future__ import annotations

import asyncio
import json
import logging
import signal
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager, suppress
from typing import Annotated, Any

import httpx
import typer
from sqlalchemy import func, select

from vk_collector.config import get_settings
from vk_collector.database.models import (
    YTChannel,
    YTComment,
    YTKeyDailyUsage,
    YTPost,
    YTScheduledMeasurement,
)
from vk_collector.database.session import create_database_engine, create_session_factory
from vk_collector.youtube.budget import YouTubeQuotaBudget, YouTubeQuotaExhaustedError
from vk_collector.youtube.client import YouTubeClient
from vk_collector.youtube.export import YouTubePanelExporter
from vk_collector.youtube.media import YouTubeMediaDownloader
from vk_collector.youtube.service import YouTubePanelService

logger = logging.getLogger(__name__)

yt_app = typer.Typer(help="Панель сбора и мониторинга YouTube (IEEE Access, TOTE)")


@asynccontextmanager
async def _service_context() -> AsyncIterator[tuple[YouTubePanelService, YouTubeQuotaBudget, Any]]:
    settings = get_settings()
    engine = create_database_engine(settings.sqlalchemy_url)
    sessions = create_session_factory(engine)
    keys = settings.effective_youtube_api_keys

    budget = YouTubeQuotaBudget(
        keys,
        daily_limit=settings.yt_daily_quota_limit,
        safety_margin=settings.yt_quota_safety_margin,
        sessions=sessions,
    )
    client = YouTubeClient(budget)
    media_downloader = YouTubeMediaDownloader(thumbs_dir=settings.yt_thumbs_dir)
    service = YouTubePanelService(sessions, client, settings, media_downloader)
    try:
        yield service, budget, sessions
    finally:
        await client.aclose()
        await media_downloader.aclose()
        await engine.dispose()


@yt_app.command("check-connection")
def yt_check_connection() -> None:
    """Проверить доступ к YouTube API на тестовом видео и доступность CDN обложек."""
    settings = get_settings()
    keys = settings.effective_youtube_api_keys
    if not keys:
        typer.secho("ОШИБКА: Не настроены ключи YouTube API в .env!", fg=typer.colors.RED)
        raise typer.Exit(code=1)

    test_video_id = "dQw4w9WgXcQ"
    cdn_url = f"https://i.ytimg.com/vi/{test_video_id}/hq1.jpg"

    async def go() -> dict[str, Any]:
        async with _service_context() as (service, budget, _):
            # 1. Проверка API
            api_status = "ok"
            api_video_title = ""
            try:
                items = await service._client.get_videos_batch([test_video_id])
                if items:
                    api_video_title = items[0].get("snippet", {}).get("title", "")
                else:
                    api_status = "empty_response"
            except Exception as err:
                api_status = f"error: {err}"

            # 2. Проверка CDN картинок
            cdn_status = "ok"
            cdn_bytes = 0
            async with httpx.AsyncClient(timeout=10.0) as http:
                try:
                    resp = await http.get(cdn_url)
                    if resp.status_code == 200:
                        cdn_bytes = len(resp.content)
                    else:
                        cdn_status = f"http_{resp.status_code}"
                except Exception as err:
                    cdn_status = f"error: {err}"

            day_pt = budget.today_pt()
            return {
                "keys_configured": len(keys),
                "day_pt": str(day_pt),
                "next_reset_pt": budget.next_reset_pt().isoformat(),
                "api_check": {
                    "status": api_status,
                    "video_title": api_video_title,
                },
                "cdn_check": {
                    "status": cdn_status,
                    "url": cdn_url,
                    "bytes_received": cdn_bytes,
                },
            }

    result = asyncio.run(go())
    typer.echo(json.dumps(result, ensure_ascii=False, indent=2))


@yt_app.command("discover-channels")
def yt_discover_channels(
    seed: Annotated[int, typer.Option(help="Зерно детерминированной стратификации")] = 20261108,
    max_queries: Annotated[
        int | None, typer.Option(help="Ограничение числа запросов (для отладки)")
    ] = None,
) -> None:
    """Поиск каналов по 9 доменам, загрузка метаданных и стратификация 9×4×8."""

    async def go() -> dict[str, Any]:
        async with _service_context() as (service, _, _):
            return await service.discover_and_stratify_channels(seed=seed, max_queries=max_queries)

    res = asyncio.run(go())
    typer.echo(json.dumps(res, ensure_ascii=False, indent=2))


@yt_app.command("backfill")
def yt_backfill(
    days: Annotated[int, typer.Option(help="Глубина ретроспективы в днях")] = 90,
) -> None:
    """Выгрузка видео за 90 дней для отобранных каналов панели."""

    async def go() -> dict[str, int]:
        async with _service_context() as (service, _, _):
            return await service.backfill_90d_videos(days=days)

    res = asyncio.run(go())
    typer.echo(json.dumps(res, ensure_ascii=False, indent=2))


@yt_app.command("download-media")
def yt_download_media(
    limit: Annotated[
        int | None, typer.Option(help="Максимальное количество видео для скачивания")
    ] = None,
) -> None:
    """Однопоточное скачивание обложек и кадров hq1..hq3 с паузой 1–2 с."""

    async def go() -> dict[str, int]:
        async with _service_context() as (service, _, sessions):
            count = await service._media_downloader.download_media_for_posts(sessions, limit=limit)
            return {"downloaded_posts": count}

    res = asyncio.run(go())
    typer.echo(json.dumps(res, ensure_ascii=False, indent=2))


@yt_app.command("collect-comments")
def yt_collect_comments(
    sample_size: Annotated[int, typer.Option(help="Размер выборки видео")] = 3000,
    seed: Annotated[int, typer.Option(help="Зерно случайного отбора")] = 20261108,
) -> None:
    """Стратифицированный отбор 3 000 видео и выгрузка топ-20 комментариев."""

    async def go() -> dict[str, int]:
        async with _service_context() as (service, _, _):
            return await service.sample_and_collect_comments(sample_size=sample_size, seed=seed)

    res = asyncio.run(go())
    typer.echo(json.dumps(res, ensure_ascii=False, indent=2))


@yt_app.command("tick")
def yt_tick() -> None:
    """Один цикл мониторинга новых видео и плановых замеров (1, 3, 6, 12, 24, 48, 72, 168 ч)."""

    async def go() -> dict[str, Any]:
        async with _service_context() as (service, _, _):
            return await service.panel_tick()

    res = asyncio.run(go())
    typer.echo(json.dumps(res, ensure_ascii=False, indent=2))


@yt_app.command("worker")
def yt_worker(
    interval_seconds: Annotated[
        int, typer.Option(help="Интервал между тактами мониторинга в секундах")
    ] = 300,
) -> None:
    """Непрерывный демон для запуска в Docker Compose с 09.11 по 17.11."""

    async def go() -> None:
        stop = asyncio.Event()
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, stop.set)

        logger.info("Запуск демона YouTube Virality Panel (интервал %d с)...", interval_seconds)
        async with _service_context() as (service, budget, _):
            while not stop.is_set():
                try:
                    result = await service.panel_tick()
                    logger.info("yt_tick результат: %s", json.dumps(result, ensure_ascii=False))
                except YouTubeQuotaExhaustedError as quota_err:
                    sec_reset = budget.seconds_until_reset()
                    logger.warning(
                        "Квота исчерпана: %s. Ожидание сброса (%.0f с)...", quota_err, sec_reset
                    )
                    with suppress(TimeoutError):
                        await asyncio.wait_for(stop.wait(), timeout=min(sec_reset, 3600.0))
                    continue
                except Exception:
                    logger.exception("Ошибка при выполнении такта YouTube панели:")

                with suppress(TimeoutError):
                    await asyncio.wait_for(stop.wait(), timeout=float(interval_seconds))

    asyncio.run(go())


@yt_app.command("status")
def yt_status() -> None:
    """Вывод заполненности страт (9×4), прогресса картинок/комментариев и остатка квоты."""

    async def go() -> dict[str, Any]:
        async with _service_context() as (_, budget, sessions):
            day_pt = budget.today_pt()
            async with sessions() as session:
                # 1. Каналы
                total_channels = await session.scalar(select(func.count(YTChannel.channel_id)))
                selected_channels = await session.scalar(
                    select(func.count(YTChannel.channel_id)).where(YTChannel.is_selected.is_(True))
                )
                strata_rows = (
                    await session.execute(
                        select(YTChannel.domain, YTChannel.size_tier, func.count())
                        .where(YTChannel.is_selected.is_(True))
                        .group_by(YTChannel.domain, YTChannel.size_tier)
                    )
                ).all()
                strata_map = {f"{d}:{t}": c for d, t, c in strata_rows}

                # 2. Посты
                total_posts = await session.scalar(select(func.count(YTPost.post_id)))
                backfill_posts = await session.scalar(
                    select(func.count(YTPost.post_id)).where(YTPost.is_backfill.is_(True))
                )
                live_posts = await session.scalar(
                    select(func.count(YTPost.post_id)).where(YTPost.is_backfill.is_(False))
                )

                # 3. Медиа
                media_done = await session.scalar(
                    select(func.count(YTPost.post_id)).where(YTPost.media_downloaded.is_(True))
                )

                # 4. Комментарии
                comments_done = await session.scalar(
                    select(func.count(YTPost.post_id)).where(YTPost.comments_status == "done")
                )
                comments_disabled = await session.scalar(
                    select(func.count(YTPost.post_id)).where(YTPost.comments_status == "disabled")
                )
                total_comments = await session.scalar(select(func.count(YTComment.comment_id)))

                # 5. Очередь замеров
                sched_rows = (
                    await session.execute(
                        select(YTScheduledMeasurement.status, func.count()).group_by(
                            YTScheduledMeasurement.status
                        )
                    )
                ).all()
                sched_map = {st: c for st, c in sched_rows}

                # 6. Квоты по ключам
                quota_usage_rows = (
                    await session.execute(
                        select(
                            YTKeyDailyUsage.key_fingerprint,
                            func.sum(YTKeyDailyUsage.units_spent),
                        )
                        .where(YTKeyDailyUsage.day_pt == day_pt)
                        .group_by(YTKeyDailyUsage.key_fingerprint)
                    )
                ).all()
                spent_by_fp = {fp: int(u or 0) for fp, u in quota_usage_rows}

                keys_status = []
                for fp in budget._fingerprints:
                    spent = spent_by_fp.get(fp, 0)
                    blocked = await budget.is_key_blocked(fp, day_pt)
                    limit = budget._daily_limit
                    keys_status.append(
                        {
                            "fingerprint": fp,
                            "units_spent": spent,
                            "daily_limit": limit,
                            "remaining": max(0, limit - spent),
                            "is_blocked": blocked,
                        }
                    )

            return {
                "day_pt": str(day_pt),
                "next_reset_pt": budget.next_reset_pt().isoformat(),
                "channels": {
                    "total_discovered": total_channels or 0,
                    "selected": selected_channels or 0,
                    "strata_36_counts": strata_map,
                },
                "posts": {
                    "total": total_posts or 0,
                    "backfill_90d": backfill_posts or 0,
                    "live_monitored": live_posts or 0,
                },
                "media_downloaded": media_done or 0,
                "comments": {
                    "posts_done": comments_done or 0,
                    "posts_disabled": comments_disabled or 0,
                    "total_collected": total_comments or 0,
                },
                "scheduled_measurements": sched_map,
                "keys_quota": keys_status,
            }

    status_data = asyncio.run(go())
    typer.echo(json.dumps(status_data, ensure_ascii=False, indent=2))


@yt_app.command("export")
def yt_export(
    label: Annotated[
        str | None,
        typer.Option(help="Метка выгрузки (поддиректория, например snapshot-10-11)"),
    ] = None,
) -> None:
    """Экспорт датасетов в Parquet, CSV и сводного отчёта yt_report.md."""
    settings = get_settings()
    engine = create_database_engine(settings.sqlalchemy_url)
    sessions = create_session_factory(engine)

    async def go() -> dict[str, str]:
        exporter = YouTubePanelExporter(
            sessions,
            export_dir=settings.yt_export_dir,
            thumbs_dir=settings.yt_thumbs_dir,
        )
        try:
            return await exporter.export_all(label=label)
        finally:
            await engine.dispose()

    files = asyncio.run(go())
    typer.echo("Экспорт YouTube панели успешно завершён:")
    typer.echo(json.dumps(files, ensure_ascii=False, indent=2))

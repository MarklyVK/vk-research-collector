"""Генерация сводного отчёта о состоянии сбора YouTube (Задачи 1 и 2).

Сохраняет отчёт в ~/markly/data/youtube/status_report.md и выводит в консоль.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from sqlalchemy import func, select, text

from vk_collector.config import get_settings
from vk_collector.database.models import (
    YTChannel,
    YTKeyDailyUsage,
    YTPost,
    YTTop,
)
from vk_collector.database.session import create_database_engine, create_session_factory
from vk_collector.youtube.budget import PT_TIMEZONE

logger = logging.getLogger(__name__)


def count_disk_media(base_dir: Path) -> dict[str, Any]:
    """Подсчёт сохранённых на диске обложек и кадров для Задач 1 и 2."""
    p = base_dir.expanduser()
    if not p.exists():
        return {
            "main_videos_with_media": 0,
            "main_files_count": 0,
            "top_videos_with_media": 0,
            "top_files_count": 0,
            "total_bytes": 0,
        }

    main_vids = 0
    main_files = 0
    top_vids = 0
    top_files = 0
    total_bytes = 0

    top_dir = p / "top"

    # 1. Main collection media (папки прямо внутри base_dir, кроме 'top')
    try:
        for item in p.iterdir():
            if item.is_dir() and item.name != "top":
                imgs = [
                    f
                    for f in item.iterdir()
                    if f.is_file() and f.suffix.lower() in (".jpg", ".jpeg")
                ]
                if imgs:
                    main_vids += 1
                    main_files += len(imgs)
                    for f in imgs:
                        total_bytes += f.stat().st_size
    except Exception as exc:
        logger.debug("Ошибка обхода директории main media: %s", exc)

    # 2. Top collection media (папки внутри base_dir/top)
    if top_dir.exists():
        try:
            for item in top_dir.iterdir():
                if item.is_dir():
                    imgs = [
                        f
                        for f in item.iterdir()
                        if f.is_file() and f.suffix.lower() in (".jpg", ".jpeg")
                    ]
                    if imgs:
                        top_vids += 1
                        top_files += len(imgs)
                        for f in imgs:
                            total_bytes += f.stat().st_size
        except Exception as exc:
            logger.debug("Ошибка обхода директории top media: %s", exc)

    return {
        "main_videos_with_media": main_vids,
        "main_files_count": main_files,
        "top_videos_with_media": top_vids,
        "top_files_count": top_files,
        "total_bytes": total_bytes,
        "total_mb": round(total_bytes / (1024 * 1024), 2),
    }


async def generate_status_report(
    output_path: Path | str = Path("~/markly/data/youtube/status_report.md"),
    quota_json_path: Path | str = Path("~/markly/data/youtube/quota_usage.json"),
) -> str:
    """Формирование сводного markdown-отчёта по состоянию YouTube Panel & Top."""
    settings = get_settings()
    engine = create_database_engine(settings.sqlalchemy_url)
    sessions = create_session_factory(engine)
    out_file = Path(output_path).expanduser()
    quota_file = Path(quota_json_path).expanduser()

    now_utc = datetime.now(UTC)
    now_pt = datetime.now(PT_TIMEZONE)

    # -------------------------------------------------------------------------
    # 1. Основной сбор: 90-дневные видео по 10 стратам и 4 размерам
    # -------------------------------------------------------------------------
    async with sessions() as session:
        # Каналы по доменам и размерам
        ch_stmt = (
            select(
                YTChannel.domain,
                YTChannel.size_tier,
                func.count(YTChannel.channel_id).label("ch_count"),
            )
            .where(YTChannel.is_selected.is_(True))
            .group_by(YTChannel.domain, YTChannel.size_tier)
            .order_by(YTChannel.domain, YTChannel.size_tier)
        )
        ch_rows = (await session.execute(ch_stmt)).all()

        # Видео по доменам и размерам
        post_stmt = (
            select(
                YTChannel.domain,
                YTChannel.size_tier,
                func.count(YTPost.post_id).label("post_count"),
                func.sum(func.coalesce(YTPost.views, 0)).label("views_sum"),
            )
            .join(YTPost, YTChannel.channel_id == YTPost.channel_id)
            .group_by(YTChannel.domain, YTChannel.size_tier)
            .order_by(YTChannel.domain, YTChannel.size_tier)
        )
        post_rows = (await session.execute(post_stmt)).all()

        total_selected_channels = await session.scalar(
            select(func.count()).select_from(YTChannel).where(YTChannel.is_selected.is_(True))
        )
        total_posts = await session.scalar(select(func.count()).select_from(YTPost))

        # ---------------------------------------------------------------------
        # 2. Таблица yt_top: статистика по категориям source
        # ---------------------------------------------------------------------
        top_total = await session.scalar(select(func.count()).select_from(YTTop))

        top_by_type_stmt = (
            select(
                text(
                    "CASE "
                    "  WHEN source LIKE 'mostPopular%' THEN 'mostPopular' "
                    "  WHEN source LIKE 'weekly_top%' THEN 'weekly_top' "
                    "  WHEN source LIKE 'candidate%' THEN 'candidate' "
                    "  ELSE 'other' "
                    "END AS source_type"
                ),
                func.count(YTTop.id).label("count"),
                func.count(func.distinct(YTTop.source)).label("distinct_sources"),
                func.sum(func.coalesce(YTTop.views, 0)).label("total_views"),
            )
            .group_by(text("source_type"))
            .order_by(text("source_type"))
        )
        top_types = (await session.execute(top_by_type_stmt)).all()

        # ---------------------------------------------------------------------
        # 3. Расход квот из БД
        # ---------------------------------------------------------------------
        usage_stmt = (
            select(
                YTKeyDailyUsage.key_fingerprint,
                YTKeyDailyUsage.day_pt,
                func.sum(YTKeyDailyUsage.units_spent).label("units"),
                func.sum(YTKeyDailyUsage.calls_count).label("calls"),
                func.max(YTKeyDailyUsage.quota_exceeded_at).label("exceeded_at"),
            )
            .where(YTKeyDailyUsage.day_pt == now_pt.date())
            .group_by(YTKeyDailyUsage.key_fingerprint, YTKeyDailyUsage.day_pt)
        )
        db_quota_rows = (await session.execute(usage_stmt)).all()

    await engine.dispose()

    # Данные с диска
    disk_stats = count_disk_media(settings.yt_media_dir)

    # Данные из quota_usage.json
    file_quota_data: dict[str, Any] = {}
    if quota_file.exists():
        with contextlib.suppress(Exception):
            file_quota_data = json.loads(quota_file.read_text(encoding="utf-8"))

    # Карта постов по домену и размеру
    post_map: dict[tuple[str, str], int] = {}
    for dom, st, cnt, _ in post_rows:
        post_map[(dom, st)] = cnt

    # Построение markdown
    lines: list[str] = [
        "# YouTube Data Collection: Status Report",
        "",
        f"- **Сформирован (UTC):** `{now_utc.strftime('%Y-%m-%d %H:%M:%S UTC')}`",
        f"- **Время Pacific Time (Google Quota):** `{now_pt.strftime('%Y-%m-%d %H:%M:%S PT')}`",
        f"- **Всего отобранных каналов (is_selected):** {total_selected_channels or 0}",
        f"- **Всего видео в yt_posts:** {total_posts or 0}",
        f"- **Всего записей в yt_top:** {top_total or 0}",
        "",
        "---",
        "",
        "## 1. Основной сбор (Задача 1: 90 дней по стратам)",
        "",
        "| Домен | Размер (Tier) | Отобрано каналов | Собрано видео |",
        "| :--- | :---: | :---: | :---: |",
    ]

    for dom, st, ch_cnt in ch_rows:
        p_cnt = post_map.get((dom, st), 0)
        lines.append(f"| {dom} | {st} | {ch_cnt} | {p_cnt} |")

    lines.extend(
        [
            "",
            "---",
            "",
            "## 2. Графические превью и кадры на диске",
            "",
            f"- **Базовая директория:** `{settings.yt_media_dir}`",
            f"- **Задача 1 (Основной сбор) видео с медиа:** {disk_stats['main_videos_with_media']}",
            f"- **Задача 1 файлов (cover + hq1-3):** {disk_stats['main_files_count']}",
            f"- **Задача 2 (Топ YouTube) видео с медиа:** {disk_stats['top_videos_with_media']}",
            f"- **Задача 2 файлов (cover + hq1-3):** {disk_stats['top_files_count']}",
            f"- **Общий размер файлов на диске:** {disk_stats['total_mb']} MB",
            "",
            "---",
            "",
            "## 3. Таблица yt_top (Задача 2: Виральный топ)",
            "",
            "| Тип источника | Записей видео | Уникальных source | Суммарные просмотры |",
            "| :--- | :---: | :---: | :---: |",
        ]
    )

    for s_type, cnt, dist_src, v_sum in top_types:
        lines.append(f"| `{s_type}` | {cnt} | {dist_src} | {v_sum:,} |")

    lines.extend(
        [
            "",
            "---",
            "",
            "## 4. Расход квоты YouTube API за текущие сутки",
            "",
            f"Учётная дата (PT): `{now_pt.date()}` (суточный лимит: 10 000 ед. на ключ).",
            "",
            "| Key Fingerprint | Израсходовано (ед.) | Вызовов API | Статус 403 quotaExceeded |",
            "| :--- | :---: | :---: | :---: |",
        ]
    )

    displayed_fps: set[str] = set()
    if db_quota_rows:
        for fp, _, units, calls, exc_at in db_quota_rows:
            displayed_fps.add(fp)
            st = "🔴 ЗАБЛОКИРОВАН" if exc_at else "🟢 В норме"
            lines.append(f"| `{fp}` | {units} | {calls} | {st} |")

    # Добавляем данные из quota_usage.json, если в БД ещё нет строк
    file_keys = file_quota_data.get("keys", {})
    for fp, k_data in file_keys.items():
        if fp not in displayed_fps:
            spent = k_data.get("total_spent", 0)
            calls = sum(ep.get("calls", 0) for ep in k_data.get("endpoints", {}).values())
            st = "🔴 ЗАБЛОКИРОВАН" if k_data.get("blocked") else "🟢 В норме"
            lines.append(f"| `{fp}` | {spent} | {calls} | {st} |")
            displayed_fps.add(fp)

    if not displayed_fps:
        lines.append("| — | 0 | 0 | 🟢 В норме |")

    report_text = "\n".join(lines) + "\n"

    try:
        out_file.parent.mkdir(parents=True, exist_ok=True)
        out_file.write_text(report_text, encoding="utf-8")
        logger.info("Отчёт сохранён в %s", out_file)
    except Exception as exc:
        logger.warning("Не удалось сохранить отчёт в %s: %s", out_file, exc)

    return report_text


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    report = asyncio.run(generate_status_report())
    print(report)


if __name__ == "__main__":
    main()

"""Экспорт датасетов YouTube Virality Panel (Parquet, CSV, Markdown report)."""

from __future__ import annotations

import json
import logging
import os
from contextlib import suppress
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pandas as pd  # type: ignore[import-untyped]
from sqlalchemy import func, select

from vk_collector.database.models import (
    YTChannel,
    YTComment,
    YTKeyDailyUsage,
    YTPost,
    YTScheduledMeasurement,
    YTSearchQueryLog,
    YTTimeseries,
)

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

logger = logging.getLogger(__name__)


class YouTubePanelExporter:
    """Генератор выгрузок Parquet, CSV и сводного отчёта в формате Markdown."""

    def __init__(
        self,
        sessions: async_sessionmaker[AsyncSession],
        export_dir: Path = Path("data/yt_export"),
        thumbs_dir: Path = Path("data/yt_thumbs"),
    ) -> None:
        self._sessions = sessions
        self._export_dir = export_dir
        self._thumbs_dir = thumbs_dir

    async def export_all(self, label: str | None = None) -> dict[str, str]:
        """Экспортировать все датасеты и сформировать yt_report.md.

        Возвращает словарь путей к сформированным файлам.
        """
        target_dir = self._export_dir / label if label else self._export_dir
        target_dir.mkdir(parents=True, exist_ok=True)
        created_files: dict[str, str] = {}

        # Обеспечиваем доступность обложек по относительному пути thumbs/<post_id>_{max,1,2,3}.jpg
        thumbs_symlink = target_dir / "thumbs"
        if not thumbs_symlink.exists() and not thumbs_symlink.is_symlink():
            self._thumbs_dir.mkdir(parents=True, exist_ok=True)
            try:
                rel_path = os.path.relpath(self._thumbs_dir.resolve(), target_dir.resolve())
                thumbs_symlink.symlink_to(rel_path, target_is_directory=True)
            except OSError as err:
                logger.warning(
                    "Не удалось создать относительный симлинк %s -> %s: %s",
                    thumbs_symlink,
                    self._thumbs_dir,
                    err,
                )
                with suppress(OSError):
                    thumbs_symlink.symlink_to(self._thumbs_dir.resolve(), target_is_directory=True)
        created_files["thumbs_dir"] = str(thumbs_symlink)

        # ---------------------------------------------------------------------
        # 1. Экспорт yt_posts (.parquet и .csv)
        # ---------------------------------------------------------------------
        async with self._sessions() as session:
            stmt_posts = (
                select(
                    YTPost,
                    YTChannel.title.label("channel_title"),
                    YTChannel.subscribers_count.label("channel_subscribers"),
                    YTChannel.size_tier.label("channel_size_tier"),
                    YTChannel.domain.label("channel_domain"),
                )
                .join(YTChannel, YTPost.channel_id == YTChannel.channel_id)
                .order_by(YTPost.published_at.desc())
            )
            rows = (await session.execute(stmt_posts)).all()

        posts_data_parquet: list[dict[str, Any]] = []
        posts_data_csv: list[dict[str, Any]] = []

        for p, ch_title, ch_subs, ch_tier, ch_domain in rows:
            pub_iso = p.published_at.isoformat() if p.published_at else ""
            crawl_iso = p.crawled_at.isoformat() if p.crawled_at else ""
            tags_list = p.tags if isinstance(p.tags, list) else []

            base_dict = {
                "platform": "youtube",
                "post_id": p.post_id,
                "url": p.url,
                "channel_id": p.channel_id,
                "channel_title": ch_title,
                "channel_subscribers": ch_subs,
                "channel_size_tier": ch_tier,
                "domain": ch_domain,
                "published_at": pub_iso,
                "crawled_at": crawl_iso,
                "age_hours": p.age_hours,
                "views": p.views,
                "likes": p.likes,
                "comments": p.comments,
                "shares": None,
                "duration_s": p.duration_s,
                "is_short": p.is_short,
                "title": p.title,
                "description": p.description,
                "tags": tags_list,
                "category_id": p.category_id,
                "default_language": p.default_language,
                "thumbnail_url": p.thumbnail_url,
            }

            p_csv = dict(base_dict)
            p_csv["tags"] = json.dumps(tags_list, ensure_ascii=False)

            posts_data_parquet.append(base_dict)
            posts_data_csv.append(p_csv)

        df_posts_pq = pd.DataFrame(posts_data_parquet)
        df_posts_csv = pd.DataFrame(posts_data_csv)

        pq_posts_path = target_dir / "yt_posts.parquet"
        csv_posts_path = target_dir / "yt_posts.csv"
        df_posts_pq.to_parquet(pq_posts_path, index=False)
        df_posts_csv.to_csv(csv_posts_path, index=False, encoding="utf-8")
        created_files["posts_parquet"] = str(pq_posts_path)
        created_files["posts_csv"] = str(csv_posts_path)

        # ---------------------------------------------------------------------
        # 2. Экспорт yt_timeseries (.parquet, .csv и detailed)
        # ---------------------------------------------------------------------
        async with self._sessions() as session:
            stmt_ts = select(YTTimeseries).order_by(
                YTTimeseries.post_id, YTTimeseries.crawled_at.asc()
            )
            ts_rows = (await session.scalars(stmt_ts)).all()

        ts_standard: list[dict[str, Any]] = []
        ts_detailed: list[dict[str, Any]] = []

        for t in ts_rows:
            crawl_iso = t.crawled_at.isoformat() if t.crawled_at else ""
            ts_standard.append(
                {
                    "post_id": t.post_id,
                    "crawled_at": crawl_iso,
                    "views": t.views,
                    "likes": t.likes,
                    "comments": t.comments,
                }
            )
            ts_detailed.append(
                {
                    "post_id": t.post_id,
                    "crawled_at": crawl_iso,
                    "target_age_h": t.target_age_h,
                    "age_seconds": t.age_seconds,
                    "views": t.views,
                    "likes": t.likes,
                    "comments": t.comments,
                }
            )

        df_ts = pd.DataFrame(ts_standard)
        df_ts_det = pd.DataFrame(ts_detailed)

        pq_ts_path = target_dir / "yt_timeseries.parquet"
        csv_ts_path = target_dir / "yt_timeseries.csv"
        pq_ts_det_path = target_dir / "yt_timeseries_detailed.parquet"
        df_ts.to_parquet(pq_ts_path, index=False)
        df_ts.to_csv(csv_ts_path, index=False, encoding="utf-8")
        df_ts_det.to_parquet(pq_ts_det_path, index=False)
        created_files["timeseries_parquet"] = str(pq_ts_path)
        created_files["timeseries_csv"] = str(csv_ts_path)
        created_files["timeseries_detailed_parquet"] = str(pq_ts_det_path)

        # ---------------------------------------------------------------------
        # 3. Экспорт yt_comments (.parquet и .csv)
        # ---------------------------------------------------------------------
        async with self._sessions() as session:
            stmt_comm = select(YTComment).order_by(YTComment.published_at.desc())
            comm_rows = (await session.scalars(stmt_comm)).all()

        comm_list: list[dict[str, Any]] = []
        for c in comm_rows:
            comm_list.append(
                {
                    "comment_id": c.comment_id,
                    "post_id": c.post_id,
                    "author_hash": c.author_hash,
                    "text": c.text,
                    "like_count": c.like_count,
                    "reply_count": c.reply_count,
                    "published_at": c.published_at.isoformat() if c.published_at else "",
                    "crawled_at": c.crawled_at.isoformat() if c.crawled_at else "",
                }
            )

        df_comm = pd.DataFrame(comm_list)
        pq_comm_path = target_dir / "yt_comments.parquet"
        csv_comm_path = target_dir / "yt_comments.csv"
        df_comm.to_parquet(pq_comm_path, index=False)
        df_comm.to_csv(csv_comm_path, index=False, encoding="utf-8")
        created_files["comments_parquet"] = str(pq_comm_path)
        created_files["comments_csv"] = str(csv_comm_path)

        # ---------------------------------------------------------------------
        # 4. Экспорт каналов и логов поиска
        # ---------------------------------------------------------------------
        async with self._sessions() as session:
            channels = (
                await session.scalars(
                    select(YTChannel).order_by(YTChannel.domain, YTChannel.size_tier)
                )
            ).all()
            queries = (
                await session.scalars(
                    select(YTSearchQueryLog).order_by(YTSearchQueryLog.executed_at.asc())
                )
            ).all()

        df_ch = pd.DataFrame(
            [
                {
                    "channel_id": c.channel_id,
                    "title": c.title,
                    "domain": c.domain,
                    "size_tier": c.size_tier,
                    "subscribers_count": c.subscribers_count,
                    "country": c.country,
                    "default_language": c.default_language,
                    "is_selected": c.is_selected,
                    "selection_seed": c.selection_seed,
                    "uploads_playlist_id": c.uploads_playlist_id,
                    "discovered_via_query": c.discovered_via_query,
                    "created_at": c.created_at.isoformat() if c.created_at else "",
                }
                for c in channels
            ]
        )
        csv_ch_path = target_dir / "yt_channels_strata.csv"
        df_ch.to_csv(csv_ch_path, index=False, encoding="utf-8")
        created_files["channels_strata_csv"] = str(csv_ch_path)

        df_queries = pd.DataFrame(
            [
                {
                    "id": q.id,
                    "domain": q.domain,
                    "query_text": q.query_text,
                    "executed_at": q.executed_at.isoformat() if q.executed_at else "",
                    "results_count": q.results_count,
                    "units_cost": q.units_cost,
                }
                for q in queries
            ]
        )
        csv_q_path = target_dir / "yt_search_queries.csv"
        df_queries.to_csv(csv_q_path, index=False, encoding="utf-8")
        created_files["search_queries_csv"] = str(csv_q_path)

        # ---------------------------------------------------------------------
        # 5. Сводный отчёт yt_report.md
        # ---------------------------------------------------------------------
        report_md = await self._generate_markdown_report()
        report_path = target_dir / "yt_report.md"
        report_path.write_text(report_md, encoding="utf-8")
        created_files["report_md"] = str(report_path)

        return created_files

    async def _generate_markdown_report(self) -> str:
        """Сформировать аналитический отчёт по состоянию панели."""
        now_str = datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S UTC")

        async with self._sessions() as session:
            # Стратификационная матрица каналов
            stmt_matrix = (
                select(YTChannel.domain, YTChannel.size_tier, func.count())
                .where(YTChannel.is_selected.is_(True))
                .group_by(YTChannel.domain, YTChannel.size_tier)
            )
            ch_matrix = {(d, t): c for d, t, c in (await session.execute(stmt_matrix)).all()}

            # Матрица видео по стратам
            stmt_post_matrix = (
                select(YTChannel.domain, YTChannel.size_tier, func.count())
                .join(YTPost, YTPost.channel_id == YTChannel.channel_id)
                .group_by(YTChannel.domain, YTChannel.size_tier)
            )
            post_matrix = {(d, t): c for d, t, c in (await session.execute(stmt_post_matrix)).all()}

            # Мониторинг с 09.11
            monitored_posts = await session.scalar(
                select(func.count()).where(YTPost.is_backfill.is_(False))
            )
            sched_total = await session.scalar(select(func.count(YTScheduledMeasurement.id)))
            sched_done = await session.scalar(
                select(func.count(YTScheduledMeasurement.id)).where(
                    YTScheduledMeasurement.status == "completed"
                )
            )
            sched_pending = await session.scalar(
                select(func.count(YTScheduledMeasurement.id)).where(
                    YTScheduledMeasurement.status == "pending"
                )
            )
            sched_failed = await session.scalar(
                select(func.count(YTScheduledMeasurement.id)).where(
                    YTScheduledMeasurement.status == "failed"
                )
            )

            # Медиа статистика
            media_done = await session.scalar(
                select(func.count()).where(YTPost.media_downloaded.is_(True))
            )
            media_pending = await session.scalar(
                select(func.count()).where(YTPost.media_downloaded.is_(False))
            )

            # Комментарии
            comments_count = await session.scalar(select(func.count(YTComment.comment_id)))
            comments_disabled = await session.scalar(
                select(func.count()).where(YTPost.comments_status == "disabled")
            )

            # Расход квоты
            stmt_quota = (
                select(
                    YTKeyDailyUsage.day_pt,
                    YTKeyDailyUsage.endpoint,
                    func.sum(YTKeyDailyUsage.calls_count),
                    func.sum(YTKeyDailyUsage.units_spent),
                )
                .group_by(YTKeyDailyUsage.day_pt, YTKeyDailyUsage.endpoint)
                .order_by(YTKeyDailyUsage.day_pt.desc(), YTKeyDailyUsage.endpoint)
            )
            quota_rows = (await session.execute(stmt_quota)).all()

        domains = [
            "еда и доставка",
            "красота и уход",
            "авто",
            "финансы и бизнес",
            "образование и наука",
            "юмор и развлечения",
            "новости и общество",
            "спорт и фитнес",
            "технологии и гаджеты",
        ]
        tiers = ["S", "M", "L", "XL"]

        lines: list[str] = [
            "# Сводный отчёт YouTube Virality Panel (IEEE Access, TOTE)",
            "",
            f"**Дата и время формирования:** {now_str}",
            "",
            "## 1. Матрица стратификации каналов и видео (9 доменов × 4 размера)",
            "",
            "| Домен | S (кан/вид) | M (кан/вид) | L (кан/вид) | XL (кан/вид) | Всего |",
            "|---|:---:|:---:|:---:|:---:|:---:|",
        ]

        total_ch = 0
        for d in domains:
            row_items = []
            row_ch = 0
            for t in tiers:
                c_ch = ch_matrix.get((d, t), 0)
                c_p = post_matrix.get((d, t), 0)
                row_items.append(f"{c_ch} / {c_p}")
                row_ch += c_ch
            total_ch += row_ch
            lines.append(f"| {d} | {' | '.join(row_items)} | {row_ch} |")

        lines.extend(
            [
                "",
                f"**Итого отобрано каналов в панели:** {total_ch}",
                "",
                "## 2. Мониторинг новых публикаций (с 09.11 по 17.11)",
                "",
                f"- **Отслеживаемых новых видео:** {monitored_posts or 0}",
                f"- **Плановых замеров в очереди всего:** {sched_total or 0}",
                f"  - Выполнено: {sched_done or 0}",
                f"  - В ожидании наступления горизонта: {sched_pending or 0}",
                f"  - Ошибок / пропусков: {sched_failed or 0}",
                "",
                "## 3. Статистика медиафайлов и комментариев",
                "",
                f"- **Видео с загруженными обложками и кадрами:** {media_done or 0}",
                f"- **Видео, ожидающих загрузки медиа:** {media_pending or 0}",
                f"- **Собрано комментариев всего:** {comments_count or 0}",
                f"- **Видео с отключенными комментариями:** {comments_disabled or 0}",
                "",
                "## 4. Расход квоты YouTube Data API v3",
                "",
                "| Дата (PT) | Метод / Endpoint | Вызовов | Израсходовано единиц |",
                "|---|---|:---:|:---:|",
            ]
        )

        for day_pt, endpoint, calls, units in quota_rows:
            lines.append(f"| {day_pt} | `{endpoint}` | {calls} | {units} |")

        lines.extend(
            [
                "",
                "---",
                "*Отчёт сгенерирован автоматически подсистемой YouTube Virality Panel.*",
            ]
        )

        return "\n".join(lines)

"""Сервис сбора вирального топа YouTube (Задача 2: Most Popular, Weekly Top, Candidates)."""

from __future__ import annotations

import csv
import json
import logging
import re
from collections.abc import Sequence
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pandas as pd  # type: ignore[import-untyped]
from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert

from vk_collector.config import Settings
from vk_collector.database.models import YTChannel, YTTop
from vk_collector.youtube.budget import YouTubeQuotaExhaustedError
from vk_collector.youtube.client import (
    YouTubeClient,
    is_short_video,
    parse_iso8601_duration,
    parse_rfc3339_datetime,
)
from vk_collector.youtube.media import YouTubeMediaDownloader

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

logger = logging.getLogger(__name__)

# Период для 12 месяцев еженедельного топа: ровно 53 недели
WEEKLY_TOP_START_UTC = datetime(2025, 11, 11, 0, 0, 0, tzinfo=UTC)
WEEKLY_TOP_END_UTC = datetime(2026, 11, 14, 23, 59, 59, tzinfo=UTC)


def load_candidates_file(path: Path | str) -> list[dict[str, Any]]:
    """Автоопределение формата и чтение файла кандидатов (CSV, TSV, JSON, XLSX)."""
    p = Path(path).expanduser()
    if not p.exists():
        raise FileNotFoundError(f"Файл кандидатов не найден: {p}")
    suffix = p.suffix.lower()

    if suffix in (".xlsx", ".xls"):
        try:
            df = pd.read_excel(p)
            raw_records = df.to_dict(orient="records")
            return [dict(x) for x in raw_records]
        except Exception as exc:
            raise RuntimeError(
                f"Не удалось прочитать Excel-файл {p}. Убедитесь, что установлен openpyxl "
                f"или сохраните файл в CSV/JSON. Ошибка: {exc}"
            ) from exc

    if suffix == ".json":
        data = json.loads(p.read_text(encoding="utf-8"))
        if isinstance(data, list):
            return [dict(x) for x in data if isinstance(x, dict)]
        if isinstance(data, dict):
            for k in ("candidates", "items", "data"):
                if k in data and isinstance(data[k], list):
                    return [dict(x) for x in data[k] if isinstance(x, dict)]
            return [data]
        return []

    # CSV / TSV
    text_content = p.read_text(encoding="utf-8")
    try:
        dialect = csv.Sniffer().sniff(text_content[:4096])
        delim = dialect.delimiter
    except Exception:
        delim = "\t" if suffix == ".tsv" else ","

    reader = csv.DictReader(text_content.splitlines(), delimiter=delim)
    return [dict(r) for r in reader]


def extract_candidate_fields(row: dict[str, Any]) -> dict[str, Any]:
    """Нормализация полей строки кандидата: имя, поисковые фразы, флаги новостей/инфоповода."""
    norm = {str(k).strip().lower(): v for k, v in row.items()}

    # 1. Имя кандидата
    name = ""
    for k in ("candidate_name", "candidate", "name", "title", "кандидат", "название"):
        if k in norm and norm[k] is not None:
            val = str(norm[k]).strip()
            if val and val.lower() != "nan":
                name = val
                break

    # 2. Поисковые фразы
    queries: list[str] = []
    raw_queries = ""
    for k in (
        "search_queries",
        "search_query",
        "queries",
        "query",
        "поисковые_запросы",
        "поисковый_запрос",
        "запрос",
    ):
        if k in norm and norm[k] is not None:
            val = str(norm[k]).strip()
            if val and val.lower() != "nan":
                raw_queries = val
                break

    if raw_queries:
        parts = re.split(r"[;|]", raw_queries)
        for p in parts:
            clean = p.strip()
            if clean and clean not in queries:
                queries.append(clean)
    if not queries and name:
        queries.append(name)

    # 3. Флаг новостей (flag_news)
    flag_news_raw = None
    for k in ("flag_news", "is_news", "news", "новости", "новость"):
        if k in norm:
            flag_news_raw = norm[k]
            break
    flag_news = False
    if flag_news_raw is not None:
        v_str = str(flag_news_raw).strip().lower()
        flag_news = v_str in ("1", "true", "yes", "t", "y", "1.0")

    # 4. Флаг инфоповода (flag_infopovod)
    flag_infopovod_raw = None
    for k in ("flag_infopovod", "is_infopovod", "infopovod", "инфоповод"):
        if k in norm:
            flag_infopovod_raw = norm[k]
            break
    flag_infopovod = False
    if flag_infopovod_raw is not None:
        v_str = str(flag_infopovod_raw).strip().lower()
        flag_infopovod = v_str in ("1", "true", "yes", "t", "y", "1.0")

    return {
        "name": name or "unknown",
        "search_queries": queries,
        "flag_news": flag_news,
        "flag_infopovod": flag_infopovod,
    }


class YouTubeTopService:
    """Сервис сбора данных контура 'Топ YouTube' (Задача 2) строго через YT_TOP_API_KEY."""

    def __init__(
        self,
        sessions: async_sessionmaker[AsyncSession],
        client: YouTubeClient,
        settings: Settings,
        media_downloader: YouTubeMediaDownloader | None = None,
    ) -> None:
        self._sessions = sessions
        self._client = client
        self._settings = settings
        self._media = media_downloader or YouTubeMediaDownloader(base_dir=settings.yt_media_dir)
        self._subscribers_cache: dict[str, int | None] = {}

    async def get_channel_subscribers_batch(
        self, channel_ids: Sequence[str]
    ) -> dict[str, int | None]:
        """Пакетное получение числа подписчиков каналов с кешированием в памяти и БД."""
        result: dict[str, int | None] = {}
        missing: list[str] = []

        # 1. In-memory кеш
        for cid in channel_ids:
            if not cid:
                continue
            if cid in self._subscribers_cache:
                result[cid] = self._subscribers_cache[cid]
            elif cid not in missing:
                missing.append(cid)

        if not missing:
            return result

        # 2. Кеш в таблицах базы данных (yt_channels и yt_top)
        async with self._sessions() as session:
            # yt_channels
            stmt_ch = select(YTChannel.channel_id, YTChannel.subscribers_count).where(
                YTChannel.channel_id.in_(missing)
            )
            for row_cid, subs in (await session.execute(stmt_ch)).all():
                if subs is not None:
                    self._subscribers_cache[row_cid] = subs
                    result[row_cid] = subs

            still_missing = [c for c in missing if c not in result]
            if still_missing:
                stmt_top = (
                    select(YTTop.channel_id, YTTop.channel_subscribers)
                    .where(
                        YTTop.channel_id.in_(still_missing),
                        YTTop.channel_subscribers.is_not(None),
                    )
                    .distinct()
                )
                for top_cid, top_subs in (await session.execute(stmt_top)).all():
                    if top_cid and top_subs is not None:
                        cid_str = str(top_cid)
                        self._subscribers_cache[cid_str] = top_subs
                        result[cid_str] = top_subs

        # 3. Запрос к YouTube Data API (channels.list, 1 ед. квоты на 50 каналов)
        api_missing = [c for c in missing if c not in result]
        if api_missing:
            try:
                ch_details = await self._client.get_channels_batch(api_missing)
                for ch in ch_details:
                    c_id = ch.get("id")
                    if not c_id:
                        continue
                    stats = ch.get("statistics", {})
                    s_str = stats.get("subscriberCount")
                    s_count = int(s_str) if s_str and str(s_str).isdigit() else None
                    self._subscribers_cache[c_id] = s_count
                    result[c_id] = s_count
            except Exception as exc:
                logger.warning("Ошибка запроса подписчиков каналов: %s", exc)

        # Для каналов, у которых подписчики скрыты или не найдены
        for cid in missing:
            if cid not in result:
                self._subscribers_cache[cid] = None
                result[cid] = None

        return result

    async def _upsert_and_download_video(
        self,
        video_data: dict[str, Any],
        source: str,
        rank: int | None = None,
        flag_news: bool = False,
        flag_infopovod: bool = False,
        subscribers_map: dict[str, int | None] | None = None,
        now: datetime | None = None,
    ) -> bool:
        """Сохранение видео в yt_top и скачивание превью и кадров в ~/markly/data/youtube/top/."""
        v_id_raw = video_data.get("id")
        if not v_id_raw:
            return False
        v_id = v_id_raw.get("videoId") if isinstance(v_id_raw, dict) else str(v_id_raw)
        if not v_id:
            return False

        snippet = video_data.get("snippet", {})
        stats = video_data.get("statistics", {})
        content = video_data.get("contentDetails", {})
        now_utc = now or datetime.now(UTC)

        c_id = snippet.get("channelId")
        subs = subscribers_map.get(c_id) if (subscribers_map and c_id) else None
        if subs is None and c_id and c_id in self._subscribers_cache:
            subs = self._subscribers_cache[c_id]

        duration_s = parse_iso8601_duration(content.get("duration", ""))
        is_short = is_short_video(duration_s)

        views = (
            int(stats["viewCount"])
            if "viewCount" in stats and str(stats["viewCount"]).isdigit()
            else None
        )
        likes = (
            int(stats["likeCount"])
            if "likeCount" in stats and str(stats["likeCount"]).isdigit()
            else None
        )
        comments = (
            int(stats["commentCount"])
            if "commentCount" in stats and str(stats["commentCount"]).isdigit()
            else None
        )

        pub_raw = snippet.get("publishedAt")
        pub_dt = parse_rfc3339_datetime(pub_raw) if pub_raw else None

        stmt = insert(YTTop).values(
            video_id=v_id,
            url=f"https://www.youtube.com/watch?v={v_id}",
            title=snippet.get("title"),
            description=snippet.get("description"),
            channel_id=c_id,
            channel_title=snippet.get("channelTitle"),
            channel_subscribers=subs,
            published_at=pub_dt,
            crawled_at=now_utc,
            views=views,
            likes=likes,
            comments=comments,
            duration_s=duration_s,
            is_short=is_short,
            source=source,
            rank=rank,
            flag_news=flag_news,
            flag_infopovod=flag_infopovod,
        )
        upsert_stmt = stmt.on_conflict_do_update(
            index_elements=["video_id", "source"],
            set_={
                "views": stmt.excluded.views,
                "likes": stmt.excluded.likes,
                "comments": stmt.excluded.comments,
                "crawled_at": stmt.excluded.crawled_at,
                "channel_subscribers": func.coalesce(
                    stmt.excluded.channel_subscribers, YTTop.channel_subscribers
                ),
                "rank": stmt.excluded.rank,
                "duration_s": stmt.excluded.duration_s,
                "is_short": stmt.excluded.is_short,
            },
        )

        async with self._sessions() as session:
            await session.execute(upsert_stmt)
            await session.commit()

        # Скачиваем графические превью и кадры строго в 1 каталог для Задачи 2
        thumb_url = snippet.get("thumbnails", {}).get("high", {}).get("url", "")
        try:
            await self._media.download_video_media(v_id, is_top=True, thumbnail_url=thumb_url)
        except Exception as dl_err:
            logger.debug("Ошибка скачивания медиа top для %s: %s", v_id, dl_err)

        return True

    # -------------------------------------------------------------------------
    # Подрежим 2а: Daily Most Popular (--mode top-daily)
    # -------------------------------------------------------------------------
    async def collect_daily_most_popular(self, now: datetime | None = None) -> dict[str, Any]:
        """Сбор суточного чарта Most Popular по региону RU (до 200 видео)."""
        now_utc = now or datetime.now(UTC)
        source = f"mostPopular_{now_utc.strftime('%Y-%m-%d')}"
        logger.info("Запуск подрежима 2а (top-daily) для source=%s", source)

        videos = await self._client.get_most_popular_videos(region_code="RU", max_total=200)
        if not videos:
            logger.warning("Чарт mostPopular вернул 0 видео")
            return {"source": source, "count": 0}

        channel_ids = [v.get("snippet", {}).get("channelId") for v in videos if v.get("snippet")]
        subscribers_map = await self.get_channel_subscribers_batch([c for c in channel_ids if c])

        saved = 0
        for rank, v in enumerate(videos, start=1):
            ok = await self._upsert_and_download_video(
                v,
                source=source,
                rank=rank,
                flag_news=False,
                flag_infopovod=False,
                subscribers_map=subscribers_map,
                now=now_utc,
            )
            if ok:
                saved += 1

        logger.info("Подрежим 2а (top-daily) успешно сохранён: %d видео", saved)
        return {"source": source, "count": saved}

    # -------------------------------------------------------------------------
    # Подрежим 2б: Weekly Top for 12 Months (--mode top-weekly --batch 1|2)
    # -------------------------------------------------------------------------
    def get_weekly_intervals(self) -> list[tuple[int, datetime, datetime]]:
        """Формирование 53 недельных интервалов за 12 месяцев (11.11.2025 - 14.11.2026)."""
        intervals: list[tuple[int, datetime, datetime]] = []
        for w in range(1, 54):
            w_start = WEEKLY_TOP_START_UTC + timedelta(days=(w - 1) * 7)
            w_end = min(w_start + timedelta(days=7), WEEKLY_TOP_END_UTC)
            intervals.append((w, w_start, w_end))
        return intervals

    async def collect_weekly_top(self, batch: int) -> dict[str, Any]:
        """Сбор недельного топа по просмотрам за 12 месяцев с разбивкой по батчам и чекпоинтами."""
        if batch not in (1, 2):
            raise ValueError(f"batch должен быть 1 или 2, получено {batch}")

        all_intervals = self.get_weekly_intervals()
        if batch == 1:
            # Недели 1–27 (5 400 ед. квоты на поиск)
            active_weeks = [item for item in all_intervals if 1 <= item[0] <= 27]
        else:
            # Недели 28–53 (5 200 ед. квоты на поиск)
            active_weeks = [item for item in all_intervals if 28 <= item[0] <= 53]

        logger.info(
            "Запуск подрежима 2б (top-weekly) батч %d: недели %d..%d",
            batch,
            active_weeks[0][0],
            active_weeks[-1][0],
        )

        weeks_processed = 0
        weeks_skipped = 0
        total_videos = 0

        for _w_num, w_start, w_end in active_weeks:
            year, week, _ = w_start.isocalendar()
            source = f"weekly_top_{year}-W{week:02d}"

            # Проверка чекпоинта: если неделя уже собрана в yt_top, пропускаем
            async with self._sessions() as session:
                existing_count = await session.scalar(
                    select(func.count()).select_from(YTTop).where(YTTop.source == source)
                )
            if existing_count and existing_count > 0:
                logger.info(
                    "Чекпоинт: неделя %s уже содержит %d видео, пропускаем",
                    source,
                    existing_count,
                )
                weeks_skipped += 1
                continue

            try:
                # 2 страницы search.list (200 единиц квоты, до 100 видео)
                video_ids = await self._client.search_videos(
                    published_after=w_start,
                    published_before=w_end,
                    order="viewCount",
                    region_code="RU",
                    relevance_language="ru",
                    max_pages=2,
                    max_results=50,
                )

                if not video_ids:
                    logger.info("Неделя %s вернула 0 видео", source)
                    weeks_processed += 1
                    continue

                # Добор деталей через videos.list
                details = await self._client.get_videos_batch(video_ids)

                # Добор подписчиков каналов
                c_ids = [v.get("snippet", {}).get("channelId") for v in details if v.get("snippet")]
                subs_map = await self.get_channel_subscribers_batch([c for c in c_ids if c])

                # Сохраняем в yt_top и скачиваем превью
                saved_in_week = 0
                for rank, v in enumerate(details, start=1):
                    ok = await self._upsert_and_download_video(
                        v,
                        source=source,
                        rank=rank,
                        flag_news=False,
                        flag_infopovod=False,
                        subscribers_map=subs_map,
                    )
                    if ok:
                        saved_in_week += 1

                total_videos += saved_in_week
                weeks_processed += 1
                logger.info("Неделя %s сохранена: %d видео", source, saved_in_week)

            except YouTubeQuotaExhaustedError:
                logger.warning(
                    "Квота исчерпана при обработке недели %s. Сохраняем прогресс и выходим.",
                    source,
                )
                break

        return {
            "batch": batch,
            "weeks_processed": weeks_processed,
            "weeks_skipped": weeks_skipped,
            "total_videos": total_videos,
        }

    # -------------------------------------------------------------------------
    # Подрежим 2в: Search by Candidates List (--mode top-candidates --file <path>)
    # -------------------------------------------------------------------------
    async def collect_candidates_top(self, file_path: Path | str) -> dict[str, Any]:
        """Поиск и сбор видео по списку кандидатов с фильтрацией новостей."""
        rows = load_candidates_file(file_path)
        logger.info("Загружено %d строк кандидатов из файла %s", len(rows), file_path)

        candidates_processed = 0
        candidates_skipped_news = 0
        total_videos = 0

        for row in rows:
            extracted = extract_candidate_fields(row)
            c_name = extracted["name"]

            # КРИТИЧНО: Фильтрация новостей
            if extracted["flag_news"]:
                logger.info("Пропуск кандидата '%s': flag_news=True", c_name)
                candidates_skipped_news += 1
                continue

            queries = extracted["search_queries"]
            if not queries:
                continue

            source = f"candidate_{c_name}"
            # Проверяем чекпоинт
            async with self._sessions() as session:
                existing_count = await session.scalar(
                    select(func.count()).select_from(YTTop).where(YTTop.source == source)
                )
            if existing_count and existing_count >= 10:
                logger.info(
                    "Чекпоинт: кандидат '%s' уже собран (%d видео), пропускаем",
                    c_name,
                    existing_count,
                )
                candidates_processed += 1
                continue

            found_video_ids: list[str] = []
            try:
                for q in queries:
                    # search.list (100 ед. квоты)
                    v_ids = await self._client.search_videos(
                        query=q,
                        region_code="RU",
                        relevance_language="ru",
                        max_pages=1,
                        max_results=50,
                    )
                    for vid in v_ids:
                        if vid not in found_video_ids:
                            found_video_ids.append(vid)

                if not found_video_ids:
                    candidates_processed += 1
                    continue

                details = await self._client.get_videos_batch(found_video_ids)
                c_ids = [v.get("snippet", {}).get("channelId") for v in details if v.get("snippet")]
                subs_map = await self.get_channel_subscribers_batch([c for c in c_ids if c])

                saved_for_candidate = 0
                for rank, v in enumerate(details, start=1):
                    ok = await self._upsert_and_download_video(
                        v,
                        source=source,
                        rank=rank,
                        flag_news=False,
                        flag_infopovod=extracted["flag_infopovod"],
                        subscribers_map=subs_map,
                    )
                    if ok:
                        saved_for_candidate += 1

                total_videos += saved_for_candidate
                candidates_processed += 1
                logger.info("Кандидат '%s' сохранён: %d видео", c_name, saved_for_candidate)

            except YouTubeQuotaExhaustedError:
                logger.warning(
                    "Квота исчерпана при обработке кандидата '%s'. Завершаем сбор.", c_name
                )
                break

        return {
            "candidates_processed": candidates_processed,
            "candidates_skipped_news": candidates_skipped_news,
            "total_videos": total_videos,
        }

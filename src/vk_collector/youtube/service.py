"""Сервис YouTube Virality Panel: обнаружение каналов, стратификация, backfill, мониторинг."""

from __future__ import annotations

import hashlib
import logging
import math
from collections import defaultdict
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any

from sqlalchemy import func, select, text, update
from sqlalchemy.dialects.postgresql import insert

from vk_collector.config import Settings, load_yt_keywords
from vk_collector.database.models import (
    YTChannel,
    YTComment,
    YTPost,
    YTScheduledMeasurement,
    YTSearchQueryLog,
    YTTimeseries,
)
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

# Горизонты плановых замеров для новых видео панели
MEASUREMENT_HORIZONS_HOURS: list[int] = [1, 3, 6, 12, 24, 48, 72, 168]

# Начало окна активного мониторинга новых публикаций
MONITORING_START_UTC = datetime(2026, 11, 9, 0, 0, 0, tzinfo=UTC)


def compute_size_tier(subscribers_count: int | None) -> str:
    """Определить размерную страту канала (S < 10k, M 10k-100k, L 100k-1M, XL > 1M)."""
    if subscribers_count is None or subscribers_count < 10_000:
        return "S"
    if subscribers_count < 100_000:
        return "M"
    if subscribers_count < 1_000_000:
        return "L"
    return "XL"


def is_russian_channel(country: str | None, title: str, description: str) -> bool:
    """Проверка русскоязычности канала: страна RU или >= 30% кириллицы в названии/описании."""
    if country and country.upper() == "RU":
        return True
    combined = f"{title} {description}"
    letters = sum(1 for c in combined if c.isalpha())
    if letters == 0:
        return False
    cyrillic = sum(1 for c in combined if "\u0400" <= c <= "\u04ff")
    return (cyrillic / letters) >= 0.30


def derive_uploads_playlist_id(channel_id: str) -> str:
    """Вывести ID плейлиста всех загрузок канала (замена префикса UC на UU)."""
    if channel_id.startswith("UC") and len(channel_id) > 2:
        return "UU" + channel_id[2:]
    return ""


def pick_best_thumbnail_url(thumbnails: dict[str, Any]) -> str:
    """Выбрать URL обложки с наибольшим разрешением из структуры snippet.thumbnails."""
    for res in ("maxres", "standard", "high", "medium", "default"):
        thumb = thumbnails.get(res)
        if isinstance(thumb, dict) and thumb.get("url"):
            return str(thumb["url"])
    return ""


class YouTubePanelService:
    """Основной сервис управления жизненным циклом YouTube Virality Panel."""

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
        self._media_downloader = media_downloader or YouTubeMediaDownloader(
            thumbs_dir=settings.yt_thumbs_dir
        )

    async def discover_and_stratify_channels(
        self,
        seed: int = 20261108,
        max_queries: int | None = None,
        keywords_path: Path = Path("config/yt_keywords.yml"),
    ) -> dict[str, Any]:
        """Обнаружение каналов по 9 доменам, загрузка метаданных и стратификация 9×4×8."""
        domains_keywords = load_yt_keywords(keywords_path)
        # Канал может встречаться в нескольких доменах: channel_id -> list[(domain, query)]
        discovered_channels: dict[str, list[tuple[str, str]]] = defaultdict(list)
        queries_run = 0

        # 1. Поиск каналов через search.list
        for domain, queries in domains_keywords.items():
            for query in queries:
                if max_queries is not None and queries_run >= max_queries:
                    break
                items = await self._client.search_channels(query, max_results=50)
                queries_run += 1

                # Запись в yt_search_query_logs
                async with self._sessions() as session:
                    log_entry = YTSearchQueryLog(
                        domain=domain,
                        query_text=query,
                        results_count=len(items),
                        units_cost=100,
                    )
                    session.add(log_entry)
                    await session.commit()

                for item in items:
                    c_id = item.get("id", {}).get("channelId") or item.get("snippet", {}).get(
                        "channelId"
                    )
                    if c_id and not any(d == domain for d, _ in discovered_channels[c_id]):
                        discovered_channels[c_id].append((domain, query))

        # 2. Пакетная загрузка каналов через channels.list (пачками до 50)
        channel_ids = list(discovered_channels.keys())
        channel_details = await self._client.get_channels_batch(channel_ids)

        valid_channels: list[dict[str, Any]] = []
        for details in channel_details:
            c_id = details.get("id", "")
            snippet = details.get("snippet", {})
            statistics = details.get("statistics", {})
            content_details = details.get("contentDetails", {})

            # Исключение каналов со скрытыми подписчиками
            if statistics.get("hiddenSubscriberCount", False):
                continue

            # Исключение каналов без видео (videoCount == 0)
            raw_videos = statistics.get("videoCount")
            video_count = (
                int(raw_videos) if raw_videos is not None and str(raw_videos).isdigit() else 0
            )
            if video_count == 0:
                continue

            country = snippet.get("country")
            title = snippet.get("title", "")
            description = snippet.get("description", "")

            # Исключение нерусскоязычных каналов
            if not is_russian_channel(country, title, description):
                continue

            raw_subs = statistics.get("subscriberCount")
            subs = int(raw_subs) if raw_subs is not None and str(raw_subs).isdigit() else None
            tier = compute_size_tier(subs)

            # Домены, по которым найден канал
            cand_domains = discovered_channels.get(c_id, [("разное", "")])
            domain, query = cand_domains[0]

            uploads_id = content_details.get("relatedPlaylists", {}).get(
                "uploads"
            ) or derive_uploads_playlist_id(c_id)

            valid_channels.append(
                {
                    "channel_id": c_id,
                    "title": title,
                    "description": description,
                    "country": country,
                    "default_language": snippet.get("defaultLanguage"),
                    "subscribers_count": subs,
                    "size_tier": tier,
                    "domain": domain,
                    "candidate_domains": cand_domains,
                    "uploads_playlist_id": uploads_id,
                    "discovered_via_query": query,
                }
            )

        # 3. Стратификация: защита от перезаписи домена при коллизии и приоритет активных каналов
        strata: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
        for ch in valid_channels:
            strata[(ch["domain"], ch["size_tier"])].append(ch)

        # Если канал найден в нескольких доменах [d1, d2], не перезаписываем d1,
        # если в страте d1 ещё не набралось 8 каналов. Переносим в d2 только при избытке в d1.
        for ch in valid_channels:
            cand_domains = ch.get("candidate_domains", [])
            if len(cand_domains) > 1:
                d1 = cand_domains[0][0]
                tier = ch["size_tier"]
                # Переносим в d_alt только если в страте d1 уже есть с запасом > 8 каналов
                if len(strata[(d1, tier)]) > 8:
                    for d_alt, q_alt in cand_domains[1:]:
                        if len(strata[(d_alt, tier)]) < 8:
                            strata[(d1, tier)].remove(ch)
                            ch["domain"] = d_alt
                            ch["discovered_via_query"] = q_alt
                            strata[(d_alt, tier)].append(ch)
                            break

        # Проверка недавней активности через бесплатный RSS (0 единиц квоты)
        now_utc = datetime.now(UTC)
        cutoff_90d = now_utc - timedelta(days=90)
        selected_ids: set[str] = set()

        for (_domain, _size_tier), candidates in strata.items():
            # Детерминированная сортировка по seed
            candidates.sort(
                key=lambda c: hashlib.sha256(f"{seed}:{c['channel_id']}".encode()).hexdigest()
            )
            active_candidates: list[dict[str, Any]] = []
            other_candidates: list[dict[str, Any]] = []

            for cand in candidates:
                if len(active_candidates) >= 8:
                    other_candidates.append(cand)
                    continue

                rss_videos = await self._client.fetch_channel_rss_video_ids(cand["channel_id"])
                has_recent = any(
                    rv.get("published_at") is not None and rv["published_at"] >= cutoff_90d
                    for rv in rss_videos
                )
                if has_recent:
                    active_candidates.append(cand)
                else:
                    other_candidates.append(cand)

            # Строгий приоритет активным каналам, добираем до 8 из остальных кандидатов
            top_candidates = active_candidates[:8]
            if len(top_candidates) < 8:
                needed = 8 - len(top_candidates)
                top_candidates.extend(other_candidates[:needed])

            for cand in top_candidates:
                selected_ids.add(cand["channel_id"])

        # 4. Сохранение в yt_channels
        async with self._sessions() as session:
            for ch in valid_channels:
                is_sel = ch["channel_id"] in selected_ids
                stmt = insert(YTChannel).values(
                    channel_id=ch["channel_id"],
                    title=ch["title"],
                    description=ch["description"],
                    country=ch["country"],
                    default_language=ch["default_language"],
                    subscribers_count=ch["subscribers_count"],
                    size_tier=ch["size_tier"],
                    domain=ch["domain"],
                    uploads_playlist_id=ch["uploads_playlist_id"],
                    is_selected=is_sel,
                    selection_seed=seed if is_sel else None,
                    discovered_via_query=ch["discovered_via_query"],
                )
                upsert_stmt = stmt.on_conflict_do_update(
                    index_elements=["channel_id"],
                    set_={
                        "title": stmt.excluded.title,
                        "description": stmt.excluded.description,
                        "country": stmt.excluded.country,
                        "default_language": stmt.excluded.default_language,
                        "subscribers_count": stmt.excluded.subscribers_count,
                        "size_tier": stmt.excluded.size_tier,
                        "domain": stmt.excluded.domain,
                        "uploads_playlist_id": stmt.excluded.uploads_playlist_id,
                        "is_selected": stmt.excluded.is_selected,
                        "selection_seed": stmt.excluded.selection_seed,
                        "updated_at": func.now(),
                    },
                )
                await session.execute(upsert_stmt)
            await session.commit()

        # Статистика заполненности страт
        distribution: dict[str, int] = {}
        for dom, tier in strata:
            count = sum(
                1
                for c in valid_channels
                if c["domain"] == dom and c["size_tier"] == tier and c["channel_id"] in selected_ids
            )
            distribution[f"{dom}:{tier}"] = count

        return {
            "queries_executed": queries_run,
            "total_discovered": len(discovered_channels),
            "valid_russian_channels": len(valid_channels),
            "selected_channels": len(selected_ids),
            "strata_distribution": distribution,
        }

    async def backfill_90d_videos(self, days: int = 90) -> dict[str, int]:
        """Выгрузка ретроспективы видео за 90 дней для отобранных каналов панели."""
        async with self._sessions() as session:
            stmt = select(YTChannel).where(YTChannel.is_selected.is_(True))
            channels = (await session.scalars(stmt)).all()

        now_utc = datetime.now(UTC)
        cutoff_date = now_utc - timedelta(days=days)
        total_videos_saved = 0
        channels_processed = 0

        for ch in channels:
            playlist_id = ch.uploads_playlist_id or derive_uploads_playlist_id(ch.channel_id)
            if not playlist_id:
                continue

            # Получаем элементы плейлиста за окно 90 дней
            items = await self._client.get_playlist_items(
                playlist_id, published_after=cutoff_date, max_pages=15
            )
            video_ids: list[str] = []
            for it in items:
                v_id = it.get("contentDetails", {}).get("videoId") or it.get("snippet", {}).get(
                    "resourceId", {}
                ).get("videoId")
                if v_id:
                    video_ids.append(v_id)

            if not video_ids:
                channels_processed += 1
                continue

            # Запрашиваем детали пачками до 50 через videos.list
            video_details = await self._client.get_videos_batch(video_ids)

            async with self._sessions() as session:
                for v in video_details:
                    v_id = v.get("id")
                    if not v_id:
                        continue
                    snippet = v.get("snippet", {})
                    stats = v.get("statistics", {})
                    content = v.get("contentDetails", {})

                    pub_raw = snippet.get("publishedAt", "")
                    if not pub_raw:
                        continue
                    pub_dt = parse_rfc3339_datetime(pub_raw)
                    if pub_dt < cutoff_date:
                        continue

                    duration_s = parse_iso8601_duration(content.get("duration", ""))
                    age_h = max(0.0, (now_utc - pub_dt).total_seconds() / 3600.0)

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

                    thumb_url = pick_best_thumbnail_url(snippet.get("thumbnails", {}))
                    raw_tags = snippet.get("tags", [])
                    tags = [str(t) for t in raw_tags] if isinstance(raw_tags, list) else []

                    # 1. Upsert в yt_posts
                    post_stmt = insert(YTPost).values(
                        post_id=v_id,
                        url=f"https://www.youtube.com/watch?v={v_id}",
                        channel_id=ch.channel_id,
                        published_at=pub_dt,
                        crawled_at=now_utc,
                        age_hours=age_h,
                        views=views,
                        likes=likes,
                        comments=comments,
                        shares=None,
                        duration_s=duration_s,
                        is_short=is_short_video(duration_s),
                        title=snippet.get("title", ""),
                        description=snippet.get("description", ""),
                        tags=tags,
                        category_id=snippet.get("categoryId"),
                        default_language=snippet.get("defaultLanguage")
                        or snippet.get("defaultAudioLanguage"),
                        thumbnail_url=thumb_url,
                        is_backfill=True,
                        media_downloaded=False,
                        comments_status="pending",
                    )
                    upsert_post = post_stmt.on_conflict_do_update(
                        index_elements=["post_id"],
                        set_={
                            "views": post_stmt.excluded.views,
                            "likes": post_stmt.excluded.likes,
                            "comments": post_stmt.excluded.comments,
                            "crawled_at": post_stmt.excluded.crawled_at,
                            "age_hours": post_stmt.excluded.age_hours,
                        },
                    )
                    await session.execute(upsert_post)

                    # 2. Первичный срез в yt_timeseries (target_age_h = 0)
                    ts_stmt = insert(YTTimeseries).values(
                        post_id=v_id,
                        crawled_at=now_utc,
                        target_age_h=0,
                        age_seconds=int((now_utc - pub_dt).total_seconds()),
                        views=views,
                        likes=likes,
                        comments=comments,
                    )
                    upsert_ts = ts_stmt.on_conflict_do_nothing(
                        index_elements=["post_id", "target_age_h"],
                        index_where=text("target_age_h IS NOT NULL"),
                    )
                    await session.execute(upsert_ts)
                    total_videos_saved += 1

                await session.commit()

            channels_processed += 1

        return {
            "channels_processed": channels_processed,
            "videos_saved": total_videos_saved,
        }

    async def sample_and_collect_comments(
        self,
        sample_size: int = 3000,
        seed: int = 20261108,
    ) -> dict[str, int]:
        """Стратифицированная выборка 3 000 видео из 90-дневного окна и сбор топ-20 комментариев."""
        async with self._sessions() as session:
            # Выбираем ретроспективные видео с комментариями > 0 и статусом != 'done'
            stmt = (
                select(YTPost.post_id, YTChannel.domain, YTChannel.size_tier)
                .join(YTChannel, YTPost.channel_id == YTChannel.channel_id)
                .where(
                    YTPost.is_backfill.is_(True),
                    YTPost.comments.is_not(None),
                    YTPost.comments > 0,
                    YTPost.comments_status.notin_(["done", "disabled"]),
                )
            )
            candidates = (await session.execute(stmt)).all()

        if not candidates:
            return {"sampled_posts": 0, "comments_collected": 0, "disabled_comments": 0}

        # Группируем кандидатов по 36 стратам
        strata_posts: dict[tuple[str, str], list[str]] = defaultdict(list)
        for post_id, domain, size_tier in candidates:
            strata_posts[(domain, size_tier)].append(post_id)

        target_per_stratum = max(1, math.ceil(sample_size / max(1, len(strata_posts))))
        sampled_post_ids: list[str] = []

        for _, post_ids in strata_posts.items():
            # Детерминированный отбор
            post_ids.sort(key=lambda pid: hashlib.sha256(f"{seed}:{pid}".encode()).hexdigest())
            sampled_post_ids.extend(post_ids[:target_per_stratum])

        # Ограничиваем общим лимитом выборки
        sampled_post_ids = sampled_post_ids[:sample_size]

        comments_collected = 0
        disabled_count = 0
        now_utc = datetime.now(UTC)

        for pid in sampled_post_ids:
            status, items = await self._client.get_top_comments(pid, max_results=20)
            async with self._sessions() as session:
                if status == "disabled":
                    disabled_count += 1
                    await session.execute(
                        update(YTPost)
                        .where(YTPost.post_id == pid)
                        .values(comments_status="disabled")
                    )
                    await session.commit()
                    continue

                for thread in items:
                    top_c = thread.get("snippet", {}).get("topLevelComment", {})
                    c_id = top_c.get("id")
                    if not c_id:
                        continue
                    snip = top_c.get("snippet", {})
                    author_raw = (
                        snip.get("authorChannelId", {}).get("value")
                        or snip.get("authorDisplayName")
                        or ""
                    )
                    # 152-ФЗ compliance: SHA-256 хеширование автора
                    author_hash = hashlib.sha256(str(author_raw).encode("utf-8")).hexdigest()
                    pub_dt = parse_rfc3339_datetime(snip.get("publishedAt", ""))

                    c_stmt = insert(YTComment).values(
                        comment_id=c_id,
                        post_id=pid,
                        author_hash=author_hash,
                        text=snip.get("textDisplay", ""),
                        like_count=int(snip.get("likeCount", 0)),
                        reply_count=int(thread.get("snippet", {}).get("totalReplyCount", 0)),
                        published_at=pub_dt,
                        crawled_at=now_utc,
                    )
                    await session.execute(
                        c_stmt.on_conflict_do_nothing(index_elements=["comment_id"])
                    )
                    comments_collected += 1

                await session.execute(
                    update(YTPost).where(YTPost.post_id == pid).values(comments_status="done")
                )
                await session.commit()

        return {
            "sampled_posts": len(sampled_post_ids),
            "comments_collected": comments_collected,
            "disabled_comments": disabled_count,
        }

    async def panel_tick(
        self,
        now: datetime | None = None,
        monitoring_start: datetime = MONITORING_START_UTC,
    ) -> dict[str, Any]:
        """Один такт панели: обнаружение новых видео, плановые замеры и подкачка медиа."""
        now_utc = now or datetime.now(UTC)

        # ---------------------------------------------------------------------
        # 1. Обнаружение новых публикаций выбранных каналов (RSS + fallback)
        # ---------------------------------------------------------------------
        async with self._sessions() as session:
            stmt_ch = select(YTChannel).where(YTChannel.is_selected.is_(True))
            selected_channels = (await session.scalars(stmt_ch)).all()

        new_video_ids: list[tuple[str, str]] = []  # (video_id, channel_id)

        for ch in selected_channels:
            # 0 units: пробуем бесплатный публичный XML-фид
            rss_videos = await self._client.fetch_channel_rss_video_ids(ch.channel_id)
            if rss_videos:
                for rv in rss_videos:
                    pub_at = rv.get("published_at")
                    v_id = rv.get("video_id")
                    if v_id and pub_at and pub_at >= monitoring_start:
                        new_video_ids.append((v_id, ch.channel_id))
            else:
                # Фоллбэк на 1 страницу playlistItems.list (1 единица квоты)
                p_id = ch.uploads_playlist_id or derive_uploads_playlist_id(ch.channel_id)
                if p_id:
                    p_items = await self._client.get_playlist_items(
                        p_id, published_after=monitoring_start, max_pages=1
                    )
                    for it in p_items:
                        v_id = it.get("contentDetails", {}).get("videoId")
                        pub_raw = it.get("snippet", {}).get("publishedAt")
                        if v_id and pub_raw:
                            try:
                                if parse_rfc3339_datetime(pub_raw) >= monitoring_start:
                                    new_video_ids.append((v_id, ch.channel_id))
                            except Exception:
                                pass

        # Исключаем видео, которые уже есть в yt_posts
        unique_new: list[tuple[str, str]] = []
        if new_video_ids:
            all_vids = [v[0] for v in new_video_ids]
            async with self._sessions() as session:
                existing_stmt = select(YTPost.post_id).where(YTPost.post_id.in_(all_vids))
                existing = set((await session.scalars(existing_stmt)).all())

            seen_vids: set[str] = set()
            for vid, cid in new_video_ids:
                if vid not in existing and vid not in seen_vids:
                    seen_vids.add(vid)
                    unique_new.append((vid, cid))

        new_posts_registered = 0
        if unique_new:
            # Запрашиваем детали пачками до 50 (1 единица на 50 видео)
            v_ids = [v[0] for v in unique_new]
            v_details = await self._client.get_videos_batch(v_ids)
            ch_map = dict(unique_new)

            async with self._sessions() as session:
                for vd in v_details:
                    v_id = vd.get("id")
                    if not v_id:
                        continue
                    snip = vd.get("snippet", {})
                    stats = vd.get("statistics", {})
                    content = vd.get("contentDetails", {})
                    pub_dt = parse_rfc3339_datetime(snip.get("publishedAt", ""))
                    dur_s = parse_iso8601_duration(content.get("duration", ""))
                    age_h = max(0.0, (now_utc - pub_dt).total_seconds() / 3600.0)

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

                    thumb_url = pick_best_thumbnail_url(snip.get("thumbnails", {}))
                    raw_tags = snip.get("tags", [])
                    tags = [str(t) for t in raw_tags] if isinstance(raw_tags, list) else []

                    # Добавляем в yt_posts
                    post_row = YTPost(
                        post_id=v_id,
                        url=f"https://www.youtube.com/watch?v={v_id}",
                        channel_id=ch_map.get(v_id, ""),
                        published_at=pub_dt,
                        crawled_at=now_utc,
                        age_hours=age_h,
                        views=views,
                        likes=likes,
                        comments=comments,
                        shares=None,
                        duration_s=dur_s,
                        is_short=is_short_video(dur_s),
                        title=snip.get("title", ""),
                        description=snip.get("description", ""),
                        tags=tags,
                        category_id=snip.get("categoryId"),
                        default_language=snip.get("defaultLanguage")
                        or snip.get("defaultAudioLanguage"),
                        thumbnail_url=thumb_url,
                        is_backfill=False,
                        media_downloaded=False,
                        comments_status="pending",
                    )
                    session.add(post_row)

                    # Начальный срез в yt_timeseries (0h)
                    ts_row = YTTimeseries(
                        post_id=v_id,
                        crawled_at=now_utc,
                        target_age_h=0,
                        age_seconds=int((now_utc - pub_dt).total_seconds()),
                        views=views,
                        likes=likes,
                        comments=comments,
                    )
                    session.add(ts_row)

                    # 8 плановых замеров в очередь (с правилом допуска 30 минут)
                    for h in MEASUREMENT_HORIZONS_HOURS:
                        sched_time = pub_dt + timedelta(hours=h)
                        if sched_time < now_utc - timedelta(minutes=30):
                            sched_status = "skipped"
                            comp_at: datetime | None = now_utc
                        else:
                            sched_status = "pending"
                            comp_at = None

                        sched = YTScheduledMeasurement(
                            post_id=v_id,
                            target_age_h=h,
                            scheduled_for=sched_time,
                            status=sched_status,
                            completed_at=comp_at,
                        )
                        session.add(sched)

                    new_posts_registered += 1
                await session.commit()

        # ---------------------------------------------------------------------
        # 2. Выполнение плановых замеров (SELECT ... FOR UPDATE SKIP LOCKED)
        # ---------------------------------------------------------------------
        measurements_completed = 0
        measurements_skipped = 0
        async with self._sessions() as session:
            stmt_sched = (
                select(YTScheduledMeasurement)
                .where(
                    YTScheduledMeasurement.status == "pending",
                    YTScheduledMeasurement.scheduled_for <= now_utc,
                )
                .order_by(YTScheduledMeasurement.scheduled_for.asc())
                .limit(100)
                .with_for_update(skip_locked=True)
            )
            due_measurements = (await session.scalars(stmt_sched)).all()

            if due_measurements:
                valid_measurements: list[YTScheduledMeasurement] = []
                for m in due_measurements:
                    # Проверка просроченности горизонта (grace window)
                    is_expired = (
                        m.target_age_h in (1, 3, 6)
                        and now_utc > m.scheduled_for + timedelta(minutes=90)
                    ) or (m.target_age_h >= 12 and now_utc > m.scheduled_for + timedelta(hours=4))

                    if is_expired:
                        m.status = "skipped"
                        m.completed_at = now_utc
                        measurements_skipped += 1
                    else:
                        valid_measurements.append(m)

                if valid_measurements:
                    post_ids_to_fetch = list({m.post_id for m in valid_measurements})
                    video_data_list = await self._client.get_videos_batch(post_ids_to_fetch)
                    video_map = {v.get("id"): v for v in video_data_list if v.get("id")}

                    for m in valid_measurements:
                        v_info = video_map.get(m.post_id)
                        if not v_info:
                            m.status = "failed"
                            continue

                        stats = v_info.get("statistics", {})
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

                        # Находим публикацию для вычисления age_seconds
                        post = await session.get(YTPost, m.post_id)
                        if post:
                            age_sec = int((now_utc - post.published_at).total_seconds())
                            post.views = views
                            post.likes = likes
                            post.comments = comments
                            post.crawled_at = now_utc
                            post.age_hours = max(0.0, age_sec / 3600.0)
                        else:
                            age_sec = m.target_age_h * 3600

                        # Вставляем временной ряд для планового замера
                        ts_entry = insert(YTTimeseries).values(
                            post_id=m.post_id,
                            crawled_at=now_utc,
                            target_age_h=m.target_age_h,
                            age_seconds=age_sec,
                            views=views,
                            likes=likes,
                            comments=comments,
                        )
                        await session.execute(
                            ts_entry.on_conflict_do_update(
                                index_elements=["post_id", "target_age_h"],
                                index_where=text("target_age_h IS NOT NULL"),
                                set_={
                                    "views": ts_entry.excluded.views,
                                    "likes": ts_entry.excluded.likes,
                                    "comments": ts_entry.excluded.comments,
                                    "crawled_at": ts_entry.excluded.crawled_at,
                                    "age_seconds": ts_entry.excluded.age_seconds,
                                },
                            )
                        )

                        m.status = "completed"
                        m.completed_at = now_utc
                        measurements_completed += 1

                await session.commit()

        # ---------------------------------------------------------------------
        # 3. Подкачка медиа порциями
        # ---------------------------------------------------------------------
        media_downloaded = await self._media_downloader.download_media_for_posts(
            self._sessions, limit=10
        )

        return {
            "timestamp": now_utc.isoformat(),
            "new_posts_registered": new_posts_registered,
            "measurements_completed": measurements_completed,
            "measurements_skipped": measurements_skipped,
            "media_downloaded": media_downloaded,
        }

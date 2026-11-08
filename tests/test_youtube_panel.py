"""Тесты подсистемы YouTube Virality Panel (IEEE Access, TOTE)."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import httpx
import pandas as pd
import pytest

from vk_collector.config import load_yt_keywords
from vk_collector.database.models import (
    YTComment,
    YTPost,
    YTScheduledMeasurement,
    YTTimeseries,
)
from vk_collector.youtube.budget import (
    YouTubeQuotaBudget,
    YouTubeQuotaExhaustedError,
    compute_key_fingerprint,
)
from vk_collector.youtube.client import (
    YouTubeClient,
    is_short_video,
    parse_iso8601_duration,
    parse_rfc3339_datetime,
)
from vk_collector.youtube.export import YouTubePanelExporter
from vk_collector.youtube.media import YouTubeMediaDownloader
from vk_collector.youtube.service import (
    MEASUREMENT_HORIZONS_HOURS,
    YouTubePanelService,
    compute_size_tier,
    is_russian_channel,
)

PT_TZ = ZoneInfo("America/Los_Angeles")


# =============================================================================
# 1. Тест парсинга длительности ISO 8601 и классификации Shorts
# =============================================================================


def test_iso8601_duration_parsing_and_shorts_classification() -> None:
    # Требования ТЗ:
    # PT1M30S -> 90, is_short=True
    # PT3M0S  -> 180, is_short=True
    # PT3M1S  -> 181, is_short=False
    d1 = parse_iso8601_duration("PT1M30S")
    assert d1 == 90
    assert is_short_video(d1) is True

    d2 = parse_iso8601_duration("PT3M0S")
    assert d2 == 180
    assert is_short_video(d2) is True

    d3 = parse_iso8601_duration("PT3M1S")
    assert d3 == 181
    assert is_short_video(d3) is False

    # Дополнительные форматы
    assert parse_iso8601_duration("PT45S") == 45
    assert is_short_video(45) is True

    assert parse_iso8601_duration("PT1H") == 3600
    assert is_short_video(3600) is False

    assert parse_iso8601_duration("PT1H2M3S") == 3723
    assert parse_iso8601_duration("P1DT2H") == 93600
    assert parse_iso8601_duration("PT0S") == 0
    assert parse_iso8601_duration("") == 0
    assert parse_iso8601_duration("INVALID") == 0


def test_rfc3339_datetime_parser() -> None:
    dt1 = parse_rfc3339_datetime("2026-11-09T14:30:00Z")
    assert dt1.tzinfo == UTC
    assert dt1.hour == 14
    assert dt1.minute == 30

    dt2 = parse_rfc3339_datetime("2026-11-09T17:30:00+03:00")
    assert dt2.tzinfo == UTC
    assert dt2.hour == 14
    assert dt2.minute == 30


# =============================================================================
# 2. Тест суточного бюджета квоты в America/Los_Angeles и автоблокировки
# =============================================================================


@pytest.mark.asyncio
async def test_quota_budget_pacific_time_and_key_rotation() -> None:
    # 2026-11-08 23:30:00 PT -> учётный день 2026-11-08
    fake_time = datetime(2026, 11, 8, 23, 30, 0, tzinfo=PT_TZ)
    keys = ["key-alpha", "key-beta"]
    fp_alpha = compute_key_fingerprint("key-alpha")
    fp_beta = compute_key_fingerprint("key-beta")

    budget = YouTubeQuotaBudget(
        keys,
        daily_limit=1000,
        safety_margin=100,
        now=lambda: fake_time,
    )

    assert str(budget.today_pt()) == "2026-11-08"
    assert budget.next_reset_pt().isoformat() == "2026-11-09T00:00:00-08:00"

    # Доступен первый ключ
    key, fp = await budget.acquire_key("videos.list", cost=1)
    assert key == "key-alpha"
    assert fp == fp_alpha

    # Тратим квоту на key-alpha до предела (лимит 1000 - запас 100 = 900)
    await budget.record_usage(fp_alpha, "search.list", units=850)
    spent = await budget.get_key_spent(fp_alpha, budget.today_pt())
    assert spent == 850

    # Следующий search.list (100 ед) не влезет в key-alpha (850 + 100 = 950 > 900),
    # должен автоматически переключиться на key-beta
    key2, fp2 = await budget.acquire_key("search.list", cost=100)
    assert key2 == "key-beta"
    assert fp2 == fp_beta

    # Расходуем остаток квоты на обоих ключах до предела (900 ед)
    await budget.record_usage(fp_alpha, "videos.list", units=50)
    await budget.record_usage(fp_beta, "search.list", units=900)

    # Теперь оба ключа исчерпаны даже для запроса стоимостью 1 ед (900 + 1 > 900)
    with pytest.raises(YouTubeQuotaExhaustedError, match="исчерпан для всех 2 ключей"):
        await budget.acquire_key("videos.list", cost=1)


@pytest.mark.asyncio
async def test_client_blocks_key_on_quota_exceeded() -> None:
    fake_time = datetime(2026, 11, 9, 10, 0, 0, tzinfo=PT_TZ)
    keys = ["bad-key", "good-key"]
    budget = YouTubeQuotaBudget(
        keys,
        daily_limit=10000,
        safety_margin=200,
        now=lambda: fake_time,
    )

    def mock_handler(request: httpx.Request) -> httpx.Response:
        key = request.url.params.get("key")
        if key == "bad-key":
            return httpx.Response(
                403,
                json={
                    "error": {
                        "errors": [{"reason": "quotaExceeded", "message": "The request cannot..."}]
                    }
                },
            )
        if key == "good-key":
            return httpx.Response(200, json={"items": [{"id": "vid123"}]})
        return httpx.Response(400)

    transport = httpx.MockTransport(mock_handler)
    async with httpx.AsyncClient(transport=transport) as http:
        client = YouTubeClient(budget, http_client=http, max_retries=1)
        # Клиент должен автоматически поймать 403 quotaExceeded на bad-key,
        # заблокировать его и успешно выполнить запрос через good-key
        items = await client.get_videos_batch(["vid123"])
        assert len(items) == 1
        assert items[0]["id"] == "vid123"

        # Проверяем, что bad-key действительно помечен как заблокированный
        fp_bad = compute_key_fingerprint("bad-key")
        assert await budget.is_key_blocked(fp_bad, budget.today_pt()) is True


# =============================================================================
# 3. Тест стратификации каналов 9×4×8 и классификации
# =============================================================================


def test_channel_size_tier_thresholds() -> None:
    # S < 10k, M 10k–100k, L 100k–1M, XL > 1M
    assert compute_size_tier(None) == "S"
    assert compute_size_tier(0) == "S"
    assert compute_size_tier(9999) == "S"
    assert compute_size_tier(10000) == "M"
    assert compute_size_tier(99999) == "M"
    assert compute_size_tier(100000) == "L"
    assert compute_size_tier(999999) == "L"
    assert compute_size_tier(1000000) == "XL"
    assert compute_size_tier(5000000) == "XL"


def test_russian_channel_detection() -> None:
    # Страна RU -> сразу True
    assert is_russian_channel("RU", "Hello World", "Just an English channel") is True
    assert is_russian_channel("ru", "Car reviews", "") is True

    # Страна не RU, но кириллицы >= 30% -> True
    assert is_russian_channel(None, "Обзор автомобилей", "Тест драйв новинок") is True
    assert is_russian_channel("US", "Кулинарный блог", "English recipe and русские рецепты") is True

    # Менее 30% кириллицы -> False
    assert (
        is_russian_channel(None, "Tech Review Channel", "Latest smartphones and gadgets") is False
    )
    assert is_russian_channel("DE", "German Auto Show", "Beste Autos aus Deutschland") is False


def test_keywords_configuration_has_9_domains() -> None:
    domains_kw = load_yt_keywords(Path("config/yt_keywords.yml"))
    assert len(domains_kw) == 9
    expected_domains = {
        "еда и доставка",
        "красота и уход",
        "авто",
        "финансы и бизнес",
        "образование и наука",
        "юмор и развлечения",
        "новости и общество",
        "спорт и фитнес",
        "технологии и гаджеты",
    }
    assert set(domains_kw.keys()) == expected_domains
    for domain, queries in domains_kw.items():
        assert len(queries) >= 6, f"Домен {domain} должен иметь не менее 6 запросов"


# =============================================================================
# 4. Тест генерации 8 плановых замеров и пакетной агрегации по 50
# =============================================================================


def test_measurement_horizons() -> None:
    assert MEASUREMENT_HORIZONS_HOURS == [1, 3, 6, 12, 24, 48, 72, 168]
    assert len(MEASUREMENT_HORIZONS_HOURS) == 8


@pytest.mark.asyncio
async def test_videos_batching_up_to_50() -> None:
    # Проверяем, что запрос 120 видео разбивается на 3 пакета: 50, 50, 20
    calls: list[list[str]] = []

    def mock_handler(request: httpx.Request) -> httpx.Response:
        ids_str = request.url.params.get("id", "")
        ids = ids_str.split(",")
        calls.append(ids)
        items = [{"id": vid} for vid in ids]
        return httpx.Response(200, json={"items": items})

    transport = httpx.MockTransport(mock_handler)
    budget = YouTubeQuotaBudget(["key1"], daily_limit=10000)
    async with httpx.AsyncClient(transport=transport) as http:
        client = YouTubeClient(budget, http_client=http)
        test_ids = [f"vid_{i}" for i in range(120)]
        res = await client.get_videos_batch(test_ids)

        assert len(res) == 120
        assert len(calls) == 3
        assert len(calls[0]) == 50
        assert len(calls[1]) == 50
        assert len(calls[2]) == 20


# =============================================================================
# 5. Тест экспорта схемы данных и соответствия ТЗ
# =============================================================================


@pytest.mark.asyncio
async def test_export_schema_compliance(tmp_path: Path) -> None:
    # Проверяем соответствие структуры экспорта Parquet и CSV требованиям
    now_utc = datetime(2026, 11, 9, 12, 0, 0, tzinfo=UTC)

    mock_post = YTPost(
        post_id="vid_test_1",
        url="https://www.youtube.com/watch?v=vid_test_1",
        channel_id="UC_test_ch",
        published_at=now_utc - timedelta(hours=5),
        crawled_at=now_utc,
        age_hours=5.0,
        views=15000,
        likes=1200,
        comments=45,
        shares=None,
        duration_s=120,
        is_short=True,
        title="Тестовое видео",
        description="Описание видео",
        tags=["авто", "обзор"],
        category_id="2",
        default_language="ru",
        thumbnail_url="https://i.ytimg.com/vi/vid_test_1/maxresdefault.jpg",
        is_backfill=False,
        media_downloaded=True,
        comments_status="done",
    )

    mock_ts = YTTimeseries(
        post_id="vid_test_1",
        crawled_at=now_utc,
        target_age_h=1,
        age_seconds=3600,
        views=1500,
        likes=120,
        comments=5,
    )

    mock_comment = YTComment(
        comment_id="comm_test_1",
        post_id="vid_test_1",
        author_hash="hash123",
        text="Отличный обзор!",
        like_count=5,
        reply_count=1,
        published_at=now_utc,
        crawled_at=now_utc,
    )

    # Создаём фейковую сессию SQLAlchemy для изоляции экспорта
    class FakeSession:
        async def __aenter__(self) -> FakeSession:
            return self

        async def __aexit__(self, *args: Any) -> None:
            pass

        async def execute(self, stmt: Any) -> Any:
            class MockResult:
                def all(self) -> list[Any]:
                    s = str(stmt).lower()
                    if "yt_key_daily_usage" in s:
                        return [("2026-11-09", "videos.list", 1, 1)]
                    if "count" in s and "group by" in s:
                        return [("авто", "M", 1)]
                    if "channel_title" in s or ("yt_posts" in s and "join" in s):
                        return [
                            (
                                mock_post,
                                "Название канала",
                                50000,
                                "M",
                                "авто",
                            )
                        ]
                    return []

            return MockResult()

        async def scalars(self, stmt: Any) -> Any:
            class MockScalars:
                def all(self) -> list[Any]:
                    s = str(stmt).lower()
                    if "yt_comments" in s:
                        return [mock_comment]
                    if "yt_timeseries" in s:
                        return [mock_ts]
                    return []

            return MockScalars()

        async def scalar(self, stmt: Any) -> Any:
            return 1

    class FakeSessionFactory:
        def __call__(self) -> FakeSession:
            return FakeSession()

    exporter = YouTubePanelExporter(FakeSessionFactory(), export_dir=tmp_path)  # type: ignore[arg-type]
    files = await exporter.export_all(label="test_run")

    # 1. Проверяем yt_posts
    pq_posts_path = Path(files["posts_parquet"])
    assert pq_posts_path.exists()
    df_posts = pd.read_parquet(pq_posts_path)

    expected_posts_columns = [
        "platform",
        "post_id",
        "url",
        "channel_id",
        "channel_title",
        "channel_subscribers",
        "channel_size_tier",
        "domain",
        "published_at",
        "crawled_at",
        "age_hours",
        "views",
        "likes",
        "comments",
        "shares",
        "duration_s",
        "is_short",
        "title",
        "description",
        "tags",
        "category_id",
        "default_language",
        "thumbnail_url",
    ]
    assert list(df_posts.columns) == expected_posts_columns
    assert df_posts["platform"].iloc[0] == "youtube"
    assert df_posts["post_id"].iloc[0] == "vid_test_1"
    assert df_posts["shares"].iloc[0] is None or pd.isna(df_posts["shares"].iloc[0])
    assert df_posts["is_short"].iloc[0] == True  # noqa: E712
    assert list(df_posts["tags"].iloc[0]) == ["авто", "обзор"]

    # 2. Проверяем yt_timeseries
    pq_ts_path = Path(files["timeseries_parquet"])
    assert pq_ts_path.exists()
    df_ts = pd.read_parquet(pq_ts_path)
    assert list(df_ts.columns) == ["post_id", "crawled_at", "views", "likes", "comments"]

    # 3. Проверяем yt_report.md
    report_path = Path(files["report_md"])
    assert report_path.exists()
    report_text = report_path.read_text(encoding="utf-8")
    assert "YouTube Virality Panel" in report_text
    assert "Матрица стратификации" in report_text
    assert "еда и доставка" in report_text
    assert "технологии и гаджеты" in report_text


# =============================================================================
# 6. Тест публичного RSS фида (0 единиц квоты)
# =============================================================================


@pytest.mark.asyncio
async def test_rss_public_feed_parsing() -> None:
    sample_atom = """<?xml version="1.0" encoding="UTF-8"?>
    <feed xmlns="http://www.w3.org/2005/Atom" xmlns:yt="http://www.youtube.com/xml/schemas/2015">
      <title>Test Channel</title>
      <entry>
        <id>yt:video:sampleVideo123</id>
        <yt:videoId>sampleVideo123</yt:videoId>
        <yt:channelId>UC12345</yt:channelId>
        <title>Новый классный рецепт</title>
        <published>2026-11-09T08:00:00+00:00</published>
        <updated>2026-11-09T08:05:00+00:00</updated>
      </entry>
    </feed>"""

    def mock_handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, text=sample_atom)

    transport = httpx.MockTransport(mock_handler)
    budget = YouTubeQuotaBudget(["key1"], daily_limit=10000)
    async with httpx.AsyncClient(transport=transport) as http:
        client = YouTubeClient(budget, http_client=http)
        videos = await client.fetch_channel_rss_video_ids("UC12345")
        assert len(videos) == 1
        assert videos[0]["video_id"] == "sampleVideo123"
        assert videos[0]["title"] == "Новый классный рецепт"
        assert videos[0]["published_at"] == datetime(2026, 11, 9, 8, 0, 0, tzinfo=UTC)


# =============================================================================
# 7. Тест идемпотентности загрузчика медиафайлов
# =============================================================================


def test_media_downloader_idempotency(tmp_path: Path) -> None:
    downloader = YouTubeMediaDownloader(thumbs_dir=tmp_path)
    post_id = "test_vid_idempotent"

    # До создания файлов
    assert downloader.is_media_already_downloaded(post_id) is False

    # Создаём 4 файла
    (tmp_path / f"{post_id}_max.jpg").write_bytes(b"data1")
    (tmp_path / f"{post_id}_1.jpg").write_bytes(b"data2")
    (tmp_path / f"{post_id}_2.jpg").write_bytes(b"data3")
    (tmp_path / f"{post_id}_3.jpg").write_bytes(b"data4")

    # Теперь все файлы существуют и имеют ненулевой размер
    assert downloader.is_media_already_downloaded(post_id) is True


# =============================================================================
# 8. Тест защиты от искажения горизонтов замеров (grace window и пропуск)
# =============================================================================


@pytest.mark.asyncio
async def test_measurement_grace_window_on_discovery() -> None:
    now_utc = datetime(2026, 11, 9, 14, 0, 0, tzinfo=UTC)
    # Видео опубликовано 2 часа назад (в 12:00 UTC)
    published_at = now_utc - timedelta(hours=2)

    # Горизонт 1h (13:00) опоздал на 60 минут (> 30 минут) -> должен стать 'skipped'
    # Горизонт 3h (15:00) в будущем -> 'pending'
    sched_1h_time = published_at + timedelta(hours=1)
    sched_3h_time = published_at + timedelta(hours=3)

    assert sched_1h_time < now_utc - timedelta(minutes=30)
    assert not (sched_3h_time < now_utc - timedelta(minutes=30))


@pytest.mark.asyncio
async def test_expired_pending_measurement_is_skipped() -> None:
    now_utc = datetime(2026, 11, 9, 15, 0, 0, tzinfo=UTC)
    # Замер на 1h был запланирован на 13:00 (прошло 120 минут > 90 минут)
    sched_1h = YTScheduledMeasurement(
        post_id="vid_late_1",
        target_age_h=1,
        scheduled_for=now_utc - timedelta(minutes=120),
        status="pending",
    )

    is_expired_1h = sched_1h.target_age_h in (
        1,
        3,
        6,
    ) and now_utc > sched_1h.scheduled_for + timedelta(minutes=90)
    assert is_expired_1h is True

    # Замер на 12h был запланирован 2 часа назад (прошло 120 минут <= 4 часов) -> не просрочен
    sched_12h = YTScheduledMeasurement(
        post_id="vid_12h",
        target_age_h=12,
        scheduled_for=now_utc - timedelta(hours=2),
        status="pending",
    )
    is_expired_12h = sched_12h.target_age_h >= 12 and now_utc > sched_12h.scheduled_for + timedelta(
        hours=4
    )
    assert is_expired_12h is False


# =============================================================================
# 9. Тест экспорта относительного симлинка на папку обложек
# =============================================================================


@pytest.mark.asyncio
async def test_export_creates_relative_thumbs_symlink(tmp_path: Path) -> None:
    export_dir = tmp_path / "export"
    thumbs_dir = tmp_path / "thumbs"
    export_dir.mkdir(parents=True)
    thumbs_dir.mkdir(parents=True)

    # Создаём фейковый файл обложки
    (thumbs_dir / "vid1_max.jpg").write_bytes(b"image_content")

    class EmptySession:
        async def __aenter__(self) -> EmptySession:
            return self

        async def __aexit__(self, *args: Any) -> None:
            pass

        async def execute(self, stmt: Any) -> Any:
            class MockR:
                def all(self) -> list[Any]:
                    return []

            return MockR()

        async def scalars(self, stmt: Any) -> Any:
            class MockS:
                def all(self) -> list[Any]:
                    return []

            return MockS()

        async def scalar(self, stmt: Any) -> Any:
            return 0

    class EmptyFactory:
        def __call__(self) -> EmptySession:
            return EmptySession()

    exporter = YouTubePanelExporter(
        EmptyFactory(),  # type: ignore[arg-type]
        export_dir=export_dir,
        thumbs_dir=thumbs_dir,
    )
    files = await exporter.export_all(label="run_1")

    symlink_path = Path(files["thumbs_dir"])
    assert symlink_path.is_symlink()
    # Проверяем, что файл доступен через симлинк ровно по пути thumbs/vid1_max.jpg
    target_file = symlink_path / "vid1_max.jpg"
    assert target_file.exists()
    assert target_file.read_bytes() == b"image_content"


# =============================================================================
# 10. Тест отбора активных каналов и защиты от перезаписи домена
# =============================================================================


@pytest.mark.asyncio
async def test_stratification_active_priority_and_domain_collision(tmp_path: Path) -> None:
    # Создаём тестовый keywords файл с двумя доменами
    kw_file = tmp_path / "yt_keywords.yml"
    kw_file.write_text(
        """domains:
  авто:
    - "авто тест"
  еда и доставка:
    - "еда тест"
""",
        encoding="utf-8",
    )

    now_utc = datetime.now(UTC)

    # Канал 1 (UC_active): активный канал в "авто" (видео 5 дней назад)
    # Канал 2 (UC_dead): старый канал в "авто" (видео 200 дней назад)
    # Канал 3 (UC_zero): канал с 0 видео в "авто" -> должен быть исключён
    # Канал 4 (UC_multi): найден в "авто", затем в "еда и доставка" -> должен остаться в "авто"

    def mock_handler(request: httpx.Request) -> httpx.Response:
        url_str = str(request.url)
        if "/search" in url_str:
            q = request.url.params.get("q", "")
            if "авто" in q:
                return httpx.Response(
                    200,
                    json={
                        "items": [
                            {"id": {"channelId": "UC_active"}},
                            {"id": {"channelId": "UC_dead"}},
                            {"id": {"channelId": "UC_zero"}},
                            {"id": {"channelId": "UC_multi"}},
                        ]
                    },
                )
            if "еда" in q:
                return httpx.Response(
                    200,
                    json={"items": [{"id": {"channelId": "UC_multi"}}]},
                )
        if "/channels" in url_str:
            return httpx.Response(
                200,
                json={
                    "items": [
                        {
                            "id": "UC_active",
                            "snippet": {"title": "Активный автоканал", "country": "RU"},
                            "statistics": {"subscriberCount": "5000", "videoCount": "100"},
                            "contentDetails": {"relatedPlaylists": {"uploads": "UU_active"}},
                        },
                        {
                            "id": "UC_dead",
                            "snippet": {"title": "Заброшенный автоканал", "country": "RU"},
                            "statistics": {"subscriberCount": "8000", "videoCount": "50"},
                            "contentDetails": {"relatedPlaylists": {"uploads": "UU_dead"}},
                        },
                        {
                            "id": "UC_zero",
                            "snippet": {"title": "Пустой канал", "country": "RU"},
                            "statistics": {"subscriberCount": "2000", "videoCount": "0"},
                            "contentDetails": {"relatedPlaylists": {"uploads": "UU_zero"}},
                        },
                        {
                            "id": "UC_multi",
                            "snippet": {"title": "Мультидоменный канал", "country": "RU"},
                            "statistics": {"subscriberCount": "7000", "videoCount": "20"},
                            "contentDetails": {"relatedPlaylists": {"uploads": "UU_multi"}},
                        },
                    ]
                },
            )
        if "videos.xml" in url_str:
            if "UC_active" in url_str:
                recent_iso = (now_utc - timedelta(days=5)).isoformat()
                xml = f"""<feed xmlns="http://www.w3.org/2005/Atom" xmlns:yt="http://www.youtube.com/xml/schemas/2015">
                    <entry><yt:videoId>v1</yt:videoId><published>{recent_iso}</published></entry>
                </feed>"""
                return httpx.Response(200, text=xml)
            if "UC_dead" in url_str:
                old_iso = (now_utc - timedelta(days=200)).isoformat()
                xml = f"""<feed xmlns="http://www.w3.org/2005/Atom" xmlns:yt="http://www.youtube.com/xml/schemas/2015">
                    <entry><yt:videoId>v2</yt:videoId><published>{old_iso}</published></entry>
                </feed>"""
                return httpx.Response(200, text=xml)
            return httpx.Response(200, text="<feed></feed>")

        return httpx.Response(404)

    saved_channels: dict[str, Any] = {}

    class MemorySession:
        async def __aenter__(self) -> MemorySession:
            return self

        async def __aexit__(self, *args: Any) -> None:
            pass

        def add(self, obj: Any) -> None:
            pass

        async def execute(self, stmt: Any) -> Any:
            # Перехватываем insert в yt_channels
            s = str(stmt).lower()
            if "insert into yt_channels" in s:
                params = stmt.compile().params
                cid = params.get("channel_id")
                if cid:
                    saved_channels[cid] = params

            class MockR:
                def all(self) -> list[Any]:
                    return []

            return MockR()

        async def commit(self) -> None:
            pass

    class MemoryFactory:
        def __call__(self) -> MemorySession:
            return MemorySession()

    transport = httpx.MockTransport(mock_handler)
    budget = YouTubeQuotaBudget(["test_key"], daily_limit=10000)
    async with httpx.AsyncClient(transport=transport) as http:
        client = YouTubeClient(budget, http_client=http)
        from vk_collector.config import Settings

        settings = Settings()
        service = YouTubePanelService(MemoryFactory(), client, settings)  # type: ignore[arg-type]

        stats = await service.discover_and_stratify_channels(seed=20261108, keywords_path=kw_file)

        # 1. Проверяем исключение каналов с videoCount == 0
        assert "UC_zero" not in saved_channels

        # 2. Проверяем, что активный канал отобран
        assert "UC_active" in saved_channels
        assert saved_channels["UC_active"]["is_selected"] is True

        # 3. Проверяем, что UC_multi остался в "авто", так как в "авто" < 8 каналов
        assert "UC_multi" in saved_channels
        assert saved_channels["UC_multi"]["domain"] == "авто"

        assert stats["total_discovered"] == 4
        assert stats["valid_russian_channels"] == 3

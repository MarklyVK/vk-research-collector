"""Тесты контуров сбора YouTube: Media Downloader, Top Collection, Quota Guard и Reporting."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx
import pytest

from vk_collector.config import Settings
from vk_collector.youtube.budget import YouTubeQuotaBudget
from vk_collector.youtube.client import YouTubeClient
from vk_collector.youtube.media import YouTubeMediaDownloader
from vk_collector.youtube.report import count_disk_media
from vk_collector.youtube.top_service import (
    YouTubeTopService,
    extract_candidate_fields,
    load_candidates_file,
)


# =============================================================================
# 1. Тест загрузчика медиа: fallback для cover.jpg, кадры hq1-hq3 и пути Task 1 & 2
# =============================================================================
@pytest.mark.asyncio
async def test_media_downloader_task1_and_task2_paths(tmp_path: Path) -> None:
    base_dir = tmp_path / "youtube"

    def mock_handler(request: httpx.Request) -> httpx.Response:
        url_str = str(request.url)
        # maxresdefault -> 404, sddefault -> 200 (тестируем fallback цепочку)
        if "maxresdefault.jpg" in url_str:
            return httpx.Response(404)
        if "sddefault.jpg" in url_str:
            return httpx.Response(200, content=b"fake_sd_cover")
        if any(h in url_str for h in ("hq1.jpg", "hq2.jpg", "hq3.jpg")):
            return httpx.Response(200, content=b"fake_frame_data")
        return httpx.Response(404)

    transport = httpx.MockTransport(mock_handler)
    async with httpx.AsyncClient(transport=transport) as http:
        downloader = YouTubeMediaDownloader(base_dir=base_dir, http_client=http)

        # 1. Задача 1: путь base_dir/{video_id}/
        vid1 = "video_main_123"
        ok1 = await downloader.download_video_media(vid1, is_top=False)
        assert ok1 is True
        v1_dir = base_dir / vid1
        assert (v1_dir / "cover.jpg").read_bytes() == b"fake_sd_cover"
        assert (v1_dir / "hq1.jpg").read_bytes() == b"fake_frame_data"
        assert (v1_dir / "hq2.jpg").read_bytes() == b"fake_frame_data"
        assert (v1_dir / "hq3.jpg").read_bytes() == b"fake_frame_data"
        assert downloader.is_media_already_downloaded(vid1, is_top=False) is True

        # 2. Задача 2: путь base_dir/top/{video_id}/
        vid2 = "video_top_456"
        ok2 = await downloader.download_video_media(vid2, is_top=True)
        assert ok2 is True
        v2_dir = base_dir / "top" / vid2
        assert (v2_dir / "cover.jpg").read_bytes() == b"fake_sd_cover"
        assert (v2_dir / "hq1.jpg").read_bytes() == b"fake_frame_data"
        assert (v2_dir / "hq2.jpg").read_bytes() == b"fake_frame_data"
        assert (v2_dir / "hq3.jpg").read_bytes() == b"fake_frame_data"
        assert downloader.is_media_already_downloaded(vid2, is_top=True) is True

        # 3. Проверка подсчёта медиа на диске
        disk_stats = count_disk_media(base_dir)
        assert disk_stats["main_videos_with_media"] == 1
        assert disk_stats["main_files_count"] == 4
        assert disk_stats["top_videos_with_media"] == 1
        assert disk_stats["top_files_count"] == 4


# =============================================================================
# 2. Тест парсинга файлов кандидатов и строгой фильтрации новостей
# =============================================================================
def test_candidates_file_loading_and_news_filter(tmp_path: Path) -> None:
    csv_file = tmp_path / "candidates.csv"
    csv_file.write_text(
        "candidate_name,search_queries,flag_news,flag_infopovod\n"
        "Юрий Дудь,вдудь;интервью дудь,0,1\n"
        "Новости России,новости дня;срочные новости,1,0\n"
        "Wylsacom,wylsa;вилсаком обзор,false,0\n"
        "РИА Новости,риа новости,True,1\n",
        encoding="utf-8",
    )

    rows = load_candidates_file(csv_file)
    assert len(rows) == 4

    parsed = [extract_candidate_fields(r) for r in rows]

    # Дудь: новости=False, инфоповод=True
    assert parsed[0]["name"] == "Юрий Дудь"
    assert parsed[0]["search_queries"] == ["вдудь", "интервью дудь"]
    assert parsed[0]["flag_news"] is False
    assert parsed[0]["flag_infopovod"] is True

    # Новости России: новости=True (должен отфильтроваться)
    assert parsed[1]["flag_news"] is True

    # Wylsacom: новости=False
    assert parsed[2]["name"] == "Wylsacom"
    assert parsed[2]["flag_news"] is False

    # РИА Новости: новости=True (должен отфильтроваться)
    assert parsed[3]["flag_news"] is True


def test_candidates_json_loading(tmp_path: Path) -> None:
    json_file = tmp_path / "candidates.json"
    json_file.write_text(
        json.dumps(
            [
                {
                    "candidate": "AcademeG",
                    "queries": "академик;тест драйв",
                    "flag_news": 0,
                    "flag_infopovod": 0,
                },
                {
                    "candidate": "Вести FM",
                    "queries": "вести фм прямой эфир",
                    "flag_news": 1,
                    "flag_infopovod": 1,
                },
            ]
        ),
        encoding="utf-8",
    )

    rows = load_candidates_file(json_file)
    assert len(rows) == 2
    f1 = extract_candidate_fields(rows[0])
    assert f1["name"] == "AcademeG"
    assert f1["flag_news"] is False
    assert len(f1["search_queries"]) == 2

    f2 = extract_candidate_fields(rows[1])
    assert f2["name"] == "Вести FM"
    assert f2["flag_news"] is True


# =============================================================================
# 3. Тест логирования квоты в quota_usage.json
# =============================================================================
@pytest.mark.asyncio
async def test_quota_logging_to_json_file(tmp_path: Path) -> None:
    json_path = tmp_path / "quota_usage.json"
    budget = YouTubeQuotaBudget(
        ["AIzaFakeKeyMain123", "AIzaFakeKeyTop456"],
        daily_limit=10000,
        json_path=json_path,
    )

    fp_main = budget._fingerprints[0]
    fp_top = budget._fingerprints[1]

    # Списываем вызовы
    await budget.record_usage(fp_main, "videos.list", 5, calls=5)
    await budget.record_usage(fp_top, "search.list", 200, calls=2)

    assert json_path.exists()
    data = json.loads(json_path.read_text(encoding="utf-8"))

    assert "keys" in data
    assert fp_main in data["keys"]
    assert fp_top in data["keys"]
    assert data["keys"][fp_main]["total_spent"] == 5
    assert data["keys"][fp_top]["total_spent"] == 200
    assert data["keys"][fp_top]["endpoints"]["search.list"]["units"] == 200


# =============================================================================
# 4. Тест подрежимов YouTubeTopService (2a, 2b, 2c) на Mock API
# =============================================================================
@pytest.mark.asyncio
async def test_top_service_submodes(tmp_path: Path) -> None:
    base_dir = tmp_path / "youtube"
    saved_top: dict[tuple[str, str], dict[str, Any]] = {}

    class FakeSession:
        async def __aenter__(self) -> FakeSession:
            return self

        async def __aexit__(self, *args: Any) -> None:
            pass

        async def execute(self, stmt: Any) -> Any:
            class FakeResult:
                def all(self) -> list[Any]:
                    return []

            return FakeResult()

        async def scalar(self, stmt: Any) -> int:
            return 0

        async def commit(self) -> None:
            pass

    class FakeSessionFactory:
        def __call__(self) -> FakeSession:
            return FakeSession()

    def mock_yt_api(request: httpx.Request) -> httpx.Response:
        url_str = str(request.url)
        path = request.url.path

        # 1. mostPopular
        if path.endswith("/videos") and "chart=mostPopular" in str(request.url):
            return httpx.Response(
                200,
                json={
                    "items": [
                        {
                            "id": "pop_1",
                            "snippet": {
                                "title": "Популярное видео 1",
                                "description": "Описание 1",
                                "channelId": "UC_channel_1",
                                "channelTitle": "Канал 1",
                                "publishedAt": "2026-11-10T12:00:00Z",
                            },
                            "statistics": {
                                "viewCount": "500000",
                                "likeCount": "20000",
                                "commentCount": "1000",
                            },
                            "contentDetails": {"duration": "PT5M30S"},
                        }
                    ]
                },
            )

        # 2. search.list
        if path.endswith("/search"):
            return httpx.Response(
                200,
                json={
                    "items": [
                        {"id": {"videoId": "cand_vid_1"}},
                        {"id": {"videoId": "cand_vid_2"}},
                    ]
                },
            )

        # 3. videos.list batch
        if path.endswith("/videos"):
            return httpx.Response(
                200,
                json={
                    "items": [
                        {
                            "id": "cand_vid_1",
                            "snippet": {
                                "title": "Результат поиска 1",
                                "channelId": "UC_ch_2",
                                "publishedAt": "2026-05-01T10:00:00Z",
                            },
                            "statistics": {"viewCount": "1000000", "likeCount": "50000"},
                            "contentDetails": {"duration": "PT2M10S"},  # 130s -> Shorts!
                        }
                    ]
                },
            )

        # 4. channels.list batch
        if path.endswith("/channels"):
            return httpx.Response(
                200,
                json={
                    "items": [
                        {"id": "UC_channel_1", "statistics": {"subscriberCount": "150000"}},
                        {"id": "UC_ch_2", "statistics": {"subscriberCount": "2500000"}},
                    ]
                },
            )

        # 5. CDN
        if "i.ytimg.com" in url_str:
            return httpx.Response(200, content=b"fake_image")

        return httpx.Response(200, json={})

    transport = httpx.MockTransport(mock_yt_api)
    async with httpx.AsyncClient(transport=transport) as http:
        budget = YouTubeQuotaBudget(["top_key_123"], daily_limit=10000)
        client = YouTubeClient(budget, http_client=http)
        settings = Settings(yt_media_dir=base_dir)
        media_downloader = YouTubeMediaDownloader(base_dir=base_dir, http_client=http)

        service = YouTubeTopService(
            FakeSessionFactory(),  # type: ignore[arg-type]
            client,
            settings,
            media_downloader=media_downloader,
        )

        # Подменяем внутренний метод сохранения для проверки без живого Postgres
        async def fake_upsert(video_data: dict[str, Any], source: str, **kwargs: Any) -> bool:
            v_id = video_data.get("id")
            saved_top[(v_id, source)] = {"video": video_data, "source": source, "kwargs": kwargs}
            await media_downloader.download_video_media(v_id, is_top=True)
            return True

        service._upsert_and_download_video = fake_upsert  # type: ignore[assignment]

        # 1. Тест подрежима 2а: Daily Most Popular
        res_2a = await service.collect_daily_most_popular(now=datetime(2026, 11, 11, tzinfo=UTC))
        assert res_2a["source"] == "mostPopular_2026-11-11"
        assert res_2a["count"] == 1
        assert ("pop_1", "mostPopular_2026-11-11") in saved_top
        assert (base_dir / "top" / "pop_1" / "cover.jpg").exists()

        # 2. Тест подрежима 2б: Weekly Top (проверяем разбиение на 53 недели)
        weeks = service.get_weekly_intervals()
        assert len(weeks) == 53
        assert weeks[0][0] == 1
        assert weeks[-1][0] == 53

        # 3. Тест подрежима 2в: Candidates Top
        cand_file = tmp_path / "test_cand.csv"
        cand_file.write_text(
            "name,search_queries,flag_news,flag_infopovod\n"
            "Интервьюер,интервьюер выпуск;новости шоу,0,1\n"
            "Срочные Новости,новости дня,1,0\n",
            encoding="utf-8",
        )

        res_2c = await service.collect_candidates_top(cand_file)
        assert res_2c["candidates_processed"] == 1
        assert res_2c["candidates_skipped_news"] == 1  # "Срочные Новости" пропущены!
        assert ("cand_vid_1", "candidate_Интервьюер") in saved_top
        assert (base_dir / "top" / "cand_vid_1" / "cover.jpg").exists()

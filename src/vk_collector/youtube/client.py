"""Асинхронный клиент YouTube Data API v3 и RSS с контролем квот и retry."""

from __future__ import annotations

import asyncio
import logging
import random
import re
import xml.etree.ElementTree as ET
from collections.abc import Sequence
from datetime import UTC, datetime
from typing import Any

import httpx

from vk_collector.youtube.budget import (
    ENDPOINT_QUOTA_COSTS,
    YouTubeQuotaBudget,
    YouTubeQuotaExhaustedError,
)

logger = logging.getLogger(__name__)

ISO8601_DURATION_REGEX = re.compile(
    r"^P"
    r"(?:(?P<days>\d+)D)?"
    r"(?:T"
    r"(?:(?P<hours>\d+)H)?"
    r"(?:(?P<minutes>\d+)M)?"
    r"(?:(?P<seconds>\d+)S)?"
    r")?$"
)

ATOM_NS = {"atom": "http://www.w3.org/2005/Atom", "yt": "http://www.youtube.com/xml/schemas/2015"}


def parse_iso8601_duration(duration_str: str) -> int:
    """Распарсить длительность формата ISO 8601 (например PT1M30S) в целое число секунд.

    Примеры:
        PT1M30S -> 90
        PT3M0S  -> 180
        PT3M1S  -> 181
        PT45S   -> 45
        PT1H    -> 3600
        P1D     -> 86400
    """
    if not duration_str:
        return 0
    match = ISO8601_DURATION_REGEX.match(duration_str.strip())
    if not match:
        return 0
    days = int(match.group("days") or 0)
    hours = int(match.group("hours") or 0)
    minutes = int(match.group("minutes") or 0)
    seconds = int(match.group("seconds") or 0)
    return days * 86400 + hours * 3600 + minutes * 60 + seconds


def is_short_video(duration_s: int) -> bool:
    """Определить, является ли видео Shorts (длительность не более 180 секунд)."""
    return duration_s <= 180


def parse_rfc3339_datetime(val: str) -> datetime:
    """Распарсить строку времени RFC 3339 / ISO 8601 от YouTube в datetime с tzinfo=UTC."""
    clean = val.strip()
    if clean.endswith("Z"):
        clean = clean[:-1] + "+00:00"
    dt = datetime.fromisoformat(clean)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


class YouTubeClient:
    """Асинхронный клиент YouTube API v3 с ротацией ключей и автоматическим backoff."""

    def __init__(
        self,
        budget: YouTubeQuotaBudget,
        *,
        http_client: httpx.AsyncClient | None = None,
        base_url: str = "https://www.googleapis.com/youtube/v3",
        max_retries: int = 4,
        backoff_base: float = 1.0,
    ) -> None:
        self._budget = budget
        self._external_client = http_client is not None
        self._http = http_client or httpx.AsyncClient(http2=True, timeout=30.0)
        self._base_url = base_url.rstrip("/")
        self._max_retries = max_retries
        self._backoff_base = backoff_base

    async def aclose(self) -> None:
        if not self._external_client:
            await self._http.aclose()

    async def __aenter__(self) -> YouTubeClient:
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        await self.aclose()

    async def _request_with_quota(
        self,
        endpoint: str,
        path: str,
        params: dict[str, Any],
        cost: int | None = None,
    ) -> dict[str, Any]:
        """Выполнить запрос к YouTube Data API с выбором ключа, retry и списанием квоты."""
        actual_cost = cost if cost is not None else ENDPOINT_QUOTA_COSTS.get(endpoint, 1)
        url = f"{self._base_url}/{path.lstrip('/')}"

        for _ in range(max(1, self._budget.key_count)):
            api_key, fingerprint = await self._budget.acquire_key(endpoint, actual_cost)
            request_params = dict(params)
            request_params["key"] = api_key

            attempt = 0
            while attempt <= self._max_retries:
                try:
                    response = await self._http.get(url, params=request_params)
                    if response.status_code == 200:
                        await self._budget.record_usage(fingerprint, endpoint, actual_cost)
                        payload: dict[str, Any] = response.json()
                        return payload

                    if response.status_code == 403:
                        try:
                            err_data = response.json()
                            errors = err_data.get("error", {}).get("errors", [])
                            reasons = [e.get("reason") for e in errors]
                        except Exception:
                            reasons = []

                        if "quotaExceeded" in reasons or "dailyLimitExceeded" in reasons:
                            logger.warning(
                                "YouTube API вернул 403 quotaExceeded для ключа %s...",
                                fingerprint[:8],
                            )
                            await self._budget.mark_quota_exceeded(fingerprint)
                            # Прерываем внутренний retry цикл и переключаемся на следующий ключ
                            break

                        if "commentsDisabled" in reasons:
                            # Специфическая ошибка, возвращаем для обработки вызывающим
                            await self._budget.record_usage(fingerprint, endpoint, actual_cost)
                            response.raise_for_status()

                    # 5xx или 429: пробуем retry с backoff
                    if response.status_code in (429, 500, 502, 503, 504):
                        attempt += 1
                        if attempt <= self._max_retries:
                            delay = self._backoff_base * (2 ** (attempt - 1)) + random.uniform(
                                0.1, 0.5
                            )
                            logger.warning(
                                "HTTP %s от YouTube API, повтор %d/%d через %.2f с",
                                response.status_code,
                                attempt,
                                self._max_retries,
                                delay,
                            )
                            await asyncio.sleep(delay)
                            continue

                    response.raise_for_status()

                except httpx.TransportError as err:
                    attempt += 1
                    if attempt <= self._max_retries:
                        delay = self._backoff_base * (2 ** (attempt - 1)) + random.uniform(0.1, 0.5)
                        logger.warning(
                            "Сетевая ошибка %s при обращении к YouTube, повтор %d/%d через %.2f с",
                            err,
                            attempt,
                            self._max_retries,
                            delay,
                        )
                        await asyncio.sleep(delay)
                        continue
                    raise

        # Если все ключи перебраны и вернули quotaExceeded
        raise YouTubeQuotaExhaustedError("Все доступные ключи YouTube исчерпали квоту.")

    async def search_channels(self, query: str, max_results: int = 50) -> list[dict[str, Any]]:
        """Поиск каналов YouTube по ключевому запросу (search.list, стоимость 100 ед.)."""
        endpoint = "search.list"
        params = {
            "part": "snippet",
            "type": "channel",
            "regionCode": "RU",
            "relevanceLanguage": "ru",
            "q": query,
            "maxResults": min(max(1, max_results), 50),
        }
        data = await self._request_with_quota(endpoint, "search", params)
        items: list[dict[str, Any]] = data.get("items", [])
        return items

    async def get_channels_batch(self, channel_ids: Sequence[str]) -> list[dict[str, Any]]:
        """Пакетное получение метаданных каналов до 50 ID за вызов (channels.list, 1 ед.)."""
        if not channel_ids:
            return []
        endpoint = "channels.list"
        results: list[dict[str, Any]] = []

        for i in range(0, len(channel_ids), 50):
            chunk = channel_ids[i : i + 50]
            params = {
                "part": "snippet,statistics,contentDetails",
                "id": ",".join(chunk),
                "maxResults": 50,
            }
            data = await self._request_with_quota(endpoint, "channels", params)
            results.extend(data.get("items", []))

        return results

    async def get_playlist_items(
        self,
        playlist_id: str,
        published_after: datetime | None = None,
        max_pages: int = 20,
    ) -> list[dict[str, Any]]:
        """Обход элементов плейлиста загрузок канала (playlistItems.list, 1 ед. за страницу).

        Если задан published_after, обход прекращается, как только дата публикации
        видео становится старше порога (для эффективной выгрузки 90-дневного окна).
        """
        if not playlist_id:
            return []
        endpoint = "playlistItems.list"
        results: list[dict[str, Any]] = []
        page_token: str | None = None
        pages_fetched = 0

        while pages_fetched < max_pages:
            params: dict[str, Any] = {
                "part": "snippet,contentDetails",
                "playlistId": playlist_id,
                "maxResults": 50,
            }
            if page_token:
                params["pageToken"] = page_token

            data = await self._request_with_quota(endpoint, "playlistItems", params)
            items: list[dict[str, Any]] = data.get("items", [])
            if not items:
                break

            reached_older = False
            for item in items:
                snippet = item.get("snippet", {})
                pub_raw = snippet.get("publishedAt")
                if pub_raw and published_after is not None:
                    try:
                        pub_dt = parse_rfc3339_datetime(pub_raw)
                        if pub_dt < published_after:
                            reached_older = True
                            break
                    except Exception:
                        pass
                results.append(item)

            if reached_older:
                break

            page_token = data.get("nextPageToken")
            pages_fetched += 1
            if not page_token:
                break

        return results

    async def get_videos_batch(self, video_ids: Sequence[str]) -> list[dict[str, Any]]:
        """Пакетное получение деталей видео строго пачками до 50 ID (videos.list, 1 ед. за 50)."""
        if not video_ids:
            return []
        endpoint = "videos.list"
        results: list[dict[str, Any]] = []

        for i in range(0, len(video_ids), 50):
            chunk = video_ids[i : i + 50]
            params = {
                "part": "snippet,contentDetails,statistics",
                "id": ",".join(chunk),
                "maxResults": 50,
            }
            data = await self._request_with_quota(endpoint, "videos", params)
            results.extend(data.get("items", []))

        return results

    async def get_top_comments(
        self,
        video_id: str,
        max_results: int = 20,
    ) -> tuple[str, list[dict[str, Any]]]:
        """Получить верхние комментарии по релевантности (commentThreads.list, 1 ед.).

        Возвращает:
            tuple[status, items]: ("ok", [...]) или ("disabled", []) при отключенных комментариях.
        """
        endpoint = "commentThreads.list"
        params = {
            "part": "snippet",
            "videoId": video_id,
            "order": "relevance",
            "maxResults": min(max(1, max_results), 100),
            "textFormat": "plainText",
        }
        try:
            data = await self._request_with_quota(endpoint, "commentThreads", params)
            items: list[dict[str, Any]] = data.get("items", [])
            return "ok", items
        except httpx.HTTPStatusError as err:
            if err.response.status_code == 403:
                try:
                    err_json = err.response.json()
                    reasons = [e.get("reason") for e in err_json.get("error", {}).get("errors", [])]
                    if "commentsDisabled" in reasons:
                        logger.info("Комментарии отключены для видео %s", video_id)
                        return "disabled", []
                except Exception:
                    pass
            raise

    async def fetch_channel_rss_video_ids(self, channel_id: str) -> list[dict[str, Any]]:
        """Запросить публичный XML-фид канала (0 единиц квоты API!).

        Формат URL: https://www.youtube.com/feeds/videos.xml?channel_id={channel_id}
        Возвращает список словарей: [{"video_id": ..., "title": ..., "published_at": ...}].
        При недоступности или ошибке возвращает пустой список для фоллбэка.
        """
        feed_url = f"https://www.youtube.com/feeds/videos.xml?channel_id={channel_id}"
        try:
            response = await self._http.get(feed_url, timeout=15.0)
            if response.status_code != 200:
                logger.debug(
                    "RSS-фид для канала %s вернул статус %s", channel_id, response.status_code
                )
                return []
            root = ET.fromstring(response.text)
            videos: list[dict[str, Any]] = []
            for entry in root.findall("atom:entry", ATOM_NS):
                video_id_el = entry.find("yt:videoId", ATOM_NS)
                title_el = entry.find("atom:title", ATOM_NS)
                published_el = entry.find("atom:published", ATOM_NS)
                if video_id_el is not None and video_id_el.text:
                    v_id = video_id_el.text.strip()
                    title = (
                        title_el.text.strip() if (title_el is not None and title_el.text) else ""
                    )
                    pub_str = (
                        published_el.text.strip()
                        if (published_el is not None and published_el.text)
                        else ""
                    )
                    videos.append(
                        {
                            "video_id": v_id,
                            "title": title,
                            "published_at": parse_rfc3339_datetime(pub_str) if pub_str else None,
                        }
                    )
            return videos
        except Exception as err:
            logger.debug("Ошибка получения RSS-фида канала %s: %s", channel_id, err)
            return []

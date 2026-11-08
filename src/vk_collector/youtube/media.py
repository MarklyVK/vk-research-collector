"""Бережный загрузчик обложек и кадров YouTube в data/yt_thumbs/ с проверкой диска."""

from __future__ import annotations

import asyncio
import logging
import random
import shutil
from collections.abc import Sequence
from pathlib import Path
from typing import TYPE_CHECKING, Any

import httpx
from sqlalchemy import select, update

from vk_collector.database.models import YTPost

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

logger = logging.getLogger(__name__)

# Минимальный резерв свободного места на диске (2 ГиБ)
MIN_FREE_DISK_BYTES = 2 * 1024 * 1024 * 1024


class YouTubeMediaDownloader:
    """Загрузчик медиафайлов (обложка + 3 кадра) с контролем диска и rate limit."""

    def __init__(
        self,
        thumbs_dir: Path = Path("data/yt_thumbs"),
        *,
        http_client: httpx.AsyncClient | None = None,
        min_free_bytes: int = MIN_FREE_DISK_BYTES,
        sleep_range: tuple[float, float] = (1.0, 2.0),
    ) -> None:
        self._thumbs_dir = thumbs_dir
        self._external_client = http_client is not None
        self._http = http_client or httpx.AsyncClient(timeout=30.0, follow_redirects=True)
        self._min_free_bytes = min_free_bytes
        self._sleep_range = sleep_range
        self._semaphore = asyncio.Semaphore(1)

    async def aclose(self) -> None:
        if not self._external_client:
            await self._http.aclose()

    async def __aenter__(self) -> YouTubeMediaDownloader:
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        await self.aclose()

    def _ensure_disk_capacity(self) -> None:
        """Проверить наличие свободного места на диске (не менее 2 ГиБ резерва)."""
        target = self._thumbs_dir if self._thumbs_dir.exists() else Path.cwd()
        usage = shutil.disk_usage(target)
        if usage.free < self._min_free_bytes:
            raise RuntimeError(
                f"Недостаточно места на диске: свободно {usage.free / 1024**3:.2f} GiB, "
                f"требуется не менее {self._min_free_bytes / 1024**3:.2f} GiB"
            )

    def is_media_already_downloaded(self, post_id: str) -> bool:
        """Проверить наличие всех 4 файлов размера > 0 байт на диске."""
        files = [
            self._thumbs_dir / f"{post_id}_max.jpg",
            self._thumbs_dir / f"{post_id}_1.jpg",
            self._thumbs_dir / f"{post_id}_2.jpg",
            self._thumbs_dir / f"{post_id}_3.jpg",
        ]
        return all(f.exists() and f.stat().st_size > 0 for f in files)

    async def _download_one_file(self, urls: Sequence[str], destination: Path) -> bool:
        """Скачать файл, пробуя URL по цепочке (для fallback с maxresdefault на hqdefault)."""
        destination.parent.mkdir(parents=True, exist_ok=True)
        for url in urls:
            if not url:
                continue
            try:
                resp = await self._http.get(url)
                if resp.status_code == 200 and len(resp.content) > 0:
                    destination.write_bytes(resp.content)
                    return True
                if resp.status_code == 404:
                    continue
            except httpx.HTTPError as err:
                logger.debug("Ошибка загрузки %s: %s", url, err)
                continue
        return False

    async def download_post_media(self, post_id: str, thumbnail_url: str) -> bool:
        """Скачать максимальную обложку и 3 кадра для видео строго в 1 поток."""
        if self.is_media_already_downloaded(post_id):
            return True

        async with self._semaphore:
            self._ensure_disk_capacity()
            self._thumbs_dir.mkdir(parents=True, exist_ok=True)

            # 1. Максимальная обложка (_max.jpg): цепочка fallback
            max_urls: list[str] = [
                f"https://i.ytimg.com/vi/{post_id}/maxresdefault.jpg",
                thumbnail_url,
                f"https://i.ytimg.com/vi/{post_id}/hqdefault.jpg",
                f"https://i.ytimg.com/vi/{post_id}/mqdefault.jpg",
                f"https://i.ytimg.com/vi/{post_id}/default.jpg",
            ]
            max_file = self._thumbs_dir / f"{post_id}_max.jpg"
            if not (max_file.exists() and max_file.stat().st_size > 0):
                await self._download_one_file(max_urls, max_file)

            # 2. Кадры hq1, hq2, hq3
            for i in (1, 2, 3):
                frame_file = self._thumbs_dir / f"{post_id}_{i}.jpg"
                if not (frame_file.exists() and frame_file.stat().st_size > 0):
                    frame_urls = [
                        f"https://i.ytimg.com/vi/{post_id}/hq{i}.jpg",
                        f"https://i.ytimg.com/vi/{post_id}/{i}.jpg",
                    ]
                    await self._download_one_file(frame_urls, frame_file)

            # Пауза между видео по ТЗ
            if self._sleep_range[1] > 0:
                delay = random.uniform(self._sleep_range[0], self._sleep_range[1])
                await asyncio.sleep(delay)

            return self.is_media_already_downloaded(post_id)

    async def download_media_for_posts(
        self,
        sessions: async_sessionmaker[AsyncSession],
        *,
        limit: int | None = None,
    ) -> int:
        """Выбрать посты без медиа и скачать файлы порцией."""
        async with sessions() as session:
            stmt = (
                select(YTPost.post_id, YTPost.thumbnail_url)
                .where(YTPost.media_downloaded.is_(False))
                .order_by(YTPost.published_at.desc())
            )
            if limit is not None:
                stmt = stmt.limit(limit)
            rows = (await session.execute(stmt)).all()

        downloaded_count = 0
        for post_id, thumb_url in rows:
            try:
                ok = await self.download_post_media(post_id, thumb_url)
                if ok:
                    async with sessions() as session:
                        await session.execute(
                            update(YTPost)
                            .where(YTPost.post_id == post_id)
                            .values(media_downloaded=True)
                        )
                        await session.commit()
                    downloaded_count += 1
            except Exception as err:
                logger.warning("Не удалось скачать медиа для видео %s: %s", post_id, err)
                if "Недостаточно места на диске" in str(err):
                    break

        return downloaded_count

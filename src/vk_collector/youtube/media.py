"""Бережный загрузчик обложек и кадров YouTube в ~/markly/data/youtube/ с проверкой диска."""

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


class _RateLimiter:
    """Ограничитель частоты запросов к i.ytimg.com (по умолчанию не более 5-10 rps)."""

    def __init__(self, min_interval: float = 0.12) -> None:
        self._min_interval = min_interval
        self._last_time = 0.0
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        async with self._lock:
            loop = asyncio.get_running_loop()
            now = loop.time()
            elapsed = now - self._last_time
            if elapsed < self._min_interval:
                await asyncio.sleep(self._min_interval - elapsed)
            self._last_time = loop.time()


class YouTubeMediaDownloader:
    """Загрузчик медиафайлов (обложка + 3 кадра) с контролем диска и rate limit."""

    def __init__(
        self,
        base_dir: Path | str = Path("~/markly/data/youtube"),
        *,
        thumbs_dir: Path | str | None = None,
        http_client: httpx.AsyncClient | None = None,
        min_free_bytes: int = MIN_FREE_DISK_BYTES,
        sleep_range: tuple[float, float] = (0.05, 0.15),
    ) -> None:
        self._thumbs_dir: Path | None
        if thumbs_dir is not None and (
            base_dir == Path("~/markly/data/youtube") or base_dir == "~/markly/data/youtube"
        ):
            self._base_dir = Path(thumbs_dir).expanduser()
            self._thumbs_dir = self._base_dir
            self._legacy_mode = True
        else:
            self._base_dir = Path(base_dir).expanduser()
            self._thumbs_dir = Path(thumbs_dir).expanduser() if thumbs_dir is not None else None
            self._legacy_mode = self._thumbs_dir is not None

        self._external_client = http_client is not None
        self._http = http_client or httpx.AsyncClient(
            timeout=15.0,
            follow_redirects=True,
            limits=httpx.Limits(max_keepalive_connections=20, max_connections=30),
        )
        self._min_free_bytes = min_free_bytes
        self._sleep_range = sleep_range
        self._semaphore = asyncio.Semaphore(4)
        self._rate_limiter = _RateLimiter(min_interval=0.12)
        self._prefer_http: bool | None = None

    async def _resolve_scheme(self) -> str:
        if self._prefer_http is not None:
            return "http" if self._prefer_http else "https"
        if self._external_client:
            self._prefer_http = False
            return "https"
        try:
            resp = await self._http.get(
                "https://i.ytimg.com/vi/dQw4w9WgXcQ/hq1.jpg",
                timeout=2.5,
            )
            if resp.status_code == 200:
                self._prefer_http = False
                return "https"
        except Exception:
            pass
        logger.info("HTTPS к i.ytimg.com заблокирован/недоступен, переключаемся на HTTP")
        self._prefer_http = True
        return "http"

    @property
    def base_dir(self) -> Path:
        return self._base_dir

    async def aclose(self) -> None:
        if not self._external_client:
            await self._http.aclose()

    async def __aenter__(self) -> YouTubeMediaDownloader:
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        await self.aclose()

    def _ensure_disk_capacity(self) -> None:
        """Проверить наличие свободного места на диске (не менее 2 ГиБ резерва)."""
        target = self._base_dir if self._base_dir.exists() else Path.cwd()
        usage = shutil.disk_usage(target)
        if usage.free < self._min_free_bytes:
            raise RuntimeError(
                f"Недостаточно места на диске: свободно {usage.free / 1024**3:.2f} GiB, "
                f"требуется не менее {self._min_free_bytes / 1024**3:.2f} GiB"
            )

    def get_video_dir(self, video_id: str, is_top: bool = False) -> Path:
        """Получить путь к директории хранения изображений видео."""
        if is_top:
            return self._base_dir / "top" / video_id
        return self._base_dir / video_id

    def is_media_already_downloaded(self, post_id: str, is_top: bool = False) -> bool:
        """Проверить наличие всех 4 файлов размера > 0 байт на диске."""
        # 1. Структура: {base_dir}/[{top}/]{video_id}/{cover.jpg, hq1.jpg, hq2.jpg, hq3.jpg}
        v_dir = self.get_video_dir(post_id, is_top=is_top)
        std_files = [
            v_dir / "cover.jpg",
            v_dir / "hq1.jpg",
            v_dir / "hq2.jpg",
            v_dir / "hq3.jpg",
        ]
        if all(f.exists() and f.stat().st_size > 0 for f in std_files):
            return True

        # 2. Обратная совместимость с плоской структурой: {chk_dir}/{post_id}_max.jpg, etc.
        chk_dir = self._thumbs_dir or self._base_dir
        legacy_files = [
            chk_dir / f"{post_id}_max.jpg",
            chk_dir / f"{post_id}_1.jpg",
            chk_dir / f"{post_id}_2.jpg",
            chk_dir / f"{post_id}_3.jpg",
        ]
        return all(f.exists() and f.stat().st_size > 0 for f in legacy_files)

    async def _download_one_file(self, urls: Sequence[str], destination: Path) -> bool:
        """Скачать файл, пробуя URL по цепочке с вежливым rate limit."""
        destination.parent.mkdir(parents=True, exist_ok=True)
        for url in urls:
            if not url:
                continue
            await self._rate_limiter.acquire()
            try:
                resp = await self._http.get(url)
                if resp.status_code == 200 and len(resp.content) > 0:
                    destination.write_bytes(resp.content)
                    return True
                if resp.status_code == 404:
                    continue
            except (httpx.TimeoutException, httpx.NetworkError) as err:
                logger.debug("Таймаут/сеть при скачивании %s: %s", url, err)
                continue
            except httpx.HTTPError as err:
                logger.debug("Ошибка HTTP при загрузке %s: %s", url, err)
                continue
        return False

    async def download_video_media(
        self,
        video_id: str,
        *,
        is_top: bool = False,
        thumbnail_url: str = "",
    ) -> bool:
        """Скачать обложку (cover.jpg) и 3 кадра (hq1.jpg, hq2.jpg, hq3.jpg)."""
        if self.is_media_already_downloaded(video_id, is_top=is_top):
            return True

        async with self._semaphore:
            self._ensure_disk_capacity()
            scheme = await self._resolve_scheme()
            v_dir = self.get_video_dir(video_id, is_top=is_top)
            v_dir.mkdir(parents=True, exist_ok=True)

            # 1. Обложка (cover.jpg): цепочка fallback maxresdefault -> sddefault -> hqdefault
            cover_file = v_dir / "cover.jpg"
            if not (cover_file.exists() and cover_file.stat().st_size > 0):
                cover_urls: list[str] = [
                    f"{scheme}://i.ytimg.com/vi/{video_id}/maxresdefault.jpg",
                    f"{scheme}://i.ytimg.com/vi/{video_id}/sddefault.jpg",
                    f"{scheme}://i.ytimg.com/vi/{video_id}/hqdefault.jpg",
                ]
                if thumbnail_url:
                    cover_urls.append(thumbnail_url)
                await self._download_one_file(cover_urls, cover_file)

            # 2. Кадры hq1, hq2, hq3
            for i in (1, 2, 3):
                frame_file = v_dir / f"hq{i}.jpg"
                if not (frame_file.exists() and frame_file.stat().st_size > 0):
                    frame_urls = [
                        f"{scheme}://i.ytimg.com/vi/{video_id}/hq{i}.jpg",
                        f"{scheme}://i.ytimg.com/vi/{video_id}/{i}.jpg",
                    ]
                    await self._download_one_file(frame_urls, frame_file)

            # Поддержка legacy-файлов, если задан thumbs_dir
            if self._legacy_mode and self._thumbs_dir is not None:
                max_legacy = self._thumbs_dir / f"{video_id}_max.jpg"
                if cover_file.exists() and not max_legacy.exists():
                    shutil.copyfile(cover_file, max_legacy)
                for i in (1, 2, 3):
                    f_new = v_dir / f"hq{i}.jpg"
                    f_leg = self._thumbs_dir / f"{video_id}_{i}.jpg"
                    if f_new.exists() and not f_leg.exists():
                        shutil.copyfile(f_new, f_leg)

            if self._sleep_range[1] > 0:
                delay = random.uniform(self._sleep_range[0], self._sleep_range[1])
                await asyncio.sleep(delay)

            return self.is_media_already_downloaded(video_id, is_top=is_top)

    async def download_post_media(self, post_id: str, thumbnail_url: str = "") -> bool:
        """Совместимость с методом загрузки для поста основной выборки."""
        return await self.download_video_media(post_id, is_top=False, thumbnail_url=thumbnail_url)

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
                ok = await self.download_video_media(post_id, is_top=False, thumbnail_url=thumb_url)
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

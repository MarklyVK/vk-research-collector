"""Модуль разрешения, оптимизации и безопасной локализации медиаресурсов.

Реализует требования:
- Ограничение разрешения фото по разделу 3.5 (размеры 'y'/'x' до 768-807px вместо w/z).
- Ограничение видеопотока не выше 480p и обрезка первыми 60 секундами (ffmpeg -t 60, -threads 1).
- Безотказный Fallback для видео (разделы 3.6-3.7) на превью из first_frame/image.
- JIT-обновление протухших подписей URL через VK API (wall.getById / video.get).
- Общий для сервера шлагбаум CDN (cdn_gate): 1 поток, не чаще 1 файла в 20–30 с на весь сервер,
  первый 429 — стоп-флаг без повтора (снимает только человек), без шлагбаума скачивание запрещено.
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import os
import random
import shutil
import subprocess
from collections.abc import Coroutine
from pathlib import Path
from typing import Any, TypeVar

import httpx
from PIL import Image

from vk_collector.ml.cdn_gate import CdnGate, CdnGateError, CdnRateLimited
from vk_collector.ml.contracts import MultimodalPost, PostAttachmentItem

logger = logging.getLogger(__name__)

T = TypeVar("T")

# Ограничение числа потоков вычислений до 1 ядра
os.environ.setdefault("OMP_NUM_THREADS", "1")
os.environ.setdefault("OPENBLAS_NUM_THREADS", "1")
os.environ.setdefault("MKL_NUM_THREADS", "1")

DEFAULT_BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36"
    ),
    "Referer": "https://vk.com/",
    "Accept": "image/avif,image/webp,image/apng,image/svg+xml,image/*,video/*,*/*;q=0.8",
    "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
    "Connection": "keep-alive",
}


try:
    import h2  # noqa: F401

    HTTP2_SUPPORTED = True
except ImportError:
    HTTP2_SUPPORTED = False


def _run_sync(coro: Coroutine[Any, Any, T]) -> T:
    """Безопасный синхронный запуск корутины даже из контекста работающего event loop."""
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None

    if loop and loop.is_running():
        import concurrent.futures

        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            return executor.submit(asyncio.run, coro).result()
    else:
        return asyncio.run(coro)


class MediaDownloadHTTPError(Exception):
    """Ошибка загрузки медиафайла по HTTP."""

    def __init__(self, status_code: int, url: str) -> None:
        super().__init__(f"HTTP {status_code} при загрузке {url}")
        self.status_code = status_code
        self.url = url


class MediaResolver:
    """Управляет бережной загрузкой, JIT-обновлением URL, фолбэками и кэшированием медиафайлов."""

    def __init__(
        self,
        cache_dir: Path | None = None,
        *,
        vk_client: Any | None = None,
        max_concurrency: int = 1,
        gate: CdnGate | None = None,
        timeout: float = 30.0,
        max_video_duration_sec: int = 60,
        http_client: httpx.AsyncClient | None = None,
    ) -> None:
        self.cache_dir = cache_dir or Path("tmp/ml_cache")
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.vk_client = vk_client
        if max_concurrency != 1:
            logger.warning(
                "MediaResolver: max_concurrency=%s игнорируется — CDN качается в 1 поток",
                max_concurrency,
            )
        self.max_concurrency = 1
        self._gate = gate
        self.timeout = timeout
        self.max_video_duration_sec = max_video_duration_sec
        self._http_client = http_client
        self._semaphore: asyncio.Semaphore | None = None

    def _get_gate(self) -> CdnGate:
        if self._gate is None:
            self._gate = CdnGate.from_env()
        return self._gate

    def _get_semaphore(self) -> asyncio.Semaphore:
        if self._semaphore is None:
            self._semaphore = asyncio.Semaphore(self.max_concurrency)
        return self._semaphore

    def _get_http_client(self) -> httpx.AsyncClient:
        if self._http_client is None or self._http_client.is_closed:
            self._http_client = httpx.AsyncClient(
                http2=HTTP2_SUPPORTED,
                timeout=self.timeout,
                headers=DEFAULT_BROWSER_HEADERS,
                follow_redirects=True,
            )
        return self._http_client

    async def aclose(self) -> None:
        """Закрыть базовый HTTP-клиент."""
        if self._http_client is not None and not self._http_client.is_closed:
            await self._http_client.aclose()

    @staticmethod
    def select_photo_url(attachment: PostAttachmentItem, max_long_side: int = 768) -> str | None:
        """Выбрать URL фото не более 768-807px по длинной стороне (размеры 'y' или 'x').

        Исключает гигантские размеры 'w'/'z' (2560px) для защиты от блокировок CDN.
        """
        meta = attachment.attachment_metadata or {}
        raw_sizes = meta.get("sizes")

        if isinstance(raw_sizes, list) and raw_sizes:
            valid_sizes = [s for s in raw_sizes if isinstance(s, dict) and s.get("url")]
            if valid_sizes:
                # 1. Приоритет размеру 'y' (до 807px)
                for s in valid_sizes:
                    if s.get("type") == "y":
                        return str(s["url"])

                # 2. Приоритет размеру 'x' (до 604px)
                for s in valid_sizes:
                    if s.get("type") == "x":
                        return str(s["url"])

                # 3. Фильтрация размеров с шириной/высотой <= 807
                bounded = [
                    s
                    for s in valid_sizes
                    if max(int(s.get("width") or 0), int(s.get("height") or 0))
                    <= max(807, max_long_side)
                    and max(int(s.get("width") or 0), int(s.get("height") or 0)) > 0
                ]
                if bounded:
                    best = max(
                        bounded,
                        key=lambda s: max(int(s.get("width") or 0), int(s.get("height") or 0)),
                    )
                    return str(best["url"])

                # 4. Если все размеры крупнее, выбираем наименьший, исключая 'w'/'z'
                non_huge = [s for s in valid_sizes if s.get("type") not in {"w", "z"}]
                pool = non_huge if non_huge else valid_sizes
                best_small = min(
                    pool,
                    key=lambda s: max(int(s.get("width") or 99999), int(s.get("height") or 99999)),
                )
                return str(best_small["url"])

        return attachment.external_url

    @staticmethod
    def select_video_stream(attachment: PostAttachmentItem) -> tuple[str | None, str | None]:
        """Выбрать видеопоток не выше 480p и безопасный превью-фолбэк (first_frame/image).

        Возвращает кортеж (stream_url, preview_fallback_url).
        """
        meta = attachment.attachment_metadata or {}
        files = meta.get("files")
        stream_url: str | None = None

        if isinstance(files, dict) and files:
            # Приоритет качеству <= 480p
            for quality_key in ("mp4_480", "mp4_360", "mp4_240"):
                url = files.get(quality_key)
                if isinstance(url, str) and url.startswith("http"):
                    stream_url = url
                    break

            # Если <= 480p нет, проверяем остальные mp4
            if not stream_url:
                for k, v in files.items():
                    if isinstance(v, str) and v.startswith("http") and ("mp4" in k or ".mp4" in v):
                        stream_url = v
                        break

        if not stream_url and attachment.external_url and ".mp4" in attachment.external_url:
            stream_url = attachment.external_url

        # Поиск превью-фолбэка в first_frame или image
        preview_fallback_url: str | None = None
        for key in ("first_frame", "image"):
            frames = meta.get(key)
            if isinstance(frames, list) and frames:
                valid = [f for f in frames if isinstance(f, dict) and f.get("url")]
                if valid:
                    # Приоритет 'y' или 'x'
                    for f in valid:
                        if f.get("type") in {"y", "x"}:
                            preview_fallback_url = str(f["url"])
                            break
                    if not preview_fallback_url:
                        preview_fallback_url = str(valid[-1]["url"])
                    break
            elif isinstance(frames, str) and frames.startswith("http"):
                preview_fallback_url = frames
                break

        return stream_url, preview_fallback_url

    async def download_file_async(
        self,
        url: str,
        destination: Path,
        *,
        is_video: bool = False,
        max_duration_sec: int | None = None,
    ) -> Path:
        """Скачать один файл через общий шлагбаум CDN: одна попытка, 429 — стоп без повтора."""
        gate = self._get_gate()
        duration = max_duration_sec or self.max_video_duration_sec
        destination.parent.mkdir(parents=True, exist_ok=True)
        temp_dest = destination.with_suffix(
            f"{destination.suffix}.part_{random.randint(1000, 9999)}"
        )
        host = httpx.URL(url).host

        async with self._get_semaphore():
            await asyncio.to_thread(gate.acquire)
            status: int | None = None
            nbytes = 0
            try:
                # Видео при наличии ffmpeg: один поток, не длиннее duration секунд,
                # без второй попытки
                ffmpeg_bin = shutil.which("ffmpeg")
                if is_video and ffmpeg_bin:
                    cmd = [
                        ffmpeg_bin,
                        "-y",
                        "-threads",
                        "1",
                        "-i",
                        url,
                        "-t",
                        str(duration),
                        "-c",
                        "copy",
                        "-bsf:a",
                        "aac_adtstoasc",
                        str(temp_dest),
                    ]
                    try:
                        proc = await asyncio.to_thread(
                            subprocess.run,
                            cmd,
                            capture_output=True,
                            text=True,
                            timeout=self.timeout + 30.0,
                        )
                        if (
                            proc.returncode == 0
                            and temp_dest.exists()
                            and temp_dest.stat().st_size > 0
                        ):
                            nbytes = temp_dest.stat().st_size
                            temp_dest.replace(destination)
                            status = 200
                            return destination
                        if "429" in (proc.stderr or ""):
                            status = 429
                            raise CdnRateLimited(host)
                        raise MediaDownloadHTTPError(502, url)
                    finally:
                        temp_dest.unlink(missing_ok=True)

                client = self._get_http_client()
                try:
                    async with client.stream("GET", url) as response:
                        status = response.status_code
                        if status == 429:
                            raise CdnRateLimited(host)
                        if status in {403, 404, 410}:
                            raise MediaDownloadHTTPError(status, url)
                        response.raise_for_status()
                        with temp_dest.open("wb") as f:
                            async for chunk in response.aiter_bytes(chunk_size=65536):
                                f.write(chunk)
                    if temp_dest.exists() and temp_dest.stat().st_size > 0:
                        nbytes = temp_dest.stat().st_size
                        temp_dest.replace(destination)
                        return destination
                    raise MediaDownloadHTTPError(502, url)
                except (httpx.TimeoutException, httpx.NetworkError, httpx.HTTPStatusError) as exc:
                    raise MediaDownloadHTTPError(500, url) from exc
                finally:
                    temp_dest.unlink(missing_ok=True)
            finally:
                await asyncio.to_thread(gate.release, status, host=host, nbytes=nbytes)

    async def refresh_post_attachments(self, posts: list[MultimodalPost]) -> None:
        """Just-In-Time (JIT) обновление метаданных постов и видео через VK API."""
        if not self.vk_client or not posts:
            return

        # 1. Пакетное обновление постов через wall.getById (до 100 за запрос)
        wall_ids: list[str] = []
        post_map: dict[str, MultimodalPost] = {}
        for p in posts:
            owner_id = -abs(p.community_vk_id) if p.community_vk_id else p.group_id
            post_key = f"{owner_id}_{p.post_id}"
            wall_ids.append(post_key)
            post_map[post_key] = p

        chunk_size = 100
        for i in range(0, len(wall_ids), chunk_size):
            chunk_keys = wall_ids[i : i + chunk_size]
            try:
                raw_response = await self.vk_client.call(
                    "wall.getById",
                    {"posts": ",".join(chunk_keys), "extended": 1, "copy_history_depth": 1},
                )
                items = (
                    raw_response.get("items", [])
                    if isinstance(raw_response, dict)
                    else (raw_response if isinstance(raw_response, list) else [])
                )
                for item in items:
                    if not isinstance(item, dict):
                        continue
                    item_id = item.get("id")
                    item_owner = item.get("owner_id")
                    matched_key = f"{item_owner}_{item_id}"
                    matched_post = post_map.get(matched_key)
                    if not matched_post:
                        continue

                    # Обновляем метаданные вложений
                    new_attachments = item.get("attachments", [])
                    if isinstance(new_attachments, list):
                        for pos, att_raw in enumerate(new_attachments):
                            if pos < len(matched_post.attachments):
                                current_att = matched_post.attachments[pos]
                                att_type = att_raw.get("type")
                                payload = (
                                    att_raw.get(att_type, {}) if isinstance(att_raw, dict) else {}
                                )
                                if isinstance(payload, dict):
                                    current_att.attachment_metadata.update(payload)
            except Exception as e:
                logger.warning("Ошибка JIT обновления wall.getById: %s", e)

        # 2. Пакетное обновление видео через video.get
        video_ids: list[str] = []
        att_video_map: dict[str, PostAttachmentItem] = {}
        for p in posts:
            for att in p.attachments:
                if att.attachment_type == "video" and att.vk_owner_id and att.vk_attachment_id:
                    v_key = f"{att.vk_owner_id}_{att.vk_attachment_id}"
                    if att.access_key:
                        v_key += f"_{att.access_key}"
                    video_ids.append(v_key)
                    att_video_map[v_key] = att

        if video_ids:
            for i in range(0, len(video_ids), chunk_size):
                chunk_v = video_ids[i : i + chunk_size]
                try:
                    v_res = await self.vk_client.call("video.get", {"videos": ",".join(chunk_v)})
                    items = (
                        v_res.get("items", [])
                        if isinstance(v_res, dict)
                        else (v_res if isinstance(v_res, list) else [])
                    )
                    for v_item in items:
                        if not isinstance(v_item, dict):
                            continue
                        v_owner = v_item.get("owner_id")
                        v_id = v_item.get("id")
                        for vk, target_att in att_video_map.items():
                            if str(v_owner) in vk and str(v_id) in vk:
                                target_att.attachment_metadata.update(v_item)
                except Exception as e:
                    logger.warning("Ошибка JIT обновления video.get: %s", e)

    async def resolve_image_async(
        self,
        post: MultimodalPost,
        attachment: PostAttachmentItem,
    ) -> Path | None:
        """Асинхронно получить путь к изображению с JIT обновлением при 403/404/410."""
        if attachment.attachment_type != "photo":
            return None

        key = f"photo_{attachment.vk_owner_id}_{attachment.vk_attachment_id}_{attachment.position}"
        cached_file = self.cache_dir / f"{key}.jpg"
        if cached_file.exists() and cached_file.stat().st_size > 0:
            return cached_file

        url = self.select_photo_url(attachment)
        if not url:
            return None

        try:
            return await self.download_file_async(url, cached_file, is_video=False)
        except MediaDownloadHTTPError as exc:
            if exc.status_code in {403, 404, 410} and self.vk_client:
                # JIT-обновление ссылки через VK API и повторная попытка
                logger.info(
                    "JIT-обновление URL фото для поста %s после HTTP %s",
                    post.post_id,
                    exc.status_code,
                )
                await self.refresh_post_attachments([post])
                fresh_url = self.select_photo_url(attachment)
                if fresh_url and fresh_url != url:
                    try:
                        return await self.download_file_async(
                            fresh_url, cached_file, is_video=False
                        )
                    except CdnGateError:
                        raise
                    except Exception as retry_err:
                        logger.warning("Повторная загрузка фото не удалась: %s", retry_err)
            logger.warning("Не удалось загрузить изображение %s: %s", url, exc)
            return None
        except CdnGateError:
            raise
        except Exception as e:
            logger.warning("Ошибка при загрузке фото %s: %s", key, e)
            return None

    async def resolve_video_async(
        self,
        post: MultimodalPost,
        attachment: PostAttachmentItem,
    ) -> Path | None:
        """Асинхронно получить видеофайл с JIT-обновлением и безотказным фолбэком на превью."""
        if attachment.attachment_type != "video":
            return None

        key = f"video_{attachment.vk_owner_id}_{attachment.vk_attachment_id}_{attachment.position}"
        cached_file = self.cache_dir / f"{key}.mp4"
        fallback_file = self.cache_dir / f"{key}_preview.jpg"

        if cached_file.exists() and cached_file.stat().st_size > 0:
            return cached_file
        if fallback_file.exists() and fallback_file.stat().st_size > 0:
            return fallback_file

        stream_url, preview_url = self.select_video_stream(attachment)

        # 1. Попытка скачивания видеопотока
        if stream_url:
            try:
                return await self.download_file_async(stream_url, cached_file, is_video=True)
            except MediaDownloadHTTPError as exc:
                if exc.status_code in {403, 404, 410} and self.vk_client:
                    logger.info(
                        "JIT-обновление URL видео для поста %s после HTTP %s",
                        post.post_id,
                        exc.status_code,
                    )
                    await self.refresh_post_attachments([post])
                    stream_url, preview_url = self.select_video_stream(attachment)
                    if stream_url:
                        try:
                            return await self.download_file_async(
                                stream_url, cached_file, is_video=True
                            )
                        except CdnGateError:
                            raise
                        except Exception as retry_err:
                            logger.warning(
                                "Повторное скачивание видеопотока не удалось: %s", retry_err
                            )
            except CdnGateError:
                raise
            except Exception as e:
                logger.warning("Сбой скачивания видеопотока %s: %s", stream_url, e)

        # 2. Безотказный Fallback на превью из first_frame/image при блокировке или недоступности
        if preview_url:
            logger.info(
                "Применение Fallback на превью для видео %s (пост %s)",
                key,
                post.post_id,
            )
            try:
                res = await self.download_file_async(preview_url, fallback_file, is_video=False)
                attachment.attachment_metadata["video_fallback_used"] = True
                attachment.attachment_metadata["video_fallback_reason"] = (
                    "Stream blocked or unavailable"
                )
                attachment.attachment_metadata["video_fallback_source"] = preview_url
                return res
            except CdnGateError:
                raise
            except Exception as fallback_err:
                logger.warning("Fallback на превью видео не удался: %s", fallback_err)

        return None

    def resolve_image(self, post: MultimodalPost, attachment: PostAttachmentItem) -> Path | None:
        """Синхронная обертка для resolve_image_async (обратная совместимость)."""
        key = f"photo_{attachment.vk_owner_id}_{attachment.vk_attachment_id}_{attachment.position}"
        cached_file = self.cache_dir / f"{key}.jpg"
        if cached_file.exists() and cached_file.stat().st_size > 0:
            return cached_file

        return _run_sync(self.resolve_image_async(post, attachment))

    def resolve_video(self, post: MultimodalPost, attachment: PostAttachmentItem) -> Path | None:
        """Синхронная обертка для resolve_video_async (обратная совместимость)."""
        key = f"video_{attachment.vk_owner_id}_{attachment.vk_attachment_id}_{attachment.position}"
        cached_file = self.cache_dir / f"{key}.mp4"
        fallback_file = self.cache_dir / f"{key}_preview.jpg"
        if cached_file.exists() and cached_file.stat().st_size > 0:
            return cached_file
        if fallback_file.exists() and fallback_file.stat().st_size > 0:
            return fallback_file

        return _run_sync(self.resolve_video_async(post, attachment))

    def compute_file_hash(self, path: Path) -> str:
        """Вычислить SHA-256 хэш файла для контроля неизменности."""
        hasher = hashlib.sha256()
        with path.open("rb") as f:
            while chunk := f.read(65536):
                hasher.update(chunk)
        return hasher.hexdigest()

    def validate_image_file(self, path: Path) -> bool:
        """Проверить, что файл является валидным изображением."""
        try:
            with Image.open(path) as img:
                img.verify()
            return True
        except Exception:
            return False

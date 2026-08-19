"""Модуль разрешения и локализации медиаресурсов (изображений и видео)."""

from __future__ import annotations

import hashlib
from pathlib import Path

from PIL import Image

from vk_collector.ml.contracts import MultimodalPost, PostAttachmentItem


class MediaResolver:
    """Управляет локализацией, проверкой целостности и доступом к медиафайлам."""

    def __init__(self, cache_dir: Path | None = None) -> None:
        self.cache_dir = cache_dir or Path("tmp/ml_cache")
        self.cache_dir.mkdir(parents=True, exist_ok=True)

    def resolve_image(self, post: MultimodalPost, attachment: PostAttachmentItem) -> Path | None:
        """Получить локальный путь к изображению или проверить его доступность."""
        if attachment.attachment_type != "photo":
            return None

        # Проверка локального файла в кэше по уникальному ключу
        key = f"photo_{attachment.vk_owner_id}_{attachment.vk_attachment_id}_{attachment.position}"
        cached_file = self.cache_dir / f"{key}.jpg"
        if cached_file.exists() and cached_file.stat().st_size > 0:
            return cached_file

        return None

    def resolve_video(self, post: MultimodalPost, attachment: PostAttachmentItem) -> Path | None:
        """Получить локальный путь к видеофайлу."""
        if attachment.attachment_type != "video":
            return None

        key = f"video_{attachment.vk_owner_id}_{attachment.vk_attachment_id}_{attachment.position}"
        cached_file = self.cache_dir / f"{key}.mp4"
        if cached_file.exists() and cached_file.stat().st_size > 0:
            return cached_file

        return None

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

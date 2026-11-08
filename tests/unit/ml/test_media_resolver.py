"""Unit-тесты для MediaResolver: JIT-обновление, обход CDN-блокировок и фолбэки."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import AsyncMock

import numpy as np
import pytest
from PIL import Image

from vk_collector.ml.contracts import MultimodalPost, PostAttachmentItem
from vk_collector.ml.media_resolver import MediaResolver
from vk_collector.ml.video_processor import VideoProcessor


def test_select_photo_url_prefers_y_and_x() -> None:
    att_y = PostAttachmentItem(
        position=0,
        attachment_type="photo",
        attachment_metadata={
            "sizes": [
                {"type": "s", "url": "http://cdn/s.jpg", "width": 100, "height": 100},
                {"type": "x", "url": "http://cdn/x.jpg", "width": 604, "height": 604},
                {"type": "y", "url": "http://cdn/y.jpg", "width": 807, "height": 807},
                {"type": "w", "url": "http://cdn/w.jpg", "width": 2560, "height": 2560},
            ]
        },
    )
    # Предпочтение размеру 'y'
    assert MediaResolver.select_photo_url(att_y) == "http://cdn/y.jpg"

    att_x = PostAttachmentItem(
        position=0,
        attachment_type="photo",
        attachment_metadata={
            "sizes": [
                {"type": "s", "url": "http://cdn/s.jpg", "width": 100, "height": 100},
                {"type": "x", "url": "http://cdn/x.jpg", "width": 604, "height": 604},
                {"type": "w", "url": "http://cdn/w.jpg", "width": 2560, "height": 2560},
            ]
        },
    )
    # Если 'y' нет, берется 'x'
    assert MediaResolver.select_photo_url(att_x) == "http://cdn/x.jpg"


def test_select_video_stream_limits_to_480p() -> None:
    att = PostAttachmentItem(
        position=0,
        attachment_type="video",
        attachment_metadata={
            "files": {
                "mp4_1080": "http://video-cdn/1080.mp4",
                "mp4_720": "http://video-cdn/720.mp4",
                "mp4_480": "http://video-cdn/480.mp4",
                "mp4_360": "http://video-cdn/360.mp4",
            },
            "first_frame": [
                {"type": "s", "url": "http://photo-cdn/frame_s.jpg"},
                {"type": "y", "url": "http://photo-cdn/frame_y.jpg"},
            ],
        },
    )
    stream_url, preview_url = MediaResolver.select_video_stream(att)
    # Поток не выше 480p
    assert stream_url == "http://video-cdn/480.mp4"
    # Превью из first_frame на обычном фото-CDN
    assert preview_url == "http://photo-cdn/frame_y.jpg"


@pytest.mark.asyncio
async def test_video_fallback_on_stream_failure(tmp_path: Path) -> None:
    post = MultimodalPost(
        post_id=999,
        group_id=10,
        community_vk_id=10,
        subject="test",
        published_at="2026-08-01T00:00:00Z",
    )
    att = PostAttachmentItem(
        position=0,
        attachment_type="video",
        vk_owner_id=-10,
        vk_attachment_id=555,
        attachment_metadata={
            "files": {"mp4_480": "http://video-blocked-cdn/stream.mp4"},
            "first_frame": [{"type": "y", "url": "http://userapi.com/preview.jpg"}],
        },
    )

    resolver = MediaResolver(cache_dir=tmp_path)

    async def mock_download(
        url: str, destination: Path, *, is_video: bool = False, max_duration_sec: int | None = None
    ) -> Path:
        if is_video:
            raise RuntimeError("CDN 403 Forbidden")
        destination.write_bytes(b"fake_image_bytes")
        return destination

    resolver.download_file_async = AsyncMock(side_effect=mock_download)  # type: ignore[method-assign]

    resolved_path = await resolver.resolve_video_async(post, att)
    assert resolved_path is not None
    assert resolved_path.name.endswith("_preview.jpg")
    assert att.attachment_metadata.get("video_fallback_used") is True
    assert "video_fallback_reason" in att.attachment_metadata


@pytest.mark.asyncio
async def test_jit_refresh_calls_vk_api(tmp_path: Path) -> None:
    mock_vk = AsyncMock()
    mock_vk.call.return_value = {
        "items": [
            {
                "id": 1234,
                "owner_id": -50,
                "attachments": [
                    {
                        "type": "photo",
                        "photo": {
                            "sizes": [
                                {
                                    "type": "y",
                                    "url": "http://refreshed-cdn/photo_y.jpg",
                                    "width": 800,
                                    "height": 600,
                                }
                            ]
                        },
                    }
                ],
            }
        ]
    }

    resolver = MediaResolver(cache_dir=tmp_path, vk_client=mock_vk)
    post = MultimodalPost(
        post_id=1234,
        group_id=50,
        community_vk_id=50,
        subject="food",
        published_at="2026-08-01T00:00:00Z",
        attachments=[
            PostAttachmentItem(
                position=0,
                attachment_type="photo",
                vk_owner_id=-50,
                vk_attachment_id=1234,
                attachment_metadata={"sizes": [{"type": "y", "url": "http://expired-cdn/old.jpg"}]},
            )
        ],
    )

    await resolver.refresh_post_attachments([post])
    assert mock_vk.call.called
    assert resolver.select_photo_url(post.attachments[0]) == "http://refreshed-cdn/photo_y.jpg"


def test_video_processor_cleans_up_raw_file_in_finally(tmp_path: Path) -> None:
    raw_video = tmp_path / "raw_video.mp4"
    raw_video.write_bytes(b"dummy_raw_content")
    assert raw_video.exists()

    processor = VideoProcessor()
    # Обрабатываем с флагом cleanup_raw=True
    processor.process_video(raw_video, cleanup_raw=True)
    # Сырой файл гарантированно удален
    assert not raw_video.exists()


def test_video_processor_handles_fallback_image(tmp_path: Path) -> None:
    preview_img = tmp_path / "preview.jpg"
    img = Image.new("RGB", (600, 400), color="blue")
    img.save(preview_img)

    processor = VideoProcessor()
    frames = processor.process_video(preview_img)
    # Превью успешно обработано как 1 ключевой кадр
    assert len(frames) == 1
    assert isinstance(frames[0], np.ndarray)
    assert max(frames[0].shape[:2]) <= 768

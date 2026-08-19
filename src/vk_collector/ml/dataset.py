"""Датасет и обработка батчей для мультимодальных постов компаний."""

from __future__ import annotations

from dataclasses import dataclass, field

import numpy as np
from PIL import Image

from vk_collector.ml.contracts import ModalityProfile, MultimodalPost
from vk_collector.ml.media_resolver import MediaResolver
from vk_collector.ml.video_processor import FramePostprocessor, VideoProcessor


@dataclass
class MultimodalBatchItem:
    post_id: int
    group_id: int
    subject: str
    text: str
    modality_profile: ModalityProfile
    images: list[np.ndarray] = field(default_factory=list)
    video_frames: list[np.ndarray] = field(default_factory=list)


class CompanyPostsDataset:
    """Датасет постов компаний, загружающий текст, изображения и сжатые кадры видео."""

    def __init__(
        self,
        posts: list[MultimodalPost],
        media_resolver: MediaResolver | None = None,
        video_processor: VideoProcessor | None = None,
    ) -> None:
        self.posts = posts
        self.media_resolver = media_resolver or MediaResolver()
        self.video_processor = video_processor or VideoProcessor()

    def __len__(self) -> int:
        return len(self.posts)

    def __getitem__(self, index: int) -> MultimodalBatchItem:
        post = self.posts[index]
        images: list[np.ndarray] = []
        video_frames: list[np.ndarray] = []

        for att in post.attachments:
            if att.attachment_type == "photo":
                img_path = self.media_resolver.resolve_image(post, att)
                if img_path and img_path.exists():
                    try:
                        with Image.open(img_path) as img:
                            rgb_img = img.convert("RGB")
                            arr = FramePostprocessor.resize_frame(np.array(rgb_img))
                            images.append(arr)
                    except Exception:
                        pass
            elif att.attachment_type == "video":
                vid_path = self.media_resolver.resolve_video(post, att)
                if vid_path and vid_path.exists():
                    frames = self.video_processor.process_video(vid_path)
                    video_frames.extend(frames)

        return MultimodalBatchItem(
            post_id=post.post_id,
            group_id=post.group_id,
            subject=post.subject,
            text=post.text,
            modality_profile=post.modality_profile,
            images=images,
            video_frames=video_frames,
        )


def collate_multimodal_posts(batch: list[MultimodalBatchItem]) -> list[MultimodalBatchItem]:
    """Коллатор мультимодального батча для передачи в адаптер энкодера."""
    return batch

"""Unit-тесты для контрактов и конфигурации ML-модуля."""

from datetime import UTC, datetime

from vk_collector.ml.config import MLSettings
from vk_collector.ml.contracts import (
    ModalityProfile,
    MultimodalPost,
    PostAttachmentItem,
    SampleMode,
    VideoKeyFrame,
)


def test_sample_mode_targets() -> None:
    assert SampleMode.MICRO.target_size == 100
    assert SampleMode.DEV.target_size == 1_000
    assert SampleMode.SME.target_size == 10_000
    assert SampleMode.LARGE.target_size == 100_000
    assert SampleMode.FULL.target_size is None


def test_multimodal_post_contract() -> None:
    post = MultimodalPost(
        post_id=123,
        group_id=456,
        community_vk_id=456,
        subject="food_delivery",
        published_at=datetime.now(UTC),
        text="Свежие роллы со скидкой 20%",
        modality_profile=ModalityProfile.TEXT_IMAGE,
        attachments=[
            PostAttachmentItem(
                position=0,
                attachment_type="photo",
                width=1080,
                height=1080,
                external_url="https://example.com/photo.jpg",
            )
        ],
    )
    assert post.post_id == 123
    assert len(post.attachments) == 1
    assert post.modality_profile == ModalityProfile.TEXT_IMAGE


def test_video_key_frame_contract() -> None:
    frame = VideoKeyFrame(
        frame_index=15,
        timestamp_sec=0.5,
        mad_score=0.22,
        width=448,
        height=448,
    )
    assert frame.frame_index == 15
    assert frame.mad_score == 0.22


def test_ml_settings_defaults() -> None:
    settings = MLSettings()
    assert settings.allocated_gpu_vram_gb == 20.0
    assert settings.embedding_dim == 2048
    assert settings.max_posts_per_group == 100
    assert settings.window_days == 180

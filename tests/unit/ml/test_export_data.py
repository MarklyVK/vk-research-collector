"""Unit-тесты для модуля экспорта данных."""

from datetime import UTC, datetime
from pathlib import Path

from vk_collector.ml.contracts import (
    ModalityProfile,
    MultimodalPost,
    PostAttachmentItem,
)
from vk_collector.ml.export_data import (
    determine_modality_profile,
    export_posts_to_jsonl,
    load_posts_from_jsonl,
)


def test_determine_modality_profile() -> None:
    assert determine_modality_profile(True, False, False) == ModalityProfile.TEXT_ONLY
    assert determine_modality_profile(True, True, False) == ModalityProfile.TEXT_IMAGE
    assert determine_modality_profile(True, False, True) == ModalityProfile.TEXT_VIDEO
    assert determine_modality_profile(True, True, True) == ModalityProfile.TRIMODAL
    assert determine_modality_profile(False, True, False) == ModalityProfile.IMAGE_ONLY
    assert determine_modality_profile(False, False, True) == ModalityProfile.VIDEO_ONLY
    assert determine_modality_profile(False, False, False) == ModalityProfile.EMPTY


def test_export_and_load_posts_jsonl(tmp_path: Path) -> None:
    posts = [
        MultimodalPost(
            post_id=1,
            group_id=10,
            community_vk_id=10,
            subject="food_delivery",
            published_at=datetime.now(UTC),
            text="Скидка на пиццу!",
            modality_profile=ModalityProfile.TEXT_IMAGE,
            attachments=[
                PostAttachmentItem(
                    position=0,
                    attachment_type="photo",
                    width=800,
                    height=600,
                    external_url="https://example.com/pizza.jpg",
                )
            ],
        ),
        MultimodalPost(
            post_id=2,
            group_id=20,
            community_vk_id=20,
            subject="tender_support",
            published_at=datetime.now(UTC),
            text="Обзор 44-ФЗ",
            modality_profile=ModalityProfile.TEXT_ONLY,
            attachments=[],
        ),
    ]

    dest = tmp_path / "test_posts.jsonl"
    export_posts_to_jsonl(posts, dest)

    assert dest.exists()
    loaded = load_posts_from_jsonl(dest)
    assert len(loaded) == 2
    assert loaded[0].post_id == 1
    assert loaded[0].modality_profile == ModalityProfile.TEXT_IMAGE
    assert loaded[1].post_id == 2
    assert loaded[1].text == "Обзор 44-ФЗ"

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

    # Проверка лимитов и смещения
    limited = load_posts_from_jsonl(dest, limit=1)
    assert len(limited) == 1
    assert limited[0].post_id == 1

    offset_loaded = load_posts_from_jsonl(dest, offset=1)
    assert len(offset_loaded) == 1
    assert offset_loaded[0].post_id == 2


def test_append_and_checkpoint_posts_jsonl(tmp_path: Path) -> None:
    from vk_collector.ml.export_data import append_posts_to_jsonl, get_jsonl_checkpoint

    dest = tmp_path / "stream_posts.jsonl"
    total, max_gid, existing_pids = get_jsonl_checkpoint(dest)
    assert total == 0
    assert max_gid == 0
    assert existing_pids == set()

    posts_batch_1 = [
        MultimodalPost(
            post_id=101,
            group_id=5,
            community_vk_id=5,
            subject="food_delivery",
            published_at=datetime.now(UTC),
            text="Первый пост",
            modality_profile=ModalityProfile.TEXT_ONLY,
        )
    ]
    append_posts_to_jsonl(posts_batch_1, dest)
    total, max_gid, existing_pids = get_jsonl_checkpoint(dest)
    assert total == 1
    assert max_gid == 5
    assert 101 in existing_pids

    posts_batch_2 = [
        MultimodalPost(
            post_id=102,
            group_id=12,
            community_vk_id=12,
            subject="food_service",
            published_at=datetime.now(UTC),
            text="Второй пост",
            modality_profile=ModalityProfile.TEXT_ONLY,
        )
    ]
    append_posts_to_jsonl(posts_batch_2, dest)
    total, max_gid, existing_pids = get_jsonl_checkpoint(dest)
    assert total == 2
    assert max_gid == 12
    assert existing_pids == {101, 102}

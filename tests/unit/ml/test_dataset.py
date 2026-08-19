"""Unit-тесты для модуля dataset.py."""

from datetime import UTC, datetime

from vk_collector.ml.contracts import (
    ModalityProfile,
    MultimodalPost,
    PostAttachmentItem,
)
from vk_collector.ml.dataset import CompanyPostsDataset, collate_multimodal_posts


def test_company_posts_dataset_loading() -> None:
    posts = [
        MultimodalPost(
            post_id=1,
            group_id=10,
            community_vk_id=10,
            subject="food_service",
            published_at=datetime.now(UTC),
            text="Свежее меню недели",
            modality_profile=ModalityProfile.TEXT_ONLY,
            attachments=[],
        ),
        MultimodalPost(
            post_id=2,
            group_id=20,
            community_vk_id=20,
            subject="customer_acquisition",
            published_at=datetime.now(UTC),
            text="Кейс роста конверсии",
            modality_profile=ModalityProfile.TEXT_IMAGE,
            attachments=[
                PostAttachmentItem(
                    position=0,
                    attachment_type="photo",
                    width=600,
                    height=400,
                )
            ],
        ),
    ]

    dataset = CompanyPostsDataset(posts)
    assert len(dataset) == 2

    item0 = dataset[0]
    assert item0.post_id == 1
    assert item0.text == "Свежее меню недели"
    assert len(item0.images) == 0
    assert len(item0.video_frames) == 0

    item1 = dataset[1]
    assert item1.post_id == 2


def test_collate_multimodal_posts() -> None:
    posts = [
        MultimodalPost(
            post_id=100,
            group_id=1,
            community_vk_id=1,
            subject="food_delivery",
            published_at=datetime.now(UTC),
            text="Пост 100",
            modality_profile=ModalityProfile.TEXT_ONLY,
            attachments=[],
        )
    ]
    dataset = CompanyPostsDataset(posts)
    batch = [dataset[0]]
    collated = collate_multimodal_posts(batch)
    assert len(collated) == 1
    assert collated[0].post_id == 100

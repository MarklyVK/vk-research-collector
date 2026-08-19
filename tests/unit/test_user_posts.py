from datetime import UTC, datetime
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from vk_collector.collection.queue import CollectionQueue
from vk_collector.collection.worker import JOB_METHODS
from vk_collector.config import Settings
from vk_collector.ml.contracts import (
    ModalityProfile,
    MultimodalUserPost,
    MultimodalUserProfile,
    PostAttachmentItem,
    UserDemographicsItem,
    UserSubscriptionItem,
)
from vk_collector.ml.export_data import (
    export_user_profiles_to_jsonl,
    load_user_profiles_from_jsonl,
)
from vk_collector.vk import VKClient


def test_user_posts_settings_defaults() -> None:
    settings = Settings()
    assert settings.collection_user_posts_enabled is True
    assert settings.collection_user_posts_max_per_user == 20
    assert settings.collection_user_posts_page_size == 20
    assert settings.collection_user_posts_window_days == 180
    assert settings.collection_user_posts_ttl_days == 30


@pytest.mark.asyncio
async def test_vk_client_get_user_wall_page() -> None:
    import asyncio
    import time

    from vk_collector.vk import TokenPool

    pool = TokenPool(["vk_token_1"], rps=10, clock=time.monotonic, sleep=asyncio.sleep)
    client = VKClient(pool)
    client.call = AsyncMock(return_value={"count": 2, "items": [{"id": 101, "text": "Hello"}]})  # type: ignore[method-assign]

    response = await client.get_user_wall_page(user_vk_id=12345, offset=0, count=20)
    assert response["count"] == 2
    assert len(response["items"]) == 1
    client.call.assert_called_once_with("wall.get", {"owner_id": 12345, "offset": 0, "count": 20})


@pytest.mark.asyncio
async def test_worker_job_methods_contains_user_posts() -> None:
    assert "collect_user_posts" in JOB_METHODS
    assert JOB_METHODS["collect_user_posts"] == "wall.get"


@pytest.mark.asyncio
async def test_queue_enabled_scopes_and_config() -> None:
    settings = Settings(collection_user_posts_enabled=True, collection_user_posts_max_per_user=20)
    sessions = MagicMock()
    queue = CollectionQueue(sessions, settings)

    assert "user_posts" in queue.enabled_scopes()
    config = queue.collection_configuration()
    assert config["user_posts_enabled"] is True
    assert config["user_posts_max_per_user"] == 20
    assert config["user_posts_window_days"] == 180


@pytest.mark.asyncio
async def test_demographics_text_representation() -> None:
    demo = UserDemographicsItem(
        sex=2,
        city="Москва",
        education="МГУ",
        relation=4,
        followers_count=150,
        friends_count=200,
        gifts_count=10,
        bdate="15.5.1990",
    )
    text_rep, count = demo.build_text_representation()
    assert count == 8
    assert "пол: мужской" in text_rep
    assert "город: Москва" in text_rep
    assert "образование: МГУ" in text_rep
    assert "подписчики: 150" in text_rep


def test_user_profile_jsonl_export_import(tmp_path: Any) -> None:
    now = datetime(2026, 8, 1, 12, 0, tzinfo=UTC)
    profile = MultimodalUserProfile(
        user_id=999,
        posts=[
            MultimodalUserPost(
                post_id=1,
                user_id=999,
                published_at=now,
                text="Тестовый пост пользователя",
                modality_profile=ModalityProfile.TEXT_IMAGE,
                attachments=[
                    PostAttachmentItem(
                        position=0,
                        attachment_type="photo",
                        vk_attachment_id=123,
                    )
                ],
                likes_count=10,
            )
        ],
        subscriptions=[
            UserSubscriptionItem(
                community_vk_id=456,
                name="Сообщество 1",
                description="Описание сообщества",
                members_count=1000,
            )
        ],
        demographics=UserDemographicsItem(
            sex=1,
            city="Санкт-Петербург",
            education="СПбГУ",
        ),
    )

    dest = tmp_path / "user_profiles.jsonl"
    export_user_profiles_to_jsonl([profile], dest)
    assert dest.exists()

    loaded = load_user_profiles_from_jsonl(dest)
    assert len(loaded) == 1
    assert loaded[0].user_id == 999
    assert len(loaded[0].posts) == 1
    assert loaded[0].posts[0].text == "Тестовый пост пользователя"
    assert loaded[0].demographics.city == "Санкт-Петербург"
    assert len(loaded[0].subscriptions) == 1

from datetime import UTC, datetime
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from vk_collector.collection.queue import CollectionQueue
from vk_collector.collection.safety import DiskState
from vk_collector.collection.user_posts_campaigns import build_user_posts_capacity_projection
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
    assert settings.collection_user_posts_pilot_users == 500


def test_user_posts_hard_limits_cannot_be_expanded() -> None:
    with pytest.raises(ValueError):
        Settings(collection_user_posts_max_per_user=21)
    with pytest.raises(ValueError):
        Settings(collection_user_posts_window_days=181)
    with pytest.raises(ValueError):
        Settings(collection_user_posts_pilot_users=501)


def test_user_posts_capacity_projection_is_aggregate_and_reserved() -> None:
    result = build_user_posts_capacity_projection(
        preview={
            "snapshot_users": 1_000,
            "fresh_users": 100,
            "terminal_users": 10,
            "due_users": 900,
            "planning_configuration_hash": "a" * 64,
        },
        pilot={
            "measured_users": 100,
            "database_growth_bytes": 2_000_000,
            "user_posts": 1_000,
            "attachments": 200,
        },
        database_bytes=1024**3,
        disk=DiskState(
            used_percent=10.0,
            warning=False,
            stop=False,
            total_bytes=20 * 1024**3,
            free_bytes=18 * 1024**3,
        ),
        warning_percent=85,
    )
    assert result["decision"] == "passed"
    assert result["reserve_factor"] == 1.30
    assert result["aggregate_projected_growth_bytes"] > 900 * 20_000
    assert result["aggregate_projected_growth_bytes"] == (
        result["payload_projected_growth_bytes"] + result["snapshot_projected_growth_bytes"]
    )


def test_user_posts_capacity_projection_rejects_full_snapshot() -> None:
    result = build_user_posts_capacity_projection(
        preview={"snapshot_users": 100_000, "due_users": 100_000},
        pilot={
            "measured_users": 100,
            "database_growth_bytes": 50_000_000,
            "user_posts": 2_000,
            "attachments": 1_000,
        },
        database_bytes=7 * 1024**3 - 1,
        disk=DiskState(
            used_percent=70.0,
            warning=False,
            stop=False,
            total_bytes=20 * 1024**3,
            free_bytes=4 * 1024**3,
        ),
        warning_percent=85,
    )
    assert result["decision"] == "rejected"
    assert result["additional_disk_required_bytes"] > 0


def test_user_posts_capacity_reserves_absolute_free_space() -> None:
    result = build_user_posts_capacity_projection(
        preview={"snapshot_users": 1_000, "due_users": 1_000},
        pilot={
            "measured_users": 100,
            "database_growth_bytes": 50_000_000,
            "user_posts": 1_000,
            "attachments": 100,
        },
        database_bytes=2 * 1024**3,
        disk=DiskState(70.0, False, False, 10 * 1024**3, 3 * 1024**3),
        warning_percent=95,
        safe_database_limit_bytes=8 * 1024**3,
        min_free_bytes=2 * 1024**3,
    )
    assert result["minimum_disk_free_bytes"] == 2 * 1024**3
    assert result["available_growth_bytes"] == 1024**3
    assert result["decision"] == "passed"


def test_bounded_snapshot_settings_accept_explicit_owner_limits() -> None:
    settings = Settings(
        collection_subscription_snapshot_user_limit=150_000,
        collection_user_posts_snapshot_user_limit=250_000,
        collection_disk_min_free_bytes=2 * 1024**3,
        collection_safe_database_limit_bytes=12 * 1024**3,
    )
    assert settings.collection_subscription_snapshot_user_limit == 150_000
    assert settings.collection_user_posts_snapshot_user_limit == 250_000
    assert settings.collection_disk_min_free_bytes == 2 * 1024**3


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

"""Безопасное автоматическое восстановление уже разрешённых кампаний."""

from __future__ import annotations

import uuid

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from vk_collector.collection.campaigns import CampaignManager
from vk_collector.collection.user_posts_campaigns import (
    CAMPAIGN_TYPE as USER_POSTS_CAMPAIGN_TYPE,
)
from vk_collector.collection.user_posts_campaigns import UserPostCampaignManager
from vk_collector.config import Settings
from vk_collector.database.models import CampaignStatus, CollectionCampaign

SUBSCRIPTION_CAMPAIGN_TYPE = "subscription_enrichment"


async def reconcile_capacity_paused_campaigns(
    sessions: async_sessionmaker[AsyncSession], settings: Settings
) -> list[uuid.UUID]:
    """Повторить live gate и продолжить только уже разрешённые bounded snapshots."""
    async with sessions() as session:
        ordered_rows = (
            await session.execute(
                select(
                    CollectionCampaign.id,
                    CollectionCampaign.campaign_type,
                    CollectionCampaign.status,
                )
                .where(
                    CollectionCampaign.campaign_type.in_(
                        [SUBSCRIPTION_CAMPAIGN_TYPE, USER_POSTS_CAMPAIGN_TYPE]
                    ),
                )
                .order_by(
                    CollectionCampaign.campaign_type,
                    CollectionCampaign.created_at.desc(),
                    CollectionCampaign.id.desc(),
                )
            )
        ).all()
    latest_by_type: dict[str, tuple[uuid.UUID, str]] = {}
    for campaign_id, campaign_type, status in ordered_rows:
        latest_by_type.setdefault(campaign_type, (campaign_id, status))
    rows = [
        (campaign_id, campaign_type)
        for campaign_type, (campaign_id, status) in latest_by_type.items()
        if status == CampaignStatus.PAUSED_CAPACITY_LIMIT.value
    ]

    recovered: list[uuid.UUID] = []
    for campaign_id, campaign_type in rows:
        if campaign_type == USER_POSTS_CAMPAIGN_TYPE:
            await UserPostCampaignManager(sessions, settings).reconcile(campaign_id)
        else:
            await CampaignManager(sessions, settings).reconcile(campaign_id)
        async with sessions() as session:
            status = await session.scalar(
                select(CollectionCampaign.status).where(CollectionCampaign.id == campaign_id)
            )
        if status != CampaignStatus.PAUSED_CAPACITY_LIMIT.value:
            recovered.append(campaign_id)
    return recovered

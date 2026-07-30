from __future__ import annotations

from sqlalchemy import delete, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from vk_collector.database.models import (
    CollectionJob,
    GroupCandidate,
    GroupMembership,
    GroupPost,
    UserGroupSubscription,
    VKUser,
)


async def inspect_user(session: AsyncSession, vk_id: int) -> dict[str, int | bool]:
    """Показать только агрегаты хранимых данных пользователя."""
    return {
        "exists": await session.get(VKUser, vk_id) is not None,
        "memberships": int(
            await session.scalar(
                select(func.count(GroupMembership.id)).where(GroupMembership.user_id == vk_id)
            )
            or 0
        ),
        "subscriptions": int(
            await session.scalar(
                select(func.count(UserGroupSubscription.id)).where(
                    UserGroupSubscription.user_id == vk_id
                )
            )
            or 0
        ),
        "jobs": int(
            await session.scalar(
                select(func.count(CollectionJob.id)).where(
                    CollectionJob.entity_type == "user", CollectionJob.entity_id == vk_id
                )
            )
            or 0
        ),
    }


async def delete_user(session: AsyncSession, vk_id: int) -> dict[str, int | bool]:
    """Транзакционно удалить профиль, связи и ещё не выполненные user jobs."""
    before = await inspect_user(session, vk_id)
    await session.execute(
        delete(CollectionJob).where(
            CollectionJob.entity_type == "user", CollectionJob.entity_id == vk_id
        )
    )
    await session.execute(delete(VKUser).where(VKUser.vk_id == vk_id))
    return before


async def inspect_group(session: AsyncSession, vk_id: int) -> dict[str, int | bool]:
    group = await session.scalar(select(GroupCandidate).where(GroupCandidate.vk_id == vk_id))
    if group is None:
        return {"exists": False, "posts": 0, "memberships": 0, "jobs": 0}
    return {
        "exists": True,
        "posts": int(
            await session.scalar(
                select(func.count(GroupPost.id)).where(GroupPost.group_id == group.id)
            )
            or 0
        ),
        "memberships": int(
            await session.scalar(
                select(func.count(GroupMembership.id)).where(GroupMembership.group_id == group.id)
            )
            or 0
        ),
        "jobs": int(
            await session.scalar(
                select(func.count(CollectionJob.id)).where(
                    CollectionJob.entity_type == "group", CollectionJob.entity_id == group.id
                )
            )
            or 0
        ),
    }

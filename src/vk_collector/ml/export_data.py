"""Модуль экспорта постов компаний из PostgreSQL в автономный датасет."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from vk_collector.database.models import (
    PostAttachment,
    UserGroupSubscription,
    UserPostAttachment,
    VKCommunity,
    VKUser,
)
from vk_collector.ml.contracts import (
    ModalityProfile,
    MultimodalPost,
    MultimodalUserPost,
    MultimodalUserProfile,
    PostAttachmentItem,
    UserDemographicsItem,
    UserSubscriptionItem,
)


def determine_modality_profile(
    has_text: bool,
    has_photo: bool,
    has_video: bool,
) -> ModalityProfile:
    """Определить профиль модальности на основе присутствующих компонентов."""
    if has_text and has_photo and has_video:
        return ModalityProfile.TRIMODAL
    if has_text and has_photo:
        return ModalityProfile.TEXT_IMAGE
    if has_text and has_video:
        return ModalityProfile.TEXT_VIDEO
    if has_text:
        return ModalityProfile.TEXT_ONLY
    if has_photo and has_video:
        return ModalityProfile.TRIMODAL
    if has_photo:
        return ModalityProfile.IMAGE_ONLY
    if has_video:
        return ModalityProfile.VIDEO_ONLY
    return ModalityProfile.EMPTY


async def fetch_eligible_company_posts(
    session: AsyncSession,
    *,
    window_days: int = 180,
    max_posts_per_group: int = 100,
    reference_time: datetime | None = None,
) -> list[MultimodalPost]:
    """Извлечь посты одобренных компаний в соответствии с критериями статьи.

    - Одобренные группы: group_candidates.classification_status = 'approved'
    - Временное окно: последние window_days дней (по умолчанию 180 / 6 месяцев)
    - Ограничение: не более max_posts_per_group на группу (по умолчанию 100)
    """
    now = reference_time or datetime.now(UTC)
    cutoff = now - timedelta(days=window_days)

    # 1. Загрузка постов с ограничением через оконную функцию
    # Используем raw SQL / sqlalchemy window query для точного и эффективного отбора
    stmt = text(
        """
        WITH ranked_posts AS (
            SELECT
                p.id AS post_id,
                p.group_id AS group_id,
                p.community_vk_id AS community_vk_id,
                COALESCE(gl.label, 'customer_acquisition') AS subject,
                p.published_at AS published_at,
                p.text AS text,
                p.comments_count AS comments_count,
                p.likes_count AS likes_count,
                p.reposts_count AS reposts_count,
                p.views_count AS views_count,
                ROW_NUMBER() OVER (
                    PARTITION BY p.group_id
                    ORDER BY p.published_at DESC
                ) AS rank_in_group
            FROM group_posts p
            JOIN group_candidates g ON g.id = p.group_id
            LEFT JOIN (
                SELECT group_id, MIN(label) AS label
                FROM group_labels
                GROUP BY group_id
            ) gl ON gl.group_id = g.id
            WHERE g.classification_status = 'approved'
              AND p.published_at >= :cutoff
        )
        SELECT
            post_id,
            group_id,
            community_vk_id,
            subject,
            published_at,
            text,
            comments_count,
            likes_count,
            reposts_count,
            views_count
        FROM ranked_posts
        WHERE rank_in_group <= :max_posts
        ORDER BY group_id, published_at DESC
        """
    )

    result = await session.execute(
        stmt,
        {"cutoff": cutoff, "max_posts": max_posts_per_group},
    )
    rows = result.fetchall()
    if not rows:
        return []

    post_ids = [row.post_id for row in rows]

    # 2. Загрузка всех вложений для отобранных постов
    attachments_stmt = (
        select(PostAttachment)
        .where(PostAttachment.post_id.in_(post_ids))
        .order_by(PostAttachment.post_id, PostAttachment.position)
    )
    attachments_res = await session.scalars(attachments_stmt)
    attachments_by_post: dict[int, list[PostAttachmentItem]] = {pid: [] for pid in post_ids}

    for att in attachments_res:
        attachments_by_post[att.post_id].append(
            PostAttachmentItem(
                position=att.position,
                attachment_type=att.attachment_type,
                vk_owner_id=att.vk_owner_id,
                vk_attachment_id=att.vk_attachment_id,
                access_key=att.access_key,
                duration=att.duration,
                width=att.width,
                height=att.height,
                title=att.title,
                external_url=att.external_url,
                attachment_metadata=att.attachment_metadata or {},
            )
        )

    # 3. Формирование объектов MultimodalPost
    posts: list[MultimodalPost] = []
    for r in rows:
        atts = attachments_by_post.get(r.post_id, [])
        has_photo = any(a.attachment_type == "photo" for a in atts)
        has_video = any(a.attachment_type == "video" for a in atts)
        text_str = (r.text or "").strip()
        has_text = len(text_str) > 0

        modality = determine_modality_profile(
            has_text=has_text,
            has_photo=has_photo,
            has_video=has_video,
        )

        posts.append(
            MultimodalPost(
                post_id=r.post_id,
                group_id=r.group_id,
                community_vk_id=r.community_vk_id,
                subject=r.subject,
                published_at=r.published_at,
                text=text_str,
                modality_profile=modality,
                attachments=atts,
                comments_count=r.comments_count,
                likes_count=r.likes_count,
                reposts_count=r.reposts_count,
                views_count=r.views_count,
            )
        )

    return posts


def append_posts_to_jsonl(posts: list[MultimodalPost], destination: Path) -> int:
    """Дозапись списка постов в формате JSONL (append mode)."""
    if not posts:
        return 0
    destination.parent.mkdir(parents=True, exist_ok=True)
    with destination.open("a", encoding="utf-8") as f:
        for post in posts:
            f.write(post.model_dump_json() + "\n")
    return len(posts)


def get_jsonl_checkpoint(destination: Path) -> tuple[int, int, set[int]]:
    """Быстрое определение контрольной точки из существующего JSONL файла.

    Возвращает:
        (total_posts, max_group_id, set_of_existing_post_ids)
    """
    if not destination.exists():
        return 0, 0, set()

    import json

    total_posts = 0
    max_group_id = 0
    existing_post_ids: set[int] = set()

    with destination.open("r", encoding="utf-8") as f:
        for line in f:
            line_str = line.strip()
            if not line_str:
                continue
            total_posts += 1
            try:
                data = json.loads(line_str)
                pid = data.get("post_id")
                gid = data.get("group_id")
                if pid is not None:
                    existing_post_ids.add(int(pid))
                if gid is not None and int(gid) > max_group_id:
                    max_group_id = int(gid)
            except Exception:
                continue

    return total_posts, max_group_id, existing_post_ids


def export_posts_to_jsonl(posts: list[MultimodalPost], destination: Path) -> Path:
    """Сохранить список постов в формате JSONL (перезапись)."""
    destination.parent.mkdir(parents=True, exist_ok=True)
    with destination.open("w", encoding="utf-8") as f:
        for post in posts:
            f.write(post.model_dump_json() + "\n")
    return destination


def load_posts_from_jsonl(
    source: Path,
    limit: int | None = None,
    offset: int = 0,
) -> list[MultimodalPost]:
    """Загрузить посты из файла JSONL с поддержкой лимита и смещения."""
    posts: list[MultimodalPost] = []
    if not source.exists():
        return posts
    with source.open("r", encoding="utf-8") as f:
        for idx, line in enumerate(f):
            if idx < offset:
                continue
            line_str = line.strip()
            if line_str:
                posts.append(MultimodalPost.model_validate_json(line_str))
            if limit is not None and len(posts) >= limit:
                break
    return posts


async def fetch_eligible_user_profiles(
    session: AsyncSession,
    *,
    window_days: int = 180,
    max_posts_per_user: int = 20,
    reference_time: datetime | None = None,
) -> list[MultimodalUserProfile]:
    """Извлечь мультимодальные профили пользователей в соответствии со статьей (раздел 3.6).

    - Пользователи с постами на стене (не более max_posts_per_user за последние window_days)
    - Пользователи с подписками на сообщества
    - Демографические данные пользователя (8 атрибутов)
    - Пользователи без постов или без подписок исключаются согласно статье.
    """
    now = reference_time or datetime.now(UTC)
    cutoff = now - timedelta(days=window_days)

    # 1. Посты пользователей с ранжированием
    posts_stmt = text(
        """
        WITH ranked_user_posts AS (
            SELECT
                p.id AS post_id,
                p.user_id AS user_id,
                p.published_at AS published_at,
                p.text AS text,
                p.comments_count AS comments_count,
                p.likes_count AS likes_count,
                p.reposts_count AS reposts_count,
                p.views_count AS views_count,
                ROW_NUMBER() OVER (
                    PARTITION BY p.user_id
                    ORDER BY p.published_at DESC
                ) AS rank_in_user
            FROM user_posts p
            JOIN vk_users u ON u.vk_id = p.user_id
            WHERE u.deactivated IS NULL
              AND (u.is_closed = false OR u.can_access_closed = true)
              AND p.published_at >= :cutoff
        )
        SELECT
            post_id,
            user_id,
            published_at,
            text,
            comments_count,
            likes_count,
            reposts_count,
            views_count
        FROM ranked_user_posts
        WHERE rank_in_user <= :max_posts
        ORDER BY user_id, published_at DESC
        """
    )
    posts_res = (
        await session.execute(
            posts_stmt,
            {"cutoff": cutoff, "max_posts": max_posts_per_user},
        )
    ).fetchall()

    if not posts_res:
        return []

    eligible_user_ids = sorted({r.user_id for r in posts_res})
    post_ids = [r.post_id for r in posts_res]

    # 2. Вложения к постам пользователей
    attachments_stmt = (
        select(UserPostAttachment)
        .where(UserPostAttachment.post_id.in_(post_ids))
        .order_by(UserPostAttachment.post_id, UserPostAttachment.position)
    )
    attachments_res = await session.scalars(attachments_stmt)
    attachments_by_post: dict[int, list[PostAttachmentItem]] = {pid: [] for pid in post_ids}
    for att in attachments_res:
        attachments_by_post[att.post_id].append(
            PostAttachmentItem(
                position=att.position,
                attachment_type=att.attachment_type,
                vk_owner_id=att.vk_owner_id,
                vk_attachment_id=att.vk_attachment_id,
                access_key=att.access_key,
                duration=att.duration,
                width=att.width,
                height=att.height,
                title=att.title,
                external_url=att.external_url,
                attachment_metadata=att.attachment_metadata or {},
            )
        )

    # Группировка постов по пользователям
    user_posts_map: dict[int, list[MultimodalUserPost]] = {uid: [] for uid in eligible_user_ids}
    for r in posts_res:
        atts = attachments_by_post.get(r.post_id, [])
        has_photo = any(a.attachment_type == "photo" for a in atts)
        has_video = any(a.attachment_type == "video" for a in atts)
        text_str = (r.text or "").strip()
        has_text = len(text_str) > 0
        modality = determine_modality_profile(
            has_text=has_text, has_photo=has_photo, has_video=has_video
        )
        user_posts_map[r.user_id].append(
            MultimodalUserPost(
                post_id=r.post_id,
                user_id=r.user_id,
                published_at=r.published_at,
                text=text_str,
                modality_profile=modality,
                attachments=atts,
                comments_count=r.comments_count,
                likes_count=r.likes_count,
                reposts_count=r.reposts_count,
                views_count=r.views_count,
            )
        )

    # 3. Подписки пользователей на сообщества
    subs_stmt = (
        select(
            UserGroupSubscription.user_id,
            VKCommunity.vk_id,
            VKCommunity.name,
            VKCommunity.description,
            VKCommunity.members_count,
        )
        .join(VKCommunity, VKCommunity.vk_id == UserGroupSubscription.vk_group_id)
        .where(
            UserGroupSubscription.user_id.in_(eligible_user_ids),
            UserGroupSubscription.is_current.is_(True),
        )
        .order_by(UserGroupSubscription.user_id, VKCommunity.members_count.desc().nullslast())
    )
    subs_res = (await session.execute(subs_stmt)).fetchall()
    subs_map: dict[int, list[UserSubscriptionItem]] = {uid: [] for uid in eligible_user_ids}
    for s in subs_res:
        subs_map[s.user_id].append(
            UserSubscriptionItem(
                community_vk_id=s.vk_id,
                name=s.name or "",
                description=s.description or "",
                members_count=s.members_count,
            )
        )

    # 4. Демографические данные пользователей
    users_stmt = select(VKUser).where(VKUser.vk_id.in_(eligible_user_ids))
    users_res = (await session.scalars(users_stmt)).all()
    users_by_id = {u.vk_id: u for u in users_res}

    # 5. Сборка MultimodalUserProfile
    profiles: list[MultimodalUserProfile] = []
    for uid in eligible_user_ids:
        u_posts = user_posts_map.get(uid, [])
        u_subs = subs_map.get(uid, [])
        # Статья: пользователи без постов или без подписок исключаются
        if not u_posts or not u_subs:
            continue

        vk_user = users_by_id.get(uid)
        demo = UserDemographicsItem(
            sex=vk_user.sex if vk_user else None,
            bdate=vk_user.bdate if vk_user else None,
            city=vk_user.city if vk_user else None,
            education=vk_user.education if vk_user else None,
            relation=vk_user.relation if vk_user else None,
            followers_count=vk_user.followers_count if vk_user else None,
            friends_count=vk_user.friends_count if vk_user else None,
            gifts_count=vk_user.gifts_count if vk_user else None,
        )

        profiles.append(
            MultimodalUserProfile(
                user_id=uid,
                posts=u_posts,
                subscriptions=u_subs,
                demographics=demo,
            )
        )

    return profiles


def export_user_profiles_to_jsonl(profiles: list[MultimodalUserProfile], destination: Path) -> Path:
    """Сохранить профили пользователей в формате JSONL."""
    destination.parent.mkdir(parents=True, exist_ok=True)
    with destination.open("w", encoding="utf-8") as f:
        for p in profiles:
            f.write(p.model_dump_json() + "\n")
    return destination


def load_user_profiles_from_jsonl(source: Path) -> list[MultimodalUserProfile]:
    """Загрузить профили пользователей из файла JSONL."""
    profiles: list[MultimodalUserProfile] = []
    with source.open("r", encoding="utf-8") as f:
        for line in f:
            line_str = line.strip()
            if line_str:
                profiles.append(MultimodalUserProfile.model_validate_json(line_str))
    return profiles

"""VK-панель на PostgreSQL: поиск новых постов, отметки возраста, пропуски, удаление, выгрузка."""

from __future__ import annotations

import os
from collections.abc import Sequence
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd  # type: ignore[import-untyped]
import pytest
from sqlalchemy import delete, func, select

from vk_collector.database.models import (
    Panel,
    PanelCommunity,
    PanelMedia,
    PanelPost,
    PanelPostSnapshot,
)
from vk_collector.database.session import create_database_engine, create_session_factory
from vk_collector.panel.service import PanelService, normalize_text


def database_url() -> str:
    explicit = os.getenv("DATABASE_URL")
    if explicit:
        return explicit
    return (
        "postgresql+asyncpg://"
        f"{os.getenv('POSTGRES_USER', 'vk_collector')}:"
        f"{os.getenv('POSTGRES_PASSWORD', 'local_development_only')}@"
        f"{os.getenv('POSTGRES_HOST', 'postgres')}:{os.getenv('POSTGRES_PORT', '5432')}/"
        f"{os.getenv('POSTGRES_DB', 'vk_research')}"
    )


pytestmark = pytest.mark.skipif(
    os.getenv("RUN_INTEGRATION_TESTS") != "1"
    or not database_url().rsplit("/", 1)[-1].endswith("_test"),
    reason="PostgreSQL integration test требует явного запуска на изолированной *_test БД",
)

T0 = datetime(2026, 11, 9, 4, 0, tzinfo=UTC)


class Clock:
    def __init__(self) -> None:
        self.now = T0

    def __call__(self) -> datetime:
        return self.now


class FakeVK:
    def __init__(self) -> None:
        self.walls: dict[int, Any] = {}
        self.deleted: set[str] = set()
        self.latest_calls: list[list[int]] = []
        self.counter_calls: list[list[str]] = []
        self.views = 100

    async def wall_latest(self, community_ids: Sequence[int], count: int) -> dict[int, Any]:
        self.latest_calls.append(list(community_ids))
        return {cid: self.walls.get(cid) for cid in community_ids}

    async def wall_pages(self, community_id: int, offsets: Sequence[int], count: int) -> list[Any]:
        return [{"count": 0, "items": []} for _ in offsets]

    async def posts_counters(self, keys: Sequence[str]) -> list[dict[str, Any]]:
        self.counter_calls.append(list(keys))
        self.views += 50
        out = []
        for key in keys:
            if key in self.deleted:
                continue
            owner, post = key.split("_")
            out.append(
                {
                    "id": int(post),
                    "owner_id": int(owner),
                    "views": self.views,
                    "likes": 3,
                    "reposts": 1,
                    "comments": 0,
                }
            )
        return out

    async def groups_info(self, community_ids: Sequence[int]) -> list[dict[str, Any]]:
        return [{"id": cid, "members_count": 5000 + cid} for cid in community_ids]


def _post(owner: int, post_id: int, published: datetime, **extra: Any) -> dict[str, Any]:
    return {
        "id": post_id,
        "owner_id": owner,
        "date": int(published.timestamp()),
        "text": "Скидки! https://vk.com/x  Ёлка",
        "views": {"count": 10},
        "likes": {"count": 1},
        "reposts": {"count": 0},
        "comments": {"count": 0},
        "attachments": [
            {
                "type": "photo",
                "photo": {
                    "id": 1,
                    "owner_id": owner,
                    "sizes": [
                        {"url": "https://sun9-1.userapi.com/a.jpg", "width": 800, "height": 600}
                    ],
                },
            },
            {
                "type": "video",
                "video": {
                    "id": 2,
                    "owner_id": owner,
                    "type": "short_video",
                    "duration": 20,
                    "image": [
                        {"url": "https://sun9-1.userapi.com/c.jpg", "width": 720, "height": 1280}
                    ],
                    "first_frame": [
                        {"url": "https://sun9-1.userapi.com/f.jpg", "width": 720, "height": 1280}
                    ],
                },
            },
        ],
        **extra,
    }


@pytest.mark.asyncio
async def test_panel_live_cycle(tmp_path: Path) -> None:
    engine = create_database_engine(database_url())
    sessions = create_session_factory(engine)
    async with sessions() as session:
        await session.execute(delete(Panel).where(Panel.name == "test-panel"))
        await session.commit()
    clock = Clock()
    vk = FakeVK()
    service = PanelService(sessions, vk, export_dir=tmp_path, now=clock)
    rows = [
        {"vk_id": 101, "stratum": "auto", "size": "S", "members_count": 5000, "role": "main"},
        {
            "vk_id": 202,
            "stratum": "news_society",
            "size": "XL",
            "members_count": 2_000_000,
            "role": "main",
        },
        {"vk_id": 303, "stratum": "auto", "size": "S", "members_count": 6000, "role": "reserve"},
    ]
    panel_id = await service.freeze_selection(
        "test-panel",
        rows,
        seed=1,
        config={"poll_interval_seconds": {"S": 7200, "M": 3600, "L": 3600, "XL": 3600}},
    )
    with pytest.raises(RuntimeError):
        await service.tick("test-panel")  # draft — не работает до activate
    await service.set_status(panel_id, "active")

    fresh = T0 - timedelta(minutes=30)
    vk.walls[101] = {
        "count": 2,
        "items": [
            _post(-101, 11, fresh),
            _post(
                -101,
                10,
                T0 - timedelta(days=5),
                copy_history=[{"id": 7, "owner_id": -9, "text": "orig"}],
            ),
        ],
    }
    vk.walls[202] = None  # стена недоступна

    r1 = await service.tick("test-panel")
    assert (r1.polled, r1.new_posts, r1.members) == (2, 2, 2)
    async with sessions() as session:
        posts = {
            p.vk_post_id: p
            for p in (
                await session.scalars(select(PanelPost).where(PanelPost.panel_id == panel_id))
            ).all()
        }
        assert posts[11].origin == "backfill"  # опубликован до старта (started_at = T0)
        assert posts[11].next_target_index == 0  # но отметки 1 ч ещё не прошли — замеряем
        assert posts[11].next_due_at == fresh + timedelta(hours=1)
        assert posts[10].next_due_at is None and posts[10].missed_targets == 0
        assert posts[10].is_repost and posts[10].repost_source_owner_id == -9
        assert posts[11].n_photo == 1 and posts[11].n_video == 1
        media = (
            await session.scalars(select(PanelMedia).where(PanelMedia.panel_id == panel_id))
        ).all()
        assert {m.kind for m in media} == {"photo", "video_cover", "video_first_frame"}
        excluded = await session.scalar(
            select(PanelCommunity.excluded_reason).where(
                PanelCommunity.panel_id == panel_id, PanelCommunity.community_vk_id == 202
            )
        )
        assert excluded == "wall.get failed"

    # отметка 1 ч поста 11 (возраст 70 мин, допуск 20 мин); опрашивать ещё никого не пора
    clock.now = T0 + timedelta(minutes=40)
    r2 = await service.tick("test-panel")
    assert (r2.polled, r2.measured) == (0, 1)
    assert len(vk.latest_calls) == 1

    # XL — раз в час, S — раз в 2 ч: в 05:05 опрашивается только 202; новый пост — live
    clock.now = T0 + timedelta(minutes=65)
    vk.walls[202] = {"count": 1, "items": [_post(-202, 5, T0 + timedelta(minutes=10))]}
    r2b = await service.tick("test-panel")
    assert vk.latest_calls[-1] == [202]
    assert r2b.new_posts == 1
    async with sessions() as session:
        p5 = await session.get(PanelPost, (panel_id, -202, 5))
        assert p5 is not None and p5.origin == "live" and p5.next_target_index == 0

    # 06:40: у поста 5 отметка 1 ч (05:10, допуск до 05:30) прошла — замер «поздний», пропуск;
    # у поста 11 наступила отметка 3 ч (06:30), но пост удалён
    clock.now = T0 + timedelta(hours=2, minutes=40)
    vk.deleted.add("-101_11")
    r3 = await service.tick("test-panel")
    async with sessions() as session:
        p5 = await session.get(PanelPost, (panel_id, -202, 5))
        p11 = await session.get(PanelPost, (panel_id, -101, 11))
        assert p5 is not None and p11 is not None
        assert p5.missed_targets == 1 and p5.next_target_index == 1  # дальше — 3 ч
        assert p11.deleted_at is not None
        snaps = (
            await session.scalars(
                select(PanelPostSnapshot).where(
                    PanelPostSnapshot.panel_id == panel_id, PanelPostSnapshot.source == "getbyid"
                )
            )
        ).all()
        targets = sorted((s.vk_post_id, s.target_age_h) for s in snaps)
        assert targets == [(5, None), (11, 1)]
        assert all(s.age_seconds > 0 for s in snaps)
    assert r3.deleted == 1

    path = Path(await service.export("test-panel", "interim"))
    posts_df = pd.read_parquet(path / "posts.parquet")
    snaps_df = pd.read_parquet(path / "snapshots.parquet")
    assert len(posts_df) == 3 and len(snaps_df) >= 4
    assert (path / "manifest.json").exists()

    # потоковая выгрузка порциями: те же строки, несколько row group, типы по схеме
    import pyarrow.parquet as pq  # type: ignore[import-untyped]

    clock.now = clock.now + timedelta(seconds=1)  # другой каталог <UTC-время>
    small = Path(await service.export("test-panel", "interim_small", batch_rows=2))
    snaps_small = pd.read_parquet(small / "snapshots.parquet")
    assert pq.ParquetFile(small / "snapshots.parquet").num_row_groups == -(-len(snaps_df) // 2)
    assert pq.ParquetFile(small / "posts.parquet").num_row_groups == 2
    key = ["vk_owner_id", "vk_post_id", "measured_at"]
    pd.testing.assert_frame_equal(
        snaps_small.sort_values(key).reset_index(drop=True),
        snaps_df.sort_values(key).reset_index(drop=True),
        check_dtype=False,
    )
    schema = pq.read_schema(small / "posts.parquet")
    assert str(schema.field("published_at").type) == "timestamp[us, tz=UTC]"
    assert str(schema.field("vk_post_id").type) == "int64"
    assert str(schema.field("attachments").type) == "string"
    posts_small = pd.read_parquet(small / "posts.parquet")
    assert posts_small["next_due_at"].dtype.kind == "M"  # даты, а не object

    # заморозка по stop_at
    async with sessions() as session:
        panel = await session.get(Panel, panel_id)
        assert panel is not None
        panel.config = {**panel.config, "stop_at": (T0 + timedelta(hours=3)).isoformat()}
        await session.commit()
    clock.now = T0 + timedelta(hours=3)
    final = await service.tick("test-panel")
    assert final.frozen and final.exported
    await engine.dispose()


def test_normalize_text() -> None:
    assert normalize_text("Скидки! https://vk.com/x  Ёлка [club1|Магазин]") == "скидки елка магазин"


class BackfillVK(FakeVK):
    """Стены для ретроспективы: крупное сообщество с «тяжёлыми» страницами и одно безнадёжное."""

    def __init__(self, posts: dict[int, list[dict[str, Any]]]) -> None:
        super().__init__()
        self.posts = posts
        self.page_calls: list[tuple[int, list[int], int]] = []

    async def wall_pages(self, community_id: int, offsets: Sequence[int], count: int) -> list[Any]:
        from vk_collector.vk.errors import VKAPIError

        self.page_calls.append((community_id, list(offsets), count))
        if community_id == 501:  # не помещается в ответ даже по 20 постов
            raise VKAPIError(13, "response size is too big")
        if community_id == 502 and count > 50:  # крупное: длинные посты, 100 не влезает
            raise VKAPIError(13, "response size is too big")
        wall = self.posts.get(community_id, [])
        return [{"count": len(wall), "items": wall[o : o + count]} for o in offsets]


@pytest.mark.asyncio
async def test_panel_backfill_big_and_stuck_communities(tmp_path: Path) -> None:
    engine = create_database_engine(database_url())
    sessions = create_session_factory(engine)
    async with sessions() as session:
        await session.execute(delete(Panel).where(Panel.name == "test-backfill"))
        await session.commit()
    clock = Clock()
    # 502: 260 постов за 90 дней (по 8 ч) + старые за границей; 503: 3 поста
    big = [_post(-502, 1000 - i, T0 - timedelta(hours=8 * i + 1)) for i in range(300)]
    small = [_post(-503, 30 - i, T0 - timedelta(days=i + 1)) for i in range(3)]
    vk = BackfillVK({502: big, 503: small})
    service = PanelService(sessions, vk, export_dir=tmp_path, now=clock)
    rows = [
        {"vk_id": cid, "stratum": "auto", "size": "M", "members_count": 50_000, "role": "main"}
        for cid in (501, 502, 503)
    ]
    panel_id = await service.freeze_selection("test-backfill", rows, seed=1, config={})
    await service.set_status(panel_id, "active")

    # 1-й проход: 501 падает (1/3), 502 качается по 50, 503 готово
    stats = await service.backfill("test-backfill")
    assert stats["failed"] == 1 and stats["communities"] == 2
    in_window = sum(1 for i in range(300) if 8 * i + 1 <= 90 * 24)
    async with sessions() as session:
        n_big = await session.scalar(
            select(func.count())
            .select_from(PanelPost)
            .where(PanelPost.panel_id == panel_id, PanelPost.community_vk_id == 502)
        )
    assert n_big == in_window
    # одна страница на execute, после ошибки 13 — count 50
    assert all(len(offsets) == 1 for _, offsets, _ in vk.page_calls)
    assert {c for cid, _, c in vk.page_calls if cid == 502} == {100, 50}

    # ещё 2 прохода: 501 падает 3 раза подряд -> в конец очереди
    await service.backfill("test-backfill")
    stats = await service.backfill("test-backfill")
    assert stats["deferred"] == 1
    assert service._bf_deferred == [501]
    async with sessions() as session:
        pending = (
            await session.scalars(
                select(PanelCommunity.community_vk_id).where(
                    PanelCommunity.panel_id == panel_id,
                    PanelCommunity.backfill_done_at.is_(None),
                )
            )
        ).all()
    assert list(pending) == [501]
    async with sessions() as session:
        await session.execute(delete(Panel).where(Panel.name == "test-backfill"))
        await session.commit()
    await engine.dispose()


@pytest.mark.asyncio
async def test_panel_ext_backfill_12m_video_only_without_media(tmp_path: Path) -> None:
    engine = create_database_engine(database_url())
    sessions = create_session_factory(engine)
    async with sessions() as session:
        await session.execute(delete(Panel).where(Panel.name == "test-ext"))
        await session.commit()
    clock = Clock()

    def photo_post(pid: int, published: datetime) -> dict[str, Any]:
        post = _post(-601, pid, published)
        post["attachments"] = post["attachments"][:1]  # только фото
        return post

    def video_post(pid: int, published: datetime) -> dict[str, Any]:
        post = _post(-601, pid, published)
        post["attachments"] = post["attachments"][1:]  # только видео
        return post

    # по посту в сутки на 420 дней назад: чётные — фото, нечётные — видео
    wall = [
        (video_post if i % 2 else photo_post)(5000 - i, T0 - timedelta(days=i, hours=1))
        for i in range(420)
    ]
    vk = BackfillVK({601: wall})
    service = PanelService(sessions, vk, export_dir=tmp_path, now=clock)
    rows = [{"vk_id": 601, "stratum": "auto", "size": "M", "members_count": 9000, "role": "main"}]
    panel_id = await service.freeze_selection("test-ext", rows, seed=1, config={})
    await service.set_status(panel_id, "active")
    async with sessions() as session:
        panel = await session.get(Panel, panel_id)
        assert panel is not None
        panel.config = {**panel.config, "ext_backfill_since": "2025-11-11T00:00:00+03:00"}
        await session.commit()

    # до окончания 90 дней 12 месяцев не начинаются
    early = await service.ext_backfill("test-ext")
    assert early.get("waiting_90d") == 1
    await service.backfill("test-ext")
    media_before = await _count(sessions, PanelMedia, panel_id)
    stats = await service.ext_backfill("test-ext")
    assert stats["communities"] == 1

    since = datetime(2025, 11, 10, 21, 0, tzinfo=UTC)
    upper = T0 - timedelta(days=90)
    expected = sum(
        1 for i in range(420) if i % 2 and since <= T0 - timedelta(days=i, hours=1) < upper
    )
    async with sessions() as session:
        ext = (
            await session.scalars(
                select(PanelPost).where(
                    PanelPost.panel_id == panel_id, PanelPost.origin == "ext12m"
                )
            )
        ).all()
    assert len(ext) == expected
    assert all(p.n_video == 1 and p.n_photo == 0 for p in ext)
    assert all(since <= p.published_at < upper for p in ext)
    # медиа для 12 месяцев не ставятся в очередь
    assert await _count(sessions, PanelMedia, panel_id) == media_before
    async with sessions() as session:
        await session.execute(delete(Panel).where(Panel.name == "test-ext"))
        await session.commit()
    await engine.dispose()


async def _count(sessions: Any, model: Any, panel_id: Any) -> int:
    async with sessions() as session:
        return int(
            await session.scalar(
                select(func.count()).select_from(model).where(model.panel_id == panel_id)
            )
            or 0
        )

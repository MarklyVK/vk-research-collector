"""VK-панель для исследования виральности.

Цикл (``tick``, по умолчанию каждые 10 мин):
1. Поиск новых постов: сообщества, у которых наступил ``next_poll_at`` (раз в час или реже по
   конфигу размера), — ``wall.get`` последних ``poll_count`` постов через ``execute``.
   Новый пост → ``panel_posts`` (origin=live) + замер ``source=poll`` + план отметок возраста.
2. Замеры: посты с ``next_due_at <= now`` — ``wall.getById`` пачками по 100 через ``execute``.
   Замер засчитывается отметке T (часы), если сделан не позже T + допуск; иначе отметка
   ``missed``. Хранится всегда фактический возраст (``age_seconds``).
3. Раз в сутки — ``members_count`` (``groups.getById``).
4. Промежуточная выгрузка (``interim_export_at``) и заморозка с финальной выгрузкой (``stop_at``).
Ретроспектива (``backfill``) — разово: все посты за ``backfill_days`` + один снимок счётчиков.
"""

from __future__ import annotations

import hashlib
import json
import logging
import re
import uuid
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any
from zoneinfo import ZoneInfo

from sqlalchemy import func, select, update
from sqlalchemy.dialects.postgresql import insert

from vk_collector.database.models import (
    Panel,
    PanelCommunity,
    PanelCommunitySnapshot,
    PanelMedia,
    PanelPost,
    PanelPostSnapshot,
)
from vk_collector.panel.vk_adapter import PanelVK
from vk_collector.vk.errors import VKAPIError, VKMethodUnavailable, VKTokensUnavailable

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

logger = logging.getLogger(__name__)

VK_RESPONSE_TOO_BIG = 13  # «response size is too big» у execute
BACKFILL_FALLBACK_SIZES = (50, 20)
BACKFILL_MAX_FAILURES = 3
# решение Андрея 09.11: для video и clip ретроспектива до 12 месяцев (с 11.11.2025), без медиа
EXT_BACKFILL_SINCE = "2025-11-11T00:00:00+03:00"
EXT_BACKFILL_DAILY_PAGES = 6000  # потолок страниц в сутки (МСК) — запас лимита живой панели
EXT_ORIGIN = "ext12m"
MSK = ZoneInfo("Europe/Moscow")


def has_video_or_clip(post: dict[str, Any]) -> bool:
    """Собственное вложение поста — видео или клип (репост не считается)."""
    return any(
        a.get("type") in ("video", "clip", "short_video") for a in post.get("attachments") or []
    )


@dataclass
class _BackfillProgress:
    offset: int = 0
    max_id: int | None = None
    new: int = 0


DEFAULT_CONFIG: dict[str, Any] = {
    "targets_h": [1, 3, 6, 12, 24, 48, 72],
    "tolerance_min_minutes": 20,
    "tolerance_fraction": 0.1,
    "poll_count": 30,
    "poll_interval_seconds": {"S": 3600, "M": 3600, "L": 3600, "XL": 3600},
    "backfill_days": 90,
    "ext_backfill_since": EXT_BACKFILL_SINCE,
    "ext_backfill_daily_pages": EXT_BACKFILL_DAILY_PAGES,
    "backfill_page_size": 100,
    "poll_snapshot_max_age_h": 72,
    "members_every_hours": 24,
    "interim_export_at": None,
    "stop_at": None,
    "exports_done": [],
}

_URL_RE = re.compile(r"https?://\S+|vk\.com/\S+|\[(?:id|club|public)\d+\|([^\]]*)\]")
_NON_WORD_RE = re.compile(r"[^\w]+", re.UNICODE)


def normalize_text(text: str) -> str:
    """Нормализация для хеша дубликатов: нижний регистр, без ссылок/упоминаний, только слова."""
    text = _URL_RE.sub(lambda m: m.group(1) or " ", text or "").lower().replace("ё", "е")
    return " ".join(_NON_WORD_RE.sub(" ", text).split())


def text_norm_hash(text: str) -> str | None:
    norm = normalize_text(text)
    return hashlib.sha256(norm.encode("utf-8")).hexdigest() if norm else None


def _largest(images: Any) -> dict[str, Any] | None:
    items = [x for x in images or [] if isinstance(x, dict) and x.get("url")]
    if not items:
        return None
    best = max(items, key=lambda x: (x.get("width") or 0) * (x.get("height") or 0))
    return {"url": best["url"], "width": best.get("width"), "height": best.get("height")}


def slim_attachment(att: dict[str, Any]) -> dict[str, Any]:
    kind = str(att.get("type") or "")
    data = att.get(kind) or {}
    out: dict[str, Any] = {"type": kind}
    if kind == "photo":
        out.update({"id": data.get("id"), "owner_id": data.get("owner_id")})
        best = _largest(data.get("sizes"))
        if best:
            out["photo"] = best
        if data.get("orig_photo", {}).get("url"):
            out["orig_photo"] = _largest([data["orig_photo"]])
    elif kind in ("video", "clip", "short_video"):
        out.update(
            {
                "id": data.get("id"),
                "owner_id": data.get("owner_id"),
                "video_type": data.get("type"),
                "duration": data.get("duration"),
                "title": data.get("title"),
                "description": (data.get("description") or "")[:2000],
                "cover": _largest(data.get("image")),
                "first_frame": _largest(data.get("first_frame")),
            }
        )
    elif kind == "link":
        out.update({"url": data.get("url"), "title": data.get("title")})
    elif kind == "doc":
        out.update({"ext": data.get("ext"), "doc_type": data.get("type")})
    return out


@dataclass(frozen=True)
class TickResult:
    polled: int
    new_posts: int
    measured: int
    missed: int
    deleted: int
    members: int
    exported: str | None
    frozen: bool


class PanelService:
    def __init__(
        self,
        sessions: async_sessionmaker[AsyncSession],
        vk: PanelVK,
        *,
        export_dir: Path | None = None,
        now: Callable[[], datetime] | None = None,
    ) -> None:
        self._sessions = sessions
        self._vk = vk
        self._export_dir = export_dir or Path("exports/vk_panel")
        self._now = now or (lambda: datetime.now(UTC))
        # состояние ретроспективы в памяти процесса panel-worker
        self._bf_progress: dict[int, _BackfillProgress] = {}
        self._bf_failures: dict[int, int] = {}
        self._bf_deferred: list[int] = []
        self._ext_progress: dict[int, _BackfillProgress] = {}
        self._ext_failures: dict[int, int] = {}
        self._ext_deferred: list[int] = []
        self._ext_pages: dict[str, int] = {}

    # ------------------------------------------------------------------ выборка
    async def freeze_selection(
        self, name: str, rows: Iterable[dict[str, Any]], *, seed: int, config: dict[str, Any]
    ) -> uuid.UUID:
        """Создать панель и зафиксировать выборку (только role=main)."""
        cfg = {**DEFAULT_CONFIG, **config}
        intervals: dict[str, int] = cfg["poll_interval_seconds"]
        now = self._now()
        async with self._sessions() as session:
            if await session.scalar(select(Panel.id).where(Panel.name == name)) is not None:
                raise ValueError(f"панель {name!r} уже существует")
            panel = Panel(id=uuid.uuid4(), name=name, status="draft", config=cfg)
            session.add(panel)
            await session.flush()
            n = 0
            for row in rows:
                if row.get("role", "main") != "main":
                    continue
                size = str(row["size"])
                session.add(
                    PanelCommunity(
                        panel_id=panel.id,
                        community_vk_id=int(row["vk_id"]),
                        name=str(row.get("name") or "")[:512],
                        screen_name=row.get("screen_name") or None,
                        stratum=str(row["stratum"]),
                        size_bucket=size,
                        members_at_selection=int(row["members_count"]),
                        selection_seed=seed,
                        poll_interval_seconds=int(intervals[size]),
                        next_poll_at=now,
                    )
                )
                n += 1
            await session.commit()
            logger.info("panel_frozen name=%s communities=%s", name, n)
            return panel.id

    async def panel_by_name(self, name: str) -> Panel:
        async with self._sessions() as session:
            panel = await session.scalar(select(Panel).where(Panel.name == name))
            if panel is None:
                raise ValueError(f"панель {name!r} не найдена")
            return panel

    async def set_status(self, panel_id: uuid.UUID, status: str) -> None:
        async with self._sessions() as session:
            values: dict[str, Any] = {"status": status}
            if status == "active":
                values["started_at"] = func.coalesce(Panel.started_at, self._now())
            if status == "frozen":
                values["frozen_at"] = self._now()
            await session.execute(update(Panel).where(Panel.id == panel_id).values(**values))
            await session.commit()

    # ------------------------------------------------------------------ посты
    def _target_plan(
        self, cfg: dict[str, Any], published: datetime, after: datetime, start_index: int = 0
    ) -> tuple[int | None, datetime | None, int]:
        """Первая отметка с индекса start_index, дедлайн которой ещё не прошёл к моменту after.

        Возвращает (индекс, due_at, число пропущенных отметок)."""
        targets: list[int] = cfg["targets_h"]
        skipped = 0
        for idx in range(start_index, len(targets)):
            due = published + timedelta(hours=targets[idx])
            if after <= due + self._tolerance(cfg, targets[idx]):
                return idx, due, skipped
            skipped += 1
        return None, None, skipped

    @staticmethod
    def _tolerance(cfg: dict[str, Any], target_h: int) -> timedelta:
        return max(
            timedelta(minutes=float(cfg["tolerance_min_minutes"])),
            timedelta(hours=target_h * float(cfg["tolerance_fraction"])),
        )

    async def _store_posts(
        self,
        session: AsyncSession,
        panel: Panel,
        community_id: int,
        items: Sequence[dict[str, Any]],
        *,
        origin: str,
        now: datetime,
        snapshot_source: str,
    ) -> tuple[int, int | None]:
        """Сохранить посты (новые — со всеми атрибутами) и снимок счётчиков. → (новых, max id)."""
        cfg = panel.config
        new = 0
        max_id: int | None = None
        keys = [(int(p["owner_id"]), int(p["id"])) for p in items if "id" in p and "owner_id" in p]
        existing: set[tuple[int, int]] = set()
        if keys:
            rows = await session.execute(
                select(PanelPost.vk_owner_id, PanelPost.vk_post_id).where(
                    PanelPost.panel_id == panel.id,
                    PanelPost.community_vk_id == community_id,
                    PanelPost.vk_post_id.in_([k[1] for k in keys]),
                )
            )
            existing = {(int(a), int(b)) for a, b in rows.all()}
        max_poll_age = timedelta(hours=float(cfg["poll_snapshot_max_age_h"]))
        for p in items:
            if "id" not in p or "owner_id" not in p or not p.get("date"):
                continue
            owner, post_id = int(p["owner_id"]), int(p["id"])
            published = datetime.fromtimestamp(int(p["date"]), UTC)
            max_id = post_id if max_id is None else max(max_id, post_id)
            age = now - published
            if (owner, post_id) not in existing:
                atts = [slim_attachment(a) for a in p.get("attachments") or []]
                copy = (p.get("copy_history") or [None])[0]
                post_origin = origin
                if (
                    origin == "live"
                    and panel.started_at is not None
                    and published < panel.started_at
                ):
                    post_origin = "backfill"  # опубликован до старта панели
                # план отметок — для любого поста, у которого ещё не все отметки прошли
                idx, due, skipped = (
                    self._target_plan(cfg, published, now)
                    if snapshot_source != "backfill"
                    else (None, None, 0)
                )
                if post_origin != "live":
                    skipped = 0  # пропуски считаем только для постов живой панели
                await session.execute(
                    insert(PanelPost)
                    .values(
                        panel_id=panel.id,
                        vk_owner_id=owner,
                        vk_post_id=post_id,
                        community_vk_id=community_id,
                        published_at=published,
                        first_seen_at=now,
                        origin=post_origin,
                        post_type=p.get("post_type"),
                        is_pinned=bool(p.get("is_pinned")),
                        marked_as_ads=bool(p.get("marked_as_ads")),
                        is_repost=copy is not None,
                        repost_source_owner_id=copy.get("owner_id") if copy else None,
                        repost_source_post_id=copy.get("id") if copy else None,
                        text=p.get("text") or "",
                        text_norm_hash=text_norm_hash(
                            (p.get("text") or "") + " " + ((copy or {}).get("text") or "")
                        ),
                        attachments=atts
                        + (
                            [
                                {"from_repost": True, **slim_attachment(a)}
                                for a in copy.get("attachments") or []
                            ]
                            if copy
                            else []
                        ),
                        n_photo=sum(a["type"] == "photo" for a in atts),
                        n_video=sum(a["type"] in ("video", "clip", "short_video") for a in atts),
                        next_target_index=idx,
                        next_due_at=due,
                        missed_targets=skipped,
                    )
                    .on_conflict_do_nothing()
                )
                await self._queue_media(session, panel.id, owner, post_id, atts, post_origin)
                new += 1
            if snapshot_source == "backfill" or age <= max_poll_age:
                session.add(
                    _snapshot(panel.id, owner, post_id, p, now, published, None, snapshot_source)
                )
        return new, max_id

    async def _queue_media(
        self,
        session: AsyncSession,
        panel_id: uuid.UUID,
        owner: int,
        post_id: int,
        atts: Sequence[dict[str, Any]],
        origin: str,
    ) -> None:
        if origin == EXT_ORIGIN:
            return  # 12-месячная ретроспектива — только посты и счётчики, без медиа
        priority = 0 if origin == "live" else 1
        for pos, att in enumerate(atts):
            entries: list[tuple[str, dict[str, Any] | None]] = []
            if att["type"] == "photo":
                entries.append(("photo", att.get("orig_photo") or att.get("photo")))
            elif att["type"] in ("video", "clip", "short_video"):
                entries += [
                    ("video_cover", att.get("cover")),
                    ("video_first_frame", att.get("first_frame")),
                ]
            for kind, img in entries:
                if not img:
                    continue
                await session.execute(
                    insert(PanelMedia)
                    .values(
                        panel_id=panel_id,
                        vk_owner_id=owner,
                        vk_post_id=post_id,
                        position=pos,
                        kind=kind,
                        url=img["url"],
                        priority=priority,
                        width=img.get("width"),
                        height=img.get("height"),
                    )
                    .on_conflict_do_nothing()
                )

    # ------------------------------------------------------------------ ретроспектива
    def _page_sizes(self, cfg: dict[str, Any]) -> list[int]:
        first = int(cfg["backfill_page_size"])
        return [first] + [s for s in BACKFILL_FALLBACK_SIZES if s < first]

    async def _backfill_page(
        self, community_id: int, offset: int, sizes: Sequence[int]
    ) -> tuple[Any, int]:
        """Одна страница стены за один execute; на ошибке VK 13 count уменьшается (100→50→20)."""
        last: VKAPIError | None = None
        for size in sizes:
            try:
                pages = await self._vk.wall_pages(community_id, [offset], size)
            except VKAPIError as exc:
                if exc.code != VK_RESPONSE_TOO_BIG:
                    raise
                logger.warning(
                    "panel_backfill_page_too_big community=%s offset=%s count=%s",
                    community_id,
                    offset,
                    size,
                )
                last = exc
                continue
            return (pages[0] if pages else None), size
        assert last is not None
        raise last

    def _backfill_order(self, todo: list[PanelCommunity]) -> list[PanelCommunity]:
        """Отложенные (падали 3 раза подряд) — в конец очереди, в порядке откладывания."""
        deferred = {cid: i for i, cid in enumerate(self._bf_deferred)}
        head = [c for c in todo if c.community_vk_id not in deferred]
        tail = sorted(
            (c for c in todo if c.community_vk_id in deferred),
            key=lambda c: deferred[c.community_vk_id],
        )
        return head + tail

    async def backfill(self, panel_name: str, *, limit: int | None = None) -> dict[str, int]:
        """Ретроспектива порциями: ``limit`` сообществ за вызов, по одной странице на execute.

        Прогресс внутри сообщества (offset) держится в памяти процесса: после сбоя следующий
        вызов продолжает с той же страницы. Сообщество, упавшее ``BACKFILL_MAX_FAILURES`` раз
        подряд, уходит в конец очереди (лог ``panel_backfill_deferred``). Ошибки квоты/токенов
        (``VKMethodUnavailable``, ``VKTokensUnavailable``) прерывают всю порцию.
        """
        panel = await self.panel_by_name(panel_name)
        cfg = panel.config
        sizes = self._page_sizes(cfg)
        start = panel.started_at or self._now()
        cutoff = start - timedelta(days=int(cfg["backfill_days"]))
        async with self._sessions() as session:
            todo = list(
                (
                    await session.scalars(
                        select(PanelCommunity)
                        .where(
                            PanelCommunity.panel_id == panel.id,
                            PanelCommunity.backfill_done_at.is_(None),
                            PanelCommunity.excluded_at.is_(None),
                        )
                        .order_by(PanelCommunity.community_vk_id)
                    )
                ).all()
            )
        todo = self._backfill_order(todo)
        if limit is not None:
            todo = todo[:limit]
        stats = {"communities": 0, "posts": 0, "pages": 0, "failed": 0, "deferred": 0}
        for community in todo:
            cid = community.community_vk_id
            progress = self._bf_progress.setdefault(cid, _BackfillProgress())
            done = False
            while not done:
                try:
                    result, size = await self._backfill_page(cid, progress.offset, sizes)
                except (VKMethodUnavailable, VKTokensUnavailable):
                    raise
                except Exception as exc:
                    failures = self._bf_failures.get(cid, 0) + 1
                    self._bf_failures[cid] = failures
                    stats["failed"] += 1
                    logger.warning(
                        "panel_backfill_failed community=%s offset=%s failures=%s error=%s",
                        cid,
                        progress.offset,
                        failures,
                        type(exc).__name__
                        + (f"({exc.code})" if isinstance(exc, VKAPIError) else ""),
                    )
                    if failures >= BACKFILL_MAX_FAILURES:
                        self._bf_failures[cid] = 0
                        if cid in self._bf_deferred:
                            self._bf_deferred.remove(cid)
                        self._bf_deferred.append(cid)
                        stats["deferred"] += 1
                        logger.warning(
                            "panel_backfill_deferred community=%s offset=%s -> end of queue",
                            cid,
                            progress.offset,
                        )
                    break
                self._bf_failures.pop(cid, None)
                stats["pages"] += 1
                items = (result or {}).get("items") or [] if isinstance(result, dict) else []
                fresh = [
                    p
                    for p in items
                    if p.get("date") and datetime.fromtimestamp(int(p["date"]), UTC) >= cutoff
                ]
                async with self._sessions() as session:
                    new, mid = await self._store_posts(
                        session,
                        panel,
                        cid,
                        fresh,
                        origin="backfill",
                        now=self._now(),
                        snapshot_source="backfill",
                    )
                    await session.commit()
                progress.new += new
                if mid is not None:
                    progress.max_id = mid if progress.max_id is None else max(progress.max_id, mid)
                non_pinned = [p for p in items if not p.get("is_pinned")]
                if not items or (
                    non_pinned and datetime.fromtimestamp(int(non_pinned[-1]["date"]), UTC) < cutoff
                ):
                    done = True
                progress.offset += size
            if not done:
                continue
            async with self._sessions() as session:
                await session.execute(
                    update(PanelCommunity)
                    .where(
                        PanelCommunity.panel_id == panel.id,
                        PanelCommunity.community_vk_id == cid,
                    )
                    .values(
                        backfill_done_at=self._now(),
                        last_seen_post_id=func.greatest(
                            func.coalesce(PanelCommunity.last_seen_post_id, 0),
                            progress.max_id or 0,
                        ),
                    )
                )
                await session.commit()
            self._bf_progress.pop(cid, None)
            if cid in self._bf_deferred:
                self._bf_deferred.remove(cid)
            stats["communities"] += 1
            stats["posts"] += progress.new
            logger.info(
                "panel_backfill community=%s posts=%s pages_offset=%s",
                cid,
                progress.new,
                progress.offset,
            )
        return stats

    # ------------------------------------------------------------------ ретроспектива 12 мес.
    async def ext_backfill(self, panel_name: str, *, limit: int | None = 5) -> dict[str, int]:
        """Посты с видео/клипами от ``ext_backfill_since`` до границы 90-дневной ретроспективы.

        Начинается только когда 90-дневная ретроспектива закончена у всех сообществ. Хранит
        посты и один снимок счётчиков (origin ``ext12m``), медиа не ставит в очередь. Страниц
        в сутки (МСК) — не больше ``ext_backfill_daily_pages``; общий бюджет токенов (80 %)
        действует как обычно. Сбои — как в ``backfill`` (откат count, 3 падения → конец очереди).
        """
        panel = await self.panel_by_name(panel_name)
        cfg = panel.config
        stats = {"communities": 0, "posts": 0, "pages": 0, "failed": 0, "deferred": 0, "capped": 0}
        async with self._sessions() as session:
            pending_90d = await session.scalar(
                select(func.count())
                .select_from(PanelCommunity)
                .where(
                    PanelCommunity.panel_id == panel.id,
                    PanelCommunity.backfill_done_at.is_(None),
                    PanelCommunity.excluded_at.is_(None),
                )
            )
            if pending_90d:
                stats["waiting_90d"] = int(pending_90d)
                return stats
            todo = list(
                (
                    await session.scalars(
                        select(PanelCommunity)
                        .where(
                            PanelCommunity.panel_id == panel.id,
                            PanelCommunity.ext_backfill_done_at.is_(None),
                            PanelCommunity.excluded_at.is_(None),
                        )
                        .order_by(PanelCommunity.community_vk_id)
                    )
                ).all()
            )
        deferred = {cid: i for i, cid in enumerate(self._ext_deferred)}
        todo = [c for c in todo if c.community_vk_id not in deferred] + sorted(
            (c for c in todo if c.community_vk_id in deferred),
            key=lambda c: deferred[c.community_vk_id],
        )
        if limit is not None:
            todo = todo[:limit]
        sizes = self._page_sizes(cfg)
        start = panel.started_at or self._now()
        upper = start - timedelta(days=int(cfg["backfill_days"]))
        since = datetime.fromisoformat(str(cfg.get("ext_backfill_since", EXT_BACKFILL_SINCE)))
        cap = int(cfg.get("ext_backfill_daily_pages", EXT_BACKFILL_DAILY_PAGES))
        for community in todo:
            cid = community.community_vk_id
            if cid not in self._ext_progress:
                # начать примерно с конца 90-дневной части стены (с перекрытием в страницу)
                async with self._sessions() as session:
                    seen = await session.scalar(
                        select(func.count())
                        .select_from(PanelPost)
                        .where(
                            PanelPost.panel_id == panel.id,
                            PanelPost.community_vk_id == cid,
                            PanelPost.published_at >= upper,
                        )
                    )
                self._ext_progress[cid] = _BackfillProgress(offset=max(0, int(seen or 0) - 100))
            progress = self._ext_progress[cid]
            done = False
            while not done:
                day = self._now().astimezone(MSK).date().isoformat()
                if self._ext_pages.get(day, 0) >= cap:
                    stats["capped"] = 1
                    logger.info("panel_ext_backfill_daily_cap day=%s pages=%s", day, cap)
                    return stats
                try:
                    result, size = await self._backfill_page(cid, progress.offset, sizes)
                except (VKMethodUnavailable, VKTokensUnavailable):
                    raise
                except Exception as exc:
                    failures = self._ext_failures.get(cid, 0) + 1
                    self._ext_failures[cid] = failures
                    stats["failed"] += 1
                    logger.warning(
                        "panel_ext_backfill_failed community=%s offset=%s failures=%s error=%s",
                        cid,
                        progress.offset,
                        failures,
                        type(exc).__name__,
                    )
                    if failures >= BACKFILL_MAX_FAILURES:
                        self._ext_failures[cid] = 0
                        if cid in self._ext_deferred:
                            self._ext_deferred.remove(cid)
                        self._ext_deferred.append(cid)
                        stats["deferred"] += 1
                        logger.warning("panel_ext_backfill_deferred community=%s", cid)
                    break
                self._ext_failures.pop(cid, None)
                self._ext_pages[day] = self._ext_pages.get(day, 0) + 1
                stats["pages"] += 1
                items = (result or {}).get("items") or [] if isinstance(result, dict) else []
                keep = [
                    p
                    for p in items
                    if p.get("date")
                    and since <= datetime.fromtimestamp(int(p["date"]), UTC) < upper
                    and has_video_or_clip(p)
                ]
                if keep:
                    async with self._sessions() as session:
                        new, _ = await self._store_posts(
                            session,
                            panel,
                            cid,
                            keep,
                            origin=EXT_ORIGIN,
                            now=self._now(),
                            snapshot_source="backfill",
                        )
                        await session.commit()
                    progress.new += new
                non_pinned = [p for p in items if not p.get("is_pinned")]
                if not items or (
                    non_pinned and datetime.fromtimestamp(int(non_pinned[-1]["date"]), UTC) < since
                ):
                    done = True
                progress.offset += size
            if not done:
                continue
            async with self._sessions() as session:
                await session.execute(
                    update(PanelCommunity)
                    .where(
                        PanelCommunity.panel_id == panel.id,
                        PanelCommunity.community_vk_id == cid,
                    )
                    .values(ext_backfill_done_at=self._now())
                )
                await session.commit()
            self._ext_progress.pop(cid, None)
            if cid in self._ext_deferred:
                self._ext_deferred.remove(cid)
            stats["communities"] += 1
            stats["posts"] += progress.new
            logger.info(
                "panel_ext_backfill community=%s posts=%s offset=%s",
                cid,
                progress.new,
                progress.offset,
            )
        return stats

    # ------------------------------------------------------------------ живая панель
    async def poll_due(self, panel: Panel) -> tuple[int, int]:
        now = self._now()
        async with self._sessions() as session:
            due = list(
                (
                    await session.scalars(
                        select(PanelCommunity).where(
                            PanelCommunity.panel_id == panel.id,
                            PanelCommunity.excluded_at.is_(None),
                            PanelCommunity.next_poll_at <= now,
                        )
                    )
                ).all()
            )
        if not due:
            return 0, 0
        latest = await self._vk.wall_latest(
            [c.community_vk_id for c in due], int(panel.config["poll_count"])
        )
        new_total = 0
        now = self._now()
        async with self._sessions() as session:
            for community in due:
                result = latest.get(community.community_vk_id)
                values: dict[str, Any] = {
                    "last_poll_at": now,
                    "next_poll_at": now + timedelta(seconds=community.poll_interval_seconds),
                }
                if result is None:
                    values["excluded_reason"] = "wall.get failed"
                else:
                    new, max_id = await self._store_posts(
                        session,
                        panel,
                        community.community_vk_id,
                        result.get("items") or [],
                        origin="live",
                        now=now,
                        snapshot_source="poll",
                    )
                    new_total += new
                    if max_id is not None:
                        values["last_seen_post_id"] = func.greatest(
                            func.coalesce(PanelCommunity.last_seen_post_id, 0), max_id
                        )
                    values["excluded_reason"] = None
                await session.execute(
                    update(PanelCommunity)
                    .where(
                        PanelCommunity.panel_id == panel.id,
                        PanelCommunity.community_vk_id == community.community_vk_id,
                    )
                    .values(**values)
                )
            await session.commit()
        return len(due), new_total

    async def measure_due(self, panel: Panel) -> tuple[int, int, int]:
        cfg = panel.config
        targets: list[int] = cfg["targets_h"]
        now = self._now()
        async with self._sessions() as session:
            rows = (
                await session.execute(
                    select(
                        PanelPost.vk_owner_id,
                        PanelPost.vk_post_id,
                        PanelPost.published_at,
                        PanelPost.next_target_index,
                    ).where(
                        PanelPost.panel_id == panel.id,
                        PanelPost.next_due_at <= now,
                        PanelPost.deleted_at.is_(None),
                    )
                )
            ).all()
        if not rows:
            return 0, 0, 0
        keys = [f"{owner}_{post}" for owner, post, _, _ in rows]
        counters = await self._vk.posts_counters(keys)
        by_key = {
            (int(r["owner_id"]), int(r["id"])): r for r in counters if r.get("id") is not None
        }
        now = self._now()
        measured = missed = deleted = 0
        async with self._sessions() as session:
            for owner, post_id, published, idx in rows:
                values: dict[str, Any]
                found = by_key.get((int(owner), int(post_id)))
                if found is None:
                    values = {"deleted_at": now, "next_due_at": None, "next_target_index": None}
                    deleted += 1
                else:
                    target = targets[idx] if idx is not None and idx < len(targets) else None
                    on_time = target is not None and now <= published + timedelta(
                        hours=target
                    ) + self._tolerance(cfg, target)
                    session.add(
                        _snapshot(
                            panel.id,
                            owner,
                            post_id,
                            found,
                            now,
                            published,
                            target if on_time else None,
                            "getbyid",
                        )
                    )
                    measured += 1
                    late = 0 if on_time else 1
                    nxt, due, skipped = self._target_plan(cfg, published, now, (idx or 0) + 1)
                    missed += late + skipped
                    values = {
                        "next_target_index": nxt,
                        "next_due_at": due,
                        "missed_targets": PanelPost.missed_targets + late + skipped,
                    }
                await session.execute(
                    update(PanelPost)
                    .where(
                        PanelPost.panel_id == panel.id,
                        PanelPost.vk_owner_id == owner,
                        PanelPost.vk_post_id == post_id,
                    )
                    .values(**values)
                )
            await session.commit()
        return measured, missed, deleted

    async def members_daily(self, panel: Panel) -> int:
        every = timedelta(hours=float(panel.config["members_every_hours"]))
        now = self._now()
        async with self._sessions() as session:
            last = await session.scalar(
                select(func.max(PanelCommunitySnapshot.measured_at)).where(
                    PanelCommunitySnapshot.panel_id == panel.id
                )
            )
            if last is not None and now - last < every:
                return 0
            ids = list(
                (
                    await session.scalars(
                        select(PanelCommunity.community_vk_id).where(
                            PanelCommunity.panel_id == panel.id
                        )
                    )
                ).all()
            )
        groups = await self._vk.groups_info(ids)
        async with self._sessions() as session:
            for g in groups:
                session.add(
                    PanelCommunitySnapshot(
                        panel_id=panel.id,
                        community_vk_id=int(g["id"]),
                        measured_at=now,
                        members_count=g.get("members_count"),
                        is_closed=g.get("is_closed"),
                        deactivated=g.get("deactivated"),
                    )
                )
            await session.commit()
        return len(groups)

    async def tick(self, panel_name: str) -> TickResult:
        panel = await self.panel_by_name(panel_name)
        if panel.status != "active":
            raise RuntimeError(f"панель {panel_name!r} не активна (status={panel.status})")
        cfg = panel.config
        now = self._now()
        stop_at = _parse_dt(cfg.get("stop_at"))
        if stop_at is not None and now >= stop_at:
            path = await self.export(panel_name, "final")
            await self.set_status(panel.id, "frozen")
            return TickResult(0, 0, 0, 0, 0, 0, path, True)
        polled, new = await self.poll_due(panel)
        measured, missed, deleted = await self.measure_due(panel)
        members = await self.members_daily(panel)
        exported = None
        interim_at = _parse_dt(cfg.get("interim_export_at"))
        if (
            interim_at is not None
            and now >= interim_at
            and "interim" not in cfg.get("exports_done", [])
        ):
            exported = await self.export(panel_name, "interim")
        return TickResult(polled, new, measured, missed, deleted, members, exported, False)

    # ------------------------------------------------------------------ выгрузка
    async def export(self, panel_name: str, label: str) -> str:
        """Parquet-выгрузка всех таблиц панели в export_dir/<label>/<UTC-время>/."""
        import pandas as pd  # type: ignore[import-untyped]

        panel = await self.panel_by_name(panel_name)
        stamp = self._now().strftime("%Y%m%dT%H%M%SZ")
        dest = self._export_dir / label / stamp
        dest.mkdir(parents=True, exist_ok=True)
        tables: dict[str, Any] = {
            "communities": PanelCommunity,
            "posts": PanelPost,
            "snapshots": PanelPostSnapshot,
            "media": PanelMedia,
            "community_snapshots": PanelCommunitySnapshot,
        }
        counts: dict[str, int] = {}
        async with self._sessions() as session:
            for name, model in tables.items():
                result = await session.execute(select(model).where(model.panel_id == panel.id))
                objects: list[Any] = list(result.scalars().all())
                records = [
                    {c.name: _plain(getattr(obj, c.key)) for c in model.__table__.columns}
                    for obj in objects
                ]
                frame = pd.DataFrame.from_records(
                    records, columns=[c.name for c in model.__table__.columns]
                )
                frame.to_parquet(dest / f"{name}.parquet", index=False)
                counts[name] = len(frame)
            done = list(panel.config.get("exports_done", []))
            if label not in done:
                done.append(label)
            await session.execute(
                update(Panel)
                .where(Panel.id == panel.id)
                .values(config={**panel.config, "exports_done": done})
            )
            await session.commit()
        manifest = {
            "panel": panel_name,
            "label": label,
            "exported_at": self._now().isoformat(),
            "rows": counts,
            "config": panel.config,
        }
        (dest / "manifest.json").write_text(
            json.dumps(manifest, ensure_ascii=False, indent=1, default=str)
        )
        logger.info("panel_export label=%s dest=%s rows=%s", label, dest, counts)
        return str(dest)


def _snapshot(
    panel_id: uuid.UUID,
    owner: int,
    post_id: int,
    p: dict[str, Any],
    now: datetime,
    published: datetime,
    target: int | None,
    source: str,
) -> PanelPostSnapshot:
    def count(value: Any) -> int | None:
        if isinstance(value, dict):
            raw = value.get("count")
            return int(raw) if isinstance(raw, int) else None
        return int(value) if isinstance(value, int) else None

    return PanelPostSnapshot(
        panel_id=panel_id,
        vk_owner_id=owner,
        vk_post_id=post_id,
        measured_at=now,
        age_seconds=max(0, int((now - published).total_seconds())),
        target_age_h=target,
        views=count(p.get("views")),
        likes=count(p.get("likes")),
        reposts=count(p.get("reposts")),
        comments=count(p.get("comments")),
        source=source,
    )


def _parse_dt(value: Any) -> datetime | None:
    if not value:
        return None
    parsed = datetime.fromisoformat(str(value))
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def _plain(value: Any) -> Any:
    if isinstance(value, uuid.UUID):
        return str(value)
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False)
    return value


__all__ = ["DEFAULT_CONFIG", "PanelService", "TickResult", "normalize_text", "text_norm_hash"]

"""Unit-тесты общего шлагбаума CDN и его использования в MediaResolver."""

from __future__ import annotations

import json
import time
from pathlib import Path

import httpx
import pytest

from vk_collector.ml.cdn_gate import (
    CdnGate,
    CdnGateNotConfigured,
    CdnGateStopped,
    CdnRateLimited,
)
from vk_collector.ml.contracts import MultimodalPost, PostAttachmentItem
from vk_collector.ml.media_resolver import MediaResolver


def _gate(tmp_path: Path, pause: tuple[float, float] = (0.0, 0.0)) -> CdnGate:
    return CdnGate(tmp_path, who="test", pause_seconds=pause)


def test_gate_waits_for_pause_between_downloads(tmp_path: Path) -> None:
    gate = _gate(tmp_path, pause=(0.3, 0.3))
    gate.acquire()
    gate.release(200, host="sun9-1.userapi.com")
    t0 = time.monotonic()
    gate.acquire()
    waited = time.monotonic() - t0
    gate.release(200)
    assert waited >= 0.25


def test_gate_429_sets_stop_flag_and_blocks_everyone(tmp_path: Path) -> None:
    gate = _gate(tmp_path)
    gate.acquire()
    gate.release(429, host="sun9-1.userapi.com")
    assert (tmp_path / "STOP_429").exists()
    assert json.loads((tmp_path / "STOP_429").read_text())["host"] == "sun9-1.userapi.com"
    other = CdnGate(tmp_path, who="other", pause_seconds=(0.0, 0.0))
    with pytest.raises(CdnGateStopped):
        other.acquire()
    # замок отпущен даже после отказа
    (tmp_path / "STOP_429").unlink()
    other.acquire()
    other.release(200)


def test_gate_requires_configured_directory(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.delenv("MEDIA_CDN_GATE_DIR", raising=False)
    with pytest.raises(CdnGateNotConfigured):
        CdnGate.from_env()
    with pytest.raises(CdnGateNotConfigured):
        CdnGate(tmp_path / "missing")


def _photo_post() -> tuple[MultimodalPost, PostAttachmentItem]:
    att = PostAttachmentItem(
        position=0,
        attachment_type="photo",
        vk_owner_id=-50,
        vk_attachment_id=1,
        attachment_metadata={
            "sizes": [
                {
                    "type": "y",
                    "url": "https://sun9-1.userapi.com/a.jpg",
                    "width": 800,
                    "height": 600,
                }
            ]
        },
    )
    post = MultimodalPost(
        post_id=1,
        group_id=50,
        community_vk_id=50,
        subject="test",
        published_at="2026-08-01T00:00:00Z",
        attachments=[att],
    )
    return post, att


@pytest.mark.asyncio
async def test_resolver_429_stops_without_retry(tmp_path: Path) -> None:
    calls: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(str(request.url))
        return httpx.Response(429)

    gate_dir = tmp_path / "gate"
    gate_dir.mkdir()
    resolver = MediaResolver(
        cache_dir=tmp_path / "cache",
        gate=_gate(gate_dir),
        http_client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
        max_concurrency=3,
    )
    post, att = _photo_post()
    with pytest.raises(CdnRateLimited):
        await resolver.resolve_image_async(post, att)
    assert len(calls) == 1
    assert resolver.max_concurrency == 1
    assert (gate_dir / "STOP_429").exists()
    with pytest.raises(CdnGateStopped):
        await resolver.download_file_async("https://sun9-1.userapi.com/b.jpg", tmp_path / "b.jpg")
    assert len(calls) == 1
    await resolver.aclose()


@pytest.mark.asyncio
async def test_resolver_downloads_through_gate(tmp_path: Path) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=b"jpeg-bytes")

    gate_dir = tmp_path / "gate"
    gate_dir.mkdir()
    resolver = MediaResolver(
        cache_dir=tmp_path / "cache",
        gate=_gate(gate_dir),
        http_client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
    )
    out = await resolver.download_file_async("https://sun9-1.userapi.com/a.jpg", tmp_path / "a.jpg")
    assert out.read_bytes() == b"jpeg-bytes"
    log = [json.loads(x) for x in (gate_dir / "gate_log.jsonl").read_text().splitlines()]
    assert log[-1]["status"] == 200 and log[-1]["host"] == "sun9-1.userapi.com"
    assert "a.jpg" not in (gate_dir / "gate_log.jsonl").read_text()  # полный URL не пишется
    await resolver.aclose()


@pytest.mark.asyncio
async def test_resolver_refuses_without_gate(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.delenv("MEDIA_CDN_GATE_DIR", raising=False)
    resolver = MediaResolver(cache_dir=tmp_path)
    with pytest.raises(CdnGateNotConfigured):
        await resolver.download_file_async("https://sun9-1.userapi.com/a.jpg", tmp_path / "a.jpg")

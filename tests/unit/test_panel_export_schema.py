from __future__ import annotations

import pyarrow as pa  # type: ignore[import-untyped]
import pytest

from vk_collector.database.models import (
    PanelCommunity,
    PanelCommunitySnapshot,
    PanelMedia,
    PanelPost,
    PanelPostSnapshot,
)
from vk_collector.panel.service import EXPORT_BATCH_ROWS, _arrow_type, _plain


@pytest.mark.parametrize(
    "model", [PanelCommunity, PanelPost, PanelPostSnapshot, PanelMedia, PanelCommunitySnapshot]
)
def test_every_panel_column_has_fixed_arrow_type(model: type) -> None:
    schema = pa.schema([pa.field(c.name, _arrow_type(c.type)) for c in model.__table__.columns])
    assert len(schema) == len(model.__table__.columns)
    # пустая порция и порция с None должны собираться в ту же схему (схема не выводится из данных)
    empty = pa.Table.from_pydict({f.name: [] for f in schema}, schema=schema)
    nulls = pa.Table.from_pydict({f.name: [None] for f in schema}, schema=schema)
    assert empty.schema == nulls.schema == schema


def test_arrow_types_for_post_columns() -> None:
    cols = {c.name: _arrow_type(c.type) for c in PanelPost.__table__.columns}
    assert cols["vk_post_id"] == pa.int64()
    assert cols["is_repost"] == pa.bool_()
    assert cols["published_at"] == pa.timestamp("us", tz="UTC")
    assert cols["text"] == pa.string()
    assert cols["attachments"] == pa.string()  # JSONB -> JSON-строка через _plain
    assert cols["panel_id"] == pa.string()  # UUID -> строка через _plain


def test_plain_serializes_json_and_uuid() -> None:
    import uuid

    assert _plain({"a": "б"}) == '{"a": "б"}'
    assert _plain(uuid.UUID(int=1)) == "00000000-0000-0000-0000-000000000001"
    assert EXPORT_BATCH_ROWS >= 1

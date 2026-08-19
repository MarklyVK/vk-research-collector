"""Модуль пакетного сохранения и импорта вычисленных эмбеддингов в PostgreSQL."""

from __future__ import annotations

import json
import logging
from collections.abc import Sequence
from pathlib import Path

import numpy as np
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from vk_collector.database.models import PostEmbedding
from vk_collector.ml.contracts import MultimodalPost

logger = logging.getLogger(__name__)


async def save_post_embeddings_batch(
    session: AsyncSession,
    embeddings: np.ndarray,
    posts: Sequence[MultimodalPost],
    run_id: str,
    model_name: str,
) -> int:
    """Сохранить батч эмбеддингов в таблицу post_embeddings с защитой от дублирования."""
    if len(embeddings) == 0 or len(posts) == 0:
        return 0

    if len(embeddings) != len(posts):
        raise ValueError(
            f"Несоответствие размерностей: {len(embeddings)} векторов и {len(posts)} постов"
        )

    records = []
    dim = int(embeddings.shape[1])

    for emb, post in zip(embeddings, posts, strict=True):
        records.append(
            {
                "post_id": post.post_id,
                "run_id": run_id,
                "model_name": model_name,
                "embedding_dim": dim,
                "embedding_vector": emb.tolist(),
                "modality_profile": post.modality_profile.value,
            }
        )

    stmt = insert(PostEmbedding).values(records)
    stmt = stmt.on_conflict_do_update(
        index_elements=[PostEmbedding.post_id],
        set_={
            "run_id": stmt.excluded.run_id,
            "model_name": stmt.excluded.model_name,
            "embedding_dim": stmt.excluded.embedding_dim,
            "embedding_vector": stmt.excluded.embedding_vector,
            "modality_profile": stmt.excluded.modality_profile,
            "updated_at": stmt.excluded.created_at,
        },
    )

    await session.execute(stmt)
    await session.commit()
    return len(records)


async def import_embeddings_bundle(
    session: AsyncSession,
    bundle_dir: Path,
    batch_size: int = 1000,
) -> int:
    """Импортировать полный бандл артефактов векторизации в PostgreSQL блоками по batch_size."""
    embeddings_file = bundle_dir / "embeddings.npy"
    metadata_file = bundle_dir / "metadata.json"
    config_file = bundle_dir / "run_config.json"

    if not embeddings_file.exists():
        raise FileNotFoundError(f"Файл эмбеддингов не найден: {embeddings_file}")
    if not metadata_file.exists():
        raise FileNotFoundError(f"Файл метаданных не найден: {metadata_file}")

    embeddings = np.load(embeddings_file)
    with metadata_file.open("r", encoding="utf-8") as f:
        meta_items = json.load(f)

    posts = [MultimodalPost.model_validate(item) for item in meta_items]

    run_id = "unknown_run"
    model_name = "unknown_model"
    if config_file.exists():
        with config_file.open("r", encoding="utf-8") as f:
            cfg = json.load(f)
            run_id = cfg.get("run_id", run_id)
            model_name = cfg.get("model_name", model_name)

    total_saved = 0
    total_items = len(posts)

    for i in range(0, total_items, batch_size):
        chunk_embeddings = embeddings[i : i + batch_size]
        chunk_posts = posts[i : i + batch_size]
        saved = await save_post_embeddings_batch(
            session=session,
            embeddings=chunk_embeddings,
            posts=chunk_posts,
            run_id=run_id,
            model_name=model_name,
        )
        total_saved += saved
        logger.info("Сохранено %d / %d эмбеддингов в PostgreSQL", total_saved, total_items)

    return total_saved

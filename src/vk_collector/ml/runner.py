"""Исполнитель векторизации с двухуровневым сохранением (GPU micro-batch + commit в БД)."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from pathlib import Path

import numpy as np
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from vk_collector.ml.contracts import (
    EmbeddingFailureRecord,
    MultimodalPost,
)
from vk_collector.ml.dataset import CompanyPostsDataset, MultimodalBatchItem
from vk_collector.ml.encoders.base import BaseMultimodalEncoder
from vk_collector.ml.import_embeddings import save_post_embeddings_batch

logger = logging.getLogger(__name__)


class EmbeddingRunner:
    """Управляет инференсом энкодера, двухуровневыми чекпоинтами и потоковой записью в БД."""

    def __init__(
        self,
        encoder: BaseMultimodalEncoder,
        batch_size: int = 32,
        checkpoint_dir: Path | None = None,
        quota_gb: float = 20.0,
        db_session_factory: async_sessionmaker[AsyncSession] | None = None,
        db_save_interval: int = 1000,
    ) -> None:
        self.encoder = encoder
        self.batch_size = batch_size
        self.checkpoint_dir = checkpoint_dir or Path("tmp/ml_checkpoints")
        self.quota_gb = quota_gb
        self.db_session_factory = db_session_factory
        self.db_save_interval = db_save_interval
        self.checkpoint_dir.mkdir(parents=True, exist_ok=True)

    def run(
        self,
        dataset: CompanyPostsDataset,
        *,
        run_id: str = "default_run",
        resume: bool = True,
        progress_callback: Callable[[int, int], None] | None = None,
    ) -> tuple[np.ndarray, list[MultimodalPost], list[EmbeddingFailureRecord]]:
        """Синхронная обертка для запуска векторизации."""
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None

        if loop and loop.is_running():
            # Запуск внутри уже запущенного цикла событий (например, в Jupyter Notebook)
            import nest_asyncio  # type: ignore[import-not-found]

            nest_asyncio.apply()
            return loop.run_until_complete(
                self.run_async(
                    dataset,
                    run_id=run_id,
                    resume=resume,
                    progress_callback=progress_callback,
                )
            )
        else:
            return asyncio.run(
                self.run_async(
                    dataset,
                    run_id=run_id,
                    resume=resume,
                    progress_callback=progress_callback,
                )
            )

    async def run_async(
        self,
        dataset: CompanyPostsDataset,
        *,
        run_id: str = "default_run",
        resume: bool = True,
        progress_callback: Callable[[int, int], None] | None = None,
    ) -> tuple[np.ndarray, list[MultimodalPost], list[EmbeddingFailureRecord]]:
        """Асинхронный запуск векторизации с контрольными точками и записью в БД."""
        if not self.encoder.is_loaded:
            self.encoder.load()

        run_ckpt_dir = self.checkpoint_dir / run_id
        run_ckpt_dir.mkdir(parents=True, exist_ok=True)

        completed_ids_file = run_ckpt_dir / "completed_ids.txt"
        completed_ids: set[int] = set()
        if resume and completed_ids_file.exists():
            with completed_ids_file.open("r", encoding="utf-8") as f:
                completed_ids = {int(line.strip()) for line in f if line.strip().isdigit()}

        total_posts = len(dataset)
        successful_posts: list[MultimodalPost] = []
        embeddings_chunks: list[np.ndarray] = []
        failures: list[EmbeddingFailureRecord] = []

        # Загрузка ранее сохраненных чанков при resume
        if resume:
            chunk_files = sorted(run_ckpt_dir.glob("chunk_*.npy"))
            for cf in chunk_files:
                try:
                    chunk = np.load(cf)
                    embeddings_chunks.append(chunk)
                except Exception as e:
                    logger.warning("Не удалось прочитать чанк %s: %s", cf, e)

        current_batch_items: list[MultimodalBatchItem] = []
        current_batch_posts: list[MultimodalPost] = []
        db_buffer_embs: list[np.ndarray] = []
        db_buffer_posts: list[MultimodalPost] = []
        processed_count = len(completed_ids)

        for i in range(total_posts):
            post = dataset.posts[i]
            if post.post_id in completed_ids:
                successful_posts.append(post)
                continue

            try:
                batch_item = dataset[i]
                current_batch_items.append(batch_item)
                current_batch_posts.append(post)
            except Exception as e:
                failures.append(
                    EmbeddingFailureRecord(
                        post_id=post.post_id,
                        group_id=post.group_id,
                        stage="data_loading",
                        error_type=type(e).__name__,
                        error_message=str(e),
                    )
                )
                continue

            if len(current_batch_items) >= self.batch_size:
                new_embs = self._process_batch(
                    current_batch_items,
                    current_batch_posts,
                    embeddings_chunks,
                    successful_posts,
                    failures,
                    run_ckpt_dir,
                    completed_ids_file,
                )
                if new_embs is not None and len(new_embs) > 0:
                    db_buffer_embs.append(new_embs)
                    db_buffer_posts.extend(current_batch_posts)

                # Проверка накопления для коммита в БД каждые db_save_interval постов
                if self.db_session_factory and len(db_buffer_posts) >= self.db_save_interval:
                    await self._flush_to_db(db_buffer_embs, db_buffer_posts, run_id=run_id)
                    db_buffer_embs = []
                    db_buffer_posts = []

                processed_count += len(current_batch_items)
                if progress_callback:
                    progress_callback(processed_count, total_posts)
                current_batch_items = []
                current_batch_posts = []

        # Обработка последнего остаточного батча
        if current_batch_items:
            new_embs = self._process_batch(
                current_batch_items,
                current_batch_posts,
                embeddings_chunks,
                successful_posts,
                failures,
                run_ckpt_dir,
                completed_ids_file,
            )
            if new_embs is not None and len(new_embs) > 0:
                db_buffer_embs.append(new_embs)
                db_buffer_posts.extend(current_batch_posts)

            processed_count += len(current_batch_items)
            if progress_callback:
                progress_callback(processed_count, total_posts)

        # Сброс оставшихся векторов в БД
        if self.db_session_factory and db_buffer_posts:
            await self._flush_to_db(db_buffer_embs, db_buffer_posts, run_id=run_id)

        # Сборка единой матрицы эмбеддингов E^(C)
        if embeddings_chunks:
            full_matrix = np.vstack(embeddings_chunks).astype(np.float32)
        else:
            full_matrix = np.empty((0, self.encoder.embedding_dim), dtype=np.float32)

        return full_matrix, successful_posts, failures

    def _process_batch(
        self,
        batch_items: list[MultimodalBatchItem],
        batch_posts: list[MultimodalPost],
        embeddings_chunks: list[np.ndarray],
        successful_posts: list[MultimodalPost],
        failures: list[EmbeddingFailureRecord],
        run_ckpt_dir: Path,
        completed_ids_file: Path,
    ) -> np.ndarray | None:
        try:
            batch_emb = self.encoder.encode_batch(batch_items)
            chunk_idx = len(embeddings_chunks)
            chunk_path = run_ckpt_dir / f"chunk_{chunk_idx:06d}.npy"
            np.save(chunk_path, batch_emb)
            embeddings_chunks.append(batch_emb)
            successful_posts.extend(batch_posts)

            # Сохранение completed IDs
            with completed_ids_file.open("a", encoding="utf-8") as f:
                for p in batch_posts:
                    f.write(f"{p.post_id}\n")
            return batch_emb
        except Exception:
            # При сбое всего батча пробуем обработать по одному
            single_embs = []
            for item, post in zip(batch_items, batch_posts, strict=False):
                try:
                    single_emb = self.encoder.encode_batch([item])
                    chunk_idx = len(embeddings_chunks)
                    chunk_path = run_ckpt_dir / f"chunk_{chunk_idx:06d}.npy"
                    np.save(chunk_path, single_emb)
                    embeddings_chunks.append(single_emb)
                    successful_posts.append(post)
                    single_embs.append(single_emb)

                    with completed_ids_file.open("a", encoding="utf-8") as f:
                        f.write(f"{post.post_id}\n")
                except Exception as item_err:
                    failures.append(
                        EmbeddingFailureRecord(
                            post_id=post.post_id,
                            group_id=post.group_id,
                            stage="encode_batch",
                            error_type=type(item_err).__name__,
                            error_message=str(item_err),
                        )
                    )
            if single_embs:
                return np.vstack(single_embs).astype(np.float32)
            return None

    async def _flush_to_db(
        self,
        buffer_embs: list[np.ndarray],
        buffer_posts: list[MultimodalPost],
        run_id: str,
    ) -> None:
        if not self.db_session_factory or not buffer_posts or not buffer_embs:
            return

        combined_embs = np.vstack(buffer_embs).astype(np.float32)
        try:
            async with self.db_session_factory() as session:
                saved = await save_post_embeddings_batch(
                    session=session,
                    embeddings=combined_embs,
                    posts=buffer_posts,
                    run_id=run_id,
                    model_name=self.encoder.model_name,
                )
                logger.info("Успешно сохранено %d векторов в PostgreSQL (run_id=%s)", saved, run_id)
        except Exception as e:
            logger.error("Ошибка при сохранении векторов в PostgreSQL: %s", e)

"""Детерминированный мок-энкодер для unit-тестов, CI и локальной разработки."""

from __future__ import annotations

import hashlib

import numpy as np

from vk_collector.ml.dataset import MultimodalBatchItem
from vk_collector.ml.encoders.base import BaseMultimodalEncoder, l2_normalize


class MockMultimodalEncoder(BaseMultimodalEncoder):
    """Генерирует детерминированные L2-нормализованные эмбеддинги без обращения к GPU."""

    def __init__(
        self,
        model_name: str = "mock-multimodal-encoder-2b",
        embedding_dim: int = 2048,
        device: str = "cpu",
        precision: str = "float32",
    ) -> None:
        super().__init__(
            model_name=model_name,
            embedding_dim=embedding_dim,
            device=device,
            precision=precision,
        )

    def load(self) -> None:
        self.is_loaded = True

    def encode_batch(self, items: list[MultimodalBatchItem]) -> np.ndarray:
        if not items:
            return np.empty((0, self.embedding_dim), dtype=np.float32)

        vectors: list[np.ndarray] = []

        for item in items:
            # Детерминированный сид на основе содержимого
            content_sig = f"{item.subject}_{item.text}_{len(item.images)}_{len(item.video_frames)}"
            seed = int(hashlib.sha256(content_sig.encode()).hexdigest(), 16) % (2**31)
            rng = np.random.RandomState(seed)

            # Генерация базового вектора с семантическим кластерным сдвигом по subject
            subj_seed = int(hashlib.sha256(item.subject.encode()).hexdigest(), 16) % (2**31)
            subj_rng = np.random.RandomState(subj_seed)
            center = subj_rng.normal(loc=0.0, scale=1.0, size=self.embedding_dim)

            vec = center + rng.normal(loc=0.0, scale=0.3, size=self.embedding_dim)

            # Дополнительный семантический вклад от модальностей
            if item.images:
                vec += 0.2 * rng.normal(loc=0.5, scale=0.1, size=self.embedding_dim)
            if item.video_frames:
                vec += 0.2 * rng.normal(loc=-0.5, scale=0.1, size=self.embedding_dim)

            vectors.append(vec)

        raw_matrix = np.vstack(vectors).astype(np.float32)
        return l2_normalize(raw_matrix)

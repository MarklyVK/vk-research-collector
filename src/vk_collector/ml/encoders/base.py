"""Базовый абстрактный интерфейс для мультимодальных энкодеров."""

from __future__ import annotations

from abc import ABC, abstractmethod

import numpy as np

from vk_collector.ml.dataset import MultimodalBatchItem


def l2_normalize(matrix: np.ndarray, eps: float = 1e-12) -> np.ndarray:
    """Выполнить строгую L2-нормализацию строк матрицы эмбеддингов."""
    norms = np.linalg.norm(matrix, axis=1, keepdims=True)
    norms = np.maximum(norms, eps)
    return np.asarray(matrix / norms, dtype=np.float32)


class BaseMultimodalEncoder(ABC):
    """Единый интерфейс для мультимодальных моделей векторизации постов."""

    def __init__(
        self,
        model_name: str,
        embedding_dim: int,
        device: str = "cuda",
        precision: str = "bfloat16",
    ) -> None:
        self.model_name = model_name
        self.embedding_dim = embedding_dim
        self.device = device
        self.precision = precision
        self.is_loaded = False

    @abstractmethod
    def load(self) -> None:
        """Загрузить веса модели и процессор на целевое устройство."""
        ...

    @abstractmethod
    def encode_batch(self, items: list[MultimodalBatchItem]) -> np.ndarray:
        """Векторизовать батч мультимодальных постов.

        Возвращает матрицу эмбеддингов [B, embedding_dim] с единичной L2-нормой строк.
        """
        ...

    def get_memory_usage_gb(self) -> float:
        """Получить текущее потребление VRAM (в ГБ)."""
        try:
            import torch  # type: ignore[import-not-found]

            if torch.cuda.is_available() and "cuda" in self.device:
                return float(torch.cuda.memory_allocated() / (1024**3))
        except Exception:
            pass
        return 0.0

    def is_gpu_quota_respected(self, quota_gb: float = 20.0) -> bool:
        """Проверить непревышение выделенной квоты GPU VRAM (квота статьи ~20 ГБ)."""
        usage = self.get_memory_usage_gb()
        return usage <= quota_gb

"""Адаптер для модели векторизации Jina v5-omni (компактный baseline, d_m = 1024)."""

from __future__ import annotations

from typing import Any

import numpy as np

from vk_collector.ml.dataset import MultimodalBatchItem
from vk_collector.ml.encoders.base import BaseMultimodalEncoder, l2_normalize


class JinaOmniEmbeddingAdapter(BaseMultimodalEncoder):
    """Адаптер для Jina v5-omni (d_m = 1024, 0.5B параметров)."""

    def __init__(
        self,
        model_name: str = "jinaai/jina-embeddings-v5-omni",
        embedding_dim: int = 1024,
        device: str = "cuda",
        precision: str = "bfloat16",
    ) -> None:
        super().__init__(
            model_name=model_name,
            embedding_dim=embedding_dim,
            device=device,
            precision=precision,
        )
        self.model: Any = None

    def load(self) -> None:
        try:
            import torch  # type: ignore[import-not-found]
            from transformers import AutoModel  # type: ignore[import-not-found]

            dtype = torch.bfloat16 if self.precision == "bfloat16" else torch.float16
            self.model = AutoModel.from_pretrained(
                self.model_name,
                torch_dtype=dtype,
                trust_remote_code=True,
                device_map=self.device,
            )
            self.model.eval()
            self.is_loaded = True
        except Exception as e:
            self.is_loaded = False
            raise RuntimeError(
                f"Не удалось загрузить модель {self.model_name} на устройстве {self.device}: {e}"
            ) from e

    def encode_batch(self, items: list[MultimodalBatchItem]) -> np.ndarray:
        if not items:
            return np.empty((0, self.embedding_dim), dtype=np.float32)

        if not self.is_loaded or self.model is None:
            raise RuntimeError("Модель не загружена. Сначала вызовите .load()")

        # Вызов встроенного мультимодального encode-метода jina
        texts = [it.text.strip() if it.text else "Компания" for it in items]
        raw_embeddings = self.model.encode(texts)
        if not isinstance(raw_embeddings, np.ndarray):
            raw_embeddings = np.array(raw_embeddings)

        return l2_normalize(raw_embeddings)

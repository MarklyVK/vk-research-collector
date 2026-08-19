"""Адаптер для модели векторизации Qwen3-VL-Embedding-2B (и Qwen2.5-VL)."""

from __future__ import annotations

from typing import Any

import numpy as np
from PIL import Image

from vk_collector.ml.dataset import MultimodalBatchItem
from vk_collector.ml.encoders.base import BaseMultimodalEncoder, l2_normalize


class QwenVLEmbeddingAdapter(BaseMultimodalEncoder):
    """Адаптер для Qwen3-VL-Embedding-2B (d_m = 2048, единый мультимодальный граф)."""

    def __init__(
        self,
        model_name: str = "Qwen/Qwen3-VL-Embedding-2B",
        embedding_dim: int = 2048,
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
        self.processor: Any = None

    def load(self) -> None:
        """Загрузить модель Qwen3-VL и процессор через Hugging Face transformers."""
        try:
            import torch  # type: ignore[import-not-found]
            from transformers import AutoModel, AutoProcessor  # type: ignore[import-not-found]

            dtype = torch.bfloat16 if self.precision == "bfloat16" else torch.float16
            self.processor = AutoProcessor.from_pretrained(self.model_name, trust_remote_code=True)
            self.model = AutoModel.from_pretrained(
                self.model_name,
                torch_dtype=dtype,
                trust_remote_code=True,
                device_map=self.device,
            )
            self.model.eval()
            self.is_loaded = True
        except Exception as e:
            # Если библиотеки или веса недоступны в текущем окружении
            # (например, локальный dev без GPU)
            self.is_loaded = False
            raise RuntimeError(
                f"Не удалось загрузить модель {self.model_name} на устройстве {self.device}: {e}"
            ) from e

    def encode_batch(self, items: list[MultimodalBatchItem]) -> np.ndarray:
        if not items:
            return np.empty((0, self.embedding_dim), dtype=np.float32)

        if not self.is_loaded or self.model is None or self.processor is None:
            raise RuntimeError("Модель не загружена. Сначала вызовите .load()")

        import torch

        embeddings: list[np.ndarray] = []

        with torch.no_grad():
            for item in items:
                # Подготовка мультимодального контента для единого графа трансформера
                text_content = item.text.strip() if item.text else "Компания"
                images_list = [Image.fromarray(img) for img in item.images]
                # Кадры видео обрабатываются как последовательность изображений
                for vf in item.video_frames:
                    images_list.append(Image.fromarray(vf))

                if images_list:
                    inputs = self.processor(
                        text=[text_content],
                        images=images_list,
                        return_tensors="pt",
                        padding=True,
                    ).to(self.device)
                else:
                    inputs = self.processor(
                        text=[text_content],
                        return_tensors="pt",
                        padding=True,
                    ).to(self.device)

                outputs = self.model(**inputs)
                # Mean pooling по последнему скрытому слою или извлечение pooler_output
                if hasattr(outputs, "last_hidden_state"):
                    hidden = outputs.last_hidden_state
                    mask = inputs.get("attention_mask")
                    if mask is not None:
                        expanded_mask = mask.unsqueeze(-1).expand(hidden.size()).float()
                        sum_embeddings = torch.sum(hidden * expanded_mask, dim=1)
                        sum_mask = torch.clamp(expanded_mask.sum(dim=1), min=1e-9)
                        pooled = sum_embeddings / sum_mask
                    else:
                        pooled = torch.mean(hidden, dim=1)
                elif hasattr(outputs, "pooler_output"):
                    pooled = outputs.pooler_output
                else:
                    pooled = outputs[0][:, 0, :]

                vec = pooled.squeeze(0).cpu().to(torch.float32).numpy()
                embeddings.append(vec)

        raw_matrix = np.vstack(embeddings)
        return l2_normalize(raw_matrix)

"""Модульная система адаптеров мультимодальных энкодеров."""

from __future__ import annotations

from vk_collector.ml.encoders.base import BaseMultimodalEncoder
from vk_collector.ml.encoders.jina_omni import JinaOmniEmbeddingAdapter
from vk_collector.ml.encoders.mock_encoder import MockMultimodalEncoder
from vk_collector.ml.encoders.qwen_vl import QwenVLEmbeddingAdapter

__all__ = [
    "BaseMultimodalEncoder",
    "JinaOmniEmbeddingAdapter",
    "MockMultimodalEncoder",
    "QwenVLEmbeddingAdapter",
]

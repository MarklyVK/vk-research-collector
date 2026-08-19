"""Унифицированный бенчмарк для сравнительной оценки мультимодальных энкодеров."""

from __future__ import annotations

import time
from typing import Any

import numpy as np

from vk_collector.ml.dataset import MultimodalBatchItem
from vk_collector.ml.encoders.base import BaseMultimodalEncoder


class MultimodalEncoderBenchmark:
    """Сравнительное тестирование кандидатов-энкодеров на едином наборе данных."""

    def __init__(self, encoders: list[BaseMultimodalEncoder]) -> None:
        self.encoders = encoders

    def run_benchmark(
        self,
        benchmark_items: list[MultimodalBatchItem],
        quota_gb: float = 20.0,
    ) -> list[dict[str, Any]]:
        results: list[dict[str, Any]] = []

        for enc in self.encoders:
            enc_name = enc.model_name
            try:
                if not enc.is_loaded:
                    enc.load()

                start_time = time.perf_counter()
                embeddings = enc.encode_batch(benchmark_items)
                elapsed = time.perf_counter() - start_time

                items_count = len(benchmark_items)
                throughput = items_count / max(1e-5, elapsed)
                vram_gb = enc.get_memory_usage_gb()
                quota_ok = enc.is_gpu_quota_respected(quota_gb)

                # Проверка строгой L2-нормы
                norms = np.linalg.norm(embeddings, axis=1)
                norm_errors = float(np.max(np.abs(norms - 1.0)))

                results.append(
                    {
                        "model_name": enc_name,
                        "embedding_dim": enc.embedding_dim,
                        "status": "success",
                        "items_processed": items_count,
                        "elapsed_seconds": round(elapsed, 4),
                        "throughput_items_per_sec": round(throughput, 2),
                        "vram_allocated_gb": round(vram_gb, 3),
                        "quota_respected": quota_ok,
                        "max_l2_norm_deviation": round(norm_errors, 6),
                    }
                )
            except Exception as e:
                results.append(
                    {
                        "model_name": enc_name,
                        "embedding_dim": enc.embedding_dim,
                        "status": "failed",
                        "error": str(e),
                    }
                )

        return results

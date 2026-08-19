"""Формирование и сохранение самодостаточного бандла артефактов векторизации."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd  # type: ignore[import-untyped]

from vk_collector.ml.contracts import (
    EmbeddingFailureRecord,
    EmbeddingQualityReport,
    ExecutionProvenance,
    MultimodalPost,
    SamplingReport,
    VideoCalibrationResult,
)


def save_vectorization_bundle(
    output_dir: Path,
    *,
    embeddings: np.ndarray,
    posts: list[MultimodalPost],
    provenance: ExecutionProvenance,
    sampling_report: SamplingReport,
    quality_report: EmbeddingQualityReport,
    failures: list[EmbeddingFailureRecord] | None = None,
    calibration_result: VideoCalibrationResult | None = None,
) -> Path:
    """Сохранить полный комплект артефактов векторизации для последующих этапов кластеризации.

    Сохраняются файлы:
    - embeddings.npy (матрица E^(C))
    - metadata.json / metadata.parquet (метаданные постов)
    - failures.json (журнал сбоев)
    - run_config.json (метаданные запуска и воспроизводимости)
    - sampling_report.json (диагностика выборки)
    - embedding_quality_report.json (диагностика эмбеддингов)
    - video_calibration_report.json (калибровка MAD)
    """
    output_dir.mkdir(parents=True, exist_ok=True)

    # 1. Сохранение матрицы эмбеддингов E^(C)
    np.save(output_dir / "embeddings.npy", embeddings)

    # 2. Сохранение метаданных постов
    meta_rows = [
        {
            "post_id": p.post_id,
            "group_id": p.group_id,
            "community_vk_id": p.community_vk_id,
            "subject": p.subject,
            "published_at": p.published_at.isoformat(),
            "text": p.text,
            "modality_profile": p.modality_profile.value,
            "attachments_count": len(p.attachments),
            "comments_count": p.comments_count,
            "likes_count": p.likes_count,
            "reposts_count": p.reposts_count,
            "views_count": p.views_count,
        }
        for p in posts
    ]
    with (output_dir / "metadata.json").open("w", encoding="utf-8") as f:
        json.dump(meta_rows, f, ensure_ascii=False, indent=2)

    try:
        df_meta = pd.DataFrame(meta_rows)
        df_meta.to_parquet(output_dir / "metadata.parquet", index=False)
    except Exception:
        pass

    # 3. Сохранение failures
    failure_rows = [f.model_dump(mode="json") for f in (failures or [])]
    with (output_dir / "failures.json").open("w", encoding="utf-8") as f:
        json.dump(failure_rows, f, ensure_ascii=False, indent=2)

    # 4. Сохранение отчетов и конфигурации
    with (output_dir / "run_config.json").open("w", encoding="utf-8") as f:
        json.dump(provenance.model_dump(mode="json"), f, ensure_ascii=False, indent=2)

    with (output_dir / "sampling_report.json").open("w", encoding="utf-8") as f:
        json.dump(sampling_report.model_dump(mode="json"), f, ensure_ascii=False, indent=2)

    with (output_dir / "embedding_quality_report.json").open("w", encoding="utf-8") as f:
        json.dump(quality_report.model_dump(mode="json"), f, ensure_ascii=False, indent=2)

    if calibration_result:
        with (output_dir / "video_calibration_report.json").open("w", encoding="utf-8") as f:
            json.dump(calibration_result.model_dump(mode="json"), f, ensure_ascii=False, indent=2)

    return output_dir


def load_vectorization_bundle(run_dir: Path) -> dict[str, Any]:
    """Загрузить полный бандл артефактов из директории запуска."""
    if not run_dir.exists():
        raise FileNotFoundError(f"Каталог запуска не найден: {run_dir}")

    embeddings = np.load(run_dir / "embeddings.npy")

    with (run_dir / "metadata.json").open("r", encoding="utf-8") as f:
        metadata = json.load(f)

    with (run_dir / "run_config.json").open("r", encoding="utf-8") as f:
        run_config = json.load(f)

    with (run_dir / "embedding_quality_report.json").open("r", encoding="utf-8") as f:
        quality_report = json.load(f)

    with (run_dir / "sampling_report.json").open("r", encoding="utf-8") as f:
        sampling_report = json.load(f)

    return {
        "embeddings": embeddings,
        "metadata": metadata,
        "run_config": run_config,
        "quality_report": quality_report,
        "sampling_report": sampling_report,
    }

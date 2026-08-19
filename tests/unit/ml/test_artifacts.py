"""Unit-тесты для модуля artifacts.py."""

from datetime import UTC, datetime
from pathlib import Path

import numpy as np

from vk_collector.ml.artifacts import (
    load_vectorization_bundle,
    save_vectorization_bundle,
)
from vk_collector.ml.contracts import (
    EmbeddingQualityReport,
    ExecutionProvenance,
    ModalityProfile,
    MultimodalPost,
    SampleMode,
    SamplingReport,
)


def test_save_and_load_vectorization_bundle(tmp_path: Path) -> None:
    run_dir = tmp_path / "test_run_123"
    embeddings = np.random.normal(size=(5, 64)).astype(np.float32)

    posts = [
        MultimodalPost(
            post_id=i,
            group_id=1,
            community_vk_id=1,
            subject="food_delivery",
            published_at=datetime.now(UTC),
            text=f"Пост {i}",
            modality_profile=ModalityProfile.TEXT_ONLY,
            attachments=[],
        )
        for i in range(5)
    ]

    provenance = ExecutionProvenance(
        run_id="test_run_123",
        seed=42,
        model_name="mock-encoder",
        config_hash="conf123",
        dataset_hash="data123",
    )

    sampling_report = SamplingReport(
        sample_mode=SampleMode.MICRO,
        target_size=100,
        actual_size=5,
        population_size=10,
        seed=42,
        modality_shares_sample={},
        modality_shares_population={},
        subject_shares_sample={},
        subject_shares_population={},
        delta_shares={},
        strata_coverage=1.0,
        text_length_quantiles_pop={},
        video_duration_quantiles_pop={},
        ks_statistic_text_length=0.0,
        ks_pvalue_text_length=1.0,
        chi2_statistic_modality=0.0,
        chi2_pvalue_modality=1.0,
    )

    quality_report = EmbeddingQualityReport(
        run_id="test_run_123",
        model_name="mock-encoder",
        sample_size=5,
        embedding_dim=64,
        hopkins_statistic=0.75,
        pca_95_components=3,
        pca_explained_variance_ratio=[0.5, 0.3, 0.15],
        effective_rank=2.8,
        is_l2_normalized=True,
    )

    save_vectorization_bundle(
        run_dir,
        embeddings=embeddings,
        posts=posts,
        provenance=provenance,
        sampling_report=sampling_report,
        quality_report=quality_report,
    )

    assert (run_dir / "embeddings.npy").exists()
    assert (run_dir / "metadata.json").exists()
    assert (run_dir / "run_config.json").exists()
    assert (run_dir / "embedding_quality_report.json").exists()

    loaded = load_vectorization_bundle(run_dir)
    assert loaded["embeddings"].shape == (5, 64)
    assert len(loaded["metadata"]) == 5
    assert loaded["run_config"]["run_id"] == "test_run_123"
    assert loaded["quality_report"]["hopkins_statistic"] == 0.75

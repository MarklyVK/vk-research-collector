"""Unit-тесты для метрик качества векторизации и диагностики эмбеддингов."""

import numpy as np

from vk_collector.ml.encoders.base import l2_normalize
from vk_collector.ml.metrics import (
    compute_hopkins_statistic,
    compute_pca_cumulative_variance,
    evaluate_embedding_quality,
)


def test_hopkins_statistic_on_clustered_and_uniform_data() -> None:
    rng = np.random.RandomState(42)

    # 1. Равномерные данные (H ≈ 0.5)
    uniform_data = rng.uniform(low=0.0, high=1.0, size=(300, 10))
    h_uniform = compute_hopkins_statistic(uniform_data, seed=42)

    # 2. Выраженные кластеры (H > 0.7)
    c1 = rng.normal(loc=-10.0, scale=0.1, size=(150, 10))
    c2 = rng.normal(loc=10.0, scale=0.1, size=(150, 10))
    clustered_data = np.vstack([c1, c2])
    h_clustered = compute_hopkins_statistic(clustered_data, seed=42)

    assert abs(h_uniform - 0.5) < 0.2
    assert h_clustered > 0.75
    assert h_clustered > h_uniform


def test_pca_cumulative_variance_calculation() -> None:
    rng = np.random.RandomState(42)
    # Создаем данные с 3 доминирующими компонентами в 50-мерном пространстве
    low_dim = rng.normal(loc=0.0, scale=10.0, size=(200, 3))
    proj_matrix = rng.normal(size=(3, 50))
    high_dim = np.dot(low_dim, proj_matrix) + rng.normal(loc=0.0, scale=0.01, size=(200, 50))

    comp_95, ratios, _erank = compute_pca_cumulative_variance(high_dim, target_variance=0.95)

    assert comp_95 <= 4  # Должно быть около 3 компонент
    assert len(ratios) > 0
    assert sum(ratios[:comp_95]) >= 0.95


def test_evaluate_embedding_quality_report() -> None:
    rng = np.random.RandomState(42)
    raw_emb = rng.normal(loc=0.0, scale=1.0, size=(100, 128))
    norm_emb = l2_normalize(raw_emb)

    report = evaluate_embedding_quality(
        norm_emb,
        run_id="test_run_metrics",
        model_name="mock-model",
        seed=42,
    )

    assert report.run_id == "test_run_metrics"
    assert report.sample_size == 100
    assert report.embedding_dim == 128
    assert report.is_l2_normalized is True
    assert report.nan_count == 0
    assert report.inf_count == 0
    assert report.zero_vector_count == 0
    assert report.effective_rank > 1.0

"""Метрики качества и диагностика латентного пространства эмбеддингов."""

from __future__ import annotations

import numpy as np

from vk_collector.ml.contracts import EmbeddingQualityReport


def compute_hopkins_statistic(
    embeddings: np.ndarray,
    sample_ratio: float = 0.1,
    max_samples: int = 500,
    seed: int = 42,
) -> float:
    """Вычислить статистику Хопкинса (H in [0, 1]).

    H > 0.5 указывает на статистически значимую кластерную структуру.
    H ≈ 0.5 указывает на случайное равномерное распределение.
    """
    n, d = embeddings.shape
    if n < 5 or d < 2:
        return 0.5

    rng = np.random.RandomState(seed)
    m = min(max_samples, max(2, int(n * sample_ratio)))

    # 1. Выборка m реальных точек
    sample_indices = rng.choice(n, size=m, replace=False)

    # 2. Генерация m синтетических равномерных точек в гиперкубе эмбеддингов
    min_bounds = np.min(embeddings, axis=0)
    max_bounds = np.max(embeddings, axis=0)
    synthetic_sample = rng.uniform(min_bounds, max_bounds, size=(m, d))

    # 3. Расчёт расстояний до ближайших соседей (u_i для синтетических, w_i для реальных)
    u_sum = 0.0
    w_sum = 0.0

    # Ближайшие соседи для синтетических точек
    for i in range(m):
        synth_pt = synthetic_sample[i : i + 1]
        diffs = embeddings - synth_pt
        dists = np.linalg.norm(diffs, axis=1)
        u_sum += float(np.min(dists))

    # Ближайшие соседи для реальных точек (исключая саму точку)
    for idx in sample_indices:
        real_pt = embeddings[idx : idx + 1]
        diffs = embeddings - real_pt
        dists = np.linalg.norm(diffs, axis=1)
        # Исключаем расстояние 0 до самой себя
        dists[idx] = np.inf
        w_sum += float(np.min(dists))

    total = u_sum + w_sum
    if total <= 1e-12:
        return 0.5

    return float(u_sum / total)


def compute_pca_cumulative_variance(
    embeddings: np.ndarray,
    target_variance: float = 0.95,
) -> tuple[int, list[float], float]:
    """Вычислить число компонент для 95% вариации, спектр PCA и эффективный ранг (erank)."""
    n, d = embeddings.shape
    if n < 2 or d < 1:
        return 1, [1.0], 1.0

    # Центрирование
    mean_vec = np.mean(embeddings, axis=0, keepdims=True)
    centered = embeddings - mean_vec

    # SVD факторизация
    # Для n x d вычисляем сингулярные числа
    try:
        # np.linalg.svd возвращает s (вектор сингулярных чисел)
        _, s, _ = np.linalg.svd(centered, full_matrices=False)
    except Exception:
        return d, [1.0 / d] * d, float(d)

    variances = (s**2) / (n - 1)
    total_var = np.sum(variances)
    if total_var <= 1e-12:
        return 1, [1.0], 1.0

    explained_ratios = variances / total_var
    cum_variance = np.cumsum(explained_ratios)

    # Число компонент для достижения target_variance (95%)
    idx_95 = int(np.searchsorted(cum_variance, target_variance)) + 1
    components_95 = min(d, idx_95)

    # Эффективный ранг матрицы (erank = exp(Entropy(p)))
    p = s / np.sum(s)
    p_nz = p[p > 1e-12]
    entropy = -np.sum(p_nz * np.log(p_nz))
    erank = float(np.exp(entropy))

    return components_95, [float(x) for x in explained_ratios[:50]], erank


def evaluate_embedding_quality(
    embeddings: np.ndarray,
    *,
    run_id: str,
    model_name: str,
    seed: int = 42,
) -> EmbeddingQualityReport:
    """Сформировать полный диагностический отчёт качества матрицы эмбеддингов."""
    n, d = embeddings.shape

    nan_count = int(np.isnan(embeddings).sum())
    inf_count = int(np.isinf(embeddings).sum())

    norms = np.linalg.norm(embeddings, axis=1)
    zero_vector_count = int((norms < 1e-5).sum())
    is_l2 = bool(np.all(np.abs(norms - 1.0) < 1e-3))

    if n > 1:
        hopkins = compute_hopkins_statistic(embeddings, seed=seed)
        comp_95, ratios, erank = compute_pca_cumulative_variance(embeddings, target_variance=0.95)

        # Расчёт анизотропии: среднее косинусное сходство между случайными парами векторов
        rng = np.random.RandomState(seed)
        m_pairs = min(1000, n * (n - 1) // 2)
        idx_a = rng.choice(n, size=m_pairs)
        idx_b = rng.choice(n, size=m_pairs)
        # Исключаем одинаковые индексы
        mask = idx_a != idx_b
        if np.any(mask):
            dots = np.sum(embeddings[idx_a[mask]] * embeddings[idx_b[mask]], axis=1)
            anisotropy = float(np.mean(dots))
        else:
            anisotropy = 0.0
    else:
        hopkins = 0.5
        comp_95 = d
        ratios = [1.0]
        erank = 1.0
        anisotropy = 0.0

    return EmbeddingQualityReport(
        run_id=run_id,
        model_name=model_name,
        sample_size=n,
        embedding_dim=d,
        hopkins_statistic=round(hopkins, 4),
        pca_95_components=comp_95,
        pca_explained_variance_ratio=ratios,
        effective_rank=round(erank, 2),
        is_l2_normalized=is_l2,
        nan_count=nan_count,
        inf_count=inf_count,
        zero_vector_count=zero_vector_count,
        anisotropy_score=round(anisotropy, 4),
    )

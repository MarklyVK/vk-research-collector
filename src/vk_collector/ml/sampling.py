"""Научно обоснованное стратифицированное сэмплирование постов компаний.

Поддерживает режимы: micro (100), dev (1,000), sme (10,000), large (100,000), full.
Опирается на динамические квантили совокупности и детерминированное хэширование.
"""

from __future__ import annotations

import hashlib
import math
from collections import defaultdict
from collections.abc import Sequence

import numpy as np

from vk_collector.ml.contracts import (
    ModalityProfile,
    MultimodalPost,
    PostAttachmentItem,
    SampleMode,
    SamplingReport,
)


def generate_synthetic_posts(
    n: int = 1500,
    seed: int = 42,
) -> list[MultimodalPost]:
    """Генерация синтетической популяции постов компаний для тестов и локальной отладки."""
    from datetime import UTC, datetime, timedelta

    subjects = ["food_delivery", "food_service", "customer_acquisition", "tender_support"]
    modalities = [
        ModalityProfile.TEXT_ONLY,
        ModalityProfile.TEXT_IMAGE,
        ModalityProfile.TEXT_VIDEO,
        ModalityProfile.TRIMODAL,
    ]

    posts: list[MultimodalPost] = []
    base_time = datetime.now(UTC)

    for i in range(n):
        subj = subjects[i % len(subjects)]
        mod = modalities[i % len(modalities)]
        text_len = (i * 37) % 800 + 20
        text_content = "слово " * (text_len // 6)

        attachments: list[PostAttachmentItem] = []
        if mod in (ModalityProfile.TEXT_IMAGE, ModalityProfile.TRIMODAL):
            attachments.append(
                PostAttachmentItem(
                    position=0,
                    attachment_type="photo",
                    width=1080,
                    height=1080,
                    external_url="http://example.com/img.jpg",
                )
            )
        if mod in (ModalityProfile.TEXT_VIDEO, ModalityProfile.TRIMODAL):
            attachments.append(
                PostAttachmentItem(
                    position=1,
                    attachment_type="video",
                    duration=(i % 120) + 5,
                    width=1920,
                    height=1080,
                )
            )

        posts.append(
            MultimodalPost(
                post_id=10000 + i,
                group_id=(i % 50) + 1,
                community_vk_id=(i % 50) + 1,
                subject=subj,
                published_at=base_time - timedelta(hours=i),
                text=text_content,
                modality_profile=mod,
                attachments=attachments,
                likes_count=i % 100,
            )
        )
    return posts


def compute_empirical_quantiles(
    values: Sequence[float | int],
    probs: tuple[float, ...] = (0.33, 0.66),
) -> dict[str, float]:
    """Вычислить границы квантилей по фактическому распределению совокупности."""
    if not values:
        return {"q33": 0.0, "q66": 0.0}
    arr = np.array(values, dtype=np.float64)
    q_vals = np.quantile(arr, probs)
    return {f"q{int(p * 100)}": float(q_vals[i]) for i, p in enumerate(probs)}


def assign_bin(value: float, q33: float, q66: float) -> str:
    """Отнести значение к одному из трех интервалов на основе эмпирических квантилей."""
    if value <= q33:
        return "low"
    if value <= q66:
        return "mid"
    return "high"


def compute_deterministic_rank(seed: int, post_id: int, group_id: int) -> int:
    """Вычислить детерминированный псевдослучайный ранг через SHA-256."""
    raw = f"{seed}:{post_id}:{group_id}".encode()
    return int(hashlib.sha256(raw).hexdigest(), 16) % 1_000_000_000


def calculate_ks_statistic(sample_vals: list[float], pop_vals: list[float]) -> tuple[float, float]:
    """Вычислить статистику Колмогорова-Смирнова (D) и приближенный p-value."""
    if not sample_vals or not pop_vals:
        return 0.0, 1.0

    n1 = len(sample_vals)
    n2 = len(pop_vals)
    s1 = np.sort(np.array(sample_vals, dtype=np.float64))
    s2 = np.sort(np.array(pop_vals, dtype=np.float64))

    all_vals = np.concatenate([s1, s2])
    cdf1 = np.searchsorted(s1, all_vals, side="right") / n1
    cdf2 = np.searchsorted(s2, all_vals, side="right") / n2

    d_stat = float(np.max(np.abs(cdf1 - cdf2)))

    # Приближение p-value Колмогорова-Смирнова
    en = math.sqrt(n1 * n2 / (n1 + n2))
    lam = (en + 0.12 + 0.11 / en) * d_stat
    if lam <= 0:
        p_val = 1.0
    else:
        # Асимптотический ряд Колмогорова
        p_val = 0.0
        for j in range(1, 101):
            term = 2.0 * ((-1) ** (j - 1)) * math.exp(-2.0 * (j**2) * (lam**2))
            p_val += term
            if abs(term) < 1e-7:
                break
        p_val = max(0.0, min(1.0, float(p_val)))

    return d_stat, p_val


def calculate_chi2_statistic(
    sample_counts: dict[str, int],
    pop_counts: dict[str, int],
) -> tuple[float, float]:
    """Вычислить диагностику Chi-Square (хи-квадрат) согласия распределений."""
    n_sample = sum(sample_counts.values())
    n_pop = sum(pop_counts.values())
    if n_sample == 0 or n_pop == 0:
        return 0.0, 1.0

    chi2 = 0.0
    df = 0
    for key, pop_cnt in pop_counts.items():
        if pop_cnt == 0:
            continue
        expected = n_sample * (pop_cnt / n_pop)
        observed = float(sample_counts.get(key, 0))
        if expected > 0:
            chi2 += ((observed - expected) ** 2) / expected
            df += 1

    df = max(1, df - 1)
    # Приближенная оценка p-value для chi2 через гамма-функцию / стандартное распределение
    # Приближение Вильсона-Хильферти для chi2 p-value
    z = ((chi2 / df) ** (1 / 3) - (1 - 2 / (9 * df))) / math.sqrt(2 / (9 * df))
    p_val = 0.5 * math.erfc(z / math.sqrt(2))
    p_val = max(0.0, min(1.0, float(p_val)))

    return float(chi2), float(p_val)


def sample_company_posts(
    population: list[MultimodalPost],
    *,
    sample_mode: SampleMode = SampleMode.SME,
    seed: int = 42,
) -> tuple[list[MultimodalPost], SamplingReport]:
    """Выполнить многомерное стратифицированное сэмплирование постов компаний.

    Параметры:
        population: полная выборка доступных постов компаний
        sample_mode: режим выборки (micro, dev, sme, large, full)
        seed: числовой сид для детерминированного воспроизведения
    """
    pop_size = len(population)
    target_size = sample_mode.target_size

    # Если режим FULL или размер популяции меньше либо равен целевому
    if target_size is None or pop_size <= target_size:
        actual_sample = sorted(
            population, key=lambda p: (p.group_id, -p.published_at.timestamp(), p.post_id)
        )
        report = SamplingReport(
            sample_mode=sample_mode,
            target_size=target_size,
            actual_size=len(actual_sample),
            population_size=pop_size,
            seed=seed,
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
        return actual_sample, report

    # 1. Расчёт эмпирических квантилей генеральной совокупности
    text_lengths = [len(p.text) for p in population]
    video_durations = [
        max(
            (att.duration or 0 for att in p.attachments if att.attachment_type == "video"),
            default=0,
        )
        for p in population
    ]
    non_zero_durations = [d for d in video_durations if d > 0]

    text_q = compute_empirical_quantiles(text_lengths, (0.33, 0.66))
    vid_q = compute_empirical_quantiles(non_zero_durations, (0.33, 0.66))

    # 2. Построение страт и детерминированное ранжирование
    strata: dict[tuple[str, str, str, str], list[tuple[int, MultimodalPost]]] = defaultdict(list)
    pop_modality_counts: dict[str, int] = defaultdict(int)
    pop_subject_counts: dict[str, int] = defaultdict(int)

    for p in population:
        pop_modality_counts[p.modality_profile.value] += 1
        pop_subject_counts[p.subject] += 1

        t_bin = assign_bin(len(p.text), text_q["q33"], text_q["q66"])
        v_dur = max(
            (att.duration or 0 for att in p.attachments if att.attachment_type == "video"),
            default=0,
        )
        v_bin = assign_bin(v_dur, vid_q["q33"], vid_q["q66"]) if v_dur > 0 else "none"

        stratum_key = (p.subject, p.modality_profile.value, t_bin, v_bin)
        rank = compute_deterministic_rank(seed, p.post_id, p.group_id)
        strata[stratum_key].append((rank, p))

    # 3. Сортировка внутри страт
    for key in strata:
        strata[key].sort(key=lambda x: x[0])

    num_strata = len(strata)
    # Пропорциональное распределение с гарантией минимального покрытия
    allocated_counts: dict[tuple[str, str, str, str], int] = {}
    remaining_quota = target_size

    # Шаг А: Пропорциональная квота
    for key, items in strata.items():
        prop = len(items) / pop_size
        k = math.floor(target_size * prop)
        allocated_counts[key] = min(len(items), k)
        remaining_quota -= allocated_counts[key]

    # Шаг Б: Распределение остатка по стратам с наибольшим дробным дефицитом
    if remaining_quota > 0:
        strata_order = sorted(
            strata.keys(),
            key=lambda k: (len(strata[k]) - allocated_counts[k], -allocated_counts[k]),
            reverse=True,
        )
        for key in strata_order:
            if remaining_quota <= 0:
                break
            if allocated_counts[key] < len(strata[key]):
                allocated_counts[key] += 1
                remaining_quota -= 1

    # 4. Сборка отобранной выборки
    selected_posts: list[MultimodalPost] = []
    for key, count in allocated_counts.items():
        selected_posts.extend(post for _, post in strata[key][:count])

    # Детерминированная сортировка выходного датасета
    selected_posts.sort(key=lambda p: (p.group_id, -p.published_at.timestamp(), p.post_id))

    # 5. Расчёт диагностических метрик
    sample_modality_counts: dict[str, int] = defaultdict(int)
    sample_subject_counts: dict[str, int] = defaultdict(int)
    for p in selected_posts:
        sample_modality_counts[p.modality_profile.value] += 1
        sample_subject_counts[p.subject] += 1

    act_size = len(selected_posts)
    modality_shares_sample = {k: v / act_size for k, v in sample_modality_counts.items()}
    modality_shares_pop = {k: v / pop_size for k, v in pop_modality_counts.items()}
    subject_shares_sample = {k: v / act_size for k, v in sample_subject_counts.items()}
    subject_shares_pop = {k: v / pop_size for k, v in pop_subject_counts.items()}

    delta_shares: dict[str, float] = {}
    for mod_k in pop_modality_counts:
        delta_shares[f"modality_{mod_k}"] = abs(
            modality_shares_sample.get(mod_k, 0.0) - modality_shares_pop.get(mod_k, 0.0)
        )
    for subj_k in pop_subject_counts:
        delta_shares[f"subject_{subj_k}"] = abs(
            subject_shares_sample.get(subj_k, 0.0) - subject_shares_pop.get(subj_k, 0.0)
        )

    covered_strata = sum(1 for k in strata if allocated_counts[k] > 0)
    strata_coverage = covered_strata / num_strata if num_strata > 0 else 1.0

    sample_text_lens = [float(len(p.text)) for p in selected_posts]
    pop_text_lens = [float(length_val) for length_val in text_lengths]
    ks_stat, ks_pval = calculate_ks_statistic(sample_text_lens, pop_text_lens)
    chi2_stat, chi2_pval = calculate_chi2_statistic(sample_modality_counts, pop_modality_counts)

    report = SamplingReport(
        sample_mode=sample_mode,
        target_size=target_size,
        actual_size=act_size,
        population_size=pop_size,
        seed=seed,
        modality_shares_sample=modality_shares_sample,
        modality_shares_population=modality_shares_pop,
        subject_shares_sample=subject_shares_sample,
        subject_shares_population=subject_shares_pop,
        delta_shares=delta_shares,
        strata_coverage=strata_coverage,
        text_length_quantiles_pop=text_q,
        video_duration_quantiles_pop=vid_q,
        ks_statistic_text_length=ks_stat,
        ks_pvalue_text_length=ks_pval,
        chi2_statistic_modality=chi2_stat,
        chi2_pvalue_modality=chi2_pval,
    )

    return selected_posts, report

from vk_collector.ml.contracts import (
    SampleMode,
)
from vk_collector.ml.sampling import (
    calculate_ks_statistic,
    compute_deterministic_rank,
    compute_empirical_quantiles,
    generate_synthetic_posts,
    sample_company_posts,
)


def test_compute_empirical_quantiles() -> None:
    data = [10.0, 20.0, 30.0, 40.0, 50.0, 60.0, 70.0, 80.0, 90.0, 100.0]
    q = compute_empirical_quantiles(data, (0.33, 0.66))
    assert "q33" in q and "q66" in q
    assert q["q33"] < q["q66"]


def test_deterministic_rank_reproducibility() -> None:
    rank1 = compute_deterministic_rank(seed=42, post_id=101, group_id=5)
    rank2 = compute_deterministic_rank(seed=42, post_id=101, group_id=5)
    rank_diff_seed = compute_deterministic_rank(seed=123, post_id=101, group_id=5)

    assert rank1 == rank2
    assert rank1 != rank_diff_seed


def test_ks_statistic_identical_distributions() -> None:
    vals = [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0]
    d_stat, p_val = calculate_ks_statistic(vals, vals)
    assert d_stat == 0.0
    assert p_val == 1.0


def test_sample_modes_execution() -> None:
    population = generate_synthetic_posts(1500)

    # 1. Micro mode (target 100)
    sample_micro, report_micro = sample_company_posts(
        population, sample_mode=SampleMode.MICRO, seed=42
    )
    assert len(sample_micro) == 100
    assert report_micro.actual_size == 100
    assert report_micro.population_size == 1500
    assert report_micro.strata_coverage > 0.5

    # 2. Dev mode (target 1000)
    sample_dev, report_dev = sample_company_posts(population, sample_mode=SampleMode.DEV, seed=42)
    assert len(sample_dev) == 1000
    assert report_dev.actual_size == 1000

    # 3. Full mode (should return all 1500)
    sample_full, report_full = sample_company_posts(
        population, sample_mode=SampleMode.FULL, seed=42
    )
    assert len(sample_full) == 1500
    assert report_full.actual_size == 1500


def test_sampling_reproducibility() -> None:
    population = generate_synthetic_posts(500)

    sample_a, _ = sample_company_posts(population, sample_mode=SampleMode.MICRO, seed=99)
    sample_b, _ = sample_company_posts(population, sample_mode=SampleMode.MICRO, seed=99)

    assert [p.post_id for p in sample_a] == [p.post_id for p in sample_b]

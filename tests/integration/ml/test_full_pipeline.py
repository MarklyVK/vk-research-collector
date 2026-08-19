"""Сквозной интеграционный тест полного ML-пайплайна векторизации постов компаний."""

from pathlib import Path

from vk_collector.ml.artifacts import (
    load_vectorization_bundle,
    save_vectorization_bundle,
)
from vk_collector.ml.contracts import (
    ExecutionProvenance,
    SampleMode,
)
from vk_collector.ml.dataset import CompanyPostsDataset
from vk_collector.ml.encoders.mock_encoder import MockMultimodalEncoder
from vk_collector.ml.metrics import evaluate_embedding_quality
from vk_collector.ml.runner import EmbeddingRunner
from vk_collector.ml.sampling import generate_synthetic_posts, sample_company_posts
from vk_collector.ml.video_calibration import VideoMADCalibrator
from vk_collector.ml.video_processor import VideoProcessor


def test_full_vectorization_pipeline_e2e(tmp_path: Path) -> None:
    # 1. Генерация популяции постов компаний
    population = generate_synthetic_posts(n=350)
    assert len(population) == 350

    # 2. Научное стратифицированное сэмплирование (n = 100 для быстрого теста)
    sample_posts, sampling_report = sample_company_posts(
        population,
        sample_mode=SampleMode.MICRO,
        seed=42,
    )
    assert len(sample_posts) == 100
    assert sampling_report.actual_size == 100
    assert sampling_report.strata_coverage > 0.0

    # 3. Калибровка параметров MAD
    calibrator = VideoMADCalibrator(thetas=[0.1, 0.2], k_maxs=[4, 8])
    calib_result = calibrator.calibrate(["video_test_1.mp4", "video_test_2.mp4"])
    assert calib_result.optimal_theta in [0.1, 0.2]
    assert calib_result.optimal_k_max in [4, 8]

    # 4. Настройка процессора видео и датасета
    video_proc = VideoProcessor(
        theta=calib_result.optimal_theta,
        k_max=calib_result.optimal_k_max,
        target_size=448,
    )
    dataset = CompanyPostsDataset(sample_posts, video_processor=video_proc)
    assert len(dataset) == 100

    # 5. Инициализация энкодера и запуск векторизации
    encoder = MockMultimodalEncoder(embedding_dim=512)
    runner = EmbeddingRunner(
        encoder=encoder,
        batch_size=16,
        checkpoint_dir=tmp_path / "checkpoints",
    )

    run_id = "integration_test_run_e2e"
    embeddings, succ_posts, failures = runner.run(
        dataset,
        run_id=run_id,
        resume=False,
    )

    assert embeddings.shape == (100, 512)
    assert len(succ_posts) == 100
    assert len(failures) == 0

    # 6. Диагностика качества эмбеддингов
    quality_report = evaluate_embedding_quality(
        embeddings,
        run_id=run_id,
        model_name="mock-multimodal-encoder",
        seed=42,
    )

    assert quality_report.is_l2_normalized is True
    assert quality_report.nan_count == 0
    assert quality_report.inf_count == 0
    assert quality_report.pca_95_components > 0
    assert quality_report.hopkins_statistic > 0.0

    # 7. Сохранение бандла артефактов
    provenance = ExecutionProvenance(
        run_id=run_id,
        seed=42,
        model_name="mock-multimodal-encoder",
        config_hash="test_config_hash",
        dataset_hash="test_dataset_hash",
        mad_params=calib_result.model_dump(),
        allocated_gpu_vram_gb=20.0,
    )

    run_output_dir = tmp_path / "exports" / run_id
    save_vectorization_bundle(
        run_output_dir,
        embeddings=embeddings,
        posts=succ_posts,
        provenance=provenance,
        sampling_report=sampling_report,
        quality_report=quality_report,
        failures=failures,
        calibration_result=calib_result,
    )

    # 8. Проверка самодостаточности и целостности бандла
    loaded_bundle = load_vectorization_bundle(run_output_dir)
    assert loaded_bundle["embeddings"].shape == (100, 512)
    assert len(loaded_bundle["metadata"]) == 100
    assert loaded_bundle["run_config"]["run_id"] == run_id
    assert loaded_bundle["quality_report"]["is_l2_normalized"] is True

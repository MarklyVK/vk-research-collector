"""Unit-тесты для адаптеров мультимодальных энкодеров и бенчмарка."""

import numpy as np

from vk_collector.ml.contracts import ModalityProfile
from vk_collector.ml.dataset import MultimodalBatchItem
from vk_collector.ml.encoders.base import l2_normalize
from vk_collector.ml.encoders.benchmark import MultimodalEncoderBenchmark
from vk_collector.ml.encoders.mock_encoder import MockMultimodalEncoder


def test_l2_normalize_function() -> None:
    raw_vectors = np.array(
        [
            [3.0, 4.0, 0.0],
            [1.0, 1.0, 1.0],
            [0.0, 0.0, 0.0],
        ],
        dtype=np.float32,
    )
    normalized = l2_normalize(raw_vectors)

    # Норма первого вектора (3,4,0) должна быть ровно 1.0 (0.6, 0.8, 0)
    assert abs(np.linalg.norm(normalized[0]) - 1.0) < 1e-5
    assert abs(normalized[0, 0] - 0.6) < 1e-5
    assert abs(np.linalg.norm(normalized[1]) - 1.0) < 1e-5


def test_mock_encoder_all_modality_combinations() -> None:
    encoder = MockMultimodalEncoder(embedding_dim=2048)
    encoder.load()

    items = [
        # 1. Text-only
        MultimodalBatchItem(
            post_id=1,
            group_id=10,
            subject="food_delivery",
            text="Скидка 10% на первый заказ",
            modality_profile=ModalityProfile.TEXT_ONLY,
            images=[],
            video_frames=[],
        ),
        # 2. Text + Image
        MultimodalBatchItem(
            post_id=2,
            group_id=10,
            subject="food_delivery",
            text="Новый десерт в меню",
            modality_profile=ModalityProfile.TEXT_IMAGE,
            images=[np.zeros((200, 200, 3), dtype=np.uint8)],
            video_frames=[],
        ),
        # 3. Text + Video
        MultimodalBatchItem(
            post_id=3,
            group_id=20,
            subject="customer_acquisition",
            text="Видео-интервью с экспертом",
            modality_profile=ModalityProfile.TEXT_VIDEO,
            images=[],
            video_frames=[
                np.zeros((100, 100, 3), dtype=np.uint8),
                np.ones((100, 100, 3), dtype=np.uint8),
            ],
        ),
        # 4. Tri-modal
        MultimodalBatchItem(
            post_id=4,
            group_id=30,
            subject="tender_support",
            text="Полный разбор тендера",
            modality_profile=ModalityProfile.TRIMODAL,
            images=[np.zeros((150, 150, 3), dtype=np.uint8)],
            video_frames=[np.zeros((100, 100, 3), dtype=np.uint8)],
        ),
    ]

    embeddings = encoder.encode_batch(items)

    assert isinstance(embeddings, np.ndarray)
    assert embeddings.shape == (4, 2048)

    # Проверка строгой L2-нормы для каждого вектора
    norms = np.linalg.norm(embeddings, axis=1)
    for n in norms:
        assert abs(n - 1.0) < 1e-5


def test_encoder_benchmark_execution() -> None:
    encoder = MockMultimodalEncoder(embedding_dim=2048)
    benchmark = MultimodalEncoderBenchmark([encoder])

    items = [
        MultimodalBatchItem(
            post_id=1,
            group_id=1,
            subject="food_service",
            text="Тестовый пост для бенчмарка",
            modality_profile=ModalityProfile.TEXT_ONLY,
            images=[],
            video_frames=[],
        )
    ]

    results = benchmark.run_benchmark(items, quota_gb=20.0)
    assert len(results) == 1
    assert results[0]["status"] == "success"
    assert results[0]["embedding_dim"] == 2048
    assert results[0]["quota_respected"] is True

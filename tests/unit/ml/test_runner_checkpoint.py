"""Unit-тесты для EmbeddingRunner, проверки контрольных точек и возобновления работы."""

from datetime import UTC, datetime
from pathlib import Path

from vk_collector.ml.contracts import ModalityProfile, MultimodalPost
from vk_collector.ml.dataset import CompanyPostsDataset
from vk_collector.ml.encoders.mock_encoder import MockMultimodalEncoder
from vk_collector.ml.runner import EmbeddingRunner


def test_embedding_runner_checkpoint_and_resume(tmp_path: Path) -> None:
    posts = [
        MultimodalPost(
            post_id=i,
            group_id=1,
            community_vk_id=1,
            subject="food_delivery",
            published_at=datetime.now(UTC),
            text=f"Тестовый пост {i}",
            modality_profile=ModalityProfile.TEXT_ONLY,
            attachments=[],
        )
        for i in range(10)
    ]

    dataset = CompanyPostsDataset(posts)
    encoder = MockMultimodalEncoder(embedding_dim=128)

    # 1. Первый запуск: обработка части записей
    runner1 = EmbeddingRunner(
        encoder=encoder,
        batch_size=4,
        checkpoint_dir=tmp_path / "checkpoints",
    )
    mat1, succ1, fail1 = runner1.run(dataset, run_id="test_run_1", resume=True)

    assert mat1.shape == (10, 128)
    assert len(succ1) == 10
    assert len(fail1) == 0

    # 2. Второй запуск с тем же run_id и resume=True (должен использовать контрольные точки)
    runner2 = EmbeddingRunner(
        encoder=encoder,
        batch_size=4,
        checkpoint_dir=tmp_path / "checkpoints",
    )
    mat2, succ2, _fail2 = runner2.run(dataset, run_id="test_run_1", resume=True)

    assert mat2.shape == (10, 128)
    assert len(succ2) == 10

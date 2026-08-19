from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest

from vk_collector.ml.dataset import CompanyPostsDataset
from vk_collector.ml.encoders.mock_encoder import MockMultimodalEncoder
from vk_collector.ml.runner import EmbeddingRunner
from vk_collector.ml.sampling import generate_synthetic_posts


@pytest.mark.asyncio
async def test_embedding_runner_database_flush_at_intervals(tmp_path: Path) -> None:
    posts = generate_synthetic_posts(n=18)
    dataset = CompanyPostsDataset(posts)
    encoder = MockMultimodalEncoder(embedding_dim=64)

    mock_session = AsyncMock()
    mock_session.execute = AsyncMock()
    mock_session.commit = AsyncMock()

    mock_session_maker = MagicMock()
    mock_session_maker.return_value.__aenter__.return_value = mock_session
    mock_session_maker.return_value.__aexit__.return_value = None

    runner = EmbeddingRunner(
        encoder=encoder,
        batch_size=4,  # GPU micro-batch
        db_save_interval=8,  # Database commit interval
        checkpoint_dir=tmp_path / "checkpoints",
        db_session_factory=mock_session_maker,
    )

    embs, succ, fails = await runner.run_async(dataset, run_id="db_flush_test")

    assert embs.shape == (18, 64)
    assert len(succ) == 18
    assert len(fails) == 0

    # 18 записей при интервале 8 должно вызвать 3 коммита: (8, 8, 2)
    assert mock_session.commit.call_count == 3

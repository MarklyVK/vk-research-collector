from unittest.mock import AsyncMock

import numpy as np
import pytest

from vk_collector.ml.import_embeddings import save_post_embeddings_batch
from vk_collector.ml.sampling import generate_synthetic_posts


@pytest.mark.asyncio
async def test_save_post_embeddings_batch_success() -> None:
    posts = generate_synthetic_posts(n=5)
    embeddings = np.random.randn(5, 128).astype(np.float32)

    mock_session = AsyncMock()
    mock_session.execute = AsyncMock()
    mock_session.commit = AsyncMock()

    saved_count = await save_post_embeddings_batch(
        session=mock_session,
        embeddings=embeddings,
        posts=posts,
        run_id="test_run_123",
        model_name="test_model",
    )

    assert saved_count == 5
    mock_session.execute.assert_called_once()
    mock_session.commit.assert_called_once()


@pytest.mark.asyncio
async def test_save_post_embeddings_batch_mismatch_raises() -> None:
    posts = generate_synthetic_posts(n=3)
    embeddings = np.random.randn(5, 128).astype(np.float32)

    mock_session = AsyncMock()

    with pytest.raises(ValueError, match="Несоответствие размерностей"):
        await save_post_embeddings_batch(
            session=mock_session,
            embeddings=embeddings,
            posts=posts,
            run_id="test_run_123",
            model_name="test_model",
        )


@pytest.mark.asyncio
async def test_save_post_embeddings_batch_empty() -> None:
    mock_session = AsyncMock()
    saved = await save_post_embeddings_batch(
        session=mock_session,
        embeddings=np.empty((0, 128), dtype=np.float32),
        posts=[],
        run_id="test_run",
        model_name="test_model",
    )
    assert saved == 0

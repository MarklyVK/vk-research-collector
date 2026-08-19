"""Unit-тесты для модульного процессора видео и расчета MAD."""

import numpy as np

from vk_collector.ml.video_processor import (
    FramePostprocessor,
    KeyFrameSelector,
    MADCalculator,
    MockVideoDecoder,
    VideoProcessor,
)


def test_mad_identical_frames() -> None:
    frame = np.full((100, 100, 3), 128, dtype=np.uint8)
    mad = MADCalculator.calculate_mad(frame, frame)
    assert mad == 0.0


def test_mad_different_frames() -> None:
    frame_black = np.zeros((100, 100, 3), dtype=np.uint8)
    frame_white = np.full((100, 100, 3), 255, dtype=np.uint8)
    mad = MADCalculator.calculate_mad(frame_black, frame_white)
    assert abs(mad - 1.0) < 1e-5


def test_keyframe_selector_bounds() -> None:
    # 20 кадров с постоянным изменением
    frames = [np.full((50, 50, 3), (i * 20) % 256, dtype=np.uint8) for i in range(20)]
    selector = KeyFrameSelector(theta=0.05, k_max=5)
    keyframes = selector.select_keyframes(frames)

    assert len(keyframes) <= 5
    assert keyframes[0].frame_index == 0  # Первый кадр всегда включен


def test_frame_postprocessor_resize() -> None:
    large_frame = np.zeros((1080, 1920, 3), dtype=np.uint8)
    resized = FramePostprocessor.resize_frame(large_frame, target_long_side=448)
    assert max(resized.shape[:2]) == 448
    assert resized.shape[1] == 448  # long side was width 1920 -> 448


def test_video_processor_end_to_end() -> None:
    processor = VideoProcessor(decoder=MockVideoDecoder(num_frames=15), theta=0.1, k_max=4)
    processed_frames = processor.process_video("dummy_source.mp4")
    assert len(processed_frames) <= 4
    assert all(isinstance(f, np.ndarray) for f in processed_frames)

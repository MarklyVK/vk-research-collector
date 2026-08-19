"""Unit-тесты для калибровки параметров MAD."""

from vk_collector.ml.video_calibration import VideoMADCalibrator
from vk_collector.ml.video_processor import MockVideoDecoder


def test_video_mad_calibrator_optimization() -> None:
    calibrator = VideoMADCalibrator(
        thetas=[0.05, 0.15, 0.25],
        k_maxs=[4, 8],
        decoder=MockVideoDecoder(num_frames=20),
    )
    result = calibrator.calibrate(["video_1.mp4", "video_2.mp4"])

    assert result.pilot_video_count == 2
    assert result.optimal_theta in [0.05, 0.15, 0.25]
    assert result.optimal_k_max in [4, 8]
    assert result.mean_semantic_similarity > 0.0
    assert result.mean_compression_ratio >= 1.0

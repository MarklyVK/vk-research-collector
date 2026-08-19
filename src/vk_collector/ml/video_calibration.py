"""Модуль калибровки параметров MAD-сжатия (theta, Kmax) на пилотной выборке видео."""

from __future__ import annotations

from typing import Any

import numpy as np

from vk_collector.ml.contracts import VideoCalibrationResult
from vk_collector.ml.video_processor import (
    KeyFrameSelector,
    MockVideoDecoder,
    VideoDecoderProtocol,
)


def compute_frame_histogram_embedding(frames: list[np.ndarray], bins: int = 16) -> np.ndarray:
    """Простое бейзлайн-представление видео для оценки семантического сходства в пилоте."""
    if not frames:
        return np.zeros(bins * 3, dtype=np.float32)

    hists: list[np.ndarray] = []
    for f in frames:
        f_hists = []
        for c in range(3):
            hist, _ = np.histogram(f[:, :, c], bins=bins, range=(0, 256), density=True)
            f_hists.append(hist)
        hists.append(np.concatenate(f_hists))

    # Mean pooling по кадрам
    emb = np.mean(hists, axis=0)
    norm = float(np.linalg.norm(emb))
    return np.asarray(emb / max(1e-8, norm), dtype=np.float32)


class VideoMADCalibrator:
    """Поиск оптимальных (theta*, Kmax*) на пилотных видео."""

    def __init__(
        self,
        thetas: list[float] | None = None,
        k_maxs: list[int] | None = None,
        decoder: VideoDecoderProtocol | None = None,
    ) -> None:
        self.thetas = thetas or [0.05, 0.10, 0.15, 0.20, 0.25]
        self.k_maxs = k_maxs or [4, 6, 8, 12, 16]
        self.decoder = decoder or MockVideoDecoder()

    def calibrate(self, pilot_videos: list[Any]) -> VideoCalibrationResult:
        """Оценить сетку параметров (theta, Kmax) на пилотной выборке видео."""
        if not pilot_videos:
            return VideoCalibrationResult(
                thetas_tested=self.thetas,
                k_maxs_tested=self.k_maxs,
                optimal_theta=0.15,
                optimal_k_max=8,
                mean_semantic_similarity=1.0,
                mean_compression_ratio=1.0,
                pilot_video_count=0,
            )

        # 1. Декодирование всех пилотных видео
        all_video_frames: list[list[np.ndarray]] = []
        for v in pilot_videos:
            frames = self.decoder.decode_frames(v)
            if frames:
                all_video_frames.append(frames)

        if not all_video_frames:
            return VideoCalibrationResult(
                thetas_tested=self.thetas,
                k_maxs_tested=self.k_maxs,
                optimal_theta=0.15,
                optimal_k_max=8,
                mean_semantic_similarity=1.0,
                mean_compression_ratio=1.0,
                pilot_video_count=0,
            )

        best_score = -1.0
        best_theta = 0.15
        best_k_max = 8
        best_sim = 0.0
        best_comp = 1.0

        # 2. Перебор сетки гиперпараметров
        for theta in self.thetas:
            for k_max in self.k_maxs:
                selector = KeyFrameSelector(theta=theta, k_max=k_max)
                sims: list[float] = []
                comp_ratios: list[float] = []

                for frames in all_video_frames:
                    full_emb = compute_frame_histogram_embedding(frames)
                    keyframes = selector.select_keyframes(frames)
                    selected_frames = [frames[kf.frame_index] for kf in keyframes]
                    mad_emb = compute_frame_histogram_embedding(selected_frames)

                    # Косинусное сходство между полным и сжатым видео
                    cos_sim = float(np.dot(full_emb, mad_emb))
                    sims.append(cos_sim)
                    comp_ratio = len(frames) / max(1, len(selected_frames))
                    comp_ratios.append(comp_ratio)

                mean_sim = float(np.mean(sims))
                mean_comp = float(np.mean(comp_ratios))

                # Функция качества: баланс семантического сходства (>=0.90) и степени сжатия
                # Мы штрафуем сходство ниже 0.90
                score = mean_sim * (1.0 + 0.2 * np.log1p(mean_comp))
                if mean_sim < 0.85:
                    score *= 0.5

                if score > best_score:
                    best_score = score
                    best_theta = theta
                    best_k_max = k_max
                    best_sim = mean_sim
                    best_comp = mean_comp

        return VideoCalibrationResult(
            thetas_tested=self.thetas,
            k_maxs_tested=self.k_maxs,
            optimal_theta=best_theta,
            optimal_k_max=best_k_max,
            mean_semantic_similarity=best_sim,
            mean_compression_ratio=best_comp,
            pilot_video_count=len(all_video_frames),
        )

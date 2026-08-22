"""Модульный процессор видео с межкадровым сжатием по формуле MAD (Mean Absolute Difference).

Формула (1) из статьи:
MAD(f_t, f_{t+1}) = 1/(H * W) * sum_{i=1}^H sum_{j=1}^W |f_t(i, j) - f_{t+1}(i, j)|
"""

from __future__ import annotations

from typing import Any, Protocol

import numpy as np
from PIL import Image

from vk_collector.ml.contracts import VideoKeyFrame


class VideoDecoderProtocol(Protocol):
    def decode_frames(self, source: Any) -> list[np.ndarray]:
        """Декодировать видео в список RGB кадров [H, W, 3] (uint8)."""
        ...


class MockVideoDecoder:
    """Мок-декодер для синтетических тестов и окружений без ffmpeg/opencv."""

    def __init__(self, num_frames: int = 10, height: int = 240, width: int = 320) -> None:
        self.num_frames = num_frames
        self.height = height
        self.width = width

    def decode_frames(self, source: Any) -> list[np.ndarray]:
        frames: list[np.ndarray] = []
        for i in range(self.num_frames):
            # Создаем кадры с контролируемым межкадровым изменением
            val = (i * 25) % 256
            frame = np.full((self.height, self.width, 3), val, dtype=np.uint8)
            frames.append(frame)
        return frames


class OpenCVVideoDecoder:
    """Декодер видео с использованием OpenCV (когда cv2 установлен)."""

    def decode_frames(self, source: Any) -> list[np.ndarray]:
        try:
            import cv2
        except ImportError as e:
            raise RuntimeError("OpenCV (cv2) не установлен в текущем окружении.") from e

        cap = cv2.VideoCapture(str(source))
        frames: list[np.ndarray] = []
        try:
            while cap.isOpened():
                ret, frame = cap.read()
                if not ret:
                    break
                # BGR -> RGB
                rgb_frame = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
                frames.append(rgb_frame)
        finally:
            cap.release()
        return frames


class MADCalculator:
    """Вычисление межкадрового изменения по формуле (1) статьи."""

    @staticmethod
    def calculate_mad(frame_a: np.ndarray, frame_b: np.ndarray) -> float:
        """Рассчитать нормализованное MAD между двумя кадрами [0, 1].

        MAD(f_t, f_{t+1}) = 1/(H * W) * sum_{i,j} |f_t(i, j) - f_{t+1}(i, j)| / 255.0
        """
        if frame_a.shape != frame_b.shape:
            # Привести к единому размеру при несовпадении
            h, w = min(frame_a.shape[0], frame_b.shape[0]), min(frame_a.shape[1], frame_b.shape[1])
            f_a = frame_a[:h, :w]
            f_b = frame_b[:h, :w]
        else:
            f_a = frame_a
            f_b = frame_b

        # Перевод в grayscale / усреднение по каналам для ускорения вычисления
        if f_a.ndim == 3 and f_a.shape[2] == 3:
            gray_a = 0.299 * f_a[:, :, 0] + 0.587 * f_a[:, :, 1] + 0.114 * f_a[:, :, 2]
            gray_b = 0.299 * f_b[:, :, 0] + 0.587 * f_b[:, :, 1] + 0.114 * f_b[:, :, 2]
        else:
            gray_a = f_a.astype(np.float32)
            gray_b = f_b.astype(np.float32)

        diff = np.abs(gray_a - gray_b)
        mad_val = float(np.mean(diff) / 255.0)
        return mad_val


class KeyFrameSelector:
    """Отбор ключевых кадров K = {t | MAD(f_t, f_{t+1}) > theta}, bounded by |K| <= Kmax."""

    def __init__(self, theta: float = 0.15, k_max: int = 8, min_frames: int = 1) -> None:
        self.theta = theta
        self.k_max = k_max
        self.min_frames = min_frames

    def select_keyframes(self, frames: list[np.ndarray], fps: float = 30.0) -> list[VideoKeyFrame]:
        if not frames:
            return []

        total_frames = len(frames)
        if total_frames == 1:
            h, w = frames[0].shape[:2]
            return [
                VideoKeyFrame(
                    frame_index=0,
                    timestamp_sec=0.0,
                    mad_score=1.0,
                    width=w,
                    height=h,
                )
            ]

        # 1. Вычисление MAD для всех последовательных пар
        scores: list[float] = [1.0]  # Первый кадр всегда имеет максимальный приоритет
        for t in range(total_frames - 1):
            mad = MADCalculator.calculate_mad(frames[t], frames[t + 1])
            scores.append(mad)

        # 2. Кандидаты, превышающие порог theta (индекс 0 включается всегда)
        candidate_indices = [0] + [t for t in range(1, total_frames) if scores[t] > self.theta]

        # 3. Ограничение не более K_max
        if len(candidate_indices) > self.k_max:
            # Выбираем первый кадр + топ по MAD скору, отсортированные по времени
            sorted_by_score = sorted(
                candidate_indices[1:], key=lambda idx: scores[idx], reverse=True
            )
            top_k_indices = [0, *sorted_by_score[: self.k_max - 1]]
            candidate_indices = sorted(top_k_indices)

        # 4. Если кандидатов меньше min_frames (например, статичное видео)
        if len(candidate_indices) < self.min_frames:
            step = max(1, total_frames // self.min_frames)
            candidate_indices = list(range(0, total_frames, step))[: self.min_frames]

        result: list[VideoKeyFrame] = []
        for idx in candidate_indices:
            h, w = frames[idx].shape[:2]
            result.append(
                VideoKeyFrame(
                    frame_index=idx,
                    timestamp_sec=idx / max(1.0, fps),
                    mad_score=float(scores[idx]),
                    width=w,
                    height=h,
                )
            )
        return result


class FramePostprocessor:
    """Масштабирование и нормализация кадров для передачи в мультимодальный энкодер."""

    @staticmethod
    def resize_frame(frame: np.ndarray, target_long_side: int = 448) -> np.ndarray:
        h, w = frame.shape[:2]
        if max(h, w) <= target_long_side:
            return frame

        scale = target_long_side / max(h, w)
        new_w = max(1, round(w * scale))
        new_h = max(1, round(h * scale))

        img = Image.fromarray(frame)
        resized_img = img.resize((new_w, new_h), Image.Resampling.BILINEAR)
        return np.array(resized_img, dtype=np.uint8)


class VideoProcessor:
    """Высокоуровневый фасад для обработки видео с сжатием по MAD."""

    def __init__(
        self,
        decoder: VideoDecoderProtocol | None = None,
        theta: float = 0.15,
        k_max: int = 8,
        target_size: int = 448,
    ) -> None:
        self.decoder = decoder or MockVideoDecoder()
        self.selector = KeyFrameSelector(theta=theta, k_max=k_max)
        self.target_size = target_size

    def process_video(self, source: Any) -> list[np.ndarray]:
        """Декодировать, сжать по MAD и вернуть список обработанных ключевых кадров."""
        raw_frames = self.decoder.decode_frames(source)
        if not raw_frames:
            return []
        keyframes = self.selector.select_keyframes(raw_frames)
        processed: list[np.ndarray] = []
        for kf in keyframes:
            raw_frame = raw_frames[kf.frame_index]
            resized = FramePostprocessor.resize_frame(raw_frame, self.target_size)
            processed.append(resized)
        return processed

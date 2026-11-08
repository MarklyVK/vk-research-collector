"""Модульный процессор видео с межкадровым сжатием по формуле MAD (Mean Absolute Difference).

Формула (1) из статьи:
MAD(f_t, f_{t+1}) = 1/(H * W) * sum_{i=1}^H sum_{j=1}^W |f_t(i, j) - f_{t+1}(i, j)|

Оптимизирован для сервера с 2 vCPU / 4 GB RAM:
- Ограничение вычислений и декодирования 1 ядром (cv2, OMP_NUM_THREADS=1, ffmpeg -threads 1).
- Частота 2 кадра/с, K_max = 32 ключевых кадра (MAD > 10).
- Безотказная поддержка изображений (превью-фолбэков).
- Потоковое немедленное удаление сырых медиафайлов в блоке finally после сжатия.
"""

from __future__ import annotations

import logging
import os
import shutil
import subprocess
from pathlib import Path
from typing import Any, Protocol

import numpy as np
from PIL import Image

from vk_collector.ml.contracts import VideoKeyFrame

logger = logging.getLogger(__name__)

# Принудительное ограничение потоков вычислений до 1 ядра во избежание голодания PostgreSQL
os.environ.setdefault("OMP_NUM_THREADS", "1")
os.environ.setdefault("OPENBLAS_NUM_THREADS", "1")
os.environ.setdefault("MKL_NUM_THREADS", "1")
os.environ.setdefault("VECLIB_MAXIMUM_THREADS", "1")
os.environ.setdefault("NUMEXPR_NUM_THREADS", "1")

try:
    import cv2  # type: ignore[import-not-found]

    cv2.setNumThreads(1)
except ImportError:
    pass


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
        source_path = Path(source) if isinstance(source, (str, Path)) else None
        if source_path and source_path.suffix.lower() in {".jpg", ".jpeg", ".png", ".webp"}:
            try:
                with Image.open(source_path) as img:
                    rgb_img = img.convert("RGB")
                    return [np.array(rgb_img, dtype=np.uint8)]
            except Exception:
                pass

        frames: list[np.ndarray] = []
        for i in range(self.num_frames):
            val = (i * 25) % 256
            frame = np.full((self.height, self.width, 3), val, dtype=np.uint8)
            frames.append(frame)
        return frames


class OpenCVVideoDecoder:
    """Декодер видео на OpenCV (строго 1 поток, поддержка 2 fps и fallback-картинок)."""

    def __init__(self, target_fps: float = 2.0, max_frames: int = 120) -> None:
        self.target_fps = target_fps
        self.max_frames = max_frames

    def decode_frames(self, source: Any) -> list[np.ndarray]:
        source_path = Path(source) if isinstance(source, (str, Path)) else None
        # Безотказный фолбэк: если источник — превью-картинка
        if source_path and source_path.suffix.lower() in {".jpg", ".jpeg", ".png", ".webp"}:
            try:
                with Image.open(source_path) as img:
                    rgb_img = img.convert("RGB")
                    return [np.array(rgb_img, dtype=np.uint8)]
            except Exception:
                pass

        try:
            import cv2

            cv2.setNumThreads(1)
        except ImportError as e:
            raise RuntimeError("OpenCV (cv2) не установлен в текущем окружении.") from e

        cap = cv2.VideoCapture(str(source))
        frames: list[np.ndarray] = []
        try:
            source_fps = cap.get(cv2.CAP_PROP_FPS)
            if not source_fps or source_fps <= 0:
                source_fps = 30.0

            step = max(1, round(source_fps / self.target_fps))
            frame_idx = 0
            while cap.isOpened():
                ret, frame = cap.read()
                if not ret:
                    break
                if frame_idx % step == 0:
                    rgb_frame = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
                    frames.append(rgb_frame)
                    if len(frames) >= self.max_frames:
                        break
                frame_idx += 1
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
    """Отбор ключевых кадров K = {t | MAD(f_t, f_{t+1}) > theta}, bounded by |K| <= Kmax.

    По умолчанию согласно разделу 3.5 статьи:
    theta = 10.0 / 255.0 (порог MAD > 10), K_max = 32.
    """

    def __init__(self, theta: float = 10.0 / 255.0, k_max: int = 32, min_frames: int = 1) -> None:
        self.theta = theta
        self.k_max = k_max
        self.min_frames = min_frames

    def select_keyframes(self, frames: list[np.ndarray], fps: float = 2.0) -> list[VideoKeyFrame]:
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
            sorted_by_score = sorted(
                candidate_indices[1:], key=lambda idx: scores[idx], reverse=True
            )
            top_k_indices = [0, *sorted_by_score[: self.k_max - 1]]
            candidate_indices = sorted(top_k_indices)

        # 4. Если кандидатов меньше min_frames (например, очень статичное видео)
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
    """Масштабирование и нормализация кадров для передачи в мультимодальный энкодер (до 768px)."""

    @staticmethod
    def resize_frame(frame: np.ndarray, target_long_side: int = 768) -> np.ndarray:
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
    """Высокоуровневый фасад для обработки видео с сжатием по MAD и защитой ресурсов сервера."""

    def __init__(
        self,
        decoder: VideoDecoderProtocol | None = None,
        theta: float = 10.0 / 255.0,
        k_max: int = 32,
        target_size: int = 768,
    ) -> None:
        self.decoder = decoder or MockVideoDecoder()
        self.selector = KeyFrameSelector(theta=theta, k_max=k_max)
        self.target_size = target_size

    def process_video(
        self,
        source: Any,
        *,
        cleanup_raw: bool = False,
        output_compressed_mp4: Path | None = None,
    ) -> list[np.ndarray]:
        """Декодировать, сжать по MAD и вернуть список обработанных ключевых кадров.

        Если cleanup_raw=True, гарантирует немедленное удаление исходного файла в finally.
        """
        raw_path = Path(source) if isinstance(source, (str, Path)) else None
        try:
            raw_frames = self.decoder.decode_frames(source)
            if not raw_frames:
                return []
            keyframes = self.selector.select_keyframes(raw_frames)
            processed: list[np.ndarray] = []
            for kf in keyframes:
                raw_frame = raw_frames[kf.frame_index]
                resized = FramePostprocessor.resize_frame(raw_frame, self.target_size)
                processed.append(resized)

            if output_compressed_mp4 is not None and raw_path is not None and raw_path.exists():
                self.compress_video_to_mp4(
                    raw_source=raw_path,
                    output_mp4_path=output_compressed_mp4,
                    max_keyframes=self.selector.k_max,
                    target_size=self.target_size,
                    cleanup_raw=False,
                )

            return processed
        finally:
            if (
                cleanup_raw
                and raw_path is not None
                and raw_path.exists()
                and (
                    output_compressed_mp4 is None
                    or raw_path.resolve() != output_compressed_mp4.resolve()
                )
            ):
                raw_path.unlink(missing_ok=True)

    def compress_video_to_mp4(
        self,
        raw_source: Path | str,
        output_mp4_path: Path,
        max_keyframes: int = 32,
        target_size: int = 768,
        fps: float = 2.0,
        cleanup_raw: bool = True,
    ) -> Path:
        """Сформировать сжатый MP4 (до 32 кадров, 768px по длинной стороне, 2 fps, 1 поток).

        Исходный сырой файл гарантированно немедленно удаляется в блоке finally.
        """
        raw_path = Path(raw_source)
        try:
            output_mp4_path.parent.mkdir(parents=True, exist_ok=True)

            ffmpeg_bin = shutil.which("ffmpeg")
            if ffmpeg_bin and raw_path.exists() and raw_path.stat().st_size > 0:
                cmd = [
                    ffmpeg_bin,
                    "-y",
                    "-threads",
                    "1",
                    "-i",
                    str(raw_path),
                    "-t",
                    "60",
                    "-r",
                    str(fps),
                    "-vf",
                    (
                        f"scale=if(gte(iw\\,ih)\\,min({target_size}\\,iw)\\,-2):"
                        f"if(lt(iw\\,ih)\\,min({target_size}\\,ih)\\,-2)"
                    ),
                    "-vframes",
                    str(max_keyframes),
                    "-c:v",
                    "libx264",
                    "-preset",
                    "ultrafast",
                    "-crf",
                    "28",
                    "-pix_fmt",
                    "yuv420p",
                    str(output_mp4_path),
                ]
                proc = subprocess.run(cmd, capture_output=True, text=True, timeout=90)
                if (
                    proc.returncode == 0
                    and output_mp4_path.exists()
                    and output_mp4_path.stat().st_size > 0
                ):
                    return output_mp4_path

            # Фолбэк через декодированные кадры
            frames = self.process_video(raw_path, cleanup_raw=False)
            if not frames:
                output_mp4_path.touch()
                return output_mp4_path

            try:
                import cv2

                cv2.setNumThreads(1)
                h, w = frames[0].shape[:2]
                fourcc = cv2.VideoWriter_fourcc(*"mp4v")
                writer = cv2.VideoWriter(str(output_mp4_path), fourcc, fps, (w, h))
                try:
                    for f in frames[:max_keyframes]:
                        bgr = cv2.cvtColor(f, cv2.COLOR_RGB2BGR)
                        writer.write(bgr)
                finally:
                    writer.release()
            except Exception:
                img = Image.fromarray(frames[0])
                img.save(output_mp4_path)

            return output_mp4_path
        finally:
            if (
                cleanup_raw
                and raw_path.exists()
                and raw_path.resolve() != output_mp4_path.resolve()
            ):
                raw_path.unlink(missing_ok=True)

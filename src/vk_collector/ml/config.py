"""Конфигурация параметров ML-пайплайна векторизации."""

from __future__ import annotations

from pathlib import Path
from typing import Literal

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

from vk_collector.ml.contracts import SampleMode


class MLSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="ML_",
        env_file=".env",
        extra="ignore",
    )

    # Режимы выборки и воспроизводимость
    sample_mode: SampleMode = SampleMode.SME
    seed: int = 42

    # Оборудование и квоты (H100 квота 20 ГБ)
    allocated_gpu_vram_gb: float = 20.0
    device: str = "cuda"
    precision: Literal["bfloat16", "float16", "float32"] = "bfloat16"
    use_flash_attention: bool = True
    use_torch_compile: bool = False

    # Модели векторизации
    model_name: str = "Qwen/Qwen3-VL-Embedding-2B"
    fallback_model_name: str = "jinaai/jina-embeddings-v5-omni"
    embedding_dim: int = 2048
    # Параметры батчинга, чекпоинтов и сохранения в БД
    batch_size: int = 32
    checkpoint_interval: int = 50
    db_save_interval: int = 1000

    # Параметры выборки из БД (статья: последние 6 месяцев, не более 100 на группу)
    max_posts_per_group: int = 100
    window_days: int = 180

    # Подключение к базе данных PostgreSQL (локальной или удаленной)
    database_url: str | None = None
    remote_postgres_host: str | None = None
    remote_postgres_port: int = 5432
    remote_postgres_db: str = "vk_research"
    remote_postgres_user: str = "vk_collector"
    remote_postgres_password: str = ""
    remote_postgres_password_file: Path | None = None
    postgres_ssl_mode: str = "prefer"
    postgres_ssl_ca_file: Path | None = None

    # Видеопроцессор и MAD-сжатие
    video_backend: Literal["opencv", "pyav", "decord", "mock"] = "opencv"
    mad_theta_default: float = 0.15
    mad_k_max_default: int = 8
    target_frame_size: int = 448

    # Пути экспорта и артефактов
    export_dir: Path = Field(default_factory=lambda: Path("exports/vectorization_runs"))
    cache_dir: Path = Field(default_factory=lambda: Path("tmp/ml_cache"))

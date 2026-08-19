"""Pydantic-модели, перечисления и контракты данных для ML-пайплайна векторизации."""

from __future__ import annotations

from datetime import UTC, datetime
from enum import StrEnum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class SampleMode(StrEnum):
    MICRO = "micro"
    DEV = "dev"
    SME = "sme"
    LARGE = "large"
    FULL = "full"

    @property
    def target_size(self) -> int | None:
        limits = {
            SampleMode.MICRO: 100,
            SampleMode.DEV: 1_000,
            SampleMode.SME: 10_000,
            SampleMode.LARGE: 100_000,
            SampleMode.FULL: None,
        }
        return limits[self]


class ModalityProfile(StrEnum):
    TEXT_ONLY = "text_only"
    TEXT_IMAGE = "text_image"
    TEXT_VIDEO = "text_video"
    TRIMODAL = "trimodal"
    IMAGE_ONLY = "image_only"
    VIDEO_ONLY = "video_only"
    EMPTY = "empty"


class PostAttachmentItem(BaseModel):
    model_config = ConfigDict(extra="ignore")

    position: int
    attachment_type: str
    vk_owner_id: int | None = None
    vk_attachment_id: int | None = None
    access_key: str | None = None
    duration: int | None = None
    width: int | None = None
    height: int | None = None
    title: str | None = None
    external_url: str | None = None
    attachment_metadata: dict[str, Any] = Field(default_factory=dict)


class MultimodalPost(BaseModel):
    model_config = ConfigDict(extra="ignore")

    post_id: int
    group_id: int
    community_vk_id: int
    subject: str
    published_at: datetime
    text: str = ""
    modality_profile: ModalityProfile = ModalityProfile.TEXT_ONLY
    attachments: list[PostAttachmentItem] = Field(default_factory=list)
    comments_count: int = 0
    likes_count: int = 0
    reposts_count: int = 0
    views_count: int = 0


class MultimodalUserPost(BaseModel):
    model_config = ConfigDict(extra="ignore")

    post_id: int
    user_id: int
    published_at: datetime
    text: str = ""
    modality_profile: ModalityProfile = ModalityProfile.TEXT_ONLY
    attachments: list[PostAttachmentItem] = Field(default_factory=list)
    comments_count: int = 0
    likes_count: int = 0
    reposts_count: int = 0
    views_count: int = 0


class UserSubscriptionItem(BaseModel):
    model_config = ConfigDict(extra="ignore")

    community_vk_id: int
    name: str = ""
    description: str = ""
    members_count: int | None = None


class UserDemographicsItem(BaseModel):
    model_config = ConfigDict(extra="ignore")

    sex: int | None = None
    bdate: str | None = None
    city: str | None = None
    education: str | None = None
    relation: int | None = None
    followers_count: int | None = None
    friends_count: int | None = None
    gifts_count: int | None = None

    def build_text_representation(self) -> tuple[str, int]:
        """Сформировать текстовую строку полей и число атрибутов r_j."""
        parts: list[str] = []
        if self.sex is not None:
            sex_str = "мужской" if self.sex == 2 else "женский" if self.sex == 1 else str(self.sex)
            parts.append(f"пол: {sex_str}")
        if self.city:
            parts.append(f"город: {self.city}")
        if self.education:
            parts.append(f"образование: {self.education}")
        if self.relation is not None:
            parts.append(f"семейное положение: {self.relation}")
        if self.gifts_count is not None:
            parts.append(f"подарки: {self.gifts_count}")
        if self.followers_count is not None:
            parts.append(f"подписчики: {self.followers_count}")
        if self.friends_count is not None:
            parts.append(f"друзья: {self.friends_count}")
        if self.bdate is not None:
            parts.append(f"день рождения: {self.bdate}")
        return ", ".join(parts), len(parts)


class MultimodalUserProfile(BaseModel):
    model_config = ConfigDict(extra="ignore")

    user_id: int
    posts: list[MultimodalUserPost] = Field(default_factory=list)
    subscriptions: list[UserSubscriptionItem] = Field(default_factory=list)
    demographics: UserDemographicsItem = Field(default_factory=UserDemographicsItem)


class VideoKeyFrame(BaseModel):
    model_config = ConfigDict(extra="ignore")

    frame_index: int
    timestamp_sec: float
    mad_score: float
    width: int
    height: int
    frame_bytes: bytes | None = None


class VideoCalibrationConfig(BaseModel):
    model_config = ConfigDict(extra="ignore")

    theta: float = 0.15
    k_max: int = 8
    target_size: int = 448
    min_frames: int = 1


class VideoCalibrationResult(BaseModel):
    model_config = ConfigDict(extra="ignore")

    thetas_tested: list[float]
    k_maxs_tested: list[int]
    optimal_theta: float
    optimal_k_max: int
    mean_semantic_similarity: float
    mean_compression_ratio: float
    pilot_video_count: int


class SamplingReport(BaseModel):
    model_config = ConfigDict(extra="ignore")

    sample_mode: SampleMode
    target_size: int | None
    actual_size: int
    population_size: int
    seed: int
    modality_shares_sample: dict[str, float]
    modality_shares_population: dict[str, float]
    subject_shares_sample: dict[str, float]
    subject_shares_population: dict[str, float]
    delta_shares: dict[str, float]
    strata_coverage: float
    text_length_quantiles_pop: dict[str, float]
    video_duration_quantiles_pop: dict[str, float]
    ks_statistic_text_length: float
    ks_pvalue_text_length: float
    chi2_statistic_modality: float
    chi2_pvalue_modality: float


class EmbeddingFailureRecord(BaseModel):
    model_config = ConfigDict(extra="ignore")

    post_id: int
    group_id: int
    stage: str
    error_type: str
    error_message: str
    timestamp: datetime = Field(default_factory=lambda: datetime.now(UTC))


class EmbeddingQualityReport(BaseModel):
    model_config = ConfigDict(extra="ignore")

    run_id: str
    model_name: str
    sample_size: int
    embedding_dim: int
    hopkins_statistic: float
    pca_95_components: int
    pca_explained_variance_ratio: list[float]
    effective_rank: float
    is_l2_normalized: bool
    nan_count: int = 0
    inf_count: int = 0
    zero_vector_count: int = 0
    anisotropy_score: float = 0.0
    modality_stability_score: float | None = None
    video_mad_fidelity_score: float | None = None


class ExecutionProvenance(BaseModel):
    model_config = ConfigDict(extra="ignore")

    run_id: str
    seed: int
    timestamp: datetime = Field(default_factory=lambda: datetime.now(UTC))
    git_commit_sha: str = "unknown"
    model_name: str
    model_revision: str = "main"
    config_hash: str
    dataset_hash: str
    mad_params: dict[str, Any] = Field(default_factory=dict)
    cuda_version: str | None = None
    pytorch_version: str | None = None
    allocated_gpu_vram_gb: float = 20.0


class PostEmbeddingRecord(BaseModel):
    """Контракт единичной записи эмбеддинга для базы данных PostgreSQL."""

    model_config = ConfigDict(extra="ignore")

    post_id: int
    run_id: str
    model_name: str
    embedding_dim: int
    embedding_vector: list[float]
    modality_profile: str
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))

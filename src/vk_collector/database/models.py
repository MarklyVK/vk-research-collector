"""Нормализованная модель данных первого этапа."""

import uuid
from datetime import datetime
from decimal import Decimal
from enum import StrEnum

from sqlalchemy import (
    BigInteger,
    Boolean,
    CheckConstraint,
    DateTime,
    Enum,
    ForeignKey,
    Index,
    Integer,
    Numeric,
    String,
    Text,
    UniqueConstraint,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column

from vk_collector.database.base import Base


class ClassificationStatus(StrEnum):
    PENDING = "pending"
    APPROVED = "approved"
    REJECTED = "rejected"


class RunStatus(StrEnum):
    RUNNING = "running"
    PAUSED = "paused"
    COMPLETED = "completed"
    FAILED = "failed"


class JobStatus(StrEnum):
    PENDING = "pending"
    RUNNING = "running"
    RETRY_WAIT = "retry_wait"
    PAUSED = "paused"
    COMPLETED = "completed"
    SKIPPED = "skipped"
    FAILED = "failed"
    CANCELLED = "cancelled"


class CollectionRunStatus(StrEnum):
    PLANNED = "planned"
    RUNNING = "running"
    PAUSED = "paused"
    PAUSED_NO_TOKENS = "paused_no_tokens"
    PAUSED_CAPACITY_LIMIT = "paused_capacity_limit"
    WAITING_METHOD_LIMIT = "waiting_method_limit"
    COMPLETED = "completed"
    COMPLETED_WITH_ERRORS = "completed_with_errors"
    FAILED = "failed"
    CANCELLED = "cancelled"


class CampaignStatus(StrEnum):
    PLANNED = "planned"
    RUNNING = "running"
    PAUSED = "paused"
    WAITING_METHOD_LIMIT = "waiting_method_limit"
    PAUSED_CAPACITY_LIMIT = "paused_capacity_limit"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class CampaignPhase(StrEnum):
    SUBSCRIPTION_DISCOVERY = "subscription_discovery"
    SUBSCRIPTION_METADATA = "subscription_metadata"
    USER_POSTS_COLLECTION = "user_posts_collection"
    WAITING_METHOD_LIMIT = "waiting_method_limit"
    PAUSED_CAPACITY_LIMIT = "paused_capacity_limit"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class TimestampMixin:
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )


class GroupCandidate(TimestampMixin, Base):
    __tablename__ = "group_candidates"
    __table_args__ = (
        CheckConstraint("vk_id > 0", name="vk_id_positive"),
        CheckConstraint(
            "confidence IS NULL OR (confidence >= 0 AND confidence <= 1)",
            name="confidence_range",
        ),
        Index("ix_group_candidates_classification_status", "classification_status"),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    vk_id: Mapped[int] = mapped_column(BigInteger, nullable=False, unique=True)
    name: Mapped[str] = mapped_column(String(512), nullable=False)
    description: Mapped[str] = mapped_column(Text, nullable=False, default="", server_default="")
    status_text: Mapped[str] = mapped_column(Text, nullable=False, default="", server_default="")
    screen_name: Mapped[str | None] = mapped_column(String(255))
    address: Mapped[str] = mapped_column(String(1024), nullable=False)
    first_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    last_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    classification_status: Mapped[ClassificationStatus] = mapped_column(
        Enum(
            ClassificationStatus,
            name="classification_status",
            values_callable=lambda x: [e.value for e in x],
        ),
        nullable=False,
        default=ClassificationStatus.PENDING,
        server_default=ClassificationStatus.PENDING.value,
    )
    confidence: Mapped[Decimal | None] = mapped_column(Numeric(5, 4))


class SearchKeyword(TimestampMixin, Base):
    __tablename__ = "search_keywords"
    __table_args__ = (
        UniqueConstraint("subject", "keyword"),
        CheckConstraint(
            "subject IN ('food_delivery', 'customer_acquisition', 'tender_support', "
            "'food_service')",
            name="subject_allowed",
        ),
        Index("ix_search_keywords_subject", "subject"),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    subject: Mapped[str] = mapped_column(String(100), nullable=False)
    keyword: Mapped[str] = mapped_column(String(500), nullable=False)
    enabled: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=True, server_default="true"
    )


class SearchRun(TimestampMixin, Base):
    __tablename__ = "search_runs"
    __table_args__ = (Index("ix_search_runs_status_created_at", "status", "created_at"),)

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    status: Mapped[RunStatus] = mapped_column(
        Enum(
            RunStatus,
            name="search_run_status",
            values_callable=lambda x: [e.value for e in x],
        ),
        nullable=False,
        default=RunStatus.RUNNING,
        server_default=RunStatus.RUNNING.value,
    )
    started_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    error_message: Mapped[str | None] = mapped_column(Text)
    configuration: Mapped[dict[str, object]] = mapped_column(
        JSONB, nullable=False, default=dict, server_default="{}"
    )
    api_results_count: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")
    private_results_count: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")
    deleted_results_count: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")
    error_count: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")


class SearchRunKeyword(TimestampMixin, Base):
    __tablename__ = "search_run_keywords"
    __table_args__ = (
        UniqueConstraint("search_run_id", "keyword_id", "community_type"),
        CheckConstraint("next_offset >= 0", name="next_offset_nonnegative"),
        Index("ix_search_run_keywords_resume", "search_run_id", "status"),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    search_run_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("search_runs.id", ondelete="CASCADE"),
        nullable=False,
    )
    keyword_id: Mapped[int] = mapped_column(
        BigInteger,
        ForeignKey("search_keywords.id", ondelete="RESTRICT"),
        nullable=False,
    )
    community_type: Mapped[str] = mapped_column(String(50), nullable=False, server_default="group")
    status: Mapped[RunStatus] = mapped_column(
        Enum(
            RunStatus,
            name="search_run_status",
            values_callable=lambda x: [e.value for e in x],
            create_type=False,
        ),
        nullable=False,
        server_default=RunStatus.RUNNING.value,
    )
    next_offset: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    error_message: Mapped[str | None] = mapped_column(Text)


class SearchRunGroup(Base):
    __tablename__ = "search_run_groups"
    __table_args__ = (
        UniqueConstraint("search_run_id", "group_id", name="uq_search_run_groups_run_group"),
        Index("ix_search_run_groups_run_new", "search_run_id", "was_new"),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    search_run_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("search_runs.id", ondelete="CASCADE"), nullable=False
    )
    group_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("group_candidates.id", ondelete="CASCADE"), nullable=False
    )
    was_new: Mapped[bool] = mapped_column(Boolean, nullable=False)
    first_seen_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class GroupKeywordMatch(Base):
    __tablename__ = "group_keyword_matches"
    __table_args__ = (
        UniqueConstraint("group_id", "keyword_id"),
        Index("ix_group_keyword_matches_keyword_id", "keyword_id"),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    group_id: Mapped[int] = mapped_column(
        BigInteger,
        ForeignKey("group_candidates.id", ondelete="CASCADE"),
        nullable=False,
    )
    keyword_id: Mapped[int] = mapped_column(
        BigInteger,
        ForeignKey("search_keywords.id", ondelete="RESTRICT"),
        nullable=False,
    )
    first_search_run_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("search_runs.id", ondelete="RESTRICT"),
        nullable=False,
    )
    first_matched_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    last_matched_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class ClassificationBatch(Base):
    __tablename__ = "classification_batches"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    batch_id: Mapped[str] = mapped_column(String(100), nullable=False, unique=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    imported_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class ClassificationBatchItem(Base):
    __tablename__ = "classification_batch_items"
    __table_args__ = (
        UniqueConstraint("batch_id", "group_id", name="uq_batch_items_batch_group"),
        UniqueConstraint("batch_id", "position", name="uq_batch_items_batch_position"),
        CheckConstraint("position >= 0", name="position_nonnegative"),
        Index("ix_classification_batch_items_group_id", "group_id"),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    batch_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("classification_batches.id", ondelete="RESTRICT"),
        nullable=False,
    )
    group_id: Mapped[int] = mapped_column(
        BigInteger,
        ForeignKey("group_candidates.id", ondelete="RESTRICT"),
        nullable=False,
    )
    position: Mapped[int] = mapped_column(Integer, nullable=False)


class GroupLabel(Base):
    __tablename__ = "group_labels"
    __table_args__ = (
        UniqueConstraint("group_id", "label"),
        CheckConstraint(
            "label IN ('food_delivery', 'customer_acquisition', 'tender_support', 'food_service')",
            name="label_allowed",
        ),
        Index("ix_group_labels_label", "label"),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    group_id: Mapped[int] = mapped_column(
        BigInteger,
        ForeignKey("group_candidates.id", ondelete="CASCADE"),
        nullable=False,
    )
    label: Mapped[str] = mapped_column(String(100), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class ClassificationReview(Base):
    """Неизменяемая запись аудита повторной классификации группы."""

    __tablename__ = "classification_reviews"
    __table_args__ = (
        UniqueConstraint("operation_id", "group_id", name="uq_reviews_operation_group"),
        CheckConstraint(
            "confidence >= 0 AND confidence <= 1",
            # Совпадает с безопасно усечённым PostgreSQL-именем старой migration 0005.
            name="classification_review_confide_f6e2",
        ),
        Index("ix_classification_reviews_group", "group_id", "created_at"),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    operation_id: Mapped[str] = mapped_column(String(100), nullable=False)
    group_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("group_candidates.id", ondelete="RESTRICT"), nullable=False
    )
    previous_approved: Mapped[bool] = mapped_column(Boolean, nullable=False)
    previous_labels: Mapped[list[str]] = mapped_column(JSONB, nullable=False)
    food_service: Mapped[bool] = mapped_column(Boolean, nullable=False)
    final_approved: Mapped[bool] = mapped_column(Boolean, nullable=False)
    final_labels: Mapped[list[str]] = mapped_column(JSONB, nullable=False)
    confidence: Mapped[Decimal] = mapped_column(Numeric(5, 4), nullable=False)
    reason: Mapped[str] = mapped_column(Text, nullable=False)
    source: Mapped[str] = mapped_column(String(100), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class CollectionCampaign(TimestampMixin, Base):
    """Durable fixed-snapshot orchestration for phased subscription enrichment."""

    __tablename__ = "collection_campaigns"
    __table_args__ = (
        CheckConstraint(
            "status IN ('planned','running','paused','waiting_method_limit',"
            "'paused_capacity_limit','completed','failed','cancelled')",
            name="collection_campaign_status_allowed",
        ),
        CheckConstraint(
            "phase IN ('subscription_discovery','subscription_metadata',"
            "'user_posts_collection',"
            "'waiting_method_limit','paused_capacity_limit','completed','failed','cancelled')",
            name="collection_campaign_phase_allowed",
        ),
        Index("ix_collection_campaigns_status_phase", "status", "phase", "created_at"),
        Index(
            "uq_collection_campaigns_active_type",
            "campaign_type",
            unique=True,
            postgresql_where=text(
                "status IN ('planned','running','paused','waiting_method_limit',"
                "'paused_capacity_limit')"
            ),
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    campaign_type: Mapped[str] = mapped_column(String(100), nullable=False)
    status: Mapped[str] = mapped_column(
        String(50), nullable=False, default=CampaignStatus.PLANNED, server_default="planned"
    )
    phase: Mapped[str] = mapped_column(
        String(50),
        nullable=False,
        default=CampaignPhase.SUBSCRIPTION_DISCOVERY,
        server_default="subscription_discovery",
    )
    snapshot_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    snapshot_max_user_id: Mapped[int] = mapped_column(
        BigInteger, nullable=False, default=0, server_default="0"
    )
    snapshot_user_count: Mapped[int] = mapped_column(
        BigInteger, nullable=False, default=0, server_default="0"
    )
    configuration: Mapped[dict[str, object]] = mapped_column(
        JSONB, nullable=False, default=dict, server_default="{}"
    )
    configuration_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    last_planned_user_id: Mapped[int] = mapped_column(
        BigInteger, nullable=False, default=0, server_default="0"
    )
    last_metadata_vk_id: Mapped[int] = mapped_column(
        BigInteger, nullable=False, default=0, server_default="0"
    )
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    next_wakeup_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    error_message: Mapped[str | None] = mapped_column(Text)


class CollectionCampaignUser(Base):
    """Minimal immutable user-ID snapshot belonging to one campaign."""

    __tablename__ = "collection_campaign_users"
    campaign_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("collection_campaigns.id", ondelete="CASCADE"),
        primary_key=True,
    )
    user_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("vk_users.vk_id", ondelete="CASCADE"), primary_key=True
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class CollectionRun(TimestampMixin, Base):
    __tablename__ = "collection_runs"
    __table_args__ = (
        Index("ix_collection_runs_status_created_at", "status", "created_at"),
        Index("ix_collection_runs_campaign_status", "campaign_id", "status", "created_at"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    campaign_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), ForeignKey("collection_campaigns.id", ondelete="SET NULL")
    )
    scope: Mapped[str] = mapped_column(String(50), nullable=False)
    status: Mapped[CollectionRunStatus] = mapped_column(
        Enum(
            CollectionRunStatus,
            name="collection_run_status",
            values_callable=lambda x: [e.value for e in x],
        ),
        nullable=False,
        default=CollectionRunStatus.PLANNED,
        server_default=CollectionRunStatus.PLANNED.value,
    )
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    requested_by: Mapped[str] = mapped_column(String(100), nullable=False, server_default="cli")
    configuration: Mapped[dict[str, object]] = mapped_column(
        JSONB, nullable=False, default=dict, server_default="{}"
    )
    total_jobs: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")
    completed_jobs: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")
    failed_jobs: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")
    skipped_jobs: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")
    error_message: Mapped[str | None] = mapped_column(Text)
    next_wakeup_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class CollectionJob(TimestampMixin, Base):
    __tablename__ = "collection_jobs"
    __table_args__ = (
        CheckConstraint("progress_offset >= 0", name="progress_offset_nonnegative"),
        CheckConstraint("attempt_count >= 0", name="collection_jobs_attempt_nonnegative"),
        CheckConstraint("max_attempts > 0", name="collection_jobs_max_attempts_positive"),
        UniqueConstraint(
            "collection_run_id",
            "job_type",
            "entity_type",
            "entity_id",
            name="uq_collection_jobs_run_type_entity",
        ),
        Index(
            "ix_collection_jobs_queue",
            "status",
            "next_attempt_at",
            "priority",
            "created_at",
        ),
        Index("ix_collection_jobs_lease", "status", "locked_at"),
        Index("ix_collection_jobs_run_status", "collection_run_id", "status"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    collection_run_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("collection_runs.id", ondelete="CASCADE"),
        nullable=False,
    )
    job_type: Mapped[str] = mapped_column(String(100), nullable=False)
    entity_type: Mapped[str] = mapped_column(String(50), nullable=False)
    entity_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    status: Mapped[JobStatus] = mapped_column(
        Enum(
            JobStatus,
            name="collection_job_status",
            values_callable=lambda x: [e.value for e in x],
        ),
        nullable=False,
        server_default=JobStatus.PENDING.value,
    )
    priority: Mapped[int] = mapped_column(Integer, nullable=False, server_default="100")
    attempt_count: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")
    max_attempts: Mapped[int] = mapped_column(Integer, nullable=False, server_default="5")
    next_attempt_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    locked_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    locked_by: Mapped[str | None] = mapped_column(String(255))
    heartbeat_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    checkpoint: Mapped[dict[str, object]] = mapped_column(
        JSONB, nullable=False, default=dict, server_default="{}"
    )
    progress_offset: Mapped[int] = mapped_column(BigInteger, nullable=False, server_default="0")
    last_error_type: Mapped[str | None] = mapped_column(String(100))
    last_error_message: Mapped[str | None] = mapped_column(Text)
    error_message: Mapped[str | None] = mapped_column(Text)
    api_requests: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")
    rows_inserted: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")
    rows_updated: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class GroupCollectionState(Base):
    __tablename__ = "group_collection_states"

    group_id: Mapped[int] = mapped_column(
        BigInteger,
        ForeignKey("group_candidates.id", ondelete="CASCADE"),
        primary_key=True,
    )
    posts_checkpoint: Mapped[dict[str, object]] = mapped_column(
        JSONB, nullable=False, default=dict, server_default="{}"
    )
    members_checkpoint: Mapped[dict[str, object]] = mapped_column(
        JSONB, nullable=False, default=dict, server_default="{}"
    )
    last_group_success_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_posts_success_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_members_success_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_error_type: Mapped[str | None] = mapped_column(String(100))
    last_error_message: Mapped[str | None] = mapped_column(Text)
    next_scheduled_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    unavailable: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default="false")
    skip_reason: Mapped[str | None] = mapped_column(String(255))
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now()
    )


class GroupPost(TimestampMixin, Base):
    __tablename__ = "group_posts"
    __table_args__ = (
        UniqueConstraint("vk_owner_id", "vk_post_id", name="uq_group_posts_owner_post"),
        Index("ix_group_posts_group_published", "group_id", "published_at"),
        Index("ix_group_posts_community_published", "community_vk_id", "published_at"),
        Index("ix_group_posts_signer", "signer_vk_user_id"),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    vk_owner_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    vk_post_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    group_id: Mapped[int | None] = mapped_column(
        BigInteger, ForeignKey("group_candidates.id", ondelete="SET NULL")
    )
    community_vk_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("vk_communities.vk_id", ondelete="RESTRICT"), nullable=False
    )
    published_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    edited_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    text: Mapped[str] = mapped_column(Text, nullable=False, server_default="")
    post_type: Mapped[str] = mapped_column(String(50), nullable=False, server_default="post")
    is_pinned: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default="false")
    is_ad: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default="false")
    marked_as_ads: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default="false")
    comments_count: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")
    likes_count: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")
    reposts_count: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")
    views_count: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")
    signer_vk_user_id: Mapped[int | None] = mapped_column(BigInteger)
    source_updated_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    first_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    last_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    content_hash: Mapped[str] = mapped_column(String(64), nullable=False)


class PostAttachment(Base):
    __tablename__ = "post_attachments"
    __table_args__ = (
        UniqueConstraint("post_id", "position", name="uq_post_attachments_post_position"),
        Index("ix_post_attachments_type", "attachment_type"),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    post_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("group_posts.id", ondelete="CASCADE"), nullable=False
    )
    position: Mapped[int] = mapped_column(Integer, nullable=False)
    attachment_type: Mapped[str] = mapped_column(String(50), nullable=False)
    vk_owner_id: Mapped[int | None] = mapped_column(BigInteger)
    vk_attachment_id: Mapped[int | None] = mapped_column(BigInteger)
    access_key: Mapped[str | None] = mapped_column(String(255))
    duration: Mapped[int | None] = mapped_column(Integer)
    width: Mapped[int | None] = mapped_column(Integer)
    height: Mapped[int | None] = mapped_column(Integer)
    title: Mapped[str | None] = mapped_column(String(1000))
    external_url: Mapped[str | None] = mapped_column(Text)
    attachment_metadata: Mapped[dict[str, object]] = mapped_column(
        "metadata", JSONB, nullable=False, default=dict, server_default="{}"
    )


class VKUser(Base):
    __tablename__ = "vk_users"
    __table_args__ = (Index("ix_vk_users_profile_updated_at", "profile_updated_at"),)

    vk_id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=False)
    first_name: Mapped[str] = mapped_column(String(255), nullable=False, server_default="")
    last_name: Mapped[str] = mapped_column(String(255), nullable=False, server_default="")
    screen_name: Mapped[str | None] = mapped_column(String(255))
    is_closed: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default="false")
    can_access_closed: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default="false")
    deactivated: Mapped[str | None] = mapped_column(String(50))
    first_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    last_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    profile_updated_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    sex: Mapped[int | None] = mapped_column(Integer)
    bdate: Mapped[str | None] = mapped_column(String(50))
    city: Mapped[str | None] = mapped_column(String(255))
    education: Mapped[str | None] = mapped_column(String(512))
    relation: Mapped[int | None] = mapped_column(Integer)
    followers_count: Mapped[int | None] = mapped_column(Integer)
    friends_count: Mapped[int | None] = mapped_column(Integer)
    gifts_count: Mapped[int | None] = mapped_column(Integer)
    demographics: Mapped[dict[str, object]] = mapped_column(
        JSONB, nullable=False, default=dict, server_default="{}"
    )


class GroupMembership(Base):
    __tablename__ = "group_memberships"
    __table_args__ = (
        UniqueConstraint("group_id", "user_id", name="uq_group_memberships_group_user"),
        Index("ix_group_memberships_user", "user_id"),
        Index("ix_group_memberships_current", "group_id", "is_current"),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    group_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("group_candidates.id", ondelete="CASCADE"), nullable=False
    )
    user_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("vk_users.vk_id", ondelete="CASCADE"), nullable=False
    )
    first_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    last_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    is_current: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default="true")
    source_run_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), ForeignKey("collection_runs.id", ondelete="SET NULL")
    )


class UserGroupSubscription(Base):
    __tablename__ = "user_group_subscriptions"
    __table_args__ = (
        UniqueConstraint("user_id", "vk_group_id", name="uq_user_group_subscriptions"),
        Index("ix_user_group_subscriptions_group", "vk_group_id"),
        Index("ix_user_group_subscriptions_current", "user_id", "is_current"),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    user_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("vk_users.vk_id", ondelete="CASCADE"), nullable=False
    )
    vk_group_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("vk_communities.vk_id", ondelete="RESTRICT"), nullable=False
    )
    first_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    last_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    is_current: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default="true")
    source_run_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), ForeignKey("collection_runs.id", ondelete="SET NULL")
    )


class CollectionJobError(Base):
    __tablename__ = "collection_job_errors"
    __table_args__ = (
        Index("ix_collection_job_errors_run_created", "collection_run_id", "created_at"),
        Index("ix_collection_job_errors_category", "error_category"),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    collection_run_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("collection_runs.id", ondelete="CASCADE"), nullable=False
    )
    job_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), ForeignKey("collection_jobs.id", ondelete="CASCADE")
    )
    token_fingerprint: Mapped[str | None] = mapped_column(String(32))
    endpoint: Mapped[str] = mapped_column(String(100), nullable=False)
    error_category: Mapped[str] = mapped_column(String(100), nullable=False)
    vk_error_code: Mapped[int | None] = mapped_column(Integer)
    http_status: Mapped[int | None] = mapped_column(Integer)
    attempt: Mapped[int] = mapped_column(Integer, nullable=False)
    sanitized_message: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class VKCommunity(Base):
    __tablename__ = "vk_communities"

    vk_id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=False)
    name: Mapped[str] = mapped_column(String(512), nullable=False, server_default="")
    description: Mapped[str] = mapped_column(Text, nullable=False, server_default="")
    status_text: Mapped[str] = mapped_column(Text, nullable=False, server_default="")
    screen_name: Mapped[str | None] = mapped_column(String(255))
    community_type: Mapped[str | None] = mapped_column(String(32))
    is_closed: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default="false")
    deactivated: Mapped[str | None] = mapped_column(String(50))
    members_count: Mapped[int | None] = mapped_column(Integer)
    first_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    last_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    metadata_updated_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class UserSubscriptionState(Base):
    __tablename__ = "user_subscription_states"
    __table_args__ = (
        Index("ix_user_subscription_states_campaign", "last_campaign_id", "last_success_at"),
    )

    user_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("vk_users.vk_id", ondelete="CASCADE"), primary_key=True
    )
    last_attempt_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_success_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    total_reported: Mapped[int | None] = mapped_column(BigInteger)
    collected_count: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")
    collected_limit: Mapped[int] = mapped_column(Integer, nullable=False, server_default="50")
    privacy_denied: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default="false")
    is_truncated: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default="false")
    terminal_reason: Mapped[str | None] = mapped_column(String(100))
    last_error_code: Mapped[int | None] = mapped_column(Integer)
    last_run_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), ForeignKey("collection_runs.id", ondelete="SET NULL")
    )
    last_campaign_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), ForeignKey("collection_campaigns.id", ondelete="SET NULL")
    )
    next_scheduled_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class CommunityPostCollectionState(Base):
    __tablename__ = "community_post_collection_states"
    __table_args__ = (Index("ix_community_post_states_next", "next_scheduled_at"),)

    community_vk_id: Mapped[int] = mapped_column(
        BigInteger,
        ForeignKey("vk_communities.vk_id", ondelete="CASCADE"),
        primary_key=True,
    )
    last_attempt_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_success_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    next_scheduled_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_run_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), ForeignKey("collection_runs.id", ondelete="SET NULL")
    )
    last_error_code: Mapped[int | None] = mapped_column(Integer)
    last_error_reason: Mapped[str | None] = mapped_column(String(255))
    collected_count: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")
    wall_private: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default="false")
    unavailable: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default="false")


class VKTokenState(Base):
    __tablename__ = "vk_token_states"

    token_fingerprint: Mapped[str] = mapped_column(String(64), primary_key=True)
    next_request_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    global_blocked_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    disabled: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default="false")
    disabled_reason: Mapped[str | None] = mapped_column(String(255))
    last_success_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_error_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now()
    )


class VKTokenMethodState(Base):
    __tablename__ = "vk_token_method_states"
    __table_args__ = (
        UniqueConstraint("token_fingerprint", "method", name="uq_vk_token_method_state"),
        Index("ix_vk_token_method_blocked", "method", "blocked_until"),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    token_fingerprint: Mapped[str] = mapped_column(
        String(64),
        ForeignKey("vk_token_states.token_fingerprint", ondelete="CASCADE"),
        nullable=False,
    )
    method: Mapped[str] = mapped_column(String(100), nullable=False)
    blocked_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    next_probe_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    consecutive_limit_hits: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")
    last_error_code: Mapped[int | None] = mapped_column(Integer)
    last_error_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_success_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    successful_requests: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")
    cooldown_seconds: Mapped[int] = mapped_column(BigInteger, nullable=False, server_default="0")
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now()
    )


class PostEmbedding(Base):
    """Мультимодальные L2-нормализованные эмбеддинги постов компаний."""

    __tablename__ = "post_embeddings"
    __table_args__ = (
        Index("ix_post_embeddings_run_id", "run_id"),
        Index("ix_post_embeddings_model_name", "model_name"),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    post_id: Mapped[int] = mapped_column(
        BigInteger,
        ForeignKey("group_posts.id", ondelete="CASCADE"),
        nullable=False,
        unique=True,
    )
    run_id: Mapped[str] = mapped_column(String(64), nullable=False)
    model_name: Mapped[str] = mapped_column(String(255), nullable=False)
    embedding_dim: Mapped[int] = mapped_column(Integer, nullable=False, server_default="2048")
    embedding_vector: Mapped[list[float]] = mapped_column(JSONB, nullable=False)
    modality_profile: Mapped[str] = mapped_column(String(50), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now()
    )


class UserPost(TimestampMixin, Base):
    """Посты со стен пользователей VK."""

    __tablename__ = "user_posts"
    __table_args__ = (
        UniqueConstraint("vk_owner_id", "vk_post_id", name="uq_user_posts_owner_post"),
        Index("ix_user_posts_user_published", "user_id", "published_at"),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    vk_owner_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    vk_post_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    user_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("vk_users.vk_id", ondelete="CASCADE"), nullable=False
    )
    published_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    edited_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    text: Mapped[str] = mapped_column(Text, nullable=False, server_default="")
    post_type: Mapped[str] = mapped_column(String(50), nullable=False, server_default="post")
    is_pinned: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default="false")
    comments_count: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")
    likes_count: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")
    reposts_count: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")
    views_count: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")
    signer_vk_user_id: Mapped[int | None] = mapped_column(BigInteger)
    source_updated_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    first_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    last_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    content_hash: Mapped[str] = mapped_column(String(64), nullable=False)


class UserPostAttachment(Base):
    """Медиавложения к постам со стены пользователя."""

    __tablename__ = "user_post_attachments"
    __table_args__ = (
        UniqueConstraint("post_id", "position", name="uq_user_post_attachments_post_position"),
        Index("ix_user_post_attachments_type", "attachment_type"),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    post_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("user_posts.id", ondelete="CASCADE"), nullable=False
    )
    position: Mapped[int] = mapped_column(Integer, nullable=False)
    attachment_type: Mapped[str] = mapped_column(String(50), nullable=False)
    vk_owner_id: Mapped[int | None] = mapped_column(BigInteger)
    vk_attachment_id: Mapped[int | None] = mapped_column(BigInteger)
    access_key: Mapped[str | None] = mapped_column(String(255))
    duration: Mapped[int | None] = mapped_column(Integer)
    width: Mapped[int | None] = mapped_column(Integer)
    height: Mapped[int | None] = mapped_column(Integer)
    title: Mapped[str | None] = mapped_column(String(1000))
    external_url: Mapped[str | None] = mapped_column(Text)
    attachment_metadata: Mapped[dict[str, object]] = mapped_column(
        "metadata", JSONB, nullable=False, default=dict, server_default="{}"
    )


class UserPostCollectionState(Base):
    """Состояние сбора постов со стены пользователя."""

    __tablename__ = "user_post_collection_states"
    __table_args__ = (Index("ix_user_post_collection_states_next", "next_scheduled_at"),)

    user_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("vk_users.vk_id", ondelete="CASCADE"), primary_key=True
    )
    last_attempt_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_success_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    next_scheduled_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_run_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), ForeignKey("collection_runs.id", ondelete="SET NULL")
    )
    last_error_code: Mapped[int | None] = mapped_column(Integer)
    last_error_reason: Mapped[str | None] = mapped_column(String(255))
    collected_count: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")
    wall_private: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default="false")
    unavailable: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default="false")

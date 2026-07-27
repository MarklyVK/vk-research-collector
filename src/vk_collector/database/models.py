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
)
from sqlalchemy.dialects.postgresql import UUID
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
    PAUSED = "paused"
    COMPLETED = "completed"
    FAILED = "failed"


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
            "label IN ('food_delivery', 'customer_acquisition', 'tender_support')",
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


class CollectionJob(TimestampMixin, Base):
    __tablename__ = "collection_jobs"
    __table_args__ = (
        CheckConstraint("progress_offset >= 0", name="progress_offset_nonnegative"),
        Index("ix_collection_jobs_status_created_at", "status", "created_at"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    job_type: Mapped[str] = mapped_column(String(100), nullable=False)
    status: Mapped[JobStatus] = mapped_column(
        Enum(
            JobStatus,
            name="collection_job_status",
            values_callable=lambda x: [e.value for e in x],
        ),
        nullable=False,
        server_default=JobStatus.PENDING.value,
    )
    progress_offset: Mapped[int] = mapped_column(BigInteger, nullable=False, server_default="0")
    error_message: Mapped[str | None] = mapped_column(Text)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

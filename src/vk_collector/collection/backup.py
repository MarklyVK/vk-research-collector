from __future__ import annotations

import hashlib
import uuid
from pathlib import Path

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from vk_collector.database.models import (
    CampaignStatus,
    CollectionCampaign,
    CollectionRun,
    CollectionRunStatus,
)

ROTATE_BACKUP_CONFIRMATION = "ROTATE_ACTIVE_BACKUP_EVIDENCE"
ROTATABLE_CAMPAIGN_TYPES = ("subscription_enrichment", "user_posts_enrichment")
ACTIVE_CAMPAIGN_STATUSES = (
    CampaignStatus.PLANNED.value,
    CampaignStatus.RUNNING.value,
    CampaignStatus.PAUSED.value,
    CampaignStatus.WAITING_METHOD_LIMIT.value,
    CampaignStatus.PAUSED_CAPACITY_LIMIT.value,
)
ACTIVE_RUN_STATUSES = (
    CollectionRunStatus.PLANNED,
    CollectionRunStatus.RUNNING,
    CollectionRunStatus.PAUSED,
    CollectionRunStatus.PAUSED_NO_TOKENS,
    CollectionRunStatus.WAITING_METHOD_LIMIT,
    CollectionRunStatus.PAUSED_CAPACITY_LIMIT,
)


class BackupVerifier:
    """Verify immutable pg_dump files and cache full hashes for one process."""

    def __init__(self) -> None:
        self._verified: set[tuple[str, int, int, str]] = set()

    def fingerprint(self, backup: Path, *, cache: bool = True) -> dict[str, object]:
        """Read and hash a PostgreSQL custom-format backup."""
        resolved, size, modified_ns = self._stat(backup)
        digest = hashlib.sha256()
        try:
            with resolved.open("rb") as stream:
                header = stream.read(5)
                digest.update(header)
                while chunk := stream.read(1024 * 1024):
                    digest.update(chunk)
        except OSError as exc:
            raise ValueError(f"Backup не читается: {exc}") from exc
        if header != b"PGDMP":
            raise ValueError("Нужен непустой PostgreSQL backup формата pg_dump -Fc")
        metadata: dict[str, object] = {
            "path": str(resolved),
            "size_bytes": size,
            "modified_ns": modified_ns,
            "sha256": digest.hexdigest(),
        }
        if cache:
            self._verified.add(self._cache_key(metadata))
        return metadata

    def verify(self, backup: Path, expected: dict[str, object]) -> dict[str, object]:
        """Cheaply validate stat data and hash once per process and fingerprint."""
        resolved, size, modified_ns = self._stat(backup)
        actual_stat = {
            "path": str(resolved),
            "size_bytes": size,
            "modified_ns": modified_ns,
        }
        if any(actual_stat[key] != expected.get(key) for key in actual_stat):
            raise ValueError("Проверенный backup отсутствует или изменился после capacity-apply")
        key = self._cache_key(expected)
        if key in self._verified:
            return dict(expected)
        actual = self.fingerprint(resolved, cache=False)
        if actual != expected:
            raise ValueError("Проверенный backup отсутствует или изменился после capacity-apply")
        self._verified.add(key)
        return actual

    @staticmethod
    def _stat(backup: Path) -> tuple[Path, int, int]:
        try:
            resolved = backup.resolve(strict=True)
            stat = resolved.stat()
        except OSError as exc:
            raise ValueError(f"Backup не читается: {exc}") from exc
        if not resolved.is_file() or stat.st_size <= 5:
            raise ValueError("Нужен непустой PostgreSQL backup формата pg_dump -Fc")
        return resolved, stat.st_size, stat.st_mtime_ns

    @staticmethod
    def _cache_key(metadata: dict[str, object]) -> tuple[str, int, int, str]:
        try:
            raw_size = metadata["size_bytes"]
            raw_modified = metadata["modified_ns"]
            if not isinstance(raw_size, int) or not isinstance(raw_modified, int):
                raise ValueError
            return (
                str(metadata["path"]),
                raw_size,
                raw_modified,
                str(metadata["sha256"]),
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise ValueError("Метаданные проверенного backup повреждены") from exc


def _is_missing_backup_pause(message: str | None) -> bool:
    return bool(
        message
        and (
            message.startswith("Backup не читается:")
            or message.startswith("Проверенный backup отсутствует или изменился")
        )
    )


async def rotate_active_backup_evidence(
    sessions: async_sessionmaker[AsyncSession],
    backup: Path,
    *,
    confirmation: str,
) -> dict[str, int | str]:
    """Заменить только operational backup proof активных immutable campaigns/runs."""
    if confirmation != ROTATE_BACKUP_CONFIRMATION:
        raise ValueError(f"Требуется confirmation={ROTATE_BACKUP_CONFIRMATION}")
    metadata = BackupVerifier().fingerprint(backup)
    async with sessions() as session:
        campaigns = list(
            (
                await session.scalars(
                    select(CollectionCampaign)
                    .where(
                        CollectionCampaign.campaign_type.in_(ROTATABLE_CAMPAIGN_TYPES),
                        CollectionCampaign.status.in_(ACTIVE_CAMPAIGN_STATUSES),
                    )
                    .with_for_update()
                )
            ).all()
        )
        campaign_ids = [campaign.id for campaign in campaigns]
        runs: list[CollectionRun] = []
        if campaign_ids:
            runs = list(
                (
                    await session.scalars(
                        select(CollectionRun)
                        .where(
                            CollectionRun.campaign_id.in_(campaign_ids),
                            CollectionRun.status.in_(ACTIVE_RUN_STATUSES),
                            CollectionRun.configuration["capacity_gate"].astext == "passed",
                        )
                        .with_for_update()
                    )
                ).all()
            )
        reopened_campaign_ids: set[uuid.UUID] = set()
        reopened_runs = 0
        for campaign in campaigns:
            campaign.configuration = {
                **campaign.configuration,
                "verified_backup": metadata,
            }
        for run in runs:
            run.configuration = {**run.configuration, "verified_backup": metadata}
            if run.status == CollectionRunStatus.PAUSED_CAPACITY_LIMIT and _is_missing_backup_pause(
                run.error_message
            ):
                run.status = CollectionRunStatus.RUNNING
                run.next_wakeup_at = None
                run.error_message = None
                if run.campaign_id is not None:
                    reopened_campaign_ids.add(run.campaign_id)
                reopened_runs += 1
        for campaign in campaigns:
            if campaign.id in reopened_campaign_ids and _is_missing_backup_pause(
                campaign.error_message
            ):
                campaign.status = CampaignStatus.RUNNING.value
                campaign.next_wakeup_at = None
                campaign.error_message = None
        await session.commit()
    return {
        "backup_path": str(metadata["path"]),
        "campaigns_updated": len(campaigns),
        "runs_updated": len(runs),
        "runs_reopened": reopened_runs,
    }

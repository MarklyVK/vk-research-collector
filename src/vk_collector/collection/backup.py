from __future__ import annotations

import hashlib
from pathlib import Path


class BackupVerifier:
    """Verify immutable pg_dump files and cache full hashes for one process."""

    def __init__(self) -> None:
        self._verified: set[tuple[str, int, int, str]] = set()

    def fingerprint(self, backup: Path) -> dict[str, object]:
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
        actual = self.fingerprint(resolved)
        if actual != expected:
            self._verified.discard(key)
            raise ValueError("Проверенный backup отсутствует или изменился после capacity-apply")
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

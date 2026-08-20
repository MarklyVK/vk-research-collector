from __future__ import annotations

import re
import shutil
from dataclasses import dataclass
from pathlib import Path

SECRET_PATTERNS = (
    re.compile(r"(?i)(access_token|token|password)=([^&\s]+)"),
    re.compile(r"postgresql(?:\+asyncpg)?://[^\s]+"),
)


def sanitize_message(message: str) -> str:
    """Маскировать токены, пароли и полные строки подключения."""
    result = message
    for pattern in SECRET_PATTERNS:
        result = pattern.sub("[СКРЫТО]", result)
    return result[:2000]


@dataclass(frozen=True, slots=True)
class DiskState:
    used_percent: float
    warning: bool
    stop: bool
    total_bytes: int
    free_bytes: int


def inspect_disk(
    path: Path,
    warning_percent: int,
    stop_percent: int,
    *,
    min_free_bytes: int = 0,
) -> DiskState:
    """Проверить файловую систему без удаления данных."""
    target = path if path.exists() else Path.cwd()
    usage = shutil.disk_usage(target)
    used = 100.0 * (usage.total - usage.free) / usage.total
    absolute_stop = min_free_bytes > 0 and usage.free <= min_free_bytes
    absolute_warning = min_free_bytes > 0 and usage.free <= min_free_bytes + 512 * 1024**2
    return DiskState(
        used,
        used >= warning_percent or absolute_warning,
        used >= stop_percent or absolute_stop,
        usage.total,
        usage.free,
    )

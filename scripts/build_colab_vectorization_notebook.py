"""Собрать Colab-first notebook возобновляемой векторизации.

Скрипт хранит code cells как обычный Python-текст, чтобы notebook можно было
проверять и воспроизводимо пересобирать без ручного редактирования JSON.
"""

# ruff: noqa: E501

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT = ROOT / "notebooks" / "02_company_posts_vectorization_resumable.ipynb"
OUTPUT = Path(sys.argv[1]).expanduser().resolve() if len(sys.argv) > 1 else DEFAULT_OUTPUT


def markdown(source: str, cell_id: str) -> dict[str, Any]:
    return {
        "cell_type": "markdown",
        "id": cell_id,
        "metadata": {},
        "source": source.strip() + "\n",
    }


def code(source: str, cell_id: str) -> dict[str, Any]:
    return {
        "cell_type": "code",
        "execution_count": None,
        "id": cell_id,
        "metadata": {},
        "outputs": [],
        "source": source.strip() + "\n",
    }


cells = [
    markdown(
        r"""
# Векторизация 100 постов компаний на реальной GPU в Google Colab

Этот notebook выполняет один воспроизводимый тестовый запуск `Qwen/Qwen3-VL-Embedding-2B`:

1. подключает Google Drive до создания файлов;
2. проверяет CUDA и устанавливает совместимые Python-зависимости;
3. подключается к существующей PostgreSQL напрямую или через проверяемый SSH-туннель;
4. создаёт immutable snapshot **ровно из 100 постов**;
5. считает мультимодальные эмбеддинги GPU-батчами `32 → 16 → 8` при OOM;
6. записывает их в PostgreSQL одним tail-UPSERT (DB batch остаётся равным 500);
7. при перезапуске использует тот же manifest и пропускает `post_id`, уже подтверждённые БД;
8. сохраняет manifest, логи, метрики ресурсов и итог в Google Drive.

Исходный notebook `01_company_posts_vectorization_standalone (5).ipynb` не изменяется.
Notebook не создаёт таблицы, не запускает миграции и не удаляет данные. VK API вызывается
только для получения краткоживущих URL уже сохранённых photo/video attachments.

## Перед `Runtime → Run all`

Выберите `Runtime → Change runtime type → GPU`. В Colab Secrets добавьте:

| Secret | Назначение |
|---|---|
| `DATABASE_URL` | PostgreSQL URL, видимый с сервера или SSH-host |
| `VECTORIZATION_CONFIRMATION` | точное значение `WRITE_POST_EMBEDDINGS` |
| `VK_ACCESS_TOKEN` | пользовательский VK-токен для `photos.getById` и `video.get` |
| `VK_ACCESS_TOKENS` | необязательно: несколько токенов через пробел/запятую/новую строку |
| `SSH_ENABLED` | `true`, если БД доступна через SSH |
| `SSH_HOST`, `SSH_PORT`, `SSH_USER` | параметры SSH |
| `SSH_PRIVATE_KEY` | полный многострочный private key |
| `SSH_KNOWN_HOSTS` | проверенная строка `known_hosts` для SSH-host |

Если SSH не нужен, обязательны `DATABASE_URL`, `VECTORIZATION_CONFIRMATION` и
`VK_ACCESS_TOKEN` (либо `VK_ACCESS_TOKENS`).
Значения секретов никогда не печатаются и не сохраняются в артефактах.

> В текущей БД URL фотографий и видео намеренно не сохраняются. Notebook получает
> краткоживущие URL через VK API по сохранённым owner/media ID и `access_key`, скачивает
> bounded-файлы в persistent cache и останавливается до векторизации, если хотя бы один
> обязательный photo/video не удалось получить или проверить.
""",
        "intro",
    ),
    markdown(
        "## 1. Установка зависимостей\n\nPyTorch Colab не переустанавливается, чтобы не сломать CUDA runtime.",
        "dependencies-title",
    ),
    code(
        r"""
from __future__ import annotations

import subprocess
import sys

TRANSFORMERS_WAS_IMPORTED = "transformers" in sys.modules
PACKAGES = [
    "transformers==4.57.3",
    "qwen-vl-utils==0.0.14",
    "accelerate>=1.12,<2",
    "huggingface-hub>=0.35,<2",
    "SQLAlchemy>=2,<3",
    "asyncpg>=0.29",
    "asyncssh>=2.14,<3",
    "python-dotenv>=1,<2",
    "pydantic>=2,<3",
    "psutil>=5.9",
    "httpx>=0.27,<1",
    "Pillow>=10",
    "opencv-python-headless>=4.9,<5",
]

subprocess.check_call([sys.executable, "-m", "pip", "install", "-q", "--upgrade", *PACKAGES])

import torch
import transformers
from packaging.version import Version

if transformers.__version__ != "4.57.3":
    raise RuntimeError(f"Ожидался transformers 4.57.3, получен {transformers.__version__}.")
if TRANSFORMERS_WAS_IMPORTED:
    raise RuntimeError(
        "transformers уже был импортирован до установки закреплённой версии. "
        "Выполните Runtime → Restart session, затем Run all."
    )
torch_version = Version(torch.__version__.split("+")[0])
if torch_version < Version("2.8"):
    raise RuntimeError(
        f"Для Qwen3-VL-Embedding требуется torch>=2.8, а runtime содержит {torch.__version__}. "
        "Выберите более новый Colab GPU runtime и перезапустите notebook."
    )
print("Зависимости готовы; torch:", torch.__version__, "CUDA runtime:", torch.version.cuda)
""",
        "install-dependencies",
    ),
    markdown(
        "## 2. Конфигурация запуска\n\nДля другого независимого теста измените `TEST_SHARD_INDEX` и `DATASET_VERSION`.",
        "config-title",
    ),
    code(
        r"""
# Размеры независимых батчей
GPU_BATCH_SIZE = 32
GPU_MIN_BATCH_SIZE = 8
DB_BATCH_SIZE = 500

# Ровно 100 постов на весь запуск, а не на каждую группу
TOTAL_POST_LIMIT = 100
REQUIRE_EXACT_POST_COUNT = True
WINDOW_DAYS = 180
MAX_POSTS_PER_GROUP = 100
SNAPSHOT_SQL_BATCH_SIZE = 100
ATTACHMENT_SQL_BATCH_SIZE = 250

# Детерминированный shard. Для второго окружения используйте, например, count=2/index=1.
TEST_SHARD_COUNT = 1
TEST_SHARD_INDEX = 0
DATASET_VERSION = "v2_media_qwen_fix"
DATASET_NAME = f"approved_company_posts_colab_gpu_100_{DATASET_VERSION}_shard_{TEST_SHARD_INDEX}_of_{TEST_SHARD_COUNT}"

MODEL_NAME = "Qwen/Qwen3-VL-Embedding-2B"
MODEL_REVISION = "9f2f7e710d6d81056aa5c0a4f04764fec6bb7bda"
MODEL_IMPLEMENTATION_FILE = "scripts/qwen3_vl_embedding.py"
MODEL_IMPLEMENTATION_SHA256 = "8ffa74a1a6bb759610c57865ea416fd4daf9936cb787520e1112a3e1d547f36a"
EMBEDDING_DIM = 2048
MODEL_INSTRUCTION = "Represent this VK company post for semantic clustering and retrieval."
MODEL_PRECISION = "auto"  # auto = BF16 на совместимой GPU, иначе FP16.

MEDIA_SOURCE_MODE = "vk_api"
MEDIA_MISSING_POLICY = "fail"
MEDIA_DOWNLOAD_TIMEOUT_SECONDS = 30.0
MEDIA_DOWNLOAD_RETRIES = 3
MEDIA_MAX_FILE_BYTES = 100 * 1024 * 1024
MEDIA_MAX_VIDEO_FRAMES = 180
MEDIA_MAX_DECODED_BYTES = 256 * 1024 * 1024
MAX_MEDIA_TILES = 8
CONTACT_SHEET_TILE_SIZE = 336
MAD_THETA = 0.15
VK_API_VERSION = "5.199"
VK_PER_TOKEN_RPS = 2.5
VK_API_MAX_RETRIES = 5

RESOURCE_SAMPLE_INTERVAL_SECONDS = 10
DB_MAX_RETRIES = 4
RESUME = True
FORCE_NEW_RUN = False
APPLY_DB_WRITES = True
EXECUTE_PIPELINE = True
WRITE_CONFIRMATION = "WRITE_POST_EMBEDDINGS"

if not (8 <= GPU_MIN_BATCH_SIZE <= GPU_BATCH_SIZE <= 32):
    raise ValueError("GPU batch должен находиться в диапазоне 8–32.")
if DB_BATCH_SIZE != 500:
    raise ValueError("Для этого notebook DB_BATCH_SIZE должен оставаться равным 500.")
if TOTAL_POST_LIMIT != 100:
    raise ValueError("Colab test notebook рассчитан ровно на 100 постов.")
if TEST_SHARD_COUNT < 1 or not 0 <= TEST_SHARD_INDEX < TEST_SHARD_COUNT:
    raise ValueError("Некорректная конфигурация shard.")
""",
        "user-config",
    ),
    markdown("## 3. Google Drive, CUDA и постоянные каталоги", "runtime-title"),
    code(
        r"""
import asyncio
import contextlib
import csv
import hashlib
import importlib.util
import importlib.metadata
import json
import logging
import math
import os
import platform
import random
import re
import signal
import socket
import tempfile
import threading
import time
import uuid
from collections import defaultdict
from collections.abc import Iterator, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any, Literal

import numpy as np
import psutil
import torch
from PIL import Image, ImageOps
from pydantic import BaseModel, ConfigDict, Field, SecretStr
from sqlalchemy import URL, BigInteger, Integer, String, bindparam, column, inspect, make_url, table as table_clause, text
from sqlalchemy.dialects.postgresql import JSONB, insert
from sqlalchemy.exc import DBAPIError, IntegrityError, OperationalError, ProgrammingError, StatementError
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

try:
    from google.colab import drive, userdata
except ImportError as error:
    raise RuntimeError("Этот notebook предназначен для Google Colab.") from error

DRIVE_ROOT = Path("/content/drive/MyDrive")
if not DRIVE_ROOT.exists():
    drive.mount("/content/drive")
if not DRIVE_ROOT.exists():
    raise RuntimeError("Google Drive не смонтирован; постоянная запись запрещена.")

PROJECT_STORAGE = DRIVE_ROOT / "vk-research-collector"
STORAGE_ROOT = PROJECT_STORAGE / "vectorization_runs"
MEDIA_CACHE = PROJECT_STORAGE / "media_cache"
HF_CACHE = PROJECT_STORAGE / "cache" / "huggingface"
for path in (STORAGE_ROOT, MEDIA_CACHE, HF_CACHE):
    path.mkdir(parents=True, exist_ok=True)

probe = STORAGE_ROOT / ".write_probe"
probe.write_text("ok", encoding="utf-8")
if probe.read_text(encoding="utf-8") != "ok":
    raise RuntimeError("Google Drive write/read probe не пройден.")
probe.unlink(missing_ok=True)

os.environ["HF_HOME"] = str(HF_CACHE)
os.environ["HUGGINGFACE_HUB_CACHE"] = str(HF_CACHE / "hub")
os.environ["TRANSFORMERS_CACHE"] = str(HF_CACHE / "transformers")
os.environ["TOKENIZERS_PARALLELISM"] = "false"

if not torch.cuda.is_available() or torch.cuda.device_count() < 1:
    raise RuntimeError("CUDA GPU не найдена. Выберите Runtime → Change runtime type → GPU.")

GPU_NAME = torch.cuda.get_device_name(0)
GPU_VRAM_BYTES = int(torch.cuda.get_device_properties(0).total_memory)
BF16_SUPPORTED = bool(torch.cuda.is_bf16_supported())
RESOLVED_PRECISION = "bfloat16" if MODEL_PRECISION == "auto" and BF16_SUPPORTED else (
    "float16" if MODEL_PRECISION == "auto" else MODEL_PRECISION
)
if RESOLVED_PRECISION not in {"float16", "bfloat16"}:
    raise ValueError("MODEL_PRECISION должен быть auto, float16 или bfloat16.")
if RESOLVED_PRECISION == "bfloat16" and not BF16_SUPPORTED:
    raise RuntimeError("GPU не поддерживает BF16; установите MODEL_PRECISION='float16'.")

print(f"GPU: {GPU_NAME}; VRAM: {GPU_VRAM_BYTES / 1024**3:.1f} GiB; precision: {RESOLVED_PRECISION}")
print("Постоянное хранилище:", STORAGE_ROOT)
""",
        "initialize-colab",
    ),
    markdown("## 4. Секреты Colab и защита логов", "secrets-title"),
    code(
        r"""
from dotenv import load_dotenv

load_dotenv(override=False)

SECRET_NAMES = (
    "DATABASE_URL", "DB_SSL_MODE", "DB_SSL_CA_FILE",
    "SSH_ENABLED", "SSH_HOST", "SSH_PORT", "SSH_USER",
    "SSH_PRIVATE_KEY", "SSH_PRIVATE_KEY_FILE",
    "SSH_KNOWN_HOSTS", "SSH_KNOWN_HOSTS_FILE",
    "VECTORIZATION_CONFIRMATION", "HF_TOKEN",
    "VK_ACCESS_TOKEN", "VK_ACCESS_TOKENS", "VK_API_VERSION",
)

def read_secret(name: str) -> str | None:
    value: str | None = None
    with contextlib.suppress(Exception):
        value = userdata.get(name)
    value = value or os.getenv(name)
    file_name = os.getenv(f"{name}_FILE")
    if not value and file_name:
        path = Path(file_name).expanduser()
        if not path.is_file():
            raise RuntimeError(f"Файл секрета {name} не найден.")
        value = path.read_text(encoding="utf-8").strip()
    return value

SECRETS = {name: read_secret(name) for name in SECRET_NAMES}
VK_TOKENS = list(dict.fromkeys(
    token
    for name in ("VK_ACCESS_TOKEN", "VK_ACCESS_TOKENS")
    for token in re.split(r"[,\s]+", str(SECRETS.get(name) or "").strip())
    if token
))

class SecretRedactor:
    def __init__(self, values: Sequence[str]) -> None:
        self.values = sorted({value for value in values if value}, key=len, reverse=True)

    def redact(self, message: object) -> str:
        result = str(message)
        for value in self.values:
            result = result.replace(value, "***")
        result = re.sub(r"(?i)postgres(?:ql)?(?:\+asyncpg)?://[^\s]+", "<DATABASE_URL скрыт>", result)
        result = re.sub(r"-----BEGIN[\s\S]*?-----END[^-]*PRIVATE KEY-----", "<SSH PRIVATE KEY скрыт>", result)
        return result

REDACTOR = SecretRedactor([*[value for value in SECRETS.values() if value], *VK_TOKENS])

if not SECRETS.get("DATABASE_URL"):
    raise RuntimeError("Добавьте DATABASE_URL в Colab Secrets.")
if APPLY_DB_WRITES and SECRETS.get("VECTORIZATION_CONFIRMATION") != WRITE_CONFIRMATION:
    raise RuntimeError(
        "Для тестовой записи добавьте Colab Secret VECTORIZATION_CONFIRMATION=" + WRITE_CONFIRMATION
    )
if MEDIA_SOURCE_MODE == "vk_api" and not VK_TOKENS:
    raise RuntimeError(
        "Для загрузки photo/video добавьте Colab Secret VK_ACCESS_TOKEN "
        "или VK_ACCESS_TOKENS."
    )
VK_API_VERSION = str(SECRETS.get("VK_API_VERSION") or VK_API_VERSION).strip()
if not re.fullmatch(r"5\.\d{2,3}", VK_API_VERSION):
    raise RuntimeError("VK_API_VERSION должна иметь формат 5.xxx.")

SSH_ENABLED = str(SECRETS.get("SSH_ENABLED") or "").strip().lower() in {"1", "true", "yes", "on"}
if SSH_ENABLED:
    missing = [name for name in ("SSH_HOST", "SSH_USER") if not SECRETS.get(name)]
    if not (SECRETS.get("SSH_PRIVATE_KEY") or SECRETS.get("SSH_PRIVATE_KEY_FILE")):
        missing.append("SSH_PRIVATE_KEY or SSH_PRIVATE_KEY_FILE")
    if not (SECRETS.get("SSH_KNOWN_HOSTS") or SECRETS.get("SSH_KNOWN_HOSTS_FILE")):
        missing.append("SSH_KNOWN_HOSTS or SSH_KNOWN_HOSTS_FILE")
    if missing:
        raise RuntimeError("Для SSH отсутствуют Colab Secrets: " + ", ".join(missing))

if SECRETS.get("HF_TOKEN"):
    os.environ["HF_TOKEN"] = str(SECRETS["HF_TOKEN"])
print("Секреты проверены; значения не выводятся.")
""",
        "load-secrets",
    ),
    markdown("## 5. Логирование и служебные функции", "logging-title"),
    code(
        r"""
def utc_now() -> datetime:
    return datetime.now(UTC)

def slugify(value: str) -> str:
    return re.sub(r"[^a-zA-Z0-9._-]+", "-", value).strip("-.").lower()[:100] or "unnamed"

def canonical_sha256(value: Any) -> str:
    payload = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()

def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()

def atomic_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    temporary.write_text(json.dumps(value, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    os.replace(temporary, path)

def redact_value(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): redact_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [redact_value(item) for item in value]
    if isinstance(value, str):
        return REDACTOR.redact(value)
    return value

def append_jsonl(path: Path, value: Any) -> None:
    safe = redact_value(value)
    with path.open("a", encoding="utf-8") as stream:
        stream.write(json.dumps(safe, ensure_ascii=False) + "\n")
        stream.flush()
        with contextlib.suppress(OSError):
            os.fsync(stream.fileno())

def configure_logger(path: Path) -> logging.Logger:
    logger = logging.getLogger(f"vectorization.{uuid.uuid4().hex}")
    logger.setLevel(logging.INFO)
    logger.propagate = False
    handler = RotatingFileHandler(path, maxBytes=10 * 1024 * 1024, backupCount=3, encoding="utf-8")
    handler.setFormatter(logging.Formatter("%(asctime)sZ %(levelname)s %(message)s"))
    logger.addHandler(handler)
    return logger

@dataclass
class RunContext:
    run_dir: Path
    logger: logging.Logger
    state: dict[str, Any]
    started_at: datetime = field(default_factory=utc_now)

    def event(self, event: str, **fields: Any) -> None:
        payload = {"timestamp_utc": utc_now().isoformat(), "event": event, **fields}
        append_jsonl(self.run_dir / "events.jsonl", payload)
        self.logger.info(REDACTOR.redact(json.dumps(payload, ensure_ascii=False, default=str)))
""",
        "logging-helpers",
    ),
    markdown("## 6. SSH-туннель и PostgreSQL", "database-title"),
    code(
        r"""
import asyncssh

def build_database_url(host_override: str | None = None, port_override: int | None = None) -> URL:
    url = make_url(str(SECRETS["DATABASE_URL"])).set(drivername="postgresql+asyncpg")
    if host_override is not None or port_override is not None:
        url = url.set(host=host_override or url.host, port=port_override or url.port)
    return url

def masked_database_url(url: URL) -> str:
    return url.render_as_string(hide_password=True)

class SshTunnel:
    def __init__(self, connection: Any, listener: Any) -> None:
        self.connection = connection
        self.listener = listener

    @property
    def local_port(self) -> int:
        return int(self.listener.get_port())

    async def close(self) -> None:
        self.listener.close()
        with contextlib.suppress(Exception):
            await self.listener.wait_closed()
        self.connection.close()
        with contextlib.suppress(Exception):
            await self.connection.wait_closed()

def materialize_known_hosts() -> str:
    file_name = SECRETS.get("SSH_KNOWN_HOSTS_FILE")
    if file_name:
        path = Path(str(file_name)).expanduser()
        if not path.is_file():
            raise RuntimeError("SSH_KNOWN_HOSTS_FILE не найден.")
        return str(path)
    content = str(SECRETS.get("SSH_KNOWN_HOSTS") or "").strip()
    if not content:
        raise RuntimeError("SSH host-key verification обязательна.")
    path = Path("/content") / f"known_hosts_{hashlib.sha256(content.encode()).hexdigest()[:12]}"
    path.write_text(content + "\n", encoding="utf-8")
    path.chmod(0o600)
    return str(path)

async def open_ssh_tunnel() -> SshTunnel | None:
    if not SSH_ENABLED:
        return None
    remote_url = make_url(str(SECRETS["DATABASE_URL"]))
    if not remote_url.host:
        raise RuntimeError("DATABASE_URL не содержит remote DB host.")
    if SECRETS.get("SSH_PRIVATE_KEY"):
        key = asyncssh.import_private_key(str(SECRETS["SSH_PRIVATE_KEY"]))
    else:
        key_path = Path(str(SECRETS["SSH_PRIVATE_KEY_FILE"])).expanduser()
        if not key_path.is_file():
            raise RuntimeError("SSH_PRIVATE_KEY_FILE не найден.")
        key = asyncssh.read_private_key(key_path)
    connection = await asyncssh.connect(
        str(SECRETS["SSH_HOST"]),
        port=int(SECRETS.get("SSH_PORT") or 22),
        username=str(SECRETS["SSH_USER"]),
        client_keys=[key],
        known_hosts=materialize_known_hosts(),
    )
    listener = await connection.forward_local_port("127.0.0.1", 0, remote_url.host, int(remote_url.port or 5432))
    return SshTunnel(connection, listener)

def create_engine(url: URL) -> AsyncEngine:
    ssl_mode = str(SECRETS.get("DB_SSL_MODE") or "prefer")
    connect_args: dict[str, Any] = {"timeout": 30, "command_timeout": 180}
    if ssl_mode == "disable":
        connect_args["ssl"] = False
    elif ssl_mode in {"require", "verify-ca", "verify-full"}:
        import ssl

        context = ssl.create_default_context(cafile=SECRETS.get("DB_SSL_CA_FILE") or None)
        if ssl_mode == "require":
            context.check_hostname = False
            context.verify_mode = ssl.CERT_NONE
        connect_args["ssl"] = context
    return create_async_engine(
        url,
        pool_pre_ping=True,
        pool_size=2,
        max_overflow=0,
        hide_parameters=True,
        connect_args=connect_args,
    )
""",
        "ssh-and-db",
    ),
    markdown("## 7. Контракты данных, manifest и immutable revision", "contracts-title"),
    code(
        r"""
from huggingface_hub import HfApi

class Attachment(BaseModel):
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

class Post(BaseModel):
    model_config = ConfigDict(extra="ignore")
    post_id: int
    group_id: int | None
    community_vk_id: int
    subject: str
    published_at: datetime
    text: str = ""
    modality_profile: str
    attachments: list[Attachment] = Field(default_factory=list)
    comments_count: int = 0
    likes_count: int = 0
    reposts_count: int = 0
    views_count: int = 0

RESUMABLE_STATUSES = {"materializing_snapshot", "ready", "running", "interrupted", "failed", "incomplete", "completed"}

class RunManifest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    manifest_version: int = 1
    run_id: str
    dataset_name: str
    model_name: str
    model_revision: str
    embedding_dim: int
    critical_config: dict[str, Any]
    critical_config_sha256: str
    status: str = "materializing_snapshot"
    snapshot_cutoff_utc: datetime
    snapshot_high_water_post_id: int
    snapshot_last_post_id: int = 0
    snapshot_record_count: int = 0
    snapshot_sha256: str = ""
    effective_gpu_batch: int = GPU_BATCH_SIZE
    created_at_utc: datetime = Field(default_factory=utc_now)
    updated_at_utc: datetime = Field(default_factory=utc_now)

def manifest_candidates() -> list[Path]:
    base = STORAGE_ROOT / slugify(DATASET_NAME) / slugify(MODEL_NAME)
    return sorted(base.glob("*/run_manifest.json"), key=lambda path: path.stat().st_mtime, reverse=True)

def existing_resume_manifest() -> tuple[Path, RunManifest] | None:
    if FORCE_NEW_RUN or not RESUME:
        return None
    matches: list[tuple[Path, RunManifest]] = []
    for path in manifest_candidates():
        try:
            manifest = RunManifest.model_validate_json(path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            continue
        if manifest.dataset_name == DATASET_NAME and manifest.model_name == MODEL_NAME and manifest.status in RESUMABLE_STATUSES:
            matches.append((path.parent, manifest))
    if len(matches) > 1:
        raise RuntimeError("Найдено несколько resumable manifests для dataset/model; требуется ручной аудит.")
    return matches[0] if matches else None

RESUME_CANDIDATE = existing_resume_manifest()
if MODEL_REVISION.strip():
    RESOLVED_MODEL_REVISION = MODEL_REVISION.strip()
elif RESUME_CANDIDATE:
    RESOLVED_MODEL_REVISION = RESUME_CANDIDATE[1].model_revision
else:
    RESOLVED_MODEL_REVISION = str(HfApi().model_info(MODEL_NAME, token=SECRETS.get("HF_TOKEN")).sha)
if not re.fullmatch(r"[0-9a-f]{40}", RESOLVED_MODEL_REVISION):
    raise RuntimeError("MODEL_REVISION должна быть immutable 40-символьным commit SHA.")

CRITICAL_CONFIG = {
    "total_post_limit": TOTAL_POST_LIMIT,
    "window_days": WINDOW_DAYS,
    "max_posts_per_group": MAX_POSTS_PER_GROUP,
    "test_shard_count": TEST_SHARD_COUNT,
    "test_shard_index": TEST_SHARD_INDEX,
    "embedding_dim": EMBEDDING_DIM,
    "model_revision": RESOLVED_MODEL_REVISION,
    "model_loader": "official_qwen3_vl_embedding",
    "model_implementation_sha256": MODEL_IMPLEMENTATION_SHA256,
    "model_instruction": MODEL_INSTRUCTION,
    "precision": RESOLVED_PRECISION,
    "gpu_batch_size": GPU_BATCH_SIZE,
    "gpu_min_batch_size": GPU_MIN_BATCH_SIZE,
    "db_batch_size": DB_BATCH_SIZE,
    "media_source_mode": MEDIA_SOURCE_MODE,
    "media_missing_policy": MEDIA_MISSING_POLICY,
    "vk_api_version": VK_API_VERSION,
    "vk_per_token_rps": VK_PER_TOKEN_RPS,
}
CRITICAL_CONFIG_SHA256 = canonical_sha256(CRITICAL_CONFIG)

if RESUME_CANDIDATE and RESUME_CANDIDATE[1].critical_config_sha256 != CRITICAL_CONFIG_SHA256:
    raise RuntimeError("Существующий manifest несовместим с текущей критической конфигурацией.")
print("Model revision зафиксирована:", RESOLVED_MODEL_REVISION)
""",
        "contracts-and-revision",
    ),
    markdown("## 8. Проверка схемы и snapshot ровно из 100 постов", "snapshot-title"),
    code(
        r'''
REQUIRED_COLUMNS = {
    "group_posts": {"id", "group_id", "community_vk_id", "published_at", "text"},
    "group_candidates": {"id", "classification_status"},
    "group_labels": {"group_id", "label"},
    "post_attachments": {
        "post_id", "position", "attachment_type", "vk_owner_id", "vk_attachment_id",
        "access_key", "duration", "width", "height", "title", "external_url", "metadata",
    },
    "post_embeddings": {"post_id", "run_id", "model_name", "embedding_dim", "embedding_vector", "modality_profile"},
}

async def postgres_preflight(engine: AsyncEngine) -> dict[str, Any]:
    async with engine.connect() as connection:
        await connection.execute(text("SET TRANSACTION READ ONLY"))
        version = str((await connection.execute(text("SELECT version()"))).scalar_one())

        def inspect_schema(sync_connection: Any) -> dict[str, Any]:
            inspector = inspect(sync_connection)
            tables = set(inspector.get_table_names())
            return {
                "tables": sorted(tables),
                "columns": {
                    name: sorted({item["name"] for item in inspector.get_columns(name)}) if name in tables else []
                    for name in REQUIRED_COLUMNS
                },
                "unique_constraints": inspector.get_unique_constraints("post_embeddings") if "post_embeddings" in tables else [],
                "indexes": inspector.get_indexes("post_embeddings") if "post_embeddings" in tables else [],
            }

        schema = await connection.run_sync(inspect_schema)
        privileges = {}
        for table_name in REQUIRED_COLUMNS:
            privileges[table_name] = bool((await connection.execute(
                text("SELECT has_table_privilege(current_user, :table_name, 'SELECT')"),
                {"table_name": table_name},
            )).scalar_one())
        privileges["insert"] = bool((await connection.execute(
            text("SELECT has_table_privilege(current_user, 'post_embeddings', 'INSERT')")
        )).scalar_one())
        privileges["update"] = bool((await connection.execute(
            text("SELECT has_table_privilege(current_user, 'post_embeddings', 'UPDATE')")
        )).scalar_one())

    for table_name, required in REQUIRED_COLUMNS.items():
        actual = set(schema["columns"].get(table_name, []))
        if not required <= actual:
            raise RuntimeError(f"Таблица {table_name}: отсутствуют столбцы {sorted(required - actual)}")
        if not privileges[table_name]:
            raise PermissionError(f"Нет SELECT privilege для {table_name}.")
    if APPLY_DB_WRITES and not privileges["insert"]:
        raise PermissionError("Нет INSERT privilege для post_embeddings.")
    if APPLY_DB_WRITES and not privileges["update"]:
        raise PermissionError("Нет UPDATE privilege для post_embeddings.")

    unique_sets = [
        set(item.get("column_names") or [])
        for item in [*schema["unique_constraints"], *[index for index in schema["indexes"] if index.get("unique")]]
    ]
    if {"post_id"} not in unique_sets:
        raise RuntimeError("Не подтверждён conflict key post_embeddings(post_id).")
    return {"postgres_version": version, "privileges": privileges, "conflict_key": ["post_id"]}

POST_PAGE_SQL = text("""
WITH ranked AS (
    SELECT
        p.id AS post_id,
        p.group_id,
        p.community_vk_id,
        COALESCE(gl.label, 'customer_acquisition') AS subject,
        p.published_at,
        COALESCE(p.text, '') AS text,
        COALESCE(p.comments_count, 0) AS comments_count,
        COALESCE(p.likes_count, 0) AS likes_count,
        COALESCE(p.reposts_count, 0) AS reposts_count,
        COALESCE(p.views_count, 0) AS views_count,
        ROW_NUMBER() OVER (PARTITION BY p.group_id ORDER BY p.published_at DESC, p.id DESC) AS rank_in_group
    FROM group_posts p
    JOIN group_candidates g ON g.id = p.group_id
    LEFT JOIN (
        SELECT group_id, MIN(label) AS label
        FROM group_labels
        GROUP BY group_id
    ) gl ON gl.group_id = g.id
    WHERE g.classification_status = 'approved'
      AND p.published_at >= :cutoff
      AND p.id <= :high_water
      AND NOT EXISTS (
          SELECT 1 FROM post_embeddings existing_embedding
          WHERE existing_embedding.post_id = p.id
      )
)
SELECT *
FROM ranked
WHERE rank_in_group <= :max_posts_per_group
  AND MOD(post_id, CAST(:shard_count AS bigint)) = :shard_index
  AND post_id > :last_post_id
ORDER BY post_id
LIMIT :page_size
""")

ATTACHMENT_SQL = text("""
SELECT post_id, position, attachment_type, vk_owner_id, vk_attachment_id,
       access_key, duration, width, height, title, external_url, metadata AS attachment_metadata
FROM post_attachments
WHERE post_id IN :post_ids
ORDER BY post_id, position
""").bindparams(bindparam("post_ids", expanding=True))

def modality_for(text_value: str, attachments: Sequence[Attachment]) -> str:
    has_text = bool(text_value.strip())
    has_image = any(item.attachment_type == "photo" for item in attachments)
    has_video = any(item.attachment_type == "video" for item in attachments)
    if has_text and has_image and has_video:
        return "trimodal"
    if has_text and has_image:
        return "text_image"
    if has_text and has_video:
        return "text_video"
    if has_text:
        return "text_only"
    if has_image and has_video:
        return "image_video"
    if has_image:
        return "image_only"
    if has_video:
        return "video_only"
    return "empty"

def save_manifest(run_dir: Path, manifest: RunManifest) -> RunManifest:
    updated = manifest.model_copy(update={"updated_at_utc": utc_now()})
    atomic_json(run_dir / "run_manifest.json", updated.model_dump(mode="json"))
    return updated

async def select_or_create_manifest(engine: AsyncEngine, schema: dict[str, Any]) -> tuple[Path, RunManifest, bool]:
    if RESUME_CANDIDATE and not FORCE_NEW_RUN:
        run_dir, manifest = RESUME_CANDIDATE
        snapshot = run_dir / "dataset_snapshot.jsonl"
        if snapshot.exists() and manifest.snapshot_sha256 and file_sha256(snapshot) != manifest.snapshot_sha256:
            raise RuntimeError("SHA-256 snapshot не совпадает с manifest.")
        return run_dir, manifest, True

    cutoff = utc_now() - timedelta(days=WINDOW_DAYS)
    async with engine.connect() as connection:
        await connection.execute(text("SET TRANSACTION READ ONLY"))
        high_water = int((await connection.execute(text("""
            SELECT COALESCE(MAX(p.id), 0)
            FROM group_posts p
            JOIN group_candidates g ON g.id = p.group_id
            WHERE g.classification_status = 'approved' AND p.published_at >= :cutoff
        """), {"cutoff": cutoff})).scalar_one())
    if high_water <= 0:
        raise RuntimeError("Не найдены подходящие посты для snapshot.")

    run_id = f"vec-{utc_now():%Y%m%dT%H%M%SZ}-{uuid.uuid4().hex[:12]}"
    run_dir = STORAGE_ROOT / slugify(DATASET_NAME) / slugify(MODEL_NAME) / run_id
    run_dir.mkdir(parents=True, exist_ok=False)
    for name in ("run.log", "events.jsonl", "failures.jsonl", "committed_batches.jsonl", "resource_metrics.csv"):
        (run_dir / name).touch()
    manifest = RunManifest(
        run_id=run_id,
        dataset_name=DATASET_NAME,
        model_name=MODEL_NAME,
        model_revision=RESOLVED_MODEL_REVISION,
        embedding_dim=EMBEDDING_DIM,
        critical_config=CRITICAL_CONFIG,
        critical_config_sha256=CRITICAL_CONFIG_SHA256,
        snapshot_cutoff_utc=cutoff,
        snapshot_high_water_post_id=high_water,
    )
    return run_dir, save_manifest(run_dir, manifest), False

def restore_partial_snapshot(run_dir: Path, manifest: RunManifest) -> tuple[Any, int]:
    partial = run_dir / "dataset_snapshot.jsonl.partial"
    digest = hashlib.sha256()
    last_id = 0
    count = 0
    if not partial.exists():
        return digest, last_id
    with partial.open("rb") as stream:
        for raw_line in stream:
            digest.update(raw_line)
            post = Post.model_validate_json(raw_line)
            last_id = post.post_id
            count += 1
    if count != manifest.snapshot_record_count or last_id != manifest.snapshot_last_post_id:
        raise RuntimeError("Partial snapshot не совпадает с checkpoint manifest.")
    if count and digest.hexdigest() != manifest.snapshot_sha256:
        raise RuntimeError("SHA-256 partial snapshot не совпадает с manifest.")
    return digest, last_id

async def materialize_snapshot(engine: AsyncEngine, run_dir: Path, manifest: RunManifest, context: RunContext) -> RunManifest:
    final = run_dir / "dataset_snapshot.jsonl"
    if final.exists():
        return manifest
    partial = run_dir / "dataset_snapshot.jsonl.partial"
    digest, last_post_id = restore_partial_snapshot(run_dir, manifest)
    with partial.open("ab") as stream:
        while manifest.snapshot_record_count < TOTAL_POST_LIMIT:
            remaining = TOTAL_POST_LIMIT - manifest.snapshot_record_count
            page_size = min(SNAPSHOT_SQL_BATCH_SIZE, remaining)
            async with engine.connect() as connection:
                await connection.execute(text("SET TRANSACTION READ ONLY"))
                rows = (await connection.execute(POST_PAGE_SQL, {
                    "cutoff": manifest.snapshot_cutoff_utc,
                    "high_water": manifest.snapshot_high_water_post_id,
                    "max_posts_per_group": MAX_POSTS_PER_GROUP,
                    "shard_count": TEST_SHARD_COUNT,
                    "shard_index": TEST_SHARD_INDEX,
                    "last_post_id": last_post_id,
                    "page_size": page_size,
                })).mappings().all()
                ids = [int(row["post_id"]) for row in rows]
                by_post: dict[int, list[Attachment]] = defaultdict(list)
                for offset in range(0, len(ids), ATTACHMENT_SQL_BATCH_SIZE):
                    attachment_rows = (await connection.execute(
                        ATTACHMENT_SQL, {"post_ids": ids[offset:offset + ATTACHMENT_SQL_BATCH_SIZE]}
                    )).mappings().all()
                    for item in attachment_rows:
                        by_post[int(item["post_id"])].append(Attachment.model_validate(dict(item)))
            if not rows:
                break
            for row in rows:
                values = dict(row)
                post_id = int(values.pop("post_id"))
                values.pop("rank_in_group", None)
                attachments = by_post.get(post_id, [])
                post = Post(post_id=post_id, attachments=attachments, modality_profile=modality_for(values.get("text", ""), attachments), **values)
                raw = (post.model_dump_json() + "\n").encode("utf-8")
                stream.write(raw)
                stream.flush()
                digest.update(raw)
                last_post_id = post_id
                manifest = save_manifest(run_dir, manifest.model_copy(update={
                    "snapshot_last_post_id": last_post_id,
                    "snapshot_record_count": manifest.snapshot_record_count + 1,
                    "snapshot_sha256": digest.hexdigest(),
                }))
            context.event("snapshot_page", records=manifest.snapshot_record_count, limit=TOTAL_POST_LIMIT)

    if REQUIRE_EXACT_POST_COUNT and manifest.snapshot_record_count != TOTAL_POST_LIMIT:
        raise RuntimeError(f"Snapshot содержит {manifest.snapshot_record_count}, ожидалось ровно {TOTAL_POST_LIMIT} постов.")
    os.replace(partial, final)
    digest_path = run_dir / "dataset_snapshot.sha256"
    digest_path.write_text(manifest.snapshot_sha256 + "\n", encoding="utf-8")
    return save_manifest(run_dir, manifest.model_copy(update={"status": "ready"}))

def iter_snapshot(path: Path, skip: set[int] | None = None) -> Iterator[Post]:
    skipped = skip or set()
    with path.open("r", encoding="utf-8") as stream:
        for line in stream:
            if line.strip():
                post = Post.model_validate_json(line)
                if post.post_id not in skipped:
                    yield post
''',
        "schema-and-snapshot",
    ),
    markdown("## 9. Media: bounded download, MAD-кадры и contact sheet", "media-title"),
    code(
        r"""
@dataclass
class PreparedPost:
    post: Post
    model_input: str | dict[str, Any]
    degraded: bool

def resize_rgb(frame: np.ndarray, target: int = CONTACT_SHEET_TILE_SIZE) -> np.ndarray:
    image = Image.fromarray(np.asarray(frame, dtype=np.uint8)).convert("RGB")
    image.thumbnail((target, target), Image.Resampling.LANCZOS)
    return np.asarray(image)

def frame_mad(first: np.ndarray, second: np.ndarray) -> float:
    left = np.asarray(Image.fromarray(first).resize((128, 128))).astype(np.float32)
    right = np.asarray(Image.fromarray(second).resize((128, 128))).astype(np.float32)
    return float(np.mean(np.abs(left - right)) / 255.0)

def select_video_frames(frames: Sequence[np.ndarray]) -> list[np.ndarray]:
    if not frames:
        return []
    selected = [frames[0]]
    for frame in frames[1:]:
        if frame_mad(selected[-1], frame) > MAD_THETA:
            selected.append(frame)
        if len(selected) >= MAX_MEDIA_TILES:
            break
    return [resize_rgb(frame) for frame in selected]

class MediaResolver:
    def __init__(self, cache_dir: Path) -> None:
        self.cache_dir = cache_dir
        cache_dir.mkdir(parents=True, exist_ok=True)

    def path_for(self, attachment: Attachment) -> Path:
        suffix = ".jpg" if attachment.attachment_type == "photo" else ".mp4"
        return self.cache_dir / f"{attachment.attachment_type}_{attachment.vk_owner_id}_{attachment.vk_attachment_id}_{attachment.position}{suffix}"

    def load_image(self, path: Path) -> np.ndarray:
        with Image.open(path) as image:
            return resize_rgb(np.asarray(image.convert("RGB")))

    def load_video(self, path: Path) -> list[np.ndarray]:
        import cv2

        capture = cv2.VideoCapture(str(path))
        frames: list[np.ndarray] = []
        decoded_bytes = 0
        try:
            frame_count = int(capture.get(cv2.CAP_PROP_FRAME_COUNT) or 0)
            if frame_count > 0:
                positions = np.linspace(
                    0,
                    max(0, frame_count - 1),
                    num=min(MEDIA_MAX_VIDEO_FRAMES, frame_count),
                    dtype=np.int64,
                )
            else:
                positions = np.arange(MEDIA_MAX_VIDEO_FRAMES, dtype=np.int64)
            for position in positions:
                if frame_count > 0:
                    capture.set(cv2.CAP_PROP_POS_FRAMES, int(position))
                ok, frame = capture.read()
                if not ok:
                    if frame_count > 0:
                        continue
                    break
                rgb = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
                decoded_bytes += rgb.nbytes
                if decoded_bytes > MEDIA_MAX_DECODED_BYTES:
                    raise RuntimeError("Видео превысило bounded decoded-memory limit.")
                frames.append(rgb)
        finally:
            capture.release()
        if not frames:
            raise RuntimeError("Видео не декодировано.")
        return select_video_frames(frames)

    def validate(self, attachment: Attachment, path: Path) -> None:
        if attachment.attachment_type == "photo":
            self.load_image(path)
        elif attachment.attachment_type == "video":
            self.load_video(path)

    def prepare(self, post: Post, state: dict[str, Any]) -> PreparedPost:
        frames: list[np.ndarray] = []
        degraded = False
        for attachment in post.attachments:
            if attachment.attachment_type not in {"photo", "video"}:
                continue
            path = self.path_for(attachment)
            if not path.is_file():
                state["media_missing"] += 1
                degraded = True
                if MEDIA_MISSING_POLICY == "fail":
                    raise RuntimeError("Media cache miss.")
                continue
            try:
                if attachment.attachment_type == "photo":
                    frames.append(self.load_image(path))
                    state["images_used"] += 1
                else:
                    frames.extend(self.load_video(path))
                    state["videos_used"] += 1
            except Exception:
                state["media_skipped"] += 1
                degraded = True
                if MEDIA_MISSING_POLICY == "fail":
                    raise
        text_value = post.text.strip() or "VK company post without available text."
        sheet = make_contact_sheet(frames[:MAX_MEDIA_TILES]) if frames else None
        model_input: str | dict[str, Any] = text_value if sheet is None else {"text": text_value, "image": sheet}
        return PreparedPost(post=post, model_input=model_input, degraded=degraded)

def make_contact_sheet(frames: Sequence[np.ndarray]) -> Image.Image:
    if not frames:
        raise ValueError("Contact sheet требует хотя бы один frame.")
    columns = min(4, len(frames))
    rows = math.ceil(len(frames) / columns)
    canvas = Image.new("RGB", (columns * CONTACT_SHEET_TILE_SIZE, rows * CONTACT_SHEET_TILE_SIZE), "black")
    for index, frame in enumerate(frames):
        image = Image.fromarray(frame).convert("RGB")
        tile = ImageOps.pad(image, (CONTACT_SHEET_TILE_SIZE, CONTACT_SHEET_TILE_SIZE), method=Image.Resampling.LANCZOS, color="black")
        canvas.paste(tile, ((index % columns) * CONTACT_SHEET_TILE_SIZE, (index // columns) * CONTACT_SHEET_TILE_SIZE))
    return canvas

class VkApiError(RuntimeError):
    def __init__(self, method: str, code: int, message: str = "") -> None:
        self.method = method
        self.code = code
        safe_message = REDACTOR.redact(message)[:300]
        super().__init__(f"VK API {method}: code={code}; {safe_message}")

class VkMediaClient:
    RETRYABLE_CODES = {1, 6, 9, 10, 29}

    def __init__(self, client: Any, tokens: Sequence[str]) -> None:
        if not tokens:
            raise RuntimeError("Не задан ни один VK-токен для media resolution.")
        self.client = client
        self.tokens = list(tokens)
        self.next_request_at = [0.0 for _ in self.tokens]
        self.cursor = 0

    async def _token(self) -> str:
        index = self.cursor % len(self.tokens)
        self.cursor += 1
        wait_seconds = self.next_request_at[index] - time.monotonic()
        if wait_seconds > 0:
            await asyncio.sleep(wait_seconds)
        self.next_request_at[index] = time.monotonic() + (1.0 / VK_PER_TOKEN_RPS)
        return self.tokens[index]

    async def call(self, method: str, params: dict[str, Any]) -> Any:
        last_error: Exception | None = None
        for attempt in range(VK_API_MAX_RETRIES):
            token = await self._token()
            try:
                response = await self.client.post(
                    f"https://api.vk.com/method/{method}",
                    data={**params, "access_token": token, "v": VK_API_VERSION},
                )
                response.raise_for_status()
                payload = response.json()
                if not isinstance(payload, dict):
                    raise RuntimeError("VK API вернул не JSON object.")
                error_payload = payload.get("error")
                if isinstance(error_payload, dict):
                    code = int(error_payload.get("error_code") or 0)
                    message = str(error_payload.get("error_msg") or "")
                    error = VkApiError(method, code, message)
                    if code not in self.RETRYABLE_CODES and not (code == 5 and len(self.tokens) > 1):
                        raise error
                    last_error = error
                elif "response" in payload:
                    return payload["response"]
                else:
                    last_error = RuntimeError("VK API response не содержит response/error.")
            except VkApiError:
                raise
            except Exception as error:
                last_error = error
            if attempt + 1 < VK_API_MAX_RETRIES:
                await asyncio.sleep(min(8.0, 0.5 * (2 ** attempt)))
        raise RuntimeError(
            f"VK API {method} не ответил после {VK_API_MAX_RETRIES} попыток; "
            f"последняя ошибка: {type(last_error).__name__}."
        ) from last_error

    @staticmethod
    def reference(attachment: Attachment) -> str:
        if attachment.vk_owner_id is None or attachment.vk_attachment_id is None:
            raise RuntimeError("У photo/video отсутствует VK owner/media ID.")
        value = f"{attachment.vk_owner_id}_{attachment.vk_attachment_id}"
        if attachment.access_key:
            value += f"_{attachment.access_key}"
        return value

    async def resolve_urls(self, attachment: Attachment) -> list[str]:
        if attachment.external_url:
            return [attachment.external_url]
        reference = self.reference(attachment)
        if attachment.attachment_type == "photo":
            response = await self.call(
                "photos.getById",
                {"photos": reference, "photo_sizes": 1},
            )
            items = response if isinstance(response, list) else []
            if not items:
                raise RuntimeError("VK API не вернул запрошенное фото.")
            sizes = items[0].get("sizes") if isinstance(items[0], dict) else None
            candidates = [
                item for item in (sizes or [])
                if isinstance(item, dict) and str(item.get("url") or "").startswith("https://")
            ]
            if not candidates:
                raise RuntimeError("VK API не вернул HTTPS URL фото.")
            ordered = sorted(
                candidates,
                key=lambda item: (int(item.get("width") or 0) * int(item.get("height") or 0)),
                reverse=True,
            )
            return [str(item["url"]) for item in ordered]
        if attachment.attachment_type == "video":
            response = await self.call("video.get", {"videos": reference})
            items = response.get("items") if isinstance(response, dict) else None
            if not items or not isinstance(items[0], dict):
                raise RuntimeError("VK API не вернул запрошенное видео.")
            files = items[0].get("files")
            if not isinstance(files, dict):
                raise RuntimeError("VK API не вернул downloadable video files.")
            candidates: list[tuple[int, str]] = []
            for name, value in files.items():
                match = re.fullmatch(r"mp4_(\d+)", str(name))
                if match and str(value).startswith("https://"):
                    height = int(match.group(1))
                    if height <= 720:
                        candidates.append((height, str(value)))
            if not candidates:
                raise RuntimeError("VK API не вернул bounded HTTPS MP4 (до 720p).")
            return [url for _, url in sorted(candidates, key=lambda item: item[0], reverse=True)]
        raise RuntimeError(f"Неподдерживаемый media type: {attachment.attachment_type}.")

class MediaTooLargeError(RuntimeError):
    pass

async def download_media(media_url: str, target: Path, client: Any) -> int:
    if not media_url.startswith("https://"):
        raise RuntimeError("Разрешена загрузка media только по HTTPS.")
    temporary = target.with_name(f".{target.name}.{uuid.uuid4().hex}.partial")
    try:
        async with client.stream("GET", media_url) as response:
            response.raise_for_status()
            content_length = int(response.headers.get("content-length") or 0)
            if content_length > MEDIA_MAX_FILE_BYTES:
                raise MediaTooLargeError("Media превышает max file size по Content-Length.")
            total = 0
            with temporary.open("wb") as stream:
                async for chunk in response.aiter_bytes():
                    total += len(chunk)
                    if total > MEDIA_MAX_FILE_BYTES:
                        raise MediaTooLargeError("Media превышает max file size.")
                    stream.write(chunk)
        os.replace(temporary, target)
        return total
    finally:
        temporary.unlink(missing_ok=True)

async def media_preflight(snapshot: Path, resolver: MediaResolver, run_dir: Path) -> dict[str, Any]:
    import httpx

    report = {
        "source_mode": MEDIA_SOURCE_MODE,
        "missing_policy": MEDIA_MISSING_POLICY,
        "cache_hits": 0,
        "cache_invalid": 0,
        "vk_api_resolved": 0,
        "external_url_used": 0,
        "downloaded": 0,
        "download_failures": 0,
        "media_bytes": 0,
        "degraded_posts": 0,
        "failure_classes": {},
    }
    degraded_posts: set[int] = set()
    client = httpx.AsyncClient(
        timeout=httpx.Timeout(MEDIA_DOWNLOAD_TIMEOUT_SECONDS),
        follow_redirects=True,
        headers={"User-Agent": "vk-research-collector-vectorization/1.0"},
    )
    vk_media = VkMediaClient(client, VK_TOKENS)
    try:
        for post in iter_snapshot(snapshot):
            for attachment in post.attachments:
                if attachment.attachment_type not in {"photo", "video"}:
                    continue
                target = resolver.path_for(attachment)
                try:
                    if target.is_file():
                        try:
                            resolver.validate(attachment, target)
                            report["cache_hits"] += 1
                            continue
                        except Exception:
                            target.unlink(missing_ok=True)
                            report["cache_invalid"] += 1
                    if MEDIA_SOURCE_MODE != "vk_api":
                        raise RuntimeError("Media cache miss, а VK API resolution выключен.")
                    last_error: Exception | None = None
                    media_urls = await vk_media.resolve_urls(attachment)
                    for media_url in media_urls:
                        for attempt in range(MEDIA_DOWNLOAD_RETRIES):
                            try:
                                size = await download_media(media_url, target, client)
                                resolver.validate(attachment, target)
                                if attachment.external_url:
                                    report["external_url_used"] += 1
                                else:
                                    report["vk_api_resolved"] += 1
                                report["downloaded"] += 1
                                report["media_bytes"] += size
                                last_error = None
                                break
                            except Exception as error:
                                last_error = error
                                target.unlink(missing_ok=True)
                                if isinstance(error, MediaTooLargeError):
                                    break
                                if attempt + 1 < MEDIA_DOWNLOAD_RETRIES:
                                    await asyncio.sleep(0.5 * (2 ** attempt))
                        if last_error is None:
                            break
                    if last_error is not None:
                        raise last_error
                except Exception as error:
                    report["download_failures"] += 1
                    degraded_posts.add(post.post_id)
                    name = type(error).__name__
                    report["failure_classes"][name] = int(report["failure_classes"].get(name, 0)) + 1
        report["degraded_posts"] = len(degraded_posts)
        atomic_json(run_dir / "media_preflight.json", report)
        if degraded_posts and MEDIA_MISSING_POLICY == "fail":
            raise RuntimeError(
                f"Media preflight не пройден для {len(degraded_posts)} постов; "
                "DB-векторизация не начиналась. Проверьте media_preflight.json."
            )
        return report
    finally:
        await client.aclose()
""",
        "media-processing",
    ),
    markdown("## 10. Официальный Qwen loader, строгая проверка весов и GPU batch", "encoder-title"),
    code(
        r"""
from huggingface_hub import hf_hub_download

def validate_embeddings(value: Any, expected_rows: int) -> np.ndarray:
    matrix = np.asarray(value, dtype=np.float32)
    if matrix.shape != (expected_rows, EMBEDDING_DIM):
        raise ValueError(f"Получена форма {matrix.shape}, ожидалась {(expected_rows, EMBEDDING_DIM)}")
    if not np.isfinite(matrix).all():
        raise ValueError("Embeddings содержат NaN/Inf.")
    norms = np.linalg.norm(matrix, axis=1)
    if np.any(norms < 1e-8):
        raise ValueError("Embeddings содержат zero vector.")
    matrix = matrix / norms[:, None]
    return np.asarray(matrix, dtype=np.float32)

class QwenEncoder:
    def __init__(self) -> None:
        self.runtime: Any | None = None
        self.dtype = torch.bfloat16 if RESOLVED_PRECISION == "bfloat16" else torch.float16
        self.loading_report: dict[str, Any] = {}
        self.model_logger: logging.Logger | None = None

    @staticmethod
    def official_module() -> Any:
        implementation_path = Path(hf_hub_download(
            repo_id=MODEL_NAME,
            filename=MODEL_IMPLEMENTATION_FILE,
            revision=RESOLVED_MODEL_REVISION,
            token=SECRETS.get("HF_TOKEN"),
        ))
        actual_sha256 = file_sha256(implementation_path)
        if actual_sha256 != MODEL_IMPLEMENTATION_SHA256:
            raise RuntimeError(
                "SHA-256 официальной реализации Qwen не совпал с закреплённым значением."
            )
        module_name = "pinned_qwen3_vl_embedding"
        spec = importlib.util.spec_from_file_location(module_name, implementation_path)
        if spec is None or spec.loader is None:
            raise RuntimeError("Не удалось создать import spec официальной реализации Qwen.")
        module = importlib.util.module_from_spec(spec)
        sys.modules[module_name] = module
        spec.loader.exec_module(module)
        return module

    def load(self) -> None:
        module = self.official_module()
        model, loading_info = module.Qwen3VLForEmbedding.from_pretrained(
            MODEL_NAME,
            revision=RESOLVED_MODEL_REVISION,
            token=SECRETS.get("HF_TOKEN"),
            torch_dtype=self.dtype,
            low_cpu_mem_usage=True,
            output_loading_info=True,
        )
        problem_counts = {
            "missing_keys": len(loading_info.get("missing_keys") or []),
            "unexpected_keys": len(loading_info.get("unexpected_keys") or []),
            "mismatched_keys": len(loading_info.get("mismatched_keys") or []),
            "error_messages": len(loading_info.get("error_msgs") or []),
        }
        if any(problem_counts.values()):
            del model
            with contextlib.suppress(Exception):
                torch.cuda.empty_cache()
            raise RuntimeError(
                "Checkpoint Qwen загружен нестрого: "
                + ", ".join(f"{name}={count}" for name, count in problem_counts.items())
                + ". Запись эмбеддингов запрещена."
            )

        model = model.to("cuda")
        model.eval()
        processor = module.Qwen3VLProcessor.from_pretrained(
            MODEL_NAME,
            revision=RESOLVED_MODEL_REVISION,
            token=SECRETS.get("HF_TOKEN"),
            padding_side="right",
        )
        runtime = module.Qwen3VLEmbedder.__new__(module.Qwen3VLEmbedder)
        runtime.max_length = module.MAX_LENGTH
        runtime.min_pixels = module.MIN_PIXELS
        runtime.max_pixels = module.MAX_PIXELS
        runtime.total_pixels = module.MAX_TOTAL_PIXELS
        runtime.fps = module.FPS
        runtime.num_frames = module.MAX_FRAMES
        runtime.max_frames = module.MAX_FRAMES
        runtime.default_instruction = MODEL_INSTRUCTION
        runtime.model = model
        runtime.processor = processor
        self.runtime = runtime
        self.model_logger = module.logger
        self.loading_report = {
            "loader": "official_qwen3_vl_embedding",
            "implementation_file": MODEL_IMPLEMENTATION_FILE,
            "implementation_sha256": MODEL_IMPLEMENTATION_SHA256,
            "strict_checkpoint": True,
            "parameter_count": int(model.num_parameters()),
            **problem_counts,
        }

    def encode(self, items: Sequence[PreparedPost], batch_size: int) -> np.ndarray:
        if self.runtime is None:
            raise RuntimeError("Модель не загружена.")
        del batch_size  # Размер батча уже ограничен вызывающим adaptive runner.
        payloads: list[dict[str, Any]] = []
        for item in items:
            if isinstance(item.model_input, dict):
                payload = dict(item.model_input)
            else:
                payload = {"text": str(item.model_input)}
            payload["instruction"] = MODEL_INSTRUCTION
            payloads.append(payload)
        preprocessing_errors: list[str] = []

        class PreprocessingErrorHandler(logging.Handler):
            def emit(self, record: logging.LogRecord) -> None:
                if record.levelno >= logging.ERROR:
                    preprocessing_errors.append(record.getMessage())

        handler = PreprocessingErrorHandler()
        if self.model_logger is not None:
            self.model_logger.addHandler(handler)
        try:
            result = self.runtime.process(payloads, normalize=True)
        finally:
            if self.model_logger is not None:
                self.model_logger.removeHandler(handler)
        if preprocessing_errors:
            raise RuntimeError(
                "Официальный Qwen preprocessor попытался заменить media/input на NULL; "
                "батч отклонён до записи в БД."
            )
        if isinstance(result, torch.Tensor):
            result = result.detach().float().cpu().numpy()
        return validate_embeddings(result, len(items))

def is_cuda_oom(error: BaseException) -> bool:
    return isinstance(error, torch.OutOfMemoryError) or ("cuda" in str(error).lower() and "out of memory" in str(error).lower())

def gpu_environment() -> dict[str, Any]:
    return {
        "torch": torch.__version__,
        "cuda_runtime": torch.version.cuda,
        "cudnn": torch.backends.cudnn.version(),
        "gpu_name": GPU_NAME,
        "gpu_count": torch.cuda.device_count(),
        "vram_total_bytes": GPU_VRAM_BYTES,
        "bf16_supported": BF16_SUPPORTED,
        "precision": RESOLVED_PRECISION,
    }

def real_gpu_preflight(encoder: QwenEncoder) -> dict[str, Any]:
    frame = np.zeros((64, 64, 3), dtype=np.uint8)
    post = Post(
        post_id=-1,
        group_id=None,
        community_vk_id=-1,
        subject="gpu_preflight",
        published_at=utc_now(),
        text="Проверка реальной GPU модели.",
        modality_profile="text_image",
    )
    sample = PreparedPost(post, {"text": post.text, "image": Image.fromarray(frame)}, False)
    torch.cuda.reset_peak_memory_stats()
    started = time.perf_counter()
    matrix = encoder.encode([sample], 1)
    return {
        "shape": list(matrix.shape),
        "seconds": time.perf_counter() - started,
        "peak_vram_bytes": int(torch.cuda.max_memory_allocated()),
        "checkpoint_loading": encoder.loading_report,
    }
""",
        "qwen-encoder",
    ),
    markdown("## 11. DB-based resume, UPSERT и retry", "writer-title"),
    code(
        r'''
@dataclass
class EncodedPost:
    post: Post
    embedding: np.ndarray

POST_EMBEDDINGS = table_clause(
    "post_embeddings",
    column("post_id", BigInteger),
    column("run_id", String(64)),
    column("model_name", String(255)),
    column("embedding_dim", Integer),
    column("embedding_vector", JSONB),
    column("modality_profile", String(50)),
    column("updated_at"),
)

def sqlstate_of(error: BaseException) -> str | None:
    for candidate in (error, getattr(error, "orig", None), getattr(getattr(error, "orig", None), "__cause__", None)):
        if candidate is not None:
            value = getattr(candidate, "sqlstate", None) or getattr(candidate, "pgcode", None)
            if value:
                return str(value)
    return None

def retryable_db_error(error: BaseException) -> bool:
    state = sqlstate_of(error)
    if isinstance(error, OperationalError):
        return True
    if isinstance(error, DBAPIError) and (error.connection_invalidated or (state and (state.startswith("08") or state in {"40001", "40P01", "57014"}))):
        return True
    if isinstance(error, (TimeoutError, ConnectionError, asyncio.TimeoutError)):
        return True
    if isinstance(error, (IntegrityError, ProgrammingError, StatementError, ValueError, TypeError)):
        return False
    return bool(state and (state.startswith("08") or state in {"40001", "40P01", "57014"}))

class PostgresWriter:
    def __init__(self, engine: AsyncEngine) -> None:
        self.engine = engine

    async def completed_ids(self, run_id: str, model_name: str) -> set[int]:
        values: set[int] = set()
        async with self.engine.connect() as connection:
            await connection.execute(text("SET TRANSACTION READ ONLY"))
            result = await connection.stream(text("""
                SELECT post_id
                FROM post_embeddings
                WHERE run_id = :run_id AND model_name = :model_name
                ORDER BY post_id
            """), {"run_id": run_id, "model_name": model_name})
            async for partition in result.partitions(1000):
                values.update(int(row.post_id) for row in partition)
        return values

    async def foreign_conflicts(self, post_ids: Sequence[int], run_id: str, model_name: str) -> set[int]:
        if not post_ids:
            return set()
        statement = text("""
            SELECT post_id
            FROM post_embeddings
            WHERE post_id IN :ids AND (run_id <> :run_id OR model_name <> :model_name)
        """).bindparams(bindparam("ids", expanding=True))
        async with self.engine.connect() as connection:
            await connection.execute(text("SET TRANSACTION READ ONLY"))
            rows = (await connection.execute(statement, {"ids": list(post_ids), "run_id": run_id, "model_name": model_name})).all()
        return {int(row.post_id) for row in rows}

    async def upsert(self, items: Sequence[EncodedPost], run_id: str, model_name: str) -> set[int]:
        if not APPLY_DB_WRITES or SECRETS.get("VECTORIZATION_CONFIRMATION") != WRITE_CONFIRMATION:
            raise PermissionError("DB write gate не пройден.")
        expected = {item.post.post_id for item in items}
        if len(expected) != len(items):
            raise ValueError("DB batch содержит duplicate post_id.")
        conflicts = await self.foreign_conflicts(list(expected), run_id, model_name)
        if conflicts:
            raise IntegrityError("post_id уже принадлежит другому run/model", {}, RuntimeError("foreign ownership"))

        values = [{
            "post_id": item.post.post_id,
            "run_id": run_id,
            "model_name": model_name,
            "embedding_dim": EMBEDDING_DIM,
            "embedding_vector": item.embedding.tolist(),
            "modality_profile": item.post.modality_profile,
        } for item in items]
        statement = insert(POST_EMBEDDINGS).values(values)
        statement = statement.on_conflict_do_update(
            index_elements=[POST_EMBEDDINGS.c.post_id],
            set_={
                "embedding_dim": statement.excluded.embedding_dim,
                "embedding_vector": statement.excluded.embedding_vector,
                "modality_profile": statement.excluded.modality_profile,
                "updated_at": text("now()"),
            },
            where=(POST_EMBEDDINGS.c.run_id == run_id) & (POST_EMBEDDINGS.c.model_name == model_name),
        ).returning(POST_EMBEDDINGS.c.post_id)

        async with self.engine.connect() as connection:
            transaction = await connection.begin()
            try:
                returned = {int(row.post_id) for row in (await connection.execute(statement)).all()}
                if returned != expected:
                    raise RuntimeError("UPSERT RETURNING не совпал с ожидаемым batch.")
                await transaction.commit()
                return returned
            except Exception:
                await transaction.rollback()
                raise

async def upsert_with_retry(writer: PostgresWriter, items: Sequence[EncodedPost], manifest: RunManifest, context: RunContext) -> set[int]:
    for attempt in range(1, DB_MAX_RETRIES + 1):
        context.event("db_transaction_start", attempt=attempt, count=len(items))
        try:
            returned = await writer.upsert(items, manifest.run_id, manifest.model_name)
            context.event("db_transaction_commit", attempt=attempt, count=len(returned))
            return returned
        except Exception as error:
            context.event("db_transaction_rollback", attempt=attempt, error_class=type(error).__name__)
            if not retryable_db_error(error) or attempt == DB_MAX_RETRIES:
                raise
            context.state["retries"] += 1
            await asyncio.sleep(min(8.0, 0.5 * (2 ** (attempt - 1))) + random.random() * 0.2)
    raise RuntimeError("Недостижимое retry state.")
''',
        "database-writer",
    ),
    markdown("## 12. Монитор ресурсов и resumable runner", "runner-title"),
    code(
        r"""
def initial_state() -> dict[str, Any]:
    return {
        "processed": 0,
        "calculated": 0,
        "committed_this_session": 0,
        "existing_db_rows": 0,
        "failed": 0,
        "degraded": 0,
        "retries": 0,
        "db_transactions": 0,
        "effective_gpu_batch": GPU_BATCH_SIZE,
        "images_used": 0,
        "videos_used": 0,
        "media_missing": 0,
        "media_skipped": 0,
        "peak_ram_bytes": 0,
        "peak_vram_bytes": 0,
        "durations": defaultdict(float),
    }

def nvidia_smi_sample() -> dict[str, Any]:
    try:
        output = subprocess.check_output([
            "nvidia-smi", "--query-gpu=utilization.gpu,temperature.gpu,memory.used,memory.total",
            "--format=csv,noheader,nounits",
        ], text=True, timeout=5).strip().splitlines()[0]
        utilization, temperature, used, total = [value.strip() for value in output.split(",")]
        return {
            "gpu_utilization_percent": float(utilization),
            "gpu_temperature_c": float(temperature),
            "gpu_memory_used_mib": float(used),
            "gpu_memory_total_mib": float(total),
        }
    except Exception:
        return {}

class ResourceMonitor:
    def __init__(self, path: Path, state: dict[str, Any]) -> None:
        self.path = path
        self.state = state
        self.stop_event = threading.Event()
        self.thread: threading.Thread | None = None

    def sample(self) -> None:
        process = psutil.Process()
        disk = psutil.disk_usage(str(PROJECT_STORAGE))
        row = {
            "timestamp_utc": utc_now().isoformat(),
            "process_rss_bytes": process.memory_info().rss,
            "system_ram_percent": psutil.virtual_memory().percent,
            "cpu_percent": psutil.cpu_percent(),
            "disk_free_bytes": disk.free,
            "vram_allocated_bytes": int(torch.cuda.memory_allocated()),
            "vram_reserved_bytes": int(torch.cuda.memory_reserved()),
            "vram_peak_allocated_bytes": int(torch.cuda.max_memory_allocated()),
            **nvidia_smi_sample(),
        }
        self.state["peak_ram_bytes"] = max(self.state["peak_ram_bytes"], int(row["process_rss_bytes"]))
        self.state["peak_vram_bytes"] = max(self.state["peak_vram_bytes"], int(row["vram_peak_allocated_bytes"]))
        new_file = self.path.stat().st_size == 0
        with self.path.open("a", newline="", encoding="utf-8") as stream:
            writer = csv.DictWriter(stream, fieldnames=list(row))
            if new_file:
                writer.writeheader()
            writer.writerow(row)

    def start(self) -> None:
        self.sample()
        self.thread = threading.Thread(target=self._loop, daemon=True)
        self.thread.start()

    def _loop(self) -> None:
        while not self.stop_event.wait(RESOURCE_SAMPLE_INTERVAL_SECONDS):
            with contextlib.suppress(Exception):
                self.sample()

    def stop(self) -> None:
        self.stop_event.set()
        if self.thread:
            self.thread.join(timeout=5)
        with contextlib.suppress(Exception):
            self.sample()

STOP_REQUESTED = False

def request_stop(*_: Any) -> None:
    global STOP_REQUESTED
    STOP_REQUESTED = True

for signal_name in ("SIGINT", "SIGTERM"):
    if hasattr(signal, signal_name):
        with contextlib.suppress(ValueError):
            signal.signal(getattr(signal, signal_name), request_stop)

class ResumableRunner:
    def __init__(self, encoder: QwenEncoder, resolver: MediaResolver, writer: PostgresWriter, run_dir: Path, manifest: RunManifest, context: RunContext) -> None:
        self.encoder = encoder
        self.resolver = resolver
        self.writer = writer
        self.run_dir = run_dir
        self.manifest = manifest
        self.context = context
        self.buffer: list[EncodedPost] = []
        self.effective_batch = manifest.effective_gpu_batch

    def prepare(self, posts: Sequence[Post]) -> list[PreparedPost]:
        prepared: list[PreparedPost] = []
        for post in posts:
            try:
                item = self.resolver.prepare(post, self.context.state)
                prepared.append(item)
            except Exception as error:
                self.context.state["failed"] += 1
                append_jsonl(self.run_dir / "failures.jsonl", {
                    "timestamp_utc": utc_now().isoformat(),
                    "post_id": post.post_id,
                    "stage": "media",
                    "error_class": type(error).__name__,
                })
        return prepared

    def encode_adaptive(self, items: Sequence[PreparedPost]) -> list[EncodedPost]:
        if not items:
            return []
        try:
            started = time.perf_counter()
            matrix = self.encoder.encode(items, min(self.effective_batch, len(items)))
            self.context.state["durations"]["gpu_inference"] += time.perf_counter() - started
            return [EncodedPost(item.post, vector) for item, vector in zip(items, matrix, strict=True)]
        except Exception as error:
            if is_cuda_oom(error) and self.effective_batch > GPU_MIN_BATCH_SIZE:
                torch.cuda.empty_cache()
                self.effective_batch = max(GPU_MIN_BATCH_SIZE, self.effective_batch // 2)
                self.context.state["effective_gpu_batch"] = self.effective_batch
                self.manifest = save_manifest(self.run_dir, self.manifest.model_copy(update={"effective_gpu_batch": self.effective_batch}))
                self.context.event("gpu_batch_reduced", effective_gpu_batch=self.effective_batch)
                result: list[EncodedPost] = []
                for offset in range(0, len(items), self.effective_batch):
                    result.extend(self.encode_adaptive(items[offset:offset + self.effective_batch]))
                return result
            for item in items:
                self.context.state["failed"] += 1
                append_jsonl(self.run_dir / "failures.jsonl", {
                    "timestamp_utc": utc_now().isoformat(),
                    "post_id": item.post.post_id,
                    "stage": "embedding",
                    "error_class": type(error).__name__,
                })
            return []

    async def commit_prefix(self, size: int) -> None:
        pending = self.buffer[:size]
        returned = await upsert_with_retry(self.writer, pending, self.manifest, self.context)
        expected = {item.post.post_id for item in pending}
        if returned != expected:
            raise RuntimeError("DB confirmation set mismatch.")
        del self.buffer[:size]
        self.context.state["committed_this_session"] += len(returned)
        self.context.state["db_transactions"] += 1
        append_jsonl(self.run_dir / "committed_batches.jsonl", {
            "timestamp_utc": utc_now().isoformat(),
            "count": len(returned),
            "post_ids_sha256": canonical_sha256(sorted(returned)),
        })

    async def flush(self, final: bool = False) -> None:
        while len(self.buffer) >= DB_BATCH_SIZE:
            await self.commit_prefix(DB_BATCH_SIZE)
        if final and self.buffer:
            await self.commit_prefix(len(self.buffer))

    async def run(self, posts: Iterator[Post]) -> None:
        iterator = iter(posts)
        while not STOP_REQUESTED:
            batch: list[Post] = []
            for _ in range(self.effective_batch):
                try:
                    batch.append(next(iterator))
                except StopIteration:
                    break
            if not batch:
                break
            prepared = self.prepare(batch)
            encoded = self.encode_adaptive(prepared)
            self.buffer.extend(encoded)
            self.context.state["processed"] += len(batch)
            self.context.state["calculated"] += len(encoded)
            await self.flush()
            self.context.event(
                "progress",
                processed=self.context.state["processed"],
                total=self.manifest.snapshot_record_count,
                calculated=self.context.state["calculated"],
                buffered=len(self.buffer),
            )
        await self.flush(final=True)
""",
        "runner-and-monitor",
    ),
    markdown("## 13. Основной pipeline", "pipeline-title"),
    code(
        r"""
def collect_environment() -> dict[str, Any]:
    return {
        "timestamp_utc": utc_now().isoformat(),
        "environment": "google_colab",
        "python": sys.version,
        "platform": platform.platform(),
        "hostname": socket.gethostname(),
        "cpu_logical": psutil.cpu_count(),
        "ram_total_bytes": psutil.virtual_memory().total,
        "storage_root": str(STORAGE_ROOT),
        "packages": {
            name: importlib.metadata.version(name)
            for name in ("torch", "transformers", "qwen-vl-utils", "huggingface-hub", "sqlalchemy", "asyncpg", "asyncssh")
        },
        "gpu": gpu_environment(),
    }

async def run_pipeline() -> dict[str, Any]:
    started_at = utc_now()
    state = initial_state()
    tunnel: SshTunnel | None = None
    engine: AsyncEngine | None = None
    monitor: ResourceMonitor | None = None
    context: RunContext | None = None
    run_dir: Path | None = None
    manifest: RunManifest | None = None
    runner: ResumableRunner | None = None
    try:
        tunnel = await open_ssh_tunnel()
        database_url = build_database_url("127.0.0.1", tunnel.local_port) if tunnel else build_database_url()
        print("PostgreSQL:", masked_database_url(database_url))
        engine = create_engine(database_url)
        schema = await postgres_preflight(engine)
        run_dir, manifest, resumed = await select_or_create_manifest(engine, schema)
        state["effective_gpu_batch"] = manifest.effective_gpu_batch
        logger = configure_logger(run_dir / "run.log")
        context = RunContext(run_dir, logger, state)
        context.event("run_resumed" if resumed else "run_created", run_id=manifest.run_id)
        atomic_json(run_dir / "environment.json", collect_environment())
        atomic_json(run_dir / "database_preflight.json", schema)

        snapshot_started = time.perf_counter()
        manifest = await materialize_snapshot(engine, run_dir, manifest, context)
        state["durations"]["snapshot"] = time.perf_counter() - snapshot_started
        snapshot_path = run_dir / "dataset_snapshot.jsonl"
        all_ids = {post.post_id for post in iter_snapshot(snapshot_path)}
        if len(all_ids) != TOTAL_POST_LIMIT:
            raise RuntimeError(f"Snapshot содержит {len(all_ids)} уникальных ID вместо {TOTAL_POST_LIMIT}.")

        writer = PostgresWriter(engine)
        completed = (await writer.completed_ids(manifest.run_id, manifest.model_name)) & all_ids
        state["existing_db_rows"] = len(completed)
        context.event("db_resume", completed=len(completed), remaining=len(all_ids - completed))
        if manifest.status == "completed" and completed == all_ids:
            summary = {
                "run_id": manifest.run_id,
                "final_status": "completed",
                "no_op": True,
                "snapshot_posts": len(all_ids),
                "existing_db_rows": len(completed),
                "committed_this_session": 0,
                "confirmed_total": len(completed),
                "remaining_count": 0,
            }
            atomic_json(run_dir / "summary.json", summary)
            return summary

        conflicts = await writer.foreign_conflicts(sorted(all_ids - completed), manifest.run_id, manifest.model_name)
        if conflicts:
            raise IntegrityError(
                f"{len(conflicts)} post_id уже принадлежат другому run/model; выберите непересекающийся shard.",
                {},
                RuntimeError("foreign ownership"),
            )

        resolver = MediaResolver(MEDIA_CACHE)
        media_report = await media_preflight(snapshot_path, resolver, run_dir)
        state["degraded"] = int(media_report["degraded_posts"])

        encoder = QwenEncoder()
        model_started = time.perf_counter()
        encoder.load()
        state["durations"]["model_load"] = time.perf_counter() - model_started
        gpu_check = real_gpu_preflight(encoder)
        atomic_json(run_dir / "gpu_preflight.json", gpu_check)
        context.event("gpu_preflight_passed", **gpu_check)

        monitor = ResourceMonitor(run_dir / "resource_metrics.csv", state)
        monitor.start()
        manifest = save_manifest(run_dir, manifest.model_copy(update={"status": "running"}))
        runner = ResumableRunner(encoder, resolver, writer, run_dir, manifest, context)
        await runner.run(iter_snapshot(snapshot_path, completed))
        manifest = runner.manifest

        confirmed = (await writer.completed_ids(manifest.run_id, manifest.model_name)) & all_ids
        missing = all_ids - confirmed
        status = "interrupted" if STOP_REQUESTED else ("completed" if not missing and state["failed"] == 0 else "incomplete")
        manifest = save_manifest(run_dir, manifest.model_copy(update={"status": status}))
        total_seconds = (utc_now() - started_at).total_seconds()
        summary = {
            "run_id": manifest.run_id,
            "dataset_name": manifest.dataset_name,
            "model_name": manifest.model_name,
            "model_revision": manifest.model_revision,
            "final_status": status,
            "no_op": False,
            "snapshot_posts": manifest.snapshot_record_count,
            "processed": state["processed"],
            "calculated": state["calculated"],
            "existing_db_rows": state["existing_db_rows"],
            "committed_this_session": state["committed_this_session"],
            "confirmed_total": len(confirmed),
            "remaining_count": len(missing),
            "failed_count": state["failed"],
            "degraded_count": state["degraded"],
            "db_transactions": state["db_transactions"],
            "db_batch_size": DB_BATCH_SIZE,
            "effective_gpu_batch": state["effective_gpu_batch"],
            "peak_ram_bytes": state["peak_ram_bytes"],
            "peak_vram_bytes": state["peak_vram_bytes"],
            "durations_seconds": {**dict(state["durations"]), "total": total_seconds},
            "artifacts_directory": str(run_dir),
        }
        atomic_json(run_dir / "summary.json", summary)
        context.event("final_summary", status=status, confirmed=len(confirmed), remaining=len(missing))
        return summary
    except (KeyboardInterrupt, asyncio.CancelledError):
        request_stop()
        if runner:
            with contextlib.suppress(Exception):
                await asyncio.shield(runner.flush(final=True))
        if run_dir and manifest:
            save_manifest(run_dir, manifest.model_copy(update={"status": "interrupted"}))
        raise
    except Exception as error:
        if run_dir and manifest:
            save_manifest(run_dir, manifest.model_copy(update={"status": "failed"}))
            atomic_json(run_dir / "summary.json", {
                "run_id": manifest.run_id,
                "final_status": "failed",
                "error_class": type(error).__name__,
                "artifacts_directory": str(run_dir),
            })
        if context:
            context.event("run_failed", error_class=type(error).__name__)
        raise
    finally:
        if monitor:
            monitor.stop()
        if engine:
            await engine.dispose()
        if tunnel:
            await tunnel.close()
        if context:
            for handler in list(context.logger.handlers):
                handler.close()
                context.logger.removeHandler(handler)
""",
        "pipeline",
    ),
    markdown("## 14. Запуск", "run-title"),
    code(
        r"""
PIPELINE_SUMMARY = await run_pipeline() if EXECUTE_PIPELINE else None

if PIPELINE_SUMMARY:
    print(json.dumps(PIPELINE_SUMMARY, ensure_ascii=False, indent=2, default=str))
else:
    print("Pipeline выключен: EXECUTE_PIPELINE=False.")
""",
        "execute",
    ),
    markdown(
        r"""
## 15. Как проверить результат

Успешный первый запуск должен показать в `summary.json`:

```text
final_status = completed
snapshot_posts = 100
committed_this_session = 100
confirmed_total = 100
remaining_count = 0
db_transactions = 1
db_batch_size = 500
degraded_count = 0
```

Повторный `Runtime → Run all` должен найти тот же manifest и вернуть `no_op = true`,
`existing_db_rows = 100`, `committed_this_session = 0`.

Артефакты находятся по пути:

```text
/content/drive/MyDrive/vk-research-collector/vectorization_runs/
  <dataset>/<model>/<run_id>/
```

Основные файлы: `run_manifest.json`, `environment.json`, `gpu_preflight.json`,
`database_preflight.json`, `dataset_snapshot.jsonl`, `media_preflight.json`, `run.log`, `events.jsonl`,
`resource_metrics.csv`, `committed_batches.jsonl`, `failures.jsonl`, `summary.json`.

До DB-векторизации `media_preflight.json` должен показать `degraded_posts = 0` и
`download_failures = 0`. В `gpu_preflight.json` обязательны `strict_checkpoint = true`,
`missing_keys = 0`, `unexpected_keys = 0`, `mismatched_keys = 0` и
`error_messages = 0`. Любое другое значение останавливает notebook до UPSERT.

Проверка БД:

```sql
SELECT COUNT(*)
FROM post_embeddings
WHERE run_id = :run_id
  AND model_name = 'Qwen/Qwen3-VL-Embedding-2B';
```

Версия dataset `v2_media_qwen_fix` исключает `post_id`, уже присутствующие в
`post_embeddings`. Поэтому ошибочные 100 строк предыдущего запуска не перезаписываются
и не удаляются автоматически: исправленный тест безопасно выберет следующие 100 постов.
Для замены старых строк требуется отдельное явное решение владельца.
""",
        "verification",
    ),
]


notebook = {
    "cells": cells,
    "metadata": {
        "accelerator": "GPU",
        "colab": {"gpuType": "L4", "provenance": []},
        "kernelspec": {"display_name": "Python 3", "language": "python", "name": "python3"},
        "language_info": {"name": "python", "version": "3.12"},
    },
    "nbformat": 4,
    "nbformat_minor": 5,
}

OUTPUT.parent.mkdir(parents=True, exist_ok=True)
OUTPUT.write_text(json.dumps(notebook, ensure_ascii=False, indent=1) + "\n", encoding="utf-8")
print(OUTPUT)

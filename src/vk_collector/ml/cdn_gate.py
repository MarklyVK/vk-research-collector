"""Общий для всего сервера «шлагбаум» скачивания с CDN VK.

Ограничение 429 у CDN VK (`*.userapi.com`, сервер `kittenx`) действует на IP, поэтому темп
должен быть общим для всех процессов сервера (ML-конвейер сборщика, скрипты MLwA и т.д.):

- не чаще одного файла в 20–30 с (случайная пауза) на весь сервер;
- в каждый момент качает ровно один процесс (``flock``);
- первый же ответ 429 ставит стоп-флаг ``STOP_429``; пока он есть, никто не качает;
  снимает флаг только человек (удалить файл вручную), автоматического снятия нет.

Каталог задаётся ``MEDIA_CDN_GATE_DIR`` (на проде ``/var/lib/markly-cdn``,
группа ``vkcollector``, 2770).
Без каталога скачивание запрещено (``CdnGateNotConfigured``).

Файлы: ``gate.lock`` (flock), ``state.json`` (``next_allowed_at``), ``STOP_429``, ``gate_log.jsonl``
(журнал без полных URL: только хост, статус, байты).
"""

from __future__ import annotations

import fcntl
import json
import os
import random
import threading
import time
from pathlib import Path
from typing import Any

DEFAULT_PAUSE_SECONDS = (20.0, 30.0)
GATE_DIR_ENV = "MEDIA_CDN_GATE_DIR"


class CdnGateError(Exception):
    """Базовая ошибка шлагбаума: дальнейшие скачивания с CDN запрещены."""


class CdnGateNotConfigured(CdnGateError):
    """Не задан каталог шлагбаума (MEDIA_CDN_GATE_DIR)."""


class CdnGateStopped(CdnGateError):
    """Стоит стоп-флаг STOP_429; снять его может только человек."""


class CdnRateLimited(CdnGateError):
    """CDN ответил 429; стоп-флаг поставлен, повтора нет."""

    def __init__(self, url_host: str) -> None:
        super().__init__(f"CDN ответил 429 ({url_host}); стоп-флаг STOP_429 поставлен")
        self.url_host = url_host


class CdnGate:
    """Межпроцессный замок + пауза + стоп-флаг.

    Синхронный; из asyncio вызывать через ``asyncio.to_thread``.
    """

    def __init__(
        self,
        gate_dir: Path | str,
        *,
        who: str = "vk-collector-ml",
        pause_seconds: tuple[float, float] = DEFAULT_PAUSE_SECONDS,
    ) -> None:
        self.dir = Path(gate_dir)
        if not self.dir.is_dir():
            raise CdnGateNotConfigured(f"нет каталога шлагбаума {self.dir}")
        self.who = who
        self.pause_seconds = pause_seconds
        self.lock_path = self.dir / "gate.lock"
        self.state_path = self.dir / "state.json"
        self.stop_path = self.dir / "STOP_429"
        self.log_path = self.dir / "gate_log.jsonl"
        self._fd: int | None = None
        self._thread_lock = threading.Lock()

    @classmethod
    def from_env(cls, *, who: str = "vk-collector-ml") -> CdnGate:
        gate_dir = os.environ.get(GATE_DIR_ENV, "").strip()
        if not gate_dir:
            raise CdnGateNotConfigured(f"{GATE_DIR_ENV} не задан: скачивание с CDN запрещено")
        return cls(gate_dir, who=who)

    def stopped(self) -> bool:
        return self.stop_path.exists()

    def acquire(self) -> None:
        """Дождаться своей очереди и паузы. Бросает CdnGateStopped, если стоит стоп-флаг."""
        self._thread_lock.acquire()
        try:
            old = os.umask(0o007)
            try:
                fd = os.open(self.lock_path, os.O_RDWR | os.O_CREAT, 0o660)
            finally:
                os.umask(old)
            fcntl.flock(fd, fcntl.LOCK_EX)
            self._fd = fd
            if self.stopped():
                raise CdnGateStopped(self._stop_text())
            wait = float(self._state().get("next_allowed_at", 0.0)) - time.time()
            if wait > 0:
                time.sleep(wait)
            if self.stopped():
                raise CdnGateStopped(self._stop_text())
        except BaseException:
            self._unlock()
            raise

    def release(
        self, status: int | None, *, host: str = "", nbytes: int = 0, note: str = ""
    ) -> None:
        """Записать итог одного скачивания, назначить следующую паузу и отпустить замок."""
        try:
            self._log(event="download", status=status, host=host, nbytes=nbytes, note=note)
            if status == 429:
                self._set_stop(host)
            next_at = time.time() + random.uniform(*self.pause_seconds)
            self._write(
                self.state_path,
                json.dumps(
                    {"next_allowed_at": next_at, "last_by": self.who, "last_status": status}
                ),
            )
        finally:
            self._unlock()

    # --- служебное ---
    def _unlock(self) -> None:
        if self._fd is not None:
            fcntl.flock(self._fd, fcntl.LOCK_UN)
            os.close(self._fd)
            self._fd = None
        if self._thread_lock.locked():
            self._thread_lock.release()

    def _stop_text(self) -> str:
        try:
            return self.stop_path.read_text()
        except FileNotFoundError:
            return "STOP_429"

    def _state(self) -> dict[str, Any]:
        try:
            data = json.loads(self.state_path.read_text())
        except (FileNotFoundError, json.JSONDecodeError):
            return {}
        return data if isinstance(data, dict) else {}

    def _set_stop(self, host: str) -> None:
        if not self.stop_path.exists():
            payload = {
                "at": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
                "who": self.who,
                "host": host,
                "reason": "429 от CDN",
            }
            self._write(self.stop_path, json.dumps(payload, ensure_ascii=False))
        self._log(event="stop_flag_set", host=host)

    def _write(self, path: Path, text: str) -> None:
        old = os.umask(0o007)
        try:
            tmp = path.with_name(f"{path.name}.tmp{os.getpid()}")
            tmp.write_text(text)
            os.replace(tmp, path)
        finally:
            os.umask(old)

    def _log(self, **fields: object) -> None:
        record = {"at": time.strftime("%Y-%m-%dT%H:%M:%S%z"), "who": self.who, "pid": os.getpid()}
        record.update(fields)
        old = os.umask(0o007)
        try:
            with self.log_path.open("a") as f:
                f.write(json.dumps(record, ensure_ascii=False) + "\n")
        finally:
            os.umask(old)

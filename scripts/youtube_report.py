#!/usr/bin/env python3
"""Обертка для запуска отчета youtube/report.py из корня репозитория."""

import sys
from pathlib import Path

# Добавляем src в sys.path
root = Path(__file__).resolve().parent.parent
src = root / "src"
if str(src) not in sys.path:
    sys.path.insert(0, str(src))

from vk_collector.youtube.report import main  # noqa: E402

if __name__ == "__main__":
    main()

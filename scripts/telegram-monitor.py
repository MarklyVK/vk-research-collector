#!/usr/bin/env python3
from __future__ import annotations

import importlib
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))


if __name__ == "__main__":
    module = importlib.import_module("vk_collector.monitoring.telegram_monitor")
    raise SystemExit(module.main())

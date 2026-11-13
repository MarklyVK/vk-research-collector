#!/usr/bin/env python3
"""Wrapper ╨┤╨╗╤П ╨╖╨░╨┐╤Г╤Б╨║╨░ ╨╛╤В╤З╤С╤В╨░ youtube/report.py ╨╕╨╖ ╨║╨╛╤А╨╜╤П ╤А╨╡╨┐╨╛╨╖╨╕╤В╨╛╤А╨╕╤П."""

import sys
from pathlib import Path

# ╨Ф╨╛╨▒╨░╨▓╨╗╤П╨╡╨╝ src ╨▓ sys.path
root = Path(__file__).resolve().parent.parent
src = root / "src"
if str(src) not in sys.path:
    sys.path.insert(0, str(src))

from vk_collector.youtube.report import main  # noqa: E402

if __name__ == "__main__":
    main()

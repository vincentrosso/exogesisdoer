"""
Logging setup for the sprint.

Console: INFO and above
File:    DEBUG and above → output/sprint.log (appended each run)

Usage in any module:
    from logger import get_logger
    log = get_logger(__name__)
"""

import logging
import sys
from pathlib import Path

_LOG_DIR = Path(__file__).parent / "output"
_LOG_FILE = _LOG_DIR / "sprint.log"
_FMT = "%(asctime)s  %(levelname)-8s  %(name)-32s  %(message)s"
_DATE_FMT = "%Y-%m-%d %H:%M:%S"

_configured = False


def setup(level_console: int = logging.INFO, level_file: int = logging.DEBUG) -> None:
    """Call once from run.py main() before any scrapers are imported."""
    global _configured
    if _configured:
        return

    _LOG_DIR.mkdir(exist_ok=True)

    root = logging.getLogger("exogesisdoer")
    root.setLevel(logging.DEBUG)
    root.propagate = False

    fmt = logging.Formatter(_FMT, datefmt=_DATE_FMT)

    console = logging.StreamHandler(sys.stderr)
    console.setLevel(level_console)
    console.setFormatter(fmt)

    file_h = logging.FileHandler(_LOG_FILE, encoding="utf-8")
    file_h.setLevel(level_file)
    file_h.setFormatter(fmt)

    root.addHandler(console)
    root.addHandler(file_h)

    _configured = True
    root.info("Logging initialised — file: %s", _LOG_FILE)


def get_logger(name: str) -> logging.Logger:
    """
    Return a child logger under the 'exogesisdoer' hierarchy.
    Works before setup() is called (handlers are added lazily).
    """
    if not name.startswith("exogesisdoer"):
        # Strip package prefix so __name__ ("scrapers.edgar") becomes "exogesisdoer.scrapers.edgar"
        name = f"exogesisdoer.{name}"
    return logging.getLogger(name)

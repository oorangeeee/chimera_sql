"""Logging utilities with console and file output."""

import logging
from pathlib import Path

# Project root: two levels up from this file (src/utils/ -> project root)
_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
_LOG_DIR = _PROJECT_ROOT / "data" / "logs"
_LOG_FILE = _LOG_DIR / "chimera.log"

_FORMAT = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

_initialized = False


def _init_logging():
    """Set up root logger with console and file handlers (called once)."""
    global _initialized
    if _initialized:
        return
    _initialized = True

    _LOG_DIR.mkdir(parents=True, exist_ok=True)

    root = logging.getLogger()
    root.setLevel(logging.DEBUG)

    # Console handler — INFO level (progress visibility)
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    console.setFormatter(logging.Formatter(_FORMAT, datefmt=_DATE_FORMAT))
    root.addHandler(console)

    # File handler — DEBUG level (full experiment data)
    file_handler = logging.FileHandler(_LOG_FILE, encoding="utf-8")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(logging.Formatter(_FORMAT, datefmt=_DATE_FORMAT))
    root.addHandler(file_handler)


def get_logger(name: str) -> logging.Logger:
    """Get a named logger. Initializes logging on first call."""
    _init_logging()
    return logging.getLogger(name)

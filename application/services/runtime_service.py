"""Runtime helpers for CLI locking, logging, and environment tuning."""

from __future__ import annotations

import fcntl
import logging
import os
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path

LOGGER_NAME = "crypto_l2_loader"
DEFAULT_LOG_DIR = "/volume1/Temp/logs"
DEFAULT_FETCH_CONCURRENCY = 8


class SingleInstanceError(RuntimeError):
    """Raised when another CLI instance is already running."""


class SingleInstanceLock:
    """Non-blocking process lock backed by a lock file."""

    def __init__(self, lock_path: str) -> None:
        self.lock_path = Path(lock_path)
        self._fd: int | None = None

    def __enter__(self) -> SingleInstanceLock:
        self.lock_path.parent.mkdir(parents=True, exist_ok=True)
        self._fd = os.open(self.lock_path, os.O_CREAT | os.O_RDWR, 0o644)
        try:
            fcntl.flock(self._fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            os.close(self._fd)
            self._fd = None
            raise SingleInstanceError("Another crypto-l2-loader instance is already running. Exiting.") from exc
        os.ftruncate(self._fd, 0)
        os.write(self._fd, str(os.getpid()).encode("utf-8"))
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        if self._fd is None:
            return
        fcntl.flock(self._fd, fcntl.LOCK_UN)
        os.close(self._fd)
        self._fd = None


def configure_logging() -> logging.Logger:
    """Configure file logging with weekly rotation."""

    logger = logging.getLogger(LOGGER_NAME)
    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)
    logger.propagate = False
    formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    log_dir = Path(os.getenv("L2_SYNC_LOG_DIR", DEFAULT_LOG_DIR))
    try:
        log_dir.mkdir(parents=True, exist_ok=True)
        file_handler = TimedRotatingFileHandler(
            filename=log_dir / "crypto-l2-loader.log",
            when="D",
            interval=7,
            backupCount=0,
            encoding="utf-8",
            utc=True,
        )
        file_handler.suffix = "%Y-%m-%d"
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    except OSError:
        logger.warning("Falling back to stderr logging; cannot create log directory '%s'", log_dir)

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    return logger


def fetch_concurrency() -> int:
    """Return bounded fetch concurrency from environment."""

    raw = os.getenv("L2_FETCH_CONCURRENCY", str(DEFAULT_FETCH_CONCURRENCY))
    try:
        value = int(raw)
    except ValueError:
        return DEFAULT_FETCH_CONCURRENCY
    return max(1, value)

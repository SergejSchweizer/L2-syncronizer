"""Runtime helpers for CLI locking and logging."""

from __future__ import annotations

import fcntl
import logging
import os
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path

from ingestion.config import Config, config_int, config_section, config_str, load_config

LOGGER_NAME = "crypto_l2_loader"
DEFAULT_LOG_DIR = ".logs"
DEFAULT_LOG_ROTATION_DAYS = 7
DEFAULT_LOG_BACKUP_COUNT = 0
DEFAULT_FETCH_CONCURRENCY = 8


class SingleInstanceError(RuntimeError):
    """Raised when another CLI instance is already running."""


class SingleInstanceLock:
    """Non-blocking process lock backed by a lock file.

    Example:
        ```python
        with SingleInstanceLock("/tmp/crypto-l2-loader-bronze-builder.lock"):
            run_loader()
        ```
    """

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


def _safe_log_module_name(module_name: str) -> str:
    """Return a filesystem-safe log module name."""

    normalized = module_name.strip().replace("/", "-").replace("\\", "-")
    return normalized or "crypto-l2-loader"


def configure_logging(module_name: str = "crypto-l2-loader", config: Config | None = None) -> logging.Logger:
    """Configure module-specific file logging with weekly rotation."""

    safe_module_name = _safe_log_module_name(module_name)
    logger = logging.getLogger(f"{LOGGER_NAME}.{safe_module_name}")
    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)
    logger.propagate = False
    formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    runtime_config = config_section(config or load_config(), "runtime")
    log_dir = Path(config_str(runtime_config, "log_dir", DEFAULT_LOG_DIR))
    rotation_days = max(1, config_int(runtime_config, "log_rotation_days", DEFAULT_LOG_ROTATION_DAYS))
    backup_count = max(0, config_int(runtime_config, "log_backup_count", DEFAULT_LOG_BACKUP_COUNT))
    try:
        log_dir.mkdir(parents=True, exist_ok=True)
        file_handler = TimedRotatingFileHandler(
            filename=log_dir / f"{safe_module_name}.log",
            when="D",
            interval=rotation_days,
            backupCount=backup_count,
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


def fetch_concurrency(config: Config | None = None) -> int:
    """Return bounded fetch concurrency from config."""

    runtime_config = config_section(config or load_config(), "runtime")
    value = config_int(runtime_config, "fetch_concurrency", DEFAULT_FETCH_CONCURRENCY)
    return max(1, value)

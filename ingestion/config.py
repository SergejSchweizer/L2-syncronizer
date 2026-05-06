"""Project configuration loading from ``config.yaml``."""

from __future__ import annotations

from copy import deepcopy
from pathlib import Path
from typing import Any, TypeAlias, cast

Config: TypeAlias = dict[str, Any]

DEFAULT_CONFIG: Config = {
    "http": {
        "timeout_s": 8,
        "max_retries": 2,
        "retry_backoff_s": 1,
    },
    "runtime": {
        "log_dir": ".logs",
        "log_rotation_days": 7,
        "log_backup_count": 0,
        "fetch_concurrency": 8,
    },
    "ingestion": {
        "exchange": "deribit",
        "symbols": ["BTC", "ETH"],
        "levels": 50,
        "snapshot_count": 5,
        "poll_interval_s": 10,
        "max_runtime_s": 50,
        "save_parquet_lake": True,
        "lake_root": "lake/bronze",
        "silver_lake_root": "lake/silver",
        "json_output": False,
    },
}


def load_config(path: str = "config.yaml") -> Config:
    """Load project configuration from a small YAML subset with defaults."""

    config = deepcopy(DEFAULT_CONFIG)
    config_path = Path(path)
    if not config_path.exists():
        return config

    parsed = _parse_simple_yaml(config_path.read_text(encoding="utf-8"))
    _deep_update(target=config, updates=parsed)
    return config


def _parse_simple_yaml(content: str) -> Config:
    """Parse the config file format used by this project.

    The parser intentionally supports only nested mappings and scalar or
    inline-list values. That keeps configuration deterministic without adding
    a runtime dependency solely for this small file.
    """

    root: Config = {}
    stack: list[tuple[int, Config]] = [(-1, root)]

    for raw_line in content.splitlines():
        line = raw_line.split("#", 1)[0].rstrip()
        if not line.strip():
            continue
        indent = len(line) - len(line.lstrip(" "))
        stripped = line.strip()
        if ":" not in stripped:
            raise ValueError(f"Invalid config line: {raw_line}")

        key, raw_value = stripped.split(":", 1)
        key = key.strip()
        if not key:
            raise ValueError(f"Invalid empty config key: {raw_line}")

        while stack and indent <= stack[-1][0]:
            stack.pop()
        parent = stack[-1][1]
        value_text = raw_value.strip()
        if value_text == "":
            child: Config = {}
            parent[key] = child
            stack.append((indent, child))
        else:
            parent[key] = _parse_scalar(value_text)

    return root


def _parse_scalar(value: str) -> bool | int | float | str | list[str]:
    """Parse one scalar or inline-list YAML value."""

    if value.startswith("[") and value.endswith("]"):
        items = [item.strip() for item in value[1:-1].split(",") if item.strip()]
        return [str(_parse_scalar(item)) for item in items]

    unquoted = _strip_quotes(value)
    lowered = unquoted.lower()
    if lowered in {"true", "false"}:
        return lowered == "true"
    try:
        return int(unquoted)
    except ValueError:
        pass
    try:
        return float(unquoted)
    except ValueError:
        return unquoted


def _strip_quotes(value: str) -> str:
    """Remove matching single or double quotes from a config value."""

    if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
        return value[1:-1]
    return value


def _deep_update(target: Config, updates: Config) -> None:
    """Merge nested config updates into a target config dictionary."""

    for key, value in updates.items():
        if isinstance(value, dict) and isinstance(target.get(key), dict):
            _deep_update(cast(Config, target[key]), value)
        else:
            target[key] = value


def config_section(config: Config, section: str) -> Config:
    """Return a named config section as a dictionary."""

    value = config.get(section, {})
    if not isinstance(value, dict):
        raise ValueError(f"Config section '{section}' must be a mapping")
    return cast(Config, value)


def config_bool(section: Config, name: str, default: bool) -> bool:
    """Read a boolean value from a config section."""

    value = section.get(name, default)
    return value if isinstance(value, bool) else default


def config_float(section: Config, name: str, default: float) -> float:
    """Read a float value from a config section."""

    value = section.get(name, default)
    return float(value) if isinstance(value, int | float) else default


def config_int(section: Config, name: str, default: int) -> int:
    """Read an integer value from a config section."""

    value = section.get(name, default)
    return int(value) if isinstance(value, int) else default


def config_str(section: Config, name: str, default: str) -> str:
    """Read a string value from a config section."""

    value = section.get(name, default)
    return value if isinstance(value, str) else default


def config_str_list(section: Config, name: str, default: list[str]) -> list[str]:
    """Read a string list from a config section."""

    value = section.get(name, default)
    if isinstance(value, list) and all(isinstance(item, str) for item in value):
        return value
    if isinstance(value, str):
        values = [item.strip() for item in value.replace(",", " ").split() if item.strip()]
        return values or default
    return default

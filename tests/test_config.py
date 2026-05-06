"""Tests for YAML-backed project configuration."""

from __future__ import annotations

from pathlib import Path

from ingestion.config import config_bool, config_int, config_section, config_str_list, load_config


def test_load_config_reads_yaml_defaults(tmp_path: Path) -> None:
    """Verify project config values are loaded from config.yaml."""

    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        """
http:
  timeout_s: 30
runtime:
  fetch_concurrency: 3
ingestion:
  symbols: [BTC, SOL]
  snapshot_count: 6
  save_parquet_lake: true
""".strip(),
        encoding="utf-8",
    )

    config = load_config(str(config_path))
    runtime_config = config_section(config, "runtime")
    ingestion_config = config_section(config, "ingestion")

    assert config_int(runtime_config, "fetch_concurrency", 0) == 3
    assert config_str_list(ingestion_config, "symbols", []) == ["BTC", "SOL"]
    assert config_int(ingestion_config, "snapshot_count", 0) == 6
    assert config_bool(ingestion_config, "save_parquet_lake", False) is True

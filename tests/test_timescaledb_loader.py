"""Tests for parquet-to-TimescaleDB loader helpers."""

from __future__ import annotations

from pathlib import Path

import pytest
from ingestion.timescaledb_loader import (
    list_parquet_files,
    load_timescale_config_from_env,
    parquet_file_signature,
)


def test_load_timescale_config_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TIMESCALEDB_HOST", "10.10.10.10")
    monkeypatch.setenv("TIMESCALEDB_PORT", "54321")
    monkeypatch.setenv("TIMESCALEDB_USER", "crypto")
    monkeypatch.setenv("TIMESCALEDB_PASSWORD", "secret")
    monkeypatch.setenv("TIMESCALEDB_DB", "crypto")
    config = load_timescale_config_from_env()

    assert config.host == "10.10.10.10"
    assert config.port == 54321
    assert config.user == "crypto"
    assert config.password == "secret"
    assert config.dbname == "crypto"


def test_list_parquet_files_filters_dataset_types(tmp_path: Path) -> None:
    spot_file = (
        tmp_path
        / "dataset_type=spot_ohlcv"
        / "exchange=binance"
        / "symbol=BTCUSDT"
        / "timeframe=1m"
        / "data.parquet"
    )
    perp_file = (
        tmp_path
        / "dataset_type=perp_ohlcv"
        / "exchange=deribit"
        / "symbol=BTC-PERPETUAL"
        / "timeframe=1m"
        / "data.parquet"
    )
    spot_file.parent.mkdir(parents=True, exist_ok=True)
    perp_file.parent.mkdir(parents=True, exist_ok=True)
    spot_file.write_bytes(b"spot")
    perp_file.write_bytes(b"perp")

    spot_only = list_parquet_files(lake_root=str(tmp_path), dataset_types=["spot_ohlcv"])
    all_files = list_parquet_files(lake_root=str(tmp_path), dataset_types=None)

    assert spot_only == [spot_file]
    assert all_files == sorted([spot_file, perp_file])


def test_parquet_file_signature_changes_with_content(tmp_path: Path) -> None:
    file_path = tmp_path / "data.parquet"
    file_path.write_bytes(b"one")
    first = parquet_file_signature(file_path)

    file_path.write_bytes(b"two-two")
    second = parquet_file_signature(file_path)

    assert first != second

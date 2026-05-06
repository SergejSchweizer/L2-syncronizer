"""Tests for Polars bronze-to-silver L2 transformations."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import polars as pl

from ingestion.l2 import L2Snapshot
from ingestion.lake import save_l2_snapshot_parquet_lake
from ingestion.silver import (
    silver_l2_features_from_bronze,
    silver_l2_snapshot_partition_path,
    transform_l2_bronze_to_silver,
)


def _sample_l2_snapshot(
    *,
    second: int = 1,
    day: int = 5,
    bids: list[tuple[float, float]] | None = None,
    asks: list[tuple[float, float]] | None = None,
) -> L2Snapshot:
    """Build a representative L2 snapshot for silver transform tests."""

    return L2Snapshot(
        exchange="deribit",
        symbol="BTC-PERPETUAL",
        timestamp=datetime(2026, 5, day, 10, 0, second, tzinfo=UTC),
        fetch_duration_s=0.1,
        bids=bids or [(100.0, 2.0), (99.9, 1.0), (99.8, 4.0)],
        asks=asks or [(100.2, 3.0), (100.3, 1.5), (100.4, 2.5)],
        mark_price=100.05,
        index_price=100.0,
        open_interest=1000.0,
        funding_8h=0.0001,
        current_funding=0.00001,
    )


def test_silver_l2_snapshot_partition_path_uses_monthly_layout() -> None:
    """Verify silver feature paths are partitioned by month, not date."""

    result = silver_l2_snapshot_partition_path(
        "lake/silver",
        ("deribit", "perp", "BTC-PERPETUAL", "2026-05"),
    )

    assert str(result).endswith(
        "dataset_type=l2_snapshot_features/exchange=deribit/instrument_type=perp/"
        "symbol=BTC-PERPETUAL/month=2026-05"
    )


def test_silver_l2_features_from_bronze_computes_snapshot_features(tmp_path: Path) -> None:
    """Verify Polars transforms bronze rows into fixed-width silver feature rows."""

    bronze_files = save_l2_snapshot_parquet_lake(
        {"BTC": [_sample_l2_snapshot()]},
        lake_root=str(tmp_path / "bronze"),
        depth=50,
    )
    bronze = pl.read_parquet(bronze_files)

    silver = silver_l2_features_from_bronze(bronze=bronze, depth=50)
    row = silver.row(0, named=True)

    assert row["dataset_type"] == "l2_snapshot_features"
    assert row["ts_event"] == datetime(2026, 5, 5, 10, 0, 1, tzinfo=UTC)
    assert row["best_bid_price"] == 100.0
    assert row["best_bid_size"] == 2.0
    assert row["best_ask_price"] == 100.2
    assert row["best_ask_size"] == 3.0
    assert row["mid_price"] == 100.1
    assert round(row["spread_bps"], 6) == round((0.2 / 100.1) * 10_000, 6)
    assert row["bid_volume_1"] == 2.0
    assert row["ask_volume_1"] == 3.0
    assert row["bid_volume_5"] == 7.0
    assert row["ask_volume_5"] == 7.0
    assert row["imbalance_1"] == -0.2
    assert row["microprice"] == ((100.0 * 3.0) + (100.2 * 2.0)) / 5.0
    assert row["funding_rate"] == 0.00001
    assert len(row["bid_prices"]) == 50
    assert row["bid_prices"][:3] == [100.0, 99.9, 99.8]
    assert row["bid_prices"][3] is None
    assert row["is_valid"] is False
    assert row["validation_flags"] == ["insufficient_bid_depth", "insufficient_ask_depth"]


def test_transform_l2_bronze_to_silver_skips_manifest_and_plot_when_disabled(tmp_path: Path) -> None:
    """Verify Silver writes only parquet when plot and manifest generation are disabled."""

    bronze_root = tmp_path / "bronze"
    silver_root = tmp_path / "silver"
    snapshot = _sample_l2_snapshot()
    save_l2_snapshot_parquet_lake({"BTC": [snapshot]}, lake_root=str(bronze_root), depth=50)

    files = transform_l2_bronze_to_silver(
        bronze_lake_root=str(bronze_root),
        silver_lake_root=str(silver_root),
        depth=50,
        plot=False,
        manifest=False,
    )

    assert len(files) == 1
    assert files[0].endswith("2026-05.parquet")
    assert Path(files[0]).exists()
    assert not any(path.name.endswith(".json") for path in Path(silver_root).rglob("*.json"))
    assert not any(path.name.endswith(".png") for path in Path(silver_root).rglob("*.png"))


def test_transform_l2_bronze_to_silver_writes_monthly_idempotent_partitions(tmp_path: Path) -> None:
    """Verify bronze-to-silver writes one deduplicated monthly feature partition."""

    bronze_root = tmp_path / "bronze"
    silver_root = tmp_path / "silver"
    snapshot = _sample_l2_snapshot()
    save_l2_snapshot_parquet_lake({"BTC": [snapshot]}, lake_root=str(bronze_root), depth=50)

    first_files = transform_l2_bronze_to_silver(
        bronze_lake_root=str(bronze_root),
        silver_lake_root=str(silver_root),
        depth=50,
    )
    second_files = transform_l2_bronze_to_silver(
        bronze_lake_root=str(bronze_root),
        silver_lake_root=str(silver_root),
        depth=50,
    )

    assert first_files == second_files
    assert len(first_files) == 3
    assert any(
        "/dataset_type=l2_snapshot_features/exchange=deribit/instrument_type=perp/" in file_path
        for file_path in first_files
    )
    assert any("/symbol=BTC-PERPETUAL/month=2026-05/2026-05.parquet" in file_path for file_path in first_files)
    assert any("/symbol=BTC-PERPETUAL/month=2026-05/2026-05.json" in file_path for file_path in first_files)
    assert any("/symbol=BTC-PERPETUAL/month=2026-05/2026-05.png" in file_path for file_path in first_files)

    parquet_file = next(file_path for file_path in first_files if file_path.endswith("2026-05.parquet"))
    json_file = next(file_path for file_path in first_files if file_path.endswith("2026-05.json"))
    png_file = next(file_path for file_path in first_files if file_path.endswith("2026-05.png"))
    records = pl.read_parquet(parquet_file)
    metadata = json.loads(Path(json_file).read_text(encoding="utf-8"))

    assert records.height == 1
    assert records["ts_event"].to_list() == [snapshot.timestamp]
    assert metadata["dataset_type"] == "l2_snapshot_features"
    assert metadata["row_count"] == 1
    assert metadata["symbols"] == ["BTC-PERPETUAL"]
    assert Path(png_file).stat().st_size > 0

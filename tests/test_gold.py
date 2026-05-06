"""Tests for Polars silver-to-gold L2 M1 transformations."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import polars as pl

from ingestion.gold import (
    base_asset_symbol,
    gold_l2_m1_from_silver,
    silver_parquet_files,
    silver_source_summary,
    write_gold_l2_m1_artifacts,
)


def _silver_row(second: int, mid_price: float, symbol: str = "BTC-PERPETUAL") -> dict[str, object]:
    """Build one representative silver L2 feature row."""

    ts_event = datetime(2026, 5, 6, 10, 0, second, tzinfo=UTC)
    row: dict[str, object] = {
        "schema_version": "v1",
        "dataset_type": "l2_snapshot_features",
        "ts_event": ts_event,
        "ts_received": ts_event,
        "exchange": "deribit",
        "symbol": symbol,
        "instrument_type": "perp",
        "source": "rest_order_book",
        "run_id": "run-1",
        "depth": 50,
        "month": "2026-05",
        "mid_price": mid_price,
        "spread_bps": 2.0 + second / 10,
        "microprice": mid_price + 0.2,
        "mark_price": mid_price + 0.01,
        "index_price": mid_price - 0.01,
        "open_interest": 1000.0 + second,
        "funding_rate": 0.0001,
        "funding_8h": 0.0002,
        "is_valid": True,
        "validation_flags": [],
    }
    for window in (1, 5, 10, 20, 50):
        row[f"imbalance_{window}"] = 0.1
        row[f"bid_volume_{window}"] = float(window)
        row[f"ask_volume_{window}"] = float(window + 1)
    return row


def _sample_silver_frame() -> pl.DataFrame:
    """Build multiple silver snapshots within one minute."""

    return pl.DataFrame(
        [
            _silver_row(second=0, mid_price=100.0),
            _silver_row(second=10, mid_price=101.0),
            _silver_row(second=20, mid_price=99.5),
            _silver_row(second=30, mid_price=100.5),
            _silver_row(second=40, mid_price=100.2),
        ]
    )


def test_gold_l2_m1_from_silver_computes_ohlc_and_quality() -> None:
    """Verify M1 gold aggregation uses first/max/min/last/mean/std semantics."""

    gold = gold_l2_m1_from_silver(_sample_silver_frame())
    row = gold.row(0, named=True)

    assert gold.height == 1
    assert row["ts_minute"] == datetime(2026, 5, 6, 10, 0, tzinfo=UTC)
    assert row["snapshot_count"] == 5
    assert round(row["coverage_ratio"], 6) == round(5 / 6, 6)
    assert row["is_complete_minute"] is True
    assert row["quality_flags"] == []
    assert row["mid_open"] == 100.0
    assert row["mid_high"] == 101.0
    assert row["mid_low"] == 99.5
    assert row["mid_close"] == 100.2
    assert row["mid_mean"] == 100.24
    assert row["microprice_close"] == 100.4
    assert round(row["microprice_minus_mid_mean"], 12) == 0.2
    assert row["bid_volume_1_mean"] == 1.0
    assert row["ask_volume_1_mean"] == 2.0
    assert row["book_pressure_1_mean"] == 1.0 / 3.0
    assert row["mark_price_last"] == 100.21000000000001
    assert row["open_interest_last"] == 1040.0


def test_write_gold_l2_m1_artifacts_writes_parquet_json_and_png(tmp_path: Path) -> None:
    """Verify per-symbol gold artifacts share a basename and omit source paths from JSON."""

    silver = _sample_silver_frame()
    gold = gold_l2_m1_from_silver(silver)

    files = write_gold_l2_m1_artifacts(
        gold=gold,
        gold_lake_root=str(tmp_path),
        source_summary=silver_source_summary(silver),
        git_commit_hash="abcdef1234567890",
    )

    suffixes = sorted(Path(file_path).suffix for file_path in files)
    assert suffixes == [".json", ".parquet", ".png"]
    basenames = {Path(file_path).stem for file_path in files}
    assert len(basenames) == 1
    basename = basenames.pop()
    assert basename.startswith("BTC_")
    assert basename.endswith("_abcdef123456")

    metadata_path = next(Path(file_path) for file_path in files if file_path.endswith(".json"))
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    assert metadata["row_count"] == 1
    assert metadata["column_count"] == len(gold.columns)
    assert metadata["source_silver_dataset_summaries"]["source_symbols"] == [
        {"row_count": 5, "source_symbol": "BTC-PERPETUAL"}
    ]
    assert "path" not in json.dumps(metadata).lower()
    assert any(
        feature["name"] == "mid_mean" and feature["numeric_stats"]["mean"] == 100.24
        for feature in metadata["features"]
    )


def test_write_gold_l2_m1_artifacts_skips_manifest_and_plot_when_disabled(tmp_path: Path) -> None:
    """Verify Gold writes only parquet when plot and manifest generation are disabled."""

    silver = _sample_silver_frame()
    gold = gold_l2_m1_from_silver(silver)

    files = write_gold_l2_m1_artifacts(
        gold=gold,
        gold_lake_root=str(tmp_path),
        source_summary=silver_source_summary(silver),
        git_commit_hash="abcdef1234567890",
        plot=False,
        manifest=False,
    )

    assert len(files) == 1
    assert files[0].endswith(".parquet")
    assert Path(files[0]).exists()
    assert not any(path.name.endswith(".json") for path in Path(tmp_path).rglob("*.json"))
    assert not any(path.name.endswith(".png") for path in Path(tmp_path).rglob("*.png"))


def test_base_asset_symbol_handles_deribit_symbols() -> None:
    """Verify output artifact base symbols for inverse and USDC perps."""

    assert base_asset_symbol("BTC-PERPETUAL") == "BTC"
    assert base_asset_symbol("SOL_USDC-PERPETUAL") == "SOL"


def test_silver_parquet_files_prefers_month_named_files_over_legacy(tmp_path: Path) -> None:
    """Verify Gold readers do not double-read migrated Silver month partitions."""

    partition = (
        tmp_path
        / "dataset_type=l2_snapshot_features"
        / "exchange=deribit"
        / "instrument_type=perp"
        / "symbol=BTC-PERPETUAL"
        / "month=2026-05"
    )
    partition.mkdir(parents=True)
    legacy_path = partition / "data.parquet"
    month_path = partition / "2026-05.parquet"
    legacy_path.touch()
    month_path.touch()

    assert silver_parquet_files(str(tmp_path)) == [month_path]

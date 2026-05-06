"""Tests for L2 parquet lake helper functions."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

from ingestion.l2 import L2Snapshot
from ingestion.lake import (
    save_l2_snapshot_parquet_lake,
    snapshot_partition_path,
)


def _sample_l2_snapshot(second: int, day: int = 5) -> L2Snapshot:
    """Build a representative raw L2 snapshot for persistence tests."""

    return L2Snapshot(
        exchange="deribit",
        symbol="BTC-PERPETUAL",
        timestamp=datetime(2026, 5, day, 10, 0, second, tzinfo=UTC),
        fetch_duration_s=0.1,
        bids=[(100.0, 2.0), (99.9, 1.0)],
        asks=[(100.1, 3.0), (100.2, 1.5)],
        mark_price=100.05,
        index_price=100.0,
        open_interest=1000.0,
        funding_8h=0.0001,
        current_funding=0.00001,
    )


def test_l2_snapshot_partition_path() -> None:
    """Verify raw L2 snapshot paths use the bronze layout."""

    result = snapshot_partition_path(
        "lake/bronze",
        ("deribit", "perp", "BTC-PERPETUAL", 50, "rest_order_book", "2026-05", "2026-05-05"),
    )

    assert str(result).endswith(
        "dataset_type=l2_snapshot/exchange=deribit/instrument_type=perp/"
        "symbol=BTC-PERPETUAL/depth=50/source=rest_order_book/month=2026-05/date=2026-05-05"
    )


def test_save_l2_snapshot_parquet_lake_uses_daily_bronze_layout(tmp_path: Path) -> None:
    """Verify raw L2 snapshots are written to daily bronze partitions."""

    snapshot_1 = _sample_l2_snapshot(second=1)
    snapshot_2 = _sample_l2_snapshot(second=2, day=6)

    files = save_l2_snapshot_parquet_lake(
        {"BTC": [snapshot_1, snapshot_2]},
        lake_root=str(tmp_path),
        depth=50,
    )

    assert len(files) == 2
    assert any(
        "/dataset_type=l2_snapshot/exchange=deribit/instrument_type=perp/"
        "symbol=BTC-PERPETUAL/depth=50/source=rest_order_book/month=2026-05/"
        "date=2026-05-05/data.parquet" in file_path
        for file_path in files
    )
    assert any(
        "/dataset_type=l2_snapshot/exchange=deribit/instrument_type=perp/"
        "symbol=BTC-PERPETUAL/depth=50/source=rest_order_book/month=2026-05/"
        "date=2026-05-06/data.parquet" in file_path
        for file_path in files
    )


def test_save_l2_snapshot_parquet_lake_keeps_same_day_snapshots_as_rows(tmp_path: Path) -> None:
    """Verify same-day raw snapshots are stored as distinct rows without aggregation."""

    import pyarrow.parquet as pq

    snapshot_1 = _sample_l2_snapshot(second=1)
    snapshot_2 = _sample_l2_snapshot(second=11)

    files = save_l2_snapshot_parquet_lake(
        {"BTC": [snapshot_1, snapshot_2]},
        lake_root=str(tmp_path),
        depth=50,
    )

    records = pq.ParquetFile(files[0]).read().to_pylist()  # type: ignore[no-untyped-call]

    assert len(files) == 1
    assert len(records) == 2
    assert [record["event_time"] for record in records] == [snapshot_1.timestamp, snapshot_2.timestamp]

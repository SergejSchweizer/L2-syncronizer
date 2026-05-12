"""Schema contract tests for Bronze, Silver, and Gold L2 artifacts."""

from __future__ import annotations

from datetime import UTC, datetime

import polars as pl

from ingestion.gold import GOLD_COLUMNS, gold_l2_m1_from_silver
from ingestion.l2 import L2Snapshot, l2_snapshot_record
from ingestion.silver import silver_l2_features_from_bronze

BRONZE_COLUMNS = [
    "schema_version",
    "dataset_type",
    "exchange",
    "symbol",
    "instrument_type",
    "event_time",
    "ingested_at",
    "run_id",
    "source",
    "depth",
    "fetch_duration_s",
    "bids",
    "asks",
    "mark_price",
    "index_price",
    "open_interest",
    "funding_8h",
    "current_funding",
]

SILVER_COLUMNS = [
    "schema_version",
    "dataset_type",
    "ts_event",
    "ts_received",
    "exchange",
    "symbol",
    "instrument_type",
    "source",
    "run_id",
    "depth",
    "month",
    "mid_price",
    "spread",
    "spread_bps",
    "best_bid_price",
    "best_bid_size",
    "best_ask_price",
    "best_ask_size",
    "bid_prices",
    "bid_sizes",
    "ask_prices",
    "ask_sizes",
    "bid_volume_1",
    "ask_volume_1",
    "bid_volume_5",
    "ask_volume_5",
    "bid_volume_10",
    "ask_volume_10",
    "bid_volume_20",
    "ask_volume_20",
    "bid_volume_50",
    "ask_volume_50",
    "imbalance_1",
    "imbalance_5",
    "imbalance_10",
    "imbalance_20",
    "imbalance_50",
    "microprice",
    "mark_price",
    "index_price",
    "open_interest",
    "funding_rate",
    "funding_8h",
    "is_valid",
    "validation_flags",
]


def _snapshot() -> L2Snapshot:
    """Build a representative snapshot for schema contract tests."""

    return L2Snapshot(
        exchange="deribit",
        symbol="BTC-PERPETUAL",
        timestamp=datetime(2026, 5, 6, 10, 0, tzinfo=UTC),
        fetch_duration_s=0.1,
        bids=[(100.0, 2.0)],
        asks=[(100.2, 3.0)],
        mark_price=100.1,
        index_price=100.0,
        open_interest=1000.0,
        funding_8h=0.0001,
        current_funding=0.00001,
    )


def test_bronze_l2_snapshot_record_schema_contract() -> None:
    """Verify Bronze row fields stay explicit and ordered."""

    snapshot = _snapshot()
    record = l2_snapshot_record(
        snapshot=snapshot,
        depth=50,
        run_id="run-1",
        ingested_at=datetime(2026, 5, 6, 10, 0, 1, tzinfo=UTC),
    )

    assert list(record) == BRONZE_COLUMNS
    assert record["schema_version"] == "v1"
    assert record["dataset_type"] == "l2_snapshot"


def test_silver_l2_feature_schema_contract() -> None:
    """Verify Silver features expose the documented fixed-width schema."""

    snapshot = _snapshot()
    bronze = pl.DataFrame(
        [
            l2_snapshot_record(
                snapshot=snapshot,
                depth=50,
                run_id="run-1",
                ingested_at=datetime(2026, 5, 6, 10, 0, 1, tzinfo=UTC),
            )
        ]
    )

    silver = silver_l2_features_from_bronze(bronze=bronze, depth=50)

    assert silver.columns == SILVER_COLUMNS
    assert silver["schema_version"].to_list() == ["v1"]
    assert silver["dataset_type"].to_list() == ["l2_snapshot_features"]


def test_gold_l2_m1_schema_contract() -> None:
    """Verify Gold M1 features expose the versioned contract columns."""

    snapshot = _snapshot()
    bronze = pl.DataFrame(
        [
            l2_snapshot_record(
                snapshot=snapshot,
                depth=50,
                run_id="run-1",
                ingested_at=datetime(2026, 5, 6, 10, 0, 1, tzinfo=UTC),
            )
        ]
    )
    silver = silver_l2_features_from_bronze(bronze=bronze, depth=50)

    gold = gold_l2_m1_from_silver(silver)

    assert gold.columns == GOLD_COLUMNS
    assert gold["feature_set_version"].to_list() == ["gold_l2_m1_v1"]

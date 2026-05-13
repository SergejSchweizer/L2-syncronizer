"""Tests for Polars silver-to-gold L2 M1 transformations."""

from __future__ import annotations

import json
import math
from datetime import UTC, datetime
from pathlib import Path

import polars as pl
import pytest

from ingestion.gold import (
    GOLD_NUMERIC_FEATURES,
    base_asset_symbol,
    dataframe_content_hash,
    fill_gold_missing_minutes_hybrid,
    gold_l2_m1_dataset_path,
    gold_l2_m1_from_silver,
    gold_metadata,
    gold_plot_feature_metadata_label,
    gold_plot_metadata_lines,
    gold_plot_sample,
    gold_transform_state_path,
    silver_parquet_files,
    silver_source_summary,
    transform_l2_silver_to_gold,
    write_gold_l2_m1_artifacts,
)


def _silver_row(
    second: int,
    mid_price: float,
    symbol: str = "BTC-PERPETUAL",
    minute: int = 0,
) -> dict[str, object]:
    """Build one representative silver L2 feature row."""

    ts_event = datetime(2026, 5, 6, 10, minute, second, tzinfo=UTC)
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


def _write_silver_partition(root: Path, symbol: str, rows: list[dict[str, object]]) -> Path:
    """Write a test Silver month partition."""

    partition = (
        root
        / "dataset_type=l2_snapshot_features"
        / "exchange=deribit"
        / "instrument_type=perp"
        / f"symbol={symbol}"
        / "month=2026-05"
    )
    partition.mkdir(parents=True, exist_ok=True)
    path = partition / "2026-05.parquet"
    pl.DataFrame(rows).write_parquet(path)
    return path


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


def test_gold_l2_m1_from_silver_inserts_missing_minutes_as_nan_rows() -> None:
    """Verify Gold spans the full M1 scale and marks missing minutes explicitly."""

    silver = pl.DataFrame(
        [
            _silver_row(second=0, minute=0, mid_price=100.0),
            _silver_row(second=0, minute=2, mid_price=102.0),
        ]
    )

    gold = gold_l2_m1_from_silver(silver)
    missing = gold.row(1, named=True)

    assert gold["ts_minute"].to_list() == [
        datetime(2026, 5, 6, 10, 0, tzinfo=UTC),
        datetime(2026, 5, 6, 10, 1, tzinfo=UTC),
        datetime(2026, 5, 6, 10, 2, tzinfo=UTC),
    ]
    assert missing["snapshot_count"] == 0
    assert missing["coverage_ratio"] == 0.0
    assert missing["first_snapshot_ts"] is None
    assert missing["last_snapshot_ts"] is None
    assert missing["is_complete_minute"] is False
    assert missing["quality_flags"] == ["missing_minute"]
    assert math.isnan(missing["mid_open"])
    assert math.isnan(missing["mid_close"])


def test_gold_l2_m1_from_silver_can_fill_missing_minutes_from_neighbor_averages() -> None:
    """Verify optional Gold fill averages numeric features from adjacent observed minutes."""

    silver = pl.DataFrame(
        [
            _silver_row(second=0, minute=0, mid_price=100.0),
            _silver_row(second=0, minute=2, mid_price=102.0),
        ]
    )

    gold = gold_l2_m1_from_silver(silver, fill_missing_minutes=True)
    filled = gold.row(1, named=True)

    assert filled["snapshot_count"] == 0
    assert filled["coverage_ratio"] == 0.0
    assert filled["is_complete_minute"] is False
    assert filled["quality_flags"] == ["missing_minute", "filled_neighbor_average"]
    assert filled["mid_open"] == 101.0
    assert filled["mid_close"] == 101.0
    assert filled["mark_price_last"] == 101.01


def test_gold_l2_m1_from_silver_does_not_fill_when_one_neighbor_is_missing() -> None:
    """Verify missing-minute fill requires both adjacent minutes to be observed rows."""

    silver = pl.DataFrame(
        [
            _silver_row(second=0, minute=0, mid_price=100.0),
            _silver_row(second=0, minute=3, mid_price=103.0),
        ]
    )

    filled = gold_l2_m1_from_silver(silver, fill_missing_minutes=True)
    minute_one = filled.row(1, named=True)
    minute_two = filled.row(2, named=True)

    assert minute_one["quality_flags"] == ["missing_minute"]
    assert minute_two["quality_flags"] == ["missing_minute"]
    assert math.isnan(minute_one["mid_open"])
    assert math.isnan(minute_two["mid_open"])


def test_gold_l2_m1_from_silver_hybrid_interpolates_short_internal_gaps() -> None:
    """Verify hybrid fill linearly interpolates short internal missing runs."""

    silver = pl.DataFrame(
        [
            _silver_row(second=0, minute=0, mid_price=100.0),
            _silver_row(second=0, minute=3, mid_price=103.0),
        ]
    )
    filled = gold_l2_m1_from_silver(silver, fill_missing_minutes=True, fill_policy="hybrid")
    minute_one = filled.row(1, named=True)
    minute_two = filled.row(2, named=True)

    assert minute_one["quality_flags"] == ["missing_minute", "filled_linear_interpolation"]
    assert minute_two["quality_flags"] == ["missing_minute", "filled_linear_interpolation"]
    assert minute_one["mid_open"] == 101.0
    assert minute_two["mid_open"] == 102.0


def test_gold_l2_m1_from_silver_hybrid_boundary_fill_and_long_gap_marking() -> None:
    """Verify hybrid fill handles boundary runs and preserves long-gap missing rows."""

    observed = gold_l2_m1_from_silver(_sample_silver_frame())
    missing_template = observed.row(0, named=True)
    missing_template.update(
        {
            "snapshot_count": 0,
            "coverage_ratio": 0.0,
            "first_snapshot_ts": None,
            "last_snapshot_ts": None,
            "is_complete_minute": False,
            "quality_flags": ["missing_minute"],
        }
    )
    for feature in GOLD_NUMERIC_FEATURES:
        missing_template[feature] = float("nan")

    boundary_lead = dict(missing_template)
    boundary_lead["ts_minute"] = datetime(2026, 5, 6, 9, 59, tzinfo=UTC)
    boundary_tail = dict(missing_template)
    boundary_tail["ts_minute"] = datetime(2026, 5, 6, 10, 1, tzinfo=UTC)
    hybrid_input = pl.DataFrame([boundary_lead, observed.row(0, named=True), boundary_tail], schema=observed.schema)
    hybrid_filled = fill_gold_missing_minutes_hybrid(hybrid_input)
    lead_row = hybrid_filled.row(0, named=True)
    tail_row = hybrid_filled.row(2, named=True)
    assert "filled_backward_boundary" in lead_row["quality_flags"]
    assert "filled_forward_boundary" in tail_row["quality_flags"]
    assert lead_row["mid_open"] == observed.row(0, named=True)["mid_open"]
    assert tail_row["mid_open"] == observed.row(0, named=True)["mid_open"]

    long_gap_silver = pl.DataFrame(
        [
            _silver_row(second=0, minute=0, mid_price=100.0),
            _silver_row(second=0, minute=8, mid_price=108.0),
        ]
    )
    long_gap_filled = gold_l2_m1_from_silver(long_gap_silver, fill_missing_minutes=True, fill_policy="hybrid")
    long_gap_row = long_gap_filled.row(4, named=True)
    assert "missing_long_gap" in long_gap_row["quality_flags"]
    assert math.isnan(long_gap_row["mid_open"])


def test_gold_l2_m1_from_silver_hybrid_respects_boundary_fill_max_gap() -> None:
    """Verify hybrid mode does not boundary-fill runs larger than the configured edge limit."""

    observed = gold_l2_m1_from_silver(_sample_silver_frame())
    missing_template = observed.row(0, named=True)
    missing_template.update(
        {
            "snapshot_count": 0,
            "coverage_ratio": 0.0,
            "first_snapshot_ts": None,
            "last_snapshot_ts": None,
            "is_complete_minute": False,
            "quality_flags": ["missing_minute"],
        }
    )
    for feature in GOLD_NUMERIC_FEATURES:
        missing_template[feature] = float("nan")

    missing_lead_three = []
    for minute in (9, 8, 7):
        row = dict(missing_template)
        row["ts_minute"] = datetime(2026, 5, 6, minute, 59, tzinfo=UTC)
        missing_lead_three.append(row)

    hybrid_input = pl.DataFrame([*missing_lead_three, observed.row(0, named=True)], schema=observed.schema)
    hybrid_filled = fill_gold_missing_minutes_hybrid(hybrid_input).sort("ts_minute")
    for idx in range(3):
        row = hybrid_filled.row(idx, named=True)
        assert "missing_long_gap" in row["quality_flags"]
        assert math.isnan(row["mid_open"])


def test_write_gold_l2_m1_artifacts_writes_parquet_json_and_png(tmp_path: Path) -> None:
    """Verify Gold artifacts are written under the versioned timeframe dataset leaf."""

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
    assert basename.startswith("BTC_L2_")
    assert basename.endswith("_abcdef123456")
    expected_dir = (
        tmp_path
        / "dataset_type=l2_m1_features"
        / "feature_set_version=gold_l2_m1_v1"
        / "exchange=deribit"
        / "instrument_type=perp"
        / "base_asset=BTC"
        / "symbol=BTC-PERPETUAL"
        / "depth=50"
        / "timeframe=1m"
    )
    assert {Path(file_path).parent for file_path in files} == {expected_dir}

    metadata_path = next(Path(file_path) for file_path in files if file_path.endswith(".json"))
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    assert metadata["dataset_type"] == "l2_m1_features"
    assert metadata["feature_set_version"] == "gold_l2_m1_v1"
    assert metadata["timeframe"] == "1m"
    assert metadata["row_count"] == 1
    assert metadata["column_count"] == len(gold.columns)
    assert metadata["gold_content_hash"] == dataframe_content_hash(gold)
    assert metadata["source_fingerprint_hash"]
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


def test_write_gold_l2_m1_artifacts_hash_changes_when_output_data_changes(tmp_path: Path) -> None:
    """Verify artifact basenames are tied to actual Gold output content."""

    silver = _sample_silver_frame()
    changed_silver = pl.DataFrame(
        [
            _silver_row(second=0, mid_price=200.0),
            _silver_row(second=10, mid_price=201.0),
        ]
    )
    first_files = write_gold_l2_m1_artifacts(
        gold=gold_l2_m1_from_silver(silver),
        gold_lake_root=str(tmp_path),
        source_summary=silver_source_summary(silver),
        git_commit_hash="abcdef1234567890",
        plot=False,
        manifest=False,
    )
    second_files = write_gold_l2_m1_artifacts(
        gold=gold_l2_m1_from_silver(changed_silver),
        gold_lake_root=str(tmp_path),
        source_summary=silver_source_summary(changed_silver),
        git_commit_hash="abcdef1234567890",
        plot=False,
        manifest=False,
    )

    assert Path(first_files[0]).stem != Path(second_files[0]).stem


def test_write_gold_l2_m1_artifacts_hash_changes_when_fill_policy_changes(tmp_path: Path) -> None:
    """Verify Gold artifact basenames change when fill policy changes."""

    silver = pl.DataFrame(
        [
            _silver_row(second=0, minute=0, mid_price=100.0),
            _silver_row(second=0, minute=3, mid_price=103.0),
        ]
    )
    gold = gold_l2_m1_from_silver(silver, fill_missing_minutes=True, fill_policy="hybrid")

    neighbor_files = write_gold_l2_m1_artifacts(
        gold=gold,
        gold_lake_root=str(tmp_path),
        source_summary=silver_source_summary(silver),
        git_commit_hash="abcdef1234567890",
        plot=False,
        manifest=False,
        densify=False,
        fill_missing_minutes=True,
        fill_policy="neighbor",
    )
    hybrid_files = write_gold_l2_m1_artifacts(
        gold=gold,
        gold_lake_root=str(tmp_path),
        source_summary=silver_source_summary(silver),
        git_commit_hash="abcdef1234567890",
        plot=False,
        manifest=False,
        densify=False,
        fill_missing_minutes=True,
        fill_policy="hybrid",
    )

    assert Path(neighbor_files[0]).stem != Path(hybrid_files[0]).stem


def test_gold_plot_metadata_lines_include_manifest_context() -> None:
    """Verify Gold plot headers carry important manifest metadata."""

    silver = _sample_silver_frame()
    gold = gold_l2_m1_from_silver(silver)
    metadata = gold_metadata(
        gold=gold,
        source_summary=silver_source_summary(silver),
        hash_string="hash123",
        git_commit_hash="abcdef1234567890",
        expected_snapshots_per_minute=6,
        completeness_threshold=0.8,
        feature_set_version="gold_l2_m1_v1",
    )

    lines = gold_plot_metadata_lines(metadata=metadata, gold=gold)
    rendered = "\n".join(lines)

    assert "Gold 1m profile" in lines[0]
    assert "dataset=l2_m1_features" in rendered
    assert "version=gold_l2_m1_v1" in rendered
    assert "hash=hash123" in rendered
    assert "git=abcdef123456" in rendered
    assert "rows=1" in rendered
    assert "missing_minutes=0" in rendered
    assert "expected_snapshots_per_minute=6" in rendered
    assert "completeness_threshold=0.8" in rendered
    assert "source_silver_rows=5" in rendered
    assert "BTC-PERPETUAL:5" in rendered


def test_gold_plot_feature_metadata_label_is_limited_to_feature_time_and_rows() -> None:
    """Verify every Gold feature subplot carries only compact feature/time/row metadata."""

    silver = _sample_silver_frame()
    gold = gold_l2_m1_from_silver(silver)
    metadata = gold_metadata(
        gold=gold,
        source_summary=silver_source_summary(silver),
        hash_string="hash123",
        git_commit_hash="abcdef1234567890",
        expected_snapshots_per_minute=6,
        completeness_threshold=0.8,
        feature_set_version="gold_l2_m1_v1",
    )

    label = gold_plot_feature_metadata_label(
        metadata=metadata,
        feature="mid_close",
        feature_stats={
            "row_count": gold.height,
            "null_count": 0,
            "nan_count": 0,
            "finite_count": 1,
            "nonfinite_count": 0,
        },
        plot_gold=gold,
    )

    assert "feature=mid_close" in label
    assert "time=2026-05-06T10:00:00+00:00 -> 2026-05-06T10:00:00+00:00" in label
    assert "rows=1 plot_rows=1 missing=0" in label
    assert "valid=1 null=0 nan=0 nonfinite=0" in label
    assert "l2_m1_features" not in label
    assert "gold_l2_m1_v1" not in label
    assert "BTC-PERPETUAL" not in label
    assert "hash123" not in label
    assert "abcdef123456" not in label
    assert "threshold=0.8" not in label


def test_gold_plot_sample_caps_points_across_full_time_scale() -> None:
    """Verify Gold plots use bounded samples that preserve full time-scale endpoints."""

    gold = pl.DataFrame(
        {
            "ts_minute": [datetime(2026, 5, 6, 10, minute, tzinfo=UTC) for minute in range(10)],
            "mid_close": [float(value) for value in range(10)],
        }
    )

    sampled = gold_plot_sample(gold=gold, max_points=4)

    assert sampled.height == 4
    assert sampled["ts_minute"][0] == gold["ts_minute"][0]
    assert sampled["ts_minute"][-1] == gold["ts_minute"][-1]
    assert sampled["ts_minute"].to_list() == [
        datetime(2026, 5, 6, 10, 0, tzinfo=UTC),
        datetime(2026, 5, 6, 10, 3, tzinfo=UTC),
        datetime(2026, 5, 6, 10, 6, tzinfo=UTC),
        datetime(2026, 5, 6, 10, 9, tzinfo=UTC),
    ]


def test_gold_plot_sample_supports_single_point_cap() -> None:
    """Verify plot sampling handles a one-point cap without division errors."""

    gold = pl.DataFrame(
        {
            "ts_minute": [datetime(2026, 5, 6, 10, minute, tzinfo=UTC) for minute in range(3)],
            "mid_close": [float(value) for value in range(3)],
        }
    )

    sampled = gold_plot_sample(gold=gold, max_points=1)

    assert sampled.height == 1
    assert sampled["ts_minute"][0] == datetime(2026, 5, 6, 10, 0, tzinfo=UTC)


def test_gold_l2_m1_dataset_path_uses_timeframe_leaf() -> None:
    """Verify Gold dataset paths stop at the full versioned timeframe level."""

    result = gold_l2_m1_dataset_path(
        lake_root="lake/gold",
        feature_set_version="gold_l2_m1_v1",
        exchange="deribit",
        instrument_type="perp",
        base_asset="BTC",
        symbol="BTC-PERPETUAL",
        depth=50,
    )

    assert str(result).endswith(
        "lake/gold/dataset_type=l2_m1_features/feature_set_version=gold_l2_m1_v1/"
        "exchange=deribit/instrument_type=perp/base_asset=BTC/symbol=BTC-PERPETUAL/depth=50/timeframe=1m"
    )


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


def test_transform_l2_silver_to_gold_rebuilds_only_changed_symbols(tmp_path: Path) -> None:
    """Verify Gold transform state skips unchanged symbol inputs."""

    silver_root = tmp_path / "silver"
    gold_root = tmp_path / "gold"
    btc_rows = [_silver_row(second=0, mid_price=100.0, symbol="BTC-PERPETUAL")]
    eth_rows = [_silver_row(second=0, mid_price=200.0, symbol="ETH-PERPETUAL")]
    _write_silver_partition(silver_root, "BTC-PERPETUAL", btc_rows)
    eth_path = _write_silver_partition(silver_root, "ETH-PERPETUAL", eth_rows)

    first_files = transform_l2_silver_to_gold(
        silver_lake_root=str(silver_root),
        gold_lake_root=str(gold_root),
        plot=False,
        manifest=False,
    )
    second_files = transform_l2_silver_to_gold(
        silver_lake_root=str(silver_root),
        gold_lake_root=str(gold_root),
        plot=False,
        manifest=False,
    )

    _write_silver_partition(
        silver_root,
        "ETH-PERPETUAL",
        [*eth_rows, _silver_row(second=10, mid_price=201.0, symbol="ETH-PERPETUAL")],
    )
    third_files = transform_l2_silver_to_gold(
        silver_lake_root=str(silver_root),
        gold_lake_root=str(gold_root),
        plot=False,
        manifest=False,
    )

    assert len(first_files) == 2
    assert second_files == []
    assert len(third_files) == 1
    assert "/symbol=ETH-PERPETUAL/" in third_files[0]
    assert gold_transform_state_path(str(gold_root)).exists()
    assert eth_path.exists()


def test_transform_l2_silver_to_gold_rebuilds_when_quality_policy_changes(tmp_path: Path) -> None:
    """Verify transform settings are part of Gold incremental invalidation."""

    silver_root = tmp_path / "silver"
    gold_root = tmp_path / "gold"
    _write_silver_partition(
        silver_root,
        "BTC-PERPETUAL",
        [_silver_row(second=0, mid_price=100.0, symbol="BTC-PERPETUAL")],
    )

    first_files = transform_l2_silver_to_gold(
        silver_lake_root=str(silver_root),
        gold_lake_root=str(gold_root),
        completeness_threshold=0.8,
        plot=False,
        manifest=False,
    )
    second_files = transform_l2_silver_to_gold(
        silver_lake_root=str(silver_root),
        gold_lake_root=str(gold_root),
        completeness_threshold=0.1,
        plot=False,
        manifest=False,
    )

    assert len(first_files) == 1
    assert len(second_files) == 1
    assert Path(first_files[0]).stem != Path(second_files[0]).stem


def test_transform_l2_silver_to_gold_rebuilds_when_fill_policy_changes(tmp_path: Path) -> None:
    """Verify fill_missing_minutes setting participates in Gold incremental invalidation."""

    silver_root = tmp_path / "silver"
    gold_root = tmp_path / "gold"
    _write_silver_partition(
        silver_root,
        "BTC-PERPETUAL",
        [
            _silver_row(second=0, minute=0, mid_price=100.0, symbol="BTC-PERPETUAL"),
            _silver_row(second=0, minute=2, mid_price=102.0, symbol="BTC-PERPETUAL"),
        ],
    )

    first_files = transform_l2_silver_to_gold(
        silver_lake_root=str(silver_root),
        gold_lake_root=str(gold_root),
        fill_missing_minutes=False,
        fill_policy="neighbor",
        plot=False,
        manifest=False,
    )
    second_files = transform_l2_silver_to_gold(
        silver_lake_root=str(silver_root),
        gold_lake_root=str(gold_root),
        fill_missing_minutes=True,
        fill_policy="neighbor",
        plot=False,
        manifest=False,
    )

    assert len(first_files) == 1
    assert len(second_files) == 1
    assert Path(first_files[0]).stem != Path(second_files[0]).stem

    third_files = transform_l2_silver_to_gold(
        silver_lake_root=str(silver_root),
        gold_lake_root=str(gold_root),
        fill_missing_minutes=True,
        fill_policy="hybrid",
        plot=False,
        manifest=False,
    )

    assert len(third_files) == 1
    assert Path(second_files[0]).stem != Path(third_files[0]).stem


def test_transform_l2_silver_to_gold_rejects_unknown_fill_policy(tmp_path: Path) -> None:
    """Verify unknown fill policies fail fast before transform execution."""

    silver_root = tmp_path / "silver"
    gold_root = tmp_path / "gold"
    _write_silver_partition(
        silver_root,
        "BTC-PERPETUAL",
        [_silver_row(second=0, mid_price=100.0, symbol="BTC-PERPETUAL")],
    )

    with pytest.raises(ValueError, match="fill_policy must be one of"):
        transform_l2_silver_to_gold(
            silver_lake_root=str(silver_root),
            gold_lake_root=str(gold_root),
            fill_missing_minutes=True,
            fill_policy="bogus",
            plot=False,
            manifest=False,
        )


def test_gold_metadata_records_fill_policy_flag() -> None:
    """Verify Gold metadata includes the fill policy used for artifact generation."""

    silver = pl.DataFrame(
        [
            _silver_row(second=0, minute=0, mid_price=100.0),
            _silver_row(second=0, minute=2, mid_price=102.0),
        ]
    )
    gold = gold_l2_m1_from_silver(silver, fill_missing_minutes=True)
    metadata = gold_metadata(
        gold=gold,
        source_summary=silver_source_summary(silver),
        hash_string="hash123",
        git_commit_hash="abcdef1234567890",
        expected_snapshots_per_minute=6,
        completeness_threshold=0.8,
        feature_set_version="gold_l2_m1_v1",
        fill_missing_minutes=True,
        fill_policy="hybrid",
    )

    assert metadata["fill_missing_minutes"] is True
    assert metadata["fill_policy"] == "hybrid"

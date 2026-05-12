"""Polars transformations from silver L2 features to gold M1 artifacts."""

from __future__ import annotations

import hashlib
import json
import math
import subprocess
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import polars as pl

from ingestion.artifact_state import file_fingerprints, load_json_state, write_json_state

GOLD_FEATURE_SET_VERSION = "gold_l2_m1_v1"
GOLD_L2_M1_DATASET_TYPE = "l2_m1_features"
GOLD_TIMEFRAME = "1m"
GOLD_PLOT_MAX_POINTS = 3000
EXPECTED_SNAPSHOTS_PER_MINUTE = 6
COMPLETE_MINUTE_COVERAGE_THRESHOLD = 0.8
DEPTH_WINDOWS = (1, 5, 10, 20, 50)

GOLD_KEY_COLUMNS = ["ts_minute", "exchange", "symbol", "instrument_type", "depth", "feature_set_version"]
GOLD_DATASET_PARTITION_COLUMNS = ["exchange", "instrument_type", "symbol", "depth", "feature_set_version"]
GOLD_METADATA_COLUMNS = [
    "snapshot_count",
    "coverage_ratio",
    "first_snapshot_ts",
    "last_snapshot_ts",
    "is_complete_minute",
    "quality_flags",
]
GOLD_NUMERIC_FEATURES = [
    "mid_open",
    "mid_high",
    "mid_low",
    "mid_close",
    "mid_mean",
    "mid_std",
    "spread_bps_mean",
    "spread_bps_max",
    "spread_bps_p95",
    "microprice_mean",
    "microprice_close",
    "microprice_minus_mid_mean",
    "imbalance_1_mean",
    "imbalance_5_mean",
    "imbalance_10_mean",
    "imbalance_20_mean",
    "imbalance_50_mean",
    "bid_volume_1_mean",
    "ask_volume_1_mean",
    "bid_volume_5_mean",
    "ask_volume_5_mean",
    "bid_volume_10_mean",
    "ask_volume_10_mean",
    "bid_volume_20_mean",
    "ask_volume_20_mean",
    "bid_volume_50_mean",
    "ask_volume_50_mean",
    "book_pressure_1_mean",
    "book_pressure_5_mean",
    "book_pressure_10_mean",
    "book_pressure_20_mean",
    "book_pressure_50_mean",
    "mark_price_last",
    "index_price_last",
    "open_interest_last",
    "funding_rate_last",
]
GOLD_COLUMNS = [*GOLD_KEY_COLUMNS, *GOLD_METADATA_COLUMNS, *GOLD_NUMERIC_FEATURES]
GOLD_STATE_FILE_NAME = "_gold_transform_state.json"


@dataclass(frozen=True)
class GoldDatasetIdentity:
    """Partition identity for one full Gold timeframe dataset."""

    exchange: str
    instrument_type: str
    symbol: str
    depth: int
    feature_set_version: str

    @property
    def base_symbol(self) -> str:
        """Return the base-asset symbol used in Gold artifact filenames."""

        return base_asset_symbol(self.symbol)


def transform_l2_silver_to_gold(
    silver_lake_root: str,
    gold_lake_root: str,
    expected_snapshots_per_minute: int = EXPECTED_SNAPSHOTS_PER_MINUTE,
    completeness_threshold: float = COMPLETE_MINUTE_COVERAGE_THRESHOLD,
    feature_set_version: str = GOLD_FEATURE_SET_VERSION,
    plot: bool = True,
    manifest: bool = True,
    fill_missing_minutes: bool = False,
) -> list[str]:
    """Transform silver L2 snapshot features into per-symbol gold M1 artifacts."""

    if expected_snapshots_per_minute <= 0:
        raise ValueError("expected_snapshots_per_minute must be positive")
    if not 0 < completeness_threshold <= 1:
        raise ValueError("completeness_threshold must be in (0, 1]")

    silver_files = silver_parquet_files(silver_lake_root)
    if not silver_files:
        return []

    state_path = gold_transform_state_path(gold_lake_root)
    current_fingerprints = file_fingerprints(silver_files)
    state = load_json_state(state_path)
    symbol_state = state.get("symbols", {})
    symbol_files = silver_files_by_symbol(silver_files)
    git_commit_hash = current_git_commit_hash()
    transform_settings_unchanged = (
        state.get("expected_snapshots_per_minute") == expected_snapshots_per_minute
        and state.get("completeness_threshold") == completeness_threshold
        and state.get("feature_set_version") == feature_set_version
        and state.get("git_commit_hash") == git_commit_hash
        and state.get("fill_missing_minutes") == fill_missing_minutes
    )
    changed_symbols = [
        symbol
        for symbol, paths in sorted(symbol_files.items())
        if not transform_settings_unchanged
        or not isinstance(symbol_state, dict)
        or symbol_state.get(symbol, {}).get("silver_inputs")
        != {str(path.resolve()): current_fingerprints[str(path.resolve())] for path in paths}
    ]
    if not changed_symbols:
        return []

    written_files: list[str] = []
    next_symbol_state = symbol_state if isinstance(symbol_state, dict) else {}
    for symbol in changed_symbols:
        paths = symbol_files[symbol]
        symbol_fingerprints = {str(path.resolve()): current_fingerprints[str(path.resolve())] for path in paths}
        silver = pl.read_parquet([str(path) for path in paths])
        gold = gold_l2_m1_from_silver(
            silver=silver,
            expected_snapshots_per_minute=expected_snapshots_per_minute,
            completeness_threshold=completeness_threshold,
            feature_set_version=feature_set_version,
            fill_missing_minutes=fill_missing_minutes,
        )
        symbol_written_files = write_gold_l2_m1_artifacts(
            gold=gold,
            gold_lake_root=gold_lake_root,
            source_summary=silver_source_summary(silver),
            git_commit_hash=git_commit_hash,
            expected_snapshots_per_minute=expected_snapshots_per_minute,
            completeness_threshold=completeness_threshold,
            feature_set_version=feature_set_version,
            plot=plot,
            manifest=manifest,
            densify=False,
            source_fingerprints=symbol_fingerprints,
            fill_missing_minutes=fill_missing_minutes,
        )
        written_files.extend(symbol_written_files)
        next_symbol_state[symbol] = {
            "silver_inputs": symbol_fingerprints,
            "last_written_files": symbol_written_files,
        }

    write_json_state(
        state_path,
        {
            "schema_version": "v1",
            "silver_lake_root": str(Path(silver_lake_root).resolve()),
            "gold_lake_root": str(Path(gold_lake_root).resolve()),
            "expected_snapshots_per_minute": expected_snapshots_per_minute,
            "completeness_threshold": completeness_threshold,
            "feature_set_version": feature_set_version,
            "git_commit_hash": git_commit_hash,
            "fill_missing_minutes": fill_missing_minutes,
            "changed_symbols": changed_symbols,
            "symbols": next_symbol_state,
        },
    )
    return sorted(written_files)


def silver_parquet_files(silver_lake_root: str) -> list[Path]:
    """Return silver parquet files, preferring month-named files over legacy data.parquet files."""

    all_files = sorted(Path(silver_lake_root).glob("dataset_type=l2_snapshot_features/**/*.parquet"))
    month_named_dirs = {path.parent for path in all_files if path.name != "data.parquet"}
    return [path for path in all_files if path.name != "data.parquet" or path.parent not in month_named_dirs]


def silver_files_by_symbol(silver_files: list[Path]) -> dict[str, list[Path]]:
    """Group Silver parquet files by symbol partition."""

    grouped: dict[str, list[Path]] = {}
    for path in silver_files:
        symbol = silver_symbol_from_path(path)
        grouped.setdefault(symbol, []).append(path)
    return {symbol: sorted(paths) for symbol, paths in sorted(grouped.items())}


def silver_symbol_from_path(path: Path) -> str:
    """Extract the symbol partition value from a Silver parquet path."""

    for part in path.parts:
        if part.startswith("symbol="):
            return part.removeprefix("symbol=")
    raise ValueError(f"Silver parquet path is missing symbol partition: {path}")


def gold_transform_state_path(gold_lake_root: str) -> Path:
    """Return the Gold incremental transform state path."""

    return Path(gold_lake_root) / GOLD_STATE_FILE_NAME


def gold_l2_m1_from_silver(
    silver: pl.DataFrame,
    expected_snapshots_per_minute: int = EXPECTED_SNAPSHOTS_PER_MINUTE,
    completeness_threshold: float = COMPLETE_MINUTE_COVERAGE_THRESHOLD,
    feature_set_version: str = GOLD_FEATURE_SET_VERSION,
    fill_missing_minutes: bool = False,
) -> pl.DataFrame:
    """Aggregate silver L2 snapshot features to one M1 gold row per symbol."""

    if silver.is_empty():
        return pl.DataFrame(schema={column: pl.Null for column in GOLD_COLUMNS})
    if expected_snapshots_per_minute <= 0:
        raise ValueError("expected_snapshots_per_minute must be positive")
    if not 0 < completeness_threshold <= 1:
        raise ValueError("completeness_threshold must be in (0, 1]")

    prepared = (
        silver.sort(["exchange", "symbol", "instrument_type", "depth", "ts_event"])
        .with_columns(
            pl.col("ts_event").dt.truncate("1m").alias("ts_minute"),
            (pl.col("microprice") - pl.col("mid_price")).alias("microprice_minus_mid"),
            *_book_pressure_exprs(),
        )
        .with_columns(pl.lit(feature_set_version).alias("feature_set_version"))
    )

    grouped = prepared.group_by(GOLD_KEY_COLUMNS, maintain_order=True).agg(
        pl.len().alias("snapshot_count"),
        (pl.len() / expected_snapshots_per_minute).alias("coverage_ratio"),
        pl.col("ts_event").first().alias("first_snapshot_ts"),
        pl.col("ts_event").last().alias("last_snapshot_ts"),
        pl.col("mid_price").first().alias("mid_open"),
        pl.col("mid_price").max().alias("mid_high"),
        pl.col("mid_price").min().alias("mid_low"),
        pl.col("mid_price").last().alias("mid_close"),
        pl.col("mid_price").mean().alias("mid_mean"),
        pl.col("mid_price").std().alias("mid_std"),
        pl.col("spread_bps").mean().alias("spread_bps_mean"),
        pl.col("spread_bps").max().alias("spread_bps_max"),
        pl.col("spread_bps").quantile(0.95, interpolation="nearest").alias("spread_bps_p95"),
        pl.col("microprice").mean().alias("microprice_mean"),
        pl.col("microprice").last().alias("microprice_close"),
        pl.col("microprice_minus_mid").mean().alias("microprice_minus_mid_mean"),
        *[pl.col(f"imbalance_{window}").mean().alias(f"imbalance_{window}_mean") for window in DEPTH_WINDOWS],
        *[pl.col(f"bid_volume_{window}").mean().alias(f"bid_volume_{window}_mean") for window in DEPTH_WINDOWS],
        *[pl.col(f"ask_volume_{window}").mean().alias(f"ask_volume_{window}_mean") for window in DEPTH_WINDOWS],
        *[pl.col(f"book_pressure_{window}").mean().alias(f"book_pressure_{window}_mean") for window in DEPTH_WINDOWS],
        pl.col("mark_price").last().alias("mark_price_last"),
        pl.col("index_price").last().alias("index_price_last"),
        pl.col("open_interest").last().alias("open_interest_last"),
        pl.col("funding_rate").last().alias("funding_rate_last"),
        pl.col("is_valid").not_().sum().alias("_invalid_snapshot_count"),
    )

    observed = (
        grouped.with_columns(
            (pl.col("coverage_ratio") >= completeness_threshold).alias("is_complete_minute"),
            _gold_quality_flags_expr(completeness_threshold=completeness_threshold),
        )
        .select(GOLD_COLUMNS)
        .sort(["symbol", "ts_minute"])
    )
    dense = densify_gold_m1_timeframe(observed)
    if fill_missing_minutes:
        return fill_gold_missing_minutes_with_neighbor_averages(dense)
    return dense


def densify_gold_m1_timeframe(gold: pl.DataFrame) -> pl.DataFrame:
    """Insert explicit missing M1 rows from each dataset's first to last observed minute."""

    if gold.is_empty():
        return gold

    dense_frames: list[pl.DataFrame] = []
    for partition in gold.partition_by(GOLD_DATASET_PARTITION_COLUMNS):
        identity = gold_dataset_identity(partition)
        ts_min = partition["ts_minute"].min()
        ts_max = partition["ts_minute"].max()
        if not isinstance(ts_min, datetime) or not isinstance(ts_max, datetime):
            dense_frames.append(partition)
            continue

        timeline = _minute_range(start=ts_min, end=ts_max)
        scaffold = pl.DataFrame(
            {
                "ts_minute": timeline,
                "exchange": [identity.exchange] * len(timeline),
                "symbol": [identity.symbol] * len(timeline),
                "instrument_type": [identity.instrument_type] * len(timeline),
                "depth": [identity.depth] * len(timeline),
                "feature_set_version": [identity.feature_set_version] * len(timeline),
            }
        )
        dense = (
            scaffold.join(partition, on=GOLD_KEY_COLUMNS, how="left")
            .with_columns(pl.col("snapshot_count").is_null().alias("_is_missing_timeframe_row"))
            .with_columns(
                pl.col("snapshot_count").fill_null(0),
                pl.col("coverage_ratio").fill_null(0.0),
                pl.col("is_complete_minute").fill_null(False),
                pl.when(pl.col("_is_missing_timeframe_row"))
                .then(pl.lit(["missing_minute"]))
                .otherwise(pl.col("quality_flags"))
                .alias("quality_flags"),
                *[pl.col(column).fill_null(float("nan")).alias(column) for column in GOLD_NUMERIC_FEATURES],
            )
            .drop("_is_missing_timeframe_row")
            .select(GOLD_COLUMNS)
        )
        dense_frames.append(dense)

    return pl.concat(dense_frames, how="vertical").sort(["symbol", "ts_minute"])


def fill_gold_missing_minutes_with_neighbor_averages(gold: pl.DataFrame) -> pl.DataFrame:
    """Fill missing Gold numeric features from adjacent observed minute averages."""

    if gold.is_empty():
        return gold

    filled_frames: list[pl.DataFrame] = []
    for partition in gold.partition_by(GOLD_DATASET_PARTITION_COLUMNS):
        rows = partition.sort("ts_minute").to_dicts()
        for index, row in enumerate(rows):
            if "missing_minute" not in _quality_flags(row):
                continue
            if index == 0 or index >= len(rows) - 1:
                continue
            previous_row = rows[index - 1]
            following_row = rows[index + 1]
            if "missing_minute" in _quality_flags(previous_row) or "missing_minute" in _quality_flags(following_row):
                continue
            for feature in GOLD_NUMERIC_FEATURES:
                row[feature] = _average_gold_feature(previous_row.get(feature), following_row.get(feature))
            row["quality_flags"] = _merged_quality_flags(row, "filled_neighbor_average")
        filled_frames.append(pl.DataFrame(rows, schema=partition.schema))

    return pl.concat(filled_frames, how="vertical").select(GOLD_COLUMNS).sort(["symbol", "ts_minute"])


def write_gold_l2_m1_artifacts(
    gold: pl.DataFrame,
    gold_lake_root: str,
    source_summary: dict[str, Any],
    git_commit_hash: str,
    expected_snapshots_per_minute: int = EXPECTED_SNAPSHOTS_PER_MINUTE,
    completeness_threshold: float = COMPLETE_MINUTE_COVERAGE_THRESHOLD,
    feature_set_version: str = GOLD_FEATURE_SET_VERSION,
    plot: bool = True,
    manifest: bool = True,
    densify: bool = True,
    source_fingerprints: dict[str, Any] | None = None,
    fill_missing_minutes: bool = False,
) -> list[str]:
    """Write full versioned Gold datasets under lake-style timeframe leaves."""

    if gold.is_empty():
        return []

    if densify:
        gold = densify_gold_m1_timeframe(gold)
        if fill_missing_minutes:
            gold = fill_gold_missing_minutes_with_neighbor_averages(gold)
    written_files: list[str] = []

    for symbol_frame in gold.partition_by(GOLD_DATASET_PARTITION_COLUMNS):
        identity = gold_dataset_identity(symbol_frame)
        source_symbol_summary = source_summary_for_symbol(source_summary, identity.symbol)
        gold_content_hash = dataframe_content_hash(symbol_frame.select(GOLD_COLUMNS))
        hash_payload = {
            "dataset_type": GOLD_L2_M1_DATASET_TYPE,
            "feature_set_version": identity.feature_set_version,
            "timeframe": GOLD_TIMEFRAME,
            "exchange": identity.exchange,
            "instrument_type": identity.instrument_type,
            "symbol": identity.symbol,
            "depth": identity.depth,
            "expected_snapshots_per_minute": expected_snapshots_per_minute,
            "completeness_threshold": completeness_threshold,
            "git_commit_hash": git_commit_hash,
            "source_summary": source_symbol_summary,
            "source_fingerprints": source_fingerprints or {},
            "gold_content_hash": gold_content_hash,
            "fill_missing_minutes": fill_missing_minutes,
            "gold_schema": {name: str(dtype) for name, dtype in symbol_frame.schema.items()},
        }
        hash_string = stable_json_hash(hash_payload)
        basename = f"{identity.base_symbol}_L2_{hash_string}_{git_commit_hash[:12]}"
        dataset_dir = gold_l2_m1_dataset_path(
            lake_root=gold_lake_root,
            feature_set_version=identity.feature_set_version,
            exchange=identity.exchange,
            instrument_type=identity.instrument_type,
            base_asset=identity.base_symbol,
            symbol=identity.symbol,
            depth=identity.depth,
            timeframe=GOLD_TIMEFRAME,
        )
        dataset_dir.mkdir(parents=True, exist_ok=True)
        parquet_path = dataset_dir / f"{basename}.parquet"
        json_path = dataset_dir / f"{basename}.json"
        png_path = dataset_dir / f"{basename}.png"

        symbol_frame.write_parquet(parquet_path)
        written_files.append(str(parquet_path.resolve()))

        metadata: dict[str, Any] | None = None
        if manifest or plot:
            metadata = gold_metadata(
                gold=symbol_frame,
                source_summary=source_symbol_summary,
                hash_string=hash_string,
                git_commit_hash=git_commit_hash,
                expected_snapshots_per_minute=expected_snapshots_per_minute,
                completeness_threshold=completeness_threshold,
                feature_set_version=identity.feature_set_version,
                source_fingerprints=source_fingerprints or {},
                gold_content_hash=gold_content_hash,
                fill_missing_minutes=fill_missing_minutes,
            )

        if manifest:
            json_path.write_text(json.dumps(metadata, indent=2, sort_keys=True), encoding="utf-8")
            written_files.append(str(json_path.resolve()))

        if plot:
            assert metadata is not None
            write_gold_profile_png(gold=symbol_frame, metadata=metadata, path=png_path)
            written_files.append(str(png_path.resolve()))

    return sorted(written_files)


def gold_l2_m1_dataset_path(
    lake_root: str,
    feature_set_version: str,
    exchange: str,
    instrument_type: str,
    base_asset: str,
    symbol: str,
    depth: int,
    timeframe: str = GOLD_TIMEFRAME,
) -> Path:
    """Return the Gold destination directory for one full versioned timeframe dataset."""

    return (
        Path(lake_root)
        / f"dataset_type={GOLD_L2_M1_DATASET_TYPE}"
        / f"feature_set_version={feature_set_version}"
        / f"exchange={exchange}"
        / f"instrument_type={instrument_type}"
        / f"base_asset={base_asset}"
        / f"symbol={symbol}"
        / f"depth={depth}"
        / f"timeframe={timeframe}"
    )


def gold_dataset_identity(gold: pl.DataFrame) -> GoldDatasetIdentity:
    """Extract the Gold dataset identity from a non-empty partition frame."""

    if gold.is_empty():
        raise ValueError("gold dataset identity requires at least one row")

    first = gold.row(0, named=True)
    return GoldDatasetIdentity(
        exchange=str(first["exchange"]),
        instrument_type=str(first["instrument_type"]),
        symbol=str(first["symbol"]),
        depth=int(first["depth"]),
        feature_set_version=str(first["feature_set_version"]),
    )


def silver_source_summary(silver: pl.DataFrame) -> dict[str, Any]:
    """Return source silver dataset summaries without filesystem paths."""

    by_symbol: list[dict[str, Any]] = []
    if not silver.is_empty():
        counts = silver.group_by("symbol").agg(pl.len().alias("row_count")).sort("symbol")
        by_symbol = [
            {
                "source_symbol": str(row["symbol"]),
                "row_count": int(row["row_count"]),
            }
            for row in counts.to_dicts()
        ]
    return {
        "columns": list(silver.columns),
        "row_count": silver.height,
        "source_symbols": by_symbol,
    }


def gold_metadata(
    gold: pl.DataFrame,
    source_summary: dict[str, Any],
    hash_string: str,
    git_commit_hash: str,
    expected_snapshots_per_minute: int,
    completeness_threshold: float,
    feature_set_version: str,
    source_fingerprints: dict[str, Any] | None = None,
    gold_content_hash: str | None = None,
    fill_missing_minutes: bool = False,
) -> dict[str, Any]:
    """Build JSON metadata for one gold artifact without filesystem paths."""

    timestamp_min = _scalar(gold["ts_minute"].min()) if "ts_minute" in gold.columns and gold.height else None
    timestamp_max = _scalar(gold["ts_minute"].max()) if "ts_minute" in gold.columns and gold.height else None
    return {
        "dataset_type": GOLD_L2_M1_DATASET_TYPE,
        "hash_string": hash_string,
        "git_commit_hash": git_commit_hash,
        "build_timestamp_utc": datetime.now(UTC).isoformat(),
        "feature_set_version": feature_set_version,
        "timeframe": GOLD_TIMEFRAME,
        "expected_snapshots_per_minute": expected_snapshots_per_minute,
        "completeness_threshold": completeness_threshold,
        "fill_missing_minutes": fill_missing_minutes,
        "row_count": gold.height,
        "column_count": len(gold.columns),
        "timestamp_min": timestamp_min,
        "timestamp_max": timestamp_max,
        "missing_minute_count": _missing_minute_count(gold),
        "source_silver_dataset_summaries": source_summary,
        "source_fingerprint_hash": stable_json_hash({"source_fingerprints": source_fingerprints or {}}),
        "gold_content_hash": gold_content_hash or dataframe_content_hash(gold.select(GOLD_COLUMNS)),
        "features": [_column_metadata(gold, column) for column in gold.columns],
    }


def write_gold_profile_png(gold: pl.DataFrame, metadata: dict[str, Any], path: Path) -> None:
    """Write a dark profile PNG with line plots and distribution histograms for numeric features."""

    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    numeric_features = [column for column in GOLD_NUMERIC_FEATURES if column in gold.columns]
    row_count = max(1, len(numeric_features))
    fig_height = max(6.0, row_count * 1.15)
    fig, axes = plt.subplots(
        nrows=row_count,
        ncols=2,
        figsize=(18, fig_height),
        gridspec_kw={"width_ratios": [4, 1]},
        squeeze=False,
    )
    fig.patch.set_facecolor("#111217")
    metadata_lines = gold_plot_metadata_lines(metadata=metadata, gold=gold)
    fig.suptitle(metadata_lines[0], color="#eceff4", fontsize=12, y=0.997)
    fig.text(
        0.01,
        0.982,
        "\n".join(metadata_lines[1:]),
        color="#d8dee9",
        fontsize=8,
        va="top",
        family="monospace",
    )
    plot_gold = gold_plot_sample(gold=gold, max_points=GOLD_PLOT_MAX_POINTS)
    ts_values = plot_gold["ts_minute"].to_list() if "ts_minute" in plot_gold.columns else list(range(plot_gold.height))
    missing_ts_values = _missing_minute_timestamps(plot_gold)
    feature_stats = gold_plot_feature_row_stats_map(gold=gold, features=numeric_features)

    legend = (
        f"{gold['symbol'][0] if gold.height else 'unknown'} | rows={metadata['row_count']} | "
        f"missing={metadata.get('missing_minute_count', 0)} | "
        f"plot_points={plot_gold.height}/{gold.height} | "
        f"{metadata['timestamp_min']} to {metadata['timestamp_max']} | hash={metadata['hash_string']}"
    )
    for index, feature in enumerate(numeric_features):
        values = plot_gold[feature].to_list()
        clean_values = [
            value for value in gold[feature].to_list() if isinstance(value, int | float) and math.isfinite(float(value))
        ]
        line_ax = axes[index][0]
        hist_ax = axes[index][1]
        for ax in (line_ax, hist_ax):
            ax.set_facecolor("#161922")
            ax.tick_params(colors="#d8dee9", labelsize=7)
            for spine in ax.spines.values():
                spine.set_color("#3b4252")
        for missing_ts in missing_ts_values:
            line_ax.axvspan(
                missing_ts - timedelta(seconds=30),
                missing_ts + timedelta(seconds=30),
                color="#bf616a",
                alpha=0.16,
                linewidth=0,
            )
        line_ax.plot(ts_values, values, color="#88c0d0", linewidth=0.9)
        line_ax.set_ylabel(feature, color="#eceff4", fontsize=7)
        feature_metadata_label = gold_plot_feature_metadata_label(
            metadata=metadata,
            plot_gold=plot_gold,
            feature=feature,
            feature_stats=feature_stats[feature],
        )
        line_ax.text(
            0.005,
            0.985,
            feature_metadata_label,
            transform=line_ax.transAxes,
            ha="left",
            va="top",
            fontsize=6,
            color="#d8dee9",
            family="monospace",
            bbox={
                "boxstyle": "round,pad=0.25",
                "facecolor": "#111217",
                "edgecolor": "#3b4252",
                "alpha": 0.82,
            },
        )
        if index == 0:
            line_ax.set_title("Gold M1 numeric feature lines", color="#eceff4", fontsize=11)
            line_ax.legend([legend], loc="upper left", fontsize=7, facecolor="#161922", labelcolor="#eceff4")
        if clean_values:
            hist_ax.hist(clean_values, bins=min(24, max(4, len(clean_values))), color="#a3be8c", alpha=0.85)
        if index == 0:
            hist_ax.set_title("Distribution", color="#eceff4", fontsize=11)

    fig.autofmt_xdate(rotation=20)
    fig.tight_layout(rect=(0, 0, 1, 0.94))
    fig.savefig(path, dpi=120, facecolor=fig.get_facecolor())
    plt.close(fig)


def gold_plot_metadata_lines(metadata: dict[str, Any], gold: pl.DataFrame) -> list[str]:
    """Return compact manifest metadata lines for Gold profile plots."""

    source_summary = metadata.get("source_silver_dataset_summaries", {})
    source_symbols = source_summary.get("source_symbols", []) if isinstance(source_summary, dict) else []
    source_symbol_text = ", ".join(
        f"{item.get('source_symbol')}:{item.get('row_count')}"
        for item in source_symbols
        if isinstance(item, dict)
    )
    symbol = str(gold["symbol"][0]) if gold.height and "symbol" in gold.columns else "unknown"
    exchange = str(gold["exchange"][0]) if gold.height and "exchange" in gold.columns else "unknown"
    instrument_type = (
        str(gold["instrument_type"][0]) if gold.height and "instrument_type" in gold.columns else "unknown"
    )
    depth = str(gold["depth"][0]) if gold.height and "depth" in gold.columns else "unknown"
    title = f"Gold {metadata.get('timeframe', GOLD_TIMEFRAME)} profile"
    source_row_count = source_summary.get("row_count") if isinstance(source_summary, dict) else None
    return [
        f"{title} | {exchange} {symbol} {instrument_type} depth={depth}",
        (
            f"dataset={metadata.get('dataset_type')} version={metadata.get('feature_set_version')} "
            f"hash={metadata.get('hash_string')} git={str(metadata.get('git_commit_hash', 'unknown'))[:12]}"
        ),
        (
            f"window={metadata.get('timestamp_min')} -> {metadata.get('timestamp_max')} "
            f"rows={metadata.get('row_count')} columns={metadata.get('column_count')} "
            f"missing_minutes={metadata.get('missing_minute_count', 0)}"
        ),
        (
            f"quality expected_snapshots_per_minute={metadata.get('expected_snapshots_per_minute')} "
            f"completeness_threshold={metadata.get('completeness_threshold')}"
        ),
        f"source_silver_rows={source_row_count} symbols={source_symbol_text or 'none'}",
        f"built_utc={metadata.get('build_timestamp_utc')}",
    ]


def gold_plot_sample(gold: pl.DataFrame, max_points: int = GOLD_PLOT_MAX_POINTS) -> pl.DataFrame:
    """Return at most ``max_points`` rows evenly representing the full Gold time scale."""

    if max_points <= 0:
        raise ValueError("max_points must be positive")
    if gold.height <= max_points:
        return gold
    if max_points == 1:
        return gold.head(1)

    sampled_indices = sorted(
        {
            round(index * (gold.height - 1) / (max_points - 1))
            for index in range(max_points)
        }
    )
    return gold.with_row_index("_plot_index").filter(pl.col("_plot_index").is_in(sampled_indices)).drop("_plot_index")


def gold_plot_feature_metadata_label(
    metadata: dict[str, Any],
    feature: str,
    feature_stats: dict[str, int],
    plot_gold: pl.DataFrame | None = None,
) -> str:
    """Return compact per-feature plot metadata."""

    plot_row_count = plot_gold.height if plot_gold is not None else feature_stats["row_count"]
    return "\n".join(
        [
            f"feature={feature}",
            f"time={metadata.get('timestamp_min')} -> {metadata.get('timestamp_max')}",
            (
                f"rows={feature_stats['row_count']} plot_rows={plot_row_count} "
                f"missing={metadata.get('missing_minute_count', 0)}"
            ),
            (
                f"valid={feature_stats['finite_count']} null={feature_stats['null_count']} "
                f"nan={feature_stats['nan_count']} nonfinite={feature_stats['nonfinite_count']}"
            ),
        ]
    )


def gold_plot_feature_row_stats_map(gold: pl.DataFrame, features: list[str]) -> dict[str, dict[str, int]]:
    """Return row-level statistics for plotted Gold features."""

    return {feature: gold_plot_feature_row_stats(gold=gold, feature=feature) for feature in features}


def gold_plot_feature_row_stats(gold: pl.DataFrame, feature: str) -> dict[str, int]:
    """Return row-level statistics for one plotted Gold feature."""

    if feature not in gold.columns:
        return {"row_count": gold.height, "null_count": 0, "nan_count": 0, "finite_count": 0, "nonfinite_count": 0}

    series = gold[feature]
    null_count = int(series.null_count())
    nan_count = int(series.is_nan().sum()) if series.dtype.is_numeric() else 0
    finite_count = int(series.is_finite().fill_null(False).sum()) if series.dtype.is_numeric() else 0
    nonfinite_count = max(0, gold.height - null_count - finite_count)
    return {
        "row_count": gold.height,
        "null_count": null_count,
        "nan_count": nan_count,
        "finite_count": finite_count,
        "nonfinite_count": nonfinite_count,
    }


def base_asset_symbol(symbol: str) -> str:
    """Return base-asset symbol for Deribit perpetual instruments."""

    value = symbol.upper()
    if "_" in value:
        return value.split("_", 1)[0]
    if "-" in value:
        return value.split("-", 1)[0]
    return value


def current_git_commit_hash() -> str:
    """Return the current git commit hash, or ``unknown`` when unavailable."""

    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            check=True,
            capture_output=True,
            text=True,
        )
    except (OSError, subprocess.CalledProcessError):
        return "unknown"
    return result.stdout.strip() or "unknown"


def stable_json_hash(payload: dict[str, Any]) -> str:
    """Return a short deterministic hash for JSON-serializable metadata."""

    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()[:12]


def dataframe_content_hash(frame: pl.DataFrame) -> str:
    """Return a stable content hash for a Polars frame."""

    sort_columns = [column for column in GOLD_KEY_COLUMNS if column in frame.columns]
    payload_frame = frame.sort(sort_columns) if sort_columns else frame
    payload = payload_frame.to_dicts()
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def source_summary_for_symbol(source_summary: dict[str, Any], symbol: str) -> dict[str, Any]:
    """Filter source summary to one symbol while preserving dataset columns."""

    symbols = [
        item
        for item in source_summary.get("source_symbols", [])
        if isinstance(item, dict) and item.get("source_symbol") == symbol
    ]
    return {
        "columns": source_summary.get("columns", []),
        "row_count": sum(int(item.get("row_count", 0)) for item in symbols),
        "source_symbols": symbols,
    }


def _minute_range(start: datetime, end: datetime) -> list[datetime]:
    """Return inclusive one-minute timestamps from start through end."""

    values: list[datetime] = []
    current = start
    while current <= end:
        values.append(current)
        current += timedelta(minutes=1)
    return values


def _missing_minute_timestamps(gold: pl.DataFrame) -> list[datetime]:
    """Return Gold timestamps explicitly marked as missing timeframe rows."""

    if "quality_flags" not in gold.columns or "ts_minute" not in gold.columns:
        return []
    rows = gold.select("ts_minute", "quality_flags").to_dicts()
    return [
        row["ts_minute"]
        for row in rows
        if isinstance(row["ts_minute"], datetime)
        and isinstance(row["quality_flags"], list)
        and "missing_minute" in row["quality_flags"]
    ]


def _missing_minute_count(gold: pl.DataFrame) -> int:
    """Return the number of explicit missing timeframe rows in a Gold dataset."""

    if "quality_flags" not in gold.columns:
        return 0
    return int(gold.select(pl.col("quality_flags").list.contains("missing_minute").sum()).item())


def _quality_flags(row: dict[str, Any]) -> list[str]:
    """Return quality flags from a Gold row dictionary."""

    flags = row.get("quality_flags", [])
    return [str(flag) for flag in flags] if isinstance(flags, list) else []


def _merged_quality_flags(row: dict[str, Any], flag: str) -> list[str]:
    """Return row quality flags with one additional unique flag."""

    flags = _quality_flags(row)
    return flags if flag in flags else [*flags, flag]


def _average_gold_feature(previous_value: object, following_value: object) -> float:
    """Average adjacent Gold numeric feature values, preserving NaN when either side is non-finite."""

    if not isinstance(previous_value, int | float) or not isinstance(following_value, int | float):
        return float("nan")
    previous = float(previous_value)
    following = float(following_value)
    if not math.isfinite(previous) or not math.isfinite(following):
        return float("nan")
    return (previous + following) / 2


def _book_pressure_exprs() -> list[pl.Expr]:
    """Return book-pressure expressions for standard depth windows."""

    expressions: list[pl.Expr] = []
    for window in DEPTH_WINDOWS:
        bid = pl.col(f"bid_volume_{window}")
        ask = pl.col(f"ask_volume_{window}")
        denominator = bid + ask
        expressions.append(
            pl.when(denominator > 0)
            .then(bid / denominator)
            .otherwise(None)
            .alias(f"book_pressure_{window}")
        )
    return expressions


def _gold_quality_flags_expr(completeness_threshold: float) -> pl.Expr:
    """Return deterministic Gold quality flags."""

    return pl.concat_list(
        [
            _flag_expr(pl.col("coverage_ratio") < completeness_threshold, "incomplete_minute"),
            _flag_expr(pl.col("_invalid_snapshot_count") > 0, "invalid_snapshot_present"),
        ]
    ).alias("quality_flags")


def _flag_expr(condition: pl.Expr, flag: str) -> pl.Expr:
    """Return a one-item flag list when a quality condition is true."""

    return pl.when(condition.fill_null(False)).then(pl.lit([flag])).otherwise(pl.lit([]))


def _column_metadata(gold: pl.DataFrame, column: str) -> dict[str, Any]:
    """Return dtype, null counts, and numeric stats for one column."""

    series = gold[column]
    metadata: dict[str, Any] = {
        "name": column,
        "dtype": str(series.dtype),
        "null_count": int(series.null_count()),
        "null_ratio": float(series.null_count() / max(1, gold.height)),
    }
    if series.dtype.is_numeric():
        metadata["nan_count"] = int(series.is_nan().sum())
        metadata["numeric_stats"] = {
            "mean": _scalar(series.mean()),
            "std": _scalar(series.std()),
            "min": _scalar(series.min()),
            "max": _scalar(series.max()),
        }
    return metadata


def _scalar(value: object) -> object:
    """Convert scalar values to JSON-safe representations."""

    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, float) and not math.isfinite(value):
        return None
    return value

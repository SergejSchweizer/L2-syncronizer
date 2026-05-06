"""Polars transformations from silver L2 features to gold M1 artifacts."""

from __future__ import annotations

import hashlib
import json
import math
import subprocess
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import polars as pl

GOLD_FEATURE_SET_VERSION = "gold_l2_m1_v1"
EXPECTED_SNAPSHOTS_PER_MINUTE = 6
COMPLETE_MINUTE_COVERAGE_THRESHOLD = 0.8
DEPTH_WINDOWS = (1, 5, 10, 20, 50)

GOLD_KEY_COLUMNS = ["ts_minute", "exchange", "symbol", "instrument_type", "depth", "feature_set_version"]
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


def transform_l2_silver_to_gold(
    silver_lake_root: str,
    gold_lake_root: str,
    expected_snapshots_per_minute: int = EXPECTED_SNAPSHOTS_PER_MINUTE,
    completeness_threshold: float = COMPLETE_MINUTE_COVERAGE_THRESHOLD,
    feature_set_version: str = GOLD_FEATURE_SET_VERSION,
    plot: bool = True,
    manifest: bool = True,
) -> list[str]:
    """Transform silver L2 snapshot features into per-symbol gold M1 artifacts."""

    if expected_snapshots_per_minute <= 0:
        raise ValueError("expected_snapshots_per_minute must be positive")
    if not 0 < completeness_threshold <= 1:
        raise ValueError("completeness_threshold must be in (0, 1]")

    silver_files = silver_parquet_files(silver_lake_root)
    if not silver_files:
        return []

    silver = pl.read_parquet([str(path) for path in silver_files])
    gold = gold_l2_m1_from_silver(
        silver=silver,
        expected_snapshots_per_minute=expected_snapshots_per_minute,
        completeness_threshold=completeness_threshold,
        feature_set_version=feature_set_version,
    )
    source_summary = silver_source_summary(silver)
    git_commit_hash = current_git_commit_hash()
    return write_gold_l2_m1_artifacts(
        gold=gold,
        gold_lake_root=gold_lake_root,
        source_summary=source_summary,
        git_commit_hash=git_commit_hash,
        expected_snapshots_per_minute=expected_snapshots_per_minute,
        completeness_threshold=completeness_threshold,
        feature_set_version=feature_set_version,
        plot=plot,
        manifest=manifest,
    )


def silver_parquet_files(silver_lake_root: str) -> list[Path]:
    """Return silver parquet files, preferring month-named files over legacy data.parquet files."""

    all_files = sorted(Path(silver_lake_root).glob("dataset_type=l2_snapshot_features/**/*.parquet"))
    month_named_dirs = {path.parent for path in all_files if path.name != "data.parquet"}
    return [path for path in all_files if path.name != "data.parquet" or path.parent not in month_named_dirs]


def gold_l2_m1_from_silver(
    silver: pl.DataFrame,
    expected_snapshots_per_minute: int = EXPECTED_SNAPSHOTS_PER_MINUTE,
    completeness_threshold: float = COMPLETE_MINUTE_COVERAGE_THRESHOLD,
    feature_set_version: str = GOLD_FEATURE_SET_VERSION,
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

    return (
        grouped.with_columns(
            (pl.col("coverage_ratio") >= completeness_threshold).alias("is_complete_minute"),
            _gold_quality_flags_expr(completeness_threshold=completeness_threshold),
        )
        .select(GOLD_COLUMNS)
        .sort(["symbol", "ts_minute"])
    )


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
) -> list[str]:
    """Write per-base-symbol gold Parquet, JSON metadata, and PNG profile artifacts."""

    if gold.is_empty():
        return []

    output_root = Path(gold_lake_root)
    output_root.mkdir(parents=True, exist_ok=True)
    written_files: list[str] = []

    for symbol_frame in gold.partition_by("symbol"):
        symbol = str(symbol_frame["symbol"][0])
        base_symbol = base_asset_symbol(symbol)
        hash_payload = {
            "feature_set_version": feature_set_version,
            "expected_snapshots_per_minute": expected_snapshots_per_minute,
            "completeness_threshold": completeness_threshold,
            "git_commit_hash": git_commit_hash,
            "source_summary": source_summary_for_symbol(source_summary, symbol),
            "gold_schema": {name: str(dtype) for name, dtype in symbol_frame.schema.items()},
        }
        hash_string = stable_json_hash(hash_payload)
        basename = f"{base_symbol}_{hash_string}_{git_commit_hash[:12]}"
        parquet_path = output_root / f"{basename}.parquet"
        json_path = output_root / f"{basename}.json"
        png_path = output_root / f"{basename}.png"

        symbol_frame.write_parquet(parquet_path)
        written_files.append(str(parquet_path.resolve()))

        metadata: dict[str, Any] | None = None
        if manifest or plot:
            metadata = gold_metadata(
                gold=symbol_frame,
                source_summary=source_summary_for_symbol(source_summary, symbol),
                hash_string=hash_string,
                git_commit_hash=git_commit_hash,
                expected_snapshots_per_minute=expected_snapshots_per_minute,
                completeness_threshold=completeness_threshold,
                feature_set_version=feature_set_version,
            )

        if manifest:
            json_path.write_text(json.dumps(metadata, indent=2, sort_keys=True), encoding="utf-8")
            written_files.append(str(json_path.resolve()))

        if plot:
            assert metadata is not None
            write_gold_profile_png(gold=symbol_frame, metadata=metadata, path=png_path)
            written_files.append(str(png_path.resolve()))

    return sorted(written_files)


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
) -> dict[str, Any]:
    """Build JSON metadata for one gold artifact without filesystem paths."""

    timestamp_min = _scalar(gold["ts_minute"].min()) if "ts_minute" in gold.columns and gold.height else None
    timestamp_max = _scalar(gold["ts_minute"].max()) if "ts_minute" in gold.columns and gold.height else None
    return {
        "hash_string": hash_string,
        "git_commit_hash": git_commit_hash,
        "build_timestamp_utc": datetime.now(UTC).isoformat(),
        "feature_set_version": feature_set_version,
        "expected_snapshots_per_minute": expected_snapshots_per_minute,
        "completeness_threshold": completeness_threshold,
        "row_count": gold.height,
        "column_count": len(gold.columns),
        "timestamp_min": timestamp_min,
        "timestamp_max": timestamp_max,
        "source_silver_dataset_summaries": source_summary,
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
    ts_values = gold["ts_minute"].to_list() if "ts_minute" in gold.columns else list(range(gold.height))

    legend = (
        f"{gold['symbol'][0] if gold.height else 'unknown'} | rows={metadata['row_count']} | "
        f"{metadata['timestamp_min']} to {metadata['timestamp_max']} | hash={metadata['hash_string']}"
    )
    for index, feature in enumerate(numeric_features):
        values = gold[feature].to_list()
        clean_values = [value for value in values if isinstance(value, int | float) and math.isfinite(float(value))]
        line_ax = axes[index][0]
        hist_ax = axes[index][1]
        for ax in (line_ax, hist_ax):
            ax.set_facecolor("#161922")
            ax.tick_params(colors="#d8dee9", labelsize=7)
            for spine in ax.spines.values():
                spine.set_color("#3b4252")
        line_ax.plot(ts_values, values, color="#88c0d0", linewidth=0.9)
        line_ax.set_ylabel(feature, color="#eceff4", fontsize=7)
        if index == 0:
            line_ax.set_title("Gold M1 numeric feature lines", color="#eceff4", fontsize=11)
            line_ax.legend([legend], loc="upper left", fontsize=7, facecolor="#161922", labelcolor="#eceff4")
        if clean_values:
            hist_ax.hist(clean_values, bins=min(24, max(4, len(clean_values))), color="#a3be8c", alpha=0.85)
        if index == 0:
            hist_ax.set_title("Distribution", color="#eceff4", fontsize=11)

    fig.autofmt_xdate(rotation=20)
    fig.tight_layout()
    fig.savefig(path, dpi=120, facecolor=fig.get_facecolor())
    plt.close(fig)


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

"""Polars transformations from bronze L2 snapshots to silver features."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import cast

import polars as pl

SILVER_L2_FEATURE_DATASET_TYPE = "l2_snapshot_features"
SILVER_SCHEMA_VERSION = "v1"
DEPTH_WINDOWS = (1, 5, 10, 20, 50)
SILVER_NATURAL_KEY = ["exchange", "symbol", "instrument_type", "source", "depth", "ts_event"]

SilverPartitionKey = tuple[str, str, str, str]


def silver_l2_snapshot_partition_path(lake_root: str, key: SilverPartitionKey) -> Path:
    """Return the monthly silver destination directory for L2 snapshot features."""

    exchange, instrument_type, symbol, month_partition = key
    return (
        Path(lake_root)
        / f"dataset_type={SILVER_L2_FEATURE_DATASET_TYPE}"
        / f"exchange={exchange}"
        / f"instrument_type={instrument_type}"
        / f"symbol={symbol}"
        / f"month={month_partition}"
    )


def transform_l2_bronze_to_silver(
    bronze_lake_root: str,
    silver_lake_root: str,
    depth: int = 50,
) -> list[str]:
    """Transform bronze L2 parquet snapshots into monthly silver feature partitions."""

    if depth <= 0:
        raise ValueError("depth must be positive")

    bronze_files = sorted(Path(bronze_lake_root).glob("dataset_type=l2_snapshot/**/*.parquet"))
    if not bronze_files:
        return []

    bronze = pl.read_parquet([str(path) for path in bronze_files])
    silver = silver_l2_features_from_bronze(bronze=bronze, depth=depth)
    return save_silver_l2_snapshot_features(silver=silver, lake_root=silver_lake_root)


def silver_l2_features_from_bronze(bronze: pl.DataFrame, depth: int = 50) -> pl.DataFrame:
    """Build one silver L2 feature row per bronze snapshot using Polars expressions."""

    if depth <= 0:
        raise ValueError("depth must be positive")

    enriched = (
        bronze.with_columns(
            pl.lit(SILVER_SCHEMA_VERSION).alias("schema_version"),
            pl.lit(SILVER_L2_FEATURE_DATASET_TYPE).alias("dataset_type"),
            pl.col("event_time").alias("ts_event"),
            pl.col("ingested_at").alias("ts_received"),
            pl.col("current_funding").alias("funding_rate"),
            pl.col("bids").list.eval(pl.element().struct.field("price")).alias("_bid_prices_raw"),
            pl.col("bids").list.eval(pl.element().struct.field("amount")).alias("_bid_sizes_raw"),
            pl.col("asks").list.eval(pl.element().struct.field("price")).alias("_ask_prices_raw"),
            pl.col("asks").list.eval(pl.element().struct.field("amount")).alias("_ask_sizes_raw"),
        )
        .with_columns(
            pl.col("_bid_prices_raw").list.get(0, null_on_oob=True).alias("best_bid_price"),
            pl.col("_bid_sizes_raw").list.get(0, null_on_oob=True).alias("best_bid_size"),
            pl.col("_ask_prices_raw").list.get(0, null_on_oob=True).alias("best_ask_price"),
            pl.col("_ask_sizes_raw").list.get(0, null_on_oob=True).alias("best_ask_size"),
            pl.col("ts_event").dt.strftime("%Y-%m").alias("month"),
            *_depth_volume_exprs(),
        )
        .with_columns(
            ((pl.col("best_bid_price") + pl.col("best_ask_price")) / 2).alias("mid_price"),
            (pl.col("best_ask_price") - pl.col("best_bid_price")).alias("spread"),
            *_imbalance_exprs(),
        )
        .with_columns(
            pl.when(pl.col("mid_price") > 0)
            .then(pl.col("spread") / pl.col("mid_price") * 10_000)
            .otherwise(None)
            .alias("spread_bps"),
            _microprice_expr(),
            _validation_flags_expr(depth=depth),
            _pad_list_expr("_bid_prices_raw", depth).alias("bid_prices"),
            _pad_list_expr("_bid_sizes_raw", depth).alias("bid_sizes"),
            _pad_list_expr("_ask_prices_raw", depth).alias("ask_prices"),
            _pad_list_expr("_ask_sizes_raw", depth).alias("ask_sizes"),
        )
        .with_columns((pl.col("validation_flags").list.len() == 0).alias("is_valid"))
    )

    return enriched.select(
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
    )


def save_silver_l2_snapshot_features(silver: pl.DataFrame, lake_root: str) -> list[str]:
    """Persist silver L2 feature rows to monthly parquet partitions idempotently."""

    written_files: list[str] = []
    if silver.is_empty():
        return written_files

    for partition in silver.partition_by(["exchange", "instrument_type", "symbol", "month"]):
        first = partition.row(0, named=True)
        key = (
            str(first["exchange"]),
            str(first["instrument_type"]),
            str(first["symbol"]),
            str(first["month"]),
        )
        part_dir = silver_l2_snapshot_partition_path(lake_root=lake_root, key=key)
        part_dir.mkdir(parents=True, exist_ok=True)
        file_path = part_dir / "data.parquet"
        staging_path = part_dir / f".staging-{datetime.now().strftime('%Y%m%dT%H%M%S%f')}.parquet"

        output = partition
        if file_path.exists():
            output = pl.concat([pl.read_parquet(file_path), partition], how="vertical")

        output = output.unique(subset=SILVER_NATURAL_KEY, keep="last").sort("ts_event")
        output.write_parquet(staging_path)
        staging_path.replace(file_path)
        written_files.append(str(file_path.resolve()))

    return sorted(written_files)


def _depth_volume_exprs() -> list[pl.Expr]:
    """Return bid and ask cumulative volume expressions for standard depth windows."""

    expressions: list[pl.Expr] = []
    for window in DEPTH_WINDOWS:
        expressions.extend(
            [
                pl.col("_bid_sizes_raw").list.head(window).list.sum().alias(f"bid_volume_{window}"),
                pl.col("_ask_sizes_raw").list.head(window).list.sum().alias(f"ask_volume_{window}"),
            ]
        )
    return expressions


def _imbalance_exprs() -> list[pl.Expr]:
    """Return volume imbalance expressions for standard depth windows."""

    expressions: list[pl.Expr] = []
    for window in DEPTH_WINDOWS:
        bid = pl.col(f"bid_volume_{window}")
        ask = pl.col(f"ask_volume_{window}")
        denominator = bid + ask
        expressions.append(
            pl.when(denominator > 0)
            .then((bid - ask) / denominator)
            .otherwise(None)
            .alias(f"imbalance_{window}")
        )
    return expressions


def _microprice_expr() -> pl.Expr:
    """Return top-of-book microprice expression."""

    denominator = pl.col("best_bid_size") + pl.col("best_ask_size")
    return (
        pl.when(denominator > 0)
        .then(
            (
                pl.col("best_bid_price") * pl.col("best_ask_size")
                + pl.col("best_ask_price") * pl.col("best_bid_size")
            )
            / denominator
        )
        .otherwise(None)
        .alias("microprice")
    )


def _pad_list_expr(column: str, depth: int) -> pl.Expr:
    """Pad a list column with nulls to exactly the requested depth."""

    return pl.concat_list([pl.col(column), pl.lit([None] * depth)]).list.head(depth)


def _validation_flags_expr(depth: int) -> pl.Expr:
    """Return deterministic validation flags for silver L2 feature rows."""

    flag_exprs = [
        _flag_expr(pl.col("_bid_prices_raw").list.len() == 0, "empty_bids"),
        _flag_expr(pl.col("_ask_prices_raw").list.len() == 0, "empty_asks"),
        _flag_expr(pl.col("_bid_prices_raw").list.len() < depth, "insufficient_bid_depth"),
        _flag_expr(pl.col("_ask_prices_raw").list.len() < depth, "insufficient_ask_depth"),
        _flag_expr(pl.col("best_bid_price") >= pl.col("best_ask_price"), "crossed_book"),
        _flag_expr(pl.col("_bid_prices_raw") != pl.col("_bid_prices_raw").list.sort(descending=True), "unsorted_bids"),
        _flag_expr(pl.col("_ask_prices_raw") != pl.col("_ask_prices_raw").list.sort(), "unsorted_asks"),
        _flag_expr(pl.col("_bid_prices_raw").list.eval(pl.element() <= 0).list.any(), "non_positive_bid_price"),
        _flag_expr(pl.col("_ask_prices_raw").list.eval(pl.element() <= 0).list.any(), "non_positive_ask_price"),
        _flag_expr(pl.col("_bid_sizes_raw").list.eval(pl.element() <= 0).list.any(), "non_positive_bid_size"),
        _flag_expr(pl.col("_ask_sizes_raw").list.eval(pl.element() <= 0).list.any(), "non_positive_ask_size"),
    ]
    return pl.concat_list(flag_exprs).alias("validation_flags")


def _flag_expr(condition: pl.Expr, flag: str) -> pl.Expr:
    """Return a one-item flag list when a validation condition is true."""

    return pl.when(condition.fill_null(False)).then(pl.lit([flag])).otherwise(pl.lit([]))


def silver_record_natural_key(record: dict[str, object]) -> tuple[str, str, str, str, int, datetime]:
    """Build the idempotent natural key for one silver L2 feature row."""

    ts_event = record["ts_event"]
    if not isinstance(ts_event, datetime):
        raise ValueError("ts_event must be datetime")
    return (
        str(record["exchange"]),
        str(record["symbol"]),
        str(record["instrument_type"]),
        str(record["source"]),
        int(cast(int, record["depth"])),
        ts_event,
    )

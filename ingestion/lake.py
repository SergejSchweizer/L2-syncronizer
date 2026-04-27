"""Parquet lake writing utilities for fetched market data."""

from __future__ import annotations

import concurrent.futures
from collections import defaultdict
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import DefaultDict

from ingestion.spot import SpotCandle

DatasetType = str
PartitionKey = tuple[str, str, str]
NaturalKey = tuple[str, str, str, datetime]



def utc_run_id() -> str:
    """Create a UTC run identifier for lake writes."""

    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")



def candle_partition_key(candle: SpotCandle) -> PartitionKey:
    """Build partition key as exchange/symbol/timeframe."""

    return (
        candle.exchange,
        candle.symbol,
        candle.interval,
    )



def partition_path(lake_root: str, dataset_type: DatasetType, key: PartitionKey) -> Path:
    """Return destination path for one parquet partition."""

    exchange, symbol, timeframe = key
    return (
        Path(lake_root)
        / f"dataset_type={dataset_type}"
        / f"exchange={exchange}"
        / f"symbol={symbol}"
        / f"timeframe={timeframe}"
    )



def candle_record(candle: SpotCandle, market: str, run_id: str, ingested_at: datetime) -> dict[str, object]:
    """Convert a candle to generic parquet-lake row format."""

    return {
        "schema_version": "v1",
        "dataset_type": "spot_ohlcv" if market == "spot" else "perp_ohlcv",
        "exchange": candle.exchange,
        "symbol": candle.symbol,
        "instrument_type": market,
        "event_time": candle.open_time,
        "ingested_at": ingested_at,
        "run_id": run_id,
        "source_endpoint": "public_market_data",
        "open_time": candle.open_time,
        "close_time": candle.close_time,
        "timeframe": candle.interval,
        "open": candle.open_price,
        "high": candle.high_price,
        "low": candle.low_price,
        "close": candle.close_price,
        "volume": candle.volume,
        "quote_volume": candle.quote_volume,
        "trade_count": candle.trade_count,
        "extra": asdict(candle),
    }



def record_natural_key(record: dict[str, object]) -> NaturalKey:
    """Build natural key for per-partition deduplication."""

    open_time = record["open_time"]
    if not isinstance(open_time, datetime):
        raise ValueError("open_time must be datetime")
    return (str(record["exchange"]), str(record["symbol"]), str(record["timeframe"]), open_time)


def merge_and_deduplicate_rows(existing: list[dict[str, object]], new: list[dict[str, object]]) -> list[dict[str, object]]:
    """Merge old/new rows and keep latest version for duplicate keys."""

    merged: dict[NaturalKey, dict[str, object]] = {}
    for record in existing:
        merged[record_natural_key(record)] = record
    for record in new:
        merged[record_natural_key(record)] = record

    rows = list(merged.values())
    rows.sort(key=lambda item: item["open_time"])
    return rows


def open_times_in_lake(
    lake_root: str,
    market: str,
    exchange: str,
    symbol: str,
    timeframe: str,
) -> list[datetime]:
    """Return all stored open_time values for one instrument/timeframe parquet file."""

    try:
        import pyarrow.parquet as pq
    except ImportError as exc:
        raise RuntimeError("pyarrow is required for parquet lake output. Install project dependencies.") from exc

    dataset_type = "spot_ohlcv" if market == "spot" else "perp_ohlcv"
    data_file = (
        Path(lake_root)
        / f"dataset_type={dataset_type}"
        / f"exchange={exchange}"
        / f"symbol={symbol}"
        / f"timeframe={timeframe}"
        / "data.parquet"
    )
    if not data_file.exists():
        return []

    table = pq.ParquetFile(data_file).read(columns=["open_time"])
    values = [value for value in table.column("open_time").to_pylist() if isinstance(value, datetime)]
    return sorted(set(values))


def save_spot_candles_parquet_lake(
    candles_by_exchange: dict[str, dict[str, list[SpotCandle]]],
    market: str,
    lake_root: str,
) -> list[str]:
    """Save fetched candles to parquet lake partitions.

    Args:
        candles_by_exchange: Nested mapping ``exchange -> symbol_key -> candles``.
        market: Market type (`spot` or `perp`).
        lake_root: Root directory of parquet lake.

    Returns:
        List of absolute file paths written.

    Raises:
        RuntimeError: If ``pyarrow`` is unavailable.
    """

    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ImportError as exc:
        raise RuntimeError("pyarrow is required for parquet lake output. Install project dependencies.") from exc

    run_id = utc_run_id()
    ingested_at = datetime.now(timezone.utc)
    dataset_type = "spot_ohlcv" if market == "spot" else "perp_ohlcv"

    grouped: DefaultDict[PartitionKey, list[dict[str, object]]] = defaultdict(list)

    for symbol_map in candles_by_exchange.values():
        for candles in symbol_map.values():
            for candle in candles:
                key = candle_partition_key(candle)
                grouped[key].append(candle_record(candle=candle, market=market, run_id=run_id, ingested_at=ingested_at))

    def _write_one_partition(key: PartitionKey, rows: list[dict[str, object]]) -> str:
        part_dir = partition_path(lake_root=lake_root, dataset_type=dataset_type, key=key)
        part_dir.mkdir(parents=True, exist_ok=True)
        file_path = part_dir / "data.parquet"
        staging_path = part_dir / f".staging-{run_id}.parquet"

        existing_rows: list[dict[str, object]] = []
        if file_path.exists():
            existing_table = pq.ParquetFile(file_path).read()
            existing_rows = existing_table.to_pylist()

        merged_rows = merge_and_deduplicate_rows(existing=existing_rows, new=rows)
        table = pa.Table.from_pylist(merged_rows)
        pq.write_table(table, staging_path)
        staging_path.replace(file_path)
        return str(file_path.resolve())

    written_files: list[str] = []
    if grouped:
        max_workers = min(4, len(grouped))
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(_write_one_partition, key, rows) for key, rows in grouped.items()]
            for future in concurrent.futures.as_completed(futures):
                written_files.append(future.result())

    return sorted(written_files)

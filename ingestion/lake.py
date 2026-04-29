"""Parquet lake writing utilities for fetched market data."""

from __future__ import annotations

import concurrent.futures
from collections import defaultdict
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

from ingestion.open_interest import OpenInterestPoint
from ingestion.spot import SpotCandle

DatasetType = str
PartitionKey = tuple[str, str, str, str, str]
NaturalKey = tuple[str, str, str, str, datetime]


def utc_run_id() -> str:
    """Create a UTC run identifier for lake writes."""

    return datetime.now(UTC).strftime("%Y%m%dT%H%M%S%fZ")


def candle_partition_key(candle: SpotCandle, market: str) -> PartitionKey:
    """Build partition key as exchange/instrument_type/symbol/timeframe/date."""

    return (
        candle.exchange,
        market,
        candle.symbol,
        candle.interval,
        candle.open_time.strftime("%Y-%m"),
    )


def partition_path(lake_root: str, dataset_type: DatasetType, key: PartitionKey) -> Path:
    """Return destination path for one parquet partition."""

    exchange, instrument_type, symbol, timeframe, date_partition = key
    return (
        Path(lake_root)
        / f"dataset_type={dataset_type}"
        / f"exchange={exchange}"
        / f"instrument_type={instrument_type}"
        / f"symbol={symbol}"
        / f"timeframe={timeframe}"
        / f"date={date_partition}"
    )


def candle_record(candle: SpotCandle, market: str, run_id: str, ingested_at: datetime) -> dict[str, object]:
    """Convert a candle to generic parquet-lake row format."""

    return {
        "schema_version": "v1",
        "dataset_type": "ohlcv",
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


def open_interest_partition_key(item: OpenInterestPoint, market: str) -> PartitionKey:
    """Build partition key for open-interest records."""

    return (
        item.exchange,
        market,
        item.symbol,
        item.interval,
        item.open_time.strftime("%Y-%m"),
    )


def open_interest_record(
    item: OpenInterestPoint,
    market: str,
    run_id: str,
    ingested_at: datetime,
) -> dict[str, object]:
    """Convert open-interest point to parquet-lake row format."""

    return {
        "schema_version": "v1",
        "dataset_type": "open_interest",
        "exchange": item.exchange,
        "symbol": item.symbol,
        "instrument_type": market,
        "event_time": item.open_time,
        "ingested_at": ingested_at,
        "run_id": run_id,
        "source_endpoint": "public_open_interest",
        "open_time": item.open_time,
        "close_time": item.close_time,
        "timeframe": item.interval,
        "open_interest": item.open_interest,
        "open_interest_value": item.open_interest_value,
    }


def open_times_in_lake_by_dataset(
    lake_root: str,
    dataset_type: str,
    market: str,
    exchange: str,
    symbol: str,
    timeframe: str,
) -> list[datetime]:
    """Return stored open_time values for selected dataset/instrument/timeframe."""

    try:
        import pyarrow.parquet as pq
    except ImportError as exc:
        raise RuntimeError("pyarrow is required for parquet lake output. Install project dependencies.") from exc

    partition_root = (
        Path(lake_root)
        / f"dataset_type={dataset_type}"
        / f"exchange={exchange}"
        / f"instrument_type={market}"
        / f"symbol={symbol}"
        / f"timeframe={timeframe}"
    )
    if not partition_root.exists():
        return []

    values: list[datetime] = []
    for data_file in sorted(partition_root.glob("date=*/data.parquet")):
        parquet_file = pq.ParquetFile(data_file)  # type: ignore[no-untyped-call]
        for batch in parquet_file.iter_batches(columns=["open_time"], batch_size=10_000):  # type: ignore[no-untyped-call]
            for row in batch.to_pylist():
                value = row.get("open_time")
                if isinstance(value, datetime):
                    values.append(value)
    return sorted(set(values))


def record_natural_key(record: dict[str, object]) -> NaturalKey:
    """Build natural key for per-partition deduplication."""

    open_time = record["open_time"]
    if not isinstance(open_time, datetime):
        raise ValueError("open_time must be datetime")
    return (
        str(record["exchange"]),
        str(record["instrument_type"]),
        str(record["symbol"]),
        str(record["timeframe"]),
        open_time,
    )


def merge_and_deduplicate_rows(
    existing: list[dict[str, object]], new: list[dict[str, object]]
) -> list[dict[str, object]]:
    """Merge old/new rows and keep latest version for duplicate keys."""

    merged: dict[NaturalKey, dict[str, object]] = {}
    for record in existing:
        merged[record_natural_key(record)] = record
    for record in new:
        merged[record_natural_key(record)] = record

    rows = list(merged.values())
    rows.sort(key=lambda item: cast(datetime, item["open_time"]))
    return rows


def open_times_in_lake(
    lake_root: str,
    market: str,
    exchange: str,
    symbol: str,
    timeframe: str,
) -> list[datetime]:
    """Return all stored open_time values for one instrument/timeframe parquet file.

    Reads parquet files in batches to keep memory usage stable for large partitions.
    """

    return open_times_in_lake_by_dataset(
        lake_root=lake_root,
        dataset_type="ohlcv",
        market=market,
        exchange=exchange,
        symbol=symbol,
        timeframe=timeframe,
    )


def load_spot_candles_from_lake(
    lake_root: str,
    market: str,
    exchange: str,
    symbol: str,
    timeframe: str,
) -> list[SpotCandle]:
    """Load all stored candles for one exchange/symbol/timeframe from parquet lake.

    Uses batched parquet reads to avoid materializing complete files in memory.
    """

    try:
        import pyarrow.parquet as pq
    except ImportError as exc:
        raise RuntimeError("pyarrow is required for parquet lake output. Install project dependencies.") from exc

    partition_root = (
        Path(lake_root)
        / "dataset_type=ohlcv"
        / f"exchange={exchange}"
        / f"instrument_type={market}"
        / f"symbol={symbol}"
        / f"timeframe={timeframe}"
    )
    if not partition_root.exists():
        return []

    candles_by_open_time: dict[datetime, SpotCandle] = {}
    for data_file in sorted(partition_root.glob("date=*/data.parquet")):
        parquet_file = pq.ParquetFile(data_file)  # type: ignore[no-untyped-call]
        for batch in parquet_file.iter_batches(batch_size=10_000):  # type: ignore[no-untyped-call]
            for row in batch.to_pylist():
                open_time = row.get("open_time")
                close_time = row.get("close_time")
                if not isinstance(open_time, datetime) or not isinstance(close_time, datetime):
                    continue
                candles_by_open_time[open_time] = SpotCandle(
                    exchange=str(row.get("exchange", exchange)),
                    symbol=str(row.get("symbol", symbol)),
                    interval=str(row.get("timeframe", timeframe)),
                    open_time=open_time,
                    close_time=close_time,
                    open_price=float(row.get("open", 0.0)),
                    high_price=float(row.get("high", 0.0)),
                    low_price=float(row.get("low", 0.0)),
                    close_price=float(row.get("close", 0.0)),
                    volume=float(row.get("volume", 0.0)),
                    quote_volume=float(row.get("quote_volume", 0.0)),
                    trade_count=int(row.get("trade_count", 0)),
                )
    return [candles_by_open_time[key] for key in sorted(candles_by_open_time)]


def load_open_interest_from_lake(
    lake_root: str,
    market: str,
    exchange: str,
    symbol: str,
    timeframe: str,
) -> list[OpenInterestPoint]:
    """Load all stored open-interest rows for one exchange/symbol/timeframe from parquet lake."""

    try:
        import pyarrow.parquet as pq
    except ImportError as exc:
        raise RuntimeError("pyarrow is required for parquet lake output. Install project dependencies.") from exc

    partition_root = (
        Path(lake_root)
        / "dataset_type=open_interest"
        / f"exchange={exchange}"
        / f"instrument_type={market}"
        / f"symbol={symbol}"
        / f"timeframe={timeframe}"
    )
    if not partition_root.exists():
        return []

    items_by_open_time: dict[datetime, OpenInterestPoint] = {}
    for data_file in sorted(partition_root.glob("date=*/data.parquet")):
        parquet_file = pq.ParquetFile(data_file)  # type: ignore[no-untyped-call]
        for batch in parquet_file.iter_batches(batch_size=10_000):  # type: ignore[no-untyped-call]
            for row in batch.to_pylist():
                open_time = row.get("open_time")
                close_time = row.get("close_time")
                if not isinstance(open_time, datetime) or not isinstance(close_time, datetime):
                    continue
                items_by_open_time[open_time] = OpenInterestPoint(
                    exchange=str(row.get("exchange", exchange)),
                    symbol=str(row.get("symbol", symbol)),
                    interval=str(row.get("timeframe", timeframe)),
                    open_time=open_time,
                    close_time=close_time,
                    open_interest=float(row.get("open_interest", 0.0)),
                    open_interest_value=float(row.get("open_interest_value", 0.0)),
                )
    return [items_by_open_time[key] for key in sorted(items_by_open_time)]


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
    ingested_at = datetime.now(UTC)
    dataset_type = "ohlcv"

    grouped: defaultdict[PartitionKey, list[dict[str, object]]] = defaultdict(list)

    for symbol_map in candles_by_exchange.values():
        for candles in symbol_map.values():
            for candle in candles:
                key = candle_partition_key(candle=candle, market=market)
                grouped[key].append(candle_record(candle=candle, market=market, run_id=run_id, ingested_at=ingested_at))

    def _write_one_partition(key: PartitionKey, rows: list[dict[str, object]]) -> str:
        part_dir = partition_path(lake_root=lake_root, dataset_type=dataset_type, key=key)
        part_dir.mkdir(parents=True, exist_ok=True)
        file_path = part_dir / "data.parquet"
        staging_path = part_dir / f".staging-{run_id}.parquet"

        existing_rows: list[dict[str, object]] = []
        if file_path.exists():
            existing_table = pq.ParquetFile(file_path).read()  # type: ignore[no-untyped-call]
            existing_rows = existing_table.to_pylist()

        merged_rows = merge_and_deduplicate_rows(existing=existing_rows, new=rows)
        table = pa.Table.from_pylist(merged_rows)
        pq.write_table(table, staging_path)  # type: ignore[no-untyped-call]
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


def save_open_interest_parquet_lake(
    open_interest_by_exchange: dict[str, dict[str, list[OpenInterestPoint]]],
    market: str,
    lake_root: str,
) -> list[str]:
    """Save fetched open-interest data to parquet lake partitions."""

    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ImportError as exc:
        raise RuntimeError("pyarrow is required for parquet lake output. Install project dependencies.") from exc

    run_id = utc_run_id()
    ingested_at = datetime.now(UTC)
    dataset_type = "open_interest"

    grouped: defaultdict[PartitionKey, list[dict[str, object]]] = defaultdict(list)
    for symbol_map in open_interest_by_exchange.values():
        for items in symbol_map.values():
            for item in items:
                key = open_interest_partition_key(item=item, market=market)
                grouped[key].append(
                    open_interest_record(item=item, market=market, run_id=run_id, ingested_at=ingested_at)
                )

    def _write_one_partition(key: PartitionKey, rows: list[dict[str, object]]) -> str:
        part_dir = partition_path(lake_root=lake_root, dataset_type=dataset_type, key=key)
        part_dir.mkdir(parents=True, exist_ok=True)
        file_path = part_dir / "data.parquet"
        staging_path = part_dir / f".staging-{run_id}.parquet"

        existing_rows: list[dict[str, object]] = []
        if file_path.exists():
            existing_table = pq.ParquetFile(file_path).read()  # type: ignore[no-untyped-call]
            existing_rows = existing_table.to_pylist()

        merged_rows = merge_and_deduplicate_rows(existing=existing_rows, new=rows)
        table = pa.Table.from_pylist(merged_rows)
        pq.write_table(table, staging_path)  # type: ignore[no-untyped-call]
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


def load_combined_dataframe_from_lake(
    lake_root: str,
    exchanges: list[str] | None = None,
    symbols: list[str] | None = None,
    timeframes: list[str] | None = None,
    instrument_types: list[str] | None = None,
    start_time: datetime | None = None,
    end_time: datetime | None = None,
    limit: int | None = None,
    include_open_interest: bool = False,
) -> Any:
    """Load combined spot/perp OHLCV rows from parquet lake as a pandas DataFrame."""

    if limit is not None and limit <= 0:
        raise ValueError("limit must be positive when provided")

    try:
        import pandas as pd
    except ImportError as exc:
        raise RuntimeError("pandas is required for dataframe export. Install project dependencies.") from exc

    try:
        import pyarrow.parquet as pq
    except ImportError as exc:
        raise RuntimeError("pyarrow is required for dataframe export. Install project dependencies.") from exc

    exchange_filter = {item.lower() for item in exchanges} if exchanges else None
    symbol_filter = {item.upper() for item in symbols} if symbols else None
    timeframe_filter = {item.lower() for item in timeframes} if timeframes else None
    instrument_filter = {item.lower() for item in instrument_types} if instrument_types else None

    data_files = sorted(
        Path(lake_root).glob(
            "dataset_type=ohlcv/exchange=*/instrument_type=*/symbol=*/timeframe=*/date=*/data.parquet"
        )
    )

    frames: list[Any] = []
    for data_file in data_files:
        parts = data_file.parts
        partition_values: dict[str, str] = {}
        for segment in parts:
            if "=" not in segment:
                continue
            key, value = segment.split("=", 1)
            partition_values[key] = value

        exchange_value = partition_values.get("exchange", "")
        instrument_value = partition_values.get("instrument_type", "")
        symbol_value = partition_values.get("symbol", "")
        timeframe_value = partition_values.get("timeframe", "")

        if exchange_filter is not None and exchange_value.lower() not in exchange_filter:
            continue
        if instrument_filter is not None and instrument_value.lower() not in instrument_filter:
            continue
        if symbol_filter is not None and symbol_value.upper() not in symbol_filter:
            continue
        if timeframe_filter is not None and timeframe_value.lower() not in timeframe_filter:
            continue

        parquet_file = pq.ParquetFile(data_file)  # type: ignore[no-untyped-call]
        for batch in parquet_file.iter_batches(batch_size=10_000):  # type: ignore[no-untyped-call]
            frame = batch.to_pandas()
            if start_time is not None:
                frame = frame[frame["open_time"] >= start_time]
            if end_time is not None:
                frame = frame[frame["open_time"] <= end_time]
            if frame.empty:
                continue
            frames.append(frame)

    columns = [
        "exchange",
        "instrument_type",
        "symbol",
        "timeframe",
        "open_time",
        "close_time",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "quote_volume",
        "trade_count",
        "dataset_type",
        "run_id",
        "source_endpoint",
    ]
    if not frames:
        dataframe = pd.DataFrame(columns=columns)
    else:
        dataframe = pd.concat(frames, ignore_index=True)
        dataframe = dataframe.sort_values(
            by=["open_time", "exchange", "instrument_type", "symbol", "timeframe"],
            kind="mergesort",
        )
        if limit is not None:
            dataframe = dataframe.head(limit)

    if include_open_interest:
        oi_files = sorted(
            Path(lake_root).glob(
                "dataset_type=open_interest/exchange=*/instrument_type=*/symbol=*/timeframe=*/date=*/data.parquet"
            )
        )
        oi_frames: list[Any] = []
        for data_file in oi_files:
            oi_parts = data_file.parts
            oi_partition_values: dict[str, str] = {}
            for segment in oi_parts:
                if "=" not in segment:
                    continue
                key, value = segment.split("=", 1)
                oi_partition_values[key] = value

            exchange_value = oi_partition_values.get("exchange", "")
            instrument_value = oi_partition_values.get("instrument_type", "")
            symbol_value = oi_partition_values.get("symbol", "")
            timeframe_value = oi_partition_values.get("timeframe", "")

            if exchange_filter is not None and exchange_value.lower() not in exchange_filter:
                continue
            if instrument_filter is not None and instrument_value.lower() not in instrument_filter:
                continue
            if symbol_filter is not None and symbol_value.upper() not in symbol_filter:
                continue
            if timeframe_filter is not None and timeframe_value.lower() not in timeframe_filter:
                continue

            parquet_file = pq.ParquetFile(data_file)  # type: ignore[no-untyped-call]
            for batch in parquet_file.iter_batches(batch_size=10_000):  # type: ignore[no-untyped-call]
                frame = batch.to_pandas()
                if start_time is not None:
                    frame = frame[frame["open_time"] >= start_time]
                if end_time is not None:
                    frame = frame[frame["open_time"] <= end_time]
                if frame.empty:
                    continue
                oi_frames.append(frame)

        if oi_frames:
            oi_frame = pd.concat(oi_frames, ignore_index=True)
            oi_frame = oi_frame.sort_values(by=["open_time"], kind="mergesort")
            oi_frame = oi_frame.drop_duplicates(
                subset=["exchange", "instrument_type", "symbol", "timeframe", "open_time"],
                keep="last",
            )
            oi_frame = oi_frame[
                [
                    "exchange",
                    "instrument_type",
                    "symbol",
                    "timeframe",
                    "open_time",
                    "open_interest",
                    "open_interest_value",
                ]
            ]
            dataframe = dataframe.merge(
                oi_frame,
                on=["exchange", "instrument_type", "symbol", "timeframe", "open_time"],
                how="left",
            )
        else:
            dataframe["open_interest"] = None
            dataframe["open_interest_value"] = None

    return dataframe

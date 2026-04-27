"""Tests for parquet lake helper functions."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from ingestion.lake import (
    candle_partition_key,
    candle_record,
    merge_and_deduplicate_rows,
    open_times_in_lake,
    partition_path,
    save_spot_candles_parquet_lake,
)
from ingestion.spot import SpotCandle



def _sample_candle() -> SpotCandle:
    return SpotCandle(
        exchange="binance",
        symbol="BTCUSDT",
        interval="1m",
        open_time=datetime(2026, 4, 27, 10, 0, tzinfo=timezone.utc),
        close_time=datetime(2026, 4, 27, 10, 0, 59, 999000, tzinfo=timezone.utc),
        open_price=100.0,
        high_price=110.0,
        low_price=90.0,
        close_price=105.0,
        volume=15.0,
        quote_volume=1500.0,
        trade_count=42,
    )



def test_partition_key_and_path() -> None:
    candle = _sample_candle()
    key = candle_partition_key(candle)
    assert key == ("binance", "BTCUSDT", "1m")

    result = partition_path("lake/bronze", "spot_ohlcv", key)
    assert str(result).endswith("dataset_type=spot_ohlcv/exchange=binance/symbol=BTCUSDT/timeframe=1m")



def test_candle_record_contains_core_fields() -> None:
    candle = _sample_candle()
    ingested_at = datetime(2026, 4, 27, 12, 0, tzinfo=timezone.utc)
    record = candle_record(candle=candle, market="spot", run_id="run-1", ingested_at=ingested_at)

    assert record["dataset_type"] == "spot_ohlcv"
    assert record["instrument_type"] == "spot"
    assert record["run_id"] == "run-1"
    assert record["open"] == 100.0
    assert record["close"] == 105.0


def test_merge_and_deduplicate_rows_keeps_latest_record() -> None:
    first_time = datetime(2026, 4, 27, 10, 0, tzinfo=timezone.utc)
    second_time = datetime(2026, 4, 27, 11, 0, tzinfo=timezone.utc)
    base = {
        "exchange": "binance",
        "symbol": "BTCUSDT",
        "timeframe": "1m",
        "open_time": first_time,
        "close": 100.0,
    }
    existing = [base, {**base, "open_time": second_time, "close": 101.0}]
    new = [{**base, "close": 102.0}]

    merged = merge_and_deduplicate_rows(existing=existing, new=new)
    assert len(merged) == 2
    assert merged[0]["open_time"] == first_time
    assert merged[0]["close"] == 102.0


def test_save_spot_candles_parquet_lake_rewrites_single_partition_file(tmp_path: Path) -> None:
    candle_1 = SpotCandle(
        exchange="binance",
        symbol="BTCUSDT",
        interval="1m",
        open_time=datetime(2026, 4, 27, 10, 0, tzinfo=timezone.utc),
        close_time=datetime(2026, 4, 27, 10, 0, 59, 999000, tzinfo=timezone.utc),
        open_price=100.0,
        high_price=101.0,
        low_price=99.0,
        close_price=100.5,
        volume=10.0,
        quote_volume=1000.0,
        trade_count=10,
    )
    candle_2 = SpotCandle(
        exchange="binance",
        symbol="BTCUSDT",
        interval="1m",
        open_time=datetime(2026, 4, 27, 10, 1, tzinfo=timezone.utc),
        close_time=datetime(2026, 4, 27, 10, 1, 59, 999000, tzinfo=timezone.utc),
        open_price=100.5,
        high_price=102.0,
        low_price=100.0,
        close_price=101.5,
        volume=11.0,
        quote_volume=1100.0,
        trade_count=11,
    )

    first = {"binance": {"BTCUSDT": [candle_1]}}
    second = {"binance": {"BTCUSDT": [candle_1, candle_2]}}

    files_1 = save_spot_candles_parquet_lake(first, market="spot", lake_root=str(tmp_path))
    files_2 = save_spot_candles_parquet_lake(second, market="spot", lake_root=str(tmp_path))

    assert files_1 == files_2
    assert len(files_2) == 1
    assert files_2[0].endswith("/data.parquet")


def test_open_times_in_lake_returns_sorted_unique(tmp_path: Path) -> None:
    candle_1 = SpotCandle(
        exchange="binance",
        symbol="BTCUSDT",
        interval="1m",
        open_time=datetime(2026, 4, 27, 10, 0, tzinfo=timezone.utc),
        close_time=datetime(2026, 4, 27, 10, 0, 59, 999000, tzinfo=timezone.utc),
        open_price=100.0,
        high_price=101.0,
        low_price=99.0,
        close_price=100.5,
        volume=10.0,
        quote_volume=1000.0,
        trade_count=10,
    )
    candle_2 = SpotCandle(
        exchange="binance",
        symbol="BTCUSDT",
        interval="1m",
        open_time=datetime(2026, 4, 27, 10, 1, tzinfo=timezone.utc),
        close_time=datetime(2026, 4, 27, 10, 1, 59, 999000, tzinfo=timezone.utc),
        open_price=101.0,
        high_price=102.0,
        low_price=100.0,
        close_price=101.5,
        volume=11.0,
        quote_volume=1100.0,
        trade_count=11,
    )
    save_spot_candles_parquet_lake({"binance": {"BTCUSDT": [candle_2, candle_1, candle_1]}}, "spot", str(tmp_path))

    values = open_times_in_lake(
        lake_root=str(tmp_path),
        market="spot",
        exchange="binance",
        symbol="BTCUSDT",
        timeframe="1m",
    )

    assert values == [candle_1.open_time, candle_2.open_time]

"""Tests for fetch orchestration service."""

from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime

from application.services.fetch_service import (
    CandleFetchTask,
    OpenInterestFetchTask,
    fetch_candle_tasks_parallel,
    fetch_open_interest_tasks_parallel,
)
from ingestion.open_interest import OpenInterestPoint
from ingestion.spot import SpotCandle


def test_fetch_candle_tasks_parallel_splits_success_and_errors() -> None:
    task_ok = CandleFetchTask(exchange="deribit", market="spot", symbol="BTCUSDT", timeframe="1m")
    task_fail = CandleFetchTask(exchange="deribit", market="spot", symbol="ETHUSDT", timeframe="1m")

    candle = SpotCandle(
        exchange="deribit",
        symbol="BTCUSDT",
        interval="1m",
        open_time=datetime(2026, 4, 27, 10, 0, tzinfo=UTC),
        close_time=datetime(2026, 4, 27, 10, 0, 59, 999000, tzinfo=UTC),
        open_price=100.0,
        high_price=101.0,
        low_price=99.0,
        close_price=100.5,
        volume=10.0,
        quote_volume=1000.0,
        trade_count=10,
    )

    def fake_symbol_fetcher(
        exchange: str, market: str, symbol: str, timeframe: str, lake_root: str
    ) -> list[SpotCandle]:
        del exchange, market, timeframe, lake_root
        if symbol == "ETHUSDT":
            raise RuntimeError("boom")
        return [candle]

    result = asyncio.run(
        fetch_candle_tasks_parallel(
            tasks=[task_ok, task_fail],
            lake_root="lake/bronze",
            concurrency=2,
            logger=logging.getLogger("test"),
            symbol_fetcher=fake_symbol_fetcher,
        )
    )

    assert (task_ok.exchange, task_ok.market, task_ok.symbol, task_ok.timeframe) in result.rows
    assert (task_fail.exchange, task_fail.market, task_fail.symbol, task_fail.timeframe) in result.errors


def test_fetch_open_interest_tasks_parallel_splits_success_and_errors() -> None:
    task_ok = OpenInterestFetchTask(exchange="deribit", symbol="BTCUSDT", timeframe="5m")
    task_fail = OpenInterestFetchTask(exchange="deribit", symbol="ETHUSDT", timeframe="5m")

    point = OpenInterestPoint(
        exchange="deribit",
        symbol="BTCUSDT",
        interval="5m",
        open_time=datetime(2026, 4, 27, 10, 0, tzinfo=UTC),
        close_time=datetime(2026, 4, 27, 10, 4, 59, 999000, tzinfo=UTC),
        open_interest=1000.0,
        open_interest_value=2000.0,
    )

    def fake_symbol_fetcher(
        exchange: str, market: str, symbol: str, timeframe: str, lake_root: str
    ) -> list[OpenInterestPoint]:
        del exchange, market, timeframe, lake_root
        if symbol == "ETHUSDT":
            raise RuntimeError("boom")
        return [point]

    result = asyncio.run(
        fetch_open_interest_tasks_parallel(
            tasks=[task_ok, task_fail],
            lake_root="lake/bronze",
            concurrency=2,
            logger=logging.getLogger("test"),
            symbol_fetcher=fake_symbol_fetcher,
        )
    )

    assert (task_ok.exchange, task_ok.symbol, task_ok.timeframe) in result.rows
    assert (task_fail.exchange, task_fail.symbol, task_fail.timeframe) in result.errors

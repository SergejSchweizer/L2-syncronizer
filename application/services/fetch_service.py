"""Fetch orchestration services for OHLCV and open-interest datasets."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from datetime import datetime

from application.dto import (
    CandleFetchResultDTO,
    CandleFetchTaskDTO,
    FundingFetchResultDTO,
    FundingFetchTaskDTO,
    OpenInterestFetchResultDTO,
    OpenInterestFetchTaskDTO,
)
from application.services.gapfill_service import _last_closed_open_ms, _missing_ranges_ms
from ingestion.funding import (
    FundingPoint,
    fetch_funding_all_history,
    fetch_funding_range,
    funding_interval_to_milliseconds,
    normalize_funding_timeframe,
)
from ingestion.lake import open_times_in_lake, open_times_in_lake_by_dataset
from ingestion.open_interest import (
    OpenInterestPoint,
    fetch_open_interest_all_history,
    fetch_open_interest_range,
    normalize_open_interest_timeframe,
    open_interest_interval_to_milliseconds,
)
from ingestion.spot import (
    Exchange,
    Market,
    SpotCandle,
    fetch_candles_all_history,
    fetch_candles_range,
    interval_to_milliseconds,
    normalize_storage_symbol,
)

# Backward-compat aliases
CandleFetchTask = CandleFetchTaskDTO
OpenInterestFetchTask = OpenInterestFetchTaskDTO
FundingFetchTask = FundingFetchTaskDTO


def fetch_symbol_candles(
    exchange: Exchange,
    market: Market,
    symbol: str,
    timeframe: str,
    lake_root: str,
    open_times_reader: Callable[..., list[datetime]] = open_times_in_lake,
    symbol_normalizer: Callable[..., str] = normalize_storage_symbol,
    interval_ms_resolver: Callable[..., int] = interval_to_milliseconds,
    now_open_resolver: Callable[..., int] = _last_closed_open_ms,
    ranges_builder: Callable[..., list[tuple[int, int]]] = _missing_ranges_ms,
    history_fetcher: Callable[..., list[SpotCandle]] = fetch_candles_all_history,
    range_fetcher: Callable[..., list[SpotCandle]] = fetch_candles_range,
) -> list[SpotCandle]:
    """Fetch candles for one symbol with auto bootstrap/gap-fill behavior."""

    storage_symbol = symbol_normalizer(exchange=exchange, symbol=symbol, market=market)
    stored_open_times = open_times_reader(
        lake_root=lake_root,
        market=market,
        exchange=exchange,
        symbol=storage_symbol,
        timeframe=timeframe,
    )

    interval_ms = interval_ms_resolver(exchange=exchange, interval=timeframe)
    end_open_ms = now_open_resolver(interval_ms=interval_ms)
    if not stored_open_times:
        return history_fetcher(
            exchange=exchange,
            symbol=symbol,
            market=market,
            interval=timeframe,
        )
    if end_open_ms < int(min(stored_open_times).timestamp() * 1000):
        return []

    missing_ranges = ranges_builder(
        existing_open_times=stored_open_times,
        interval_ms=interval_ms,
        end_open_ms=end_open_ms,
    )
    if not missing_ranges:
        return []

    fetched: list[SpotCandle] = []
    for start_open_ms, gap_end_ms in missing_ranges:
        fetched.extend(
            range_fetcher(
                exchange=exchange,
                symbol=symbol,
                interval=timeframe,
                start_open_ms=start_open_ms,
                end_open_ms=gap_end_ms,
                market=market,
            )
        )

    unique_by_open_time = {item.open_time: item for item in fetched}
    return [unique_by_open_time[key] for key in sorted(unique_by_open_time)]


def fetch_symbol_open_interest(
    exchange: Exchange,
    market: Market,
    symbol: str,
    timeframe: str,
    lake_root: str,
    open_times_reader: Callable[..., list[datetime]] = open_times_in_lake_by_dataset,
    timeframe_normalizer: Callable[..., str] = normalize_open_interest_timeframe,
    symbol_normalizer: Callable[..., str] = normalize_storage_symbol,
    interval_ms_resolver: Callable[..., int] = open_interest_interval_to_milliseconds,
    now_open_resolver: Callable[..., int] = _last_closed_open_ms,
    ranges_builder: Callable[..., list[tuple[int, int]]] = _missing_ranges_ms,
    history_fetcher: Callable[..., list[OpenInterestPoint]] = fetch_open_interest_all_history,
    range_fetcher: Callable[..., list[OpenInterestPoint]] = fetch_open_interest_range,
) -> list[OpenInterestPoint]:
    """Fetch open-interest for one symbol with auto bootstrap/gap-fill behavior."""

    if market != "perp":
        return []

    normalized_interval = timeframe_normalizer(exchange=exchange, value=timeframe)
    storage_symbol = symbol_normalizer(exchange=exchange, symbol=symbol, market=market)
    stored_open_times = open_times_reader(
        lake_root=lake_root,
        dataset_type="open_interest",
        market=market,
        exchange=exchange,
        symbol=storage_symbol,
        timeframe=normalized_interval,
    )

    interval_ms = interval_ms_resolver(exchange=exchange, interval=normalized_interval)
    end_open_ms = now_open_resolver(interval_ms=interval_ms)
    if not stored_open_times:
        return history_fetcher(
            exchange=exchange,
            symbol=symbol,
            interval=normalized_interval,
            market=market,
        )
    if end_open_ms < int(min(stored_open_times).timestamp() * 1000):
        return []

    missing_ranges = ranges_builder(
        existing_open_times=stored_open_times,
        interval_ms=interval_ms,
        end_open_ms=end_open_ms,
    )
    if not missing_ranges:
        return []

    fetched: list[OpenInterestPoint] = []
    for start_open_ms, gap_end_ms in missing_ranges:
        fetched.extend(
            range_fetcher(
                exchange=exchange,
                symbol=symbol,
                interval=normalized_interval,
                start_open_ms=start_open_ms,
                end_open_ms=gap_end_ms,
                market=market,
            )
        )

    unique_by_open_time = {item.open_time: item for item in fetched}
    return [unique_by_open_time[key] for key in sorted(unique_by_open_time)]


def fetch_symbol_funding(
    exchange: Exchange,
    market: Market,
    symbol: str,
    timeframe: str,
    lake_root: str,
    open_times_reader: Callable[..., list[datetime]] = open_times_in_lake_by_dataset,
    timeframe_normalizer: Callable[..., str] = normalize_funding_timeframe,
    symbol_normalizer: Callable[..., str] = normalize_storage_symbol,
    interval_ms_resolver: Callable[..., int] = funding_interval_to_milliseconds,
    now_open_resolver: Callable[..., int] = _last_closed_open_ms,
    ranges_builder: Callable[..., list[tuple[int, int]]] = _missing_ranges_ms,
    history_fetcher: Callable[..., list[FundingPoint]] = fetch_funding_all_history,
    range_fetcher: Callable[..., list[FundingPoint]] = fetch_funding_range,
) -> list[FundingPoint]:
    """Fetch funding for one symbol with auto bootstrap/gap-fill behavior."""

    if market != "perp":
        return []

    normalized_interval = timeframe_normalizer(exchange=exchange, value=timeframe)
    storage_symbol = symbol_normalizer(exchange=exchange, symbol=symbol, market=market)
    stored_open_times = open_times_reader(
        lake_root=lake_root,
        dataset_type="funding",
        market=market,
        exchange=exchange,
        symbol=storage_symbol,
        timeframe=normalized_interval,
    )

    interval_ms = interval_ms_resolver(exchange=exchange, interval=normalized_interval)
    end_open_ms = now_open_resolver(interval_ms=interval_ms)
    if not stored_open_times:
        return history_fetcher(
            exchange=exchange,
            symbol=symbol,
            interval=normalized_interval,
            market=market,
        )
    if end_open_ms < int(min(stored_open_times).timestamp() * 1000):
        return []

    missing_ranges = ranges_builder(
        existing_open_times=stored_open_times,
        interval_ms=interval_ms,
        end_open_ms=end_open_ms,
    )
    if not missing_ranges:
        return []

    fetched: list[FundingPoint] = []
    for start_open_ms, gap_end_ms in missing_ranges:
        fetched.extend(
            range_fetcher(
                exchange=exchange,
                symbol=symbol,
                interval=normalized_interval,
                start_open_ms=start_open_ms,
                end_open_ms=gap_end_ms,
                market=market,
            )
        )

    unique_by_open_time = {item.open_time: item for item in fetched}
    return [unique_by_open_time[key] for key in sorted(unique_by_open_time)]


async def fetch_candle_tasks_parallel(
    tasks: list[CandleFetchTaskDTO],
    lake_root: str,
    concurrency: int,
    logger: logging.Logger,
    symbol_fetcher: Callable[..., list[SpotCandle]] = fetch_symbol_candles,
) -> CandleFetchResultDTO:
    """Fetch OHLCV tasks concurrently with bounded parallelism."""

    semaphore = asyncio.Semaphore(max(1, concurrency))
    total_tasks = len(tasks)

    async def _worker(
        idx: int,
        task: CandleFetchTaskDTO,
    ) -> tuple[tuple[Exchange, Market, str, str], list[SpotCandle], str | None]:
        logger.info(
            "Fetch start [%s/%s] exchange=%s market=%s symbol=%s timeframe=%s mode=%s",
            idx,
            total_tasks,
            task.exchange,
            task.market,
            task.symbol,
            task.timeframe,
            "auto-bootstrap-or-gap-fill",
        )
        key = (task.exchange, task.market, task.symbol, task.timeframe)
        async with semaphore:
            try:
                candles = await asyncio.to_thread(
                    symbol_fetcher,
                    task.exchange,
                    task.market,
                    task.symbol,
                    task.timeframe,
                    lake_root,
                )
                logger.info(
                    "Fetch complete [%s/%s] exchange=%s market=%s symbol=%s timeframe=%s candles=%s",
                    idx,
                    total_tasks,
                    task.exchange,
                    task.market,
                    task.symbol,
                    task.timeframe,
                    len(candles),
                )
                return key, candles, None
            except Exception as exc:  # noqa: BLE001
                logger.exception(
                    "Fetch failed exchange=%s market=%s symbol=%s timeframe=%s",
                    task.exchange,
                    task.market,
                    task.symbol,
                    task.timeframe,
                )
                return key, [], str(exc)

    results = await asyncio.gather(*[_worker(idx, task) for idx, task in enumerate(tasks, start=1)])
    task_results: dict[tuple[Exchange, Market, str, str], list[SpotCandle]] = {}
    task_errors: dict[tuple[Exchange, Market, str, str], str] = {}
    for key, candles, error in results:
        if error is None:
            task_results[key] = candles
        else:
            task_errors[key] = error
    return CandleFetchResultDTO(rows=task_results, errors=task_errors)


async def fetch_open_interest_tasks_parallel(
    tasks: list[OpenInterestFetchTaskDTO],
    lake_root: str,
    concurrency: int,
    logger: logging.Logger,
    symbol_fetcher: Callable[..., list[OpenInterestPoint]] = fetch_symbol_open_interest,
) -> OpenInterestFetchResultDTO:
    """Fetch open-interest tasks concurrently with bounded parallelism."""

    semaphore = asyncio.Semaphore(max(1, concurrency))
    total_tasks = len(tasks)

    async def _worker(
        idx: int,
        task: OpenInterestFetchTaskDTO,
    ) -> tuple[tuple[Exchange, str, str], list[OpenInterestPoint], str | None]:
        logger.info(
            "OI fetch start [%s/%s] exchange=%s market=oi symbol=%s timeframe=%s mode=%s",
            idx,
            total_tasks,
            task.exchange,
            task.symbol,
            task.timeframe,
            "auto-bootstrap-or-gap-fill",
        )
        key = (task.exchange, task.symbol, task.timeframe)
        async with semaphore:
            try:
                rows = await asyncio.to_thread(
                    symbol_fetcher,
                    task.exchange,
                    "perp",
                    task.symbol,
                    task.timeframe,
                    lake_root,
                )
                logger.info(
                    "OI fetch complete [%s/%s] exchange=%s symbol=%s timeframe=%s rows=%s",
                    idx,
                    total_tasks,
                    task.exchange,
                    task.symbol,
                    task.timeframe,
                    len(rows),
                )
                return key, rows, None
            except Exception as exc:  # noqa: BLE001
                logger.exception(
                    "OI fetch failed exchange=%s symbol=%s timeframe=%s",
                    task.exchange,
                    task.symbol,
                    task.timeframe,
                )
                return key, [], str(exc)

    results = await asyncio.gather(*[_worker(idx, task) for idx, task in enumerate(tasks, start=1)])
    task_results: dict[tuple[Exchange, str, str], list[OpenInterestPoint]] = {}
    task_errors: dict[tuple[Exchange, str, str], str] = {}
    for key, rows, error in results:
        if error is None:
            task_results[key] = rows
        else:
            task_errors[key] = error
    return OpenInterestFetchResultDTO(rows=task_results, errors=task_errors)


async def fetch_funding_tasks_parallel(
    tasks: list[FundingFetchTaskDTO],
    lake_root: str,
    concurrency: int,
    logger: logging.Logger,
    symbol_fetcher: Callable[..., list[FundingPoint]] = fetch_symbol_funding,
) -> FundingFetchResultDTO:
    """Fetch funding tasks concurrently with bounded parallelism."""

    semaphore = asyncio.Semaphore(max(1, concurrency))
    total_tasks = len(tasks)

    async def _worker(
        idx: int,
        task: FundingFetchTaskDTO,
    ) -> tuple[tuple[Exchange, str, str], list[FundingPoint], str | None]:
        logger.info(
            "Funding fetch start [%s/%s] exchange=%s market=funding symbol=%s timeframe=%s mode=%s",
            idx,
            total_tasks,
            task.exchange,
            task.symbol,
            task.timeframe,
            "auto-bootstrap-or-gap-fill",
        )
        key = (task.exchange, task.symbol, task.timeframe)
        async with semaphore:
            try:
                rows = await asyncio.to_thread(
                    symbol_fetcher,
                    task.exchange,
                    "perp",
                    task.symbol,
                    task.timeframe,
                    lake_root,
                )
                logger.info(
                    "Funding fetch complete [%s/%s] exchange=%s symbol=%s timeframe=%s rows=%s",
                    idx,
                    total_tasks,
                    task.exchange,
                    task.symbol,
                    task.timeframe,
                    len(rows),
                )
                return key, rows, None
            except Exception as exc:  # noqa: BLE001
                logger.exception(
                    "Funding fetch failed exchange=%s symbol=%s timeframe=%s",
                    task.exchange,
                    task.symbol,
                    task.timeframe,
                )
                return key, [], str(exc)

    results = await asyncio.gather(*[_worker(idx, task) for idx, task in enumerate(tasks, start=1)])
    task_results: dict[tuple[Exchange, str, str], list[FundingPoint]] = {}
    task_errors: dict[tuple[Exchange, str, str], str] = {}
    for key, rows, error in results:
        if error is None:
            task_results[key] = rows
        else:
            task_errors[key] = error
    return FundingFetchResultDTO(rows=task_results, errors=task_errors)

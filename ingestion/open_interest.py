"""Open-interest ingestion interface across exchanges."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, cast

from ingestion.exchanges import binance_open_interest, bybit_open_interest, deribit_open_interest
from ingestion.spot import Exchange, Market, interval_to_milliseconds, normalize_storage_symbol, normalize_timeframe


@dataclass(frozen=True)
class OpenInterestPoint:
    """Open-interest datapoint for one instrument interval."""

    exchange: str
    symbol: str
    interval: str
    open_time: datetime
    close_time: datetime
    open_interest: float
    open_interest_value: float


def normalize_open_interest_timeframe(exchange: Exchange, value: str) -> str:
    """Normalize open-interest timeframe by exchange."""

    if exchange == "binance":
        return binance_open_interest.normalize_period(value)
    if exchange == "bybit":
        return bybit_open_interest.normalize_period(value)
    if exchange == "deribit":
        # Deribit adapter currently supports snapshot collection bucketed by requested timeframe.
        return normalize_timeframe(exchange=exchange, value=value)
    raise ValueError(f"Unsupported exchange '{exchange}'")


def open_interest_interval_to_milliseconds(exchange: Exchange, interval: str) -> int:
    """Convert open-interest interval to milliseconds."""

    if exchange == "binance":
        return binance_open_interest.period_to_milliseconds(interval)
    if exchange == "bybit":
        return bybit_open_interest.period_to_milliseconds(interval)
    if exchange == "deribit":
        return interval_to_milliseconds(exchange=exchange, interval=interval)
    raise ValueError(f"Unsupported exchange '{exchange}'")


def _bucket_close_times(point_time: datetime, interval_ms: int) -> tuple[datetime, datetime]:
    """Bucket a timestamp into interval-aligned open/close times."""

    ts_ms = int(point_time.timestamp() * 1000)
    open_ms = (ts_ms // interval_ms) * interval_ms
    close_ms = open_ms + interval_ms - 1
    return (
        datetime.fromtimestamp(open_ms / 1000, tz=UTC),
        datetime.fromtimestamp(close_ms / 1000, tz=UTC),
    )


def fetch_open_interest_all_history(
    exchange: Exchange,
    symbol: str,
    interval: str,
    market: Market,
) -> list[OpenInterestPoint]:
    """Fetch all available open-interest history."""

    if market != "perp":
        return []
    normalized_interval = normalize_open_interest_timeframe(exchange=exchange, value=interval)
    normalized_symbol = normalize_storage_symbol(exchange=exchange, symbol=symbol, market=market)
    parsed: list[dict[str, object]] = []
    if exchange == "binance":
        rows = binance_open_interest.fetch_open_interest_all(symbol=normalized_symbol, period=normalized_interval)
        parsed = [
            binance_open_interest.parse_open_interest_row(normalized_symbol, normalized_interval, row)
            for row in rows
        ]
    elif exchange == "bybit":
        rows = bybit_open_interest.fetch_open_interest_all(symbol=normalized_symbol, period=normalized_interval)
        parsed = [
            bybit_open_interest.parse_open_interest_row(normalized_symbol, normalized_interval, row)
            for row in rows
        ]
    elif exchange == "deribit":
        snapshot = deribit_open_interest.fetch_current_open_interest(instrument_name=normalized_symbol)
        if snapshot is None:
            return []
        interval_ms = open_interest_interval_to_milliseconds(exchange=exchange, interval=normalized_interval)
        open_time, close_time = _bucket_close_times(
            point_time=datetime.fromtimestamp(int(cast(Any, snapshot["timestamp"])) / 1000, tz=UTC),
            interval_ms=interval_ms,
        )
        parsed = [
            {
                "open_time": open_time,
                "close_time": close_time,
                "open_interest": float(cast(Any, snapshot["open_interest"])),
                "open_interest_value": 0.0,
            }
        ]
    else:
        return []
    return [
        OpenInterestPoint(
            exchange=exchange,
            symbol=normalized_symbol,
            interval=normalized_interval,
            open_time=cast(datetime, cast(Any, item["open_time"])),
            close_time=cast(datetime, cast(Any, item["close_time"])),
            open_interest=float(cast(Any, item["open_interest"])),
            open_interest_value=float(cast(Any, item["open_interest_value"])),
        )
        for item in parsed
    ]


def fetch_open_interest_range(
    exchange: Exchange,
    symbol: str,
    interval: str,
    start_open_ms: int,
    end_open_ms: int,
    market: Market,
) -> list[OpenInterestPoint]:
    """Fetch open-interest by inclusive open-time range."""

    if market != "perp":
        return []
    normalized_interval = normalize_open_interest_timeframe(exchange=exchange, value=interval)
    normalized_symbol = normalize_storage_symbol(exchange=exchange, symbol=symbol, market=market)
    parsed: list[dict[str, object]] = []
    if exchange == "binance":
        rows = binance_open_interest.fetch_open_interest_range(
            symbol=normalized_symbol,
            period=normalized_interval,
            start_open_ms=start_open_ms,
            end_open_ms=end_open_ms,
        )
        parsed = [
            binance_open_interest.parse_open_interest_row(normalized_symbol, normalized_interval, row)
            for row in rows
        ]
    elif exchange == "bybit":
        rows = bybit_open_interest.fetch_open_interest_range(
            symbol=normalized_symbol,
            period=normalized_interval,
            start_open_ms=start_open_ms,
            end_open_ms=end_open_ms,
        )
        parsed = [
            bybit_open_interest.parse_open_interest_row(normalized_symbol, normalized_interval, row)
            for row in rows
        ]
    elif exchange == "deribit":
        snapshot = deribit_open_interest.fetch_current_open_interest(instrument_name=normalized_symbol)
        if snapshot is None:
            return []
        interval_ms = open_interest_interval_to_milliseconds(exchange=exchange, interval=normalized_interval)
        open_time, close_time = _bucket_close_times(
            point_time=datetime.fromtimestamp(int(cast(Any, snapshot["timestamp"])) / 1000, tz=UTC),
            interval_ms=interval_ms,
        )
        open_ms = int(open_time.timestamp() * 1000)
        if start_open_ms <= open_ms <= end_open_ms:
            parsed = [
                {
                    "open_time": open_time,
                    "close_time": close_time,
                    "open_interest": float(cast(Any, snapshot["open_interest"])),
                    "open_interest_value": 0.0,
                }
            ]
    else:
        return []
    return [
        OpenInterestPoint(
            exchange=exchange,
            symbol=normalized_symbol,
            interval=normalized_interval,
            open_time=cast(datetime, cast(Any, item["open_time"])),
            close_time=cast(datetime, cast(Any, item["close_time"])),
            open_interest=float(cast(Any, item["open_interest"])),
            open_interest_value=float(cast(Any, item["open_interest_value"])),
        )
        for item in parsed
    ]

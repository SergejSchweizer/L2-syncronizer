"""Spot/perpetual candle ingestion interface across exchanges."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Literal

from ingestion.exchanges import binance, deribit

Exchange = Literal["binance", "deribit"]
Market = Literal["spot", "perp"]


@dataclass(frozen=True)
class SpotCandle:
    """OHLCV candle for an instrument."""

    exchange: str
    symbol: str
    interval: str
    open_time: datetime
    close_time: datetime
    open_price: float
    high_price: float
    low_price: float
    close_price: float
    volume: float
    quote_volume: float
    trade_count: int



def _ms_to_utc(ts_ms: int) -> datetime:
    """Convert epoch milliseconds to timezone-aware UTC datetime."""

    return datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)



def parse_kline(exchange: Exchange, symbol: str, interval: str, row: list[object]) -> SpotCandle:
    """Parse a common kline row into a typed candle object."""

    return SpotCandle(
        exchange=exchange,
        symbol=symbol,
        interval=interval,
        open_time=_ms_to_utc(int(row[0])),
        close_time=_ms_to_utc(int(row[6])),
        open_price=float(row[1]),
        high_price=float(row[2]),
        low_price=float(row[3]),
        close_price=float(row[4]),
        volume=float(row[5]),
        quote_volume=float(row[7]),
        trade_count=int(row[8]),
    )



def list_supported_intervals(exchange: Exchange) -> tuple[str, ...]:
    """List supported intervals for the requested exchange."""

    if exchange == "binance":
        return binance.list_supported_intervals()
    if exchange == "deribit":
        return deribit.list_supported_intervals()
    raise ValueError(f"Unsupported exchange '{exchange}'")



def normalize_timeframe(exchange: Exchange, value: str) -> str:
    """Normalize timeframe aliases per exchange format."""

    if exchange == "binance":
        return binance.normalize_timeframe(value)
    if exchange == "deribit":
        return deribit.normalize_timeframe(value)
    raise ValueError(f"Unsupported exchange '{exchange}'")



def max_candles_per_request(exchange: Exchange) -> int:
    """Return max single-request candle count for exchange."""

    if exchange == "binance":
        return binance.max_limit()
    if exchange == "deribit":
        return deribit.max_limit()
    raise ValueError(f"Unsupported exchange '{exchange}'")



def interval_to_milliseconds(exchange: Exchange, interval: str) -> int:
    """Convert normalized interval into milliseconds for exchange."""

    if exchange == "binance":
        return binance.interval_to_milliseconds(interval)
    if exchange == "deribit":
        return deribit.interval_to_milliseconds(interval)
    raise ValueError(f"Unsupported exchange '{exchange}'")



def normalize_storage_symbol(exchange: Exchange, symbol: str, market: Market) -> str:
    """Normalize symbol to storage form for selected exchange/market."""

    if exchange == "binance":
        return binance.normalize_symbol(symbol=symbol, market=market)
    if exchange == "deribit":
        return deribit.normalize_symbol(symbol=symbol, market=market)
    raise ValueError(f"Unsupported exchange '{exchange}'")



def fetch_candles(
    exchange: Exchange,
    symbol: str,
    interval: str = "1h",
    limit: int = 100,
    market: Market = "spot",
) -> list[SpotCandle]:
    """Fetch latest candles from supported exchanges."""

    normalized_interval = normalize_timeframe(exchange=exchange, value=interval)
    normalized_symbol = normalize_storage_symbol(exchange=exchange, symbol=symbol, market=market)

    if exchange == "binance":
        rows = binance.fetch_klines(
            symbol=normalized_symbol,
            interval=normalized_interval,
            limit=limit,
            market=market,
        )
    elif exchange == "deribit":
        rows = deribit.fetch_klines(
            symbol=symbol,
            market=market,
            interval=normalized_interval,
            limit=limit,
        )
    else:
        raise ValueError(f"Unsupported exchange '{exchange}'")

    return [
        parse_kline(exchange=exchange, symbol=normalized_symbol, interval=normalized_interval, row=row)
        for row in rows
    ]



def fetch_candles_all_history(
    exchange: Exchange,
    symbol: str,
    interval: str = "1h",
    market: Market = "spot",
) -> list[SpotCandle]:
    """Fetch all available candles from exchange history."""

    normalized_interval = normalize_timeframe(exchange=exchange, value=interval)
    normalized_symbol = normalize_storage_symbol(exchange=exchange, symbol=symbol, market=market)

    if exchange == "binance":
        rows = binance.fetch_klines_all(
            symbol=normalized_symbol,
            interval=normalized_interval,
            market=market,
        )
    elif exchange == "deribit":
        rows = deribit.fetch_klines_all(
            symbol=symbol,
            market=market,
            interval=normalized_interval,
        )
    else:
        raise ValueError(f"Unsupported exchange '{exchange}'")

    return [
        parse_kline(exchange=exchange, symbol=normalized_symbol, interval=normalized_interval, row=row)
        for row in rows
    ]


def fetch_candles_range(
    exchange: Exchange,
    symbol: str,
    interval: str,
    start_open_ms: int,
    end_open_ms: int,
    market: Market = "spot",
) -> list[SpotCandle]:
    """Fetch candles by open-time range inclusive."""

    normalized_interval = normalize_timeframe(exchange=exchange, value=interval)
    normalized_symbol = normalize_storage_symbol(exchange=exchange, symbol=symbol, market=market)

    if exchange == "binance":
        rows = binance.fetch_klines_range(
            symbol=normalized_symbol,
            interval=normalized_interval,
            start_open_ms=start_open_ms,
            end_open_ms=end_open_ms,
            market=market,
        )
    elif exchange == "deribit":
        rows = deribit.fetch_klines_range(
            symbol=symbol,
            market=market,
            interval=normalized_interval,
            start_open_ms=start_open_ms,
            end_open_ms=end_open_ms,
        )
    else:
        raise ValueError(f"Unsupported exchange '{exchange}'")

    return [
        parse_kline(exchange=exchange, symbol=normalized_symbol, interval=normalized_interval, row=row)
        for row in rows
    ]



def fetch_binance_spot_candles(symbol: str, interval: str = "1h", limit: int = 100) -> list[SpotCandle]:
    """Compatibility wrapper for existing Binance-only callers."""

    return fetch_candles(exchange="binance", symbol=symbol, interval=interval, limit=limit, market="spot")



def list_binance_supported_intervals() -> tuple[str, ...]:
    """Compatibility wrapper for existing Binance-only callers."""

    return list_supported_intervals(exchange="binance")

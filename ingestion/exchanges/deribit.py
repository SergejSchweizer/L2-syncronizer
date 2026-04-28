"""Deribit candle download adapter."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, cast

from ingestion.http_client import get_json

DERIBIT_SUPPORTED_INTERVALS: tuple[str, ...] = (
    "1m",
    "3m",
    "5m",
    "10m",
    "15m",
    "30m",
    "1h",
    "2h",
    "3h",
    "6h",
    "12h",
    "1d",
)
DERIBIT_MAX_POINTS_PER_REQUEST = 5000


def list_supported_intervals() -> tuple[str, ...]:
    """Return Deribit-supported candle intervals."""

    return DERIBIT_SUPPORTED_INTERVALS


def max_limit() -> int:
    """Return max chart points Deribit allows per request window."""

    return DERIBIT_MAX_POINTS_PER_REQUEST


def normalize_timeframe(value: str) -> str:
    """Normalize user-provided timeframe aliases into Deribit interval format."""

    raw = value.strip()
    if not raw:
        raise ValueError("timeframe cannot be empty")

    lowered = raw.lower()
    if lowered.startswith("mn") and raw[2:].isdigit():
        candidate = f"{raw[2:]}M"
    elif raw[0].isalpha() and raw[1:].isdigit():
        candidate = f"{raw[1:]}{raw[0].lower()}"
    elif raw[:-1].isdigit() and raw[-1].isalpha():
        unit = raw[-1]
        if unit == "M":
            candidate = f"{raw[:-1]}M"
        else:
            candidate = f"{raw[:-1]}{unit.lower()}"
    else:
        candidate = lowered

    if candidate in DERIBIT_SUPPORTED_INTERVALS:
        return candidate

    raise ValueError(
        f"Unsupported timeframe '{value}' for deribit. Supported values: {', '.join(DERIBIT_SUPPORTED_INTERVALS)}"
    )


def to_deribit_resolution(interval: str) -> str:
    """Convert normalized interval into Deribit resolution parameter."""

    if interval.endswith("m"):
        return interval[:-1]
    if interval.endswith("h"):
        return str(int(interval[:-1]) * 60)
    if interval == "1d":
        return "1D"
    raise ValueError(f"Cannot map interval '{interval}' to Deribit resolution")


def interval_to_milliseconds(interval: str) -> int:
    """Convert normalized interval into milliseconds."""

    if interval.endswith("m"):
        return int(interval[:-1]) * 60_000
    if interval.endswith("h"):
        return int(interval[:-1]) * 3_600_000
    if interval == "1d":
        return 86_400_000
    raise ValueError(f"Unsupported interval '{interval}'")


def normalize_symbol(symbol: str, market: str) -> str:
    """Normalize user symbols for Deribit spot/perpetual markets."""

    upper = symbol.upper()
    if market == "perp":
        if upper in {"BTC", "BTCUSDT", "BTCUSD", "BTC-PERPETUAL"}:
            return "BTC-PERPETUAL"
        if upper in {"ETH", "ETHUSDT", "ETHUSD", "ETH-PERPETUAL"}:
            return "ETH-PERPETUAL"
        if upper.endswith("-PERPETUAL"):
            return upper
        raise ValueError("Unsupported Deribit perp symbol. Use BTC-PERPETUAL/ETH-PERPETUAL or BTC/ETH aliases.")

    if market == "spot":
        if upper in {"BTC", "BTCUSDT", "BTCUSD", "BTC_USDC"}:
            return "BTC_USDC"
        if upper in {"ETH", "ETHUSDT", "ETHUSD", "ETH_USDC"}:
            return "ETH_USDC"
        if "_" in upper:
            return upper
        raise ValueError("Unsupported Deribit spot symbol. Use BTC_USDC/ETH_USDC or BTC/ETH aliases.")

    raise ValueError("market must be either 'spot' or 'perp'")


def fetch_klines(symbol: str, market: str, interval: str, limit: int) -> list[list[object]]:
    """Fetch Deribit OHLCV-style chart data with pagination."""

    if limit <= 0:
        raise ValueError("limit must be positive")

    instrument_name = normalize_symbol(symbol=symbol, market=market)
    resolution = to_deribit_resolution(interval)
    now_ms = _utc_now_ms()
    window_ms = interval_to_milliseconds(interval)

    remaining = limit
    end_time_ms = now_ms
    pages: list[list[list[object]]] = []

    while remaining > 0:
        page_limit = min(remaining, DERIBIT_MAX_POINTS_PER_REQUEST)
        start_time_ms = end_time_ms - (page_limit * window_ms)

        page = _fetch_chart_page(
            instrument_name=instrument_name,
            resolution=resolution,
            candle_width_ms=window_ms,
            start_time_ms=start_time_ms,
            end_time_ms=end_time_ms,
        )
        if not page:
            break

        pages.append(page)
        remaining -= len(page)

        earliest_tick_ms = _extract_open_time_ms(page[0])
        end_time_ms = earliest_tick_ms - 1

        if len(page) < page_limit:
            break

    rows = [row for page in reversed(pages) for row in page]
    if len(rows) > limit:
        return rows[-limit:]
    return rows


def fetch_klines_all(symbol: str, market: str, interval: str) -> list[list[object]]:
    """Fetch all available Deribit candles by paging backward until exhaustion."""

    instrument_name = normalize_symbol(symbol=symbol, market=market)
    resolution = to_deribit_resolution(interval)
    now_ms = _utc_now_ms()
    window_ms = interval_to_milliseconds(interval)

    end_time_ms = now_ms
    pages: list[list[list[object]]] = []

    while True:
        start_time_ms = end_time_ms - (DERIBIT_MAX_POINTS_PER_REQUEST * window_ms)
        page = _fetch_chart_page(
            instrument_name=instrument_name,
            resolution=resolution,
            candle_width_ms=window_ms,
            start_time_ms=start_time_ms,
            end_time_ms=end_time_ms,
        )
        if not page:
            break

        pages.append(page)
        earliest_tick_ms = _extract_open_time_ms(page[0])
        next_end_time_ms = earliest_tick_ms - 1
        if next_end_time_ms < 0:
            break
        if next_end_time_ms >= end_time_ms:
            break
        end_time_ms = next_end_time_ms

        if len(page) < DERIBIT_MAX_POINTS_PER_REQUEST:
            break

    rows = [row for page in reversed(pages) for row in page]
    dedup: dict[int, list[object]] = {}
    for row in rows:
        dedup[_extract_open_time_ms(row)] = row
    return [dedup[key] for key in sorted(dedup)]


def fetch_klines_range(
    symbol: str,
    market: str,
    interval: str,
    start_open_ms: int,
    end_open_ms: int,
) -> list[list[object]]:
    """Fetch Deribit candles in a forward open-time range."""

    if end_open_ms < start_open_ms:
        return []

    instrument_name = normalize_symbol(symbol=symbol, market=market)
    resolution = to_deribit_resolution(interval)
    interval_ms = interval_to_milliseconds(interval)

    cursor = start_open_ms
    rows: list[list[object]] = []

    while cursor <= end_open_ms:
        window_end_ms = min(
            end_open_ms + interval_ms - 1,
            cursor + (DERIBIT_MAX_POINTS_PER_REQUEST * interval_ms) - 1,
        )

        page = _fetch_chart_page(
            instrument_name=instrument_name,
            resolution=resolution,
            candle_width_ms=interval_ms,
            start_time_ms=cursor,
            end_time_ms=window_end_ms,
        )

        if page:
            filtered = [row for row in page if start_open_ms <= _extract_open_time_ms(row) <= end_open_ms]
            rows.extend(filtered)
            last_open_ms = _extract_open_time_ms(page[-1])
            cursor = max(window_end_ms + 1, last_open_ms + interval_ms)
        else:
            cursor = window_end_ms + 1

    dedup: dict[int, list[object]] = {}
    for row in rows:
        dedup[_extract_open_time_ms(row)] = row
    return [dedup[key] for key in sorted(dedup)]


def _utc_now_ms() -> int:
    """Return current UTC time in milliseconds."""

    return int(datetime.now(UTC).timestamp() * 1000)


def _extract_open_time_ms(row: list[Any]) -> int:
    """Return candle open timestamp in milliseconds."""

    return int(row[0])


def _fetch_chart_page(
    instrument_name: str,
    resolution: str,
    candle_width_ms: int,
    start_time_ms: int,
    end_time_ms: int,
) -> list[list[object]]:
    """Fetch one chart-data page from Deribit and map to common row layout."""

    params: dict[str, Any] = {
        "instrument_name": instrument_name,
        "start_timestamp": start_time_ms,
        "end_timestamp": end_time_ms,
        "resolution": resolution,
    }
    payload = get_json("https://www.deribit.com/api/v2/public/get_tradingview_chart_data", params=params)

    if not isinstance(payload, dict):
        raise ValueError("Unexpected Deribit response format")
    result = payload.get("result")
    if not isinstance(result, dict):
        raise ValueError("Unexpected Deribit response payload")

    ticks = result.get("ticks")
    opens = result.get("open")
    highs = result.get("high")
    lows = result.get("low")
    closes = result.get("close")
    volumes = result.get("volume")
    status = result.get("status")

    if status not in {"ok", None}:
        raise ValueError(f"Deribit chart status is '{status}'")

    if not all(isinstance(values, list) for values in (ticks, opens, highs, lows, closes, volumes)):
        raise ValueError("Unexpected Deribit chart arrays")
    tick_list = cast(list[Any], ticks)
    open_list = cast(list[Any], opens)
    high_list = cast(list[Any], highs)
    low_list = cast(list[Any], lows)
    close_list = cast(list[Any], closes)
    volume_list = cast(list[Any], volumes)

    rows: list[list[object]] = []
    for ts, open_price, high_price, low_price, close_price, volume in zip(
        tick_list, open_list, high_list, low_list, close_list, volume_list, strict=True
    ):
        close_ts = int(ts) + candle_width_ms - 1
        rows.append(
            [
                int(ts),
                str(open_price),
                str(high_price),
                str(low_price),
                str(close_price),
                str(volume),
                close_ts,
                str(volume),
                0,
                "0",
                "0",
                "0",
            ]
        )

    return rows

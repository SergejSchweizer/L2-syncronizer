"""Binance candle download adapter."""

from __future__ import annotations

from typing import Any

from ingestion.http_client import get_json

BINANCE_SUPPORTED_INTERVALS: tuple[str, ...] = (
    "1s",
    "1m",
    "3m",
    "5m",
    "15m",
    "30m",
    "1h",
    "2h",
    "4h",
    "6h",
    "8h",
    "12h",
    "1d",
    "3d",
    "1w",
    "1M",
)
BINANCE_MAX_KLINES_PER_REQUEST = 1000
BINANCE_SPOT_KLINES_URL = "https://api.binance.com/api/v3/klines"
BINANCE_PERP_KLINES_URL = "https://fapi.binance.com/fapi/v1/klines"


def list_supported_intervals() -> tuple[str, ...]:
    """Return Binance-supported kline intervals."""

    return BINANCE_SUPPORTED_INTERVALS


def max_limit() -> int:
    """Return max kline points Binance allows per single request."""

    return BINANCE_MAX_KLINES_PER_REQUEST


def interval_to_milliseconds(interval: str) -> int:
    """Convert normalized Binance interval to milliseconds."""

    if interval.endswith("s"):
        return int(interval[:-1]) * 1_000
    if interval.endswith("m"):
        return int(interval[:-1]) * 60_000
    if interval.endswith("h"):
        return int(interval[:-1]) * 3_600_000
    if interval.endswith("d"):
        return int(interval[:-1]) * 86_400_000
    if interval.endswith("w"):
        return int(interval[:-1]) * 7 * 86_400_000
    if interval.endswith("M"):
        raise ValueError("Monthly interval is not supported for gap-fill mode. Use --limit with 1M.")
    raise ValueError(f"Unsupported interval '{interval}'")


def normalize_timeframe(value: str) -> str:
    """Normalize user-provided timeframe aliases into Binance interval format."""

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

    if candidate in BINANCE_SUPPORTED_INTERVALS:
        return candidate

    raise ValueError(
        f"Unsupported timeframe '{value}' for binance. Supported values: {', '.join(BINANCE_SUPPORTED_INTERVALS)}"
    )


def normalize_symbol(symbol: str, market: str) -> str:
    """Normalize user symbols for Binance spot/perpetual markets."""

    upper = symbol.upper()
    if market == "spot":
        return upper
    if market == "perp":
        if upper in {"BTC", "BTCUSDT"}:
            return "BTCUSDT"
        if upper in {"ETH", "ETHUSDT"}:
            return "ETHUSDT"
        return upper
    raise ValueError("market must be either 'spot' or 'perp'")


def fetch_klines(symbol: str, interval: str, limit: int, market: str = "spot") -> list[list[object]]:
    """Fetch Binance klines with pagination for large limits."""

    if limit <= 0:
        raise ValueError("limit must be positive")

    remaining = limit
    end_time_ms: int | None = None
    pages: list[list[list[object]]] = []

    while remaining > 0:
        page_limit = min(remaining, BINANCE_MAX_KLINES_PER_REQUEST)
        page = _fetch_klines_page(
            symbol=symbol,
            interval=interval,
            limit=page_limit,
            end_time_ms=end_time_ms,
            market=market,
        )
        if not page:
            break

        pages.append(page)
        remaining -= len(page)

        if len(page) < page_limit:
            break

        earliest_open_time_ms = _extract_open_time_ms(page[0])
        end_time_ms = earliest_open_time_ms - 1

    return [row for page in reversed(pages) for row in page]


def fetch_klines_all(symbol: str, interval: str, market: str = "spot") -> list[list[object]]:
    """Fetch all available Binance klines by paging backward until exhaustion."""

    end_time_ms: int | None = None
    pages: list[list[list[object]]] = []

    while True:
        page = _fetch_klines_page(
            symbol=symbol,
            interval=interval,
            limit=BINANCE_MAX_KLINES_PER_REQUEST,
            end_time_ms=end_time_ms,
            market=market,
        )
        if not page:
            break

        pages.append(page)
        earliest_open_time_ms = _extract_open_time_ms(page[0])
        next_end_time_ms = earliest_open_time_ms - 1
        if next_end_time_ms < 0:
            break
        if end_time_ms is not None and next_end_time_ms >= end_time_ms:
            break
        end_time_ms = next_end_time_ms

        if len(page) < BINANCE_MAX_KLINES_PER_REQUEST:
            break

    rows = [row for page in reversed(pages) for row in page]
    dedup: dict[int, list[object]] = {}
    for row in rows:
        dedup[_extract_open_time_ms(row)] = row
    return [dedup[key] for key in sorted(dedup)]


def fetch_klines_range(
    symbol: str,
    interval: str,
    start_open_ms: int,
    end_open_ms: int,
    market: str = "spot",
) -> list[list[object]]:
    """Fetch Binance klines in a forward time range inclusive by open time."""

    if end_open_ms < start_open_ms:
        return []

    interval_ms = interval_to_milliseconds(interval)
    cursor = start_open_ms
    rows: list[list[object]] = []

    while cursor <= end_open_ms:
        page = _fetch_klines_page(
            symbol=symbol,
            interval=interval,
            limit=BINANCE_MAX_KLINES_PER_REQUEST,
            start_time_ms=cursor,
            end_time_ms=end_open_ms + interval_ms - 1,
            market=market,
        )
        if not page:
            break

        filtered = [row for row in page if _extract_open_time_ms(row) <= end_open_ms]
        rows.extend(filtered)

        last_open_ms = _extract_open_time_ms(page[-1])
        if last_open_ms < cursor:
            break
        cursor = last_open_ms + interval_ms

        if len(page) < BINANCE_MAX_KLINES_PER_REQUEST:
            break

    dedup: dict[int, list[object]] = {}
    for row in rows:
        dedup[_extract_open_time_ms(row)] = row
    return [dedup[key] for key in sorted(dedup)]


def _extract_open_time_ms(row: list[Any]) -> int:
    """Return kline open time in milliseconds."""

    return int(row[0])


def _fetch_klines_page(
    symbol: str,
    interval: str,
    limit: int,
    end_time_ms: int | None,
    start_time_ms: int | None = None,
    market: str = "spot",
) -> list[list[object]]:
    """Fetch one page of klines from Binance."""

    params: dict[str, Any] = {"symbol": symbol.upper(), "interval": interval, "limit": limit}
    if start_time_ms is not None:
        params["startTime"] = start_time_ms
    if end_time_ms is not None:
        params["endTime"] = end_time_ms

    if market == "spot":
        endpoint = BINANCE_SPOT_KLINES_URL
    elif market == "perp":
        endpoint = BINANCE_PERP_KLINES_URL
    else:
        raise ValueError("market must be either 'spot' or 'perp'")

    payload = get_json(endpoint, params=params)
    if not isinstance(payload, list):
        raise ValueError("Unexpected Binance response format")
    return payload

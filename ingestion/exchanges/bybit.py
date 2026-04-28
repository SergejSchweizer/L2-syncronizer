"""Bybit candle download adapter."""

from __future__ import annotations

from typing import Any

from ingestion.http_client import get_json

BYBIT_SUPPORTED_INTERVALS: tuple[str, ...] = (
    "1m",
    "3m",
    "5m",
    "15m",
    "30m",
    "1h",
    "2h",
    "4h",
    "6h",
    "12h",
    "1d",
    "1w",
)
BYBIT_MAX_KLINES_PER_REQUEST = 1000
BYBIT_KLINES_URL = "https://api.bybit.com/v5/market/kline"


def list_supported_intervals() -> tuple[str, ...]:
    """Return Bybit-supported kline intervals."""

    return BYBIT_SUPPORTED_INTERVALS


def max_limit() -> int:
    """Return max kline points Bybit allows per single request."""

    return BYBIT_MAX_KLINES_PER_REQUEST


def normalize_timeframe(value: str) -> str:
    """Normalize user-provided timeframe aliases into Bybit interval format."""

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

    if candidate in BYBIT_SUPPORTED_INTERVALS:
        return candidate

    raise ValueError(
        f"Unsupported timeframe '{value}' for bybit. Supported values: {', '.join(BYBIT_SUPPORTED_INTERVALS)}"
    )


def interval_to_milliseconds(interval: str) -> int:
    """Convert normalized Bybit interval to milliseconds."""

    if interval.endswith("m"):
        return int(interval[:-1]) * 60_000
    if interval.endswith("h"):
        return int(interval[:-1]) * 3_600_000
    if interval.endswith("d"):
        return int(interval[:-1]) * 86_400_000
    if interval.endswith("w"):
        return int(interval[:-1]) * 7 * 86_400_000
    raise ValueError(f"Unsupported interval '{interval}'")


def _to_bybit_interval(interval: str) -> str:
    """Map normalized interval to Bybit request interval value."""

    if interval.endswith("m"):
        return interval[:-1]
    if interval.endswith("h"):
        return str(int(interval[:-1]) * 60)
    if interval.endswith("d"):
        return "D"
    if interval.endswith("w"):
        return "W"
    raise ValueError(f"Unsupported interval '{interval}'")


def normalize_symbol(symbol: str, market: str) -> str:
    """Normalize user symbols for Bybit spot/perpetual markets."""

    upper = symbol.upper()
    if market == "spot":
        return upper
    if market == "perp":
        if upper in {"BTC", "BTCUSD", "BTCUSDT"}:
            return "BTCUSDT"
        if upper in {"ETH", "ETHUSD", "ETHUSDT"}:
            return "ETHUSDT"
        return upper
    raise ValueError("market must be either 'spot' or 'perp'")


def fetch_klines(symbol: str, interval: str, limit: int, market: str = "spot") -> list[list[object]]:
    """Fetch Bybit klines with pagination for large limits."""

    if limit <= 0:
        raise ValueError("limit must be positive")

    remaining = limit
    end_time_ms: int | None = None
    pages: list[list[list[object]]] = []

    while remaining > 0:
        page_limit = min(remaining, BYBIT_MAX_KLINES_PER_REQUEST)
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
    """Fetch all available Bybit klines by paging backward until exhaustion."""

    end_time_ms: int | None = None
    pages: list[list[list[object]]] = []

    while True:
        page = _fetch_klines_page(
            symbol=symbol,
            interval=interval,
            limit=BYBIT_MAX_KLINES_PER_REQUEST,
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

        if len(page) < BYBIT_MAX_KLINES_PER_REQUEST:
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
    """Fetch Bybit klines in a forward time range inclusive by open time."""

    if end_open_ms < start_open_ms:
        return []

    interval_ms = interval_to_milliseconds(interval)
    cursor = start_open_ms
    rows: list[list[object]] = []

    while cursor <= end_open_ms:
        page = _fetch_klines_page(
            symbol=symbol,
            interval=interval,
            limit=BYBIT_MAX_KLINES_PER_REQUEST,
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

        if len(page) < BYBIT_MAX_KLINES_PER_REQUEST:
            break

    dedup: dict[int, list[object]] = {}
    for row in rows:
        dedup[_extract_open_time_ms(row)] = row
    return [dedup[key] for key in sorted(dedup)]


def _extract_open_time_ms(row: list[Any]) -> int:
    """Return kline open time in milliseconds."""

    return int(row[0])


def _category_for_market(market: str) -> str:
    """Map local market to Bybit category value."""

    if market == "spot":
        return "spot"
    if market == "perp":
        return "linear"
    raise ValueError("market must be either 'spot' or 'perp'")


def _fetch_klines_page(
    symbol: str,
    interval: str,
    limit: int,
    end_time_ms: int | None,
    start_time_ms: int | None = None,
    market: str = "spot",
) -> list[list[object]]:
    """Fetch one page of klines from Bybit."""

    bybit_interval = _to_bybit_interval(interval)
    params: dict[str, Any] = {
        "category": _category_for_market(market),
        "symbol": symbol.upper(),
        "interval": bybit_interval,
        "limit": limit,
    }
    if start_time_ms is not None:
        params["start"] = start_time_ms
    if end_time_ms is not None:
        params["end"] = end_time_ms

    payload = get_json(BYBIT_KLINES_URL, params=params)
    if not isinstance(payload, dict):
        raise ValueError("Unexpected Bybit response format")
    if payload.get("retCode", 0) != 0:
        raise ValueError(f"Bybit returned retCode={payload.get('retCode')}")
    result = payload.get("result")
    if not isinstance(result, dict):
        raise ValueError("Unexpected Bybit result payload")
    kline_list = result.get("list")
    if not isinstance(kline_list, list):
        return []

    interval_ms = interval_to_milliseconds(interval)
    rows: list[list[object]] = []
    for item in reversed(kline_list):
        if not isinstance(item, list) or len(item) < 7:
            continue
        open_time_ms = int(item[0])
        close_time_ms = open_time_ms + interval_ms - 1
        rows.append(
            [
                open_time_ms,
                str(item[1]),
                str(item[2]),
                str(item[3]),
                str(item[4]),
                str(item[5]),
                close_time_ms,
                str(item[6]),
                0,
                "0",
                "0",
                "0",
            ]
        )

    return rows

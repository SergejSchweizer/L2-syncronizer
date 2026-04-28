"""Bybit perpetual open-interest adapter."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, cast

from ingestion.http_client import get_json

BYBIT_OPEN_INTEREST_URL = "https://api.bybit.com/v5/market/open-interest"
BYBIT_OPEN_INTEREST_MAX_POINTS_PER_REQUEST = 200
BYBIT_OPEN_INTEREST_PERIODS: tuple[str, ...] = ("5m", "15m", "30m", "1h", "4h", "1d")


def normalize_period(value: str) -> str:
    """Normalize user period into Bybit intervalTime format."""

    raw = value.strip()
    if not raw:
        raise ValueError("timeframe cannot be empty")

    lowered = raw.lower()
    if raw[0].isalpha() and raw[1:].isdigit():
        candidate = f"{raw[1:]}{raw[0].lower()}"
    elif raw[:-1].isdigit() and raw[-1].isalpha():
        candidate = f"{raw[:-1]}{raw[-1].lower()}"
    else:
        candidate = lowered

    if candidate in BYBIT_OPEN_INTEREST_PERIODS:
        return candidate
    raise ValueError(
        "Unsupported open-interest timeframe "
        f"'{value}' for bybit. Supported values: {', '.join(BYBIT_OPEN_INTEREST_PERIODS)}"
    )


def period_to_milliseconds(period: str) -> int:
    """Convert normalized period to milliseconds."""

    if period.endswith("m"):
        return int(period[:-1]) * 60_000
    if period.endswith("h"):
        return int(period[:-1]) * 3_600_000
    if period.endswith("d"):
        return int(period[:-1]) * 86_400_000
    raise ValueError(f"Unsupported period '{period}'")


def to_interval_time(period: str) -> str:
    """Convert normalized period to Bybit intervalTime parameter."""

    if period.endswith("m"):
        return f"{period[:-1]}min"
    return period


def fetch_open_interest_all(symbol: str, period: str) -> list[dict[str, object]]:
    """Fetch all available open-interest history by paging backward."""

    end_time_ms: int | None = None
    pages: list[list[dict[str, object]]] = []

    while True:
        page = _fetch_open_interest_page(
            symbol=symbol,
            period=period,
            limit=BYBIT_OPEN_INTEREST_MAX_POINTS_PER_REQUEST,
            end_time_ms=end_time_ms,
        )
        if not page:
            break

        pages.append(page)
        min_ts = min(int(cast(Any, item["timestamp"])) for item in page)
        next_end_time_ms = min_ts - 1
        if next_end_time_ms < 0:
            break
        if end_time_ms is not None and next_end_time_ms >= end_time_ms:
            break
        end_time_ms = next_end_time_ms

        if len(page) < BYBIT_OPEN_INTEREST_MAX_POINTS_PER_REQUEST:
            break

    rows = [row for page in reversed(pages) for row in page]
    dedup: dict[int, dict[str, object]] = {}
    for row in rows:
        dedup[int(cast(Any, row["timestamp"]))] = row
    return [dedup[key] for key in sorted(dedup)]


def fetch_open_interest_range(
    symbol: str,
    period: str,
    start_open_ms: int,
    end_open_ms: int,
) -> list[dict[str, object]]:
    """Fetch open-interest records by inclusive open-time range."""

    if end_open_ms < start_open_ms:
        return []

    period_ms = period_to_milliseconds(period)
    cursor = start_open_ms
    rows: list[dict[str, object]] = []

    while cursor <= end_open_ms:
        window_end_ms = min(
            end_open_ms,
            cursor + (BYBIT_OPEN_INTEREST_MAX_POINTS_PER_REQUEST * period_ms) - 1,
        )
        page = _fetch_open_interest_page(
            symbol=symbol,
            period=period,
            limit=BYBIT_OPEN_INTEREST_MAX_POINTS_PER_REQUEST,
            start_time_ms=cursor,
            end_time_ms=window_end_ms,
        )
        if not page:
            cursor = window_end_ms + 1
            continue

        filtered = [item for item in page if start_open_ms <= int(cast(Any, item["timestamp"])) <= end_open_ms]
        rows.extend(filtered)
        last_ts = max(int(cast(Any, item["timestamp"])) for item in page)
        if last_ts < cursor:
            break
        cursor = last_ts + period_ms

    dedup: dict[int, dict[str, object]] = {}
    for row in rows:
        dedup[int(cast(Any, row["timestamp"]))] = row
    return [dedup[key] for key in sorted(dedup)]


def parse_open_interest_row(symbol: str, period: str, row: dict[str, object]) -> dict[str, object]:
    """Convert Bybit open-interest payload to normalized record fields."""

    open_time_ms = int(cast(Any, row["timestamp"]))
    period_ms = period_to_milliseconds(period)
    open_time = datetime.fromtimestamp(open_time_ms / 1000, tz=UTC)
    close_time = datetime.fromtimestamp((open_time_ms + period_ms - 1) / 1000, tz=UTC)

    return {
        "symbol": symbol,
        "timeframe": period,
        "open_time": open_time,
        "close_time": close_time,
        "open_interest": float(cast(Any, row["openInterest"])),
        "open_interest_value": 0.0,
    }


def _fetch_open_interest_page(
    symbol: str,
    period: str,
    limit: int,
    end_time_ms: int | None,
    start_time_ms: int | None = None,
) -> list[dict[str, object]]:
    """Fetch one open-interest page from Bybit."""

    params: dict[str, Any] = {
        "category": "linear",
        "symbol": symbol.upper(),
        "intervalTime": to_interval_time(period),
        "limit": min(max(limit, 1), BYBIT_OPEN_INTEREST_MAX_POINTS_PER_REQUEST),
    }
    if start_time_ms is not None:
        params["startTime"] = start_time_ms
    if end_time_ms is not None:
        params["endTime"] = end_time_ms

    payload = get_json(BYBIT_OPEN_INTEREST_URL, params=params)
    if not isinstance(payload, dict):
        raise ValueError("Unexpected Bybit open-interest response format")
    if payload.get("retCode", 0) != 0:
        raise ValueError(f"Bybit open-interest returned retCode={payload.get('retCode')}")
    result = payload.get("result")
    if not isinstance(result, dict):
        raise ValueError("Unexpected Bybit open-interest result payload")
    oi_list = result.get("list")
    if not isinstance(oi_list, list):
        return []

    rows: list[dict[str, object]] = []
    for item in oi_list:
        if not isinstance(item, dict):
            continue
        ts = item.get("timestamp")
        oi = item.get("openInterest")
        if ts is None or oi is None:
            continue
        rows.append({"timestamp": ts, "openInterest": oi})

    rows.sort(key=lambda x: int(cast(Any, x["timestamp"])))
    return rows

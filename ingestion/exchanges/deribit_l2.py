"""Deribit L2 orderbook snapshot adapter."""

from __future__ import annotations

from typing import Any, cast

from ingestion.http_client import get_json

DERIBIT_ORDER_BOOK_URL = "https://www.deribit.com/api/v2/public/get_order_book"


def fetch_order_book_snapshot(symbol: str, depth: int = 50) -> dict[str, object]:
    """Fetch one Deribit perpetual orderbook snapshot.

    Args:
        symbol: User symbol or alias (for example ``BTC``).
        depth: Number of levels per side to request from Deribit.

    Returns:
        Normalized dictionary containing timestamp, bids, asks and perp fields.
    """

    if depth <= 0:
        raise ValueError("depth must be positive")

    instrument_name = normalize_l2_symbol(symbol)
    payload = get_json(
        DERIBIT_ORDER_BOOK_URL,
        params={"instrument_name": instrument_name, "depth": depth},
    )
    if not isinstance(payload, dict):
        raise ValueError("Unexpected Deribit L2 response format")
    result = payload.get("result")
    if not isinstance(result, dict):
        raise ValueError("Unexpected Deribit L2 payload")

    timestamp = result.get("timestamp")
    bids = result.get("bids")
    asks = result.get("asks")

    if not isinstance(timestamp, (int, float)):
        raise ValueError("Deribit L2 payload missing numeric timestamp")
    if not isinstance(bids, list) or not isinstance(asks, list):
        raise ValueError("Deribit L2 payload missing bids/asks arrays")

    normalized_bids = [_normalize_level(level) for level in bids]
    normalized_asks = [_normalize_level(level) for level in asks]

    return {
        "exchange": "deribit",
        "symbol": str(result.get("instrument_name", instrument_name)),
        "timestamp_ms": int(timestamp),
        "bids": normalized_bids,
        "asks": normalized_asks,
        "mark_price": _to_optional_float(result.get("mark_price")),
        "index_price": _to_optional_float(result.get("index_price")),
        "open_interest": _to_optional_float(result.get("open_interest")),
        "funding_8h": _to_optional_float(result.get("funding_8h")),
        "current_funding": _to_optional_float(result.get("current_funding")),
    }


def _normalize_level(level: object) -> tuple[float, float]:
    """Normalize one Deribit level entry into ``(price, amount)``."""

    if not isinstance(level, list) or len(level) < 2:
        raise ValueError("Deribit L2 level format must be [price, amount]")
    price = float(cast(Any, level[0]))
    amount = float(cast(Any, level[1]))
    return (price, amount)


def normalize_l2_symbol(symbol: str) -> str:
    """Normalize a Deribit perpetual symbol alias to an instrument name."""

    value = symbol.strip().upper().replace("/", "")
    if not value:
        raise ValueError("symbol must not be empty")
    if value in {"SOL", "SOLUSDC", "SOL-USDC", "SOL_USDC"}:
        return "SOL_USDC-PERPETUAL"
    if value == "SOLUSDC-PERPETUAL" or value == "SOL-USDC-PERPETUAL":
        return "SOL_USDC-PERPETUAL"
    if value == "SOL_USDC-PERPETUAL":
        return value
    if value.endswith("-PERPETUAL"):
        return value
    if value.endswith("PERPETUAL") and "-" not in value:
        return f"{value.removesuffix('PERPETUAL')}-PERPETUAL"
    if "-" in value:
        return value
    if value.endswith("USDT"):
        return f"{value.removesuffix('USDT')}-PERPETUAL"
    if value.endswith("USD"):
        return f"{value.removesuffix('USD')}-PERPETUAL"
    return f"{value}-PERPETUAL"


def _to_optional_float(value: object) -> float | None:
    """Convert optional numeric payload field to float."""

    if value is None:
        return None
    return float(cast(Any, value))

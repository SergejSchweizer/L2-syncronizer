"""Deribit perpetual open-interest adapter (current snapshot)."""

from __future__ import annotations

from datetime import UTC, datetime

from ingestion.http_client import get_json

DERIBIT_BOOK_SUMMARY_URL = "https://www.deribit.com/api/v2/public/get_book_summary_by_instrument"


def fetch_current_open_interest(instrument_name: str) -> dict[str, object] | None:
    """Fetch current open interest snapshot for one Deribit instrument."""

    payload = get_json(DERIBIT_BOOK_SUMMARY_URL, params={"instrument_name": instrument_name})
    if not isinstance(payload, dict):
        raise ValueError("Unexpected Deribit open-interest response format")

    result = payload.get("result")
    if not isinstance(result, list) or not result:
        return None

    item = result[0]
    if not isinstance(item, dict):
        return None

    open_interest = item.get("open_interest")
    if open_interest is None:
        return None

    ts = item.get("creation_timestamp")
    if ts is None:
        ts = int(datetime.now(UTC).timestamp() * 1000)

    return {
        "timestamp": int(ts),
        "open_interest": float(open_interest),
    }

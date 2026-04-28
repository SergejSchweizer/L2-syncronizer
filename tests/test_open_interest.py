"""Tests for open-interest ingestion interface."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from ingestion import open_interest as oi
from ingestion.exchanges import binance_open_interest, deribit_open_interest


def test_normalize_open_interest_timeframe_binance_bybit_deribit() -> None:
    assert oi.normalize_open_interest_timeframe("binance", "5m") == "5m"
    assert oi.normalize_open_interest_timeframe("bybit", "M5") == "5m"
    assert oi.normalize_open_interest_timeframe("deribit", "M1") == "1m"


def test_fetch_open_interest_all_history_returns_empty_for_spot() -> None:
    rows = oi.fetch_open_interest_all_history(
        exchange="binance",
        symbol="BTC",
        interval="5m",
        market="spot",
    )
    assert rows == []


def test_fetch_open_interest_all_history_binance(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        binance_open_interest,
        "fetch_open_interest_all",
        lambda **kwargs: [{"timestamp": 1000, "sumOpenInterest": "12", "sumOpenInterestValue": "34"}],
    )

    def fake_parse(symbol: str, period: str, row: dict[str, object]) -> dict[str, object]:
        del symbol, period, row
        return {
            "open_time": datetime(1970, 1, 1, 0, 0, 1, tzinfo=UTC),
            "close_time": datetime(1970, 1, 1, 0, 4, 59, 999000, tzinfo=UTC),
            "open_interest": 12.0,
            "open_interest_value": 34.0,
        }

    monkeypatch.setattr(binance_open_interest, "parse_open_interest_row", fake_parse)

    rows = oi.fetch_open_interest_all_history(
        exchange="binance",
        symbol="BTC",
        interval="5m",
        market="perp",
    )
    assert len(rows) == 1
    assert rows[0].exchange == "binance"
    assert rows[0].open_interest == 12.0


def test_fetch_open_interest_range_deribit_snapshot(monkeypatch: pytest.MonkeyPatch) -> None:
    snapshot_ms = int(datetime(2026, 4, 28, 9, 2, tzinfo=UTC).timestamp() * 1000)
    monkeypatch.setattr(
        deribit_open_interest,
        "fetch_current_open_interest",
        lambda **kwargs: {"timestamp": snapshot_ms, "open_interest": 1000.0},
    )

    start = int(datetime(2026, 4, 28, 9, 0, tzinfo=UTC).timestamp() * 1000)
    end = int(datetime(2026, 4, 28, 9, 5, tzinfo=UTC).timestamp() * 1000)

    rows = oi.fetch_open_interest_range(
        exchange="deribit",
        symbol="BTC",
        interval="1m",
        start_open_ms=start,
        end_open_ms=end,
        market="perp",
    )
    assert len(rows) == 1
    assert rows[0].exchange == "deribit"
    assert rows[0].open_interest == 1000.0

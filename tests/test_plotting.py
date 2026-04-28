"""Tests for plot helper utilities."""

from __future__ import annotations

from datetime import UTC, datetime

from ingestion.plotting import build_plot_filename, price_value
from ingestion.spot import SpotCandle


def _sample_candle() -> SpotCandle:
    return SpotCandle(
        exchange="binance",
        symbol="BTCUSDT",
        interval="1m",
        open_time=datetime(2026, 1, 1, tzinfo=UTC),
        close_time=datetime(2026, 1, 1, 0, 0, 59, 999000, tzinfo=UTC),
        open_price=100.0,
        high_price=110.0,
        low_price=95.0,
        close_price=105.0,
        volume=10.0,
        quote_volume=1000.0,
        trade_count=20,
    )


def test_price_value_selector() -> None:
    candle = _sample_candle()
    assert price_value(candle, "spot") == 105.0
    assert price_value(candle, "close") == 105.0
    assert price_value(candle, "open") == 100.0
    assert price_value(candle, "high") == 110.0
    assert price_value(candle, "low") == 95.0


def test_build_plot_filename_sanitizes_inputs() -> None:
    file_name = build_plot_filename(
        exchange="deribit",
        symbol="BTC/PERPETUAL",
        interval="1m",
        price_field="close",
    )
    assert file_name.endswith(".png")
    assert "20260101" not in file_name
    assert "BTC_PERPETUAL" in file_name

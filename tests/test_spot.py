"""Tests for Deribit-only spot/perpetual ingestion parsing and validation."""

from __future__ import annotations

from datetime import UTC
from typing import Literal

import pytest

from ingestion.exchanges import deribit
from ingestion.spot import (
    fetch_candles,
    fetch_candles_all_history,
    normalize_timeframe,
    parse_kline,
)


def test_parse_deribit_kline_maps_fields() -> None:
    row = [1714478400000, "64000.0", "64200.0", "63850.0", "64100.0", "120.5", 1714481999999, "7720000.0", 2300]

    candle = parse_kline("deribit", "BTC-PERPETUAL", "1h", row)

    assert candle.symbol == "BTC-PERPETUAL"
    assert candle.interval == "1h"
    assert candle.open_time.tzinfo == UTC
    assert candle.close_price == pytest.approx(64100.0)
    assert candle.volume == pytest.approx(120.5)
    assert candle.trade_count == 2300


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("M1", "1m"),
        ("M5", "5m"),
        ("H1", "1h"),
        ("H6", "6h"),
        ("D1", "1d"),
    ],
)
def test_normalize_deribit_timeframe_aliases(value: str, expected: str) -> None:
    assert normalize_timeframe("deribit", value) == expected


def test_deribit_symbol_normalization_perp_aliases() -> None:
    assert deribit.normalize_symbol("BTC", "perp") == "BTC-PERPETUAL"
    assert deribit.normalize_symbol("ETHUSDT", "perp") == "ETH-PERPETUAL"


def test_fetch_deribit_candles_respects_limit(monkeypatch: pytest.MonkeyPatch) -> None:
    from ingestion.exchanges import deribit as deribit_exchange

    def fake_utc_now_ms() -> int:
        return 10_000

    def fake_get_json(url: str, params: dict[str, object] | None = None, timeout_s: float = 15.0) -> object:
        del url, timeout_s, params
        return {
            "result": {
                "status": "ok",
                "ticks": [1000, 2000, 3000, 4000],
                "open": [1, 2, 3, 4],
                "high": [1, 2, 3, 4],
                "low": [1, 2, 3, 4],
                "close": [1, 2, 3, 4],
                "volume": [1, 2, 3, 4],
            }
        }

    monkeypatch.setattr(deribit_exchange, "_utc_now_ms", fake_utc_now_ms)
    monkeypatch.setattr(deribit_exchange, "get_json", fake_get_json)

    candles = fetch_candles(exchange="deribit", market="perp", symbol="BTC", interval="1m", limit=3)
    assert len(candles) == 3
    assert [int(item.open_time.timestamp()) for item in candles] == [2, 3, 4]


@pytest.mark.parametrize(
    ("market", "symbol", "expected_instrument", "expected_symbol"),
    [
        ("spot", "BTCUSDT", "BTC_USDC", "BTC_USDC"),
        ("perp", "BTC", "BTC-PERPETUAL", "BTC-PERPETUAL"),
    ],
)
def test_fetch_deribit_routes_spot_and_perp_symbols(
    monkeypatch: pytest.MonkeyPatch,
    market: Literal["spot", "perp"],
    symbol: str,
    expected_instrument: str,
    expected_symbol: str,
) -> None:
    from ingestion.exchanges import deribit as deribit_exchange

    captured_instruments: list[str] = []

    def fake_get_json(url: str, params: dict[str, object] | None = None, timeout_s: float = 15.0) -> object:
        del url, timeout_s
        assert params is not None
        captured_instruments.append(str(params["instrument_name"]))
        return {
            "result": {
                "status": "ok",
                "ticks": [1000, 2000],
                "open": [1, 2],
                "high": [1, 2],
                "low": [1, 2],
                "close": [1, 2],
                "volume": [1, 2],
            }
        }

    monkeypatch.setattr(deribit_exchange, "_utc_now_ms", lambda: 10_000)
    monkeypatch.setattr(deribit_exchange, "get_json", fake_get_json)

    candles = fetch_candles(exchange="deribit", market=market, symbol=symbol, interval="1m", limit=2)
    assert len(candles) == 2
    assert captured_instruments[0] == expected_instrument
    assert candles[0].symbol == expected_symbol


def test_fetch_all_history_deribit(monkeypatch: pytest.MonkeyPatch) -> None:
    from ingestion.exchanges import deribit as deribit_exchange

    def fake_fetch_klines_all_deribit(symbol: str, market: str, interval: str) -> list[list[object]]:
        assert symbol == "BTC"
        assert market == "perp"
        assert interval == "1m"
        return [[1000, "1", "1", "1", "1", "1", 1999, "1", 1]]

    monkeypatch.setattr(deribit_exchange, "fetch_klines_all", fake_fetch_klines_all_deribit)

    candles = fetch_candles_all_history(exchange="deribit", market="perp", symbol="BTC", interval="1m")

    assert len(candles) == 1
    assert candles[0].symbol == "BTC-PERPETUAL"

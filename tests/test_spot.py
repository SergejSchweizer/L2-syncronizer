"""Tests for spot ingestion parsing and validation."""

from __future__ import annotations

from datetime import timezone

import pytest

from ingestion.exchanges import deribit
from ingestion.spot import (
    fetch_binance_spot_candles,
    fetch_candles,
    fetch_candles_all_history,
    normalize_timeframe,
    parse_kline,
)



def test_parse_binance_kline_maps_fields() -> None:
    row = [
        1714478400000,
        "64000.0",
        "64200.0",
        "63850.0",
        "64100.0",
        "120.5",
        1714481999999,
        "7720000.0",
        2300,
        "60.0",
        "3850000.0",
        "0",
    ]

    candle = parse_kline("binance", "BTCUSDT", "1h", row)

    assert candle.symbol == "BTCUSDT"
    assert candle.interval == "1h"
    assert candle.open_time.tzinfo == timezone.utc
    assert candle.close_price == pytest.approx(64100.0)
    assert candle.volume == pytest.approx(120.5)
    assert candle.trade_count == 2300



def test_fetch_binance_spot_candles_rejects_invalid_limit() -> None:
    with pytest.raises(ValueError):
        fetch_binance_spot_candles("BTCUSDT", limit=0)


def test_fetch_binance_spot_candles_paginates_and_orders(monkeypatch: pytest.MonkeyPatch) -> None:
    from ingestion.exchanges import binance

    calls: list[dict[str, object]] = []
    monkeypatch.setattr(binance, "BINANCE_MAX_KLINES_PER_REQUEST", 2)

    newer_page = [
        [3000, "1", "1", "1", "1", "1", 3599, "1", 1, "0", "0", "0"],
        [4000, "1", "1", "1", "1", "1", 4599, "1", 1, "0", "0", "0"],
    ]
    older_page = [
        [1000, "1", "1", "1", "1", "1", 1599, "1", 1, "0", "0", "0"],
        [2000, "1", "1", "1", "1", "1", 2599, "1", 1, "0", "0", "0"],
    ]

    def fake_get_json(url: str, params: dict[str, object] | None = None, timeout_s: float = 15.0) -> object:
        del url, timeout_s
        assert params is not None
        calls.append(params)
        if len(calls) == 1:
            assert params["limit"] == 2
            assert "endTime" not in params
            return newer_page
        assert params["limit"] == 2
        assert params["endTime"] == 2999
        return older_page

    monkeypatch.setattr(binance, "get_json", fake_get_json)
    candles = fetch_binance_spot_candles("BTCUSDT", interval="1m", limit=4)

    assert len(candles) == 4
    assert [item.open_time.timestamp() for item in candles] == [1.0, 2.0, 3.0, 4.0]


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("M1", "1m"),
        ("m5", "5m"),
        ("H1", "1h"),
        ("D1", "1d"),
        ("W1", "1w"),
        ("MN1", "1M"),
        ("1M", "1M"),
    ],
)
def test_normalize_timeframe_aliases(value: str, expected: str) -> None:
    assert normalize_timeframe("binance", value) == expected


def test_normalize_timeframe_rejects_unknown_value() -> None:
    with pytest.raises(ValueError):
        normalize_timeframe("binance", "M2")


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


def test_fetch_binance_perp_routes_to_futures_endpoint(monkeypatch: pytest.MonkeyPatch) -> None:
    from ingestion.exchanges import binance as binance_exchange

    calls: list[str] = []

    def fake_get_json(url: str, params: dict[str, object] | None = None, timeout_s: float = 15.0) -> object:
        del timeout_s
        calls.append(url)
        assert params is not None
        assert params["symbol"] == "BTCUSDT"
        return [
            [1000, "1", "2", "0.5", "1.5", "10", 1999, "15", 2, "0", "0", "0"],
            [2000, "1.5", "2.2", "1.0", "2.0", "12", 2999, "20", 3, "0", "0", "0"],
        ]

    monkeypatch.setattr(binance_exchange, "get_json", fake_get_json)
    candles = fetch_candles(exchange="binance", market="perp", symbol="BTC", interval="1m", limit=2)

    assert len(candles) == 2
    assert calls[0] == "https://fapi.binance.com/fapi/v1/klines"
    assert candles[0].symbol == "BTCUSDT"


def test_fetch_all_history_routes_to_exchange_all_fetch(monkeypatch: pytest.MonkeyPatch) -> None:
    from ingestion.exchanges import deribit as deribit_exchange

    def fake_get_json(url: str, params: dict[str, object] | None = None, timeout_s: float = 15.0) -> object:
        del url, timeout_s, params
        return {
            "result": {
                "status": "ok",
                "ticks": [1000, 2000, 3000],
                "open": [1, 2, 3],
                "high": [1, 2, 3],
                "low": [1, 2, 3],
                "close": [1, 2, 3],
                "volume": [1, 2, 3],
            }
        }

    monkeypatch.setattr(deribit_exchange, "DERIBIT_MAX_POINTS_PER_REQUEST", 5000)
    monkeypatch.setattr(deribit_exchange, "_utc_now_ms", lambda: 10_000)
    monkeypatch.setattr(deribit_exchange, "get_json", fake_get_json)

    candles = fetch_candles_all_history(exchange="deribit", market="spot", symbol="BTCUSDT", interval="1m")
    assert len(candles) == 3
    assert [int(item.open_time.timestamp()) for item in candles] == [1, 2, 3]

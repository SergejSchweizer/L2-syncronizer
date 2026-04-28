"""Tests for spot ingestion parsing and validation."""

from __future__ import annotations

from datetime import UTC

import pytest

from ingestion.exchanges import deribit
from ingestion.spot import (
    Exchange,
    Market,
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
    assert candle.open_time.tzinfo == UTC
    assert candle.close_price == pytest.approx(64100.0)
    assert candle.volume == pytest.approx(120.5)
    assert candle.trade_count == 2300


def test_fetch_binance_spot_rejects_invalid_limit() -> None:
    with pytest.raises(ValueError):
        fetch_candles(exchange="binance", symbol="BTCUSDT", market="spot", limit=0)


def test_fetch_binance_spot_paginates_and_orders(monkeypatch: pytest.MonkeyPatch) -> None:
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
    candles = fetch_candles(exchange="binance", symbol="BTCUSDT", market="spot", interval="1m", limit=4)

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


def test_fetch_binance_spot_routes_to_spot_endpoint(monkeypatch: pytest.MonkeyPatch) -> None:
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
    candles = fetch_candles(exchange="binance", market="spot", symbol="BTCUSDT", interval="1m", limit=2)

    assert len(candles) == 2
    assert calls[0] == "https://api.binance.com/api/v3/klines"
    assert candles[0].symbol == "BTCUSDT"


@pytest.mark.parametrize(
    ("market", "symbol", "expected_instrument", "expected_symbol"),
    [
        ("spot", "BTCUSDT", "BTC_USDC", "BTC_USDC"),
        ("perp", "BTC", "BTC-PERPETUAL", "BTC-PERPETUAL"),
    ],
)
def test_fetch_deribit_routes_spot_and_perp_symbols(
    monkeypatch: pytest.MonkeyPatch,
    market: Market,
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


@pytest.mark.parametrize(
    ("exchange", "market", "input_symbol", "expected_fetch_symbol", "expected_candle_symbol"),
    [
        ("binance", "spot", "BTCUSDT", "BTCUSDT", "BTCUSDT"),
        ("binance", "perp", "BTC", "BTCUSDT", "BTCUSDT"),
        ("deribit", "spot", "BTCUSDT", "BTCUSDT", "BTC_USDC"),
        ("deribit", "perp", "BTC", "BTC", "BTC-PERPETUAL"),
    ],
)
def test_fetch_all_history_supports_spot_and_perp_on_both_exchanges(
    monkeypatch: pytest.MonkeyPatch,
    exchange: Exchange,
    market: Market,
    input_symbol: str,
    expected_fetch_symbol: str,
    expected_candle_symbol: str,
) -> None:
    row = [1000, "1", "1", "1", "1", "1", 1999, "1", 1, "0", "0", "0"]

    if exchange == "binance":
        from ingestion.exchanges import binance as binance_exchange

        captured: list[dict[str, object]] = []

        def fake_fetch_klines_all_binance(symbol: str, interval: str, market: str = "spot") -> list[list[object]]:
            captured.append({"symbol": symbol, "interval": interval, "market": market})
            return [row]

        monkeypatch.setattr(binance_exchange, "fetch_klines_all", fake_fetch_klines_all_binance)
    else:
        from ingestion.exchanges import deribit as deribit_exchange

        captured = []

        def fake_fetch_klines_all_deribit(symbol: str, market: str, interval: str) -> list[list[object]]:
            captured.append({"symbol": symbol, "interval": interval, "market": market})
            return [row]

        monkeypatch.setattr(deribit_exchange, "fetch_klines_all", fake_fetch_klines_all_deribit)

    candles = fetch_candles_all_history(
        exchange=exchange,
        market=market,
        symbol=input_symbol,
        interval="1m",
    )

    assert len(candles) == 1
    assert captured[0]["symbol"] == expected_fetch_symbol
    assert captured[0]["market"] == market
    assert candles[0].symbol == expected_candle_symbol


def test_bybit_symbol_normalization_perp_aliases() -> None:
    from ingestion.exchanges import bybit as bybit_exchange

    assert bybit_exchange.normalize_symbol("BTC", "perp") == "BTCUSDT"
    assert bybit_exchange.normalize_symbol("ETHUSDT", "perp") == "ETHUSDT"


def test_fetch_bybit_spot_routes_to_bybit_market_kline(monkeypatch: pytest.MonkeyPatch) -> None:
    from ingestion.exchanges import bybit as bybit_exchange

    captured: list[dict[str, object]] = []

    def fake_get_json(url: str, params: dict[str, object] | None = None, timeout_s: float = 15.0) -> object:
        del timeout_s
        assert params is not None
        captured.append({"url": url, **params})
        return {
            "retCode": 0,
            "result": {
                "list": [
                    ["2000", "1.5", "2.2", "1.0", "2.0", "12", "20"],
                    ["1000", "1", "2", "0.5", "1.5", "10", "15"],
                ]
            },
        }

    monkeypatch.setattr(bybit_exchange, "get_json", fake_get_json)
    candles = fetch_candles(exchange="bybit", market="spot", symbol="BTCUSDT", interval="1m", limit=2)

    assert len(candles) == 2
    assert captured[0]["url"] == "https://api.bybit.com/v5/market/kline"
    assert captured[0]["category"] == "spot"
    assert candles[0].symbol == "BTCUSDT"


def test_fetch_bybit_perp_routes_with_linear_category(monkeypatch: pytest.MonkeyPatch) -> None:
    from ingestion.exchanges import bybit as bybit_exchange

    captured: list[dict[str, object]] = []

    def fake_get_json(url: str, params: dict[str, object] | None = None, timeout_s: float = 15.0) -> object:
        del timeout_s
        assert params is not None
        captured.append({"url": url, **params})
        return {
            "retCode": 0,
            "result": {
                "list": [
                    ["1000", "1", "2", "0.5", "1.5", "10", "15"],
                ]
            },
        }

    monkeypatch.setattr(bybit_exchange, "get_json", fake_get_json)
    candles = fetch_candles(exchange="bybit", market="perp", symbol="BTC", interval="1m", limit=1)

    assert len(candles) == 1
    assert captured[0]["category"] == "linear"
    assert captured[0]["symbol"] == "BTCUSDT"
    assert candles[0].symbol == "BTCUSDT"

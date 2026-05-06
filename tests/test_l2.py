"""Tests for L2 snapshot ingestion."""

from __future__ import annotations

from threading import Event, Lock

import pytest

from ingestion import l2
from ingestion.l2 import fetch_l2_snapshots_for_symbols


def test_fetch_l2_snapshots_for_symbols_polls_all_symbols_each_tick(monkeypatch: pytest.MonkeyPatch) -> None:
    events: list[tuple[str, str]] = []
    lock = Lock()
    both_started = Event()

    def fake_fetch_order_book_snapshot(symbol: str, depth: int) -> dict[str, object]:
        del depth
        with lock:
            events.append(("start", symbol))
            if len([event for event in events if event[0] == "start"]) == 2:
                both_started.set()
        both_started.wait(timeout=0.5)
        with lock:
            events.append(("finish", symbol))
            event_count = len(events)
        timestamp_ms = 1_700_000_000_000 + event_count
        return {
            "exchange": "deribit",
            "symbol": f"{symbol}-PERPETUAL",
            "timestamp_ms": timestamp_ms,
            "bids": [(100.0, 1.0)],
            "asks": [(101.0, 1.0)],
            "mark_price": 100.5,
            "index_price": 100.0,
            "open_interest": 1000.0,
            "funding_8h": 0.0001,
            "current_funding": 0.00001,
        }

    monkeypatch.setattr(l2, "fetch_order_book_snapshot", fake_fetch_order_book_snapshot)

    snapshots = fetch_l2_snapshots_for_symbols(
        exchange="deribit",
        symbols=["BTC", "ETH"],
        depth=50,
        snapshot_count=1,
        poll_interval_s=0,
        concurrency=2,
    )

    assert events[0][0] == "start"
    assert events[1][0] == "start"
    assert len(snapshots["BTC"]) == 1
    assert len(snapshots["ETH"]) == 1


def test_fetch_l2_snapshots_for_symbols_logs_and_skips_failed_symbol(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Verify one failed symbol does not discard the whole polling tick."""

    def fake_fetch_order_book_snapshot(symbol: str, depth: int) -> dict[str, object]:
        del depth
        if symbol == "ETH":
            raise RuntimeError("exchange unavailable")
        return {
            "exchange": "deribit",
            "symbol": f"{symbol}-PERPETUAL",
            "timestamp_ms": 1_700_000_000_000,
            "bids": [(100.0, 1.0)],
            "asks": [(101.0, 1.0)],
            "mark_price": 100.5,
            "index_price": 100.0,
            "open_interest": 1000.0,
            "funding_8h": 0.0001,
            "current_funding": 0.00001,
        }

    monkeypatch.setattr(l2, "fetch_order_book_snapshot", fake_fetch_order_book_snapshot)

    with caplog.at_level("WARNING", logger="ingestion.l2"):
        snapshots = fetch_l2_snapshots_for_symbols(
            exchange="deribit",
            symbols=["BTC", "ETH"],
            depth=50,
            snapshot_count=1,
            poll_interval_s=0,
            concurrency=2,
        )

    assert len(snapshots["BTC"]) == 1
    assert snapshots["ETH"] == []
    assert "L2 snapshot fetch failed symbol=ETH" in caplog.text


def test_fetch_l2_snapshots_for_symbols_respects_expired_runtime_budget(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify an expired runtime budget stops polling before network calls."""

    calls: list[str] = []

    def fake_fetch_order_book_snapshot(symbol: str, depth: int) -> dict[str, object]:
        del depth
        calls.append(symbol)
        return {
            "exchange": "deribit",
            "symbol": f"{symbol}-PERPETUAL",
            "timestamp_ms": 1_700_000_000_000,
            "bids": [(100.0, 1.0)],
            "asks": [(101.0, 1.0)],
            "mark_price": 100.5,
            "index_price": 100.0,
            "open_interest": 1000.0,
            "funding_8h": 0.0001,
            "current_funding": 0.00001,
        }

    monkeypatch.setattr(l2, "fetch_order_book_snapshot", fake_fetch_order_book_snapshot)
    monkeypatch.setattr(l2, "_deadline_from_config", lambda config: 0.0)

    snapshots = fetch_l2_snapshots_for_symbols(
        exchange="deribit",
        symbols=["BTC"],
        depth=50,
        snapshot_count=1,
        poll_interval_s=0,
        max_runtime_s=1,
    )

    assert calls == []
    assert snapshots["BTC"] == []

"""Tests for single-instance CLI locking."""

from __future__ import annotations

import fcntl
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import cast

import pandas as pd
import pytest

from api import cli
from api.cli import SingleInstanceError, SingleInstanceLock
from ingestion.spot import SpotCandle


def test_single_instance_lock_creates_lock_file(tmp_path: Path) -> None:
    lock_file = tmp_path / "test.lock"

    with SingleInstanceLock(str(lock_file)):
        assert lock_file.exists()
        content = lock_file.read_text().strip()
        assert content.isdigit()


def test_single_instance_lock_raises_on_contention(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    lock_file = tmp_path / "test.lock"

    def fake_flock(fd: int, operation: int) -> None:
        del fd, operation
        raise BlockingIOError("locked")

    monkeypatch.setattr(fcntl, "flock", fake_flock)

    with pytest.raises(SingleInstanceError):
        with SingleInstanceLock(str(lock_file)):
            pass


def test_auto_mode_fetches_all_history_when_no_lake_data(monkeypatch: pytest.MonkeyPatch) -> None:
    sample = SpotCandle(
        exchange="binance",
        symbol="BTCUSDT",
        interval="1m",
        open_time=datetime(2026, 4, 27, 10, 0, tzinfo=UTC),
        close_time=datetime(2026, 4, 27, 10, 0, 59, 999000, tzinfo=UTC),
        open_price=100.0,
        high_price=101.0,
        low_price=99.0,
        close_price=100.5,
        volume=10.0,
        quote_volume=1000.0,
        trade_count=10,
    )

    monkeypatch.setattr(cli, "open_times_in_lake", lambda **kwargs: [])
    calls: list[dict[str, object]] = []

    def fake_fetch_candles_all_history(**kwargs: object) -> list[SpotCandle]:
        calls.append(kwargs)
        return [sample]

    monkeypatch.setattr(cli, "fetch_candles_all_history", fake_fetch_candles_all_history)

    candles = cli._fetch_symbol_candles(
        exchange="binance",
        market="spot",
        symbol="BTCUSDT",
        timeframe="1m",
        lake_root="lake/bronze",
    )

    assert len(candles) == 1
    assert len(calls) == 1
    assert calls[0]["exchange"] == "binance"
    assert calls[0]["symbol"] == "BTCUSDT"


def test_gap_fill_fetches_internal_and_tail_gaps(monkeypatch: pytest.MonkeyPatch) -> None:
    interval_ms = 60_000
    open_times = [
        datetime(2026, 4, 27, 10, 0, tzinfo=UTC),
        datetime(2026, 4, 27, 10, 2, tzinfo=UTC),
        datetime(2026, 4, 27, 10, 3, tzinfo=UTC),
    ]
    end_open_ms = int(datetime(2026, 4, 27, 10, 5, tzinfo=UTC).timestamp() * 1000)
    calls: list[tuple[int, int]] = []

    monkeypatch.setattr(cli, "open_times_in_lake", lambda **kwargs: open_times)
    monkeypatch.setattr(cli, "normalize_storage_symbol", lambda **kwargs: "BTCUSDT")
    monkeypatch.setattr(cli, "interval_to_milliseconds", lambda **kwargs: interval_ms)
    monkeypatch.setattr(cli, "_last_closed_open_ms", lambda **kwargs: end_open_ms)

    def fake_fetch_candles_range(**kwargs: object) -> list[SpotCandle]:
        start_open_ms = cast(int, kwargs["start_open_ms"])
        end_open_ms = cast(int, kwargs["end_open_ms"])
        calls.append((start_open_ms, end_open_ms))
        return []

    monkeypatch.setattr(cli, "fetch_candles_range", fake_fetch_candles_range)

    candles = cli._fetch_symbol_candles(
        exchange="binance",
        market="spot",
        symbol="BTCUSDT",
        timeframe="1m",
        lake_root="lake/bronze",
    )

    gap_one_ms = int(datetime(2026, 4, 27, 10, 1, tzinfo=UTC).timestamp() * 1000)
    gap_tail_start_ms = int(datetime(2026, 4, 27, 10, 4, tzinfo=UTC).timestamp() * 1000)
    assert candles == []
    assert calls == [(gap_one_ms, gap_one_ms), (gap_tail_start_ms, end_open_ms)]


def test_main_loader_command_still_uses_single_instance_lock(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class Locked:
        def __init__(self, lock_path: str) -> None:
            del lock_path

        def __enter__(self) -> None:
            raise SingleInstanceError("loader already running")

        def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
            del exc_type, exc, tb

    monkeypatch.setattr(cli, "SingleInstanceLock", Locked)
    monkeypatch.setattr(
        "sys.argv",
        [
            "main.py",
            "loader",
            "--exchange",
            "binance",
            "--market",
            "spot",
            "--symbols",
            "BTCUSDT",
            "--timeframe",
            "1m",
            "--no-json-output",
        ],
    )

    with pytest.raises(SystemExit, match="loader already running"):
        cli.main()


def test_main_export_df_does_not_acquire_single_instance_lock(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    class FailIfEnteredLock:
        def __init__(self, lock_path: str) -> None:
            del lock_path

        def __enter__(self) -> None:
            raise AssertionError("lock should not be used for export-df command")

        def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
            del exc_type, exc, tb

    fake_df = pd.DataFrame(
        [
            {
                "exchange": "binance",
                "instrument_type": "spot",
                "symbol": "BTCUSDT",
                "timeframe": "1m",
                "open_time": datetime(2026, 4, 27, 10, 0, tzinfo=UTC),
                "close_time": datetime(2026, 4, 27, 10, 0, 59, tzinfo=UTC),
                "open": 100.0,
                "high": 101.0,
                "low": 99.0,
                "close": 100.5,
                "volume": 12.0,
                "quote_volume": 1200.0,
                "trade_count": 10,
            },
            {
                "exchange": "binance",
                "instrument_type": "perp",
                "symbol": "BTCUSDT",
                "timeframe": "1m",
                "open_time": datetime(2026, 4, 27, 10, 1, tzinfo=UTC),
                "close_time": datetime(2026, 4, 27, 10, 1, 59, tzinfo=UTC),
                "open": 100.5,
                "high": 101.5,
                "low": 100.0,
                "close": 101.0,
                "volume": 11.0,
                "quote_volume": 1110.0,
                "trade_count": 9,
            },
        ]
    )
    captured: dict[str, object] = {}

    def fake_loader(**kwargs: object) -> pd.DataFrame:
        captured.update(kwargs)
        return fake_df

    def fake_save_candle_plots(**kwargs: object) -> list[str]:
        del kwargs
        temp_plot = output_dir / "temporary_plot.png"
        temp_plot.parent.mkdir(parents=True, exist_ok=True)
        temp_plot.write_bytes(b"PNG")
        return [str(temp_plot.resolve())]

    output_dir = tmp_path / "exports"
    monkeypatch.setattr(cli, "SingleInstanceLock", FailIfEnteredLock)
    monkeypatch.setattr(cli, "load_combined_dataframe_from_lake", fake_loader)
    monkeypatch.setattr(cli, "save_candle_plots", fake_save_candle_plots)
    monkeypatch.setattr(
        "sys.argv",
        [
            "main.py",
            "export-df",
            "--lake-root",
            "lake/bronze",
            "--output",
            str(output_dir),
            "--format",
            "parquet",
            "--no-json-output",
        ],
    )

    cli.main()

    assert (output_dir / "binance_BTCUSDT_1m_full.parquet").exists()
    assert (output_dir / "binance_BTCUSDT_1m_full.png").exists()
    assert captured["lake_root"] == "lake/bronze"
    assert captured["instrument_types"] == ["spot", "perp"]


def test_main_export_df_uses_default_output_name_from_filters(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.chdir(tmp_path)
    frame = pd.DataFrame(
        [
            {
                "exchange": "binance",
                "instrument_type": "spot",
                "symbol": "BTCUSDT",
                "timeframe": "1m",
                "open_time": datetime(2026, 4, 27, 10, 0, tzinfo=UTC),
                "close_time": datetime(2026, 4, 27, 10, 0, 59, tzinfo=UTC),
                "open": 100.0,
                "high": 101.0,
                "low": 99.0,
                "close": 100.5,
                "volume": 12.0,
                "quote_volume": 1200.0,
                "trade_count": 10,
            },
            {
                "exchange": "binance",
                "instrument_type": "perp",
                "symbol": "BTCUSDT",
                "timeframe": "1m",
                "open_time": datetime(2026, 4, 27, 10, 1, tzinfo=UTC),
                "close_time": datetime(2026, 4, 27, 10, 1, 59, tzinfo=UTC),
                "open": 100.5,
                "high": 101.5,
                "low": 100.0,
                "close": 101.0,
                "volume": 11.0,
                "quote_volume": 1110.0,
                "trade_count": 9,
            },
        ]
    )
    def fake_save_candle_plots(**kwargs: object) -> list[str]:
        del kwargs
        temp_plot = tmp_path / "temporary_plot.png"
        temp_plot.write_bytes(b"PNG")
        return [str(temp_plot.resolve())]

    monkeypatch.setattr(cli, "load_combined_dataframe_from_lake", lambda **kwargs: frame)
    monkeypatch.setattr(cli, "save_candle_plots", fake_save_candle_plots)
    monkeypatch.setattr(
        "sys.argv",
        [
            "main.py",
            "export-df",
            "--format",
            "csv",
            "--output",
            str(tmp_path),
            "--exchanges",
            "binance",
            "--symbols",
            "BTCUSDT",
            "--timeframes",
            "1m",
            "--no-json-output",
        ],
    )

    cli.main()

    assert (tmp_path / "binance_BTCUSDT_1m_full.csv").exists()
    assert (tmp_path / "binance_BTCUSDT_1m_full.png").exists()


def test_main_export_df_creates_one_file_per_exchange_symbol_timeframe(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    frame = pd.DataFrame(
        [
            {
                "exchange": "binance",
                "instrument_type": "spot",
                "symbol": "BTCUSDT",
                "timeframe": "1m",
                "open_time": datetime(2026, 4, 27, 10, 0, tzinfo=UTC),
                "close_time": datetime(2026, 4, 27, 10, 0, 59, tzinfo=UTC),
                "open": 100.0,
                "high": 101.0,
                "low": 99.0,
                "close": 100.5,
                "volume": 12.0,
                "quote_volume": 1200.0,
                "trade_count": 10,
            },
            {
                "exchange": "binance",
                "instrument_type": "perp",
                "symbol": "BTCUSDT",
                "timeframe": "1m",
                "open_time": datetime(2026, 4, 27, 10, 1, tzinfo=UTC),
                "close_time": datetime(2026, 4, 27, 10, 1, 59, tzinfo=UTC),
                "open": 100.5,
                "high": 101.5,
                "low": 100.0,
                "close": 101.0,
                "volume": 11.0,
                "quote_volume": 1110.0,
                "trade_count": 9,
            },
            {
                "exchange": "binance",
                "instrument_type": "spot",
                "symbol": "ETHUSDT",
                "timeframe": "1m",
                "open_time": datetime(2026, 4, 27, 10, 0, tzinfo=UTC),
                "close_time": datetime(2026, 4, 27, 10, 0, 59, tzinfo=UTC),
                "open": 200.0,
                "high": 201.0,
                "low": 199.0,
                "close": 200.5,
                "volume": 22.0,
                "quote_volume": 4400.0,
                "trade_count": 20,
            },
            {
                "exchange": "binance",
                "instrument_type": "perp",
                "symbol": "ETHUSDT",
                "timeframe": "1m",
                "open_time": datetime(2026, 4, 27, 10, 1, tzinfo=UTC),
                "close_time": datetime(2026, 4, 27, 10, 1, 59, tzinfo=UTC),
                "open": 200.5,
                "high": 201.5,
                "low": 200.0,
                "close": 201.0,
                "volume": 21.0,
                "quote_volume": 4221.0,
                "trade_count": 19,
            },
        ]
    )
    output_dir = tmp_path / "exports"
    def fake_save_candle_plots(**kwargs: object) -> list[str]:
        del kwargs
        temp_plot = output_dir / "temporary_plot.png"
        temp_plot.parent.mkdir(parents=True, exist_ok=True)
        temp_plot.write_bytes(b"PNG")
        return [str(temp_plot.resolve())]

    monkeypatch.setattr(cli, "load_combined_dataframe_from_lake", lambda **kwargs: frame)
    monkeypatch.setattr(cli, "save_candle_plots", fake_save_candle_plots)
    monkeypatch.setattr(
        "sys.argv",
        [
            "main.py",
            "export-df",
            "--output",
            str(output_dir),
            "--format",
            "csv",
            "--no-json-output",
        ],
    )

    cli.main()

    assert (output_dir / "binance_BTCUSDT_1m_full.csv").exists()
    assert (output_dir / "binance_ETHUSDT_1m_full.csv").exists()
    assert (output_dir / "binance_BTCUSDT_1m_full.png").exists()
    assert (output_dir / "binance_ETHUSDT_1m_full.png").exists()


def test_main_export_df_reports_generated_file_time_ranges(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    frame = pd.DataFrame(
        [
            {
                "exchange": "binance",
                "instrument_type": "spot",
                "symbol": "BTCUSDT",
                "timeframe": "1m",
                "open_time": datetime(2026, 4, 27, 10, 0, tzinfo=UTC),
                "close_time": datetime(2026, 4, 27, 10, 0, 59, tzinfo=UTC),
                "open": 100.0,
                "high": 101.0,
                "low": 99.0,
                "close": 100.5,
                "volume": 12.0,
                "quote_volume": 1200.0,
                "trade_count": 10,
            },
            {
                "exchange": "binance",
                "instrument_type": "perp",
                "symbol": "BTCUSDT",
                "timeframe": "1m",
                "open_time": datetime(2026, 4, 27, 10, 1, tzinfo=UTC),
                "close_time": datetime(2026, 4, 27, 10, 1, 59, tzinfo=UTC),
                "open": 100.5,
                "high": 101.5,
                "low": 100.0,
                "close": 101.0,
                "volume": 11.0,
                "quote_volume": 1110.0,
                "trade_count": 9,
            },
        ]
    )
    output_dir = tmp_path / "exports"

    def fake_save_candle_plots(**kwargs: object) -> list[str]:
        del kwargs
        temp_plot = output_dir / "temporary_plot.png"
        temp_plot.parent.mkdir(parents=True, exist_ok=True)
        temp_plot.write_bytes(b"PNG")
        return [str(temp_plot.resolve())]

    monkeypatch.setattr(cli, "load_combined_dataframe_from_lake", lambda **kwargs: frame)
    monkeypatch.setattr(cli, "save_candle_plots", fake_save_candle_plots)
    monkeypatch.setattr(
        "sys.argv",
        [
            "main.py",
            "export-df",
            "--output",
            str(output_dir),
            "--format",
            "csv",
        ],
    )

    cli.main()

    payload = json.loads(capsys.readouterr().out)
    generated = payload["generated_files"]
    assert len(generated) == 1
    assert generated[0]["symbol"] == "BTCUSDT"
    assert generated[0]["start_open_time"] == "2026-04-27T10:00:00+00:00"
    assert generated[0]["end_open_time"] == "2026-04-27T10:01:00+00:00"

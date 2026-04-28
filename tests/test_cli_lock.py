"""Tests for single-instance CLI locking."""

from __future__ import annotations

import fcntl
from datetime import UTC, datetime
from pathlib import Path
from typing import cast

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


def test_gap_fill_bootstraps_when_no_lake_data(monkeypatch: pytest.MonkeyPatch) -> None:
    called: dict[str, object] = {}

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
    monkeypatch.setattr(cli, "max_candles_per_request", lambda exchange: 1000)

    def fake_fetch_candles(**kwargs: object) -> list[SpotCandle]:
        called.update(kwargs)
        return [sample]

    monkeypatch.setattr(cli, "fetch_candles", fake_fetch_candles)

    candles = cli._fetch_symbol_candles(
        exchange="binance",
        market="spot",
        symbol="BTCUSDT",
        timeframe="1m",
        limit=None,
        all_history=False,
        mode="gap-fill",
        lake_root="lake/bronze",
    )

    assert len(candles) == 1
    assert called["limit"] == 1000


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
        limit=None,
        all_history=False,
        mode="gap-fill",
        lake_root="lake/bronze",
    )

    gap_one_ms = int(datetime(2026, 4, 27, 10, 1, tzinfo=UTC).timestamp() * 1000)
    gap_tail_start_ms = int(datetime(2026, 4, 27, 10, 4, tzinfo=UTC).timestamp() * 1000)
    assert candles == []
    assert calls == [(gap_one_ms, gap_one_ms), (gap_tail_start_ms, end_open_ms)]


def test_all_history_mode_uses_all_history_fetch(monkeypatch: pytest.MonkeyPatch) -> None:
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
        limit=None,
        all_history=True,
        mode="gap-fill",
        lake_root="lake/bronze",
    )

    assert len(candles) == 1
    assert len(calls) == 1
    assert calls[0]["exchange"] == "binance"
    assert calls[0]["symbol"] == "BTCUSDT"


def test_main_ingest_command_does_not_acquire_single_instance_lock(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class FailIfEnteredLock:
        def __init__(self, lock_path: str) -> None:
            del lock_path

        def __enter__(self) -> None:
            raise AssertionError("lock should not be used for ingest command")

        def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
            del exc_type, exc, tb

    monkeypatch.setattr(cli, "SingleInstanceLock", FailIfEnteredLock)
    monkeypatch.setattr(cli, "load_timescale_config_from_env", lambda: object())
    monkeypatch.setattr(
        cli,
        "ingest_parquet_to_timescaledb",
        lambda **kwargs: {
            "files_scanned": 0,
            "files_ingested": 0,
            "rows_upserted": 0,
            "files_skipped": 0,
        },
    )
    monkeypatch.setattr(
        "sys.argv",
        ["main.py", "ingest-parquet-to-db", "--lake-root", "lake/bronze", "--no-json-output"],
    )

    cli.main()


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
            "--limit",
            "1",
            "--no-json-output",
        ],
    )

    with pytest.raises(SystemExit, match="loader already running"):
        cli.main()


def test_main_export_combined_df_does_not_acquire_single_instance_lock(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    class FailIfEnteredLock:
        def __init__(self, lock_path: str) -> None:
            del lock_path

        def __enter__(self) -> None:
            raise AssertionError("lock should not be used for export-combined-df command")

        def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
            del exc_type, exc, tb

    class FakeDataFrame:
        def __init__(self) -> None:
            self.shape = (2, 3)
            self.columns = ["exchange", "instrument_type", "symbol"]
            self.last_path: Path | None = None
            self.last_index: bool | None = None

        def to_parquet(self, path: Path, index: bool = False) -> None:
            self.last_path = path
            self.last_index = index
            path.write_bytes(b"PAR1")

        def to_csv(self, path: Path, index: bool = False) -> None:
            self.last_path = path
            self.last_index = index
            path.write_text("exchange,instrument_type,symbol\nbinance,spot,BTCUSDT\n", encoding="utf-8")

    fake_df = FakeDataFrame()
    captured: dict[str, object] = {}

    def fake_loader(**kwargs: object) -> FakeDataFrame:
        captured.update(kwargs)
        return fake_df

    output_path = tmp_path / "combined.parquet"
    monkeypatch.setattr(cli, "SingleInstanceLock", FailIfEnteredLock)
    monkeypatch.setattr(cli, "load_timescale_config_from_env", lambda: object())
    monkeypatch.setattr(cli, "load_combined_dataframe_from_db", fake_loader)
    monkeypatch.setattr(
        "sys.argv",
        [
            "main.py",
            "export-combined-df",
            "--output",
            str(output_path),
            "--format",
            "parquet",
            "--no-json-output",
        ],
    )

    cli.main()

    assert output_path.exists()
    assert captured["instrument_types"] == ["spot", "perp"]

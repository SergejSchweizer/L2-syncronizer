"""Tests for L2 CLI parsing and single-instance locking."""

from __future__ import annotations

import fcntl
import json
import logging
from datetime import UTC, datetime
from pathlib import Path

import pytest

from api import cli
from api.cli import SingleInstanceError, SingleInstanceLock
from ingestion.l2 import L2Snapshot


@pytest.fixture(autouse=True)
def _isolate_cli_test_logs(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Keep CLI tests from writing to the configured runtime log directory."""

    monkeypatch.setenv("L2_SYNC_LOG_DIR", str(tmp_path / "logs"))
    monkeypatch.setenv("L2_INGEST_NO_JSON_OUTPUT", "false")
    monkeypatch.setenv("L2_INGEST_SAVE_PARQUET_LAKE", "false")


def test_single_instance_lock_creates_lock_file(tmp_path: Path) -> None:
    """Verify the process lock writes a pid file while held."""

    lock_file = tmp_path / "test.lock"

    with SingleInstanceLock(str(lock_file)):
        assert lock_file.exists()
        content = lock_file.read_text().strip()
        assert content.isdigit()


def test_single_instance_lock_raises_on_contention(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Verify the process lock fails fast when another instance owns it."""

    lock_file = tmp_path / "test.lock"

    def fake_flock(fd: int, operation: int) -> None:
        del fd, operation
        raise BlockingIOError("locked")

    monkeypatch.setattr(fcntl, "flock", fake_flock)

    with pytest.raises(SingleInstanceError):
        with SingleInstanceLock(str(lock_file)):
            pass


def test_l2_parser_defaults_can_come_from_config_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify L2 loader defaults are configurable through environment variables."""

    monkeypatch.setenv("L2_INGEST_SYMBOLS", "BTC,ETH")
    monkeypatch.setenv("L2_INGEST_LEVELS", "25")
    monkeypatch.setenv("L2_INGEST_SNAPSHOT_COUNT", "5")
    monkeypatch.setenv("L2_INGEST_POLL_INTERVAL_S", "10")
    monkeypatch.setenv("L2_INGEST_MAX_RUNTIME_S", "55")
    monkeypatch.setenv("L2_INGEST_SAVE_PARQUET_LAKE", "true")
    monkeypatch.setenv("L2_INGEST_LAKE_ROOT", "custom/bronze")
    monkeypatch.setenv("L2_INGEST_NO_JSON_OUTPUT", "true")

    args = cli.build_parser().parse_args(["loader-l2-m1"])

    assert args.symbols == ["BTC", "ETH"]
    assert args.levels == 25
    assert args.snapshot_count == 5
    assert args.poll_interval_s == 10.0
    assert args.max_runtime_s == 55.0
    assert args.save_parquet_lake is True
    assert args.lake_root == "custom/bronze"
    assert args.json_output is False


def test_l2_parser_cli_can_override_boolean_env_defaults(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify paired boolean flags can override local env defaults."""

    monkeypatch.setenv("L2_INGEST_SAVE_PARQUET_LAKE", "true")
    monkeypatch.setenv("L2_INGEST_NO_JSON_OUTPUT", "true")

    args = cli.build_parser().parse_args(["loader-l2-m1", "--no-save-parquet-lake", "--json-output"])

    assert args.save_parquet_lake is False
    assert args.json_output is True


def test_l2_symbols_accept_comma_delimited_cli_values() -> None:
    """Verify the CLI symbol normalizer accepts comma-delimited values."""

    args = cli.build_parser().parse_args(["loader-l2-m1", "--symbols", "btc,eth", "SOL"])

    assert cli._normalize_cli_symbols(args.symbols) == ["BTC", "ETH", "SOL"]


def test_warn_for_long_poll_schedule_logs_cron_overlap(caplog: pytest.LogCaptureFixture) -> None:
    """Verify long polling schedules emit an operational warning."""

    logger = logging.getLogger("test_l2_schedule_warning")

    with caplog.at_level("WARNING", logger=logger.name):
        cli._warn_for_long_poll_schedule(
            logger=logger,
            snapshot_count=7,
            poll_interval_s=10,
            max_runtime_s=0,
        )

    assert "cron runs may overlap" in caplog.text


def test_validate_symbols_reports_valid_books(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify symbol validation returns normalized symbols and book status."""

    def fake_fetch_order_book_snapshot(symbol: str, depth: int) -> dict[str, object]:
        assert symbol == "SOL"
        assert depth == 1
        return {
            "exchange": "deribit",
            "symbol": "SOL_USDC-PERPETUAL",
            "timestamp_ms": 1_700_000_000_000,
            "bids": [(84.0, 2.0)],
            "asks": [(84.1, 3.0)],
            "mark_price": 84.05,
            "index_price": 84.0,
            "open_interest": 1000.0,
            "funding_8h": 0.0001,
            "current_funding": 0.00001,
        }

    monkeypatch.setattr(cli, "fetch_order_book_snapshot", fake_fetch_order_book_snapshot)

    result = cli._validate_symbol(symbol="SOL", depth=1)

    assert result["normalized_symbol"] == "SOL_USDC-PERPETUAL"
    assert result["valid_book"] is True
    assert result["error"] is None


def test_main_validate_symbols_outputs_json(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Verify the validate-symbols command prints a validation summary."""

    monkeypatch.setattr(
        cli,
        "_validate_symbol",
        lambda symbol, depth: {
            "symbol": symbol,
            "normalized_symbol": f"{symbol}-PERPETUAL",
            "valid_book": True,
            "bid_levels": depth,
            "ask_levels": depth,
            "error": None,
        },
    )
    monkeypatch.setattr(
        "sys.argv",
        [
            "main.py",
            "validate-symbols",
            "--symbols",
            "BTC,SOL",
            "--levels",
            "1",
        ],
    )

    cli.main()

    output = json.loads(capsys.readouterr().out)
    assert output["all_valid"] is True
    assert [item["symbol"] for item in output["symbols"]] == ["BTC", "SOL"]


def test_main_loader_l2_uses_single_instance_lock(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify the L2 loader command exits when the single-instance lock is held."""

    class Locked:
        """Test double that simulates an already-running L2 loader."""

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
            "loader-l2-m1",
            "--symbols",
            "BTC",
            "--snapshot-count",
            "1",
            "--poll-interval-s",
            "0",
            "--no-json-output",
        ],
    )

    with pytest.raises(SystemExit, match="loader already running"):
        cli.main()


def test_main_loader_l2_outputs_aggregated_rows(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Verify the CLI aggregates fetched snapshots and writes JSON output."""

    class NoopLock:
        """Test double that allows the L2 loader to run."""

        def __init__(self, lock_path: str) -> None:
            del lock_path

        def __enter__(self) -> None:
            return None

        def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
            del exc_type, exc, tb

    snapshot = L2Snapshot(
        exchange="deribit",
        symbol="BTC-PERPETUAL",
        timestamp=datetime(2026, 4, 29, 10, 0, 1, tzinfo=UTC),
        fetch_duration_s=0.1,
        bids=[(100.0, 1.0)],
        asks=[(101.0, 1.0)],
        mark_price=100.5,
        index_price=100.0,
        open_interest=1000.0,
        funding_8h=0.0001,
        current_funding=0.00001,
    )

    monkeypatch.setattr(cli, "SingleInstanceLock", NoopLock)
    monkeypatch.setattr(cli, "fetch_l2_snapshots_for_symbols", lambda **kwargs: {"BTC": [snapshot]})
    monkeypatch.setattr(
        "sys.argv",
        [
            "main.py",
            "loader-l2-m1",
            "--symbols",
            "BTC",
            "--snapshot-count",
            "1",
            "--poll-interval-s",
            "0",
        ],
    )

    cli.main()

    output = json.loads(capsys.readouterr().out)
    assert output["deribit"]["BTC"][0]["symbol"] == "BTC-PERPETUAL"
    assert output["deribit"]["BTC"][0]["mid_close"] == 100.5

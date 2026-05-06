"""Tests for L2 CLI parsing and single-instance locking."""

from __future__ import annotations

import fcntl
import json
import logging
from datetime import UTC, datetime
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path

import pytest

from api import cli
from api.cli import SingleInstanceError, SingleInstanceLock
from api.constants import BRONZE_BUILDER_COMMAND, SILVER_BUILDER_COMMAND
from api.runtime import configure_logging
from ingestion.config import Config
from ingestion.l2 import L2Snapshot


@pytest.fixture(autouse=True)
def _isolate_cli_test_logs(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Keep CLI tests from writing to the configured runtime log directory."""

    config = _config(log_dir=str(tmp_path / "logs"))
    monkeypatch.setattr(cli, "load_config", lambda: config)


def _config(
    *,
    log_dir: str = "/tmp/crypto-l2-loader-test-logs",
    symbols: list[str] | None = None,
    levels: int = 50,
    snapshot_count: int = 5,
    poll_interval_s: float = 10.0,
    max_runtime_s: float = 50.0,
    save_parquet_lake: bool = False,
    lake_root: str = "lake/bronze",
    silver_lake_root: str = "lake/silver",
    json_output: bool = True,
) -> Config:
    """Build a minimal test config."""

    return {
        "http": {
            "timeout_s": 8,
            "max_retries": 2,
            "retry_backoff_s": 1,
        },
        "runtime": {
            "log_dir": log_dir,
            "log_rotation_days": 7,
            "log_backup_count": 0,
            "fetch_concurrency": 8,
        },
        "ingestion": {
            "exchange": "deribit",
            "symbols": symbols or ["BTC", "ETH"],
            "levels": levels,
            "snapshot_count": snapshot_count,
            "poll_interval_s": poll_interval_s,
            "max_runtime_s": max_runtime_s,
            "save_parquet_lake": save_parquet_lake,
            "lake_root": lake_root,
            "silver_lake_root": silver_lake_root,
            "json_output": json_output,
        },
    }


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


def test_configure_logging_rotates_weekly_and_keeps_old_logs(tmp_path: Path) -> None:
    """Verify runtime logging rotates weekly and keeps rotated logs."""

    logger = configure_logging(
        module_name="test-weekly-rotation",
        config=_config(log_dir=str(tmp_path / "logs")),
    )
    try:
        file_handler = next(handler for handler in logger.handlers if isinstance(handler, TimedRotatingFileHandler))

        assert file_handler.when == "D"
        assert file_handler.interval == 7 * 24 * 60 * 60
        assert file_handler.backupCount == 0
    finally:
        for handler in list(logger.handlers):
            handler.close()
            logger.removeHandler(handler)


def test_l2_parser_defaults_can_come_from_config() -> None:
    """Verify bronze-builder defaults are configurable through config."""

    args = cli.build_parser(
        _config(
            symbols=["BTC", "ETH"],
            levels=25,
            snapshot_count=5,
            poll_interval_s=10,
            max_runtime_s=50,
            save_parquet_lake=True,
            lake_root="custom/bronze",
            json_output=False,
        )
    ).parse_args([BRONZE_BUILDER_COMMAND])

    assert args.symbols == ["BTC", "ETH"]
    assert args.levels == 25
    assert args.snapshot_count == 5
    assert args.poll_interval_s == 10.0
    assert args.max_runtime_s == 50.0
    assert args.save_parquet_lake is True
    assert args.lake_root == "custom/bronze"
    assert args.json_output is False


def test_l2_parser_defaults_to_five_snapshots_per_run() -> None:
    """Verify the default L2 cadence leaves room for one-minute cron runs."""

    args = cli.build_parser().parse_args([BRONZE_BUILDER_COMMAND])

    assert args.snapshot_count == 5
    assert args.poll_interval_s == 10.0
    assert args.max_runtime_s == 50.0


def test_l2_parser_cli_can_override_boolean_config_defaults() -> None:
    """Verify paired boolean flags can override config defaults."""

    args = cli.build_parser(_config(save_parquet_lake=True, json_output=False)).parse_args(
        [BRONZE_BUILDER_COMMAND, "--no-save-parquet-lake", "--json-output"]
    )

    assert args.save_parquet_lake is False
    assert args.json_output is True


def test_l2_symbols_accept_comma_delimited_cli_values() -> None:
    """Verify the CLI symbol normalizer accepts comma-delimited values."""

    args = cli.build_parser().parse_args([BRONZE_BUILDER_COMMAND, "--symbols", "btc,eth", "SOL"])

    assert cli._normalize_cli_symbols(args.symbols) == ["BTC", "ETH", "SOL"]


def test_silver_builder_parser_defaults_can_come_from_config() -> None:
    """Verify silver-builder defaults are configurable through config."""

    args = cli.build_parser(
        _config(
            levels=25,
            lake_root="custom/bronze",
            silver_lake_root="custom/silver",
            json_output=False,
        )
    ).parse_args([SILVER_BUILDER_COMMAND])

    assert args.bronze_lake_root == "custom/bronze"
    assert args.silver_lake_root == "custom/silver"
    assert args.depth == 25
    assert args.json_output is False


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
    """Verify bronze-builder exits when the single-instance lock is held."""

    class Locked:
        """Test double that simulates an already-running bronze-builder."""

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
            BRONZE_BUILDER_COMMAND,
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


def test_main_loader_l2_outputs_raw_snapshots(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Verify the CLI writes raw snapshots to JSON output."""

    class NoopLock:
        """Test double that allows bronze-builder to run."""

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
            BRONZE_BUILDER_COMMAND,
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
    assert output["deribit"]["BTC"][0]["timestamp"] == "2026-04-29T10:00:01+00:00"
    assert output["deribit"]["BTC"][0]["bids"] == [[100.0, 1.0]]


def test_main_loader_l2_persists_raw_snapshots_to_lake(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Verify the parquet save flag writes raw snapshots with the requested depth."""

    class NoopLock:
        """Test double that allows bronze-builder to run."""

        def __init__(self, lock_path: str) -> None:
            del lock_path

        def __enter__(self) -> None:
            return None

        def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
            del exc_type, exc, tb

    snapshot = L2Snapshot(
        exchange="deribit",
        symbol="BTC-PERPETUAL",
        timestamp=datetime(2026, 5, 5, 10, 0, 1, tzinfo=UTC),
        fetch_duration_s=0.1,
        bids=[(100.0, 1.0)],
        asks=[(101.0, 1.0)],
        mark_price=100.5,
        index_price=100.0,
        open_interest=1000.0,
        funding_8h=0.0001,
        current_funding=0.00001,
    )
    calls: list[dict[str, object]] = []

    def fake_save_l2_snapshot_parquet_lake(**kwargs: object) -> list[str]:
        calls.append(kwargs)
        return ["/tmp/lake/bronze/dataset_type=l2_snapshot/data.parquet"]

    monkeypatch.setattr(cli, "SingleInstanceLock", NoopLock)
    monkeypatch.setattr(cli, "fetch_l2_snapshots_for_symbols", lambda **kwargs: {"BTC": [snapshot]})
    monkeypatch.setattr(cli, "save_l2_snapshot_parquet_lake", fake_save_l2_snapshot_parquet_lake)
    monkeypatch.setattr(
        "sys.argv",
        [
            "main.py",
            BRONZE_BUILDER_COMMAND,
            "--symbols",
            "BTC",
            "--levels",
            "50",
            "--snapshot-count",
            "1",
            "--poll-interval-s",
            "0",
            "--save-parquet-lake",
        ],
    )

    cli.main()

    output = json.loads(capsys.readouterr().out)
    assert output["_parquet_files"] == ["/tmp/lake/bronze/dataset_type=l2_snapshot/data.parquet"]
    assert calls[0]["snapshots_by_symbol"] == {"BTC": [snapshot]}
    assert calls[0]["depth"] == 50


def test_main_silver_builder_outputs_written_files(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Verify silver-builder runs the Bronze-to-Silver transform."""

    class NoopLock:
        """Test double that allows silver-builder to run."""

        def __init__(self, lock_path: str) -> None:
            del lock_path

        def __enter__(self) -> None:
            return None

        def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
            del exc_type, exc, tb

    calls: list[dict[str, object]] = []

    def fake_transform_l2_bronze_to_silver(**kwargs: object) -> list[str]:
        calls.append(kwargs)
        return ["/tmp/lake/silver/dataset_type=l2_snapshot_features/data.parquet"]

    monkeypatch.setattr(cli, "SingleInstanceLock", NoopLock)
    monkeypatch.setattr(cli, "transform_l2_bronze_to_silver", fake_transform_l2_bronze_to_silver)
    monkeypatch.setattr(
        "sys.argv",
        [
            "main.py",
            SILVER_BUILDER_COMMAND,
            "--bronze-lake-root",
            "custom/bronze",
            "--silver-lake-root",
            "custom/silver",
            "--depth",
            "50",
        ],
    )

    cli.main()

    output = json.loads(capsys.readouterr().out)
    assert output["command"] == SILVER_BUILDER_COMMAND
    assert output["parquet_files"] == ["/tmp/lake/silver/dataset_type=l2_snapshot_features/data.parquet"]
    assert calls == [
        {
            "bronze_lake_root": "custom/bronze",
            "silver_lake_root": "custom/silver",
            "depth": 50,
        }
    ]


def test_main_silver_builder_uses_single_instance_lock(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify silver-builder exits when the single-instance lock is held."""

    class Locked:
        """Test double that simulates an already-running silver-builder."""

        def __init__(self, lock_path: str) -> None:
            del lock_path

        def __enter__(self) -> None:
            raise SingleInstanceError("silver builder already running")

        def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
            del exc_type, exc, tb

    monkeypatch.setattr(cli, "SingleInstanceLock", Locked)
    monkeypatch.setattr(
        "sys.argv",
        [
            "main.py",
            SILVER_BUILDER_COMMAND,
            "--no-json-output",
        ],
    )

    with pytest.raises(SystemExit, match="silver builder already running"):
        cli.main()

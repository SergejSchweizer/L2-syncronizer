"""Command-line interface for data ingestion tasks."""

from __future__ import annotations

import argparse
import fcntl
import json
import logging
import os
from dataclasses import asdict
from datetime import UTC, datetime
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path
from typing import Literal, cast

from ingestion.lake import (
    load_spot_candles_from_lake,
    open_times_in_lake,
    save_spot_candles_parquet_lake,
)
from ingestion.plotting import PriceField, save_candle_plots
from ingestion.spot import (
    Exchange,
    Market,
    SpotCandle,
    fetch_candles,
    fetch_candles_all_history,
    fetch_candles_range,
    interval_to_milliseconds,
    list_supported_intervals,
    max_candles_per_request,
    normalize_storage_symbol,
    normalize_timeframe,
)
from ingestion.timescaledb_loader import (
    ingest_parquet_to_timescaledb,
    load_combined_dataframe_from_db,
    load_timescale_config_from_env,
)

FetchMode = Literal["latest", "gap-fill"]
LOGGER_NAME = "crypto_l2_loader"
DEFAULT_LOG_DIR = "/volume1/Temp/logs"


class SingleInstanceError(RuntimeError):
    """Raised when another CLI instance is already running."""


class SingleInstanceLock:
    """Non-blocking process lock backed by a lock file."""

    def __init__(self, lock_path: str) -> None:
        self.lock_path = Path(lock_path)
        self._fd: int | None = None

    def __enter__(self) -> SingleInstanceLock:
        self.lock_path.parent.mkdir(parents=True, exist_ok=True)
        self._fd = os.open(self.lock_path, os.O_CREAT | os.O_RDWR, 0o644)
        try:
            fcntl.flock(self._fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            os.close(self._fd)
            self._fd = None
            raise SingleInstanceError("Another crypto-l2-loader instance is already running. Exiting.") from exc
        os.ftruncate(self._fd, 0)
        os.write(self._fd, str(os.getpid()).encode("utf-8"))
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        if self._fd is None:
            return
        fcntl.flock(self._fd, fcntl.LOCK_UN)
        os.close(self._fd)
        self._fd = None


def _configure_logging() -> logging.Logger:
    """Configure file logging with weekly rotation."""

    logger = logging.getLogger(LOGGER_NAME)
    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)
    logger.propagate = False
    formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    log_dir = Path(os.getenv("L2_SYNC_LOG_DIR", DEFAULT_LOG_DIR))
    try:
        log_dir.mkdir(parents=True, exist_ok=True)
        file_handler = TimedRotatingFileHandler(
            filename=log_dir / "crypto-l2-loader.log",
            when="D",
            interval=7,
            backupCount=0,
            encoding="utf-8",
            utc=True,
        )
        file_handler.suffix = "%Y-%m-%d"
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    except OSError:
        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(formatter)
        logger.addHandler(stream_handler)
        logger.warning("Falling back to stderr logging; cannot create log directory '%s'", log_dir)

    return logger


def _serialize_candle(candle: SpotCandle) -> dict[str, object]:
    """Convert a ``SpotCandle`` into JSON-safe dictionary."""

    data = asdict(candle)
    for key in ("open_time", "close_time"):
        value = data[key]
        if isinstance(value, datetime):
            data[key] = value.isoformat()
    return data


def _last_closed_open_ms(interval_ms: int, now_utc: datetime | None = None) -> int:
    """Return open timestamp (ms) of latest fully closed candle."""

    now = now_utc or datetime.now(UTC)
    now_ms = int(now.timestamp() * 1000)
    return ((now_ms // interval_ms) - 1) * interval_ms


def _missing_ranges_ms(
    existing_open_times: list[datetime],
    interval_ms: int,
    end_open_ms: int,
) -> list[tuple[int, int]]:
    """Build contiguous missing open-time ranges from known candles to end-open timestamp."""

    existing_ms = sorted(
        {
            int(item.timestamp() * 1000)
            for item in existing_open_times
            if item.tzinfo is not None and int(item.timestamp() * 1000) <= end_open_ms
        }
    )
    if not existing_ms:
        return []

    ranges: list[tuple[int, int]] = []
    for previous, current in zip(existing_ms, existing_ms[1:], strict=False):
        gap_start_ms = previous + interval_ms
        gap_end_ms = current - interval_ms
        if gap_start_ms <= gap_end_ms:
            ranges.append((gap_start_ms, gap_end_ms))

    last_existing_ms = existing_ms[-1]
    if last_existing_ms + interval_ms <= end_open_ms:
        ranges.append((last_existing_ms + interval_ms, end_open_ms))

    return ranges


def _fetch_symbol_candles(
    exchange: Exchange,
    market: Market,
    symbol: str,
    timeframe: str,
    limit: int | None,
    all_history: bool,
    mode: FetchMode,
    lake_root: str,
) -> list[SpotCandle]:
    """Fetch candles for one symbol using latest/gap-fill logic."""

    if all_history:
        return fetch_candles_all_history(
            exchange=exchange,
            symbol=symbol,
            interval=timeframe,
            market=market,
        )

    if limit is not None:
        return fetch_candles(exchange=exchange, symbol=symbol, interval=timeframe, limit=limit, market=market)

    if mode == "latest":
        bootstrap_limit = max_candles_per_request(exchange)
        return fetch_candles(
            exchange=exchange,
            symbol=symbol,
            interval=timeframe,
            limit=bootstrap_limit,
            market=market,
        )

    storage_symbol = normalize_storage_symbol(exchange=exchange, symbol=symbol, market=market)
    stored_open_times = open_times_in_lake(
        lake_root=lake_root,
        market=market,
        exchange=exchange,
        symbol=storage_symbol,
        timeframe=timeframe,
    )

    interval_ms = interval_to_milliseconds(exchange=exchange, interval=timeframe)
    end_open_ms = _last_closed_open_ms(interval_ms=interval_ms)
    if not stored_open_times:
        bootstrap_limit = max_candles_per_request(exchange)
        return fetch_candles(
            exchange=exchange,
            symbol=symbol,
            interval=timeframe,
            limit=bootstrap_limit,
            market=market,
        )
    if end_open_ms < int(min(stored_open_times).timestamp() * 1000):
        return []

    missing_ranges = _missing_ranges_ms(
        existing_open_times=stored_open_times,
        interval_ms=interval_ms,
        end_open_ms=end_open_ms,
    )
    if not missing_ranges:
        return []

    fetched: list[SpotCandle] = []
    for start_open_ms, gap_end_ms in missing_ranges:
        fetched.extend(
            fetch_candles_range(
                exchange=exchange,
                symbol=symbol,
                interval=timeframe,
                start_open_ms=start_open_ms,
                end_open_ms=gap_end_ms,
                market=market,
            )
        )

    unique_by_open_time = {item.open_time: item for item in fetched}
    return [unique_by_open_time[key] for key in sorted(unique_by_open_time)]


def _parse_iso_datetime(value: str | None) -> datetime | None:
    """Parse ISO datetime string (supports trailing 'Z')."""

    if value is None:
        return None
    normalized = value.strip()
    if normalized.endswith("Z"):
        normalized = normalized[:-1] + "+00:00"
    return datetime.fromisoformat(normalized)


def build_parser() -> argparse.ArgumentParser:
    """Create top-level CLI parser."""

    parser = argparse.ArgumentParser(description="crypto-l2-loader CLI")
    subparsers = parser.add_subparsers(dest="command", required=True)

    spot_parser = subparsers.add_parser("loader", help="Fetch candles from supported exchanges")
    spot_parser.add_argument("--exchange", choices=["binance", "deribit", "bybit"], default="binance")
    spot_parser.add_argument(
        "--exchanges",
        nargs="+",
        choices=["binance", "deribit", "bybit"],
        help="Optional list of exchanges to fetch in one run",
    )
    spot_parser.add_argument(
        "--market",
        nargs="+",
        choices=["spot", "perp"],
        default=["spot"],
        help="One or more markets to fetch, e.g. --market spot perp",
    )
    spot_parser.add_argument(
        "--symbols",
        nargs="+",
        default=["BTCUSDT", "ETHUSDT"],
        help="Symbols or instrument aliases (exchange specific)",
    )
    spot_parser.add_argument(
        "--timeframe",
        "--interval",
        dest="timeframe",
        default="1h",
        help="Candle timeframe, e.g. M1, M5, H1, D1, 1m, 1h, 1d",
    )
    spot_parser.add_argument(
        "--timeframes",
        nargs="+",
        help="Optional list of timeframes. When set, fetch runs for each timeframe sequentially.",
    )
    spot_parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Optional latest-candle count. If omitted, gap-fill mode is used by default unless --all-history is set.",
    )
    spot_parser.add_argument(
        "--all-history",
        action="store_true",
        help="Fetch all available exchange history for each instrument/timeframe.",
    )
    spot_parser.add_argument(
        "--mode",
        choices=["latest", "gap-fill"],
        default="gap-fill",
        help="Fetch behavior when --limit is omitted.",
    )
    spot_parser.add_argument("--plot", action="store_true", help="Create and save price/volume plots")
    spot_parser.add_argument("--plot-dir", default="plots", help="Output directory for generated plots")
    spot_parser.add_argument(
        "--plot-price",
        choices=["spot", "close", "open", "high", "low"],
        default="close",
        help="Price field to plot (spot maps to close)",
    )
    spot_parser.add_argument(
        "--save-parquet-lake",
        action="store_true",
        help="Save fetched candles to parquet lake partitions",
    )
    spot_parser.add_argument(
        "--lake-root",
        default="lake/bronze",
        help="Root directory for parquet lake files",
    )
    spot_parser.add_argument(
        "--no-json-output",
        action="store_true",
        help="Suppress JSON output from loader command",
    )

    tf_parser = subparsers.add_parser("list-spot-timeframes", help="List exchange-supported candle timeframes")
    tf_parser.add_argument("--exchange", choices=["binance", "deribit", "bybit"], default="binance")
    tf_parser.add_argument(
        "--exchanges",
        nargs="+",
        choices=["binance", "deribit", "bybit"],
        help="Optional list of exchanges to list in one run",
    )

    ingest_parser = subparsers.add_parser(
        "ingest-parquet-to-db",
        help="Ingest parquet lake files into TimescaleDB",
    )
    ingest_parser.add_argument(
        "--lake-root",
        default="lake/bronze",
        help="Root directory for parquet lake files",
    )
    ingest_parser.add_argument(
        "--batch-size",
        type=int,
        default=1000,
        help="Upsert batch size for database inserts",
    )
    ingest_parser.add_argument(
        "--dataset-types",
        nargs="+",
        choices=["ohlcv"],
        help="Optional dataset type filter",
    )
    ingest_parser.add_argument(
        "--no-json-output",
        action="store_true",
        help="Suppress JSON output from ingest-parquet-to-db command",
    )

    export_parser = subparsers.add_parser(
        "export-combined-df",
        help="Query combined spot/perp dataset from DB and export as dataframe file",
    )
    export_parser.add_argument(
        "--output",
        default="combined_spot_perp.parquet",
        help="Output file path for dataframe export",
    )
    export_parser.add_argument(
        "--format",
        choices=["parquet", "csv"],
        default="parquet",
        help="Export file format",
    )
    export_parser.add_argument(
        "--exchanges",
        nargs="+",
        choices=["binance", "deribit"],
        help="Optional exchange filter",
    )
    export_parser.add_argument(
        "--instrument-types",
        nargs="+",
        choices=["spot", "perp"],
        default=["spot", "perp"],
        help="Instrument type filter",
    )
    export_parser.add_argument(
        "--symbols",
        nargs="+",
        help="Optional symbol filter",
    )
    export_parser.add_argument(
        "--timeframes",
        nargs="+",
        help="Optional timeframe filter (e.g. 1m 5m 1h)",
    )
    export_parser.add_argument(
        "--start-time",
        help="Optional inclusive start open_time in ISO format",
    )
    export_parser.add_argument(
        "--end-time",
        help="Optional inclusive end open_time in ISO format",
    )
    export_parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Optional max rows",
    )
    export_parser.add_argument(
        "--no-json-output",
        action="store_true",
        help="Suppress JSON summary output from export-combined-df command",
    )

    return parser


def main() -> None:
    """CLI entrypoint."""

    logger = _configure_logging()
    parser = build_parser()
    args = parser.parse_args()
    logger.info("Command start: %s", args.command)

    if args.command == "loader":
        try:
            with SingleInstanceLock(".run/crypto-l2-loader.lock"):
                if args.all_history and args.limit is not None:
                    parser.error("--all-history cannot be combined with --limit")
                exchanges = cast(list[Exchange], args.exchanges if args.exchanges else [args.exchange])
                mode = cast(FetchMode, args.mode)
                markets = cast(list[Market], args.market)
                multi_market = len(markets) > 1
                requested_timeframes = cast(list[str], args.timeframes if args.timeframes else [args.timeframe])
                multi_timeframe = len(requested_timeframes) > 1
                if args.all_history:
                    effective_mode = "all-history"
                elif args.limit is not None:
                    effective_mode = "latest-limit"
                else:
                    effective_mode = mode
                output: dict[str, object] = {"_effective_mode": effective_mode}
                candles_for_storage: dict[Market, dict[str, dict[str, list[SpotCandle]]]] = {}
                tasks: list[tuple[Exchange, Market, str, str]] = []

                for exchange in exchanges:
                    exchange_output: dict[str, object] = {}
                    output[exchange] = exchange_output
                    normalized_timeframes: list[str] = []
                    for timeframe_value in requested_timeframes:
                        try:
                            normalized_timeframes.append(normalize_timeframe(exchange=exchange, value=timeframe_value))
                        except Exception as exc:  # noqa: BLE001
                            exchange_output[f"_timeframe_error_{timeframe_value}"] = str(exc)
                            logger.exception(
                                "Failed to normalize timeframe exchange=%s timeframe=%s",
                                exchange,
                                timeframe_value,
                            )
                    if not normalized_timeframes:
                        continue
                    for market in markets:
                        for timeframe in normalized_timeframes:
                            for symbol in args.symbols:
                                tasks.append((exchange, market, symbol, timeframe))

                task_results: dict[tuple[Exchange, Market, str, str], list[SpotCandle]] = {}
                task_errors: dict[tuple[Exchange, Market, str, str], str] = {}
                for exchange, market, symbol, timeframe in tasks:
                    key = (exchange, market, symbol, timeframe)
                    try:
                        task_results[key] = _fetch_symbol_candles(
                            exchange=exchange,
                            market=market,
                            symbol=symbol,
                            timeframe=timeframe,
                            limit=args.limit,
                            all_history=args.all_history,
                            mode=mode,
                            lake_root=args.lake_root,
                        )
                    except Exception as exc:  # noqa: BLE001
                        task_errors[key] = str(exc)
                        logger.exception(
                            "Fetch failed exchange=%s market=%s symbol=%s timeframe=%s",
                            exchange,
                            market,
                            symbol,
                            timeframe,
                        )

                for exchange, market, symbol, timeframe in tasks:
                    exchange_output = cast(dict[str, object], output[exchange])
                    symbol_key = symbol.upper()
                    result_key = (exchange, market, symbol, timeframe)
                    if multi_market:
                        market_bucket = cast(dict[str, object], exchange_output.setdefault(market, {}))
                    else:
                        market_bucket = exchange_output
                    if multi_timeframe:
                        timeframe_bucket = cast(dict[str, object], market_bucket.setdefault(timeframe, {}))
                    else:
                        timeframe_bucket = market_bucket
                    if result_key in task_errors:
                        timeframe_bucket[symbol_key] = {"error": task_errors[result_key]}
                        continue
                    candles = task_results.get(result_key, [])
                    timeframe_bucket[symbol_key] = [_serialize_candle(item) for item in candles]
                    by_market = candles_for_storage.setdefault(market, {})
                    exchange_candles = by_market.setdefault(exchange, {})
                    if multi_market and multi_timeframe:
                        plot_key = f"{market}_{symbol_key}__{timeframe}"
                    elif multi_market:
                        plot_key = f"{market}_{symbol_key}"
                    elif multi_timeframe:
                        plot_key = f"{symbol_key}__{timeframe}"
                    else:
                        plot_key = symbol_key
                    exchange_candles[plot_key] = candles

                if args.save_parquet_lake:
                    try:
                        parquet_files: list[str] = []
                        for market_key, candles_by_exchange in candles_for_storage.items():
                            parquet_files.extend(
                                save_spot_candles_parquet_lake(
                                    candles_by_exchange=candles_by_exchange,
                                    market=market_key,
                                    lake_root=args.lake_root,
                                )
                            )
                        output["_parquet_files"] = parquet_files
                    except Exception as exc:  # noqa: BLE001
                        output["_parquet_error"] = str(exc)
                        logger.exception("Parquet lake write failed")

                if args.plot:
                    plot_source: dict[str, dict[str, list[SpotCandle]]] = {}
                    for exchange, market, symbol, timeframe in tasks:
                        symbol_key = symbol.upper()
                        if multi_market and multi_timeframe:
                            plot_key = f"{market}_{symbol_key}__{timeframe}"
                        elif multi_market:
                            plot_key = f"{market}_{symbol_key}"
                        elif multi_timeframe:
                            plot_key = f"{symbol_key}__{timeframe}"
                        else:
                            plot_key = symbol_key
                        result_key = (exchange, market, symbol, timeframe)
                        fetched = task_results.get(result_key, [])
                        merged_by_open_time = {item.open_time: item for item in fetched}
                        try:
                            storage_symbol = normalize_storage_symbol(
                                exchange=exchange,
                                symbol=symbol,
                                market=market,
                            )
                            lake_candles = load_spot_candles_from_lake(
                                lake_root=args.lake_root,
                                market=market,
                                exchange=exchange,
                                symbol=storage_symbol,
                                timeframe=timeframe,
                            )
                            for item in lake_candles:
                                merged_by_open_time[item.open_time] = item
                        except Exception:  # noqa: BLE001
                            logger.exception(
                                "Failed to load full-history plot source exchange=%s symbol=%s timeframe=%s",
                                exchange,
                                symbol,
                                timeframe,
                            )

                        exchange_plots = plot_source.setdefault(exchange, {})
                        exchange_plots[plot_key] = [merged_by_open_time[key] for key in sorted(merged_by_open_time)]

                    try:
                        saved_paths = save_candle_plots(
                            candles_by_exchange=plot_source,
                            output_dir=args.plot_dir,
                            price_field=cast(PriceField, args.plot_price),
                        )
                        output["_plots"] = saved_paths
                    except Exception as exc:  # noqa: BLE001
                        output["_plot_error"] = str(exc)
                        logger.exception("Plot generation failed")

                if not args.no_json_output:
                    print(json.dumps(output, indent=2))
                logger.info("Command complete: loader")
        except SingleInstanceError as exc:
            logger.warning("Single-instance lock active")
            raise SystemExit(str(exc)) from exc

    elif args.command == "list-spot-timeframes":
        exchanges = cast(list[Exchange], args.exchanges if args.exchanges else [args.exchange])
        output = {exchange: list(list_supported_intervals(exchange=exchange)) for exchange in exchanges}
        print(json.dumps(output, indent=2))
        logger.info("Command complete: list-spot-timeframes")

    elif args.command == "ingest-parquet-to-db":
        config = load_timescale_config_from_env()
        summary = ingest_parquet_to_timescaledb(
            lake_root=args.lake_root,
            config=config,
            batch_size=args.batch_size,
            dataset_types=cast(list[str] | None, args.dataset_types),
        )
        if not args.no_json_output:
            print(json.dumps(summary, indent=2))
        logger.info(
            "Command complete: ingest-parquet-to-db files_scanned=%s files_ingested=%s rows_upserted=%s",
            summary.get("files_scanned", 0),
            summary.get("files_ingested", 0),
            summary.get("rows_upserted", 0),
        )

    elif args.command == "export-combined-df":
        config = load_timescale_config_from_env()
        start_time = _parse_iso_datetime(cast(str | None, args.start_time))
        end_time = _parse_iso_datetime(cast(str | None, args.end_time))
        dataframe = load_combined_dataframe_from_db(
            config=config,
            exchanges=cast(list[str] | None, args.exchanges),
            symbols=cast(list[str] | None, args.symbols),
            timeframes=cast(list[str] | None, args.timeframes),
            instrument_types=cast(list[str] | None, args.instrument_types),
            start_time=start_time,
            end_time=end_time,
            limit=cast(int | None, args.limit),
        )

        output_path = Path(args.output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        if args.format == "parquet":
            dataframe.to_parquet(output_path, index=False)
        else:
            dataframe.to_csv(output_path, index=False)

        export_summary: dict[str, object] = {
            "output": str(output_path.resolve()),
            "format": args.format,
            "rows": int(getattr(dataframe, "shape", (0, 0))[0]),
            "columns": list(getattr(dataframe, "columns", [])),
        }
        if not args.no_json_output:
            print(json.dumps(export_summary, indent=2))
        logger.info(
            "Command complete: export-combined-df output=%s rows=%s",
            export_summary["output"],
            export_summary["rows"],
        )


if __name__ == "__main__":
    main()

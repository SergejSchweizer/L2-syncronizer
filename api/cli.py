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
from typing import cast

from ingestion.lake import (
    load_combined_dataframe_from_lake,
    load_spot_candles_from_lake,
    open_times_in_lake,
    save_spot_candles_parquet_lake,
)
from ingestion.plotting import PriceField, save_candle_plots
from ingestion.spot import (
    Exchange,
    Market,
    SpotCandle,
    fetch_candles_all_history,
    fetch_candles_range,
    interval_to_milliseconds,
    list_supported_intervals,
    normalize_storage_symbol,
    normalize_timeframe,
)

LOGGER_NAME = "crypto_l2_loader"
DEFAULT_LOG_DIR = "/volume1/Temp/logs"
DEFAULT_EXPORT_DIR = "/volume1/Temp/crypto"


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
        logger.warning("Falling back to stderr logging; cannot create log directory '%s'", log_dir)

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

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
    lake_root: str,
) -> list[SpotCandle]:
    """Fetch candles for one symbol.

    Behavior:
    - If no data exists in parquet lake for the symbol/timeframe, fetch full exchange history.
    - If data exists, perform gap-fill up to latest fully closed candle.
    """

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
        return fetch_candles_all_history(
            exchange=exchange,
            symbol=symbol,
            market=market,
            interval=timeframe,
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


def _grouped_export_filename(exchange: str, symbol: str, timeframe: str, file_format: str) -> str:
    """Build export filename for one exchange/symbol/timeframe group."""

    extension = "parquet" if file_format == "parquet" else "csv"
    return f"{exchange}_{symbol}_{timeframe}_full.{extension}"


def _grouped_plot_filename(exchange: str, symbol: str, timeframe: str) -> str:
    """Build plot filename for one exchange/symbol/timeframe group."""

    return f"{exchange}_{symbol}_{timeframe}_full.png"


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

    export_parser = subparsers.add_parser(
        "export-df",
        help="Load combined spot/perp dataset from parquet lake and export as dataframe file",
    )
    export_parser.add_argument(
        "--lake-root",
        default="lake/bronze",
        help="Root directory for parquet lake files",
    )
    export_parser.add_argument(
        "--output",
        default=None,
        help=f"Output directory for dataframe/plot exports. Defaults to {DEFAULT_EXPORT_DIR}.",
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
        help="Suppress JSON summary output from export-df command",
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
                exchanges = cast(list[Exchange], args.exchanges if args.exchanges else [args.exchange])
                markets = cast(list[Market], args.market)
                multi_market = len(markets) > 1
                requested_timeframes = cast(list[str], args.timeframes if args.timeframes else [args.timeframe])
                multi_timeframe = len(requested_timeframes) > 1
                output: dict[str, object] = {}
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
                total_tasks = len(tasks)
                for idx, (exchange, market, symbol, timeframe) in enumerate(tasks, start=1):
                    key = (exchange, market, symbol, timeframe)
                    logger.info(
                            "Fetch start [%s/%s] exchange=%s market=%s symbol=%s timeframe=%s mode=%s",
                        idx,
                        total_tasks,
                        exchange,
                        market,
                        symbol,
                        timeframe,
                            "auto-bootstrap-or-gap-fill",
                    )
                    try:
                        task_results[key] = _fetch_symbol_candles(
                            exchange=exchange,
                            market=market,
                            symbol=symbol,
                            timeframe=timeframe,
                            lake_root=args.lake_root,
                        )
                        logger.info(
                            "Fetch complete [%s/%s] exchange=%s market=%s symbol=%s timeframe=%s candles=%s",
                            idx,
                            total_tasks,
                            exchange,
                            market,
                            symbol,
                            timeframe,
                            len(task_results[key]),
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

    elif args.command == "export-df":
        start_time = _parse_iso_datetime(cast(str | None, args.start_time))
        end_time = _parse_iso_datetime(cast(str | None, args.end_time))
        dataframe = load_combined_dataframe_from_lake(
            lake_root=args.lake_root,
            exchanges=cast(list[str] | None, args.exchanges),
            symbols=cast(list[str] | None, args.symbols),
            timeframes=cast(list[str] | None, args.timeframes),
            instrument_types=cast(list[str] | None, args.instrument_types),
            start_time=start_time,
            end_time=end_time,
            limit=cast(int | None, args.limit),
        )

        output_arg = cast(str | None, args.output)
        output_dir = Path(output_arg) if output_arg else Path(DEFAULT_EXPORT_DIR)
        output_dir.mkdir(parents=True, exist_ok=True)

        grouped = dataframe.groupby(["exchange", "symbol", "timeframe"], dropna=False, sort=True)
        output_paths: list[str] = []
        plot_paths: list[str] = []
        generated_files: list[dict[str, object]] = []
        group_count = 0
        for group_key, group_frame in grouped:
            exchange_value, symbol_value, timeframe_value = group_key
            file_name = _grouped_export_filename(
                exchange=str(exchange_value),
                symbol=str(symbol_value),
                timeframe=str(timeframe_value),
                file_format=cast(str, args.format),
            )
            output_path = output_dir / file_name
            if args.format == "parquet":
                group_frame.to_parquet(output_path, index=False)
            else:
                group_frame.to_csv(output_path, index=False)
            output_paths.append(str(output_path.resolve()))

            candles_for_plot: list[SpotCandle] = []
            sorted_group = group_frame.sort_values(by=["open_time", "instrument_type"], kind="mergesort")
            for row in sorted_group.to_dict(orient="records"):
                open_time = row["open_time"]
                close_time = row["close_time"]
                if not isinstance(open_time, datetime):
                    continue
                if not isinstance(close_time, datetime):
                    continue
                candles_for_plot.append(
                    SpotCandle(
                        exchange=str(exchange_value),
                        symbol=str(symbol_value),
                        interval=str(timeframe_value),
                        open_time=open_time,
                        close_time=close_time,
                        open_price=float(row["open"]),
                        high_price=float(row["high"]),
                        low_price=float(row["low"]),
                        close_price=float(row["close"]),
                        volume=float(row["volume"]),
                        quote_volume=float(row["quote_volume"]),
                        trade_count=int(row["trade_count"]),
                    )
                )

            if candles_for_plot:
                plot_symbol_key = str(Path(_grouped_plot_filename(
                    exchange=str(exchange_value),
                    symbol=str(symbol_value),
                    timeframe=str(timeframe_value),
                )).stem)
                generated_plot_paths = save_candle_plots(
                    candles_by_exchange={str(exchange_value): {plot_symbol_key: candles_for_plot}},
                    output_dir=str(output_dir),
                    price_field="spot",
                )
                if generated_plot_paths:
                    target_plot = output_dir / _grouped_plot_filename(
                        exchange=str(exchange_value),
                        symbol=str(symbol_value),
                        timeframe=str(timeframe_value),
                    )
                    Path(generated_plot_paths[0]).replace(target_plot)
                    plot_paths.append(str(target_plot.resolve()))
            time_start = None
            time_end = None
            if not group_frame.empty and "open_time" in group_frame:
                time_start = group_frame["open_time"].min()
                time_end = group_frame["open_time"].max()

            generated_files.append(
                {
                    "exchange": str(exchange_value),
                    "symbol": str(symbol_value),
                    "timeframe": str(timeframe_value),
                    "data_file": str(output_path.resolve()),
                    "plot_file": str((output_dir / _grouped_plot_filename(
                        exchange=str(exchange_value),
                        symbol=str(symbol_value),
                        timeframe=str(timeframe_value),
                    )).resolve()),
                    "rows": int(getattr(group_frame, "shape", (0, 0))[0]),
                    "start_open_time": time_start.isoformat() if isinstance(time_start, datetime) else None,
                    "end_open_time": time_end.isoformat() if isinstance(time_end, datetime) else None,
                }
            )
            group_count += 1

        export_summary: dict[str, object] = {
            "output_dir": str(output_dir.resolve()),
            "outputs": output_paths,
            "plots": plot_paths,
            "generated_files": generated_files,
            "format": args.format,
            "groups": group_count,
            "rows": int(getattr(dataframe, "shape", (0, 0))[0]),
            "columns": list(getattr(dataframe, "columns", [])),
        }
        if not args.no_json_output:
            print(json.dumps(export_summary, indent=2))
        logger.info(
            "Command complete: export-df outputs=%s rows=%s",
            export_summary["groups"],
            export_summary["rows"],
        )


if __name__ == "__main__":
    main()

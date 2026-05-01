"""Command-line interface for Deribit L2 order book ingestion."""

from __future__ import annotations

import argparse
import json
import logging
from dataclasses import asdict, dataclass
from time import perf_counter
from typing import cast

from api.runtime import (
    SingleInstanceError,
    SingleInstanceLock,
    configure_logging,
    env_bool,
    env_float,
    env_int,
    env_list,
    env_str,
    fetch_concurrency,
    load_env_file,
)
from ingestion.exchanges.deribit_l2 import fetch_order_book_snapshot, normalize_l2_symbol
from ingestion.l2 import L2MinuteBar, L2Snapshot, aggregate_snapshots_to_m1, fetch_l2_snapshots_for_symbols
from ingestion.lake import save_l2_m1_parquet_lake

__all__ = ["SingleInstanceError", "SingleInstanceLock", "build_parser", "main"]

SnapshotsBySymbol = dict[str, list[L2Snapshot]]
RowsByExchange = dict[str, dict[str, list[L2MinuteBar]]]


@dataclass(frozen=True)
class L2AggregationResult:
    """In-memory aggregation output for one CLI run."""

    json_output: dict[str, object]
    rows_by_exchange: RowsByExchange


def build_parser() -> argparse.ArgumentParser:
    """Create the L2 loader CLI parser."""

    parser = argparse.ArgumentParser(description="crypto-l2-loader CLI")
    subparsers = parser.add_subparsers(dest="command", required=True)

    l2_parser = subparsers.add_parser(
        "loader-l2-m1",
        help="Fetch Deribit L2 snapshots and aggregate them to one-minute microstructure bars",
    )
    l2_parser.add_argument("--exchange", choices=["deribit"], default=env_str("L2_INGEST_EXCHANGE", "deribit"))
    l2_parser.add_argument(
        "--symbols",
        nargs="+",
        default=env_list("L2_INGEST_SYMBOLS", ["BTC", "ETH"]),
        type=str,
        help="Symbols to fetch, separated by spaces or commas",
    )
    l2_parser.add_argument(
        "--levels",
        type=int,
        default=env_int("L2_INGEST_LEVELS", 50),
        help="Number of book levels per side to request",
    )
    l2_parser.add_argument(
        "--snapshot-count",
        type=int,
        default=env_int("L2_INGEST_SNAPSHOT_COUNT", 60),
        help="Snapshots per symbol to collect",
    )
    l2_parser.add_argument(
        "--poll-interval-s",
        type=float,
        default=env_float("L2_INGEST_POLL_INTERVAL_S", 1.0),
        help="Sleep interval between polling ticks",
    )
    l2_parser.add_argument(
        "--lake-root",
        default=env_str("L2_INGEST_LAKE_ROOT", "lake/bronze"),
        help="Root directory for parquet lake files",
    )
    l2_parser.add_argument(
        "--max-runtime-s",
        type=float,
        default=env_float("L2_INGEST_MAX_RUNTIME_S", 0.0),
        help="Maximum collection runtime in seconds; 0 disables the budget",
    )
    l2_parser.add_argument(
        "--save-parquet-lake",
        action=argparse.BooleanOptionalAction,
        default=env_bool("L2_INGEST_SAVE_PARQUET_LAKE", False),
        help="Save aggregated L2 M1 rows to parquet lake partitions",
    )
    l2_parser.add_argument(
        "--json-output",
        action=argparse.BooleanOptionalAction,
        default=not env_bool("L2_INGEST_NO_JSON_OUTPUT", False),
        help="Print JSON output",
    )

    validate_parser = subparsers.add_parser(
        "validate-symbols",
        help="Resolve symbols and check whether Deribit returns a usable L2 book",
    )
    validate_parser.add_argument("--exchange", choices=["deribit"], default=env_str("L2_INGEST_EXCHANGE", "deribit"))
    validate_parser.add_argument(
        "--symbols",
        nargs="+",
        default=env_list("L2_INGEST_SYMBOLS", ["BTC", "ETH"]),
        type=str,
        help="Symbols to validate, separated by spaces or commas",
    )
    validate_parser.add_argument(
        "--levels",
        type=int,
        default=1,
        help="Number of book levels per side to request for validation",
    )
    validate_parser.add_argument(
        "--json-output",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Print JSON output",
    )

    return parser


def _serialize_l2_row(item: L2MinuteBar) -> dict[str, object]:
    """Convert an L2 M1 row into a JSON-safe output dictionary."""

    data = asdict(item)
    minute_ts = data["minute_ts"]
    if not hasattr(minute_ts, "isoformat"):
        raise ValueError("minute_ts must be datetime-like")
    data["minute_ts"] = minute_ts.isoformat()
    return data


def _normalize_cli_symbols(values: list[str]) -> list[str]:
    """Normalize CLI symbol values from space- or comma-delimited inputs."""

    symbols: list[str] = []
    for value in values:
        symbols.extend(item.strip().upper() for item in value.replace(",", " ").split() if item.strip())
    if not symbols:
        raise ValueError("at least one symbol is required")
    return symbols


def _format_optional_float(value: float | None) -> str:
    """Format optional float values for stable log output."""

    if value is None:
        return "null"
    return f"{value:.8f}"


def _log_l2_minute_bar_stats(
    logger: logging.Logger,
    row: L2MinuteBar,
    collected_snapshots: int,
    requested_snapshots: int,
) -> None:
    """Write a stats line for one L2 M1 feature bar."""

    status = "partial" if collected_snapshots < requested_snapshots else "complete"
    logger.info(
        "L2 minute stats exchange=%s symbol=%s minute=%s status=%s snapshots_collected=%s "
        "snapshots_requested=%s bar_snapshot_count=%s mid_open=%.8f mid_high=%.8f mid_low=%.8f "
        "mid_close=%.8f spread_bps_mean=%.8f spread_bps_max=%.8f spread_bps_last=%.8f "
        "bid_depth_10_mean=%.8f ask_depth_10_mean=%.8f imbalance_10_mean=%s imbalance_10_last=%s "
        "microprice_close=%s open_interest_last=%s funding_8h_last=%s current_funding_last=%s "
        "fetch_duration_s_mean=%.6f fetch_duration_s_max=%.6f fetch_duration_s_last=%.6f",
        row.exchange,
        row.symbol,
        row.minute_ts.isoformat(),
        status,
        collected_snapshots,
        requested_snapshots,
        row.snapshot_count,
        row.mid_open,
        row.mid_high,
        row.mid_low,
        row.mid_close,
        row.spread_bps_mean,
        row.spread_bps_max,
        row.spread_bps_last,
        row.bid_depth_10_mean,
        row.ask_depth_10_mean,
        _format_optional_float(row.imbalance_10_mean),
        _format_optional_float(row.imbalance_10_last),
        _format_optional_float(row.microprice_close),
        _format_optional_float(row.open_interest_last),
        _format_optional_float(row.funding_8h_last),
        _format_optional_float(row.current_funding_last),
        row.fetch_duration_s_mean,
        row.fetch_duration_s_max,
        row.fetch_duration_s_last,
    )


def _log_l2_empty_symbol_stats(
    logger: logging.Logger,
    exchange: str,
    symbol: str,
    collected_snapshots: int,
    requested_snapshots: int,
) -> None:
    """Write a stats line when no M1 feature bars are produced for a symbol."""

    status = "partial" if collected_snapshots < requested_snapshots else "no_valid_bars"
    logger.info(
        "L2 minute stats exchange=%s symbol=%s status=%s snapshots_collected=%s "
        "snapshots_requested=%s bars=0",
        exchange,
        symbol,
        status,
        collected_snapshots,
        requested_snapshots,
    )


def _log_l2_run_summary(
    logger: logging.Logger,
    exchange: str,
    symbols: list[str],
    snapshots_by_symbol: dict[str, list[L2Snapshot]],
    rows_by_exchange: dict[str, dict[str, list[L2MinuteBar]]],
    requested_snapshots: int,
    parquet_files: list[str],
    elapsed_s: float,
    parquet_error: str | None = None,
) -> None:
    """Write a compact run-level L2 ingestion summary."""

    collected_total = sum(len(snapshots_by_symbol.get(symbol.upper(), [])) for symbol in symbols)
    requested_total = requested_snapshots * len(symbols)
    bars_total = sum(len(rows_by_exchange.get(exchange, {}).get(symbol.upper(), [])) for symbol in symbols)
    status = "partial" if collected_total < requested_total else "complete"
    if parquet_error is not None:
        status = "parquet_error"
    logger.info(
        "L2 run summary exchange=%s symbols=%s status=%s elapsed_s=%.3f snapshots_collected=%s "
        "snapshots_requested=%s bars=%s parquet_files=%s parquet_error=%s",
        exchange,
        ",".join(symbol.upper() for symbol in symbols),
        status,
        elapsed_s,
        collected_total,
        requested_total,
        bars_total,
        len(parquet_files),
        parquet_error or "none",
    )


def _aggregate_l2_rows(
    exchange: str,
    symbols: list[str],
    snapshots_by_symbol: SnapshotsBySymbol,
    requested_snapshots: int,
    logger: logging.Logger,
) -> L2AggregationResult:
    """Aggregate snapshots for all requested symbols and build CLI output."""

    output: dict[str, object] = {exchange: {}}
    rows_by_exchange: RowsByExchange = {exchange: {}}
    exchange_output = cast(dict[str, object], output[exchange])

    for symbol in symbols:
        symbol_key = symbol.upper()
        snapshots = snapshots_by_symbol.get(symbol_key, [])
        _log_partial_snapshot_warning(
            logger=logger,
            symbol=symbol_key,
            collected_snapshots=len(snapshots),
            requested_snapshots=requested_snapshots,
        )
        rows = aggregate_snapshots_to_m1(snapshots)
        exchange_output[symbol_key] = [_serialize_l2_row(item) for item in rows]
        rows_by_exchange[exchange][symbol_key] = rows
        _log_l2_symbol_stats(
            logger=logger,
            exchange=exchange,
            symbol=symbol_key,
            rows=rows,
            collected_snapshots=len(snapshots),
            requested_snapshots=requested_snapshots,
        )

    return L2AggregationResult(json_output=output, rows_by_exchange=rows_by_exchange)


def _log_partial_snapshot_warning(
    logger: logging.Logger,
    symbol: str,
    collected_snapshots: int,
    requested_snapshots: int,
) -> None:
    """Log a warning when the run collected fewer snapshots than requested."""

    if collected_snapshots >= requested_snapshots:
        return
    logger.warning(
        "L2 run collected partial snapshots symbol=%s collected=%s requested=%s",
        symbol,
        collected_snapshots,
        requested_snapshots,
    )


def _log_l2_symbol_stats(
    logger: logging.Logger,
    exchange: str,
    symbol: str,
    rows: list[L2MinuteBar],
    collected_snapshots: int,
    requested_snapshots: int,
) -> None:
    """Log per-symbol aggregation statistics."""

    if not rows:
        _log_l2_empty_symbol_stats(
            logger=logger,
            exchange=exchange,
            symbol=symbol,
            collected_snapshots=collected_snapshots,
            requested_snapshots=requested_snapshots,
        )
        return

    for row in rows:
        _log_l2_minute_bar_stats(
            logger=logger,
            row=row,
            collected_snapshots=collected_snapshots,
            requested_snapshots=requested_snapshots,
        )


def _persist_l2_rows(
    rows_by_exchange: RowsByExchange,
    lake_root: str,
    enabled: bool,
    output: dict[str, object],
    logger: logging.Logger,
) -> tuple[list[str], str | None]:
    """Persist L2 rows when requested and annotate the CLI output."""

    if not enabled:
        return [], None

    try:
        parquet_files = save_l2_m1_parquet_lake(
            rows_by_exchange=rows_by_exchange,
            lake_root=lake_root,
        )
        output["_parquet_files"] = parquet_files
        return parquet_files, None
    except Exception as exc:  # noqa: BLE001
        parquet_error = str(exc)
        output["_parquet_error"] = parquet_error
        logger.exception("L2 M1 parquet write failed")
        return [], parquet_error


def _estimated_poll_runtime_s(snapshot_count: int, poll_interval_s: float) -> float:
    """Estimate runtime spent sleeping between polling ticks."""

    return max(0, snapshot_count - 1) * poll_interval_s


def _warn_for_long_poll_schedule(
    logger: logging.Logger,
    snapshot_count: int,
    poll_interval_s: float,
    max_runtime_s: float,
) -> None:
    """Warn when polling settings are likely to collide with minute cron runs."""

    estimated_s = _estimated_poll_runtime_s(snapshot_count=snapshot_count, poll_interval_s=poll_interval_s)
    if max_runtime_s > 0 and estimated_s >= max_runtime_s:
        logger.warning(
            "L2 polling sleep budget may exceed max runtime estimated_sleep_s=%.3f max_runtime_s=%.3f",
            estimated_s,
            max_runtime_s,
        )
    if estimated_s >= 60:
        logger.warning(
            "L2 polling sleep budget is at least one minute estimated_sleep_s=%.3f; cron runs may overlap",
            estimated_s,
        )


def _run_loader_l2_m1(args: argparse.Namespace, logger: logging.Logger) -> None:
    """Run L2 snapshot collection and M1 aggregation."""

    try:
        with SingleInstanceLock(".run/crypto-l2-loader-l2.lock"):
            started_at = perf_counter()
            exchange = cast(str, args.exchange)
            symbols = _normalize_cli_symbols(cast(list[str], args.symbols))
            requested_snapshots = int(args.snapshot_count)
            max_runtime_s = float(args.max_runtime_s)
            _warn_for_long_poll_schedule(
                logger=logger,
                snapshot_count=requested_snapshots,
                poll_interval_s=float(args.poll_interval_s),
                max_runtime_s=max_runtime_s,
            )
            snapshots_by_symbol = fetch_l2_snapshots_for_symbols(
                exchange=exchange,
                symbols=symbols,
                depth=int(args.levels),
                snapshot_count=requested_snapshots,
                poll_interval_s=float(args.poll_interval_s),
                max_runtime_s=max_runtime_s if max_runtime_s > 0 else None,
                concurrency=fetch_concurrency(),
            )

            aggregation = _aggregate_l2_rows(
                exchange=exchange,
                symbols=symbols,
                snapshots_by_symbol=snapshots_by_symbol,
                requested_snapshots=requested_snapshots,
                logger=logger,
            )
            parquet_files, parquet_error = _persist_l2_rows(
                rows_by_exchange=aggregation.rows_by_exchange,
                lake_root=cast(str, args.lake_root),
                enabled=bool(args.save_parquet_lake),
                output=aggregation.json_output,
                logger=logger,
            )

            if bool(args.json_output):
                print(json.dumps(aggregation.json_output, indent=2))
            _log_l2_run_summary(
                logger=logger,
                exchange=exchange,
                symbols=symbols,
                snapshots_by_symbol=snapshots_by_symbol,
                rows_by_exchange=aggregation.rows_by_exchange,
                requested_snapshots=requested_snapshots,
                parquet_files=parquet_files,
                elapsed_s=perf_counter() - started_at,
                parquet_error=parquet_error,
            )
    except SingleInstanceError as exc:
        logger.warning("Single-instance lock active for L2 loader")
        raise SystemExit(str(exc)) from exc


def _validate_symbol(symbol: str, depth: int) -> dict[str, object]:
    """Validate one Deribit L2 symbol by fetching a shallow order book."""

    normalized_symbol = normalize_l2_symbol(symbol)
    try:
        snapshot = fetch_order_book_snapshot(symbol=symbol, depth=depth)
        bids = snapshot["bids"]
        asks = snapshot["asks"]
        valid_book = (
            isinstance(bids, list)
            and isinstance(asks, list)
            and bool(bids)
            and bool(asks)
            and bids[0][0] > 0
            and asks[0][0] > 0
            and bids[0][0] < asks[0][0]
        )
        return {
            "symbol": symbol,
            "normalized_symbol": snapshot["symbol"],
            "valid_book": valid_book,
            "bid_levels": len(bids) if isinstance(bids, list) else 0,
            "ask_levels": len(asks) if isinstance(asks, list) else 0,
            "error": None,
        }
    except Exception as exc:  # noqa: BLE001
        return {
            "symbol": symbol,
            "normalized_symbol": normalized_symbol,
            "valid_book": False,
            "bid_levels": 0,
            "ask_levels": 0,
            "error": str(exc),
        }


def _run_validate_symbols(args: argparse.Namespace, logger: logging.Logger) -> None:
    """Run symbol alias and order book validation."""

    symbols = _normalize_cli_symbols(cast(list[str], args.symbols))
    depth = int(args.levels)
    if depth <= 0:
        raise ValueError("levels must be positive")

    results = [_validate_symbol(symbol=symbol, depth=depth) for symbol in symbols]
    output = {
        "exchange": cast(str, args.exchange),
        "symbols": results,
        "all_valid": all(bool(item["valid_book"]) for item in results),
    }
    if bool(args.json_output):
        print(json.dumps(output, indent=2))
    logger.info(
        "Symbol validation complete exchange=%s symbols=%s all_valid=%s",
        args.exchange,
        ",".join(symbols),
        output["all_valid"],
    )


def main() -> None:
    """CLI entrypoint."""

    load_env_file()
    parser = build_parser()
    args = parser.parse_args()
    logger = configure_logging(module_name=str(args.command))
    if args.command == "loader-l2-m1":
        _run_loader_l2_m1(args=args, logger=logger)
    elif args.command == "validate-symbols":
        _run_validate_symbols(args=args, logger=logger)


if __name__ == "__main__":
    main()

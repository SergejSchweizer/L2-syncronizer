"""Command-line interface for Deribit L2 order book ingestion."""

from __future__ import annotations

import argparse
import json
import logging
from dataclasses import asdict
from time import perf_counter
from typing import cast

from api.constants import (
    BRONZE_BUILDER_COMMAND,
    GOLD_BUILDER_COMMAND,
    SILVER_BUILDER_COMMAND,
    VALIDATE_SYMBOLS_COMMAND,
)
from api.runtime import (
    configure_logging,
    fetch_concurrency,
)
from ingestion.config import (
    Config,
    config_bool,
    config_float,
    config_int,
    config_section,
    config_str,
    config_str_list,
    load_config,
)
from ingestion.exchanges.deribit_l2 import fetch_order_book_snapshot, normalize_l2_symbol
from ingestion.gold import transform_l2_silver_to_gold
from ingestion.l2 import L2Snapshot, fetch_l2_snapshots_for_symbols
from ingestion.lake import save_l2_snapshot_parquet_lake
from ingestion.silver import transform_l2_bronze_to_silver

__all__ = ["build_parser", "main"]

SnapshotsBySymbol = dict[str, list[L2Snapshot]]


def _boolean_optional_flag(
    parser: argparse.ArgumentParser,
    name: str,
    default: bool,
    help_text: str,
) -> None:
    parser.add_argument(
        f"--{name}",
        action=argparse.BooleanOptionalAction,
        default=default,
        help=help_text,
    )


def build_parser(config: Config | None = None) -> argparse.ArgumentParser:
    """Create the bronze-builder CLI parser."""

    config = config or load_config()
    ingestion_config = config_section(config, "ingestion")
    parser = argparse.ArgumentParser(description="crypto-l2-loader CLI")
    subparsers = parser.add_subparsers(dest="command", required=True)

    l2_parser = subparsers.add_parser(
        BRONZE_BUILDER_COMMAND,
        help="Fetch Deribit L2 snapshots and persist raw rows to the bronze parquet lake",
    )
    l2_parser.add_argument(
        "--exchange",
        choices=["deribit"],
        default=config_str(ingestion_config, "exchange", "deribit"),
    )
    l2_parser.add_argument(
        "--symbols",
        nargs="+",
        default=config_str_list(ingestion_config, "symbols", ["BTC", "ETH"]),
        type=str,
        help="Symbols to fetch, separated by spaces or commas",
    )
    l2_parser.add_argument(
        "--levels",
        type=int,
        default=config_int(ingestion_config, "levels", 50),
        help="Number of book levels per side to request",
    )
    l2_parser.add_argument(
        "--snapshot-count",
        type=int,
        default=config_int(ingestion_config, "snapshot_count", 5),
        help="Polling ticks per symbol to collect per run",
    )
    l2_parser.add_argument(
        "--poll-interval-s",
        type=float,
        default=config_float(ingestion_config, "poll_interval_s", 10.0),
        help="Sleep interval between polling ticks",
    )
    l2_parser.add_argument(
        "--lake-root",
        default=config_str(ingestion_config, "lake_root", "lake/bronze"),
        help="Root directory for parquet lake files",
    )
    l2_parser.add_argument(
        "--max-runtime-s",
        type=float,
        default=config_float(ingestion_config, "max_runtime_s", 50.0),
        help="Maximum collection runtime in seconds; 0 disables the budget",
    )
    l2_parser.add_argument(
        "--save-parquet-lake",
        action=argparse.BooleanOptionalAction,
        default=config_bool(ingestion_config, "save_parquet_lake", False),
        help="Save raw L2 snapshots to bronze parquet lake partitions",
    )
    l2_parser.add_argument(
        "--json-output",
        action=argparse.BooleanOptionalAction,
        default=config_bool(ingestion_config, "json_output", True),
        help="Print JSON output",
    )

    silver_parser = subparsers.add_parser(
        SILVER_BUILDER_COMMAND,
        help="Transform bronze L2 snapshots into monthly silver feature artifacts",
    )
    silver_parser.add_argument(
        "--bronze-lake-root",
        default=config_str(ingestion_config, "lake_root", "lake/bronze"),
        help="Root directory for bronze parquet input files",
    )
    silver_parser.add_argument(
        "--silver-lake-root",
        default=config_str(ingestion_config, "silver_lake_root", "lake/silver"),
        help="Root directory for silver output artifact files",
    )
    silver_parser.add_argument(
        "--depth",
        type=int,
        default=config_int(ingestion_config, "levels", 50),
        help="Expected book depth used for fixed-width silver arrays",
    )
    _boolean_optional_flag(
        silver_parser,
        "plot",
        True,
        "Enable or suppress Silver PNG profile generation",
    )
    _boolean_optional_flag(
        silver_parser,
        "manifest",
        True,
        "Enable or suppress Silver JSON metadata manifest generation",
    )
    _boolean_optional_flag(
        silver_parser,
        "json-output",
        config_bool(ingestion_config, "json_output", True),
        "Print JSON output",
    )

    gold_parser = subparsers.add_parser(
        GOLD_BUILDER_COMMAND,
        help="Transform silver L2 features into per-symbol gold M1 artifacts",
    )
    gold_parser.add_argument(
        "--silver-lake-root",
        default=config_str(ingestion_config, "silver_lake_root", "lake/silver"),
        help="Root directory for silver parquet input files",
    )
    gold_parser.add_argument(
        "--gold-lake-root",
        default=config_str(ingestion_config, "gold_lake_root", "lake/gold"),
        help="Root directory for gold artifact output files",
    )
    gold_parser.add_argument(
        "--expected-snapshots-per-minute",
        type=int,
        default=6,
        help="Expected silver snapshots per minute for quality coverage",
    )
    gold_parser.add_argument(
        "--completeness-threshold",
        type=float,
        default=0.8,
        help="Minimum coverage ratio for a complete minute",
    )
    _boolean_optional_flag(
        gold_parser,
        "fill-missing-minutes",
        False,
        "Fill missing Gold numeric features with adjacent-minute averages",
    )
    gold_parser.add_argument(
        "--fill-policy",
        choices=["neighbor", "hybrid", "kalman"],
        default="neighbor",
        help="Gap-filling policy used when --fill-missing-minutes is enabled",
    )
    _boolean_optional_flag(
        gold_parser,
        "plot",
        True,
        "Enable or suppress Gold PNG profile generation",
    )
    _boolean_optional_flag(
        gold_parser,
        "manifest",
        True,
        "Enable or suppress Gold JSON metadata manifest generation",
    )
    _boolean_optional_flag(
        gold_parser,
        "json-output",
        config_bool(ingestion_config, "json_output", True),
        "Print JSON output",
    )

    validate_parser = subparsers.add_parser(
        VALIDATE_SYMBOLS_COMMAND,
        help="Resolve symbols and check whether Deribit returns a usable L2 book",
    )
    validate_parser.add_argument(
        "--exchange",
        choices=["deribit"],
        default=config_str(ingestion_config, "exchange", "deribit"),
    )
    validate_parser.add_argument(
        "--symbols",
        nargs="+",
        default=config_str_list(ingestion_config, "symbols", ["BTC", "ETH"]),
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


def _serialize_l2_snapshot(item: L2Snapshot) -> dict[str, object]:
    """Convert an L2 snapshot into a JSON-safe output dictionary."""

    data = asdict(item)
    timestamp = data["timestamp"]
    if not hasattr(timestamp, "isoformat"):
        raise ValueError("timestamp must be datetime-like")
    data["timestamp"] = timestamp.isoformat()
    return data


def _normalize_cli_symbols(values: list[str]) -> list[str]:
    """Normalize CLI symbol values from space- or comma-delimited inputs."""

    symbols: list[str] = []
    for value in values:
        symbols.extend(item.strip().upper() for item in value.replace(",", " ").split() if item.strip())
    if not symbols:
        raise ValueError("at least one symbol is required")
    return symbols


def _log_bronze_builder_summary(
    logger: logging.Logger,
    exchange: str,
    symbols: list[str],
    snapshots_by_symbol: dict[str, list[L2Snapshot]],
    requested_snapshots: int,
    parquet_files: list[str],
    elapsed_s: float,
    parquet_error: str | None = None,
) -> None:
    """Write a compact run-level bronze-builder summary."""

    collected_total = sum(len(snapshots_by_symbol.get(symbol.upper(), [])) for symbol in symbols)
    requested_total = requested_snapshots * len(symbols)
    status = "partial" if collected_total < requested_total else "complete"
    if parquet_error is not None:
        status = "parquet_error"
    logger.info(
        "bronze-builder run summary exchange=%s symbols=%s status=%s elapsed_s=%.3f snapshots_collected=%s "
        "snapshots_requested=%s parquet_files=%s parquet_error=%s",
        exchange,
        ",".join(symbol.upper() for symbol in symbols),
        status,
        elapsed_s,
        collected_total,
        requested_total,
        len(parquet_files),
        parquet_error or "none",
    )


def _build_snapshot_output(
    exchange: str,
    symbols: list[str],
    snapshots_by_symbol: SnapshotsBySymbol,
    requested_snapshots: int,
    logger: logging.Logger,
) -> dict[str, object]:
    """Build JSON output for raw bronze snapshots and log per-symbol collection status."""

    output: dict[str, object] = {exchange: {}}
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
        exchange_output[symbol_key] = [_serialize_l2_snapshot(item) for item in snapshots]
        logger.info(
            "bronze-builder snapshot stats exchange=%s symbol=%s snapshots_collected=%s snapshots_requested=%s",
            exchange,
            symbol_key,
            len(snapshots),
            requested_snapshots,
        )

    return output


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
        "bronze-builder collected partial snapshots symbol=%s collected=%s requested=%s",
        symbol,
        collected_snapshots,
        requested_snapshots,
    )


def _persist_bronze_snapshots(
    snapshots_by_symbol: SnapshotsBySymbol,
    lake_root: str,
    depth: int,
    enabled: bool,
    output: dict[str, object],
    logger: logging.Logger,
) -> tuple[list[str], str | None]:
    """Persist raw L2 snapshots when requested and annotate the CLI output."""

    if not enabled:
        return [], None

    try:
        parquet_files = save_l2_snapshot_parquet_lake(
            snapshots_by_symbol=snapshots_by_symbol,
            lake_root=lake_root,
            depth=depth,
        )
        output["_parquet_files"] = parquet_files
        return parquet_files, None
    except Exception as exc:  # noqa: BLE001
        parquet_error = str(exc)
        output["_parquet_error"] = parquet_error
        logger.exception("bronze-builder raw snapshot parquet write failed")
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
    """Warn when bronze-builder polling settings are likely to collide with minute cron runs."""

    estimated_s = _estimated_poll_runtime_s(snapshot_count=snapshot_count, poll_interval_s=poll_interval_s)
    if max_runtime_s > 0 and estimated_s >= max_runtime_s:
        logger.warning(
            "bronze-builder polling sleep budget may exceed max runtime estimated_sleep_s=%.3f max_runtime_s=%.3f",
            estimated_s,
            max_runtime_s,
        )
    if estimated_s >= 60:
        logger.warning(
            "bronze-builder polling sleep budget is at least one minute estimated_sleep_s=%.3f; cron runs may overlap",
            estimated_s,
        )


def _run_bronze_builder(args: argparse.Namespace, logger: logging.Logger, config: Config) -> None:
    """Run L2 snapshot collection and optional raw bronze persistence."""

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
        concurrency=fetch_concurrency(config),
    )

    output = _build_snapshot_output(
        exchange=exchange,
        symbols=symbols,
        snapshots_by_symbol=snapshots_by_symbol,
        requested_snapshots=requested_snapshots,
        logger=logger,
    )
    parquet_files, parquet_error = _persist_bronze_snapshots(
        snapshots_by_symbol=snapshots_by_symbol,
        lake_root=cast(str, args.lake_root),
        depth=int(args.levels),
        enabled=bool(args.save_parquet_lake),
        output=output,
        logger=logger,
    )

    if bool(args.json_output):
        print(json.dumps(output, indent=2))
    _log_bronze_builder_summary(
        logger=logger,
        exchange=exchange,
        symbols=symbols,
        snapshots_by_symbol=snapshots_by_symbol,
        requested_snapshots=requested_snapshots,
        parquet_files=parquet_files,
        elapsed_s=perf_counter() - started_at,
        parquet_error=parquet_error,
    )


def _run_silver_builder(args: argparse.Namespace, logger: logging.Logger) -> None:
    """Run Bronze-to-Silver L2 feature transformation."""

    started_at = perf_counter()
    bronze_lake_root = cast(str, args.bronze_lake_root)
    silver_lake_root = cast(str, args.silver_lake_root)
    depth = int(args.depth)
    written_files = transform_l2_bronze_to_silver(
        bronze_lake_root=bronze_lake_root,
        silver_lake_root=silver_lake_root,
        depth=depth,
        plot=bool(args.plot),
        manifest=bool(args.manifest),
    )
    elapsed_s = perf_counter() - started_at
    output = {
        "command": SILVER_BUILDER_COMMAND,
        "status": "complete",
        "bronze_lake_root": bronze_lake_root,
        "silver_lake_root": silver_lake_root,
        "depth": depth,
        "artifact_files": written_files,
    }
    if bool(args.json_output):
        print(json.dumps(output, indent=2))
    logger.info(
        "silver-builder run summary status=complete elapsed_s=%.3f bronze_lake_root=%s "
        "silver_lake_root=%s depth=%s artifact_files=%s",
        elapsed_s,
        bronze_lake_root,
        silver_lake_root,
        depth,
        len(written_files),
    )


def _run_gold_builder(args: argparse.Namespace, logger: logging.Logger) -> None:
    """Run Silver-to-Gold M1 L2 feature transformation."""

    started_at = perf_counter()
    silver_lake_root = cast(str, args.silver_lake_root)
    gold_lake_root = cast(str, args.gold_lake_root)
    expected_snapshots_per_minute = int(args.expected_snapshots_per_minute)
    completeness_threshold = float(args.completeness_threshold)
    written_files = transform_l2_silver_to_gold(
        silver_lake_root=silver_lake_root,
        gold_lake_root=gold_lake_root,
        expected_snapshots_per_minute=expected_snapshots_per_minute,
        completeness_threshold=completeness_threshold,
        plot=bool(args.plot),
        manifest=bool(args.manifest),
        fill_missing_minutes=bool(args.fill_missing_minutes),
        fill_policy=cast(str, args.fill_policy),
    )
    elapsed_s = perf_counter() - started_at
    output = {
        "command": GOLD_BUILDER_COMMAND,
        "status": "complete",
        "silver_lake_root": silver_lake_root,
        "gold_lake_root": gold_lake_root,
        "expected_snapshots_per_minute": expected_snapshots_per_minute,
        "completeness_threshold": completeness_threshold,
        "fill_missing_minutes": bool(args.fill_missing_minutes),
        "fill_policy": cast(str, args.fill_policy),
        "artifact_files": written_files,
    }
    if bool(args.json_output):
        print(json.dumps(output, indent=2))
    logger.info(
        "gold-builder run summary status=complete elapsed_s=%.3f silver_lake_root=%s "
        "gold_lake_root=%s expected_snapshots_per_minute=%s completeness_threshold=%.3f "
        "fill_missing_minutes=%s fill_policy=%s artifact_files=%s",
        elapsed_s,
        silver_lake_root,
        gold_lake_root,
        expected_snapshots_per_minute,
        completeness_threshold,
        bool(args.fill_missing_minutes),
        cast(str, args.fill_policy),
        len(written_files),
    )


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

    config = load_config()
    parser = build_parser(config)
    args = parser.parse_args()
    logger = configure_logging(module_name=str(args.command), config=config)
    if args.command == BRONZE_BUILDER_COMMAND:
        _run_bronze_builder(args=args, logger=logger, config=config)
    elif args.command == SILVER_BUILDER_COMMAND:
        _run_silver_builder(args=args, logger=logger)
    elif args.command == GOLD_BUILDER_COMMAND:
        _run_gold_builder(args=args, logger=logger)
    elif args.command == VALIDATE_SYMBOLS_COMMAND:
        _run_validate_symbols(args=args, logger=logger)


if __name__ == "__main__":
    main()

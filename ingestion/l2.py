"""L2 snapshot ingestion utilities."""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from time import monotonic
from typing import cast

from ingestion.exchanges.deribit_l2 import fetch_order_book_snapshot

LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class L2FetchConfig:
    """Runtime configuration for bounded L2 snapshot collection."""

    exchange: str
    symbols: list[str]
    depth: int
    snapshot_count: int
    poll_interval_s: float
    max_runtime_s: float | None = None
    concurrency: int | None = None


@dataclass(frozen=True)
class L2Snapshot:
    """One normalized L2 snapshot.

    Example:
        ```python
        from datetime import UTC, datetime
        from ingestion.l2 import L2Snapshot

        snapshot = L2Snapshot(
            exchange="deribit",
            symbol="BTC-PERPETUAL",
            timestamp=datetime(2026, 1, 1, 0, 0, tzinfo=UTC),
            fetch_duration_s=0.25,
            bids=[(100.0, 10.0)],
            asks=[(100.1, 9.0)],
            mark_price=100.05,
            index_price=100.0,
            open_interest=1000.0,
            funding_8h=0.0001,
            current_funding=0.00001,
        )
        ```
    """

    exchange: str
    symbol: str
    timestamp: datetime
    fetch_duration_s: float
    bids: list[tuple[float, float]]
    asks: list[tuple[float, float]]
    mark_price: float | None
    index_price: float | None
    open_interest: float | None
    funding_8h: float | None
    current_funding: float | None


def fetch_l2_snapshots_for_symbols(
    exchange: str,
    symbols: list[str],
    depth: int,
    snapshot_count: int,
    poll_interval_s: float,
    max_runtime_s: float | None = None,
    concurrency: int | None = None,
) -> dict[str, list[L2Snapshot]]:
    """Collect L2 snapshots for all symbols using async polling ticks."""

    config = L2FetchConfig(
        exchange=exchange,
        symbols=symbols,
        depth=depth,
        snapshot_count=snapshot_count,
        poll_interval_s=poll_interval_s,
        max_runtime_s=max_runtime_s,
        concurrency=concurrency,
    )
    return asyncio.run(
        fetch_l2_snapshots_for_symbols_async(
            exchange=config.exchange,
            symbols=config.symbols,
            depth=config.depth,
            snapshot_count=config.snapshot_count,
            poll_interval_s=config.poll_interval_s,
            max_runtime_s=config.max_runtime_s,
            concurrency=config.concurrency,
        )
    )


async def fetch_l2_snapshots_for_symbols_async(
    exchange: str,
    symbols: list[str],
    depth: int,
    snapshot_count: int,
    poll_interval_s: float,
    max_runtime_s: float | None = None,
    concurrency: int | None = None,
) -> dict[str, list[L2Snapshot]]:
    """Collect L2 snapshots for all symbols concurrently on each polling tick.

    This keeps total runtime bounded by polling cycles rather than multiplying
    runtime by the number of symbols.
    """

    config = L2FetchConfig(
        exchange=exchange,
        symbols=symbols,
        depth=depth,
        snapshot_count=snapshot_count,
        poll_interval_s=poll_interval_s,
        max_runtime_s=max_runtime_s,
        concurrency=concurrency,
    )
    _validate_fetch_config(config)

    deadline = _deadline_from_config(config)
    snapshots_by_symbol: dict[str, list[L2Snapshot]] = {symbol.upper(): [] for symbol in config.symbols}
    semaphore = asyncio.Semaphore(max(1, config.concurrency or len(config.symbols)))

    for index in range(config.snapshot_count):
        if _deadline_reached(deadline):
            break

        tick_snapshots = await _collect_l2_tick(
            symbols=config.symbols,
            depth=config.depth,
            deadline=deadline,
            semaphore=semaphore,
        )
        _append_tick_snapshots(snapshots_by_symbol=snapshots_by_symbol, tick_snapshots=tick_snapshots)

        if index >= config.snapshot_count - 1 or config.poll_interval_s <= 0:
            continue

        slept = await _sleep_between_ticks(poll_interval_s=config.poll_interval_s, deadline=deadline)
        if not slept:
            break

    return snapshots_by_symbol


def _validate_fetch_config(config: L2FetchConfig) -> None:
    """Validate L2 fetch configuration before network calls begin."""

    if config.exchange != "deribit":
        raise ValueError(f"Unsupported exchange '{config.exchange}'")
    if not config.symbols:
        raise ValueError("symbols must not be empty")
    if config.depth <= 0:
        raise ValueError("depth must be positive")
    if config.snapshot_count <= 0:
        raise ValueError("snapshot_count must be positive")
    if config.poll_interval_s < 0:
        raise ValueError("poll_interval_s must be >= 0")
    if config.max_runtime_s is not None and config.max_runtime_s <= 0:
        raise ValueError("max_runtime_s must be positive when set")
    if config.concurrency is not None and config.concurrency <= 0:
        raise ValueError("concurrency must be positive when set")


def _deadline_from_config(config: L2FetchConfig) -> float | None:
    """Return monotonic deadline for a config, or ``None`` when disabled."""

    if config.max_runtime_s is None:
        return None
    return monotonic() + config.max_runtime_s


def _deadline_reached(deadline: float | None) -> bool:
    """Return whether a monotonic deadline has expired."""

    return deadline is not None and monotonic() >= deadline


async def _collect_l2_tick(
    symbols: list[str],
    depth: int,
    deadline: float | None,
    semaphore: asyncio.Semaphore,
) -> list[tuple[str, L2Snapshot]]:
    """Fetch one concurrent polling tick for all symbols."""

    tick_results = await asyncio.gather(
        *[_fetch_l2_symbol(symbol=symbol, depth=depth, deadline=deadline, semaphore=semaphore) for symbol in symbols],
        return_exceptions=True,
    )
    snapshots: list[tuple[str, L2Snapshot]] = []
    for symbol, result in zip(symbols, tick_results, strict=True):
        if result is None:
            continue
        if isinstance(result, BaseException):
            LOGGER.warning("L2 snapshot fetch failed symbol=%s error=%s", symbol.upper(), result)
            continue
        snapshots.append(result)
    return snapshots


async def _fetch_l2_symbol(
    symbol: str,
    depth: int,
    deadline: float | None,
    semaphore: asyncio.Semaphore,
) -> tuple[str, L2Snapshot] | None:
    """Fetch and normalize one symbol snapshot inside a bounded async tick."""

    async with semaphore:
        if _deadline_reached(deadline):
            return None
        started_at = monotonic()
        raw = await asyncio.to_thread(fetch_order_book_snapshot, symbol=symbol, depth=depth)
        return symbol.upper(), _snapshot_from_raw(raw=raw, fetch_duration_s=monotonic() - started_at)


def _append_tick_snapshots(
    snapshots_by_symbol: dict[str, list[L2Snapshot]],
    tick_snapshots: list[tuple[str, L2Snapshot]],
) -> None:
    """Append normalized tick snapshots into the per-symbol collection."""

    for symbol_key, snapshot in tick_snapshots:
        snapshots_by_symbol.setdefault(symbol_key, []).append(snapshot)


async def _sleep_between_ticks(poll_interval_s: float, deadline: float | None) -> bool:
    """Sleep between ticks, respecting deadline; return whether another tick may run."""

    if deadline is None:
        await asyncio.sleep(poll_interval_s)
        return True

    remaining_s = deadline - monotonic()
    if remaining_s <= 0:
        return False
    await asyncio.sleep(min(poll_interval_s, remaining_s))
    return not _deadline_reached(deadline)


def _snapshot_from_raw(raw: dict[str, object], fetch_duration_s: float = 0.0) -> L2Snapshot:
    """Convert normalized adapter payload into ``L2Snapshot``."""

    timestamp_ms = int(cast(int | float, raw["timestamp_ms"]))
    timestamp = datetime.fromtimestamp(timestamp_ms / 1000, tz=UTC)
    bids = [(float(price), float(amount)) for price, amount in cast(list[tuple[float, float]], raw["bids"])]
    asks = [(float(price), float(amount)) for price, amount in cast(list[tuple[float, float]], raw["asks"])]

    return L2Snapshot(
        exchange=str(raw["exchange"]),
        symbol=str(raw["symbol"]),
        timestamp=timestamp,
        fetch_duration_s=fetch_duration_s,
        bids=bids,
        asks=asks,
        mark_price=_optional_float(raw.get("mark_price")),
        index_price=_optional_float(raw.get("index_price")),
        open_interest=_optional_float(raw.get("open_interest")),
        funding_8h=_optional_float(raw.get("funding_8h")),
        current_funding=_optional_float(raw.get("current_funding")),
    )


def l2_snapshot_record(
    snapshot: L2Snapshot,
    depth: int,
    run_id: str,
    ingested_at: datetime,
    source: str = "rest_order_book",
) -> dict[str, object]:
    """Convert ``L2Snapshot`` to raw bronze parquet row format."""

    return {
        "schema_version": "v1",
        "dataset_type": "l2_snapshot",
        "exchange": snapshot.exchange,
        "symbol": snapshot.symbol,
        "instrument_type": "perp",
        "event_time": snapshot.timestamp,
        "ingested_at": ingested_at,
        "run_id": run_id,
        "source": source,
        "depth": depth,
        "fetch_duration_s": snapshot.fetch_duration_s,
        "bids": [{"price": price, "amount": amount} for price, amount in snapshot.bids],
        "asks": [{"price": price, "amount": amount} for price, amount in snapshot.asks],
        "mark_price": snapshot.mark_price,
        "index_price": snapshot.index_price,
        "open_interest": snapshot.open_interest,
        "funding_8h": snapshot.funding_8h,
        "current_funding": snapshot.current_funding,
    }


def l2_snapshot_partition_key(
    snapshot: L2Snapshot,
    depth: int,
    source: str,
) -> tuple[str, str, str, int, str, str, str]:
    """Build raw bronze partition key for an L2 snapshot."""

    return (
        snapshot.exchange,
        "perp",
        snapshot.symbol,
        depth,
        source,
        snapshot.timestamp.strftime("%Y-%m"),
        snapshot.timestamp.strftime("%Y-%m-%d"),
    )


def _optional_float(value: object) -> float | None:
    """Convert optional numeric values to floats."""

    if value is None:
        return None
    return float(cast(int | float, value))

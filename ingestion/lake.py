"""Parquet lake writer for raw L2 snapshots."""

from __future__ import annotations

import concurrent.futures
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import cast

from ingestion.file_lock import FileLock
from ingestion.l2 import (
    L2Snapshot,
    l2_snapshot_partition_key,
    l2_snapshot_record,
)

SnapshotPartitionKey = tuple[str, str, str, int, str, str, str]
SnapshotNaturalKey = tuple[str, str, str, int, str, datetime]


def utc_run_id() -> str:
    """Create a UTC run identifier for lake writes."""

    return datetime.now(UTC).strftime("%Y%m%dT%H%M%S%fZ")


def snapshot_partition_path(lake_root: str, key: SnapshotPartitionKey) -> Path:
    """Return the bronze destination directory for one raw L2 snapshot partition."""

    exchange, instrument_type, symbol, depth, source, month_partition, date_partition = key
    return (
        Path(lake_root)
        / "dataset_type=l2_snapshot"
        / f"exchange={exchange}"
        / f"instrument_type={instrument_type}"
        / f"symbol={symbol}"
        / f"depth={depth}"
        / f"source={source}"
        / f"month={month_partition}"
        / f"date={date_partition}"
    )


def snapshot_record_natural_key(record: dict[str, object]) -> SnapshotNaturalKey:
    """Build the idempotent natural key for one raw L2 snapshot row."""

    event_time = record["event_time"]
    if not isinstance(event_time, datetime):
        raise ValueError("event_time must be datetime")
    return (
        str(record["exchange"]),
        str(record["instrument_type"]),
        str(record["symbol"]),
        int(cast(int, record["depth"])),
        str(record["source"]),
        event_time,
    )


def merge_and_deduplicate_snapshot_rows(
    existing: list[dict[str, object]],
    new: list[dict[str, object]],
) -> list[dict[str, object]]:
    """Merge old and new raw snapshot rows while keeping the latest duplicate key."""

    merged: dict[SnapshotNaturalKey, dict[str, object]] = {}
    for record in existing:
        merged[snapshot_record_natural_key(record)] = record
    for record in new:
        merged[snapshot_record_natural_key(record)] = record

    rows = list(merged.values())
    rows.sort(key=lambda item: cast(datetime, item["event_time"]))
    return rows


def save_l2_snapshot_parquet_lake(
    snapshots_by_symbol: dict[str, list[L2Snapshot]],
    lake_root: str,
    depth: int,
    source: str = "rest_order_book",
) -> list[str]:
    """Save raw L2 snapshots to daily bronze parquet lake partitions."""

    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ImportError as exc:
        raise RuntimeError("pyarrow is required for parquet lake output. Install project dependencies.") from exc

    run_id = utc_run_id()
    ingested_at = datetime.now(UTC)

    grouped: defaultdict[SnapshotPartitionKey, list[dict[str, object]]] = defaultdict(list)
    for snapshots in snapshots_by_symbol.values():
        for snapshot in snapshots:
            key = l2_snapshot_partition_key(snapshot=snapshot, depth=depth, source=source)
            grouped[key].append(
                l2_snapshot_record(
                    snapshot=snapshot,
                    depth=depth,
                    run_id=run_id,
                    ingested_at=ingested_at,
                    source=source,
                )
            )

    def _write_one_partition(key: SnapshotPartitionKey, rows: list[dict[str, object]]) -> str:
        part_dir = snapshot_partition_path(lake_root=lake_root, key=key)
        part_dir.mkdir(parents=True, exist_ok=True)
        file_path = part_dir / "data.parquet"
        staging_path = part_dir / f".staging-{run_id}.parquet"
        lock_path = part_dir / ".write.lock"

        with FileLock(lock_path):
            existing_rows: list[dict[str, object]] = []
            if file_path.exists():
                existing_table = pq.ParquetFile(file_path).read()  # type: ignore[no-untyped-call]
                existing_rows = existing_table.to_pylist()

            merged_rows = merge_and_deduplicate_snapshot_rows(existing=existing_rows, new=rows)
            table = pa.Table.from_pylist(merged_rows)
            pq.write_table(table, staging_path)  # type: ignore[no-untyped-call]
            staging_path.replace(file_path)
        return str(file_path.resolve())

    written_files: list[str] = []
    if grouped:
        max_workers = min(4, len(grouped))
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(_write_one_partition, key, rows) for key, rows in grouped.items()]
            for future in concurrent.futures.as_completed(futures):
                written_files.append(future.result())

    return sorted(written_files)

"""Parquet-to-TimescaleDB ingestion utilities."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any

DatasetType = str
FileSignature = tuple[int, int]


@dataclass(frozen=True)
class TimescaleConfig:
    """Database connection settings for TimescaleDB.

    Example:
        ```python
        from ingestion.timescaledb_loader import TimescaleConfig

        config = TimescaleConfig(
            host="127.0.0.1",
            port=5432,
            user="postgres",
            password="postgres",
            dbname="crypto",
            sslmode="disable",
        )
        ```
    """

    host: str
    port: int
    user: str
    password: str
    dbname: str
    sslmode: str


def _read_env_file(path: Path) -> dict[str, str]:
    """Read simple KEY=VALUE pairs from an env file if present."""

    if not path.exists():
        return {}

    values: dict[str, str] = {}
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()
        if not key:
            continue
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {'"', "'"}:
            value = value[1:-1]
        values[key] = value
    return values


def load_timescale_config_from_env(env_file: str = ".env") -> TimescaleConfig:
    """Load TimescaleDB config from environment variables with .env fallback."""

    file_values = _read_env_file(Path(env_file))

    host = os.getenv("TIMESCALEDB_HOST", file_values.get("TIMESCALEDB_HOST", "127.0.0.1"))
    port_raw = os.getenv("TIMESCALEDB_PORT", file_values.get("TIMESCALEDB_PORT", "5432"))
    user = os.getenv("TIMESCALEDB_USER", file_values.get("TIMESCALEDB_USER", "postgres"))
    password = os.getenv("TIMESCALEDB_PASSWORD", file_values.get("TIMESCALEDB_PASSWORD", "postgres"))
    dbname = os.getenv("TIMESCALEDB_DB", file_values.get("TIMESCALEDB_DB", "postgres"))
    sslmode = os.getenv(
        "TIMESCALEDB_SSLMODE",
        os.getenv(
            "PGSSLMODE",
            file_values.get("TIMESCALEDB_SSLMODE", file_values.get("PGSSLMODE", "disable")),
        ),
    )

    try:
        port = int(port_raw)
    except ValueError as exc:
        raise ValueError("TIMESCALEDB_PORT must be an integer") from exc

    return TimescaleConfig(
        host=host,
        port=port,
        user=user,
        password=password,
        dbname=dbname,
        sslmode=sslmode,
    )


def parquet_file_signature(path: Path) -> FileSignature:
    """Build stable signature for one parquet file."""

    stat = path.stat()
    return (stat.st_size, stat.st_mtime_ns)


def list_parquet_files(lake_root: str, dataset_types: list[DatasetType] | None = None) -> list[Path]:
    """List parquet lake partition files for selected datasets."""

    root = Path(lake_root)
    if not root.exists():
        return []

    selected = set(dataset_types) if dataset_types else {"ohlcv"}
    files: list[Path] = []
    for dataset_type in selected:
        files.extend(
            sorted(
                root.glob(
                    f"dataset_type={dataset_type}/exchange=*/instrument_type=*/symbol=*/timeframe=*/date=*/data.parquet"
                )
            )
        )
    return sorted(set(files))


def _ensure_tables(connection: Any) -> None:
    """Create target and state tables if missing."""

    with connection.cursor() as cursor:
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS market_ohlcv (
                schema_version TEXT NOT NULL,
                dataset_type TEXT NOT NULL,
                exchange TEXT NOT NULL,
                symbol TEXT NOT NULL,
                instrument_type TEXT NOT NULL,
                event_time TIMESTAMPTZ NOT NULL,
                ingested_at TIMESTAMPTZ NOT NULL,
                run_id TEXT NOT NULL,
                source_endpoint TEXT NOT NULL,
                open_time TIMESTAMPTZ NOT NULL,
                close_time TIMESTAMPTZ NOT NULL,
                timeframe TEXT NOT NULL,
                open DOUBLE PRECISION NOT NULL,
                high DOUBLE PRECISION NOT NULL,
                low DOUBLE PRECISION NOT NULL,
                close DOUBLE PRECISION NOT NULL,
                volume DOUBLE PRECISION NOT NULL,
                quote_volume DOUBLE PRECISION NOT NULL,
                trade_count BIGINT NOT NULL,
                extra JSONB NOT NULL,
                PRIMARY KEY (exchange, instrument_type, symbol, timeframe, open_time)
            );
            """
        )
        cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_market_ohlcv_open_time
            ON market_ohlcv (open_time DESC);
            """
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS ingestion_state (
                file_path TEXT PRIMARY KEY,
                file_size BIGINT NOT NULL,
                file_mtime_ns BIGINT NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            """
        )

        # Best-effort hypertable conversion. If TimescaleDB extension is unavailable
        # (for example missing shared library on server), continue with plain Postgres table.
        cursor.execute("SAVEPOINT sp_create_hypertable;")
        try:
            cursor.execute(
                """
                SELECT create_hypertable(
                    'market_ohlcv',
                    'open_time',
                    if_not_exists => TRUE,
                    migrate_data => TRUE
                );
                """
            )
            cursor.execute("RELEASE SAVEPOINT sp_create_hypertable;")
        except Exception:
            cursor.execute("ROLLBACK TO SAVEPOINT sp_create_hypertable;")
            cursor.execute("RELEASE SAVEPOINT sp_create_hypertable;")


def _load_ingestion_state(connection: Any) -> dict[str, FileSignature]:
    """Load ingestion-state signatures indexed by file path."""

    with connection.cursor() as cursor:
        cursor.execute("SELECT file_path, file_size, file_mtime_ns FROM ingestion_state;")
        rows = cursor.fetchall()

    result: dict[str, FileSignature] = {}
    for file_path, file_size, file_mtime_ns in rows:
        result[str(file_path)] = (int(file_size), int(file_mtime_ns))
    return result


def _save_ingestion_state(
    connection: Any,
    file_path: str,
    signature: FileSignature,
) -> None:
    """Upsert one file signature into ingestion_state."""

    file_size, file_mtime_ns = signature
    with connection.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO ingestion_state (file_path, file_size, file_mtime_ns, updated_at)
            VALUES (%s, %s, %s, NOW())
            ON CONFLICT (file_path) DO UPDATE
            SET file_size = EXCLUDED.file_size,
                file_mtime_ns = EXCLUDED.file_mtime_ns,
                updated_at = NOW();
            """,
            (file_path, file_size, file_mtime_ns),
        )


def _json_default(value: Any) -> str:
    """Serialize non-standard JSON values used in parquet payloads."""

    if isinstance(value, (datetime, date)):
        return value.isoformat()
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


def _upsert_rows(connection: Any, rows: list[dict[str, Any]], batch_size: int) -> int:
    """Upsert rows into market_ohlcv in batches."""

    if not rows:
        return 0

    upserted = 0
    with connection.cursor() as cursor:
        for offset in range(0, len(rows), batch_size):
            batch = rows[offset : offset + batch_size]
            params = [
                (
                    str(item["schema_version"]),
                    str(item["dataset_type"]),
                    str(item["exchange"]),
                    str(item["symbol"]),
                    str(item["instrument_type"]),
                    item["event_time"],
                    item["ingested_at"],
                    str(item["run_id"]),
                    str(item["source_endpoint"]),
                    item["open_time"],
                    item["close_time"],
                    str(item["timeframe"]),
                    float(item["open"]),
                    float(item["high"]),
                    float(item["low"]),
                    float(item["close"]),
                    float(item["volume"]),
                    float(item["quote_volume"]),
                    int(item["trade_count"]),
                    json.dumps(item["extra"], default=_json_default),
                )
                for item in batch
            ]

            cursor.executemany(
                """
                INSERT INTO market_ohlcv (
                    schema_version,
                    dataset_type,
                    exchange,
                    symbol,
                    instrument_type,
                    event_time,
                    ingested_at,
                    run_id,
                    source_endpoint,
                    open_time,
                    close_time,
                    timeframe,
                    open,
                    high,
                    low,
                    close,
                    volume,
                    quote_volume,
                    trade_count,
                    extra
                )
                VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
                ON CONFLICT (exchange, instrument_type, symbol, timeframe, open_time) DO UPDATE
                SET schema_version = EXCLUDED.schema_version,
                    dataset_type = EXCLUDED.dataset_type,
                    event_time = EXCLUDED.event_time,
                    ingested_at = EXCLUDED.ingested_at,
                    run_id = EXCLUDED.run_id,
                    source_endpoint = EXCLUDED.source_endpoint,
                    close_time = EXCLUDED.close_time,
                    open = EXCLUDED.open,
                    high = EXCLUDED.high,
                    low = EXCLUDED.low,
                    close = EXCLUDED.close,
                    volume = EXCLUDED.volume,
                    quote_volume = EXCLUDED.quote_volume,
                    trade_count = EXCLUDED.trade_count,
                    extra = EXCLUDED.extra;
                """,
                params,
            )
            upserted += len(batch)

    return upserted


def ingest_parquet_to_timescaledb(
    lake_root: str,
    config: TimescaleConfig,
    batch_size: int = 1000,
    dataset_types: list[DatasetType] | None = None,
) -> dict[str, int]:
    """Load parquet-lake files into TimescaleDB with state-based incremental updates.

    Files are processed in record batches to keep memory bounded. Each upsert batch
    is committed before moving to the next batch to reduce transaction blast radius.
    """

    if batch_size <= 0:
        raise ValueError("batch_size must be positive")

    try:
        import pyarrow.parquet as pq
    except ImportError as exc:
        raise RuntimeError("pyarrow is required for parquet ingestion. Install project dependencies.") from exc

    try:
        import psycopg
    except ImportError as exc:
        raise RuntimeError("psycopg is required for TimescaleDB ingestion. Install project dependencies.") from exc

    files = list_parquet_files(lake_root=lake_root, dataset_types=dataset_types)
    summary = {
        "files_scanned": len(files),
        "files_ingested": 0,
        "files_skipped": 0,
        "rows_upserted": 0,
    }

    with psycopg.connect(
        host=config.host,
        port=config.port,
        user=config.user,
        password=config.password,
        dbname=config.dbname,
        sslmode=config.sslmode,
    ) as connection:
        _ensure_tables(connection)
        state = _load_ingestion_state(connection)

        for file_path in files:
            signature = parquet_file_signature(file_path)
            state_key = str(file_path.resolve())
            if state.get(state_key) == signature:
                summary["files_skipped"] += 1
                continue

            parquet_file = pq.ParquetFile(file_path)  # type: ignore[no-untyped-call]
            for record_batch in parquet_file.iter_batches(batch_size=batch_size):  # type: ignore[no-untyped-call]
                rows = record_batch.to_pylist()
                if not rows:
                    continue
                summary["rows_upserted"] += _upsert_rows(connection=connection, rows=rows, batch_size=batch_size)
                connection.commit()
            _save_ingestion_state(connection=connection, file_path=state_key, signature=signature)
            connection.commit()
            summary["files_ingested"] += 1

    return summary


def load_combined_dataframe_from_db(
    config: TimescaleConfig,
    exchanges: list[str] | None = None,
    symbols: list[str] | None = None,
    timeframes: list[str] | None = None,
    instrument_types: list[str] | None = None,
    start_time: datetime | None = None,
    end_time: datetime | None = None,
    limit: int | None = None,
) -> Any:
    """Load combined spot/perp OHLCV rows from DB as a pandas DataFrame."""

    if limit is not None and limit <= 0:
        raise ValueError("limit must be positive when provided")

    try:
        import pandas as pd
    except ImportError as exc:
        raise RuntimeError("pandas is required for dataframe export. Install project dependencies.") from exc

    try:
        import psycopg
    except ImportError as exc:
        raise RuntimeError("psycopg is required for dataframe export. Install project dependencies.") from exc

    conditions: list[str] = []
    params: list[Any] = []

    def _add_in_filter(field: str, values: list[str] | None) -> None:
        if not values:
            return
        placeholders = ", ".join(["%s"] * len(values))
        conditions.append(f"{field} IN ({placeholders})")
        params.extend(values)

    _add_in_filter("exchange", exchanges)
    _add_in_filter("symbol", symbols)
    _add_in_filter("timeframe", timeframes)
    _add_in_filter("instrument_type", instrument_types)

    if start_time is not None:
        conditions.append("open_time >= %s")
        params.append(start_time)
    if end_time is not None:
        conditions.append("open_time <= %s")
        params.append(end_time)

    query = """
        SELECT
            exchange,
            instrument_type,
            symbol,
            timeframe,
            open_time,
            close_time,
            open,
            high,
            low,
            close,
            volume,
            quote_volume,
            trade_count,
            dataset_type,
            run_id,
            source_endpoint
        FROM market_ohlcv
    """
    if conditions:
        query += " WHERE " + " AND ".join(conditions)
    query += " ORDER BY open_time, exchange, instrument_type, symbol, timeframe"
    if limit is not None:
        query += " LIMIT %s"
        params.append(limit)

    with psycopg.connect(
        host=config.host,
        port=config.port,
        user=config.user,
        password=config.password,
        dbname=config.dbname,
        sslmode=config.sslmode,
    ) as connection:
        return pd.read_sql_query(query, connection, params=params)

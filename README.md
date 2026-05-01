# crypto-l2-loader

## Project Overview

`crypto-l2-loader` is a focused Deribit Level 2 order book ingestion tool. It collects bounded public order book snapshots, normalizes them into typed `L2Snapshot` records, aggregates valid snapshots into one-minute (`M1`) microstructure bars, and can persist those bars to a local Parquet lake.

Current scope is intentionally narrow:

- Fetch Deribit perpetual L2 order book snapshots through the public REST API.
- Poll multiple symbols concurrently with bounded runtime controls.
- Aggregate valid, non-crossed snapshots into M1 microstructure feature rows.
- Optionally persist aggregated M1 rows to idempotent Parquet partitions.
- Expose one CLI command: `loader-l2-m1`.
- Validate symbol aliases before scheduled jobs with `validate-symbols`.

Former OHLCV, standalone open-interest, standalone funding, plotting, research-report, and database-ingestion surfaces have been removed.

## Architecture

```text
CLI
  -> Runtime config, env, and process lock
  -> Async multi-symbol L2 polling
  -> Deribit public/get_order_book adapter
  -> L2Snapshot normalization
  -> M1 microstructure aggregation
  -> JSON run output and structured logs
  -> Optional Parquet lake writer

CLI (validate-symbols)
  -> Symbol alias normalization
  -> Shallow Deribit order book fetch
  -> Valid book report
```

Current top-level code layout:

```text
api/
  cli.py        # CLI parser, run orchestration, output shaping
  runtime.py    # .env loading, logging, process lock, concurrency config
ingestion/
  http_client.py
  l2.py
  lake.py
  exchanges/deribit_l2.py
tests/
main.py
pyproject.toml
README.md
AGENTS.md
```

## Installation

### Prerequisites

- Python 3.11+

### Setup

```bash
python -m venv .venv
source .venv/bin/activate
pip install -U pip
pip install -e .
```

## Configuration

Copy `.env.example` to `.env` for local runtime configuration. `.env` is ignored by git.

Supported environment variables:

| Variable | Purpose |
|---|---|
| `L2_HTTP_TIMEOUT_S` | HTTP request timeout in seconds. |
| `L2_HTTP_MAX_RETRIES` | Retry count for transient request failures. |
| `L2_HTTP_RETRY_BACKOFF_S` | Base retry backoff in seconds. |
| `L2_SYNC_LOG_DIR` | Runtime log directory. |
| `L2_FETCH_CONCURRENCY` | Maximum concurrent symbol fetches per polling tick. |
| `L2_INGEST_EXCHANGE` | Exchange name. Currently only `deribit`. |
| `L2_INGEST_SYMBOLS` | Whitespace- or comma-delimited symbol list. |
| `L2_INGEST_LEVELS` | Requested order book depth per side. |
| `L2_INGEST_SNAPSHOT_COUNT` | Number of polling ticks per symbol. |
| `L2_INGEST_POLL_INTERVAL_S` | Sleep interval between polling ticks. |
| `L2_INGEST_MAX_RUNTIME_S` | Optional runtime budget. `0` disables the budget. |
| `L2_INGEST_SAVE_PARQUET_LAKE` | Save aggregated M1 bars when true. |
| `L2_INGEST_LAKE_ROOT` | Parquet lake root directory. |
| `L2_INGEST_NO_JSON_OUTPUT` | Suppress CLI JSON output when true. |

## Usage

### CLI Options

```text
python main.py loader-l2-m1 [options]
```

| Option | Meaning |
|---|---|
| `--exchange {deribit}` | Exchange adapter to use. Only `deribit` is currently supported. |
| `--symbols SYMBOLS [SYMBOLS ...]` | Symbols to fetch. Accepts space-separated or comma-separated values. |
| `--levels LEVELS` | Number of order book levels per side. |
| `--snapshot-count SNAPSHOT_COUNT` | Polling ticks per symbol. |
| `--poll-interval-s POLL_INTERVAL_S` | Sleep interval between polling ticks. |
| `--lake-root LAKE_ROOT` | Root directory for optional Parquet output. |
| `--max-runtime-s MAX_RUNTIME_S` | Runtime budget in seconds. `0` disables the budget. |
| `--save-parquet-lake`, `--no-save-parquet-lake` | Enable or disable Parquet persistence. |
| `--json-output`, `--no-json-output` | Enable or suppress JSON output. Logs are still emitted. |

Symbols are normalized to Deribit perpetual instruments. For example, `BTC`, `BTCUSDT`, `BTCUSD`, and `BTC-PERPETUAL` resolve to `BTC-PERPETUAL`.
`SOL` resolves to Deribit's active `SOL_USDC-PERPETUAL` market.

Fetch BTC and ETH snapshots, aggregate to M1 bars, and print JSON:

```bash
python main.py loader-l2-m1 --symbols BTC ETH --snapshot-count 60 --poll-interval-s 1
```

Comma-separated symbols are also accepted:

```bash
python main.py loader-l2-m1 --symbols BTC,ETH --snapshot-count 60 --poll-interval-s 1
```

Save aggregated bars to the Parquet lake:

```bash
python main.py loader-l2-m1 \
  --symbols BTC ETH \
  --levels 50 \
  --snapshot-count 60 \
  --poll-interval-s 1 \
  --save-parquet-lake
```

Validate symbols before adding them to cron:

```bash
python main.py validate-symbols --symbols BTC ETH SOL
```

The Parquet layout is:

```text
lake/bronze/
  dataset_type=l2_m1/
    exchange=deribit/
      instrument_type=perp/
        symbol=BTC-PERPETUAL/
          timeframe=1m/
            date=YYYY-MM/
              data.parquet
```

## Modules

| Module | Responsibility |
|---|---|
| `api/cli.py` | CLI parsing, L2 run orchestration, aggregation coordination, JSON output, parquet persistence dispatch, and run logging. |
| `api/runtime.py` | `.env` loading, process locking, logging setup, and concurrency config. |
| `ingestion/http_client.py` | Minimal JSON HTTP client with retries. |
| `ingestion/exchanges/deribit_l2.py` | Deribit order book adapter and symbol normalization. |
| `ingestion/l2.py` | L2 dataclasses, async polling, snapshot normalization, and M1 feature aggregation. |
| `ingestion/lake.py` | Idempotent Parquet writer for aggregated L2 M1 bars. |

## Data Dictionary

`L2Snapshot` captures one normalized order book response:

| Field | Description |
|---|---|
| `exchange`, `symbol`, `timestamp` | Source and event identity. |
| `fetch_duration_s` | Wall-clock fetch duration. |
| `bids`, `asks` | Price/amount levels as ordered tuples. |
| `mark_price`, `index_price` | Deribit mark and index prices when present. |
| `open_interest` | Deribit open interest value included in the order book response. |
| `funding_8h`, `current_funding` | Deribit funding fields included in the order book response. |

`L2MinuteBar` includes:

- Mid-price OHLC from valid non-crossed snapshots.
- Mean, max, and last spread in basis points.
- Depth means at 1, 10, and 50 levels.
- Book imbalance at 1, 10, and 50 levels.
- L1 microprice and side VWAP features.
- Last mark, index, open-interest, and funding fields.
- Mean, max, and last fetch duration.

## Testing

Run the full verification suite:

```bash
.venv/bin/python -m pytest
.venv/bin/python -m ruff check .
.venv/bin/python -m mypy .
```

Or use:

```bash
make check
```

## Known Limitations

- Only Deribit perpetual L2 order book snapshots are supported.
- The loader uses REST polling, not a streaming websocket feed.
- M1 bars are computed from the snapshots collected during a run; they are not full exchange-native historical bars.
- Parquet persistence is local-file based and does not include a database sink.
- Raw snapshots are not persisted; only aggregated M1 bars are written.
- Failed per-symbol fetches inside a polling tick are logged, isolated, and skipped for that tick.

## Future Improvements

- Add a websocket collector for higher-frequency L2 sampling.
- Add explicit schema-version migration tests for Parquet outputs.
- Add replay utilities for validating aggregation behavior on stored raw snapshots.
- Add exchange adapters behind the existing L2 interfaces.

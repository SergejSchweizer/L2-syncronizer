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

`L2MinuteBar` captures one generated 1-minute aggregation row per exchange, symbol, and minute. Only valid snapshots with a non-empty, non-crossed best book are included; crossed books, empty books, and non-positive best prices are dropped before aggregation.

Snapshot-level formulas used by the 1m aggregator:

| Formula | Meaning |
|---|---|
| `mid = (best_bid + best_ask) / 2` | Best bid/ask midpoint. |
| `spread_bps = ((best_ask - best_bid) / mid) * 10000` | Quoted spread in basis points. |
| `bid_depth_N`, `ask_depth_N` | Sum of side amounts through the first `N` book levels. |
| `imbalance_N = (bid_depth_N - ask_depth_N) / (bid_depth_N + ask_depth_N)` | Signed depth imbalance. Positive values indicate more bid depth; negative values indicate more ask depth. `None` when both sides have zero depth. |
| `microprice = ((best_ask * bid_amount_1) + (best_bid * ask_amount_1)) / (bid_amount_1 + ask_amount_1)` | L1 queue-weighted price. `None` when total L1 amount is zero. |
| `microprice_minus_mid = microprice - mid` | L1 microprice displacement from midpoint. |
| `side_vwap_10 = sum(price * amount) / sum(amount)` | Side-specific VWAP through the first 10 book levels. `None` when the side has zero depth. |

Generated 1m aggregation fields:

| Field | Meaning |
|---|---|
| `minute_ts` | UTC minute bucket start. Snapshots are grouped by `timestamp` truncated to the minute. |
| `exchange` | Source exchange name. Currently `deribit`. |
| `symbol` | Normalized exchange instrument symbol, for example `BTC-PERPETUAL`. |
| `snapshot_count` | Number of valid snapshots included in the minute row. |
| `mid_open` | First snapshot midpoint in timestamp order within the minute. |
| `mid_high` | Maximum snapshot midpoint within the minute. |
| `mid_low` | Minimum snapshot midpoint within the minute. |
| `mid_close` | Last snapshot midpoint in timestamp order within the minute. |
| `mark_close` | Mark price from the last included snapshot, when Deribit provides it. |
| `index_close` | Index price from the last included snapshot, when Deribit provides it. |
| `spread_bps_mean` | Arithmetic mean of snapshot `spread_bps` values within the minute. |
| `spread_bps_max` | Maximum snapshot `spread_bps` within the minute. |
| `spread_bps_last` | Last snapshot `spread_bps` in timestamp order within the minute. |
| `bid_depth_1_mean` | Mean bid amount at the best bid level. |
| `ask_depth_1_mean` | Mean ask amount at the best ask level. |
| `bid_depth_10_mean` | Mean cumulative bid amount through the first 10 bid levels. |
| `ask_depth_10_mean` | Mean cumulative ask amount through the first 10 ask levels. |
| `bid_depth_50_mean` | Mean cumulative bid amount through the first 50 bid levels. If fewer levels are present, all available levels are summed. |
| `ask_depth_50_mean` | Mean cumulative ask amount through the first 50 ask levels. If fewer levels are present, all available levels are summed. |
| `imbalance_1_mean` | Mean L1 depth imbalance across snapshots with a defined L1 imbalance. |
| `imbalance_10_mean` | Mean 10-level depth imbalance across snapshots with a defined 10-level imbalance. |
| `imbalance_50_mean` | Mean 50-level depth imbalance across snapshots with a defined 50-level imbalance. |
| `imbalance_10_last` | Last 10-level depth imbalance in timestamp order within the minute. |
| `imbalance_50_last` | Last 50-level depth imbalance in timestamp order within the minute. |
| `microprice_close` | Last L1 microprice in timestamp order within the minute. |
| `microprice_minus_mid_mean` | Mean L1 microprice displacement from midpoint across snapshots with a defined microprice. |
| `bid_vwap_10_mean` | Mean bid-side VWAP through the first 10 bid levels across snapshots with defined bid VWAP. |
| `ask_vwap_10_mean` | Mean ask-side VWAP through the first 10 ask levels across snapshots with defined ask VWAP. |
| `open_interest_last` | Open interest from the last included snapshot, when Deribit provides it. |
| `funding_8h_last` | 8-hour funding field from the last included snapshot, when Deribit provides it. |
| `current_funding_last` | Current funding field from the last included snapshot, when Deribit provides it. |
| `fetch_duration_s_mean` | Mean wall-clock fetch duration for included snapshots, in seconds. |
| `fetch_duration_s_max` | Maximum wall-clock fetch duration for included snapshots, in seconds. |
| `fetch_duration_s_last` | Last included snapshot fetch duration, in seconds. |

Parquet rows are produced from `L2MinuteBar` with additional lake metadata:

| Field | Meaning |
|---|---|
| `schema_version` | Parquet row schema version. Currently `v1`. |
| `dataset_type` | Dataset identifier. Currently `l2_m1`. |
| `instrument_type` | Instrument category. Currently `perp`. |
| `event_time` | Same timestamp as `minute_ts`; the canonical event time for the row. |
| `ingested_at` | UTC timestamp when the row was prepared for lake persistence. |
| `run_id` | Unique CLI run identifier assigned during ingestion. |
| `source_endpoint` | Logical source endpoint name. Currently `public_l2_orderbook`. |
| `open_time` | Inclusive 1m interval start, equal to `minute_ts`. |
| `close_time` | 1m interval close marker, set to `minute_ts` plus `59.999` seconds. |
| `timeframe` | Aggregation timeframe. Currently `1m`. |

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

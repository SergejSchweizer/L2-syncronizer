# crypto-l2-loader

## 1. Project Overview

This repository provides a modular framework for ingesting crypto market data with emphasis on reproducibility and production quality.

Current implemented scope (Step 1):
- Pull BTC/ETH candles from Deribit public APIs for spot/perp markets.
- Expose a CLI command for repeatable loader runs.

Scope note:
- The repository name keeps the long-term direction (`crypto-l2-loader`), while the current production implementation is Deribit-only OHLCV + OI ingestion. L2 order book ingestion is planned future scope.

## 2. Architecture Diagram

```text
CLI -> Application Services (fetch/gapfill) -> Ingestion Adapters
    -> HTTP Client -> Exchange REST API -> Parquet Lake/TimescaleDB
```

## 3. Installation Guide

### 3.1 Prerequisites

- Python 3.11+

### 3.2 Setup

```bash
python -m venv .venv
source .venv/bin/activate
pip install -U pip
pip install -e .
```

If the repository folder is renamed or moved, recreate the virtualenv (`rm -rf .venv && make setup`) so script shebangs remain valid.

## 4. Dependency Setup

Core dependencies are managed through `pyproject.toml` and include:
- `matplotlib` for optional plot generation
- `pyarrow` for parquet lake output

## 5. Module Explanations

- `ingestion/http_client.py`: lightweight JSON HTTP utilities.
- `ingestion/spot.py`: exchange-agnostic candle load/normalization interface.
- `ingestion/exchanges/deribit.py`: Deribit adapter with symbol and timeframe mapping.
- `ingestion/plotting.py`: chart rendering for loaded price and volume data.
- `ingestion/lake.py`: parquet lake writer for partitioned candle datasets.
- `application/services/gapfill_service.py`: pure time/gap range helpers used by loader synchronization logic.
- `application/services/fetch_service.py`: fetch orchestration service (task DTOs + parallel execution + symbol-level bootstrap/gap-fill fetch).
- `application/services/storage_service.py`: orchestration for parquet-lake and TimescaleDB persistence side effects.
- `application/services/artifact_service.py`: sample CSV/plot artifact generation service.
- `application/schema.py`: canonical contract mapping for CLI data types to storage `dataset_type` + `instrument_type`.
- `api/cli.py`: thin CLI command registration, argument parsing, and output formatting layer.
- `infra/`: infrastructure and runtime scaffolding.

### 5.0 Canonical Data Type Naming

Use these names consistently in code, CLI usage, and documentation:

| Data Type | Canonical Name | Meaning |
|---|---|---|
| Spot candles | `spot` | Cash/spot OHLCV candles. |
| Perpetual candles | `perp` | Perpetual futures/swap OHLCV candles. |
| Open interest | `oi` | Open-interest time series for perpetual instruments. |

Naming rules:
- Use `spot`, `perp`, and `oi` as the only user-facing data-type names.
- `dataset_type=open_interest` is the storage-layer parquet label for `oi`.

### 5.0.1 Data Type Purpose And Meaning

#### `spot`
- Why this data exists:
  Spot candles are the base market state for unlevered price discovery and benchmark return construction.
- Meaning:
  `spot` represents executed cash-market OHLCV bars for assets such as BTC and ETH.
- Typical usage:
  Baseline volatility, return, and liquidity feature generation.

#### `perp`
- Why this data exists:
  Perpetual futures dominate crypto derivatives flow and often lead or amplify directional moves.
- Meaning:
  `perp` represents OHLCV bars from perpetual swap markets, normalized to the same schema as `spot`.
- Typical usage:
  Derivatives-aware price/volume signals and cross-market spread analysis against `spot`.

#### `oi`
- Why this data exists:
  Open interest captures positioning intensity and participation in derivatives markets.
- Meaning:
  `oi` represents open-interest time series aligned to perpetual instruments and intervals.
- Typical usage:
  Position build-up/unwind detection, confirmation/divergence checks with `perp` price action.

### 5.1 Data Dictionary

Loaded candle variables (`SpotCandle`):

| Variable | Type | Description |
|---|---|---|
| `exchange` | `str` | Exchange identifier used by the adapter (`deribit`). |
| `symbol` | `str` | Normalized instrument symbol in storage form for the selected exchange/market. |
| `interval` | `str` | Candle granularity (for example `1m`, `5m`, `1h`, `1d`). |
| `open_time` | `datetime (UTC)` | Timestamp when the candle interval starts (inclusive). |
| `close_time` | `datetime (UTC)` | Timestamp when the candle interval ends (inclusive in exchange payload mapping). |
| `open_price` | `float` | First traded price observed in the candle interval. |
| `high_price` | `float` | Maximum traded price observed in the candle interval. |
| `low_price` | `float` | Minimum traded price observed in the candle interval. |
| `close_price` | `float` | Last traded price observed in the candle interval. |
| `volume` | `float` | Base-asset traded volume during the interval. |
| `quote_volume` | `float` | Quote-asset traded volume during the interval (or exchange-equivalent field). |
| `trade_count` | `int` | Number of trades aggregated into the candle (exchange dependent). |

### 5.1.1 Dataset Semantics And Variable Computation

#### `spot` dataset (`dataset_type=ohlcv`, `instrument_type=spot`)
- Meaning:
  Executed cash-market candles used as the baseline non-derivative price/volume process.
- Canonical normalized variables:
  `open_time`, `close_time`, `open`, `high`, `low`, `close`, `volume`, `quote_volume`, `trade_count`.
- Computation in pipeline:
  - `open_time = datetime_utc(row[0] / 1000)`.
  - `close_time = datetime_utc(row[6] / 1000)` for endpoints that return explicit close timestamp.
  - `open, high, low, close = float(row[1]), float(row[2]), float(row[3]), float(row[4])`.
  - `volume = float(row[5])`.
  - `quote_volume = float(row[7])` when provided by endpoint.
  - `trade_count = int(row[8])` when provided by endpoint, else `0` on adapters without trade-count payloads.
- Exchange-specific details:
  - Deribit spot: built from `ticks/open/high/low/close/volume`; `close_time = open_time + timeframe_ms - 1`, `quote_volume = volume` fallback, `trade_count = 0`.

#### `perp` dataset (`dataset_type=ohlcv`, `instrument_type=perp`)
- Meaning:
  Executed perpetual-futures candles (derivatives market) normalized to the same schema as `spot`.
- Canonical normalized variables:
  Same OHLCV schema and formulas as `spot`; only instrument source/endpoint differs.
- Computation in pipeline:
  Uses the same `SpotCandle` parser and same storage row builder (`candle_record`) as `spot`, preserving identical field semantics.
- Exchange-specific details:
  - Deribit perp: TradingView chart endpoint with computed `close_time` and fallback fields identical to Deribit spot behavior.

#### `oi` dataset (`dataset_type=open_interest`, `instrument_type=perp`)
- Meaning:
  Time-bucketed open interest for perpetual instruments; reflects outstanding open positions rather than executed candle flow.
- Availability rule:
  Collected only when market context is `perp`; requesting `oi` for `spot` returns no rows by design.
- Canonical normalized variables:
  `open_time`, `close_time`, `open_interest`, `open_interest_value`.
- Computation in pipeline:
  - `open_time` is parsed from exchange timestamp and converted to UTC.
  - `close_time = open_time + timeframe_ms - 1` (inclusive interval boundary).
  - `open_interest` comes from exchange OI quantity field.
  - `open_interest_value` is used when exchange provides notional/value OI, otherwise set to `0.0`.
- Exchange-specific OI mapping:
  - Deribit (`/api/v2/public/get_last_settlements_by_instrument`):
    - Raw settlement `timestamp` is bucketed to requested timeframe:
      `bucket_open_ms = floor(timestamp / timeframe_ms) * timeframe_ms`.
    - `open_interest = float(position)`.
    - `open_interest_value = 0.0` (not provided by adapter source payload).

Parquet row metadata fields:

| Variable | Type | Description |
|---|---|---|
| `schema_version` | `str` | Version marker for row schema evolution (`v1` currently). |
| `dataset_type` | `str` | Dataset family label (`ohlcv`). |
| `instrument_type` | `str` | Market class used for loading (`spot` or `perp`). |
| `event_time` | `datetime (UTC)` | Canonical event timestamp for the row (currently aligned to `open_time`). |
| `ingested_at` | `datetime (UTC)` | Wall-clock timestamp when the row was written by the pipeline. |
| `run_id` | `str` | Unique ingestion execution identifier for traceability. |
| `source_endpoint` | `str` | Exchange endpoint group used to produce the row (`public_market_data` currently). |
| `timeframe` | `str` | Storage timeframe field (same semantic meaning as `interval`). |
| `open`, `high`, `low`, `close` | `float` | Parquet OHLC aliases mapped from candle prices. |
| `extra` | `json/object` | Full normalized candle payload snapshot for reproducibility/debugging. |

### 5.2 Market Types

Supported `--market` values (data types):

| Market Type | Storage `instrument_type` | Meaning |
|---|---|---|
| `spot` | `spot` | Cash/spot market candles. |
| `perp` | `perp` | Perpetual futures/swap candles. |
| `oi` | `perp` | Open-interest time series for perpetual instruments. |

Exchange coverage:

| Exchange | Spot | Perp | Notes |
|---|---|---|---|
| Deribit | Yes | Yes | Spot maps to instruments like `BTC_USDC`; perp maps to `BTC-PERPETUAL`. |

### 5.3 Perpetual Field Mapping (Deribit Origin -> Storage)

Perpetual rows are normalized into the same storage schema as spot:
`open_time`, `close_time`, `open`, `high`, `low`, `close`, `volume`, `quote_volume`, `trade_count`, plus metadata.

Deribit perp (`/api/v2/public/get_tradingview_chart_data`) mapping:

| Origin Field | Storage Field |
|---|---|
| `result.ticks[i]` | `open_time` |
| `result.ticks[i] + candle_width_ms - 1` | `close_time` |
| `result.open[i]` | `open` |
| `result.high[i]` | `high` |
| `result.low[i]` | `low` |
| `result.close[i]` | `close` |
| `result.volume[i]` | `volume` |
| `result.volume[i]` (fallback proxy) | `quote_volume` |
| not provided by endpoint | `trade_count = 0` |

### 5.4 Perpetual Symbol Naming by Exchange

Perpetual symbol normalization (Deribit):

| Exchange | Canonical Perp Symbols | Accepted Input Aliases | Normalized Output |
|---|---|---|---|
| Deribit | `BTC-PERPETUAL`, `ETH-PERPETUAL` | `BTC`, `BTCUSDT`, `BTCUSD`, `BTC-PERPETUAL`; `ETH`, `ETHUSDT`, `ETHUSD`, `ETH-PERPETUAL` | `BTC-PERPETUAL`, `ETH-PERPETUAL` |

CLI recommendation:
- Use `BTC` / `ETH`; Deribit adapters normalize to canonical Deribit symbols.

## 6. Execution Workflow

Load BTC/ETH spot candles:

```bash
python3 main.py loader --exchange deribit --market spot --symbols BTC ETH --timeframe H1
```

Load spot and perp in one run:

```bash
python3 main.py loader --exchange deribit --market spot perp --symbols BTC ETH --timeframe M1
```

Load in one run:

```bash
python3 main.py loader --exchange deribit --market spot --symbols BTC ETH --timeframe M1
```

Load multiple timeframes in one run:

```bash
python3 main.py loader --exchange deribit --market spot --symbols BTC ETH --timeframes M1 M5 H1 --no-json-output
```

Load and generate plots (price + volume) under `samples/`:

```bash
python3 main.py loader --exchange deribit --market spot --symbols BTC ETH --timeframe M5 --plot --plot-price close
```

Save loaded data to parquet lake format:

```bash
python3 main.py loader --exchange deribit --market spot --symbols BTC ETH --timeframe H1 --save-parquet-lake --lake-root lake/bronze
```

Save loaded data to TimescaleDB:

```bash
python3 main.py loader --exchange deribit --market spot perp oi --symbols BTC ETH --timeframe 1m --save-timescaledb --timescaledb-schema market_data
```

TimescaleDB bootstrap options:
- Default behavior creates schema/tables/hypertables if missing.
- Use `--timescaledb-no-bootstrap` to write into pre-existing schema objects only.

Fetch OHLCV (spot+perp) and open interest in one run by including market `oi`:

```bash
python3 main.py loader --exchange deribit --market spot perp oi --symbols BTC ETH --timeframe 5m --save-parquet-lake --lake-root lake/bronze
```

Parquet lake write mode uses a stable file per partition (`data.parquet`) with staged merge+rewrite on each run to keep file counts bounded. Partition schema:

```text
dataset_type=ohlcv/
  exchange=<exchange>/
  instrument_type=<spot|perp>/
  symbol=<symbol>/
  timeframe=<interval>/
  date=<YYYY-MM>/
    data.parquet

dataset_type=open_interest/
  exchange=<exchange>/
  instrument_type=<perp>/
  symbol=<symbol>/
  timeframe=<interval>/
  date=<YYYY-MM>/
    data.parquet
```

Loader mode is automatic:
- Only two modes exist:
  1. `fetch all history` when no parquet data exists for the symbol/timeframe.
  2. `fill gaps` when parquet data exists (internal gaps + tail to latest closed candle).
- If no parquet data exists for a symbol/timeframe, it fetches full available exchange history.
- If parquet data exists, it performs gap-fill (internal gaps + tail to latest closed candle).

Example full-history bootstrap (first run can be long-running):

```bash
python3 main.py loader --exchange deribit --market spot --symbols BTC ETH --timeframe M1 --save-parquet-lake --lake-root lake/bronze --no-json-output
```

Note:
- Loader network fetch tasks run in parallel via `asyncio` with bounded concurrency.
- Parallel fetch orchestration is implemented in `application/services/fetch_service.py`; `api/cli.py` delegates to this service.
- Parquet partition writes are parallelized.
- Concurrency is controlled by `L2_FETCH_CONCURRENCY` (default: `8`).

Run silently without JSON output:

```bash
python3 main.py loader --exchange deribit --market spot --symbols BTC --timeframe M1 --no-json-output
```

Ingest existing parquet lake files into TimescaleDB (no internet fetch):

```bash
python3 main.py ingest-timescaledb --lake-root lake/bronze --timescaledb-schema market_data
```

Optional filters for offline parquet->Timescale ingestion:

```bash
python3 main.py ingest-timescaledb --lake-root lake/bronze --exchanges deribit --instrument-types perp --timeframes 1m
```

Export deterministic descriptive statistics for reporting:

```bash
python3 main.py export-descriptive-stats --lake-root lake/bronze --output-csv docs/tables/descriptive_stats_baseline.csv --start-time 2026-01-01T00:00:00+00:00 --end-time 2026-01-31T23:59:59+00:00
```

Load Deribit perpetual candles (portable perp inputs):

```bash
python3 main.py loader --exchange deribit --market perp --symbols BTC ETH --timeframe M5
```

List all currently supported spot timeframes:

```bash
python3 main.py list-spot-timeframes
python3 main.py list-spot-timeframes --exchange deribit
```

## 7. Datatype Plots

### 7.1 OHLCV (Spot)
Description:
OHLCV spot rows capture exchange-traded candle bars for cash markets.  
The loader writes grouped sample artifacts per run under `samples/`.

Plot:
`samples/spot_<exchange>_<symbol>_<timeframe>_sample_10_rows.png` (full-history price + volume plot for that group).

Example:
`samples/spot_deribit_BTCUSDT_1m_sample_10_rows.png`

### 7.2 OHLCV (Perp)
Description:
Perpetual OHLCV rows follow the same candle schema and are stored independently per market group.

Plot:
`samples/perp_<exchange>_<symbol>_<timeframe>_sample_10_rows.png` (full-history price + volume plot for that group).

Example:
`samples/perp_deribit_BTCUSDT_1m_sample_10_rows.png`

### 7.3 Open Interest (OI)
Description:
Open-interest rows are stored under `dataset_type=open_interest` and sampled per run when `--market oi` is used.

Plot:
`samples/oi_<market>_<exchange>_<symbol>_<timeframe>_sample_10_rows.png` (full-history OI time-series line chart for that group).

Example:
`samples/oi_perp_deribit_BTCUSDT_5m_sample_10_rows.png`

## 8. Testing Instructions

```bash
make check
```

Equivalent direct commands:

```bash
.venv/bin/python -m pytest
.venv/bin/python -m ruff check .
.venv/bin/python -m mypy .
```

Pre-commit hooks:

```bash
python -m pip install pre-commit
pre-commit install
pre-commit run --all-files
```

Current coverage includes:
- Exchange adapter normalization/routing and pagination behavior.
- Gap-fill utility logic (`application/services/gapfill_service.py`).
- Fetch orchestration success/error split for parallel task runners (`application/services/fetch_service.py`).
- Storage orchestration behavior for parquet+Timescale side effects (`application/services/storage_service.py`).
- Canonical dataset contract mapping (`application/schema.py`).
- CLI locking, parquet lake persistence, plotting, and TimescaleDB sink behavior.

## 9. Deployment Instructions

- For now this is a local CLI tool.
- Next stage will add scheduled runs and orchestrated pipelines.
- The CLI enforces a single running instance using `.run/crypto-l2-loader.lock`.
- Runtime logs are written to `/volume1/Temp/logs/crypto-l2-loader.log` by default.
- Logs rotate every 7 days and rotated files are date-suffixed (for example `crypto-l2-loader.log.2026-04-27`) and retained in the same directory.
- Optional override: set `L2_SYNC_LOG_DIR` to change the log directory.
- Optional override: set `L2_FETCH_CONCURRENCY` to control loader fetch parallelism (minimum `1`, default `8`).
- TimescaleDB sink env vars: `TIMESCALEDB_HOST`, `TIMESCALEDB_PORT`, `TIMESCALEDB_USER`, `TIMESCALEDB_PASSWORD`, `TIMESCALEDB_DB`, `PGSSLMODE`.
- `TIMESCALEDB_PASSWORD` is required at runtime (no insecure default fallback).
- Run quality gates via module form (`python -m pytest`, `python -m mypy`, `python -m ruff`) to avoid local venv entrypoint shebang drift when directories move.

## 10. Known Limitations

- Step 1 currently supports candles only (no funding or L2 yet).
- No exchange failover yet.

## 11. Future Improvements

- Add perpetual and funding endpoints.
- Add Deribit adapter for L2 order book snapshots.
- Add scheduled lake compaction and retention policies.

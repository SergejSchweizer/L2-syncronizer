# crypto-l2-loader

## 1. Project Overview

This repository provides a modular framework for ingesting crypto market data with emphasis on reproducibility and production quality.

Current implemented scope (Step 1):
- Pull BTC/ETH candles from Binance, Deribit, and Bybit public APIs for spot/perp markets.
- Expose a CLI command for repeatable loader runs.

## 2. Architecture Diagram

```text
CLI -> Ingestion Adapter -> HTTP Client -> Exchange REST API -> Parquet Lake
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
- `ingestion/exchanges/binance.py`: Binance adapter with pagination support.
- `ingestion/exchanges/deribit.py`: Deribit adapter with symbol and timeframe mapping.
- `ingestion/plotting.py`: chart rendering for loaded price and volume data.
- `ingestion/lake.py`: parquet lake writer for partitioned candle datasets.
- `api/cli.py`: CLI command registration and output formatting.
- `infra/`: infrastructure and runtime scaffolding.

### 5.1 Data Dictionary

Loaded candle variables (`SpotCandle`):

| Variable | Type | Description |
|---|---|---|
| `exchange` | `str` | Exchange identifier used by the adapter (for example `binance`, `deribit`). |
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

Supported `--market` values:

| Market Type | Storage `instrument_type` | Meaning |
|---|---|---|
| `spot` | `spot` | Cash/spot market candles. |
| `perp` | `perp` | Perpetual futures/swap candles. |

Exchange coverage:

| Exchange | Spot | Perp | Notes |
|---|---|---|---|
| Binance | Yes | Yes | Perp uses USDT-margined futures kline endpoint. |
| Deribit | Yes | Yes | Spot maps to instruments like `BTC_USDC`; perp maps to `BTC-PERPETUAL`. |
| Bybit | Yes | Yes | Perp uses `linear` category in Bybit v5 kline API. |

### 5.3 Perpetual Field Mapping (Origin -> Storage)

Perpetual rows are normalized into the same storage schema as spot:
`open_time`, `close_time`, `open`, `high`, `low`, `close`, `volume`, `quote_volume`, `trade_count`, plus metadata.

Binance perp (`/fapi/v1/klines`) mapping:

| Origin Field | Storage Field |
|---|---|
| `row[0]` (open time ms) | `open_time` |
| `row[6]` (close time ms) | `close_time` |
| `row[1]` | `open` |
| `row[2]` | `high` |
| `row[3]` | `low` |
| `row[4]` | `close` |
| `row[5]` | `volume` |
| `row[7]` | `quote_volume` |
| `row[8]` | `trade_count` |

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

Bybit perp (`/v5/market/kline`, `category=linear`) mapping:

| Origin Field | Storage Field |
|---|---|
| `list[i][0]` (start time ms) | `open_time` |
| `open_time + interval_ms - 1` | `close_time` |
| `list[i][1]` | `open` |
| `list[i][2]` | `high` |
| `list[i][3]` | `low` |
| `list[i][4]` | `close` |
| `list[i][5]` | `volume` |
| `list[i][6]` (turnover) | `quote_volume` |
| not provided by endpoint | `trade_count = 0` |

### 5.4 Perpetual Symbol Naming by Exchange

Perpetual symbol normalization is exchange-specific. The loader accepts aliases and maps them to canonical storage symbols.

| Exchange | Canonical Perp Symbols | Accepted Input Aliases | Normalized Output |
|---|---|---|---|
| Binance | `BTCUSDT`, `ETHUSDT` | `BTC`, `BTCUSD`, `BTCUSDT`; `ETH`, `ETHUSD`, `ETHUSDT` | `BTCUSDT`, `ETHUSDT` |
| Deribit | `BTC-PERPETUAL`, `ETH-PERPETUAL` | `BTC`, `BTCUSDT`, `BTCUSD`, `BTC-PERPETUAL`; `ETH`, `ETHUSDT`, `ETHUSD`, `ETH-PERPETUAL` | `BTC-PERPETUAL`, `ETH-PERPETUAL` |
| Bybit | `BTCUSDT`, `ETHUSDT` | `BTC`, `BTCUSD`, `BTCUSDT`; `ETH`, `ETHUSD`, `ETHUSDT` | `BTCUSDT`, `ETHUSDT` |

Portable CLI recommendation:
- Use `BTC` / `ETH` for cross-exchange commands on both `spot` and `perp`; adapters normalize to exchange-specific canonical symbols.

## 6. Execution Workflow

Load BTC/ETH spot candles:

```bash
python3 main.py loader --exchange binance --market spot --symbols BTC ETH --timeframe H1
```

Load spot and perp in one run:

```bash
python3 main.py loader --exchanges binance deribit --market spot perp --symbols BTC ETH --timeframe M1
```

Load multiple exchanges in one run:

```bash
python3 main.py loader --exchanges binance deribit --market spot --symbols BTC ETH --timeframe M1
```

Load multiple timeframes in one run (executed sequentially across exchange/market/symbol/timeframe):

```bash
python3 main.py loader --exchanges binance deribit --market spot --symbols BTC ETH --timeframes M1 M5 H1 --no-json-output
```

Load and generate plots (price + volume) under `plots/`:

```bash
python3 main.py loader --exchanges binance deribit --market spot --symbols BTC ETH --timeframe M5 --plot --plot-dir plots --plot-price close
```

Save loaded data to parquet lake format:

```bash
python3 main.py loader --exchanges binance deribit --market spot --symbols BTC ETH --timeframe H1 --save-parquet-lake --lake-root lake/bronze
```

Fetch OHLCV (spot+perp) and open interest in one run by including market `oi`:

```bash
python3 main.py loader --exchanges binance --market spot perp oi --symbols BTC ETH --timeframe 5m --save-parquet-lake --lake-root lake/bronze
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
python3 main.py loader --exchanges binance deribit --market spot --symbols BTC ETH --timeframe M1 --save-parquet-lake --lake-root lake/bronze --no-json-output
```

Note: network fetch tasks are currently sequential. Parquet partition writes are parallelized.

Run silently without JSON output:

```bash
python3 main.py loader --exchange binance --market spot --symbols BTC --timeframe M1 --no-json-output
```

Export combined spot/perp dataset from parquet lake as a dataframe file:

```bash
python3 main.py export-df --lake-root lake/bronze --format parquet --output exports --instrument-types spot perp --exchanges binance deribit --timeframes 1m
```

Open interest columns are included by default:

```bash
python3 main.py export-df --lake-root lake/bronze --format parquet --output exports
```

Disable open interest join explicitly:

```bash
python3 main.py export-df --lake-root lake/bronze --format parquet --output exports --no-open-interest
```

Export writes one file per `exchange_symbol_timeframe`.
Each file contains both `spot` and `perp` rows for that group (if available in lake data).
For each exported dataframe file, a matching full plot (`spot` price + volume) is also saved
with the same base name and `.png` extension.
If `--output` is omitted, files are written to `/volume1/Temp/crypto`.
The export JSON summary includes extended per-file metadata in `generated_files`
(data file path, plot file path, row count, and start/end `open_time` range).
If export result is empty after filters/time range, no fallback file is created.

Examples:
- `binance_BTCUSDT_1m_full.csv`
- `deribit_ETHUSDT_5m_full.parquet`

CSV export variant:

```bash
python3 main.py export-df --lake-root lake/bronze --format csv --output exports --start-time 2026-01-01T00:00:00Z --end-time 2026-12-31T23:59:59Z
```

Load Binance + Deribit perpetual candles (portable perp inputs):

```bash
python3 main.py loader --exchanges binance deribit --market perp --symbols BTC ETH --timeframe M5
```

List all currently supported spot timeframes:

```bash
python3 main.py list-spot-timeframes
python3 main.py list-spot-timeframes --exchange deribit
python3 main.py list-spot-timeframes --exchanges binance deribit
```

## 7. Datatype Plots

### 7.1 OHLCV (Spot)
Description:
OHLCV spot rows capture exchange-traded candle bars for cash markets. Exported dataframes are grouped per `exchange_symbol_timeframe`.

Plot:
`<output_dir>/<exchange>_<symbol>_<timeframe>_full.png` (price + volume).

Example:
`/volume1/Temp/crypto/binance_BTCUSDT_1m_full.png`

### 7.2 OHLCV (Perp)
Description:
Perpetual OHLCV rows follow the same candle schema and are combined with spot rows in the same grouped dataframe when both exist.

Plot:
`<output_dir>/<exchange>_<symbol>_<timeframe>_full.png` (price + volume, includes perp rows present in the group).

Example:
`/volume1/Temp/crypto/binance_BTCUSDT_1m_full.png`

### 7.3 Open Interest (OI)
Description:
Open-interest rows are stored under `dataset_type=open_interest` and are joined into exports by default (unless `--no-open-interest` is set).

Plot:
`<output_dir>/<exchange>_<symbol>_<timeframe>_open_interest.png` (OI time-series line chart).

Example:
`/volume1/Temp/crypto/binance_BTCUSDT_1m_open_interest.png`

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

## 9. Deployment Instructions

- For now this is a local CLI tool.
- Next stage will add scheduled runs and orchestrated pipelines.
- The CLI enforces a single running instance using `.run/crypto-l2-loader.lock`.
- Runtime logs are written to `/volume1/Temp/logs/crypto-l2-loader.log` by default.
- Logs rotate every 7 days and rotated files are date-suffixed (for example `crypto-l2-loader.log.2026-04-27`) and retained in the same directory.
- Optional override: set `L2_SYNC_LOG_DIR` to change the log directory.

## 10. Known Limitations

- Step 1 currently supports candles only (no funding or L2 yet).
- No exchange failover yet.

## 11. Future Improvements

- Add perpetual and funding endpoints.
- Add Deribit adapter for L2 order book snapshots.
- Add scheduled lake compaction and retention policies.

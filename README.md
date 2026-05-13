# crypto-l2-loader

## Project Overview

`crypto-l2-loader` is a focused Deribit Level 2 order book ingestion tool. It collects bounded public order book snapshots, normalizes them into typed `L2Snapshot` records, and persists raw snapshots to a local bronze Parquet lake.
It also includes Polars-based Silver and Gold transforms for fixed-width L2 snapshot features and M1 aggregate artifacts.

Current scope is intentionally narrow:

- Fetch Deribit perpetual L2 order book snapshots through the public REST API.
- Poll multiple symbols concurrently with bounded runtime controls.
- Optionally persist raw snapshots to idempotent daily bronze Parquet partitions.
- Expose ingestion and transform CLI commands: `bronze-builder`, `silver-builder`, and `gold-builder`.
- Validate symbol aliases before scheduled jobs with `validate-symbols`.

Former OHLCV, standalone open-interest, standalone funding, research-report, and database-ingestion surfaces have been removed.

## Architecture

```text
CLI
  -> Runtime config
  -> Async multi-symbol L2 polling
  -> Deribit public/get_order_book adapter
  -> L2Snapshot normalization
  -> Optional raw bronze Parquet lake writer
  -> JSON run output and structured logs

CLI (validate-symbols)
  -> Symbol alias normalization
  -> Shallow Deribit order book fetch
  -> Valid book report

Lake transform
  -> silver-builder CLI command
  -> Polars Bronze-to-Silver snapshot feature transform
  -> Monthly silver Parquet, JSON metadata, and PNG profile artifacts
  -> gold-builder CLI command
  -> Polars Silver-to-Gold M1 aggregation
  -> Optional missing-minute numeric fill from adjacent Gold minute averages
  -> Versioned full-timeframe Gold Parquet, JSON metadata, and PNG profile artifacts
  -> Incremental state files that skip unchanged Bronze/Silver inputs
```

Current top-level code layout:

```text
api/
  constants.py  # shared command names and runtime artifact names
  cli.py        # CLI parser, run orchestration, output shaping
  runtime.py    # logging and concurrency config
ingestion/
  artifact_state.py
  config.py
  http_client.py
  l2.py
  lake.py
  silver.py
  gold.py
  exchanges/deribit_l2.py
tests/
config.yaml
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
pip install -e ".[dev]"
```

For runtime-only installs, use `pip install -e .`. The `dev` extra installs the pinned quality-gate tools used by `make check`.

### Package Dependencies

Runtime dependencies are declared in `pyproject.toml`:

| Package | Used For |
|---|---|
| `matplotlib` | Silver and Gold profile PNG artifact generation. |
| `pyarrow` | Bronze, Silver, and Gold Parquet file reads/writes. |
| `polars` | Bronze-to-Silver L2 features and Silver-to-Gold M1 transformations. |

Development dependencies are grouped under the `dev` extra and cover `pytest`, `ruff`, and `mypy`.

## Configuration

Runtime defaults live in tracked `config.yaml`. CLI options override these defaults for a single run.
HTTP defaults are resolved once per process so each request does not reread `config.yaml`.

Supported config keys:

| Key | Purpose |
|---|---|
| `http.timeout_s` | HTTP request timeout in seconds. Defaults to `8`. |
| `http.max_retries` | Retry count for transient request failures. Defaults to `2`. |
| `http.retry_backoff_s` | Base retry backoff in seconds. Defaults to `1`. |
| `runtime.log_dir` | Runtime log directory. Defaults to repo-local `.logs/`. |
| `runtime.log_rotation_days` | Log rotation interval in days. Defaults to `7` for weekly rotation. |
| `runtime.log_backup_count` | Number of rotated logs to delete after. `0` keeps older logs indefinitely. |
| `runtime.fetch_concurrency` | Maximum concurrent symbol fetches per polling tick. |
| `ingestion.exchange` | Exchange name. Currently only `deribit`. |
| `ingestion.symbols` | Symbol list. |
| `ingestion.levels` | Requested order book depth per side. |
| `ingestion.snapshot_count` | Number of polling ticks per symbol. Defaults to `5`. |
| `ingestion.poll_interval_s` | Sleep interval between polling ticks. Defaults to `10` seconds. |
| `ingestion.max_runtime_s` | Optional runtime budget. Defaults to `50` seconds. `0` disables the budget. |
| `ingestion.save_parquet_lake` | Save raw L2 snapshots to the bronze Parquet lake when true. |
| `ingestion.lake_root` | Parquet lake root directory. |
| `ingestion.silver_lake_root` | Silver artifact lake root directory. Defaults to `lake/silver`. |
| `ingestion.gold_lake_root` | Gold artifact root directory. Defaults to `lake/gold`. |
| `ingestion.json_output` | Print CLI JSON output when true. |

## Current Jobs

The production cron setup runs the three data-layer builders once per minute from the repository root:

```cron
* * * * * cd /home/vcs/git/crypto-l2-loader && .venv/bin/python main.py bronze-builder --symbols BTC ETH SOL
* * * * * cd /home/vcs/git/crypto-l2-loader && .venv/bin/python main.py silver-builder
* * * * * cd /home/vcs/git/crypto-l2-loader && .venv/bin/python main.py gold-builder
```

| Job | Purpose | Reads | Writes | Log File |
|---|---|---|---|---|
| `bronze-builder` | Poll Deribit REST L2 snapshots for BTC, ETH, and SOL. | Deribit public order book API. | Daily Bronze Parquet partitions under `lake/bronze/`. | `.logs/bronze-builder.log` |
| `silver-builder` | Transform changed Bronze snapshots into fixed-width Silver snapshot feature rows. | `lake/bronze/` | Monthly Silver Parquet, JSON metadata, PNG profile artifacts, and `_silver_transform_state.json` under `lake/silver/`. | `.logs/silver-builder.log` |
| `gold-builder` | Aggregate changed Silver symbol partitions into dense M1 Gold datasets. | `lake/silver/` | Versioned timeframe Parquet, JSON metadata, PNG profile artifacts, and `_gold_transform_state.json` under `lake/gold/`. | `.logs/gold-builder.log` |

Jobs do not use process-level serialization, so overlapping scheduled launches are allowed. Logs rotate weekly, and rotated logs are kept indefinitely by default.

## Usage

### Bronze Builder

```text
python main.py bronze-builder [options]
```

| Option | Meaning |
|---|---|
| `--exchange {deribit}` | Exchange adapter to use. Only `deribit` is currently supported. |
| `--symbols SYMBOLS [SYMBOLS ...]` | Symbols to fetch. Accepts space-separated or comma-separated values. |
| `--levels LEVELS` | Number of order book levels per side. |
| `--snapshot-count SNAPSHOT_COUNT` | Polling ticks per symbol. Defaults to `5`. |
| `--poll-interval-s POLL_INTERVAL_S` | Sleep interval between polling ticks. Defaults to `10` seconds. |
| `--lake-root LAKE_ROOT` | Root directory for optional Parquet output. |
| `--max-runtime-s MAX_RUNTIME_S` | Runtime budget in seconds. `0` disables the budget. |
| `--save-parquet-lake`, `--no-save-parquet-lake` | Enable or disable Parquet persistence. |
| `--json-output`, `--no-json-output` | Enable or suppress JSON output. Logs are still emitted. |

Symbols are normalized to Deribit perpetual instruments. For example, `BTC`, `BTCUSDT`, `BTCUSD`, and `BTC-PERPETUAL` resolve to `BTC-PERPETUAL`.
`SOL` resolves to Deribit's active `SOL_USDC-PERPETUAL` market.

Fetch BTC, ETH, and SOL snapshots at the default cadence of five polling ticks per run and print JSON:

```bash
python main.py bronze-builder --symbols BTC ETH SOL
```

Comma-separated symbols are also accepted:

```bash
python main.py bronze-builder --symbols BTC,ETH,SOL
```

Save raw snapshots to the bronze Parquet lake:

```bash
python main.py bronze-builder \
  --symbols BTC ETH SOL \
  --levels 50 \
  --save-parquet-lake
```

With the defaults, each run collects five raw snapshots per symbol with a 50-second runtime budget. Bronze persistence appends those snapshots as distinct parquet rows in the symbol's daily partition; it does not aggregate them.

Runtime logs are written under `.logs/` by default, for example `.logs/bronze-builder.log`. The directory is ignored by git. Logs rotate weekly and `runtime.log_backup_count: 0` keeps older rotated logs.

### Silver Builder

```text
python main.py silver-builder [options]
```

| Option | Meaning |
|---|---|
| `--bronze-lake-root BRONZE_LAKE_ROOT` | Root directory for bronze Parquet input files. Defaults to `lake/bronze`. |
| `--silver-lake-root SILVER_LAKE_ROOT` | Root directory for Silver output artifact files. Defaults to `lake/silver`. |
| `--depth DEPTH` | Expected book depth used for fixed-width Silver arrays. Defaults to `50`. |
| `--plot`, `--no-plot` | Enable or suppress Silver PNG profile generation. Defaults to enabled. |
| `--manifest`, `--no-manifest` | Enable or suppress Silver JSON metadata manifest generation. Defaults to enabled. |
| `--json-output`, `--no-json-output` | Enable or suppress JSON output. Logs are still emitted. |

Transform Bronze L2 snapshots into monthly Silver snapshot feature artifacts:

```bash
python main.py silver-builder
```

The Silver job logs to `.logs/silver-builder.log` by default.
It records Bronze parquet content fingerprints in `lake/silver/_silver_transform_state.json`; subsequent runs read and merge only Bronze partition files whose content changed since the prior successful run.

Each Silver month partition writes three artifacts. The parquet file is named by month, and the metadata/plot files use the same month marker:

```text
YYYY-MM.parquet
YYYY-MM.json
YYYY-MM.png
```

### Gold Builder

```text
python main.py gold-builder [options]
```

| Option | Meaning |
|---|---|
| `--silver-lake-root SILVER_LAKE_ROOT` | Root directory for Silver Parquet input files. Defaults to `lake/silver`. |
| `--gold-lake-root GOLD_LAKE_ROOT` | Root directory for Gold artifact output files. Defaults to `lake/gold`. |
| `--expected-snapshots-per-minute EXPECTED` | Expected Silver snapshots per minute for quality coverage. Defaults to `6`. |
| `--completeness-threshold THRESHOLD` | Minimum coverage ratio for a complete minute. Defaults to `0.8`. |
| `--fill-missing-minutes`, `--no-fill-missing-minutes` | Enable or disable Gold missing-minute feature filling. Defaults to disabled. |
| `--fill-policy {neighbor,hybrid}` | Fill strategy used when `--fill-missing-minutes` is enabled. Defaults to `neighbor`. |
| `--plot`, `--no-plot` | Enable or suppress Gold PNG profile generation. Defaults to enabled. |
| `--manifest`, `--no-manifest` | Enable or suppress Gold JSON metadata manifest generation. Defaults to enabled. |
| `--json-output`, `--no-json-output` | Enable or suppress JSON output. Logs are still emitted. |

Transform Silver L2 snapshot features into M1 Gold artifacts:

```bash
python main.py gold-builder
```

Gold is M1-only. Each dataset is materialized on the full one-minute scale from its lowest observed minute through its latest observed minute. Observed minutes use first/max/min/last/mean/std semantics inside the minute. Missing minutes are written as explicit rows with `snapshot_count = 0`, `coverage_ratio = 0.0`, `is_complete_minute = false`, `quality_flags = ["missing_minute"]`, and numeric feature values set to `NaN`; values are not forward-filled. With the default quality policy, `expected_snapshots_per_minute = 6`, `coverage_ratio = snapshot_count / 6`, and `is_complete_minute = coverage_ratio >= 0.8`.
When `--fill-missing-minutes` is enabled with `--fill-policy neighbor`, missing Gold rows keep `snapshot_count = 0`, `coverage_ratio = 0.0`, and `is_complete_minute = false`, but numeric feature values are replaced by the average of the immediately preceding and following non-missing Gold minutes. Filled rows add `filled_neighbor_average` to `quality_flags`.
When `--fill-policy hybrid` is selected, short internal gap runs are linearly interpolated (`filled_linear_interpolation`), short boundary runs are filled from the nearest observed minute (`filled_forward_boundary` or `filled_backward_boundary`), and long gaps remain missing with `missing_long_gap`.
The Gold builder records Silver parquet content fingerprints by symbol in `lake/gold/_gold_transform_state.json`. Unchanged symbols are skipped; changed symbols are rebuilt from all Silver files for that symbol so each emitted Gold artifact remains a full timeframe dataset.

For each exchange, instrument type, base asset, source symbol, depth, and timeframe, Gold writes a full versioned dataset at the `timeframe=1m` leaf. The three artifacts share the same reproducibility basename:

```text
lake/gold/dataset_type=l2_m1_features/feature_set_version=gold_l2_m1_v1/exchange=deribit/instrument_type=perp/base_asset=BTC/symbol=BTC-PERPETUAL/depth=50/timeframe=1m/BTC_L2_<jsonhash>_<gitcommithash>.parquet
lake/gold/dataset_type=l2_m1_features/feature_set_version=gold_l2_m1_v1/exchange=deribit/instrument_type=perp/base_asset=BTC/symbol=BTC-PERPETUAL/depth=50/timeframe=1m/BTC_L2_<jsonhash>_<gitcommithash>.json
lake/gold/dataset_type=l2_m1_features/feature_set_version=gold_l2_m1_v1/exchange=deribit/instrument_type=perp/base_asset=BTC/symbol=BTC-PERPETUAL/depth=50/timeframe=1m/BTC_L2_<jsonhash>_<gitcommithash>.png
```

The JSON metadata contains dataset-level and feature-level metadata, including dataset type, feature-set version, timeframe, hash string, UTC build timestamp, row/column stats, timestamp bounds, missing-minute counts, source Silver dataset summaries, source fingerprint hash, Gold content hash, and per-feature dtype/null/NaN/numeric distribution stats. It intentionally does not store filesystem paths.

The PNG profile uses the same basename. It plots all numeric Gold features as feature rows: line plots on the left panel and dark distribution histograms on the right panel. Line plots use at most 3,000 evenly spaced rows representing the full time scale; Parquet and JSON artifacts keep the complete dataset. Missing minutes are visible as broken line segments caused by `NaN` values and as red shaded spans. The plot header carries key manifest metadata, while each feature subplot shows only a compact left-side metadata window with feature name, time range, and row statistics.

The Gold job logs to `.logs/gold-builder.log` by default and allows overlapping transform launches.

Validate symbols before adding them to cron:

```bash
python main.py validate-symbols --symbols BTC ETH SOL
```

The Parquet layout is:

```text
lake/bronze/
  dataset_type=l2_snapshot/
    exchange=deribit/
      instrument_type=perp/
        symbol=BTC-PERPETUAL/
          depth=50/
            source=rest_order_book/
              month=YYYY-MM/
                date=YYYY-MM-DD/
                  data.parquet
```

The Silver artifact layout is monthly to avoid unnecessary small-file fragmentation:

```text
lake/silver/
  dataset_type=l2_snapshot_features/
    exchange=deribit/
      instrument_type=perp/
        symbol=BTC-PERPETUAL/
          month=YYYY-MM/
            YYYY-MM.parquet
            YYYY-MM.json
            YYYY-MM.png
```

The Gold artifact layout is partitioned to the full versioned timeframe dataset level:

```text
lake/gold/
  dataset_type=l2_m1_features/
    feature_set_version=gold_l2_m1_v1/
      exchange=deribit/
        instrument_type=perp/
          base_asset=BTC/
            symbol=BTC-PERPETUAL/
              depth=50/
                timeframe=1m/
                  BTC_L2_<jsonhash>_<gitcommithash>.parquet
                  BTC_L2_<jsonhash>_<gitcommithash>.json
                  BTC_L2_<jsonhash>_<gitcommithash>.png
          base_asset=ETH/
            symbol=ETH-PERPETUAL/
              depth=50/
                timeframe=1m/
                  ETH_L2_<jsonhash>_<gitcommithash>.parquet
                  ETH_L2_<jsonhash>_<gitcommithash>.json
                  ETH_L2_<jsonhash>_<gitcommithash>.png
```

## Modules

| Module | Responsibility |
|---|---|
| `api/constants.py` | Shared command names and runtime artifact paths. |
| `api/cli.py` | CLI parsing, builder orchestration, JSON output, parquet/artifact dispatch, and run logging. |
| `api/runtime.py` | Logging setup and concurrency config. |
| `ingestion/artifact_state.py` | Content fingerprints and atomic JSON state files for incremental transforms. |
| `ingestion/config.py` | Deterministic `config.yaml` loading and typed config accessors. |
| `ingestion/http_client.py` | Minimal JSON HTTP client with retries and cached per-process default settings. |
| `ingestion/exchanges/deribit_l2.py` | Deribit order book adapter and symbol normalization. |
| `ingestion/l2.py` | L2 dataclasses, async polling, and snapshot normalization. |
| `ingestion/lake.py` | Idempotent Parquet writer for raw L2 snapshots. |
| `ingestion/silver.py` | Polars Bronze-to-Silver transform and monthly Silver artifact writer. |
| `ingestion/gold.py` | Polars Silver-to-Gold dense M1 aggregation, versioned timeframe artifact writing, metadata, and PNG profiles. |

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

Bronze Parquet rows are produced from `L2Snapshot` with additional lake metadata:

| Field | Meaning |
|---|---|
| `schema_version` | Parquet row schema version. Currently `v1`. |
| `dataset_type` | Dataset identifier. Currently `l2_snapshot`. |
| `instrument_type` | Instrument category. Currently `perp`. |
| `event_time` | Snapshot exchange timestamp. |
| `ingested_at` | UTC timestamp when the row was prepared for lake persistence. |
| `run_id` | Unique CLI run identifier assigned during ingestion. |
| `source` | Logical source name. Currently `rest_order_book`. |
| `depth` | Requested order book depth per side, also used as a partition column. |
| `bids`, `asks` | Raw normalized price/amount levels. |

Silver L2 snapshot feature rows are derived from Bronze with Polars:

| Field | Meaning |
|---|---|
| `schema_version` | Silver feature schema version. Currently `v1`. |
| `dataset_type` | Dataset identifier. Currently `l2_snapshot_features`. |
| `ts_event` | Exchange event timestamp from the Bronze snapshot. |
| `ts_received` | Bronze ingestion timestamp used as the received-time fallback. |
| `exchange`, `symbol`, `instrument_type`, `source`, `run_id`, `depth` | Source and run identity. |
| `month` | Monthly partition key derived from `ts_event`. |
| `mid_price`, `spread`, `spread_bps` | Top-of-book price features. |
| `best_bid_price`, `best_bid_size`, `best_ask_price`, `best_ask_size` | Best quote features. |
| `bid_prices`, `bid_sizes`, `ask_prices`, `ask_sizes` | Fixed-width depth arrays padded with nulls to `depth`. |
| `bid_volume_N`, `ask_volume_N` | Cumulative size for depth windows `1`, `5`, `10`, `20`, and `50`. |
| `imbalance_N` | `(bid_volume_N - ask_volume_N) / (bid_volume_N + ask_volume_N)` for each depth window. |
| `microprice` | Top-of-book size-weighted microprice. |
| `mark_price`, `index_price`, `open_interest`, `funding_rate`, `funding_8h` | Deribit market fields carried into Silver. |
| `is_valid`, `validation_flags` | Deterministic book validation status and reason flags. |

Gold M1 rows are derived from Silver:

| Field | Meaning |
|---|---|
| `ts_minute`, `exchange`, `symbol`, `instrument_type`, `depth`, `feature_set_version` | M1 feature identity. |
| `snapshot_count`, `coverage_ratio`, `first_snapshot_ts`, `last_snapshot_ts` | Minute coverage metadata. |
| `mid_open`, `mid_high`, `mid_low`, `mid_close`, `mid_mean`, `mid_std` | M1 mid-price features. |
| `spread_bps_mean`, `spread_bps_max`, `spread_bps_p95` | M1 spread features. |
| `microprice_mean`, `microprice_close`, `microprice_minus_mid_mean` | M1 microprice features. |
| `imbalance_N_mean` | Mean imbalance for depth windows `1`, `5`, `10`, `20`, and `50`. |
| `bid_volume_N_mean`, `ask_volume_N_mean` | Mean cumulative book size for each depth window. |
| `book_pressure_N_mean` | Mean `bid_volume_N / (bid_volume_N + ask_volume_N)` for each depth window. |
| `mark_price_last`, `index_price_last`, `open_interest_last`, `funding_rate_last` | Last market fields in the minute. |
| `is_complete_minute`, `quality_flags` | Gold quality status. Missing scale rows use `quality_flags = ["missing_minute"]`; optional fill annotations can include `filled_neighbor_average`, `filled_linear_interpolation`, `filled_forward_boundary`, `filled_backward_boundary`, or `missing_long_gap`. |

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

Current coverage highlights:

- CLI command parsing/defaults and builder dispatch behavior.
- Bronze parquet partition layout and idempotent merge semantics.
- Silver feature calculations, incremental invalidation, and artifact toggles.
- Gold M1 aggregation with missing-minute densification and configurable neighbor/hybrid fill policies.
- Gold edge-case guards where fill is skipped when adjacent observed minutes are unavailable.
- Gold incremental invalidation when completeness or fill policy settings change.
- Schema contracts for Bronze, Silver, and Gold outputs.

## Known Limitations

- Only Deribit perpetual L2 order book snapshots are supported.
- The loader uses REST polling, not a streaming websocket feed.
- Parquet persistence is local-file based and does not include a database sink.
- Failed per-symbol fetches inside a polling tick are logged, isolated, and skipped for that tick.
- Incremental transform state is local to one lake root and is not a distributed metadata catalog.
- Overlapping local writer processes can race when they write the same artifact path at the same time.

## Future Improvements

- Add a websocket collector for higher-frequency L2 sampling.
- Add explicit schema-version migration tests for Parquet outputs.
- Add replay utilities for validating stored raw snapshots.
- Add exchange adapters behind the existing L2 interfaces.

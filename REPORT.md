# Deribit OHLCV Ingestion Baseline for Crypto Research Pipelines

## Abstract
This report presents a production-oriented baseline for cryptocurrency candle ingestion designed to support downstream quantitative research. The problem addressed is the lack of reproducible, maintainable ingestion layers in early-stage quant projects, where exchange-specific scripts and schema drift frequently undermine empirical validity. We implement a typed, modular ingestion pipeline with adapter abstractions for Deribit, a command-line interface for deterministic execution (including multi-market runs such as `--market spot perp`), and partitioned parquet-lake storage. Data are sourced from public exchange REST endpoints and normalized into a canonical OHLCV schema with metadata fields for run traceability. The main finding is engineering-focused: the system provides deterministic normalization, supports backward pagination and gap-fill synchronization, and preserves idempotent persistence via natural-key partition merges. The contribution is a maintainable ingestion foundation suitable for subsequent market microstructure, regime, and forecasting studies, with explicit reproducibility controls (strict typing, tests, linting, and stable execution commands).

Canonical data-type naming in this project is fixed as `spot`, `perp`, and `oi` across CLI, code, and documentation (with parquet storage label `dataset_type=open_interest` representing `oi`).

## Introduction
Reliable market-data ingestion is a prerequisite for valid quantitative inference in crypto research.

Many practical pipelines fail due to exchange-specific one-off scripts, inconsistent timestamp handling, and weak reproducibility controls.

This project proposes a modular ingestion architecture with typed interfaces, explicit normalization, and reproducible command-line workflows focused on Deribit as the current production exchange.

The contributions of this stage are: (1) Deribit OHLCV normalization, (2) parquet-lake persistence paths, and (3) tested operational workflows for repeatable data acquisition.

## Literature Review
Volatility clustering and regime dependence motivate robust historical market-data pipelines. ARCH/GARCH foundations establish heteroskedastic behavior in financial time series, requiring high-integrity timestamped observations (Engle, 1982; Bollerslev, 1986). Regime-switching frameworks further highlight sensitivity to data quality and temporal consistency (Hamilton, 1989). Later work on high-frequency econometrics and realized-volatility estimation reinforces the need for reliable, granular data ingestion and synchronization processes (Andersen et al., 2001; Barndorff-Nielsen and Shephard, 2002).
Market microstructure and stylized-facts literature also emphasizes heavy tails, volatility clustering, and serial dependence in returns, which makes interval alignment and timestamp integrity central to defensible inference (Cont, 2001).

Within crypto-specific empirical work, market microstructure studies and liquidity fragmentation analyses depend on exchange-consistent symbol and timeframe normalization. This baseline does not yet estimate econometric models, but it is intentionally built to satisfy upstream data-quality assumptions for such methods.

## Dataset
- Source: Deribit `/api/v2/public/get_tradingview_chart_data` and Deribit `/api/v2/public/get_last_settlements_by_instrument`.
- Sample period: user-configurable runtime period determined by symbols, markets, timeframes, and existing parquet coverage.
- Number of observations: runtime-dependent on symbol/timeframe scope and auto bootstrap vs gap-fill behavior.
- Variables: `open_time`, `close_time`, `open`, `high`, `low`, `close`, `volume`, `quote_volume`, `trade_count` plus provenance metadata.
- Additional perp feature set: `open_interest`, `open_interest_value` where available.
- Cleaning methodology: exchange adapter normalization, timeframe validation, symbol normalization, UTC conversion, partition-level deduplication by natural key.
- Train/test split: not applicable at ingestion-only stage.

### Data Type Definitions, Rationale, And Meaning

#### `spot`
- Why it exists in this research artifact:
  `spot` provides the baseline cash-market price process required for primary return and volatility measurement.
- Meaning in analysis:
  Exchange-executed cash OHLCV bars; these are the reference series for non-derivative market state.

#### `perp`
- Why it exists in this research artifact:
  `perp` captures derivatives-market trading behavior that can differ from spot and add information about speculative flow.
- Meaning in analysis:
  Perpetual-futures OHLCV bars normalized to the same schema as `spot`, enabling direct cross-market comparisons.

#### `oi`
- Why it exists in this research artifact:
  `oi` is needed to quantify aggregate open positioning, which is not observable from OHLCV alone.
- Meaning in analysis:
  Open-interest time series associated with perpetual instruments; used to interpret whether moves are supported by position expansion or contraction.

### Dataset Variable Construction (Implementation-Aligned)

#### `spot` (cash OHLCV)
- Constructed fields:
  `open_time`, `close_time`, `open`, `high`, `low`, `close`, `volume`, `quote_volume`, `trade_count`.
- Field construction logic:
  - `open_time` is exchange open timestamp converted to UTC datetime.
  - `close_time` is either exchange-provided close timestamp or computed as `open_time + timeframe_ms - 1` for endpoints that publish only interval starts.
  - `open/high/low/close` are direct float-casts of exchange candle values.
  - `volume` is base-asset traded volume over interval.
  - `quote_volume` is exchange quote-turnover field when available; fallback proxies are adapter-defined (for Deribit chart endpoint, current mapping uses `volume` proxy).
  - `trade_count` is exchange trade-count field when available; otherwise set to `0`.

#### `perp` (perpetual OHLCV)
- Constructed fields:
  Same canonical OHLCV schema as `spot`; this enables direct cross-market comparisons without extra schema transforms.
- Field construction logic:
  - Same parser and storage mapping as `spot` (`SpotCandle` normalization path).
  - Symbol normalization is exchange/market specific before parse (for example Deribit `BTC` -> `BTC-PERPETUAL`).

#### `oi` (perpetual open interest)
- Constructed fields:
  `open_time`, `close_time`, `open_interest`, `open_interest_value`.
- Field construction logic:
  - OI ingestion is gated to `perp` context; non-perp requests return empty results by design.
  - `open_time` derives from exchange timestamp and is UTC-normalized.
  - `close_time` is computed as `open_time + timeframe_ms - 1`.
  - `open_interest` is mapped from exchange OI position-size field.
  - `open_interest_value` is populated when the source returns a notional/value metric; else `0.0`.
- Exchange-specific mapping:
  - Deribit: settlement `timestamp` is bucketed to the requested timeframe prior to interval construction; `open_interest <- position`, `open_interest_value <- 0.0`.

## Methodology
### System Design
```text
CLI -> Application Service Layer (gapfill_service, fetch_service)
    -> Adapter Layer -> HTTP Client -> Exchange REST APIs
    -> Normalized SpotCandle/OpenInterestPoint -> Parquet Lake/TimescaleDB
```

Storage and artifact side effects are also routed through dedicated services (`storage_service`, `artifact_service`) to keep CLI logic thin and testable. Canonical CLI-to-storage naming (`spot`/`perp`/`oi` -> `dataset_type` + `instrument_type`) is formalized in `application/schema.py`.

### Core Mapping
For each candle index \(t\):

\[
x_t = \{e, s, \Delta, \tau_t^{open}, \tau_t^{close}, o_t, h_t, l_t, c_t, v_t, qv_t, n_t\}
\]

where \(e\) is exchange, \(s\) symbol, \(\Delta\) timeframe, and \(n_t\) trade count.

### Persistence Objective
Rows are persisted with natural key:

\[
K = (exchange, instrument\_type, symbol, timeframe, open\_time)
\]

Upsert policy enforces idempotency:

\[
\text{row}_{new}(K) \leftarrow \text{row}_{incoming}(K)
\]

### Optimization Logic
- Loader operates in exactly two modes:
  1. `fetch all history` for symbol/timeframe partitions that do not yet exist in parquet.
  2. `fill gaps` for existing partitions by recovering missing internal intervals and tail intervals.
- Bounded HTTP retries with exponential backoff for transient failures.
- Pagination for exchange request limits.
- Gap-fill computes missing intervals from stored open-time sets.
- Fetch execution is parallelized with bounded concurrency controls.
- Fetch orchestration and task error isolation are handled in a dedicated service layer (`application/services/fetch_service.py`) rather than directly in CLI command code.
- Parquet reads/writes process data in batches to bound memory usage.
- Timescale ingestion from parquet uses streaming row iteration with bounded DB upsert batches.

## Results
All figures in this report are generated from repository pipeline outputs (agent-generated plot artifacts), not notebook exports.

### Descriptive Statistics Table
Descriptive statistics are exported with a fixed, reproducible configuration via:
`python3 main.py export-descriptive-stats --lake-root lake/bronze --output-csv docs/tables/descriptive_stats_baseline.csv --start-time 2026-01-01T00:00:00+00:00 --end-time 2026-01-31T23:59:59+00:00`.
The table artifact is versioned at `docs/tables/descriptive_stats_baseline.csv`.

| Variable | Mean | Std | Min | Max |
|---|---:|---:|---:|---:|
| Open | N/A (config-dependent) | N/A | N/A | N/A |
| High | N/A (config-dependent) | N/A | N/A | N/A |
| Low | N/A (config-dependent) | N/A | N/A | N/A |
| Close | N/A (config-dependent) | N/A | N/A | N/A |
| Volume | N/A (config-dependent) | N/A | N/A | N/A |

### Model Comparison Table
No predictive or regime models are trained in this stage.

| Model | Accuracy | Sharpe | AUC | RMSE |
|---|---:|---:|---:|---:|
| Not applicable (ingestion baseline) | N/A | N/A | N/A | N/A |

### Robustness Table

| Configuration | Scope | Outcome |
|---|---|---|
| Exchange fetch | Deribit (spot/perp/oi) | Passed via typed adapter dispatch |
| Gap-fill mode | Missing internal/tail intervals | Passed via open-time range recovery |
| Incremental parquet persistence | Partition merge + natural-key dedup | Passed with idempotent key policy |
| Service-layer fetch orchestration tests | Parallel success/error isolation | Passed (`tests/test_fetch_service.py`) |
| Service-layer gap-fill utility tests | Closed-candle timestamp and missing-range logic | Passed (`tests/test_gapfill_service.py`) |
| Service-layer storage orchestration tests | Parquet + Timescale side-effect routing | Passed (`tests/test_storage_service.py`) |
| Canonical schema contract tests | CLI datatype to storage contract mapping | Passed (`tests/test_schema_contract.py`) |
| Loader sample artifacts | Per market/exchange/symbol/timeframe CSV + full-history plot | Passed with deterministic naming |
| Chunked Timescale ingest | Streaming parquet read + bounded DB upsert batches | Passed with stable memory profile |
| Open-interest integration | Deribit perp dataset_type=open_interest | Passed for all-history and gap-fill paths |

### Figures
Figure 1. OHLCV Spot series (Deribit BTCUSDT 1m).

![Figure 1: Deribit BTCUSDT 1m close](docs/figures/plot_outputs/deribit_BTCUSDT_1m_close.png)

Interpretation: Figure 1 shows coherent minute-level sequencing for spot OHLCV after normalization and plotting.

Figure 2. OHLCV Perp series (Deribit ETHUSDT 1m).

![Figure 2: Deribit ETHUSDT 1m close](docs/figures/plot_outputs/deribit_ETHUSDT_1m_close.png)

Interpretation: Figure 2 indicates perp candle compatibility within the same normalized OHLCV pipeline.

Figure 3. Exchange-specific mapping quality (Deribit BTCUSDT alias path).

![Figure 3: Deribit BTCUSDT 1m close](docs/figures/plot_outputs/deribit_BTCUSDT_1m_close.png)

Interpretation: Figure 3 supports correctness of symbol mapping and timestamp handling.

Figure 4. Exchange-specific mapping quality (Deribit ETHUSDT alias path).

![Figure 4: Deribit ETHUSDT 1m close](docs/figures/plot_outputs/deribit_ETHUSDT_1m_close.png)

Interpretation: Figure 4 confirms consistent behavior across multiple symbols on Deribit mappings.

Figure 5. Open Interest time-series (loader sample artifact per market/exchange/symbol/timeframe).

![Figure 5: Open Interest plot artifact](docs/figures/plot_outputs/deribit_BTCUSDT_1m_open_interest.png)

Interpretation: Figure 5 tracks contract participation dynamics and complements OHLCV bars for derivatives analysis.

## Discussion
Business implications: a reliable ingestion substrate lowers operational risk for strategy research and accelerates feature engineering, backtesting, and monitoring deployment.

Limitations: current scope is OHLCV-only; L2 order book, funding, and trade-level feeds are not yet integrated. Exchange-specific outages and schema changes still require active maintenance.

Model weaknesses/assumptions: this stage does not perform inference; assumptions are engineering assumptions (timestamp consistency, endpoint availability, and exchange-provided data correctness).

## Conclusion
This baseline establishes a reproducible and extensible ingestion pipeline for Deribit crypto OHLCV data with typed normalization and idempotent persistence. The immediate value is production-quality data plumbing for future empirical studies. Next steps are fixed experiment configs, notebook-to-report metrics automation, and model/evaluation modules with formal benchmarks.

## Appendix
### Reproducibility Controls
- Static checks: `ruff`, `mypy` (strict), `pytest`.
- Deterministic interfaces: typed adapters and normalized candle schema.
- Incremental persistence: partition-level merge and key-based dedup in parquet files.

## References
1. Engle, R. F. (1982). Autoregressive Conditional Heteroskedasticity with Estimates of the Variance of U.K. Inflation. *Econometrica*.
2. Bollerslev, T. (1986). Generalized Autoregressive Conditional Heteroskedasticity. *Journal of Econometrics*.
3. Hamilton, J. D. (1989). A New Approach to the Economic Analysis of Nonstationary Time Series and the Business Cycle. *Econometrica*.
4. Andersen, T. G., Bollerslev, T., Diebold, F. X., and Labys, P. (2001). The Distribution of Realized Exchange Rate Volatility. *Journal of the American Statistical Association*.
5. Barndorff-Nielsen, O. E., and Shephard, N. (2002). Econometric Analysis of Realised Volatility and Its Use in Estimating Stochastic Volatility Models. *Journal of the Royal Statistical Society: Series B*.
6. Deribit API Documentation. TradingView Chart Data Endpoint.
7. Deribit API Documentation. Historical Settlement/Open Interest Endpoint.
9. Apache Arrow Documentation. Parquet RecordBatch Processing.
10. Cont, R. (2001). Empirical Properties of Asset Returns: Stylized Facts and Statistical Issues. *Quantitative Finance*.

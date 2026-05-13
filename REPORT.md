# crypto-l2-loader Technical State Report

## Summary

`crypto-l2-loader` is currently a focused Deribit Level 2 ingestion utility. It polls bounded REST order book snapshots, writes raw Bronze Parquet partitions, derives monthly Silver fixed-width L2 feature artifacts, and builds dense Gold M1 feature datasets.

The project is not a research-report or modeling repository. Its maintained scope is operational L2 ingestion and reproducible local-file lake transforms.

## Current Architecture

- `api/cli.py` exposes `bronze-builder`, `silver-builder`, `gold-builder`, and `validate-symbols`.
- `api/runtime.py` provides logging and bounded fetch concurrency.
- `ingestion/exchanges/deribit_l2.py` adapts Deribit's public order book API into normalized payloads.
- `ingestion/l2.py` defines `L2Snapshot`, async polling, and Bronze row normalization.
- `ingestion/lake.py` writes idempotent daily Bronze Parquet partitions.
- `ingestion/silver.py` transforms changed Bronze parquet inputs into monthly Silver feature artifacts.
- `ingestion/gold.py` transforms changed Silver symbol partitions into full dense M1 Gold datasets with optional Gold-level missing-minute filling.
- `ingestion/artifact_state.py` stores content fingerprints for incremental transform state.

## Data Layers

### Bronze

Bronze stores raw normalized L2 snapshots under daily partitions:

```text
lake/bronze/dataset_type=l2_snapshot/exchange=deribit/instrument_type=perp/symbol=.../depth=.../source=rest_order_book/month=YYYY-MM/date=YYYY-MM-DD/data.parquet
```

Rows are deduplicated by exchange, instrument type, symbol, depth, source, and event time. Writes use staging files and atomic replacement.

### Silver

Silver transforms Bronze snapshots into fixed-width L2 feature rows with top-of-book, spread, cumulative depth volumes, imbalances, microprice, carried market fields, and validation flags.

The builder records Bronze input content fingerprints in `lake/silver/_silver_transform_state.json`. If inputs are unchanged, the builder exits without rewriting artifacts. If a Bronze partition changes, only that changed input file is read, then merged idempotently into the relevant monthly Silver partition.

### Gold

Gold aggregates Silver features into dense M1 datasets. Missing minutes are explicit rows with zero coverage, `quality_flags = ["missing_minute"]`, and numeric features set to `NaN`.
When the Gold fill option is enabled, missing rows keep zero coverage but numeric features are averaged from the immediately preceding and following non-missing Gold minutes. Filled rows add `filled_neighbor_average` to their quality flags.

The builder records Silver input content fingerprints by symbol in `lake/gold/_gold_transform_state.json`. Unchanged symbols are skipped. Changed symbols are rebuilt from all Silver files for that symbol, preserving full-timeframe Gold outputs while avoiding cross-symbol rescans.

Gold artifact hashes include source fingerprints, source summary, schema, transform settings, git commit, and a content hash of the output Gold dataframe. Metadata stores a source fingerprint digest and Gold content hash without embedding filesystem paths.

## Test And Quality State

The test suite covers:

- CLI defaults, JSON output, and builder dispatch.
- HTTP retry/default config behavior.
- Deribit symbol normalization and adapter payload validation.
- L2 polling concurrency, partial failure isolation, and runtime budgets.
- Bronze partition layout and idempotent row persistence.
- Silver feature math, incremental state, idempotent monthly merge behavior, and artifact toggles.
- Gold M1 aggregation, missing-minute densification/filling, fill edge-case guards, content-sensitive artifact hashes, plot sampling, incremental symbol rebuilds, and artifact toggles.
- Gold incremental invalidation when completeness threshold or fill policy settings change.
- Explicit Bronze, Silver, and Gold schema contracts.

Current expected verification commands:

```bash
.venv/bin/python -m ruff check .
.venv/bin/python -m mypy .
.venv/bin/python -m pytest
```

## Operational Notes

- The system is local-file based; transform state is local to the configured lake roots.
- Process-level CLI locks are intentionally disabled so overlapping scheduled jobs can run.
- Runtime logs are written under `.logs/` and are ignored by git.

## Known Limitations

- Only Deribit perpetual L2 snapshots are supported.
- The collector uses REST polling rather than websocket streaming.
- There is no distributed metadata catalog, database sink, or multi-host locking layer.
- Incremental state is content-fingerprint based and assumes writers use the same local lake roots.
- Overlapping local writers can race when they target the same artifact path.

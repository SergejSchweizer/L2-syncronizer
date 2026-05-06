# crypto-l2-loader Refactor and Optimization Report

## Summary

This update refactors and optimizes the Silver and Gold transform pipeline, improves CLI behavior, and documents the changes in `README.md`.

## Key Improvements

- Added CLI steering flags to both Silver and Gold builders:
  - `--plot` / `--no-plot`
  - `--manifest` / `--no-manifest`
- Refactored CLI parser registration with a shared `_boolean_optional_flag()` helper.
- Optimized Gold artifact writing by computing metadata once when either plot or manifest output is enabled.
- Preserved default behavior: both plot and manifest generation remain enabled unless explicitly disabled.

## Testing and Validation

- Ran unit tests for the affected modules:
  - `tests/test_silver.py`
  - `tests/test_gold.py`
  - `tests/test_cli_lock.py`
- Result: `30 passed`
- Linting with Ruff passed for updated files.

## Files Changed

- `api/cli.py`
- `ingestion/gold.py`
- `README.md`
- `REPORT.md`
- `tests/test_cli_lock.py`
- `tests/test_silver.py`
- `tests/test_gold.py`

## Notes

- The report is intentionally concise and focused on the current codebase improvements.
- No research or modeling artifacts were added.

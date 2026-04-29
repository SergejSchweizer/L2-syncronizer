"""Sample artifact generation service for loader runs."""

from __future__ import annotations

import logging
import re
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd

from application.dto import ArtifactOptionsDTO, LoaderStorageDTO
from ingestion.funding import FundingPoint
from ingestion.lake import candle_record, open_interest_record
from ingestion.open_interest import OpenInterestPoint
from ingestion.plotting import save_candle_plots, save_funding_plot, save_open_interest_plot
from ingestion.spot import Market, SpotCandle


def _funding_open_time(item: FundingPoint) -> datetime:
    """Return funding row open timestamp for stable sorting."""

    return item.open_time


def write_loader_samples_dto(
    storage: LoaderStorageDTO,
    logger: logging.Logger,
    options: ArtifactOptionsDTO | None = None,
    save_candle_plots_fn: Callable[..., list[str]] = save_candle_plots,
    save_open_interest_plot_fn: Callable[..., str] = save_open_interest_plot,
    save_funding_plot_fn: Callable[..., str] = save_funding_plot,
) -> None:
    """Write grouped CSV and full-history plot sample artifacts."""

    resolved_options = options or ArtifactOptionsDTO()
    sample_dir = Path("samples")
    sample_dir.mkdir(parents=True, exist_ok=True)

    run_id = f"sample_{datetime.now(UTC).strftime('%Y%m%dT%H%M%S%fZ')}"
    ingested_at = datetime.now(UTC)

    def _safe_name(value: str) -> str:
        return re.sub(r"[^A-Za-z0-9_.-]+", "_", value)

    for market, candle_by_exchange in storage.candles.items():
        for exchange, candle_by_symbol in candle_by_exchange.items():
            for symbol_key, candles in candle_by_symbol.items():
                if not candles:
                    continue
                timeframe = candles[0].interval
                base_name = (
                    f"{market}_{_safe_name(exchange)}_{_safe_name(symbol_key)}_"
                    f"{_safe_name(timeframe)}_sample_10_rows"
                )
                rows = [
                    candle_record(
                        candle=item,
                        market=market,
                        run_id=run_id,
                        ingested_at=ingested_at,
                    )
                    for item in candles
                ]
                frame = pd.DataFrame(rows)
                sample = frame.sample(n=10, replace=len(frame) < 10, random_state=42)
                csv_path = sample_dir / f"{base_name}.csv"
                sample.to_csv(csv_path, index=False)
                logger.info("Sample CSV written rows=%s path=%s", len(sample), csv_path)

                if resolved_options.generate_plots:
                    generated = save_candle_plots_fn(
                        candles_by_exchange={exchange: {symbol_key: candles}},
                        output_dir=str(sample_dir),
                        price_field="spot",
                    )
                    if generated:
                        source = Path(generated[0])
                        target = sample_dir / f"{base_name}.png"
                        if source.resolve() != target.resolve():
                            source.replace(target)
                        logger.info("Sample plot written path=%s", target)

    for market, oi_by_exchange in storage.open_interest.items():
        for exchange, oi_by_symbol in oi_by_exchange.items():
            for symbol_key, oi_items in oi_by_symbol.items():
                if not oi_items:
                    continue
                timeframe = oi_items[0].interval
                base_name = (
                    f"oi_{market}_{_safe_name(exchange)}_{_safe_name(symbol_key)}_{_safe_name(timeframe)}_sample_10_rows"
                )
                rows = [
                    open_interest_record(item=item, market=market, run_id=run_id, ingested_at=ingested_at)
                    for item in oi_items
                ]
                frame = pd.DataFrame(rows)
                sample = frame.sample(n=10, replace=len(frame) < 10, random_state=42)
                csv_path = sample_dir / f"{base_name}.csv"
                sample.to_csv(csv_path, index=False)
                logger.info("Sample CSV written rows=%s path=%s", len(sample), csv_path)

                if resolved_options.generate_plots:
                    sorted_items = sorted(oi_items, key=lambda value: value.open_time)
                    plot_path = sample_dir / f"{base_name}.png"
                    save_open_interest_plot_fn(
                        exchange=exchange,
                        symbol=symbol_key,
                        interval=timeframe,
                        times=[item.open_time for item in sorted_items],
                        open_interest_values=[item.open_interest for item in sorted_items],
                        output_path=str(plot_path),
                    )
                    logger.info("Sample plot written path=%s", plot_path)

    for market, funding_by_exchange in storage.funding.items():
        for exchange, funding_by_symbol in funding_by_exchange.items():
            for symbol_key, funding_items in funding_by_symbol.items():
                if not funding_items:
                    continue
                timeframe = funding_items[0].interval
                base_name = (
                    f"funding_{market}_{_safe_name(exchange)}_{_safe_name(symbol_key)}_{_safe_name(timeframe)}_sample_10_rows"
                )
                rows = [
                    {
                        "schema_version": "v1",
                        "dataset_type": "funding",
                        "exchange": item.exchange,
                        "symbol": item.symbol,
                        "instrument_type": market,
                        "open_time": item.open_time,
                        "close_time": item.close_time,
                        "timeframe": item.interval,
                        "funding_rate": item.funding_rate,
                        "index_price": item.index_price,
                        "mark_price": item.mark_price,
                    }
                    for item in funding_items
                ]
                frame = pd.DataFrame(rows)
                sample = frame.sample(n=10, replace=len(frame) < 10, random_state=42)
                csv_path = sample_dir / f"{base_name}.csv"
                sample.to_csv(csv_path, index=False)
                logger.info("Sample CSV written rows=%s path=%s", len(sample), csv_path)

                if resolved_options.generate_plots:
                    funding_sorted_items = sorted(funding_items, key=_funding_open_time)
                    plot_path = sample_dir / f"{base_name}.png"
                    save_funding_plot_fn(
                        exchange=exchange,
                        symbol=symbol_key,
                        interval=timeframe,
                        times=[item.open_time for item in funding_sorted_items],
                        funding_values=[item.funding_rate for item in funding_sorted_items],
                        output_path=str(plot_path),
                    )
                    logger.info("Sample plot written path=%s", plot_path)


def write_loader_samples(
    candles_for_storage: dict[Market, dict[str, dict[str, list[SpotCandle]]]],
    open_interest_for_storage: dict[Market, dict[str, dict[str, list[OpenInterestPoint]]]],
    logger: logging.Logger,
    generate_plots: bool = True,
    save_candle_plots_fn: Callable[..., list[str]] = save_candle_plots,
    save_open_interest_plot_fn: Callable[..., str] = save_open_interest_plot,
    funding_for_storage: dict[Market, dict[str, dict[str, list[FundingPoint]]]] | None = None,
    save_funding_plot_fn: Callable[..., str] = save_funding_plot,
) -> None:
    """Backward-compatible wrapper using DTO service."""

    write_loader_samples_dto(
        storage=LoaderStorageDTO(
            candles=candles_for_storage,
            open_interest=open_interest_for_storage,
            funding=funding_for_storage or {},
        ),
        logger=logger,
        options=ArtifactOptionsDTO(generate_plots=generate_plots),
        save_candle_plots_fn=save_candle_plots_fn,
        save_open_interest_plot_fn=save_open_interest_plot_fn,
        save_funding_plot_fn=save_funding_plot_fn,
    )

"""Shared DTOs for loader orchestration and service boundaries."""

from __future__ import annotations

from dataclasses import dataclass, field

from ingestion.funding import FundingPoint
from ingestion.open_interest import OpenInterestPoint
from ingestion.spot import Exchange, Market, SpotCandle


@dataclass(frozen=True)
class CandleFetchTaskDTO:
    """One OHLCV fetch task request.

    Example:
        ```python
        task = CandleFetchTaskDTO(exchange="deribit", market="spot", symbol="BTC", timeframe="1m")
        ```
    """

    exchange: Exchange
    market: Market
    symbol: str
    timeframe: str


@dataclass(frozen=True)
class OpenInterestFetchTaskDTO:
    """One open-interest fetch task request.

    Example:
        ```python
        task = OpenInterestFetchTaskDTO(exchange="deribit", symbol="BTC", timeframe="1m")
        ```
    """

    exchange: Exchange
    symbol: str
    timeframe: str


@dataclass(frozen=True)
class FundingFetchTaskDTO:
    """One funding fetch task request.

    Example:
        ```python
        task = FundingFetchTaskDTO(exchange="deribit", symbol="ETH", timeframe="1h")
        ```
    """

    exchange: Exchange
    symbol: str
    timeframe: str


@dataclass
class CandleFetchResultDTO:
    """OHLCV fetch outcomes keyed by task tuple.

    Example:
        ```python
        result = CandleFetchResultDTO()
        ```
    """

    rows: dict[tuple[Exchange, Market, str, str], list[SpotCandle]] = field(default_factory=dict)
    errors: dict[tuple[Exchange, Market, str, str], str] = field(default_factory=dict)


@dataclass
class OpenInterestFetchResultDTO:
    """Open-interest fetch outcomes keyed by task tuple.

    Example:
        ```python
        result = OpenInterestFetchResultDTO()
        ```
    """

    rows: dict[tuple[Exchange, str, str], list[OpenInterestPoint]] = field(default_factory=dict)
    errors: dict[tuple[Exchange, str, str], str] = field(default_factory=dict)


@dataclass
class FundingFetchResultDTO:
    """Funding fetch outcomes keyed by task tuple.

    Example:
        ```python
        result = FundingFetchResultDTO()
        ```
    """

    rows: dict[tuple[Exchange, str, str], list[FundingPoint]] = field(default_factory=dict)
    errors: dict[tuple[Exchange, str, str], str] = field(default_factory=dict)


@dataclass
class LoaderStorageDTO:
    """Normalized in-memory storage payload for loader side effects.

    Example:
        ```python
        storage = LoaderStorageDTO()
        ```
    """

    candles: dict[Market, dict[str, dict[str, list[SpotCandle]]]] = field(default_factory=dict)
    open_interest: dict[Market, dict[str, dict[str, list[OpenInterestPoint]]]] = field(default_factory=dict)
    funding: dict[Market, dict[str, dict[str, list[FundingPoint]]]] = field(default_factory=dict)


@dataclass(frozen=True)
class PersistOptionsDTO:
    """Controls which storage sinks are executed.

    Example:
        ```python
        options = PersistOptionsDTO(
            save_parquet_lake=True,
            save_timescaledb=False,
            lake_root="lake/bronze",
            timescaledb_schema="market_data",
            create_schema=True,
            oi_requested=True,
        )
        ```
    """

    save_parquet_lake: bool
    save_timescaledb: bool
    lake_root: str
    timescaledb_schema: str
    create_schema: bool
    oi_requested: bool
    funding_requested: bool = False


@dataclass
class PersistResultDTO:
    """Persist side-effect summary payload.

    Example:
        ```python
        result = PersistResultDTO(parquet_files=["lake/bronze/.../data.parquet"])
        ```
    """

    parquet_files: list[str] = field(default_factory=list)
    timescaledb_summary: dict[str, int | str] | None = None

    def to_output_dict(self) -> dict[str, object]:
        """Convert DTO to existing CLI output keys."""

        output: dict[str, object] = {}
        if self.parquet_files:
            output["_parquet_files"] = self.parquet_files
        if self.timescaledb_summary is not None:
            output["_timescaledb"] = self.timescaledb_summary
        return output


@dataclass(frozen=True)
class ArtifactOptionsDTO:
    """Controls artifact generation behavior.

    Example:
        ```python
        options = ArtifactOptionsDTO(generate_plots=True)
        ```
    """

    generate_plots: bool = True

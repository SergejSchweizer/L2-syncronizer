"""Canonical dataset/instrument schema contracts for market data storage."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

CliDataType = Literal["spot", "perp", "oi", "funding"]
DatasetType = Literal["ohlcv", "open_interest", "funding", "l2_orderbook"]
InstrumentType = Literal["spot", "perp"]


@dataclass(frozen=True)
class DatasetContract:
    """Mapping contract between CLI data type and storage partition semantics."""

    cli_data_type: CliDataType
    dataset_type: DatasetType
    instrument_type: InstrumentType


_CONTRACTS: dict[CliDataType, DatasetContract] = {
    "spot": DatasetContract(cli_data_type="spot", dataset_type="ohlcv", instrument_type="spot"),
    "perp": DatasetContract(cli_data_type="perp", dataset_type="ohlcv", instrument_type="perp"),
    "oi": DatasetContract(cli_data_type="oi", dataset_type="open_interest", instrument_type="perp"),
    "funding": DatasetContract(cli_data_type="funding", dataset_type="funding", instrument_type="perp"),
}


def dataset_contract(cli_data_type: CliDataType) -> DatasetContract:
    """Return canonical storage contract for one CLI data type."""

    return _CONTRACTS[cli_data_type]

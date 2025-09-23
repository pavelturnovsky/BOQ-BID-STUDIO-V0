"""Bid Studio core package.

This package provides the modular building blocks for loading, comparing and
searching supplier bids against a master bill of quantities.  It is designed to
power both the command line interface distributed with this repository and any
future user interfaces that want to build on top of the shared application
logic.
"""

from .comparison import ComparisonResult, compare_bids
from .config import (
    AppConfig,
    BidConfig,
    ColumnMapping,
    ComparisonConfig,
    OutputConfig,
    SearchConfig,
    load_config,
)
from .io import load_bid_dataset, load_master_dataset

from .reporting import export_comparison
from .search import SearchProvider, TfidfSearchProvider

__all__ = [
    "AppConfig",
    "BidConfig",
    "ColumnMapping",
    "ComparisonConfig",
    "ComparisonResult",
    "OutputConfig",
    "SearchConfig",
    "SearchProvider",
    "TfidfSearchProvider",
    "compare_bids",
    "export_comparison",
    "load_bid_dataset",
    "load_config",
    "load_master_dataset",
]

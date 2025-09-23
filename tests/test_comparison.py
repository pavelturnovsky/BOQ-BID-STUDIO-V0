from pathlib import Path

from bidstudio.comparison import compare_bids
from bidstudio.config import load_config
from bidstudio.io import load_bid_dataset, load_master_dataset
from bidstudio.search import TfidfSearchProvider


def test_compare_bids_produces_summary_and_unmatched():
    config = load_config(Path("config/config.yaml"))

    master = load_master_dataset(
        config.master,
        config.columns,
        config.comparison.key_columns,
        chunk_size=1000,
    )
    bids = [
        load_bid_dataset(bid, config.columns, config.comparison.key_columns, chunk_size=1000)
        for bid in config.bids
    ]

    provider = TfidfSearchProvider()
    result = compare_bids(
        master,
        bids,
        config.comparison,
        search_provider=provider,
        search_fields=config.search.fields,
        search_top_k=3,
        search_metadata_fields=config.search.metadata_fields,
    )

    assert not result.summary.empty
    assert set(result.summary["supplier"]) == {bid.name for bid in config.bids}
    assert "difference" in result.summary.columns
    assert result.items["supplier"].nunique() == len(config.bids)
    if not result.unmatched.empty:
        assert set(result.unmatched["supplier"]).issubset(set(result.summary["supplier"]))

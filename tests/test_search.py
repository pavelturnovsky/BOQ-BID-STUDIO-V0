from __future__ import annotations

from core.search import hybrid_search
from core.utils import load_config


def test_hybrid_search_returns_relevant_items(offers: pd.DataFrame) -> None:
    config = load_config()
    results = hybrid_search(offers, "najdi vzduchotechnickou jednotku", config)
    assert not results.empty
    top_desc = results.iloc[0]["item_desc"].lower()
    assert "vzduchotechn" in top_desc or "air handling" in top_desc
    assert results.iloc[0]["relevance"] > 0

import pandas as pd

from bidstudio.search import TfidfSearchProvider


def test_tfidf_search_returns_relevant_result():
    frame = pd.DataFrame(
        {
            "record_key": ["A", "B"],
            "code": ["ITEM001", "ITEM002"],
            "description": [
                "Electrical wiring installation for office floors",
                "Concrete foundation works for main hall",
            ],
        }
    )

    provider = TfidfSearchProvider()
    provider.index(frame, text_columns=["description"], metadata_columns=["code"])
    results = provider.search("wiring", top_k=1)

    assert results
    assert results[0].metadata["code"] == "ITEM001"


def test_tfidf_search_ignores_blank_queries():
    frame = pd.DataFrame({"description": ["Sample"], "record_key": ["1"]})
    provider = TfidfSearchProvider()
    provider.index(frame, text_columns=["description"])

    assert provider.search("   ") == []

from __future__ import annotations

import pandas as pd


def test_match_scores_and_status(offers: pd.DataFrame) -> None:
    assert "match_score" in offers.columns
    # All matched items should have a status
    assert offers["match_status"].notna().all()

    ahu_row = offers.loc[offers["item_desc"].str.contains("Vzduchotechnická jednotka", case=False)].iloc[0]
    assert ahu_row["matched_wbs_code"] == "03.01"
    assert ahu_row["match_status"] in {"auto_accepted", "needs_review"}
    assert ahu_row["match_score"] > 0.65


def test_matching_log_created(tmp_path, offers: pd.DataFrame) -> None:
    log_file = tmp_path / "matching.jsonl"
    from core.matching import build_wbs_index, match_items
    from core.utils import load_config

    config = load_config()
    wbs_index = build_wbs_index(config)
    re_matches = match_items(offers, wbs_index, config, log_path=log_file)
    assert not re_matches.empty
    assert log_file.exists()
    content = log_file.read_text(encoding="utf-8").strip().splitlines()
    assert content

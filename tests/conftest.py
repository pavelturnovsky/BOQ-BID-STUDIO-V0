from __future__ import annotations

from pathlib import Path
from typing import Iterable
import sys

import pandas as pd
import pytest

# Ensure project root is on sys.path for absolute imports
ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

from core import (
    assign_disciplines,
    build_wbs_index,
    flag_outliers,
    load_config,
    load_offers,
    match_items,
    normalize_currency,
    normalize_units,
    validate_totals,
)


@pytest.fixture(scope="session")
def config() -> dict:
    return load_config()


@pytest.fixture(scope="session")
def offers(config: dict) -> pd.DataFrame:
    paths: Iterable[Path] = [Path("sample_data/offer_A.csv"), Path("sample_data/offer_B.csv")]
    df = load_offers(paths, config)
    df = normalize_units(df, config.get("unit_map", {}))
    currency_cfg = config.get("currency", {})
    df = normalize_currency(
        df,
        currency_cfg.get("base", "CZK"),
        currency_cfg.get("rates", {}),
        currency_cfg.get("default_vat", 0.21),
    )
    df = validate_totals(df)
    df = assign_disciplines(df, config)
    df = df.reset_index(drop=True)
    df["item_index"] = df.index
    wbs_index = build_wbs_index(config)
    matches = match_items(df, wbs_index, config)
    drop_cols = {"item_desc", "supplier", "sheet_name", "item_code"}
    merged = df.merge(
        matches.drop(columns=[col for col in drop_cols if col in matches.columns]),
        on="item_index",
        how="left",
    )
    merged = flag_outliers(merged)
    return merged

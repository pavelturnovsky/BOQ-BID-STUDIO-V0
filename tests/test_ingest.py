from __future__ import annotations

from pathlib import Path

import pytest

from core.ingest import load_offers
from core.normalize import normalize_currency, normalize_units, validate_totals
from core.utils import load_config


def test_load_offers_maps_columns() -> None:
    config = load_config()
    paths = [Path("sample_data/offer_A.csv"), Path("sample_data/offer_B.csv")]
    df = load_offers(paths, config)
    assert {"item_code", "item_desc", "qty", "unit_price", "total_price"}.issubset(df.columns)
    # Missing total price should be computed from quantity * unit price
    row = df.loc[df["item_code"] == "VZT-001"].iloc[0]
    assert row["total_price"] == pytest.approx(row["qty"] * row["unit_price"], rel=1e-6)


def test_normalization_and_currency_conversion() -> None:
    config = load_config()
    df = load_offers([Path("sample_data/offer_B.csv")], config)
    df = normalize_units(df, config["unit_map"])
    df = normalize_currency(
        df,
        config["currency"]["base"],
        config["currency"]["rates"],
        config["currency"]["default_vat"],
    )
    df = validate_totals(df)

    # Piece unit should be mapped to pcs
    assert "pcs" in set(df["unit"].unique())
    # EUR conversion
    ahu = df.loc[df["item_code"] == "03-AHU"].iloc[0]
    assert ahu["total_price_normalized"] == pytest.approx(18000 * config["currency"]["rates"]["EUR"])
    assert not ahu["total_mismatch"]

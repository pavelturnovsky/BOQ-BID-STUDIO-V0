from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from bidstudio.config import BidConfig, ColumnMapping
from bidstudio.io import (
    CANONICAL_COLUMNS,
    coerce_numeric,
    load_bid_dataset,
    load_master_dataset,
)


def test_load_master_dataset_autodetects_czech_headers(tmp_path):
    data = pd.DataFrame(
        {
            "Č.": ["001", "002"],
            "Název položky": ["Položka A", "Položka B"],
            "MJ": ["ks", "m2"],
            "Množství": ["1\u00A0234,50 Kč", "2"],
            "Cena celkem": ["12 345 Kč", "678,90 Kč"],
        }
    )
    csv_path = tmp_path / "czech_headers.csv"
    data.to_csv(csv_path, index=False, encoding="utf-8")

    mapping = ColumnMapping(
        code="auto",
        description="auto",
        unit="auto",
        quantity="auto",
        unit_price="auto",
        total_price="auto",
    )

    frame = load_master_dataset(Path(csv_path), mapping, key_columns=["code"])

    expected_columns = ["record_key", *CANONICAL_COLUMNS]
    assert list(frame.columns) == expected_columns
    assert frame.loc[0, "code"] == "001"
    assert frame.loc[0, "description"] == "Položka A"
    assert frame.loc[0, "unit"] == "ks"
    assert frame.loc[0, "quantity"] == pytest.approx(1234.5)
    assert frame.loc[1, "total_price"] == pytest.approx(678.90)
    assert np.isnan(frame.loc[0, "unit_price"])


def test_load_dataset_generates_surrogate_keys_with_dataset_prefix(tmp_path):
    data = pd.DataFrame(
        {
            "ItemCode": ["001", None],
            "Description": ["Položka A", "Položka B"],
            "Unit": ["ks", "m2"],
            "Quantity": [1, 2],
            "UnitPrice": [10, 20],
            "TotalPrice": [10, 40],
        }
    )

    master_path = tmp_path / "master.csv"
    bid_path = tmp_path / "bid.csv"
    data.to_csv(master_path, index=False)
    data.to_csv(bid_path, index=False)

    mapping = ColumnMapping(
        code="ItemCode",
        description="Description",
        unit="Unit",
        quantity="Quantity",
        unit_price="UnitPrice",
        total_price="TotalPrice",
    )

    master_frame = load_master_dataset(master_path, mapping, key_columns=["code"])
    assert master_frame.loc[0, "record_key"] == "001"
    assert master_frame.loc[1, "record_key"].startswith("__master_row_")

    bid_config = BidConfig(name="Dodavatel Č.1", path=bid_path)
    bid_frame = load_bid_dataset(bid_config, mapping, key_columns=["code"])
    assert bid_frame.loc[1, "record_key"].startswith("__bid_Dodavatel")


def test_coerce_numeric_strips_currencies_and_separators():
    series = pd.Series(["1\u00A0234,50 Kč", "12.5 EUR", "5 000", None, "-"])
    coerced = coerce_numeric(series)

    assert coerced.iloc[0] == pytest.approx(1234.5)
    assert coerced.iloc[1] == pytest.approx(12.5)
    assert coerced.iloc[2] == pytest.approx(5000)
    assert np.isnan(coerced.iloc[3])
    assert np.isnan(coerced.iloc[4])

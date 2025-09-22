import sys
import types
from pathlib import Path

import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.append(str(ROOT))
MODULE_CODE = (ROOT / "boq_bid_studio.py").read_text().split("# ------------- Sidebar Inputs -------------")[0]
module = types.ModuleType("boq_bid_helpers_recap")
exec(MODULE_CODE, module.__dict__)

build_recap_chart_data = module.build_recap_chart_data


def test_build_recap_chart_data_calculates_percentages() -> None:
    value_cols = ["Master total", "Bid1 total", "Bid2 total"]
    net_series = pd.Series({
        "Master total": 100.0,
        "Bid1 total": 110.0,
        "Bid2 total": 90.0,
    })

    chart_df = build_recap_chart_data(value_cols, net_series)

    master_row = chart_df.loc[chart_df["Dodavatel"] == "Master"].iloc[0]
    bid1_row = chart_df.loc[chart_df["Dodavatel"] == "Bid1"].iloc[0]
    bid2_row = chart_df.loc[chart_df["Dodavatel"] == "Bid2"].iloc[0]

    assert master_row["Odchylka vs Master (%)"] == pytest.approx(0.0)
    assert bid1_row["Odchylka vs Master (%)"] == pytest.approx(10.0)
    assert bid2_row["Odchylka vs Master (%)"] == pytest.approx(-10.0)
    assert master_row["Popisek"] == "+0,00 %"
    assert bid1_row["Popisek"] == "+10,00 %"
    assert bid2_row["Popisek"] == "-10,00 %"


def test_build_recap_chart_data_handles_missing_master() -> None:
    value_cols = ["Master total", "Bid1 total"]
    net_series = pd.Series({
        "Master total": 0.0,
        "Bid1 total": 120.0,
    })

    chart_df = build_recap_chart_data(value_cols, net_series)
    bid_row = chart_df.loc[chart_df["Dodavatel"] == "Bid1"].iloc[0]

    assert pd.isna(bid_row["Odchylka vs Master (%)"])
    assert bid_row["Popisek"] == "–"

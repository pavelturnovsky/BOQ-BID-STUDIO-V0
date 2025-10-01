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
generate_recap_pdf = module.generate_recap_pdf


def test_build_recap_chart_data_calculates_percentages() -> None:
    value_cols = ["Master total", "Bid1 total", "Bid2 total"]
    net_series = pd.Series({
        "Master total": 100.0,
        "Bid1 total": 110.0,
        "Bid2 total": 90.0,
    })

    chart_df = build_recap_chart_data(value_cols, net_series, currency_label="CZK")

    master_row = chart_df.loc[chart_df["Dodavatel"] == "Master"].iloc[0]
    bid1_row = chart_df.loc[chart_df["Dodavatel"] == "Bid1"].iloc[0]
    bid2_row = chart_df.loc[chart_df["Dodavatel"] == "Bid2"].iloc[0]

    assert master_row["Odchylka vs Master (%)"] == pytest.approx(0.0)
    assert bid1_row["Odchylka vs Master (%)"] == pytest.approx(10.0)
    assert bid2_row["Odchylka vs Master (%)"] == pytest.approx(-10.0)
    assert master_row["Popisek"] == "+0,00 %"
    assert bid1_row["Popisek"] == "+10,00 %"
    assert bid2_row["Popisek"] == "-10,00 %"
    assert master_row["Cena (text)"] == "100,00 CZK"
    assert bid1_row["Cena (text)"] == "110,00 CZK"
    assert bid2_row["Cena (text)"] == "90,00 CZK"


def test_build_recap_chart_data_handles_missing_master() -> None:
    value_cols = ["Master total", "Bid1 total"]
    net_series = pd.Series({
        "Master total": 0.0,
        "Bid1 total": 120.0,
    })

    chart_df = build_recap_chart_data(value_cols, net_series, currency_label="CZK")
    bid_row = chart_df.loc[chart_df["Dodavatel"] == "Bid1"].iloc[0]

    assert pd.isna(bid_row["Odchylka vs Master (%)"])
    assert bid_row["Popisek"] == "–"
    assert bid_row["Cena (text)"] == "120,00 CZK"


def test_generate_recap_pdf_embeds_unicode_font() -> None:
    main_df = pd.DataFrame(
        {"č": ["1"], "Položka": ["Žluťoučký kůň"], "Master total": [123.0]}
    )
    summary_df = pd.DataFrame(
        {"Ukazatel": ["Žluťoučký kůň"], "Jednotka": ["CZK"], "Master total": [123.0]}
    )
    pdf_bytes = generate_recap_pdf(
        title="Žluťoučký kůň",
        base_currency="CZK",
        target_currency="CZK",
        main_detail_base=main_df,
        main_detail_converted=main_df,
        summary_base=summary_df,
        summary_converted=summary_df,
        chart_df=pd.DataFrame(),
    )
    assert isinstance(pdf_bytes, bytes)
    assert b"NotoSans" in pdf_bytes

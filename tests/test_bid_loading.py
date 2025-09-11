import io
import os
import sys
import types
from pathlib import Path
import pandas as pd

# Load only helper functions from boq_bid_studio without running Streamlit app
ROOT = Path(__file__).resolve().parent.parent
sys.path.append(str(ROOT))
module_code = (ROOT / "boq_bid_studio.py").read_text().split("# ------------- Sidebar Inputs -------------")[0]
module = types.ModuleType("boq_bid_helpers")
exec(module_code, module.__dict__)
module.try_autodetect_mapping = module.try_autodetect_mapping.__wrapped__
module.build_normalized_table = module.build_normalized_table.__wrapped__
read_workbook = module.read_workbook.__wrapped__
apply_master_mapping = module.apply_master_mapping
compare = module.compare
validate_totals = module.validate_totals


def make_workbook(df: pd.DataFrame) -> io.BytesIO:
    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Sheet1")
    buffer.seek(0)
    buffer.name = "test.xlsx"
    return buffer


def test_multiple_bid_loading() -> None:
    master_df = pd.DataFrame({
        "code": ["A"],
        "description": ["Item"],
        "unit": ["m"],
        "quantity": [1],
        "unit_price": [10],
        "total_price": [10],
    })
    bid_df = pd.DataFrame({
        "code": ["A"],
        "description": ["Item"],
        "unit": ["m"],
        "quantity": [1],
        "unit_price": [12],
        "total_price": [12],
    })

    master_file = make_workbook(master_df)
    bid1 = make_workbook(bid_df)
    bid2 = make_workbook(bid_df)

    master_wb = read_workbook(master_file, limit_sheets=["Sheet1"])

    bids = {}
    for i, f in enumerate([bid1, bid2], start=1):
        f.seek(0)
        wb = read_workbook(f, limit_sheets=["Sheet1"])
        apply_master_mapping(master_wb, wb)
        bids[f"Bid{i}"] = wb

    results = compare(master_wb, bids)
    assert "Sheet1" in results
    df = results["Sheet1"]
    assert not df.empty
    assert df.shape[0] == 1


def test_coerce_numeric_european_formats() -> None:
    s = pd.Series(["1 234,56", "1 234", "1234", "1.234,5", "-"])
    res = module.coerce_numeric(s)
    assert res.iloc[0] == 1234.56
    assert res.iloc[1] == 1234
    assert res.iloc[2] == 1234
    assert res.iloc[3] == 1234.5
    assert pd.isna(res.iloc[4])


def test_total_diff_and_summary_detection() -> None:
    df = pd.DataFrame(
        {
            "code": ["1", ""],
            "description": ["práce", "součet"],
            "unit": ["m", ""],
            "quantity": ["2", ""],
            "unit_price": ["5", ""],
            "total_price": ["10", "10"],
        }
    )
    mapping = {"code": 0, "description": 1, "unit": 2, "quantity": 3, "unit_price": 4, "total_price": 5}
    out = module.build_normalized_table(df, mapping)
    assert out.loc[0, "total_diff"] == 0
    assert out.loc[1, "is_summary"]
    assert validate_totals(out) == 0


def test_detect_summary_rows_alternating() -> None:
    df = pd.DataFrame(
        {
            "code": ["1", "", "2", "", ""],
            "description": ["item1", "součet", "item2", "Sub section", "Total"],
            "unit": ["m", "", "m", "", ""],
            "quantity": ["1", "", "2", "", ""],
            "unit_price": ["10", "", "20", "", ""],
            "total_price": ["10", "10", "40", "50", "50"],
        }
    )
    mapping = {"code": 0, "description": 1, "unit": 2, "quantity": 3, "unit_price": 4, "total_price": 5}
    out = module.build_normalized_table(df, mapping)
    assert out["is_summary"].tolist() == [False, True, False, True, True]


def test_validate_totals_detects_difference() -> None:
    df = pd.DataFrame(
        {
            "code": ["1", ""],
            "description": ["práce", "součet"],
            "unit": ["m", ""],
            "quantity": ["1", ""],
            "unit_price": ["100", ""],
            "total_price": ["100", "150"],
        }
    )
    mapping = {"code": 0, "description": 1, "unit": 2, "quantity": 3, "unit_price": 4, "total_price": 5}
    out = module.build_normalized_table(df, mapping)
    assert validate_totals(out) == 50


def test_validate_totals_handles_multiple_summaries() -> None:
    df = pd.DataFrame(
        {
            "code": ["1", "2", "", "3", "4", "", ""],
            "description": [
                "item1",
                "item2",
                "součet oddíl A",
                "item3",
                "item4",
                "součet oddíl B",
                "celkem",
            ],
            "unit": ["m", "m", "", "m", "m", "", ""],
            "quantity": ["1", "2", "", "3", "4", "", ""],
            "unit_price": ["10", "20", "", "30", "40", "", ""],
            "total_price": ["10", "40", "50", "90", "160", "250", "300"],
        }
    )
    mapping = {"code": 0, "description": 1, "unit": 2, "quantity": 3, "unit_price": 4, "total_price": 5}
    out = module.build_normalized_table(df, mapping)
    assert validate_totals(out) == 0


def test_validate_totals_flags_subtotal_mismatch() -> None:
    df = pd.DataFrame(
        {
            "code": ["1", "2", "", "3", "4", "", ""],
            "description": [
                "item1",
                "item2",
                "součet oddíl A",
                "item3",
                "item4",
                "součet oddíl B",
                "celkem",
            ],
            "unit": ["m", "m", "", "m", "m", "", ""],
            "quantity": ["1", "2", "", "3", "4", "", ""],
            "unit_price": ["10", "20", "", "30", "40", "", ""],
            "total_price": ["10", "40", "60", "90", "160", "250", "300"],
        }
    )
    mapping = {"code": 0, "description": 1, "unit": 2, "quantity": 3, "unit_price": 4, "total_price": 5}
    out = module.build_normalized_table(df, mapping)
    assert validate_totals(out) == 10

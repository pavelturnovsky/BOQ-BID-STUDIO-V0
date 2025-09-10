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

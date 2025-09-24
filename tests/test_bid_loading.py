import io
import os
import sys
import types
from pathlib import Path
import pandas as pd
import numpy as np

# Ensure project root on path for imports
ROOT = Path(__file__).resolve().parent.parent
sys.path.append(str(ROOT))
from workbook import WorkbookData

# Load only helper functions from boq_bid_studio without running Streamlit app
module_code = (ROOT / "boq_bid_studio.py").read_text().split("# ------------- Sidebar Inputs -------------")[0]
module = types.ModuleType("boq_bid_helpers")
exec(module_code, module.__dict__)
module.try_autodetect_mapping = module.try_autodetect_mapping.__wrapped__
module.build_normalized_table = module.build_normalized_table.__wrapped__
read_workbook = module.read_workbook.__wrapped__
apply_master_mapping = module.apply_master_mapping
compare = module.compare
validate_totals = module.validate_totals
overview_comparison = module.overview_comparison


def make_workbook(df: pd.DataFrame) -> io.BytesIO:
    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Sheet1")
    buffer.seek(0)
    buffer.name = "test.xlsx"
    return buffer


def test_autodetect_polozka_description() -> None:
    df = pd.DataFrame([
        ["kód", "položka", "cena celkem"],
        ["1", "des", "10"],
    ])
    mapping, hdr, body = module.try_autodetect_mapping(df)
    assert mapping["code"] == 0
    assert mapping["description"] == 1


def test_autodetect_pol_abbreviation() -> None:
    df = pd.DataFrame([
        ["pol.", "popis", "cena"],
        ["1", "a", "10"],
    ])
    mapping, _, _ = module.try_autodetect_mapping(df)
    assert mapping["code"] == 0


def test_autodetect_pol_without_dot() -> None:
    df = pd.DataFrame([
        ["pol", "popis", "cena"],
        ["1", "a", "10"],
    ])
    mapping, _, _ = module.try_autodetect_mapping(df)
    assert mapping["code"] == 0


def test_autodetect_ignores_bidder_comment_for_code() -> None:
    df = pd.DataFrame([
        ["komentář uchazeče", "kód", "popis"],
        ["poznámka", "1", "a"],
    ])
    mapping, _, _ = module.try_autodetect_mapping(df)
    assert mapping["code"] == 1


def test_autodetect_summary_total() -> None:
    df = pd.DataFrame([
        ["kód", "popis", "celkem za oddíl"],
        ["1", "a", "10"],
    ])
    mapping, hdr, body = module.try_autodetect_mapping(df)
    assert mapping["summary_total"] == 2

def test_multiple_bid_loading() -> None:
    master_df = pd.DataFrame({
        "code": ["A"],
        "description": ["Item"],
        "unit": ["m"],
        "quantity": [1],
        "total_price": [10],
    })
    bid_df = pd.DataFrame({
        "code": ["A"],
        "description": ["Item"],
        "unit": ["m"],
        "quantity": [1],
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


def test_compare_master_total_with_duplicate_supplier_rows() -> None:
    master_df = pd.DataFrame(
        {
            "code": ["A"],
            "description": ["Item"],
            "unit": ["m"],
            "quantity": [2],
            "total price": [100],
        }
    )
    supplier_df = pd.DataFrame(
        {
            "code": ["A", "A"],
            "description": ["Item", "Item"],
            "unit": ["m", "m"],
            "quantity": [1, 1],
            "total price": [30, 70],
        }
    )

    master_file = make_workbook(master_df)
    supplier_file = make_workbook(supplier_df)

    master_wb = read_workbook(master_file, limit_sheets=["Sheet1"])
    supplier_wb = read_workbook(supplier_file, limit_sheets=["Sheet1"])

    apply_master_mapping(master_wb, supplier_wb)

    results = compare(master_wb, {"Bid": supplier_wb})
    assert "Sheet1" in results
    df = results["Sheet1"]
    assert df.shape[0] == 1
    assert np.isclose(df["Master total"].iloc[0], 100)
    assert np.isclose(df["Master total"].sum(), 100)
    assert np.isclose(df["Bid total"].iloc[0], 100)
    assert "master_total_sum" in df.attrs
    assert np.isclose(df.attrs["master_total_sum"], 100)


def test_apply_master_mapping_aligns_by_header_name() -> None:
    master_df = pd.DataFrame(
        {
            "code": ["A"],
            "description": ["Item"],
            "unit": ["m"],
            "quantity": [2],
            "total price": [100],
        }
    )
    supplier_df = pd.DataFrame(
        {
            "description": ["Item"],
            "code": ["A"],
            "total price": [120],
            "quantity": [2],
            "unit": ["m"],
        }
    )

    master_file = make_workbook(master_df)
    supplier_file = make_workbook(supplier_df)

    master_wb = read_workbook(master_file, limit_sheets=["Sheet1"])
    supplier_wb = read_workbook(supplier_file, limit_sheets=["Sheet1"])

    apply_master_mapping(master_wb, supplier_wb)

    results = compare(master_wb, {"Bid": supplier_wb})
    assert "Sheet1" in results
    df = results["Sheet1"]
    assert df.shape[0] == 1
    assert np.isclose(df["Bid total"].iloc[0], 120)


def test_apply_master_mapping_uses_master_header_row_fallback() -> None:
    master_header_row = 65
    header = ["code", "description", "unit", "quantity", "total price"]
    master_wb = WorkbookData(name="Master")
    master_wb.sheets["Sheet1"] = {
        "raw": None,
        "mapping": {
            "code": 0,
            "description": 1,
            "unit": 2,
            "quantity": 3,
            "total_price": 4,
        },
        "header_row": master_header_row,
        "table": pd.DataFrame(),
        "header_names": header,
    }

    filler_rows = [[f"intro {i}", "", "", "", ""] for i in range(master_header_row)]
    supplier_rows = filler_rows + [header, ["A", "Item", "m", "2", "120"]]
    supplier_raw = pd.DataFrame(supplier_rows)

    supplier_wb = WorkbookData(name="Bid")
    supplier_wb.sheets["Sheet1"] = {
        "raw": supplier_raw,
        "mapping": {},
        "header_row": -1,
        "table": pd.DataFrame(),
        "header_names": [],
    }

    apply_master_mapping(master_wb, supplier_wb)

    sheet = supplier_wb.sheets["Sheet1"]
    assert sheet["header_row"] == master_header_row
    mapping = sheet["mapping"]
    assert mapping["code"] == 0
    assert mapping["description"] == 1
    assert mapping["quantity"] == 3
    table = sheet["table"]
    assert not table.empty
    assert table.loc[0, "code"] == "A"
    assert np.isclose(table.loc[0, "total_price"], 120)


def test_coerce_numeric_european_formats() -> None:
    s = pd.Series(["1 234,56", "1 234", "1234", "1.234,5", "-"])
    res = module.coerce_numeric(s)
    assert res.iloc[0] == 1234.56
    assert res.iloc[1] == 1234
    assert res.iloc[2] == 1234
    assert res.iloc[3] == 1234.5
    assert pd.isna(res.iloc[4])


def test_coerce_numeric_strips_currency_and_suffix() -> None:
    s = pd.Series(["1 234,50 Kč", "2 500,-", "-1 111,11 €", "3.750,00CZK"])
    res = module.coerce_numeric(s)
    assert np.isclose(res.iloc[0], 1234.5)
    assert np.isclose(res.iloc[1], 2500.0)
    assert np.isclose(res.iloc[2], -1111.11)
    assert np.isclose(res.iloc[3], 3750.0)


def test_total_diff_and_summary_detection() -> None:
    df = pd.DataFrame(
        {
            "code": ["1", ""],
            "description": ["práce", "součet"],
            "unit": ["m", ""],
            "quantity": ["2", ""],
            "unit_price_material": ["5", ""],
            "total_price": ["10", "10"],
        }
    )
    mapping = {"code": 0, "description": 1, "unit": 2, "quantity": 3, "unit_price_material": 4, "total_price": 5}
    out = module.build_normalized_table(df, mapping)
    assert out.loc[0, "total_diff"] == 0
    assert out.loc[1, "is_summary"]
    assert validate_totals(out) == 0


def test_calc_total_no_fallback_to_total_price() -> None:
    df = pd.DataFrame(
        {
            "code": ["1"],
            "description": ["item"],
            "unit": ["m"],
            "quantity": ["2"],
            "total_price": ["50"],
        }
    )
    mapping = {
        "code": 0,
        "description": 1,
        "unit": 2,
        "quantity": 3,
        "total_price": 4,
    }
    out = module.build_normalized_table(df, mapping)
    assert out.loc[0, "calc_total"] == 0
    assert out.loc[0, "total_price"] == 50
    assert out.loc[0, "total_diff"] == 50
    # totals should rely solely on provided total_price
    assert out["total_price"].sum() == 50


def test_summary_keyword_requires_structural_hint() -> None:
    df = pd.DataFrame(
        {
            "code": ["1"],
            "description": ["součet položek"],
            "unit": ["m"],
            "quantity": ["2"],
            "total_price": ["10"],
        }
    )
    mapping = {
        "code": 0,
        "description": 1,
        "unit": 2,
        "quantity": 3,
        "total_price": 4,
    }
    out = module.build_normalized_table(df, mapping)
    assert not out.loc[0, "is_summary"]
    assert out.loc[0, "summary_type"] == ""


def test_ignore_rows_without_description() -> None:
    df = pd.DataFrame(
        {
            "code": ["1", ""],
            "description": ["item", ""],
            "total_price": ["10", "5"],
        }
    )
    mapping = {"code": 0, "description": 1, "total_price": 2}
    out = module.build_normalized_table(df, mapping)
    assert out.shape[0] == 1


def test_detect_summary_rows_alternating() -> None:
    df = pd.DataFrame(
        {
            "code": ["1", "", "2", "", ""],
            "description": ["item1", "součet", "item2", "Sub section", "Total"],
            "unit": ["m", "", "m", "", ""],
            "quantity": ["1", "", "2", "", ""],
            "total_price": ["10", "10", "40", "50", "50"],
        }
    )
    mapping = {"code": 0, "description": 1, "unit": 2, "quantity": 3, "total_price": 4}
    out = module.build_normalized_table(df, mapping)
    assert out["is_summary"].tolist() == [False, True, False, False, True]


def test_validate_totals_detects_difference() -> None:
    df = pd.DataFrame(
        {
            "code": ["1", ""],
            "description": ["práce", "součet"],
            "unit": ["m", ""],
            "quantity": ["1", ""],
            "total_price": ["100", "150"],
        }
    )
    mapping = {"code": 0, "description": 1, "unit": 2, "quantity": 3, "total_price": 4}
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
            "total_price": ["10", "40", "50", "90", "160", "250", "300"],
        }
    )
    mapping = {"code": 0, "description": 1, "unit": 2, "quantity": 3, "total_price": 4}
    out = module.build_normalized_table(df, mapping)
    assert validate_totals(out) == 0
    vals = out["section_total"].tolist()
    assert vals[:3] == [50, 50, 50]
    assert vals[3:6] == [250, 250, 250]
    assert pd.isna(vals[6])


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
            "total_price": ["10", "40", "60", "90", "160", "250", "300"],
        }
    )
    mapping = {"code": 0, "description": 1, "unit": 2, "quantity": 3, "total_price": 4}
    out = module.build_normalized_table(df, mapping)
    assert validate_totals(out) == 10


def test_summary_type_and_dedup() -> None:
    df = pd.DataFrame(
        {
            "code": ["1", "2", "", "", ""],
            "description": [
                "item1",
                "item2",
                "součet oddíl A",
                "součet oddíl A",
                "celkem",
            ],
            "unit": ["m", "m", "", "", ""],
            "quantity": ["1", "2", "", "", ""],
            "total_price": ["10", "40", "50", "50", "100"],
        }
    )
    mapping = {"code": 0, "description": 1, "unit": 2, "quantity": 3, "total_price": 4}
    out = module.build_normalized_table(df, mapping)
    # duplicate summary row should be removed
    assert out.shape[0] == 4
    assert out["summary_type"].tolist() == ["", "", "section", "grand"]


def test_summary_total_column() -> None:
    df = pd.DataFrame(
        {
            "code": ["1", ""],
            "description": ["item", "součet"],
            "quantity": ["1", ""],
            "total_price": ["10", ""],
            "summary_total": ["", "10"],
        }
    )
    mapping = {"code": 0, "description": 1, "quantity": 2, "total_price": 3, "summary_total": 4}
    out = module.build_normalized_table(df, mapping)
    idx = list(out.columns).index("total_price")
    assert out.columns[idx + 1] == "summary_total"
    assert pd.isna(out.loc[0, "summary_total"])
    assert out.loc[1, "summary_total"] == 10
    assert pd.isna(out.loc[1, "total_price"])


def test_continuation_and_indirect_rows() -> None:
    df = pd.DataFrame(
        {
            "code": ["1", "", "", ""],
            "description": [
                "item",
                "continued text",
                "vedlejší rozpočtové náklady",
                "součet",
            ],
            "unit": ["m", "", "", ""],
            "quantity": ["1", "", "", ""],
            "unit_price_material": ["5", "", "", ""],
            "total_price": ["5", "", "5", "10"],
        }
    )
    mapping = {
        "code": 0,
        "description": 1,
        "unit": 2,
        "quantity": 3,
        "unit_price_material": 4,
        "total_price": 5,
    }
    out = module.build_normalized_table(df, mapping)
    # continuation row should be dropped, indirect cost kept as regular item
    assert out["description"].tolist() == ["item", "vedlejší rozpočtové náklady", "součet"]
    assert out["is_summary"].tolist() == [False, False, True]
    # summary row should balance totals
    assert module.validate_totals(out) == 0


def test_overview_comparison_mixed_codes() -> None:
    master = WorkbookData(name="m", sheets={
        "s": {
            "table": pd.DataFrame(
                {"code": ["1", "A"], "description": ["one", "A"], "total_price": [1, 2]}
            )
        }
    })
    sections, _, _, _, _ = overview_comparison(master, {}, "s")
    assert sections["code"].tolist() == ["1", "A"]


def test_overview_comparison_missing_and_indirect_total() -> None:
    master = WorkbookData(
        name="m",
        sheets={
            "s": {
                "table": pd.DataFrame(
                    {
                        "code": ["1", "2", ""],
                        "description": ["a", "b", "vedlejší rozpočtové náklady"],
                        "total_price": [1, 2, 5],
                    }
                )
            }
        },
    )
    bid = WorkbookData(
        name="b",
        sheets={
            "s": {
                "table": pd.DataFrame(
                    {
                        "code": ["1", ""],
                        "description": ["a", "vedlejší rozpočtové náklady"],
                        "total_price": [1.5, 7],
                    }
                )
            }
        },
    )
    sections, indirect, added, missing, indirect_total = overview_comparison(master, {"B": bid}, "s")
    assert missing["code"].tolist() == ["2"]
    assert set(indirect_total["supplier"]) == {"Master", "B"}
    mtot = indirect_total.set_index("supplier").loc["Master", "total"]
    btot = indirect_total.set_index("supplier").loc["B", "total"]
    assert mtot == 5 and btot == 7

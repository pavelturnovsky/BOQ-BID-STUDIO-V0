import pandas as pd

from core.aggregate import collect_outline_rollups, rollup_by_outline
from core.excel_outline import OutlineNode


def test_rollup_by_outline_filters_rows():
    df = pd.DataFrame(
        {
            "row_ref": ["Sheet1!2", "Sheet1!3", "Sheet1!4", "Sheet1!6"],
            "quantity": [1, 2, 3, 4],
            "total_price": [10, 20, 30, 40],
        }
    )

    metrics = rollup_by_outline(
        df,
        sheet="Sheet1",
        axis="row",
        level=1,
        start=2,
        end=4,
        include_columns=["quantity", "total_price"],
    )
    assert metrics["quantity"] == 6
    assert metrics["total_price"] == 60
    assert metrics["__row_count__"] == 3


def test_collect_outline_rollups_with_children():
    df = pd.DataFrame(
        {
            "row_ref": ["Sheet1!2", "Sheet1!3", "Sheet1!4", "Sheet1!5"],
            "quantity": [1, 2, 3, 4],
            "total_price": [5, 6, 7, 8],
        }
    )

    child = OutlineNode(
        axis="row",
        sheet="Sheet1",
        level=2,
        start=3,
        end=4,
        collapsed=False,
        children=[],
    )
    parent = OutlineNode(
        axis="row",
        sheet="Sheet1",
        level=1,
        start=2,
        end=5,
        collapsed=True,
        children=[child],
    )

    results = collect_outline_rollups(
        df,
        [parent],
        include_columns=["quantity", "total_price"],
    )
    assert len(results) == 2
    parent_row = results.loc[results["level"] == 1].iloc[0]
    child_row = results.loc[results["level"] == 2].iloc[0]
    assert parent_row["total_price"] == 26
    assert child_row["total_price"] == 13
    assert bool(parent_row["collapsed"]) is True
    assert bool(child_row["collapsed"]) is False

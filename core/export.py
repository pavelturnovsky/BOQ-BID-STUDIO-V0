"""Export helpers for producing XLSX outputs."""
from __future__ import annotations

import json
from pathlib import Path
from typing import IO, Optional, Union

import pandas as pd

from .utils import ensure_directories


_DEF_EMPTY = pd.DataFrame([{"message": "No data available"}])


def _normalise_items(df: pd.DataFrame) -> pd.DataFrame:
    frame = df.copy()
    if "disciplines" in frame.columns:
        frame["disciplines"] = frame["disciplines"].apply(
            lambda value: ", ".join(sorted(value)) if isinstance(value, (set, list)) else value
        )
    if "signals" in frame.columns:
        frame["signals"] = frame["signals"].apply(
            lambda value: json.dumps(value, ensure_ascii=False) if isinstance(value, dict) else value
        )
    return frame


def export_to_xlsx(
    summary: Optional[pd.DataFrame],
    items: pd.DataFrame,
    unmatched: Optional[pd.DataFrame],
    output_path: Union[str, Path, IO[bytes]],
) -> Path | IO[bytes]:
    """Write the provided dataframes to an XLSX workbook."""

    if hasattr(output_path, "write"):
        output_file = output_path
    else:
        output_file = Path(output_path)
        ensure_directories([output_file.parent])
    summary_frame = summary if summary is not None and not summary.empty else _DEF_EMPTY
    items_frame = _normalise_items(items)
    unmatched_frame = (
        _normalise_items(unmatched) if unmatched is not None and not unmatched.empty else _DEF_EMPTY
    )

    with pd.ExcelWriter(output_file, engine="xlsxwriter") as writer:
        summary_frame.to_excel(writer, index=False, sheet_name="Summary")
        items_frame.to_excel(writer, index=False, sheet_name="Items")
        unmatched_frame.to_excel(writer, index=False, sheet_name="Unmatched")
        workbook = writer.book
        money_format = workbook.add_format({"num_format": "#,##0.00"})
        for sheet_name in ["Summary", "Items", "Unmatched"]:
            worksheet = writer.sheets[sheet_name]
            worksheet.freeze_panes(1, 0)
            worksheet.set_column(0, items_frame.shape[1], 20)
        if "total_diff" in summary_frame.columns:
            worksheet = writer.sheets["Summary"]
            col_idx = summary_frame.columns.get_loc("total_diff")
            worksheet.set_column(col_idx, col_idx, 18, money_format)
    return output_file


__all__ = ["export_to_xlsx"]

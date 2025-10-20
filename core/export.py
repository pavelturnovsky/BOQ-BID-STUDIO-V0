"""Excel export helpers with outline support."""

from __future__ import annotations

import io
from typing import Iterable, Optional

import pandas as pd
from openpyxl.worksheet.properties import Outline

from .excel_outline import OutlineNode, apply_outline_to_openpyxl


def dataframe_to_excel_bytes_with_outline(
    df: pd.DataFrame,
    sheet_name: str,
    *,
    outline: Optional[dict[str, Iterable[OutlineNode]]] = None,
    summary_below: bool = True,
    summary_right: bool = True,
) -> bytes:
    """Serialize dataframe to XLSX and optionally apply outline metadata."""

    buffer = io.BytesIO()
    safe_sheet = sheet_name[:31] or "Data"
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name=safe_sheet)
        if outline:
            worksheet = writer.book[safe_sheet]
            row_nodes = list(outline.get("rows", []))
            col_nodes = list(outline.get("cols", []))
            if row_nodes:
                apply_outline_to_openpyxl(worksheet, row_nodes)
            if col_nodes:
                apply_outline_to_openpyxl(worksheet, col_nodes)
            if row_nodes or col_nodes:
                outline_pr = worksheet.sheet_properties.outlinePr
                if outline_pr is None:
                    outline_pr = Outline()
                outline_pr.summaryBelow = summary_below
                outline_pr.summaryRight = summary_right
                worksheet.sheet_properties.outlinePr = outline_pr
    buffer.seek(0)
    return buffer.getvalue()

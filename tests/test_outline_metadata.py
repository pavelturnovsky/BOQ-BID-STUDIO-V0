import sys
import types
from pathlib import Path

import pandas as pd

from core.excel_outline import build_outline_nodes

# Reuse helper module without triggering full Streamlit app
ROOT = Path(__file__).resolve().parent.parent
sys.path.append(str(ROOT))
module_code = (ROOT / "boq_bid_studio.py").read_text().split("# ------------- Sidebar Inputs -------------")[0]
outline_module = types.ModuleType("outline_helpers")
exec(module_code, outline_module.__dict__)
_attach_outline_metadata = outline_module._attach_outline_metadata
_prepare_preview_table = outline_module.prepare_preview_table
_outline_node_key = outline_module._outline_node_key


def test_outline_metadata_includes_node_columns() -> None:
    table = pd.DataFrame({
        "code": ["A", "B", "C", "D"],
        "value": [1, 2, 3, 4],
    })
    level_map = {
        2: {"level": 1, "hidden": False},
        3: {"level": 2, "hidden": False},
        4: {"level": 2, "hidden": False},
        5: {"level": 1, "hidden": False},
    }
    nodes = build_outline_nodes(level_map, axis="row", sheet="Sheet1")

    enriched = _attach_outline_metadata(
        table,
        "Sheet1",
        header_row=0,
        row_outline_map=level_map,
        row_outline_nodes=nodes,
        source_index=table.index,
    )

    assert "row_outline_node_key" in enriched.columns
    assert "row_outline_range_end" in enriched.columns
    assert "row_outline_has_children" in enriched.columns

    first_key = enriched.loc[0, "row_outline_node_key"]
    assert first_key == _outline_node_key(1, 2, 5)
    assert enriched.loc[0, "row_outline_range_end"] == 5
    assert bool(enriched.loc[0, "row_outline_has_children"]) is True

    child_key = enriched.loc[1, "row_outline_node_key"]
    assert child_key == _outline_node_key(2, 3, 4)
    assert bool(enriched.loc[1, "row_outline_has_children"]) is True

    prepared = _prepare_preview_table(enriched)
    assert prepared.loc[0, "row_outline_node_key"] == first_key

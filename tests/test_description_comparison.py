import sys
import types
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
sys.path.append(str(ROOT))
MODULE_CODE = (
    ROOT / "boq_bid_studio.py"
).read_text().split("# ------------- Sidebar Inputs -------------")[0]
helpers_module = types.ModuleType("description_helpers")
exec(MODULE_CODE, helpers_module.__dict__)

normalize_description_key = helpers_module.normalize_description_key
prepare_description_comparison_table = (
    helpers_module.prepare_description_comparison_table
)


def test_description_diff_ignores_diacritics() -> None:
    template_df = pd.DataFrame({"description": ["Žárovka", "Okno"]})
    supplier_df = pd.DataFrame({"description": ["Zarovka", "Dveře"]})

    _, template_keys = prepare_description_comparison_table(template_df)
    _, supplier_keys = prepare_description_comparison_table(supplier_df)

    missing_keys = template_keys - supplier_keys
    extra_keys = supplier_keys - template_keys

    normalized_bulb = normalize_description_key("Žárovka")
    normalized_window = normalize_description_key("Okno")
    normalized_door = normalize_description_key("Dveře")

    assert normalized_bulb not in missing_keys
    assert normalized_window in missing_keys
    assert normalized_door in extra_keys

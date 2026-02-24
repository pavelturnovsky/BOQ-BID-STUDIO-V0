from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
SOURCE = (ROOT / "boq_bid_studio.py").read_text()


def test_rounds_v2_has_inter_round_supplier_mode() -> None:
    assert '"Dodavatel mezi koly"' in SOURCE
    assert 'build_rounds_supplier_long_dataset(' in SOURCE
    assert '"Typ porovnání"' in SOURCE


def test_rounds_v2_uses_supplier_id_map_for_cross_round_matching() -> None:
    assert '"supplier_id_map"' in SOURCE
    assert 'supplier_id_map[raw_name]' in SOURCE
    assert 'selected_supplier_key' in SOURCE
    assert 'Rozdíl {supplier_label} ({round_b_name} vs {round_a_name})' in SOURCE

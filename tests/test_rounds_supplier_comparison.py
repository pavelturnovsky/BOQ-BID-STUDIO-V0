import ast
import types
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
SOURCE = (ROOT / "boq_bid_studio.py").read_text()


TARGET_FUNCTIONS = [
    "normalize_text",
    "generate_supplier_id",
    "canonical_supplier_name",
    "supplier_similarity_score",
    "resolve_supplier_cluster_map",
]


def build_helper_module() -> types.ModuleType:
    tree = ast.parse(SOURCE)
    module = types.ModuleType("boq_bid_rounds")
    prelude = """
import hashlib
import re
import unicodedata
from difflib import SequenceMatcher
from typing import Any, Dict, List, Mapping, Optional, Sequence, Set, Tuple
"""
    exec(prelude, module.__dict__)
    for node in tree.body:
        if isinstance(node, ast.FunctionDef) and node.name in TARGET_FUNCTIONS:
            func_src = ast.get_source_segment(SOURCE, node)
            if func_src:
                exec(func_src, module.__dict__)
    return module


module = build_helper_module()


def test_rounds_v2_has_inter_round_supplier_mode() -> None:
    assert '"Dodavatel mezi koly"' in SOURCE
    assert 'build_rounds_supplier_long_dataset(' in SOURCE
    assert '"Typ porovnání"' in SOURCE


def test_rounds_v2_uses_supplier_id_map_for_cross_round_matching() -> None:
    assert '"supplier_id_map"' in SOURCE
    assert 'supplier_id_map[raw_name]' in SOURCE
    assert 'selected_supplier_key' in SOURCE
    assert 'Rozdíl {supplier_label} ({round_b_name} vs {round_a_name})' in SOURCE


def test_canonical_supplier_name_matches_round_suffixes() -> None:
    assert module.canonical_supplier_name("GEMO 1.kolo") == "gemo"
    assert module.canonical_supplier_name("GEMO round 2") == "gemo"


def test_resolve_supplier_cluster_map_auto_matches_gemo_across_rounds() -> None:
    loaded_rounds = {
        "r1": {
            "bids_dict": {"GEMO 1.kolo.xlsx": object()},
            "alias_map": {"GEMO 1.kolo.xlsx": "GEMO 1.kolo"},
            "supplier_id_map": {"GEMO 1.kolo.xlsx": ""},
            "meta": {"round_name": "Kolo 1"},
        },
        "r2": {
            "bids_dict": {"GEMO 2.kolo.xlsx": object()},
            "alias_map": {"GEMO 2.kolo.xlsx": "GEMO 2.kolo"},
            "supplier_id_map": {"GEMO 2.kolo.xlsx": ""},
            "meta": {"round_name": "Kolo 2"},
        },
    }

    auto_map, unresolved = module.resolve_supplier_cluster_map(
        loaded_rounds,
        {"r1::GEMO 1.kolo.xlsx", "r2::GEMO 2.kolo.xlsx"},
    )

    assert unresolved == []
    assert auto_map["r1::GEMO 1.kolo"] == auto_map["r2::GEMO 2.kolo"]


def test_resolve_supplier_cluster_map_reports_conflict_for_ambiguous_names() -> None:
    loaded_rounds = {
        "r1": {
            "bids_dict": {"GEMO Praha.xlsx": object(), "GEMO Brno.xlsx": object()},
            "alias_map": {
                "GEMO Praha.xlsx": "GEMO Praha",
                "GEMO Brno.xlsx": "GEMO Brno",
            },
            "supplier_id_map": {"GEMO Praha.xlsx": "", "GEMO Brno.xlsx": ""},
            "meta": {"round_name": "Kolo 1"},
        },
        "r2": {
            "bids_dict": {"GEMO.xlsx": object()},
            "alias_map": {"GEMO.xlsx": "GEMO"},
            "supplier_id_map": {"GEMO.xlsx": ""},
            "meta": {"round_name": "Kolo 2"},
        },
    }

    auto_map, unresolved = module.resolve_supplier_cluster_map(
        loaded_rounds,
        {
            "r1::GEMO Praha.xlsx",
            "r1::GEMO Brno.xlsx",
            "r2::GEMO.xlsx",
        },
    )

    assert "r2::GEMO" in unresolved
    assert "r2::GEMO" not in auto_map

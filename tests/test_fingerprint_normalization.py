import io
import sys
import types
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.append(str(ROOT))
module_code = (ROOT / "boq_bid_studio.py").read_text().split(
    "# ------------- Sidebar Inputs -------------"
)[0]
module = types.ModuleType("boq_bid_helpers")
module.ENGINE_VERSION = "test"
module.SCHEMA_VERSION = "test"
module_code = module_code.split("HEADER_HINTS =", maxsplit=1)[0]
exec(module_code, module.__dict__)

compute_config_fingerprint = module.compute_config_fingerprint
fingerprints_match = module.fingerprints_match
hash_fileobj = module.hash_fileobj


def test_compute_config_fingerprint_normalizes_bid_order():
    master = io.BytesIO(b"master")
    bid_a = io.BytesIO(b"bid_a")
    bid_b = io.BytesIO(b"bid_b")

    hashes_first = {
        "master": hash_fileobj(master),
        "bid_0": hash_fileobj(bid_a),
        "bid_1": hash_fileobj(bid_b),
    }

    hashes_second = {
        "master": hashes_first["master"],
        "bid_0": hashes_first["bid_1"],
        "bid_1": hashes_first["bid_0"],
    }

    fp_first = compute_config_fingerprint(mode="supplier_only", input_hashes=hashes_first)
    fp_second = compute_config_fingerprint(mode="supplier_only", input_hashes=hashes_second)

    assert fp_first["input_hashes"] == fp_second["input_hashes"]


def test_fingerprints_match_respects_master_and_bids():
    bid_a = io.BytesIO(b"bid_a")
    bid_b = io.BytesIO(b"bid_b")

    hashes_first = {
        "master": "MASTER_HASH",
        "bid_0": hash_fileobj(bid_a),
        "bid_1": hash_fileobj(bid_b),
    }

    hashes_second = {
        "master": "MASTER_HASH",
        "bid_0": hashes_first["bid_1"],
        "bid_1": hashes_first["bid_0"],
    }

    legacy_fp = {
        "mode": "supplier_only",
        "input_hashes": hashes_first,
        "engine_version": "test",
        "schema_version": "test",
        "basket_mode": None,
        "quantity_mode": None,
        "dph_mode": None,
        "currency": None,
        "exchange_rate": None,
    }
    normalized_fp = compute_config_fingerprint(
        mode="supplier_only", input_hashes=hashes_second
    )

    assert fingerprints_match(legacy_fp, normalized_fp)
    assert fingerprints_match(normalized_fp, legacy_fp)
    assert fingerprints_match(legacy_fp, legacy_fp)

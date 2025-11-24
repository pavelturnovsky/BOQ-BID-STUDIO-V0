import io
import os
import sys
import types
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.append(str(ROOT))
os.environ["BOQ_BID_TEST_MODE"] = "1"
MODULE_CODE = (ROOT / "boq_bid_studio.py").read_text().split("# ------------- Sidebar Inputs -------------")[0]
module = types.ModuleType("boq_bid_helpers_storage")
exec(MODULE_CODE, module.__dict__)

OfferStorage = module.OfferStorage


def test_offer_storage_save_and_load_master(tmp_path) -> None:
    storage = OfferStorage(user_id="tester", base_dir=tmp_path)
    payload = io.BytesIO(b"master data")
    payload.name = "master.xlsx"

    storage.save_master(payload)

    stored_entries = storage.list_master()
    assert stored_entries and stored_entries[0]["name"] == "master.xlsx"

    loaded = storage.load_master("master.xlsx")
    assert loaded.read() == b"master data"
    expected_path = storage._category_dir("master") / storage._index["master"]["master.xlsx"]["path"]
    assert loaded.name == str(expected_path)
    assert loaded.original_name == "master.xlsx"
    assert Path(loaded.name).exists()


def test_offer_storage_overwrite_and_delete_bid(tmp_path) -> None:
    storage = OfferStorage(user_id="tester", base_dir=tmp_path)
    first = io.BytesIO(b"first")
    first.name = "bid.xlsx"
    storage.save_bid(first)

    updated = io.BytesIO(b"updated")
    updated.name = "bid.xlsx"
    storage.save_bid(updated)

    loaded = storage.load_bid("bid.xlsx")
    assert loaded.read() == b"updated"
    bid_path = storage._category_dir("bids") / storage._index["bids"]["bid.xlsx"]["path"]
    assert loaded.name == str(bid_path)
    assert loaded.original_name == "bid.xlsx"

    assert storage.delete_bid("bid.xlsx") is True
    assert storage.list_bids() == []
    with pytest.raises(FileNotFoundError):
        storage.load_bid("bid.xlsx")

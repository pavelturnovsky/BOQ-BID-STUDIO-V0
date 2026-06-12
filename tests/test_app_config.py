import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.append(str(ROOT))

from app_config import get_storage_dir


def test_storage_dir_uses_local_default(monkeypatch):
    monkeypatch.delenv("BOQ_STORAGE_DIR", raising=False)

    assert get_storage_dir() == Path.home() / ".boq_bid_studio"


def test_storage_dir_uses_environment_override(monkeypatch):
    monkeypatch.setenv("BOQ_STORAGE_DIR", "~/boq-data")

    assert get_storage_dir() == Path.home() / "boq-data"

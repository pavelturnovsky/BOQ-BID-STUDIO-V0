import sqlite3
from pathlib import Path

import pandas as pd
import pytest

from core.material_store import (
    MaterialCollector,
    MaterialPriceDatabase,
    build_material_batch,
    should_collect_sheet,
)
from workbook import WorkbookData


def make_sample_table() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "code": ["A1", "S1"],
            "description": ["Instalace kabelu", "Souhrn"],
            "unit": ["m", ""],
            "quantity": [10, None],
            "unit_price_material": [100, None],
            "unit_price_install": [20, None],
            "total_price": [1200, 0],
            "calc_total": [1200, None],
            "is_summary": [False, True],
            "row_ref": ["Master!5", "Master!6"],
            "section_total": [None, 1200],
            "summary_type": ["", "section"],
        }
    )


def test_should_collect_sheet_variants() -> None:
    assert should_collect_sheet("Master")
    assert should_collect_sheet("Mater")
    assert should_collect_sheet("Master data")
    assert not should_collect_sheet("Přehled")


def test_build_material_batch_filters_rows() -> None:
    df = make_sample_table()
    batch = build_material_batch(
        df,
        workbook_name="WB.xlsx",
        sheet_name="Master",
        file_hash="abc123",
        country="CZ",
        currency="CZK",
    )
    assert batch is not None
    assert len(batch.records) == 1
    record = batch.records[0]
    assert record.description == "Instalace kabelu"
    assert pytest.approx(record.quantity or 0, rel=1e-6) == 10.0
    assert record.row_ref == "Master!5"
    assert batch.country == "CZ"
    assert batch.currency == "CZK"


def test_material_price_database_store_batch(tmp_path: Path) -> None:
    df = make_sample_table()
    db = MaterialPriceDatabase(tmp_path / "materials.sqlite")

    first_batch = build_material_batch(
        df,
        workbook_name="WB.xlsx",
        sheet_name="Master",
        file_hash="hash123",
        country="CZ",
        currency="CZK",
        project_name="Projekt X",
    )
    assert first_batch is not None
    db.store_batch(first_batch)

    df.loc[0, "quantity"] = 12
    df.loc[0, "total_price"] = 1440
    updated_batch = build_material_batch(
        df,
        workbook_name="WB.xlsx",
        sheet_name="Master",
        file_hash="hash123",
        country="CZ",
        currency="CZK",
        project_name="Projekt X",
    )
    assert updated_batch is not None
    db.store_batch(updated_batch)

    with sqlite3.connect(tmp_path / "materials.sqlite") as conn:
        qty, total = conn.execute(
            "SELECT quantity, total_price FROM material_entries"
        ).fetchone()
        assert pytest.approx(qty, rel=1e-6) == 12.0
        assert pytest.approx(total, rel=1e-6) == 1440.0
        country = conn.execute(
            "SELECT country FROM sources LIMIT 1"
        ).fetchone()[0]
        assert country == "CZ"


def test_material_collector_collects_from_workbook(tmp_path: Path) -> None:
    df = make_sample_table()
    wb = WorkbookData(name="Test.xlsx", sheets={"Master": {"table": df}})
    database = MaterialPriceDatabase(tmp_path / "collector.sqlite")
    collector = MaterialCollector(database=database)

    inserted = collector.collect_from_workbook(
        wb,
        file_hash="filehash",
        country="PL",
        currency="PLN",
        project_name="Projekt Y",
        metadata={"source": "unit-test"},
    )
    assert inserted == 1

    with sqlite3.connect(tmp_path / "collector.sqlite") as conn:
        count = conn.execute("SELECT COUNT(*) FROM material_entries").fetchone()[0]
        assert count == 1
        stored_country = conn.execute(
            "SELECT country FROM sources LIMIT 1"
        ).fetchone()[0]
        assert stored_country == "PL"


def test_material_price_database_query_helpers(tmp_path: Path) -> None:
    db = MaterialPriceDatabase(tmp_path / "materials.sqlite")
    df_one = make_sample_table()
    batch_one = build_material_batch(
        df_one,
        workbook_name="WB1.xlsx",
        sheet_name="Master",
        file_hash="hash-one",
        country="CZ",
        currency="CZK",
        project_name="Projekt Alfa",
    )
    assert batch_one is not None
    db.store_batch(batch_one)

    df_two = make_sample_table()
    df_two.loc[0, "description"] = "Hliníkový profil"
    batch_two = build_material_batch(
        df_two,
        workbook_name="WB2.xlsx",
        sheet_name="Mater",
        file_hash="hash-two",
        country="PL",
        currency="PLN",
        project_name="Projekt Beta",
    )
    assert batch_two is not None
    db.store_batch(batch_two)

    stats = db.stats()
    assert stats["entries"] >= 2
    filters = db.available_filters()
    assert "CZ" in filters["countries"]
    assert "Projekt Beta" in filters["projects"]

    materials_all = db.fetch_materials()
    assert len(materials_all) >= 2
    assert set(materials_all["country"].dropna()) == {"CZ", "PL"}

    materials_cz = db.fetch_materials(countries=["CZ"])
    assert len(materials_cz) >= 1
    assert set(materials_cz["country"].dropna()) == {"CZ"}

    materials_search = db.fetch_materials(search="profil")
    assert len(materials_search) == 1
    assert materials_search.iloc[0]["project_name"] == "Projekt Beta"

    sources_df = db.list_sources()
    assert len(sources_df) == 2
    assert "metadata" in sources_df.columns
    assert isinstance(sources_df.iloc[0]["metadata"], dict)

from __future__ import annotations

import pandas as pd

import pytest

from core.aggregate import rollup_by_discipline, rollup_by_wbs


def test_rollup_by_discipline_median_baseline(offers: pd.DataFrame) -> None:
    summary = rollup_by_discipline(offers, baseline="median")
    assert not summary.empty
    vzt_rows = summary[summary["discipline"] == "VZT"]
    assert set(vzt_rows["supplier"]) == {"offer_A", "offer_B"}
    baseline_value = vzt_rows.iloc[0]["baseline_total"]
    assert baseline_value == pytest.approx(596970.0, rel=1e-3)


def test_rollup_by_wbs_prefix(offers: pd.DataFrame) -> None:
    summary = rollup_by_wbs(offers, baseline="median", prefix="03")
    assert "03.01" in set(summary["wbs_code"])

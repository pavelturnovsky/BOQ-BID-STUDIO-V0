"""Normalization helpers for currency, VAT and units."""
from __future__ import annotations

from typing import Any, Dict, Mapping

import numpy as np
import pandas as pd

from .utils import clean_text


def _unit_lookup(unit_map: Mapping[str, Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    lookup: Dict[str, Dict[str, Any]] = {}
    for key, data in unit_map.items():
        aliases = set(data.get("aliases", [])) | {key}
        target = data.get("target_unit", data.get("canonical", key))
        factor = float(data.get("factor", 1.0))
        for alias in aliases:
            lookup[clean_text(alias).lower()] = {
                "target": clean_text(target).lower() if isinstance(target, str) else target,
                "factor": factor,
            }
    return lookup


def normalize_units(df: pd.DataFrame, unit_map: Mapping[str, Dict[str, Any]]) -> pd.DataFrame:
    """Normalise quantity and unit columns according to configuration."""

    df = df.copy()
    lookup = _unit_lookup(unit_map)

    def _convert(row: pd.Series) -> pd.Series:
        unit_value = clean_text(row.get("unit", "")).lower()
        if not unit_value:
            return row
        if unit_value not in lookup:
            return row
        rule = lookup[unit_value]
        factor = rule.get("factor", 1.0)
        target = rule.get("target", unit_value)
        if row.get("qty") is not None and not pd.isna(row.get("qty")):
            row["qty"] = float(row["qty"]) * factor
        if row.get("unit_price") is not None and not pd.isna(row.get("unit_price")) and factor:
            row["unit_price"] = float(row["unit_price"]) / factor
        row["unit"] = target
        if row.get("total_price") is not None and not pd.isna(row.get("total_price")):
            # Recompute from qty/unit price to maintain precision
            row["total_price"] = row.get("qty", 0) * row.get("unit_price", 0)
        return row

    df = df.apply(_convert, axis=1)
    return df


def normalize_currency(
    df: pd.DataFrame,
    base_currency: str,
    rates: Mapping[str, float],
    default_vat: float,
) -> pd.DataFrame:
    """Convert monetary values to the base currency and compute VAT columns."""

    df = df.copy()
    df["currency"] = df["currency"].fillna(base_currency)
    df["currency"] = df["currency"].map(lambda x: clean_text(x).upper() if isinstance(x, str) else x)
    df["currency_rate"] = df["currency"].map(rates)
    df.loc[df["currency_rate"].isna(), "currency_rate"] = 1.0

    for column in ["unit_price", "total_price"]:
        df[f"{column}_normalized"] = df[column] * df["currency_rate"]

    df["vat_rate"] = pd.to_numeric(df.get("vat_rate"), errors="coerce").fillna(default_vat)
    df["net_total_price"] = df["total_price_normalized"]
    df["gross_total_price"] = df["net_total_price"] * (1 + df["vat_rate"])
    df["net_unit_price"] = df["unit_price_normalized"]
    df["gross_unit_price"] = df["net_unit_price"] * (1 + df["vat_rate"])
    return df


def validate_totals(df: pd.DataFrame, tolerance: float = 0.005) -> pd.DataFrame:
    """Flag rows where total does not agree with quantity * unit price."""

    df = df.copy()
    expected = df["qty"] * df["unit_price"]
    df["total_difference"] = df["total_price"] - expected
    expected_abs = expected.abs().replace(0, np.nan)
    df["total_mismatch"] = (
        expected.notna()
        & df["total_price"].notna()
        & (df["total_difference"].abs() > tolerance * expected_abs.fillna(1))
    )
    return df


__all__ = ["normalize_units", "normalize_currency", "validate_totals"]

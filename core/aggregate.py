"""Aggregation helpers for comparison views."""
from __future__ import annotations

from typing import Iterable, Mapping, Optional, Sequence

import pandas as pd


def _baseline_series(pivot: pd.DataFrame, baseline: str) -> pd.Series:
    if baseline == "median":
        return pivot.median(axis=1)
    if baseline not in pivot.columns:
        raise ValueError(f"Unknown baseline '{baseline}'")
    return pivot[baseline]


def _rollup(df: pd.DataFrame, group_col: str, baseline: str, label_col: str) -> pd.DataFrame:
    totals = (
        df.groupby([group_col, "supplier"], dropna=False)["total_price_normalized"].sum().reset_index()
    )
    pivot = totals.pivot(index=group_col, columns="supplier", values="total_price_normalized").fillna(0.0)
    baseline_series = _baseline_series(pivot, baseline)

    records = []
    for group_value, row in pivot.iterrows():
        baseline_value = baseline_series.loc[group_value]
        for supplier, total in row.items():
            records.append(
                {
                    label_col: group_value,
                    "supplier": supplier,
                    "total_price": float(total),
                    "baseline_total": float(baseline_value),
                    "total_diff": float(total - baseline_value),
                }
            )
    return pd.DataFrame(records)


def rollup_by_discipline(df: pd.DataFrame, baseline: str = "median") -> pd.DataFrame:
    """Aggregate totals per discipline and supplier."""

    if "primary_discipline" not in df.columns:
        raise ValueError("Discipline annotations missing; run assign_disciplines() first")
    return _rollup(df, "primary_discipline", baseline, "discipline")


def rollup_by_wbs(
    df: pd.DataFrame,
    baseline: str = "median",
    prefix: Optional[str] = None,
) -> pd.DataFrame:
    """Aggregate totals per WBS code, optionally filtered by prefix."""

    if "matched_wbs_code" not in df.columns:
        raise ValueError("Matching results missing; run match_items() first")
    data = df[df["match_status"] != "unmatched"].copy()
    if prefix:
        data = data[data["matched_wbs_code"].str.startswith(prefix)]
    return _rollup(data, "matched_wbs_code", baseline, "wbs_code")


def flag_outliers(
    df: pd.DataFrame,
    group_cols: Sequence[str] = ("item_code",),
    value_col: str = "net_unit_price",
    threshold: float = 1.5,
) -> pd.DataFrame:
    """Flag outliers using the IQR method within each group."""

    df = df.copy()
    df["is_outlier"] = False
    if value_col not in df.columns:
        return df

    for _, group in df.groupby(list(group_cols)):
        values = group[value_col].dropna()
        if values.empty:
            continue
        q1 = values.quantile(0.25)
        q3 = values.quantile(0.75)
        iqr = q3 - q1
        lower = q1 - threshold * iqr
        upper = q3 + threshold * iqr
        mask = (group[value_col] < lower) | (group[value_col] > upper)
        df.loc[group.index, "is_outlier"] = mask
    return df


__all__ = ["rollup_by_discipline", "rollup_by_wbs", "flag_outliers"]

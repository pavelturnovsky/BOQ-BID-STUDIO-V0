"""Aggregations for Excel outline nodes."""

from __future__ import annotations

from typing import Iterable, Iterator, List, Optional

import numpy as np
import pandas as pd

from .excel_outline import OutlineNode


def _iter_nodes(nodes: Iterable[OutlineNode]) -> Iterator[OutlineNode]:
    for node in nodes:
        yield node
        if node.children:
            yield from _iter_nodes(node.children)


def _parse_row_number(value: str) -> Optional[int]:
    if not value:
        return None
    if "!" in value:
        _, raw = value.split("!", 1)
    else:
        raw = value
    try:
        return int(str(raw))
    except (TypeError, ValueError):
        return None


def rollup_by_outline(
    df: pd.DataFrame,
    sheet: str,
    axis: str,
    level: int,
    start: int,
    end: int,
    *,
    include_columns: Optional[List[str]] = None,
) -> pd.Series:
    """Aggregate numeric columns for rows mapped to the given outline node."""

    if axis != "row":
        return pd.Series(dtype=float)
    if not isinstance(df, pd.DataFrame) or df.empty:
        return pd.Series(dtype=float)
    if "row_ref" not in df.columns:
        return pd.Series(dtype=float)

    row_numbers = df["row_ref"].astype(str).map(_parse_row_number)
    mask = row_numbers.notna()
    if sheet:
        mask &= df["row_ref"].astype(str).str.startswith(f"{sheet}!")
    mask &= row_numbers.between(start, end, inclusive="both")
    if not mask.any():
        return pd.Series(dtype=float)

    working = df.loc[mask].copy()
    numeric_cols = working.select_dtypes(include=[np.number]).columns.tolist()
    if include_columns is not None:
        numeric_cols = [col for col in include_columns if col in working.columns]
    metrics = working[numeric_cols].sum(numeric_only=True)
    metrics["__row_count__"] = int(mask.sum())
    metrics["__sheet__"] = sheet
    metrics["__axis__"] = axis
    metrics["__level__"] = level
    metrics["__range_start__"] = start
    metrics["__range_end__"] = end
    return metrics


def collect_outline_rollups(
    df: pd.DataFrame,
    nodes: Iterable[OutlineNode],
    *,
    include_columns: Optional[List[str]] = None,
) -> pd.DataFrame:
    """Return aggregated metrics for all nodes in ``nodes``."""

    records: List[pd.Series] = []
    for node in _iter_nodes(nodes):
        metrics = rollup_by_outline(
            df,
            sheet=node.sheet,
            axis=node.axis,
            level=node.level,
            start=node.start,
            end=node.end,
            include_columns=include_columns,
        )
        if not metrics.empty:
            metrics["axis"] = node.axis
            metrics["level"] = node.level
            metrics["range_start"] = node.start
            metrics["range_end"] = node.end
            metrics["collapsed"] = node.collapsed
            records.append(metrics)
    if not records:
        return pd.DataFrame()
    return pd.DataFrame(records)

"""Comparison engine for supplier bids."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import numpy as np
import pandas as pd

from .config import ComparisonConfig
from .search import SearchProvider

logger = logging.getLogger(__name__)



@dataclass
class ComparisonResult:
    """Structured output from :func:`compare_bids`."""

    items: pd.DataFrame
    summary: pd.DataFrame
    unmatched: pd.DataFrame
    metadata: Dict[str, Any] = field(default_factory=dict)


def compare_bids(
    master: pd.DataFrame,
    bids: Sequence[pd.DataFrame],
    comparison: ComparisonConfig,
    search_provider: Optional[SearchProvider] = None,
    search_fields: Optional[Sequence[str]] = None,
    search_top_k: int = 5,
    search_metadata_fields: Optional[Sequence[str]] = None,
) -> ComparisonResult:
    """Compare each supplier bid against the master dataset."""

    if master.empty:
        raise ValueError("Master dataset is empty; cannot perform comparison")

    if "record_key" not in master.columns:
        raise KeyError("Master dataset must contain the 'record_key' column")

    master_prefixed = _prepare_for_join(master, "master")
    comparison_metric = _select_comparison_metric(master, comparison.numeric_columns)

    if search_provider and search_fields:
        metadata_fields = list(search_metadata_fields or [])
        if not metadata_fields:
            metadata_fields = [field for field in ("code", "description") if field in master.columns]
        try:
            search_provider.index(master, text_columns=search_fields, metadata_columns=metadata_fields)
        except Exception:  # pragma: no cover - defensive logging
            logger.exception("Failed to index master dataset for semantic search")
            search_provider = None

    item_frames: List[pd.DataFrame] = []
    summary_rows: List[Dict[str, Any]] = []
    unmatched_rows: List[Dict[str, Any]] = []

    for bid_frame in bids:
        supplier = _extract_supplier_name(bid_frame)
        prefix = _normalise_prefix(supplier)
        logger.info("Comparing bid '%s'", supplier)
        bid_items, summary_row, unmatched = _compare_single_supplier(
            master_prefixed,
            bid_frame,
            supplier,
            prefix,
            comparison.numeric_columns,
            comparison_metric,
            search_provider,
            search_top_k,
        )
        item_frames.append(bid_items)
        summary_rows.append(summary_row)
        unmatched_rows.extend(unmatched)

    items_df = pd.concat(item_frames, ignore_index=True) if item_frames else pd.DataFrame()
    summary_df = pd.DataFrame(summary_rows)
    unmatched_df = pd.DataFrame(unmatched_rows)

    metadata = {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "master_rows": int(master.shape[0]),
        "supplier_count": len(bids),
        "currency": comparison.currency,
        "comparison_metric": comparison_metric,
        "search_provider": search_provider.__class__.__name__ if search_provider else None,
        "search_top_k": search_top_k,
    }

    return ComparisonResult(
        items=items_df,
        summary=summary_df,
        unmatched=unmatched_df,
        metadata=metadata,
    )


def _compare_single_supplier(
    master_prefixed: pd.DataFrame,
    bid_frame: pd.DataFrame,
    supplier: str,
    prefix: str,
    numeric_columns: Sequence[str],
    comparison_metric: str,
    search_provider: Optional[SearchProvider],
    search_top_k: int,
) -> Tuple[pd.DataFrame, Dict[str, Any], List[Dict[str, Any]]]:
    if "record_key" not in bid_frame.columns:
        raise KeyError("Bid dataset must contain the 'record_key' column")

    bid_prefixed = _prepare_for_join(bid_frame, prefix, drop_columns={"supplier"})
    merged = master_prefixed.join(bid_prefixed, how="outer", sort=False)
    merged.index.name = "record_key"

    items = pd.DataFrame(index=merged.index)
    items["supplier"] = supplier
    items["record_key"] = merged.index
    items["code"] = _first_non_null(merged, ["code_master", f"code_{prefix}"])
    items["description_master"] = merged.get("description_master")
    items["description_supplier"] = merged.get(f"description_{prefix}")
    items["unit_master"] = merged.get("unit_master")
    items["unit_supplier"] = merged.get(f"unit_{prefix}")

    for column in numeric_columns:
        master_col = merged.get(f"{column}_master")
        if master_col is None:
            master_col = pd.Series(np.nan, index=merged.index, dtype=float)
        supplier_col = merged.get(f"{column}_{prefix}")
        if supplier_col is None:
            supplier_col = pd.Series(np.nan, index=merged.index, dtype=float)
        items[f"{column}_master"] = master_col
        items[f"{column}_supplier"] = supplier_col
        diff = supplier_col - master_col
        items[f"{column}_difference"] = diff
        items[f"{column}_pct_diff"] = _compute_pct(diff, master_col)

    comparison_master = items[f"{comparison_metric}_master"]
    comparison_supplier = items[f"{comparison_metric}_supplier"]
    items["matched"] = comparison_master.notna() & comparison_supplier.notna()

    summary_row = _build_summary_row(
        supplier,
        items,
        comparison_metric,
    )

    unmatched_rows = _extract_unmatched_rows(
        supplier,
        items,
        comparison_metric,
        search_provider,
        search_top_k,
    )

    items = items.reset_index(drop=True)
    return items, summary_row, unmatched_rows


def _prepare_for_join(
    frame: pd.DataFrame,
    prefix: str,
    drop_columns: Optional[Iterable[str]] = None,
) -> pd.DataFrame:
    drop_columns = set(drop_columns or [])
    columns_to_keep = [col for col in frame.columns if col not in drop_columns]
    prepared = frame.loc[:, columns_to_keep].copy()
    rename_map = {
        column: f"{column}_{prefix}"
        for column in prepared.columns
        if column != "record_key"
    }
    prepared = prepared.rename(columns=rename_map)
    return prepared.set_index("record_key")


def _normalise_prefix(text: str) -> str:
    import re

    normalised = re.sub(r"[^0-9A-Za-z]+", "_", text).strip("_")
    return normalised.lower() or "supplier"


def _extract_supplier_name(frame: pd.DataFrame) -> str:
    if "supplier" in frame.columns and frame["supplier"].notna().any():
        return str(frame["supplier"].dropna().iloc[0])
    return "Supplier"


def _first_non_null(frame: pd.DataFrame, columns: Sequence[str]) -> pd.Series:
    available = [frame.get(column) for column in columns if column in frame]
    if not available:
        return pd.Series(index=frame.index, dtype=object)
    combined = pd.concat(available, axis=1)
    return combined.bfill(axis=1).iloc[:, 0]


def _compute_pct(diff: pd.Series, master: Optional[pd.Series]) -> pd.Series:
    master = master if master is not None else pd.Series(np.nan, index=diff.index)
    with np.errstate(divide="ignore", invalid="ignore"):
        pct = np.where(master != 0, (diff / master) * 100, np.nan)
    return pd.Series(pct, index=diff.index)


def _build_summary_row(
    supplier: str,
    items: pd.DataFrame,
    comparison_metric: str,
) -> Dict[str, Any]:
    master_col = items[f"{comparison_metric}_master"].fillna(0.0)
    supplier_col = items[f"{comparison_metric}_supplier"].fillna(0.0)
    diff = supplier_col - master_col

    total_master = float(master_col.sum())
    total_supplier = float(supplier_col.sum())
    total_diff = float(diff.sum())
    with np.errstate(divide="ignore", invalid="ignore"):
        pct = (total_diff / total_master * 100.0) if total_master else np.nan

    missing = int(((items[f"{comparison_metric}_supplier"].isna()) & (items[f"{comparison_metric}_master"].notna())).sum())
    extra = int(((items[f"{comparison_metric}_supplier"].notna()) & (items[f"{comparison_metric}_master"].isna())).sum())

    return {
        "supplier": supplier,
        "master_total": total_master,
        "supplier_total": total_supplier,
        "difference": total_diff,
        "difference_pct": pct,
        "matched_items": int(items["matched"].sum()),
        "missing_items": missing,
        "extra_items": extra,
    }


def _extract_unmatched_rows(
    supplier: str,
    items: pd.DataFrame,
    comparison_metric: str,
    search_provider: Optional[SearchProvider],
    search_top_k: int,
) -> List[Dict[str, Any]]:
    unmatched_mask = items[f"{comparison_metric}_master"].isna() & items[f"{comparison_metric}_supplier"].notna()
    if not unmatched_mask.any():
        return []

    unmatched_items = items.loc[unmatched_mask]
    rows: List[Dict[str, Any]] = []
    for _, row in unmatched_items.iterrows():
        description = str(row.get("description_supplier", ""))
        suggestions: List[Dict[str, Any]] = []
        if search_provider and description.strip():
            try:
                results = search_provider.search(description, top_k=search_top_k)
            except Exception:  # pragma: no cover - defensive logging
                logger.exception("Search provider failed when querying '%s'", description)
                results = []
            suggestions = [
                {
                    "score": result.score,
                    **{key: result.metadata.get(key) for key in ("code", "description")},
                }
                for result in results
            ]
        rows.append(
            {
                "supplier": supplier,
                "record_key": row["record_key"],
                "code": row.get("code"),
                "description": description,
                "suggestions": json.dumps(suggestions, ensure_ascii=False) if suggestions else "",
            }
        )
    return rows


def _select_comparison_metric(master: pd.DataFrame, candidates: Sequence[str]) -> str:
    for candidate in candidates:
        if f"{candidate}" in master.columns:
            return candidate
    raise KeyError("None of the specified numeric columns are present in the master dataset")


__all__ = ["ComparisonResult", "compare_bids"]

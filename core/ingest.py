"""Data ingestion helpers for supplier offers."""
from __future__ import annotations

from pathlib import Path
from typing import Dict, Iterable, List, Mapping, Tuple

import pandas as pd

from .utils import clean_text, normalise_header

MANDATORY_COLUMNS = [
    "item_code",
    "item_desc",
    "unit",
    "qty",
    "unit_price",
    "total_price",
]


def read_workbook(path: str | Path) -> List[pd.DataFrame]:
    """Read an Excel/CSV workbook and return all sheets as data frames.

    Each returned frame stores the ``sheet_name`` and ``source_path`` metadata in
    ``DataFrame.attrs`` so downstream components can preserve provenance.
    """

    file_path = Path(path)
    if not file_path.exists():
        raise FileNotFoundError(file_path)

    frames: List[pd.DataFrame] = []
    suffix = file_path.suffix.lower()
    if suffix in {".csv", ".tsv", ".txt"}:
        df = pd.read_csv(file_path, header=None, dtype=object, sep="," if suffix != ".tsv" else "\t")
        df.attrs["sheet_name"] = "Sheet1"
        df.attrs["source_path"] = str(file_path)
        frames.append(df)
        return frames

    excel = pd.ExcelFile(file_path)
    for sheet_name in excel.sheet_names:
        df = excel.parse(sheet_name, header=None, dtype=object)
        df.attrs["sheet_name"] = sheet_name
        df.attrs["source_path"] = str(file_path)
        frames.append(df)
    return frames


def _build_synonym_lookup(column_map: Mapping[str, Iterable[str]]) -> Dict[str, str]:
    lookup: Dict[str, str] = {}
    for canonical, aliases in column_map.items():
        lookup[normalise_header(canonical)] = canonical
        for alias in aliases:
            lookup[normalise_header(str(alias))] = canonical
    return lookup


def _detect_header_row(df: pd.DataFrame, lookup: Mapping[str, str], max_rows: int = 10) -> Tuple[int, Dict[str, int]]:
    best_row = -1
    best_mapping: Dict[str, int] = {}
    best_score = -1
    rows_to_check = min(max_rows, len(df))
    for row_idx in range(rows_to_check):
        row = df.iloc[row_idx]
        mapping: Dict[str, int] = {}
        for col_idx, value in enumerate(row):
            if value is None or (isinstance(value, float) and pd.isna(value)):
                continue
            key = normalise_header(str(value))
            canonical = lookup.get(key)
            if canonical and canonical not in mapping:
                mapping[canonical] = col_idx
        score = len(mapping)
        if score > best_score:
            best_row = row_idx
            best_score = score
            best_mapping = mapping
    if best_row < 0:
        raise ValueError("Unable to detect header row")
    return best_row, best_mapping


def auto_map_columns(df: pd.DataFrame, config: Mapping[str, Mapping[str, Iterable[str]]]) -> Tuple[pd.DataFrame, Dict[str, int], int]:
    """Normalise the sheet columns based on the configuration mapping."""

    column_map = config.get("column_map", {})
    lookup = _build_synonym_lookup(column_map)
    header_row, mapping = _detect_header_row(df, lookup)

    body = df.iloc[header_row + 1 :].copy()
    body.dropna(how="all", inplace=True)
    body.reset_index(drop=True, inplace=True)

    rename_map = {idx: canonical for canonical, idx in mapping.items()}
    body.rename(columns=rename_map, inplace=True)

    # Record the original row number for traceability
    body["row_ref"] = range(header_row + 2, header_row + 2 + len(body))

    return body, mapping, header_row


def _prepare_dataframe(df: pd.DataFrame, sheet_name: str, source_path: str, supplier: str) -> pd.DataFrame:
    df = df.copy()
    df["sheet_name"] = sheet_name
    df["source_path"] = source_path
    df["supplier"] = supplier
    for column in MANDATORY_COLUMNS:
        if column not in df.columns:
            df[column] = None
    optional_defaults = {
        "discipline_hint": None,
        "currency": None,
        "vat_rate": None,
    }
    for column, default in optional_defaults.items():
        if column not in df.columns:
            df[column] = default
    df["item_desc"] = df["item_desc"].map(clean_text)

    numeric_cols = ["qty", "unit_price", "total_price"]
    for column in numeric_cols:
        df[column] = pd.to_numeric(df[column], errors="coerce")

    missing_total = df["total_price"].isna() & df["qty"].notna() & df["unit_price"].notna()
    df.loc[missing_total, "total_price"] = df.loc[missing_total, "qty"] * df.loc[missing_total, "unit_price"]
    return df


def load_offers(paths: Iterable[str | Path], config: Mapping[str, Mapping[str, Iterable[str]]]) -> pd.DataFrame:
    """Load multiple supplier offers into a single normalised dataframe."""

    frames: List[pd.DataFrame] = []
    for path in paths:
        source_path = Path(path)
        supplier = source_path.stem
        for sheet in read_workbook(source_path):
            sheet_name = sheet.attrs.get("sheet_name", "Sheet1")
            mapped, mapping, header_row = auto_map_columns(sheet, config)
            frame = _prepare_dataframe(mapped, sheet_name, str(source_path), supplier)
            frame.attrs["header_row"] = header_row
            frame.attrs["mapping"] = mapping
            frames.append(frame)
    if not frames:
        raise ValueError("No data loaded from provided inputs")
    combined = pd.concat(frames, ignore_index=True, sort=False)
    combined["item_code"] = combined["item_code"].map(lambda x: clean_text(x) or None)
    combined["unit"] = combined["unit"].map(lambda x: clean_text(x).lower() if isinstance(x, str) else x)
    combined.dropna(subset=["item_desc"], inplace=True)
    combined.reset_index(drop=True, inplace=True)
    return combined


__all__ = ["read_workbook", "auto_map_columns", "load_offers", "MANDATORY_COLUMNS"]

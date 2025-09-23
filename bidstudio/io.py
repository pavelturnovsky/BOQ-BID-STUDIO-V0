"""IO helpers for working with tabular bid data."""

from __future__ import annotations

import logging
import re
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence

import numpy as np
import pandas as pd

from .config import BidConfig, ColumnMapping

logger = logging.getLogger(__name__)

CANONICAL_COLUMNS: Sequence[str] = (
    "code",
    "description",
    "unit",
    "quantity",
    "unit_price",
    "total_price",
)
NUMERIC_COLUMNS = {"quantity", "unit_price", "total_price"}

HEADER_HINTS: Dict[str, Sequence[str]] = {
    "code": [
        "code",
        "item",
        "regex:^č\\.?$",
        "číslo položky",
        "cislo polozky",
        "kód",
        "kod",
        "pol.",
        "regex:^pol$",
    ],
    "description": [
        "description",
        "popis",
        "položka",
        "polozka",
        "název",
        "nazev",
        "specifikace",
    ],
    "unit": [
        "unit",
        "jm",
        "mj",
        "jednotka",
        "uom",
        "měrná jednotka",
        "merna jednotka",
    ],
    "quantity": ["quantity", "qty", "množství", "mnozstvi", "q"],
    "unit_price": [
        "unit price",
        "unitprice",
        "cena jednotková",
        "cena jednotkova",
        "cena ks",
    ],
    "total_price": ["cena celkem", "celková cena", "total price", "celkem"],
}

REQUIRED_AUTODETECT_KEYS: Sequence[str] = ("code", "description")


def load_master_dataset(
    path: Path,
    columns: ColumnMapping,
    key_columns: Sequence[str],
    chunk_size: Optional[int] = None,
) -> pd.DataFrame:
    """Load and normalise the master bill of quantities."""

    logger.info("Loading master dataset from %s", path)
    return _load_dataset(path, columns, key_columns, chunk_size)


def load_bid_dataset(
    bid: BidConfig,
    columns: ColumnMapping,
    key_columns: Sequence[str],
    chunk_size: Optional[int] = None,
) -> pd.DataFrame:
    """Load and normalise a supplier bid."""

    logger.info("Loading bid '%s' from %s", bid.name, bid.path)
    frame = _load_dataset(bid.path, columns, key_columns, chunk_size)
    frame.insert(0, "supplier", bid.name)
    return frame


def _load_dataset(
    path: Path,
    columns: ColumnMapping,
    key_columns: Sequence[str],
    chunk_size: Optional[int],
) -> pd.DataFrame:
    if not Path(path).exists():
        raise FileNotFoundError(f"Dataset '{path}' does not exist")

    ext = path.suffix.lower()
    if ext in {".csv", ".txt"}:
        frame = _load_csv(path, columns, key_columns, chunk_size)
    elif ext in {".xlsx", ".xls"}:
        frame = _load_excel(path, columns, key_columns)
    else:
        raise ValueError(f"Unsupported file extension '{ext}' for dataset '{path}'")

    return frame


def _load_csv(
    path: Path,
    columns: ColumnMapping,
    key_columns: Sequence[str],
    chunk_size: Optional[int],
) -> pd.DataFrame:
    logger.debug("Reading CSV %s with chunk size %s", path, chunk_size)
    read_kwargs = {"dtype": str}
    if chunk_size and chunk_size > 0:
        chunks: List[pd.DataFrame] = []
        for chunk in pd.read_csv(path, chunksize=chunk_size, **read_kwargs):
            chunks.append(_normalise_frame(chunk, columns, key_columns))
        if not chunks:
            return _empty_frame()
        return pd.concat(chunks, ignore_index=True)

    raw = pd.read_csv(path, **read_kwargs)
    return _normalise_frame(raw, columns, key_columns)


def _load_excel(
    path: Path,
    columns: ColumnMapping,
    key_columns: Sequence[str],
) -> pd.DataFrame:
    logger.debug("Reading Excel %s", path)
    raw = pd.read_excel(path, dtype=str)
    return _normalise_frame(raw, columns, key_columns)


def _normalise_frame(
    frame: pd.DataFrame,
    columns: ColumnMapping,
    key_columns: Sequence[str],
) -> pd.DataFrame:
    resolved_columns = _resolve_column_mapping(frame, columns)
    rename_map = {
        source: target
        for target, source in resolved_columns.items()
        if source is not None
    }
    normalised = frame.rename(columns=rename_map)

    for column in CANONICAL_COLUMNS:
        if column not in normalised:
            normalised[column] = np.nan if column in NUMERIC_COLUMNS else ""

    normalised = normalised.loc[:, CANONICAL_COLUMNS].copy()
    normalised["code"] = normalised["code"].astype(str).str.strip()
    normalised["description"] = normalised["description"].fillna("").astype(str)
    if "unit" in normalised.columns:
        normalised["unit"] = normalised["unit"].fillna("").astype(str)

    for column in NUMERIC_COLUMNS:
        normalised[column] = coerce_numeric(normalised[column])

    _ensure_key_columns(normalised, key_columns)
    return normalised


def _resolve_column_mapping(
    frame: pd.DataFrame, columns: ColumnMapping
) -> Dict[str, Optional[str]]:
    """Resolve canonical column names using config overrides and auto-detection."""

    config_map = columns.as_dict()
    resolved: Dict[str, Optional[str]] = {}

    available_columns = list(frame.columns)
    normalised_lookup = {
        _normalise_header(col): col for col in available_columns if isinstance(col, str)
    }

    auto_targets: List[str] = []
    for key, source in config_map.items():
        if _is_auto(source):
            resolved[key] = None
            auto_targets.append(key)
            continue

        source_str = str(source)
        if source_str in frame.columns:
            resolved[key] = source_str
            continue

        fallback = normalised_lookup.get(_normalise_header(source_str))
        if fallback is not None:
            resolved[key] = fallback
            continue

        raise KeyError(f"Column '{source}' for '{key}' was not found in dataset")

    if auto_targets:
        detected = _autodetect_column_mapping(frame, auto_targets)
        for key, value in detected.items():
            if value is not None:
                resolved[key] = value

    missing_required = [
        key for key in REQUIRED_AUTODETECT_KEYS if not resolved.get(key)
    ]
    if missing_required:
        raise KeyError(
            "Unable to resolve required columns: " + ", ".join(sorted(missing_required))
        )

    return resolved


def _autodetect_column_mapping(
    frame: pd.DataFrame, targets: Iterable[str]
) -> Dict[str, Optional[str]]:
    """Best-effort inference of canonical columns using header hints."""

    detected: Dict[str, Optional[str]] = {target: None for target in targets}
    if not list(frame.columns):
        return detected

    header_pairs = [
        (col, _normalise_header(col))
        for col in frame.columns
    ]

    for target in targets:
        patterns = _build_hint_patterns(target)
        match = _match_header(header_pairs, patterns)
        if match is not None:
            detected[target] = match
            logger.debug("Autodetected column '%s' for '%s'", match, target)
        else:
            logger.debug("Failed to autodetect column for '%s'", target)

    return detected


def _build_hint_patterns(target: str) -> Dict[str, List[str]]:
    hints = list(HEADER_HINTS.get(target, []))
    patterns: Dict[str, List[str]] = {
        "exact": [],
        "regex": [],
        "contains": [],
        "substring": [],
    }

    for hint in hints:
        if not hint:
            continue
        if hint.startswith("regex:"):
            patterns["regex"].append(hint[len("regex:") :])
        else:
            normalised = _normalise_header(hint)
            if normalised:
                patterns["exact"].append(normalised)
                escaped = re.escape(normalised)
                patterns["contains"].append(rf"(?:^|\\b){escaped}(?:\\b|$)")
                patterns["substring"].append(normalised)

    canonical = _normalise_header(target)
    patterns["exact"].append(canonical)
    if canonical:
        patterns["substring"].append(canonical)
    return patterns


def _match_header(
    headers: Sequence[tuple[str, str]], patterns: Dict[str, List[str]]
) -> Optional[str]:
    for original, normalised in headers:
        if normalised in patterns.get("exact", []):
            return original

    for regex_pattern in patterns.get("regex", []):
        try:
            compiled = re.compile(regex_pattern, flags=re.IGNORECASE)
        except re.error:
            continue
        for original, normalised in headers:
            if compiled.search(normalised):
                return original

    for contains_pattern in patterns.get("contains", []):
        try:
            compiled = re.compile(contains_pattern, flags=re.IGNORECASE)
        except re.error:
            continue
        for original, normalised in headers:
            if compiled.search(normalised):
                return original

    for substring in patterns.get("substring", []):
        if not substring:
            continue
        for original, normalised in headers:
            if substring in normalised:
                return original

    return None


def _normalise_header(value: Any) -> str:
    text = "" if value is None else str(value)
    text = text.strip().lower()
    text = re.sub(r"\s+", " ", text)
    return text


def _is_auto(value: Optional[str]) -> bool:
    if value is None:
        return True
    if isinstance(value, str) and value.strip().lower() in {"auto", "autodetect", "automatic"}:
        return True
    return False


def coerce_numeric(values: pd.Series) -> pd.Series:
    """Coerce textual representations of numbers into floats."""

    if not isinstance(values, pd.Series):
        values = pd.Series(values)
    if values.empty:
        return pd.to_numeric(values, errors="coerce")

    cleaned = values.astype(str)
    cleaned = cleaned.str.replace(r"\s+", "", regex=True)
    cleaned = cleaned.str.replace("\u00A0", "", regex=False)
    cleaned = cleaned.str.replace(
        r"(?i)(czk|kč|eur|€|usd|\$|gbp|£)", "", regex=True
    )
    cleaned = cleaned.str.replace(r"[+-]$", "", regex=True)
    cleaned = cleaned.str.replace(r"[^0-9,\.\-+]", "", regex=True)

    mask = cleaned.str.contains(",") & cleaned.str.contains(".")
    cleaned = cleaned.where(~mask, cleaned.str.replace(".", "", regex=False))

    cleaned = cleaned.str.replace(",", ".", regex=False)
    cleaned = cleaned.str.replace(r"[.,]$", "", regex=True)

    return pd.to_numeric(cleaned, errors="coerce")


def _ensure_key_columns(frame: pd.DataFrame, key_columns: Sequence[str]) -> None:
    if not key_columns:
        raise ValueError("At least one key column must be defined for comparisons")

    missing = [column for column in key_columns if column not in frame.columns]
    if missing:
        raise KeyError(f"Missing required key columns: {', '.join(missing)}")

    key_series = frame[key_columns[0]].astype(str).str.strip()
    for column in key_columns[1:]:
        key_series = key_series + "||" + frame[column].astype(str).str.strip()
    frame.insert(0, "record_key", key_series)


def _empty_frame() -> pd.DataFrame:
    return pd.DataFrame(columns=["record_key", *CANONICAL_COLUMNS])


__all__ = [
    "load_master_dataset",
    "load_bid_dataset",
]

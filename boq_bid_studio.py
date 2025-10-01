
import hashlib
import io
import math
import re
import json
import time
import unicodedata
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import numpy as np
import pandas as pd
import plotly.express as px
import streamlit as st
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4, landscape
from reportlab.lib.enums import TA_CENTER, TA_LEFT
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import mm
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.platypus import (
    Image as RLImage,
    Paragraph,
    SimpleDocTemplate,
    Spacer,
    Table,
    TableStyle,
)
from workbook import WorkbookData
from embedded_fonts import get_noto_sans_bold, get_noto_sans_regular

# ------------- App Config -------------
st.set_page_config(page_title="BoQ Bid Studio V.04", layout="wide")
st.title("🏗️ BoQ Bid Studio V.04")
st.caption("Jedna aplikace pro nahrání, mapování, porovnání nabídek a vizualizace — bez exportů do Excelu.")

# ------------- Helpers -------------

HEADER_HINTS = {
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
    "description": ["description", "popis", "položka", "polozka", "název", "nazev", "specifikace"],
    "unit": ["unit", "jm", "mj", "jednotka", "uom", "měrná jednotka", "merna jednotka"],
    "quantity": ["quantity", "qty", "množství", "mnozstvi", "q"],
    # optional extras commonly seen
    "item_id": [
        "celková cena",
        "celkova cena",
        "item id",
        "itemid",
        "id položky",
        "id polozky",
        "číslo položky",
        "cislo polozky",
        "regex:^id$",
        "kod",
        "kód",
    ],
    # extended optional columns for richer comparisons
    "quantity_supplier": [
        "množství dodavatel",
        "mnozstvi dodavatel",
        "množství dle dodavatele",
        "mnozstvi dle dodavatele",
        "qty supplier",
        "quantity supplier",
    ],
    "unit_price_material": ["cena materiál", "cena material", "unit price material", "materiál", "material"],
    "unit_price_install": ["cena montáž", "cena montaz", "unit price install", "montáž", "montaz"],
    "total_price": ["cena celkem", "celková cena", "total price", "celkem"],
    "summary_total": ["celkem za oddíl", "součet oddíl", "součet za oddíl"],
}

# For některé souhrnné listy nemusí být množství dostupné
REQUIRED_KEYS = ["code", "description"]  # unit & quantity can be optional at parse time

DEFAULT_EXCHANGE_RATE = 25.51
EXCHANGE_RATE_STATE_KEY = "exchange_rate_shared_value"
EXCHANGE_RATE_WIDGET_KEYS = {
    "summary": "summary_exchange_rate",
    "recap": "recap_exchange_rate",
}
RESERVED_ALIAS_NAMES = {"Master", "LOWEST"}
DEFAULT_STORAGE_DIR = Path.home() / ".boq_bid_studio"

try:
    MODULE_DIR = Path(__file__).resolve().parent
except NameError:
    MODULE_DIR = Path.cwd()

PDF_FONT_REGULAR = "NotoSans"
PDF_FONT_BOLD = "NotoSans-Bold"
_PDF_FONT_STATE: Optional[Tuple[str, str]] = None

RECAP_CATEGORY_CONFIG = [
    {
        "code_token": "0",
        "match_label": "Demolice a sanace",
        "fallback_label": "Demolice a sanace",
    },
    {
        "code_token": "1",
        "match_label": "Objekt",
        "fallback_label": "Objekt",
    },
    {
        "code_token": "2",
        "match_label": "Fit-out - Kanceláře pronájem 4.NP, 5.NP, 7.NP",
        "fallback_label": "Fit-out - Kanceláře pronájem 4.NP, 5.NP, 7.NP",
    },
    {
        "code_token": "3",
        "match_label": "Fit-out - Kanceláře objekt 4.NP - 5.NP",
        "fallback_label": "Fit-out - Kanceláře objekt 4.NP - 5.NP",
    },
    {
        "code_token": "4",
        "match_label": "Fit-out - Retail 1.PP, 1.NP - 3.NP, 5.NP, 6.NP",
        "fallback_label": "Fit-out - Retail 1.PP, 1.NP - 3.NP, 5.NP, 6.NP",
    },
    {
        "code_token": "5",
        "match_label": "SHELL @ CORE (Automyčka 1.PP)",
        "fallback_label": "SHELL @ CORE (Automyčka 1.PP)",
    },
    {
        "code_token": "VE",
        "match_label": "VE Alternativní řešení zadané objednatelem",
        "fallback_label": "VE Alternativní řešení zadané objednatelem",
        "is_deduction": True,
    },
    {
        "code_token": "15",
        "match_label": "15. Opční položky",
        "fallback_label": "15. Opční položky",
        "is_deduction": True,
    },
]


PERCENT_DIFF_SUFFIX = "_pct_diff"
PERCENT_DIFF_LABEL = " — ODCHYLKA VS MASTER (%)"
UNMAPPED_ROW_LABEL = "Nemapované položky"

SECTION_ONTOLOGY = {
    str(item.get("code_token", "")): item.get("fallback_label") or item.get("match_label", "")
    for item in RECAP_CATEGORY_CONFIG
    if item.get("code_token")
}
SECTION_ONTOLOGY.setdefault("", "Nezařazeno")


@dataclass
class ComparisonDataset:
    sheet: str
    analysis_df: pd.DataFrame
    value_columns: List[str]
    percent_columns: List[str]
    diff_columns: List[str]
    suppliers: List[str]
    supplier_order: List[str]
    section_labels: List[str]
    master_column: Optional[str]
    long_df: pd.DataFrame


def is_master_column(column_name: str) -> bool:
    """Return True if the provided column represents Master totals."""

    normalized = str(column_name or "").strip()
    if normalized.endswith(" total"):
        normalized = normalized[: -len(" total")]
    return normalized.casefold() == "master"


def ensure_exchange_rate_state(default: float = DEFAULT_EXCHANGE_RATE) -> None:
    """Synchronize exchange rate widgets across tabs without duplicate IDs."""

    shared_value = float(st.session_state.get(EXCHANGE_RATE_STATE_KEY, default))
    if EXCHANGE_RATE_STATE_KEY not in st.session_state:
        st.session_state[EXCHANGE_RATE_STATE_KEY] = shared_value

    for widget_key in EXCHANGE_RATE_WIDGET_KEYS.values():
        if widget_key not in st.session_state:
            st.session_state[widget_key] = shared_value

    for widget_key in EXCHANGE_RATE_WIDGET_KEYS.values():
        widget_value = st.session_state.get(widget_key)
        if widget_value is None:
            continue
        try:
            widget_float = float(widget_value)
        except (TypeError, ValueError):
            continue
        if not math.isclose(widget_float, shared_value, rel_tol=1e-9, abs_tol=1e-9):
            shared_value = widget_float
            st.session_state[EXCHANGE_RATE_STATE_KEY] = shared_value
            break

    for widget_key in EXCHANGE_RATE_WIDGET_KEYS.values():
        st.session_state[widget_key] = shared_value


def update_exchange_rate_shared(value: Any) -> float:
    """Persist the provided exchange rate into shared session state."""

    try:
        exchange_rate = float(value)
    except (TypeError, ValueError):
        exchange_rate = float(
            st.session_state.get(EXCHANGE_RATE_STATE_KEY, DEFAULT_EXCHANGE_RATE)
        )
    st.session_state[EXCHANGE_RATE_STATE_KEY] = exchange_rate
    return exchange_rate


def ensure_pdf_fonts_registered() -> Tuple[str, str]:
    """Register Unicode-capable fonts for PDF export and return (base, bold)."""

    global _PDF_FONT_STATE
    if _PDF_FONT_STATE is not None:
        return _PDF_FONT_STATE

    try:
        pdfmetrics.registerFont(
            TTFont(PDF_FONT_REGULAR, io.BytesIO(get_noto_sans_regular()))
        )
        pdfmetrics.registerFont(
            TTFont(PDF_FONT_BOLD, io.BytesIO(get_noto_sans_bold()))
        )
        pdfmetrics.registerFontFamily(
            PDF_FONT_REGULAR,
            normal=PDF_FONT_REGULAR,
            bold=PDF_FONT_BOLD,
            italic=PDF_FONT_REGULAR,
            boldItalic=PDF_FONT_BOLD,
        )
        _PDF_FONT_STATE = (PDF_FONT_REGULAR, PDF_FONT_BOLD)
    except Exception:
        _PDF_FONT_STATE = ("Helvetica", "Helvetica-Bold")
    return _PDF_FONT_STATE

def normalize_col(c):
    if not isinstance(c, str):
        c = str(c)
    return re.sub(r"\s+", " ", c.strip().lower())


def supplier_default_alias(name: str, max_length: int = 30) -> str:
    base = Path(name).stem if name else "Dodavatel"
    base = base.strip() or "Dodavatel"
    if len(base) <= max_length:
        return base
    return base[: max_length - 1] + "…"


def sanitize_key(prefix: str, name: str) -> str:
    safe = re.sub(r"[^0-9a-zA-Z_]+", "_", name)
    return f"{prefix}_{safe}" if safe else f"{prefix}_anon"


def make_widget_key(prefix: str, name: str) -> str:
    """Return a Streamlit widget key safe for arbitrary sheet names."""

    base = sanitize_key(prefix, name)
    digest = hashlib.md5(str(name).encode("utf-8")).hexdigest()[:8]
    return f"{base}_{digest}"


def normalize_text(value: Any) -> str:
    """Return lower-case text without diacritics for fuzzy comparisons."""

    if value is None:
        return ""
    text = str(value).strip()
    if not text:
        return ""
    normalized = unicodedata.normalize("NFKD", text)
    without_diacritics = "".join(ch for ch in normalized if not unicodedata.combining(ch))
    return without_diacritics.lower()


def normalize_identifier(values: Any) -> pd.Series:
    """Return normalized textual identifiers for row-level matching."""

    series = values if isinstance(values, pd.Series) else pd.Series(values)
    if series.empty:
        return series.astype(str)

    def _normalize(value: Any) -> str:
        if pd.isna(value):
            return ""
        if isinstance(value, (int, np.integer)):
            return str(int(value))
        if isinstance(value, (float, np.floating)):
            float_val = float(value)
            if math.isfinite(float_val) and float_val.is_integer():
                return str(int(float_val))
            return str(float_val).strip()
        text = str(value).strip()
        if not text:
            return ""
        lowered = text.lower()
        if lowered in {"nan", "none", "null"}:
            return ""
        if re.fullmatch(r"-?\d+\.0+", text):
            try:
                return str(int(float(text)))
            except ValueError:
                pass
        return text

    normalized = series.map(_normalize)
    return normalized.astype(str)


def extract_code_token(value: Any) -> str:
    """Return a canonical code token for grouping (e.g. "7.7", "VE")."""

    if value is None:
        return ""
    text = str(value).strip()
    if not text:
        return ""
    text = text.replace(",", ".")
    text = re.sub(r"\s+", " ", text)
    match = re.match(r"([A-Za-z0-9]+(?:[.\-][A-Za-z0-9]+)*)", text)
    token = match.group(1) if match else text.split()[0]
    token = token.replace("-", ".").strip(".")
    return token.upper()


def resolve_section_label(code: Any, description: Any) -> Tuple[str, str]:
    """Return canonical section token and display label for a row."""

    token = str(infer_section_group(code, description) or "").strip()
    if not token:
        token = extract_code_token(code) or extract_code_token(description)
    token = (token or "").upper()
    label = SECTION_ONTOLOGY.get(token, "")
    if not label:
        label = token if token else "Nezařazeno"
    return token, label


def build_comparison_dataset(sheet: str, df: pd.DataFrame) -> ComparisonDataset:
    if df is None or df.empty:
        empty = pd.DataFrame()
        return ComparisonDataset(
            sheet=sheet,
            analysis_df=pd.DataFrame(),
            value_columns=[],
            percent_columns=[],
            diff_columns=[],
            suppliers=[],
            supplier_order=[],
            section_labels=[],
            master_column=None,
            long_df=empty,
        )

    analysis_df = df.copy()
    if "__key__" not in analysis_df.columns:
        analysis_df["__key__"] = np.arange(len(analysis_df))

    section_tokens: List[str] = []
    section_labels: List[str] = []
    for _, row in analysis_df.iterrows():
        token, label = resolve_section_label(row.get("code"), row.get("description"))
        section_tokens.append(token)
        section_labels.append(label or "Nezařazeno")
    analysis_df["__section_token__"] = section_tokens
    analysis_df["Oddíl"] = section_labels

    search_columns = [col for col in ("code", "description", "Oddíl") if col in analysis_df.columns]
    if search_columns:
        search_concat = (
            analysis_df[search_columns]
            .fillna("")
            .astype(str)
            .agg(" ".join, axis=1)
            .str.strip()
        )
    else:
        search_concat = pd.Series("", index=analysis_df.index)
    analysis_df["__search_text__"] = search_concat
    analysis_df["__search_token__"] = analysis_df["__search_text__"].map(normalize_text)

    value_columns = [
        col
        for col in analysis_df.columns
        if str(col).endswith(" total") and not str(col).startswith("__present__")
    ]
    master_column = next((col for col in value_columns if is_master_column(col)), None)

    supplier_columns = [col for col in value_columns if col != master_column]
    suppliers = [col.replace(" total", "") for col in supplier_columns]
    supplier_order: List[str] = []
    if master_column:
        supplier_order.append("Master")
    supplier_order.extend(suppliers)

    if master_column:
        master_series = pd.to_numeric(analysis_df[master_column], errors="coerce")
    else:
        master_series = pd.Series(np.nan, index=analysis_df.index, dtype=float)

    supplier_series: Dict[str, pd.Series] = {}
    for col, supplier in zip(supplier_columns, suppliers):
        supplier_series[supplier] = pd.to_numeric(analysis_df[col], errors="coerce")

    diff_data: Dict[str, pd.Series] = {}
    pct_data: Dict[str, pd.Series] = {}
    percent_columns: List[str] = []
    diff_columns: List[str] = []
    for supplier, series in supplier_series.items():
        if master_column:
            diff_series = series - master_series
            pct_series = compute_percent_difference(series, master_series)
        else:
            diff_series = pd.Series(np.nan, index=analysis_df.index, dtype=float)
            pct_series = pd.Series(np.nan, index=analysis_df.index, dtype=float)
        diff_data[supplier] = diff_series
        pct_data[supplier] = pct_series
        pct_col = f"__pct__::{supplier}"
        diff_col = f"__diff__::{supplier}"
        analysis_df[pct_col] = pct_series
        analysis_df[diff_col] = diff_series
        percent_columns.append(pct_col)
        diff_columns.append(diff_col)

    if diff_data:
        diff_df = pd.DataFrame(diff_data)
        analysis_df["__abs_diff_max__"] = diff_df.abs().max(axis=1)
        analysis_df["__abs_diff_sum__"] = diff_df.abs().sum(axis=1)
    else:
        analysis_df["__abs_diff_max__"] = 0.0
        analysis_df["__abs_diff_sum__"] = 0.0

    if pct_data:
        pct_df = pd.DataFrame(pct_data)
        analysis_df["__pct_max__"] = pct_df.max(axis=1)
        analysis_df["__pct_min__"] = pct_df.min(axis=1)
    else:
        analysis_df["__pct_max__"] = np.nan
        analysis_df["__pct_min__"] = np.nan

    if supplier_series:
        supplier_matrix = pd.DataFrame(supplier_series)
        analysis_df["__missing_any__"] = supplier_matrix.isna().any(axis=1)
        analysis_df["__missing_all__"] = supplier_matrix.isna().all(axis=1)
    else:
        analysis_df["__missing_any__"] = False
        analysis_df["__missing_all__"] = False

    has_master_value = master_series.notna() & master_series.ne(0)
    if "__missing_all__" in analysis_df.columns:
        analysis_df["__missing_offer__"] = analysis_df["__missing_all__"] & has_master_value
    else:
        analysis_df["__missing_offer__"] = has_master_value

    section_labels_unique = sorted(set(analysis_df["Oddíl"].dropna().tolist()), key=natural_sort_key)

    long_records: List[Dict[str, Any]] = []
    for idx in analysis_df.index:
        row = analysis_df.loc[idx]
        key_value = row.get("__key__", idx)
        code_value = row.get("code", "")
        desc_value = row.get("description", "")
        unit_value_master = row.get("unit", "")
        if pd.isna(unit_value_master):
            unit_value_master = ""
        if isinstance(unit_value_master, str):
            unit_value_master = unit_value_master.strip()
        qty_value_master = row.get("quantity", np.nan)
        section_value = row.get("Oddíl", "Nezařazeno")

        if master_column:
            master_value = master_series.loc[idx]
            long_records.append(
                {
                    "__key__": key_value,
                    "sheet": sheet,
                    "supplier": "Master",
                    "total": master_value,
                    "difference_vs_master": 0.0,
                    "pct_vs_master": 0.0,
                    "code": code_value,
                    "description": desc_value,
                    "unit": unit_value_master,
                    "quantity": qty_value_master,
                    "section": section_value,
                }
            )
        for supplier, series in supplier_series.items():
            total_value = series.loc[idx]
            diff_series = diff_data.get(supplier)
            pct_series = pct_data.get(supplier)
            diff_value = diff_series.loc[idx] if diff_series is not None else np.nan
            pct_value = pct_series.loc[idx] if pct_series is not None else np.nan
            supplier_unit = row.get(f"{supplier} unit", np.nan)
            if pd.isna(supplier_unit):
                supplier_unit = ""
            if isinstance(supplier_unit, str):
                supplier_unit = supplier_unit.strip()
            if not supplier_unit:
                supplier_unit = unit_value_master
            supplier_qty = row.get(f"{supplier} quantity", np.nan)
            long_records.append(
                {
                    "__key__": key_value,
                    "sheet": sheet,
                    "supplier": supplier,
                    "total": total_value,
                    "difference_vs_master": diff_value,
                    "pct_vs_master": pct_value,
                    "code": code_value,
                    "description": desc_value,
                    "unit": supplier_unit,
                    "quantity": supplier_qty,
                    "section": section_value,
                }
            )

    long_df = pd.DataFrame(long_records)

    return ComparisonDataset(
        sheet=sheet,
        analysis_df=analysis_df,
        value_columns=value_columns,
        percent_columns=percent_columns,
        diff_columns=diff_columns,
        suppliers=suppliers,
        supplier_order=supplier_order,
        section_labels=section_labels_unique,
        master_column=master_column,
        long_df=long_df,
    )


def build_comparison_datasets(results: Dict[str, pd.DataFrame]) -> Dict[str, ComparisonDataset]:
    datasets: Dict[str, ComparisonDataset] = {}
    for sheet, df in results.items():
        datasets[sheet] = build_comparison_dataset(sheet, df)
    return datasets


def natural_sort_key(value: str) -> Tuple[Any, ...]:
    """Return a tuple usable for natural sorting of alphanumeric codes."""

    if value is None:
        return ("",)
    text = str(value)
    parts = re.split(r"(\d+)", text)
    key: List[Any] = []
    for part in parts:
        if part.isdigit():
            key.append(int(part))
        else:
            key.append(part.lower())
    return tuple(key)


def compute_percent_difference(values: pd.Series, reference: Any) -> pd.Series:
    """Return percentage difference of ``values`` relative to ``reference``.

    ``reference`` can be either a scalar or a Series aligned with ``values``.
    When the reference is zero, the function returns ``0`` if the compared
    value is also zero and ``NaN`` otherwise to avoid division errors.
    """

    if values is None:
        return pd.Series(dtype=float)

    numeric_values = pd.to_numeric(values, errors="coerce")
    if isinstance(reference, pd.Series):
        aligned_reference = pd.to_numeric(
            reference.reindex(numeric_values.index), errors="coerce"
        )
    else:
        aligned_reference = pd.Series(reference, index=numeric_values.index, dtype=float)

    if aligned_reference.empty:
        return pd.Series(np.nan, index=numeric_values.index, dtype=float)

    result = pd.Series(np.nan, index=numeric_values.index, dtype=float)
    valid_mask = aligned_reference.notna()
    nonzero_mask = valid_mask & (aligned_reference != 0)
    if nonzero_mask.any():
        result.loc[nonzero_mask] = (
            (numeric_values.loc[nonzero_mask] - aligned_reference.loc[nonzero_mask])
            / aligned_reference.loc[nonzero_mask]
        ) * 100.0

    zero_mask = valid_mask & (aligned_reference == 0)
    if zero_mask.any():
        zero_values = numeric_values.loc[zero_mask]
        result.loc[zero_mask & zero_values.fillna(np.nan).eq(0)] = 0.0

    return result


def add_percent_difference_columns(
    df: pd.DataFrame, reference_column: str = "Master total"
) -> pd.DataFrame:
    """Return a copy with percent differences adjacent to value columns."""

    if not isinstance(df, pd.DataFrame) or df.empty:
        return df

    working = df.copy()
    if reference_column not in working.columns:
        return working

    reference_series = pd.to_numeric(working[reference_column], errors="coerce")
    value_columns = [
        col
        for col in list(working.columns)
        if col.endswith(" total")
        and not col.startswith("__present__")
        and col != reference_column
    ]

    for col in value_columns:
        pct_col = f"{col}{PERCENT_DIFF_SUFFIX}"
        if pct_col in working.columns:
            continue
        pct_values = compute_percent_difference(working[col], reference_series)
        insert_at = working.columns.get_loc(col) + 1
        working.insert(insert_at, pct_col, pct_values)

    return working


def rename_value_columns_for_display(df: pd.DataFrame, suffix: str) -> pd.DataFrame:
    """Rename numeric value columns for display, including percent deltas."""

    if not isinstance(df, pd.DataFrame):
        return df

    prepared = add_percent_difference_columns(df)
    raw_comparison_meta: Dict[str, Dict[str, Any]] = {}
    if "Master total" in prepared.columns:
        reference_series = pd.to_numeric(prepared["Master total"], errors="coerce")
        value_columns = [
            col
            for col in prepared.columns
            if col.endswith(" total")
            and not col.startswith("__present__")
            and col != "Master total"
        ]
        for col in value_columns:
            pct_col = f"{col}{PERCENT_DIFF_SUFFIX}"
            pct_series = compute_percent_difference(prepared[col], reference_series)
            raw_comparison_meta[col] = {
                "pct_values": pct_series,
                "pct_column": pct_col if pct_col in prepared.columns else None,
            }

    rename_map: Dict[str, str] = {}
    for col in prepared.columns:
        if col.endswith(" total") and not col.startswith("__present__"):
            rename_map[col] = f"{col.replace(' total', '')}{suffix}"
        elif col.endswith(PERCENT_DIFF_SUFFIX):
            base = col[: -len(PERCENT_DIFF_SUFFIX)]
            label = base.replace(" total", "")
            if suffix:
                label = f"{label}{suffix}"
            rename_map[col] = f"{label}{PERCENT_DIFF_LABEL}"

    result = prepared.rename(columns=rename_map)

    if raw_comparison_meta:
        comparison_display: Dict[str, Dict[str, Any]] = {}
        for raw_col, meta in raw_comparison_meta.items():
            display_col = rename_map.get(raw_col, raw_col)
            pct_col_raw = meta.get("pct_column")
            pct_col_display = rename_map.get(pct_col_raw, pct_col_raw) if pct_col_raw else None
            pct_values = meta.get("pct_values")
            if isinstance(pct_values, pd.Series):
                comparison_display[display_col] = {
                    "pct_values": pct_values,
                    "pct_column": pct_col_display,
                }
        master_display = rename_map.get("Master total", "Master total")
        result.attrs["comparison_master"] = master_display
        result.attrs["comparison_info"] = comparison_display

    return result


def compute_display_column_widths(
    df: pd.DataFrame, min_width: int = 90, max_width: int = 420
) -> Dict[str, int]:
    """Return pixel widths for columns based on the longest textual value."""

    widths: Dict[str, int] = {}
    if not isinstance(df, pd.DataFrame) or df.empty:
        return widths

    for col in df.columns:
        series = df[col]
        try:
            as_text = series.astype(str).replace("nan", "")
        except Exception:
            as_text = series
        if hasattr(as_text, "map"):
            max_length = as_text.map(lambda x: len(str(x))).max()
        else:
            max_length = len(str(as_text))
        header_length = len(str(col))
        effective_len = max(int(max_length or 0), header_length)
        width_px = max(min_width, min(max_width, (effective_len + 1) * 9))
        widths[col] = int(width_px)
    return widths


def ensure_unique_aliases(
    raw_to_alias: Dict[str, str], reserved: Optional[Iterable[str]] = None
) -> Dict[str, str]:
    """Return a mapping with aliases made unique via numeric suffixes.

    Streamlit tables require unique column labels. When two suppliers share the
    same alias (or when an alias collides with a reserved name such as
    "Master"), the comparison columns would otherwise duplicate. We keep the
    first occurrence intact and append ``" (n)"`` to subsequent duplicates
    while preserving the semantic suffixes (e.g. ``" total"``) added later in
    the pipeline.
    """

    reserved_casefold = {str(name).casefold() for name in (reserved or []) if name}
    used: set[str] = set(reserved_casefold)
    unique: Dict[str, str] = {}

    for raw, alias in raw_to_alias.items():
        alias_str = str(alias).strip() if alias is not None else ""
        base_alias = alias_str or supplier_default_alias(raw)
        candidate = base_alias
        suffix = 2
        while candidate.casefold() in used:
            candidate = f"{base_alias} ({suffix})"
            suffix += 1
        used.add(candidate.casefold())
        unique[raw] = candidate

    return unique


class OfferStorage:
    """Persist uploaded workbooks on disk for reuse between sessions."""

    def __init__(self, base_dir: Optional[Path] = None) -> None:
        self.base_dir = Path(base_dir) if base_dir else DEFAULT_STORAGE_DIR
        self.index_file = self.base_dir / "index.json"
        self.base_dir.mkdir(parents=True, exist_ok=True)
        self._index = self._load_index()
        self._cleanup_missing()

    def _load_index(self) -> Dict[str, Dict[str, Any]]:
        if not self.index_file.exists():
            return {"master": {}, "bids": {}}
        try:
            with self.index_file.open("r", encoding="utf-8") as handle:
                data = json.load(handle)
        except (OSError, json.JSONDecodeError):
            return {"master": {}, "bids": {}}
        if not isinstance(data, dict):
            return {"master": {}, "bids": {}}
        data.setdefault("master", {})
        data.setdefault("bids", {})
        return data  # type: ignore[return-value]

    def _write_index(self) -> None:
        try:
            with self.index_file.open("w", encoding="utf-8") as handle:
                json.dump(self._index, handle, ensure_ascii=False, indent=2)
        except OSError:
            # Best-effort persistence; ignore filesystem issues.
            pass

    def _category_dir(self, category: str) -> Path:
        path = self.base_dir / category
        path.mkdir(parents=True, exist_ok=True)
        return path

    def _path_for(self, category: str, display_name: str) -> Path:
        digest = hashlib.sha1(display_name.encode("utf-8")).hexdigest()
        suffix = Path(display_name).suffix or ".bin"
        return self._category_dir(category) / f"{digest}{suffix}"

    def _write_file(self, category: str, display_name: str, file_obj: Any) -> Path:
        entries = self._index.setdefault(category, {})
        existing = entries.get(display_name)
        dest = self._path_for(category, display_name)
        if existing:
            old_path = self._category_dir(category) / existing.get("path", "")
            if old_path.exists() and old_path != dest:
                try:
                    old_path.unlink()
                except OSError:
                    pass
        if hasattr(file_obj, "seek"):
            try:
                file_obj.seek(0)
            except (OSError, AttributeError):
                pass
        data: bytes
        if hasattr(file_obj, "read"):
            raw = file_obj.read()
            if isinstance(raw, str):
                data = raw.encode("utf-8")
            else:
                data = bytes(raw)
        elif hasattr(file_obj, "getbuffer"):
            data = bytes(file_obj.getbuffer())
        else:
            data = bytes(file_obj)
        dest.write_bytes(data)
        if hasattr(file_obj, "seek"):
            try:
                file_obj.seek(0)
            except (OSError, AttributeError):
                pass
        entries[display_name] = {"path": dest.name, "updated_at": time.time()}
        self._write_index()
        return dest

    def _load_file(self, category: str, display_name: str) -> io.BytesIO:
        entries = self._index.get(category, {})
        meta = entries.get(display_name)
        if not meta:
            raise FileNotFoundError(display_name)
        path = self._category_dir(category) / meta.get("path", "")
        if not path.exists():
            raise FileNotFoundError(display_name)
        buffer = io.BytesIO(path.read_bytes())
        buffer.name = display_name  # type: ignore[attr-defined]
        buffer.seek(0)
        return buffer

    def _delete_file(self, category: str, display_name: str) -> bool:
        entries = self._index.get(category, {})
        meta = entries.pop(display_name, None)
        if not meta:
            return False
        path = self._category_dir(category) / meta.get("path", "")
        try:
            if path.exists():
                path.unlink()
        except OSError:
            pass
        self._write_index()
        return True

    def _cleanup_missing(self) -> None:
        changed = False
        for category in ("master", "bids"):
            entries = self._index.get(category, {})
            for name, meta in list(entries.items()):
                path = self._category_dir(category) / meta.get("path", "")
                if not path.exists():
                    entries.pop(name, None)
                    changed = True
        if changed:
            self._write_index()

    def save_master(self, file_obj: Any, *, display_name: Optional[str] = None) -> str:
        name = display_name or getattr(file_obj, "name", "Master.xlsx")
        self._write_file("master", name, file_obj)
        return name

    def save_bid(self, file_obj: Any, *, display_name: Optional[str] = None) -> str:
        name = display_name or getattr(file_obj, "name", "Bid.xlsx")
        self._write_file("bids", name, file_obj)
        return name

    def load_master(self, display_name: str) -> io.BytesIO:
        return self._load_file("master", display_name)

    def load_bid(self, display_name: str) -> io.BytesIO:
        return self._load_file("bids", display_name)

    def delete_master(self, display_name: str) -> bool:
        return self._delete_file("master", display_name)

    def delete_bid(self, display_name: str) -> bool:
        return self._delete_file("bids", display_name)

    def list_entries(self, category: str) -> List[Dict[str, Any]]:
        entries = self._index.get(category, {})
        results: List[Dict[str, Any]] = []
        for name, meta in entries.items():
            path = self._category_dir(category) / meta.get("path", "")
            results.append(
                {
                    "name": name,
                    "path": path,
                    "updated_at": meta.get("updated_at"),
                }
            )
        results.sort(key=lambda item: item["name"].casefold())
        return results

    def list_master(self) -> List[Dict[str, Any]]:
        return self.list_entries("master")

    def list_bids(self) -> List[Dict[str, Any]]:
        return self.list_entries("bids")


def format_timestamp(timestamp: Optional[float]) -> str:
    if not timestamp:
        return ""
    try:
        dt = datetime.fromtimestamp(float(timestamp))
    except (TypeError, ValueError, OSError, OverflowError):
        return ""
    return dt.strftime("%d.%m.%Y %H:%M")


def format_percent_label(value: Any) -> str:
    if pd.isna(value):
        return "–"
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return str(value)
    text = f"{numeric:+.2f} %"
    return text.replace(".", ",")


def format_currency_label(value: Any, currency: str) -> str:
    if pd.isna(value):
        return "–"
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return str(value)
    text = f"{numeric:,.2f}".replace(",", "\u00A0").replace(".", ",")
    return f"{text} {currency}".strip()


def build_recap_chart_data(
    value_cols: List[str],
    net_series: pd.Series,
    currency_label: str = "CZK",
) -> pd.DataFrame:
    if not value_cols:
        return pd.DataFrame(
            columns=[
                "Dodavatel",
                "Cena po odečtech",
                "Popisek",
            ]
        )
    if not isinstance(net_series, pd.Series):
        net_series = pd.Series(net_series)
    chart_df = pd.DataFrame(
        {
            "Dodavatel": [col.replace(" total", "") for col in value_cols],
            "Cena po odečtech": [net_series.get(col) for col in value_cols],
        }
    )
    chart_df["Cena po odečtech"] = pd.to_numeric(
        chart_df["Cena po odečtech"], errors="coerce"
    )
    master_mask = chart_df["Dodavatel"].astype(str).str.casefold() == "master"
    master_val: Optional[float] = None
    if master_mask.any():
        master_values = chart_df.loc[master_mask, "Cena po odečtech"].dropna()
        if not master_values.empty:
            master_val = float(master_values.iloc[0])
    deltas: List[float] = []
    for supplier, value in zip(chart_df["Dodavatel"], chart_df["Cena po odečtech"]):
        supplier_cf = str(supplier).casefold()
        if supplier_cf == "master":
            deltas.append(0.0 if pd.notna(value) else np.nan)
            continue
        if master_val is None or pd.isna(value) or math.isclose(
            master_val, 0.0, rel_tol=1e-9, abs_tol=1e-9
        ):
            deltas.append(np.nan)
            continue
        deltas.append(((float(value) - master_val) / master_val) * 100.0)
    chart_df["Odchylka vs Master (%)"] = deltas
    chart_df["Popisek"] = chart_df["Odchylka vs Master (%)"].apply(format_percent_label)
    chart_df["Cena (text)"] = [
        format_currency_label(value, currency_label)
        for value in chart_df["Cena po odečtech"]
    ]
    return chart_df


def format_table_value(value: Any) -> str:
    if pd.isna(value):
        return "–"
    if isinstance(value, (np.integer, int)) and not isinstance(value, bool):
        return f"{int(value):,}".replace(",", "\u00A0")
    if isinstance(value, (np.floating, float)):
        return f"{float(value):,.2f}".replace(",", "\u00A0").replace(".", ",")
    return str(value)


def dataframe_to_table_data(df: pd.DataFrame) -> List[List[str]]:
    if df is None or df.empty:
        return []
    headers = [str(col) for col in df.columns]
    data: List[List[str]] = [headers]
    for _, row in df.iterrows():
        data.append([format_table_value(row[col]) for col in df.columns])
    return data


def generate_recap_pdf(
    title: str,
    base_currency: str,
    target_currency: str,
    main_detail_base: pd.DataFrame,
    main_detail_converted: pd.DataFrame,
    summary_base: pd.DataFrame,
    summary_converted: pd.DataFrame,
    chart_df: pd.DataFrame,
    chart_figure: Optional[Any] = None,
) -> bytes:
    buffer = io.BytesIO()
    base_font, bold_font = ensure_pdf_fonts_registered()
    doc = SimpleDocTemplate(
        buffer,
        pagesize=landscape(A4),
        leftMargin=15 * mm,
        rightMargin=15 * mm,
        topMargin=15 * mm,
        bottomMargin=15 * mm,
    )
    styles = getSampleStyleSheet()
    for style_name in ("Normal", "BodyText", "Title", "Heading1", "Heading2", "Heading3"):
        if style_name in styles:
            styles[style_name].fontName = bold_font if "Heading" in style_name or style_name == "Title" else base_font
    styles["Title"].fontName = bold_font
    styles["Heading2"].fontName = bold_font
    styles["Heading1"].fontName = bold_font
    styles["Heading3"].fontName = bold_font
    story: List[Any] = [Paragraph(title, styles["Title"]), Spacer(1, 6)]

    table_header_style = ParagraphStyle(
        "RecapTableHeader",
        parent=styles.get("Heading4", styles["Heading2"]),
        fontName=bold_font,
        fontSize=8,
        leading=10,
        alignment=TA_CENTER,
    )
    table_cell_style = ParagraphStyle(
        "RecapTableCell",
        parent=styles["BodyText"],
        fontName=base_font,
        fontSize=8,
        leading=10,
        alignment=TA_LEFT,
    )

    table_style = TableStyle(
        [
            ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#f0f0f0")),
            ("TEXTCOLOR", (0, 0), (-1, 0), colors.black),
            ("ALIGN", (0, 0), (-1, 0), "CENTER"),
            ("ALIGN", (0, 1), (-1, -1), "LEFT"),
            ("VALIGN", (0, 0), (-1, -1), "TOP"),
            ("FONTNAME", (0, 0), (-1, 0), bold_font),
            ("FONTNAME", (0, 1), (-1, -1), base_font),
            ("FONTSIZE", (0, 0), (-1, -1), 8),
            ("BOTTOMPADDING", (0, 0), (-1, 0), 6),
            ("BACKGROUND", (0, 1), (-1, -1), colors.white),
            ("GRID", (0, 0), (-1, -1), 0.25, colors.HexColor("#cccccc")),
        ]
    )

    def append_table(title_text: str, df: pd.DataFrame) -> None:
        raw_data = dataframe_to_table_data(df)
        if not raw_data:
            return
        story.append(Paragraph(title_text, styles["Heading2"]))
        story.append(Spacer(1, 4))
        column_count = len(raw_data[0]) if raw_data else 0
        if column_count == 0:
            return
        column_lengths = [1] * column_count
        for row in raw_data:
            for idx, cell in enumerate(row):
                text = str(cell)
                column_lengths[idx] = max(column_lengths[idx], len(text))
        total_length = sum(column_lengths) or column_count
        available_width = doc.width
        scaled_widths = [
            (length / total_length) * available_width for length in column_lengths
        ]
        min_width = 35.0
        col_widths = [max(min_width, width) for width in scaled_widths]
        total_width = sum(col_widths)
        if total_width > available_width and total_width > 0:
            scale = available_width / total_width
            col_widths = [width * scale for width in col_widths]
        formatted_rows: List[List[Any]] = []
        for row_idx, row in enumerate(raw_data):
            formatted_row: List[Any] = []
            for cell in row:
                text = str(cell)
                style = table_header_style if row_idx == 0 else table_cell_style
                formatted_row.append(Paragraph(text, style))
            formatted_rows.append(formatted_row)
        table = Table(formatted_rows, repeatRows=1, colWidths=col_widths)
        table.setStyle(table_style)
        story.append(table)
        story.append(Spacer(1, 8))

    append_table(f"Rekapitulace hlavních položek ({base_currency})", main_detail_base)
    append_table(f"Rekapitulace hlavních položek ({target_currency})", main_detail_converted)
    append_table("Souhrn", summary_base)
    append_table(f"Souhrn ({target_currency})", summary_converted)

    image_rendered = False
    if chart_figure is not None:
        try:
            image_bytes = chart_figure.to_image(format="png", scale=2)
        except Exception:
            image_bytes = None
        if image_bytes:
            story.append(Paragraph("Graf ceny po odečtech", styles["Heading2"]))
            story.append(Spacer(1, 4))
            chart_image = RLImage(io.BytesIO(image_bytes))
            chart_image.drawHeight = 90 * mm
            chart_image.drawWidth = 160 * mm
            story.append(chart_image)
            story.append(Spacer(1, 8))
            image_rendered = True

    if not image_rendered:
        append_table("Hodnoty grafu", chart_df)

    doc.build(story)
    buffer.seek(0)
    return buffer.getvalue()


def generate_tables_pdf(title: str, tables: List[Tuple[str, pd.DataFrame]]) -> bytes:
    """Return a PDF containing the provided tables on subsequent pages."""

    buffer = io.BytesIO()
    base_font, bold_font = ensure_pdf_fonts_registered()
    doc = SimpleDocTemplate(
        buffer,
        pagesize=landscape(A4),
        leftMargin=15 * mm,
        rightMargin=15 * mm,
        topMargin=15 * mm,
        bottomMargin=15 * mm,
    )
    styles = getSampleStyleSheet()
    for style_name in ("Normal", "BodyText", "Title", "Heading1", "Heading2", "Heading3"):
        if style_name in styles:
            styles[style_name].fontName = (
                bold_font if "Heading" in style_name or style_name == "Title" else base_font
            )
    styles["Title"].fontName = bold_font
    styles["Heading2"].fontName = bold_font
    story: List[Any] = [Paragraph(title, styles["Title"]), Spacer(1, 6)]

    table_style = TableStyle(
        [
            ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#f0f0f0")),
            ("TEXTCOLOR", (0, 0), (-1, 0), colors.black),
            ("ALIGN", (0, 0), (-1, 0), "CENTER"),
            ("FONTNAME", (0, 0), (-1, 0), bold_font),
            ("FONTNAME", (0, 1), (-1, -1), base_font),
            ("FONTSIZE", (0, 0), (-1, -1), 8),
            ("BOTTOMPADDING", (0, 0), (-1, 0), 6),
            ("BACKGROUND", (0, 1), (-1, -1), colors.white),
            ("GRID", (0, 0), (-1, -1), 0.25, colors.HexColor("#cccccc")),
        ]
    )

    appended = False
    for table_title, df in tables:
        table_data = dataframe_to_table_data(df)
        if not table_data:
            continue
        story.append(Paragraph(table_title, styles["Heading2"]))
        story.append(Spacer(1, 4))
        table = Table(table_data, repeatRows=1)
        table.setStyle(table_style)
        story.append(table)
        story.append(Spacer(1, 8))
        appended = True

    if not appended:
        story.append(Paragraph("Tabulky nejsou k dispozici.", styles["Normal"]))

    doc.build(story)
    buffer.seek(0)
    return buffer.getvalue()


def _normalize_key_part(value: Any) -> str:
    """Normalize part of a widget key to avoid Streamlit duplicate IDs."""

    raw_text = str(value)
    safe = re.sub(r"[^0-9a-zA-Z_]+", "_", raw_text)
    safe = safe.strip("_")
    if not safe:
        safe = "anon"

    if safe != raw_text:
        digest = hashlib.sha1(raw_text.encode("utf-8")).hexdigest()[:8]
        return f"{safe}_{digest}"
    return safe


def make_widget_key(*parts: Any) -> str:
    """Create a stable widget key from the provided parts."""

    normalized = [_normalize_key_part(p) for p in parts]
    return "_".join(normalized)

@st.cache_data
def try_autodetect_mapping(df: pd.DataFrame) -> Tuple[Dict[str, int], int, pd.DataFrame]:
    """Autodetect header mapping using a sampled, vectorized search."""
    # probe size grows with the dataframe but is capped to keep things fast
    nprobe = min(len(df), 200)
    sample = df.head(nprobe).astype(str).applymap(normalize_col)

    hint_patterns: Dict[str, Dict[str, List[str]]] = {}
    for key, hints in HEADER_HINTS.items():
        exact_terms: List[str] = []
        regex_terms: List[str] = []
        contains_terms: List[str] = []
        for h in hints:
            if not h:
                continue
            if h.startswith("regex:"):
                regex_terms.append(h[len("regex:"):])
            else:
                normalized_hint = normalize_col(h)
                if normalized_hint:
                    exact_terms.append(normalized_hint)
                    escaped = re.escape(normalized_hint)
                    contains_terms.append(rf"(?:^|\\b){escaped}(?:\\b|$)")
        hint_patterns[key] = {
            "exact": exact_terms,
            "regex": regex_terms,
            "contains": contains_terms,
        }

    def detect_row(row: pd.Series) -> Dict[str, int]:
        mapping: Dict[str, int] = {}
        for key, patterns in hint_patterns.items():
            exact_terms = patterns.get("exact", [])
            regex_terms = patterns.get("regex", [])
            contains_terms = patterns.get("contains", [])

            matched_idx: Optional[int] = None
            for term in exact_terms:
                term_mask = row == term
                if term_mask.any():
                    matched_idx = term_mask.idxmax()
                    break
            if matched_idx is not None:
                mapping[key] = matched_idx
                continue

            for pattern in regex_terms:
                regex_mask = row.str.contains(pattern, regex=True, na=False)
                if regex_mask.any():
                    matched_idx = regex_mask.idxmax()
                    break
            if matched_idx is not None:
                mapping[key] = matched_idx
                continue

            for pattern in contains_terms:
                contains_mask = row.str.contains(pattern, regex=True, na=False)
                if contains_mask.any():
                    matched_idx = contains_mask.idxmax()
                    break
            if matched_idx is not None:
                mapping[key] = matched_idx
        return mapping

    mappings = sample.apply(detect_row, axis=1)
    for header_row, mapping in mappings.items():
        if set(REQUIRED_KEYS).issubset(mapping.keys()):
            body = df.iloc[header_row + 1:].reset_index(drop=True)
            body.columns = [normalize_col(x) for x in df.iloc[header_row].tolist()]
            return mapping, header_row, body
    return {}, -1, df

def coerce_numeric(s: pd.Series) -> pd.Series:
    """Coerce textual numbers (with currencies, commas, NBSP) into floats."""

    if not isinstance(s, pd.Series):
        s = pd.Series(s)
    if s.empty:
        return pd.to_numeric(s, errors="coerce")

    cleaned = s.astype(str)
    cleaned = cleaned.str.replace(r"\s+", "", regex=True)
    cleaned = cleaned.str.replace(r"\u00A0", "", regex=True)
    cleaned = cleaned.str.replace(r"(?i)(czk|kč|eur|€|usd|\$|gbp|£)", "", regex=True)
    cleaned = cleaned.str.replace(r"[+-]$", "", regex=True)
    cleaned = cleaned.str.replace(r"[^0-9,\.\-+]", "", regex=True)

    def _normalize_number(value: str) -> str:
        if not value:
            return value
        comma_pos = value.rfind(",")
        dot_pos = value.rfind(".")
        if comma_pos != -1 and dot_pos != -1:
            if dot_pos > comma_pos:
                value = value.replace(",", "")
            else:
                value = value.replace(".", "")
        elif comma_pos != -1:
            if value.count(",") > 1:
                value = value.replace(",", "")
            else:
                digits_after = len(value) - comma_pos - 1
                sign_offset = 1 if value and value[0] in "+-" else 0
                digits_before = comma_pos - sign_offset
                if digits_after == 3 and digits_before <= 3:
                    value = value.replace(",", "")
                else:
                    value = value.replace(",", ".")
        value = value.replace(",", ".")
        while value.endswith(('.', ',')):
            value = value[:-1]
        return value

    normalized = cleaned.apply(_normalize_number)
    return pd.to_numeric(normalized, errors="coerce")


def detect_summary_rows(df: pd.DataFrame) -> pd.Series:
    """Return boolean Series marking summary/subtotal rows.

    In addition to textual and structural patterns, any row with a numeric
    value in ``summary_total`` is treated as summary so that manually curated
    control columns are respected.
    """

    if df is None or df.empty:
        return pd.Series(dtype=bool)

    index = df.index
    desc_str = df.get("description", "").fillna("").astype(str)
    summary_patterns = (
        r"(celkem za odd[ií]l|sou[cč]et za odd[ií]l|celkov[aá] cena za list|sou[cč]et za list|"
        r"sou[cč]et|souhrn|subtotal|total|celkem)"
    )

    # Rows with explicit numeric data in summary_total are treated as summaries
    # only if the value is meaningfully non-zero to avoid flagging regular rows
    # that use ``0`` as a placeholder.
    summary_total_raw = df.get("summary_total")
    if summary_total_raw is None:
        summary_total_mask = pd.Series(False, index=index)
    else:
        summary_total_numeric = coerce_numeric(summary_total_raw)
        has_value = summary_total_raw.notna()
        if summary_total_raw.dtype == object:
            has_value = has_value | summary_total_raw.astype(str).str.strip().ne("")
        non_zero = summary_total_numeric.notna() & summary_total_numeric.abs().gt(1e-9)
        summary_total_mask = has_value & non_zero

    code_blank = df.get("code", "").astype(str).str.strip() == ""
    qty_zero = coerce_numeric(df.get("quantity", 0)).fillna(0) == 0
    unit_price_combined = (
        coerce_numeric(df.get("unit_price_material", 0)).fillna(0)
        + coerce_numeric(df.get("unit_price_install", 0)).fillna(0)
    )
    up_zero = unit_price_combined == 0
    pattern_mask = desc_str.str.contains(summary_patterns, case=False, na=False)
    total_price_numeric = coerce_numeric(df.get("total_price", 0)).fillna(0)
    structural_mask = code_blank & qty_zero & up_zero
    fallback_mask = pattern_mask & (structural_mask | total_price_numeric.eq(0))

    return summary_total_mask | fallback_mask


def is_summary_like_row(df: pd.DataFrame) -> pd.Series:
    """Return boolean mask for rows that should be treated as summaries."""

    if df is None or df.empty:
        return pd.Series(dtype=bool)

    index = df.index
    mask = pd.Series(False, index=index)

    if "is_summary" in df.columns:
        mask = mask | df["is_summary"].fillna(False).astype(bool)

    summary_total_raw = df.get("summary_total")
    if summary_total_raw is not None:
        summary_total_numeric = coerce_numeric(summary_total_raw)
        has_value = summary_total_raw.notna()
        if summary_total_raw.dtype == object:
            has_value = has_value | summary_total_raw.astype(str).str.strip().ne("")
        non_zero = summary_total_numeric.notna() & summary_total_numeric.abs().gt(1e-9)
        mask = mask | (has_value & non_zero)

    desc = df.get("description", pd.Series("", index=index, dtype="object")).fillna("").astype(str)
    pattern_mask = desc.str.contains(
        r"(sou[cč]et|celkem|sum[aá]r|subtotal|total)", case=False, na=False
    )
    code_blank = df.get("code", pd.Series("", index=index, dtype="object")).astype(str).str.strip() == ""
    totals = coerce_numeric(df.get("total_price", np.nan))
    totals_zero = totals.isna() | totals.eq(0)
    mask = mask | (pattern_mask & code_blank & totals_zero)

    return mask

def classify_summary_type(df: pd.DataFrame, summary_mask: pd.Series) -> pd.Series:
    """Categorize summary rows into section, grand, or other totals."""
    desc = df.get("description", "").fillna("").astype(str).str.lower()
    summary_type = pd.Series("", index=df.index, dtype="object")
    section = desc.str.contains(r"(celkem\s*(za)?\s*odd[ií]l|sou[cč]et\s*(za)?\s*odd[ií]l)", na=False)
    grand = desc.str.contains(r"(celkov[aá] cena|sou[cč]et za list|celkem)", na=False) & ~section
    summary_type.loc[summary_mask & section] = "section"
    summary_type.loc[summary_mask & grand] = "grand"
    summary_type.loc[summary_mask & (summary_type == "")] = "other"
    return summary_type

@st.cache_data
def build_normalized_table(df: pd.DataFrame, mapping: Dict[str, int]) -> pd.DataFrame:
    cols = df.columns.tolist()
    def pick(mapped_key, default=None):
        if mapped_key in mapping:
            idx = mapping[mapped_key]
            if 0 <= idx < len(cols):
                return df.iloc[:, idx]
        return pd.Series([default]*len(df))

    out = pd.DataFrame({
        "code": pick("code", ""),
        "description": pick("description", ""),
        "item_id": normalize_identifier(pick("item_id", "")),
        "unit": pick("unit", ""),
        "quantity": coerce_numeric(pick("quantity", 0)).fillna(0.0),
        "quantity_supplier": coerce_numeric(pick("quantity_supplier", np.nan)),
        "unit_price_material": coerce_numeric(pick("unit_price_material", np.nan)),
        "unit_price_install": coerce_numeric(pick("unit_price_install", np.nan)),
        "total_price": coerce_numeric(pick("total_price", np.nan)),
        "summary_total": coerce_numeric(pick("summary_total", np.nan)),
    })

    # Detect summary rows using centralized helper
    summary_mask = detect_summary_rows(out)
    out["is_summary"] = summary_mask
    out["summary_type"] = classify_summary_type(out, summary_mask)

    # Compute total prices and cross-check
    out["unit_price_combined"] = out[["unit_price_material", "unit_price_install"]].sum(
        axis=1, min_count=1
    )
    out["calc_total"] = out["quantity"].fillna(0) * out["unit_price_combined"].fillna(0)
    out["calc_total"] = out["calc_total"].fillna(0)
    out["total_price"] = out["total_price"].fillna(0)
    out["total_diff"] = out["total_price"] - out["calc_total"]
    out.loc[summary_mask, ["unit_price_combined", "calc_total", "total_diff"]] = np.nan

    # Remove duplicate summary rows to avoid double counting
    dup_mask = out["is_summary"] & out.duplicated(
        subset=["summary_type", "description", "total_price"], keep="first"
    )
    out = out[~dup_mask].copy()

    # Preserve summary totals separately and exclude them from item totals
    out.loc[summary_mask & out["summary_total"].isna(), "summary_total"] = out.loc[
        summary_mask & out["summary_total"].isna(), "total_price"
    ]
    out.loc[summary_mask, "total_price"] = np.nan

    # Compute section totals (propagate section summary values upwards)
    section_vals = out["summary_total"].where(out["summary_type"] == "section")
    out["section_total"] = section_vals[::-1].ffill()[::-1]
    out.drop(columns=["unit_price_combined"], inplace=True)

    # Recompute helpers after potential row drops
    desc_str = out["description"].fillna("").astype(str)
    out["description"] = desc_str

    # Filter out rows without description entirely
    out = out[desc_str.str.strip() != ""].copy()
    if "item_id" in out.columns:
        item_ids = normalize_identifier(out["item_id"])
        out["item_id"] = item_ids
        item_mask = item_ids.str.strip() != ""
    else:
        item_mask = pd.Series(False, index=out.index)
    desc_str = out["description"].fillna("").astype(str)
    numeric_cols = out.select_dtypes(include=[np.number]).columns
    summary_col = out["is_summary"].fillna(False).astype(bool)
    out = out[~((out[numeric_cols].isna() | (out[numeric_cols] == 0)).all(axis=1) & ~summary_col)]
    # Canonical key (will be overridden if user picks dedicated Item ID)
    out["__key__"] = (
        out["code"].astype(str).str.strip() + " | " + desc_str.str.strip()
    ).str.strip(" |")
    if "item_id" in out.columns:
        out.loc[item_mask, "__key__"] = out.loc[item_mask, "item_id"]
        if out["item_id"].str.strip().eq("").all():
            out.drop(columns=["item_id"], inplace=True)

    # Preserve explicit ordering from mapping for later aggregations
    out["__row_order__"] = np.arange(len(out))

    # Reorder columns for clarity
    col_order = [
        "code",
        "description",
        "item_id",
        "unit",
        "quantity",
        "quantity_supplier",
        "unit_price_material",
        "unit_price_install",
        "total_price",
        "summary_total",
        "section_total",
        "calc_total",
        "total_diff",
        "is_summary",
        "summary_type",
        "__key__",
        "__row_order__",
    ]
    out = out[[c for c in col_order if c in out.columns]]
    return out


def format_number(x):
    if pd.isna(x):
        return ""
    return f"{x:,.2f}".replace(",", " ").replace(".", ",")


def make_unique_columns(columns: Iterable[Any]) -> List[str]:
    """Generate unique column labels for display purposes."""

    unique_labels: List[str] = []
    used: set[str] = set()
    for col in columns:
        base = str(col) if col is not None else ""
        base = base.strip()
        if not base:
            base = "column"
        candidate = base
        suffix = 2
        while candidate in used:
            candidate = f"{base} ({suffix})"
            suffix += 1
        used.add(candidate)
        unique_labels.append(candidate)
    return unique_labels


def show_df(df: pd.DataFrame) -> None:
    if not isinstance(df, pd.DataFrame):
        st.dataframe(df)
        return

    attrs = getattr(df, "attrs", {}) if hasattr(df, "attrs") else {}
    comparison_info = attrs.get("comparison_info", {})
    comparison_master = attrs.get("comparison_master")

    df_to_show = df.copy()

    helper_cols = [col for col in df_to_show.columns if str(col).startswith("__present__")]
    presence_map: Dict[str, pd.Series] = {}
    for col in helper_cols:
        target_col = str(col)[len("__present__") :]
        presence_map[target_col] = df_to_show[col].astype(bool)
    df_to_show.drop(columns=helper_cols, inplace=True, errors="ignore")
    if "__row_status__" in df_to_show.columns:
        df_to_show.drop(columns=["__row_status__"], inplace=True)

    original_cols = list(df_to_show.columns)
    unique_cols = make_unique_columns(original_cols)
    rename_map = {orig: unique for orig, unique in zip(original_cols, unique_cols)}
    df_to_show.rename(columns=rename_map, inplace=True)

    presence_display: Dict[str, pd.Series] = {}
    for orig_col, series in presence_map.items():
        display_col = rename_map.get(orig_col, orig_col)
        presence_display[display_col] = series.reindex(df_to_show.index).fillna(False)

    numeric_cols = df_to_show.select_dtypes(include=[np.number]).columns
    column_widths = compute_display_column_widths(df_to_show)
    column_config = {
        col: st.column_config.Column(width=width)
        for col, width in column_widths.items()
    }
    display_kwargs = {"use_container_width": True}
    if column_config:
        display_kwargs["column_config"] = column_config

    def _apply_presence_styles(data: pd.DataFrame, presence_info: Dict[str, pd.Series]) -> pd.DataFrame:
        styles = pd.DataFrame("", index=data.index, columns=data.columns)
        master_col = None
        for col in presence_info.keys():
            if col not in styles.columns:
                continue
            if col.lower() == "master total" or col.endswith("Master total"):
                master_col = col
                break
        if master_col is None and "Master total" in presence_info:
            master_col = "Master total"
        master_presence = presence_info.get(master_col, pd.Series(False, index=data.index))
        master_presence = master_presence.reindex(data.index).fillna(False)
        supplier_cols = [col for col in presence_info.keys() if col != master_col]
        any_supplier_present = pd.Series(False, index=data.index, dtype=bool)

        for col in supplier_cols:
            if col not in styles.columns:
                continue
            col_presence = presence_info[col].reindex(data.index).fillna(False)
            any_supplier_present |= col_presence
            added_mask = col_presence & ~master_presence
            removed_mask = master_presence & ~col_presence
            styles.loc[added_mask, col] = "background-color: #d4edda"
            styles.loc[removed_mask, col] = "background-color: #f8d7da"

        if master_col in data.columns and master_col in styles.columns:
            supplier_any = any_supplier_present.reindex(data.index).fillna(False)
            if supplier_cols:
                all_supplier_present = pd.Series(True, index=data.index, dtype=bool)
                for col in supplier_cols:
                    if col not in data.columns:
                        continue
                    col_presence = presence_info[col].reindex(data.index).fillna(False)
                    all_supplier_present &= col_presence
            else:
                all_supplier_present = pd.Series(False, index=data.index, dtype=bool)
            master_added_mask = master_presence & ~supplier_any
            master_removed_mask = ~master_presence & supplier_any
            styles.loc[master_added_mask, master_col] = "background-color: #d4edda"
            styles.loc[master_removed_mask, master_col] = "background-color: #f8d7da"
        return styles

    def _blend_color(base: Tuple[int, int, int], intensity: float) -> str:
        r = int(round(255 + (base[0] - 255) * intensity))
        g = int(round(255 + (base[1] - 255) * intensity))
        b = int(round(255 + (base[2] - 255) * intensity))
        r = max(0, min(255, r))
        g = max(0, min(255, g))
        b = max(0, min(255, b))
        return f"#{r:02x}{g:02x}{b:02x}"

    def _color_for_percent(value: Any) -> str:
        if pd.isna(value):
            return ""
        try:
            pct = float(value)
        except (TypeError, ValueError):
            return ""
        if pct == 0:
            return ""
        capped = max(-200.0, min(200.0, pct))
        intensity = min(abs(capped) / 100.0, 1.0)
        if intensity <= 0:
            return ""
        base = (220, 53, 69) if pct > 0 else (40, 167, 69)
        return f"background-color: {_blend_color(base, intensity)}"

    def _apply_price_delta_styles(
        data: pd.DataFrame,
        info: Dict[str, Dict[str, Any]],
        master_col: Optional[str],
    ) -> pd.DataFrame:
        if not info:
            return pd.DataFrame("", index=data.index, columns=data.columns)

        styles = pd.DataFrame("", index=data.index, columns=data.columns)
        for display_col, meta in info.items():
            if display_col not in styles.columns:
                continue
            pct_series = meta.get("pct_values")
            if isinstance(pct_series, pd.Series):
                working_pct = pct_series.reindex(data.index)
            else:
                working_pct = pd.Series(np.nan, index=data.index)
            styles.loc[data.index, display_col] = working_pct.apply(_color_for_percent).values

            pct_col = meta.get("pct_column")
            if pct_col and pct_col in styles.columns:
                styles.loc[data.index, pct_col] = working_pct.apply(_color_for_percent).values

        if master_col and master_col in styles.columns:
            styles.loc[:, master_col] = ""

        return styles

    needs_styler = bool(len(numeric_cols)) or bool(presence_display)
    if not needs_styler:
        st.dataframe(df_to_show, **display_kwargs)
        return

    styler = df_to_show.style
    if len(numeric_cols):
        styler = styler.format({col: format_number for col in numeric_cols})
    if presence_display:
        styler = styler.apply(
            lambda data: _apply_presence_styles(data, presence_display), axis=None
        )
    if comparison_info:
        styler = styler.apply(
            lambda data: _apply_price_delta_styles(data, comparison_info, comparison_master),
            axis=None,
        )
    header_styles: List[Dict[str, str]] = []
    for idx, col in enumerate(df_to_show.columns):
        width = column_widths.get(col)
        if not width:
            continue
        styler = styler.set_properties(
            subset=pd.IndexSlice[:, col],
            **{"min-width": f"{width}px", "max-width": f"{width}px"},
        )
        header_styles.append(
            {
                "selector": f"th.col_heading.level0.col{idx}",
                "props": f"min-width: {width}px; max-width: {width}px;",
            }
        )
    if header_styles:
        styler = styler.set_table_styles(header_styles, overwrite=False)
    st.dataframe(styler, **display_kwargs)

@st.cache_data
def read_workbook(upload, limit_sheets: Optional[List[str]] = None) -> WorkbookData:
    xl = pd.ExcelFile(upload)
    sheet_names = xl.sheet_names if limit_sheets is None else [s for s in xl.sheet_names if s in limit_sheets]
    wb = WorkbookData(name=getattr(upload, "name", "workbook"))
    for s in sheet_names:
        try:
            raw = xl.parse(s, header=None)
            mapping, header_row, body = try_autodetect_mapping(raw)
            if not mapping:
                # fallback try: header=0
                fallback = xl.parse(s)
                composed = pd.concat([fallback.columns.to_frame().T, fallback], ignore_index=True)
                mapping, header_row, body = try_autodetect_mapping(composed)
                if not mapping:
                    body = fallback.copy()
            tbl = build_normalized_table(body, mapping) if mapping else pd.DataFrame()
            wb.sheets[s] = {"raw": raw, "mapping": mapping, "header_row": header_row, "table": tbl, "header_names": list(body.columns) if hasattr(body, "columns") else []}
        except Exception as e:
            wb.sheets[s] = {"raw": None, "mapping": {}, "header_row": -1, "table": pd.DataFrame(), "error": str(e), "header_names": []}
    return wb

def apply_master_mapping(master: WorkbookData, target: WorkbookData) -> None:
    """Copy mapping and align it with target workbook headers by column name."""

    def _normalize_header_row(
        raw_df: pd.DataFrame,
        desired_names: List[str],
        fallback_row: int,
        master_header_row: int,
    ) -> Tuple[int, List[str]]:
        if not isinstance(raw_df, pd.DataFrame) or raw_df.empty:
            return -1, []

        max_probe = min(len(raw_df), 250)
        probe_indices: List[int] = list(range(max_probe))
        probe_set = set(probe_indices)

        for candidate in (fallback_row, master_header_row):
            if not isinstance(candidate, (int, np.integer)):
                continue
            idx = int(candidate)
            if 0 <= idx < len(raw_df) and idx not in probe_set:
                probe_indices.append(idx)
                probe_set.add(idx)

        normalized_rows: Dict[int, List[str]] = {}
        best_row = -1
        best_score = -1
        desired = [name for name in desired_names if name]

        for idx in probe_indices:
            row_values = [normalize_col(x) for x in raw_df.iloc[idx].astype(str).tolist()]
            normalized_rows[idx] = row_values
            if desired:
                score = sum(1 for name in desired if name in row_values)
            else:
                score = 0
            if score > best_score:
                best_row = idx
                best_score = score

        if best_row < 0 or best_score <= 0:
            fallback_candidates = [fallback_row, master_header_row, 0]
            for candidate in fallback_candidates:
                if isinstance(candidate, (int, np.integer)) and 0 <= int(candidate) < len(raw_df):
                    best_row = int(candidate)
                    break
            else:
                best_row = 0

        header = normalized_rows.get(best_row)
        if header is None:
            header = [normalize_col(x) for x in raw_df.iloc[best_row].astype(str).tolist()]
        return best_row, header

    for sheet, mobj in master.sheets.items():
        if sheet not in target.sheets:
            continue

        target_sheet = target.sheets[sheet]
        raw = target_sheet.get("raw")
        master_mapping = mobj.get("mapping", {}) or {}
        master_header_row = mobj.get("header_row", -1)

        if not isinstance(raw, pd.DataFrame) or not master_mapping:
            continue

        master_header_names = [normalize_col(x) for x in mobj.get("header_names", [])]
        if not master_header_names and isinstance(mobj.get("raw"), pd.DataFrame):
            master_raw = mobj.get("raw")
            if (
                isinstance(master_header_row, (int, np.integer))
                and 0 <= int(master_header_row) < len(master_raw)
            ):
                master_header_names = [
                    normalize_col(x)
                    for x in master_raw.iloc[int(master_header_row)].astype(str).tolist()
                ]

        key_to_master_col: Dict[str, Optional[str]] = {}
        for key, idx in master_mapping.items():
            if isinstance(idx, (int, np.integer)) and 0 <= int(idx) < len(master_header_names):
                key_to_master_col[key] = master_header_names[int(idx)]
            else:
                key_to_master_col[key] = None

        existing_header_row = target_sheet.get("header_row", -1)
        target_header_row, header = _normalize_header_row(
            raw,
            list(key_to_master_col.values()),
            existing_header_row,
            master_header_row,
        )
        body = raw.iloc[target_header_row + 1 :].reset_index(drop=True)
        body.columns = header

        header_lookup: Dict[str, int] = {}
        for idx, name in enumerate(header):
            if not name:
                continue
            header_lookup.setdefault(name, idx)

        previous_mapping = target_sheet.get("mapping", {}).copy()
        all_keys = set(previous_mapping.keys()) | set(master_mapping.keys())
        new_mapping: Dict[str, int] = {}

        for key in all_keys:
            resolved_idx = -1
            master_col_name = key_to_master_col.get(key)
            if master_col_name:
                resolved_idx = header_lookup.get(master_col_name, -1)
            if resolved_idx < 0:
                prev_idx = previous_mapping.get(key, -1)
                if isinstance(prev_idx, (int, np.integer)) and 0 <= int(prev_idx) < len(header):
                    resolved_idx = int(prev_idx)
            if resolved_idx < 0:
                resolved_idx = -1
            new_mapping[key] = resolved_idx

        try:
            table = build_normalized_table(body, new_mapping)
        except Exception:
            continue

        target_sheet.update(
            {
                "mapping": new_mapping,
                "header_row": target_header_row,
                "table": table,
                "header_names": header,
            }
        )

def mapping_ui(
    section_title: str,
    wb: WorkbookData,
    minimal: bool = False,
    minimal_sheets: Optional[List[str]] = None,
    *,
    section_id: Optional[str] = None,
) -> bool:
    """Render mapping UI and return True if any mapping changed."""
    st.subheader(section_title)
    tabs = st.tabs(list(wb.sheets.keys()))
    changed_any = False
    section_key_input = section_id if section_id is not None else f"{wb.name}__{section_title}"
    section_key = _normalize_key_part(section_key_input)

    for tab, (sheet, obj) in zip(tabs, wb.sheets.items()):
        use_minimal = minimal or (minimal_sheets is not None and sheet in minimal_sheets)
        with tab:
            st.markdown(f"**List:** `{sheet}`")
            raw = obj.get("raw")
            header_row = obj.get("header_row", -1)
            stored_mapping = obj.get("mapping", {}).copy()
            prev_header = header_row
            hdr_preview = raw.head(10) if isinstance(raw, pd.DataFrame) else None
            if hdr_preview is not None:
                show_df(hdr_preview)
            # Header row selector
            sheet_key = _normalize_key_part(sheet)
            header_row = st.number_input(
                f"Řádek s hlavičkou (0 = první řádek) — {sheet}",
                min_value=0,
                max_value=9,
                value=header_row if header_row >= 0 else 0,
                step=1,
                key=make_widget_key("hdr", section_key, sheet_key),
            )
            # Build header names for the selected row
            if isinstance(raw, pd.DataFrame) and header_row < len(raw):
                header_names = [
                    normalize_col(x)
                    for x in raw.iloc[header_row].astype(str).tolist()
                ]
            else:
                header_names = obj.get("header_names", [])
            header_names = [normalize_col(x) for x in header_names]

            header_lookup: Dict[str, int] = {}
            for idx, name in enumerate(header_names):
                raw_key = str(name).strip()
                if raw_key and raw_key not in header_lookup:
                    header_lookup[raw_key] = idx
                normalized_key = normalize_col(raw_key)
                if normalized_key and normalized_key not in header_lookup:
                    header_lookup[normalized_key] = idx

            def sanitize_index(value: Any) -> int:
                idx_val: Optional[int]
                if isinstance(value, (int, np.integer)):
                    idx_val = int(value)
                elif isinstance(value, (float, np.floating)):
                    as_float = float(value)
                    if math.isnan(as_float):
                        return -1
                    idx_val = int(as_float)
                elif isinstance(value, str):
                    stripped = value.strip()
                    if not stripped:
                        return -1
                    try:
                        idx_val = int(float(stripped))
                    except ValueError:
                        normalized = normalize_col(stripped)
                        if normalized in header_lookup:
                            idx_val = header_lookup[normalized]
                        elif stripped in header_lookup:
                            idx_val = header_lookup[stripped]
                        else:
                            return -1
                else:
                    return -1
                idx_int = int(idx_val)
                if idx_int < 0 or idx_int >= len(header_names):
                    return -1
                return idx_int

            mapping = {key: sanitize_index(val) for key, val in stored_mapping.items()}
            mapping.setdefault("item_id", -1)
            prev_mapping = mapping.copy()

            # Select boxes for mapping
            cols = list(range(len(header_names)))
            if not cols:
                cols = [0]

            def pick_default(key: str) -> int:
                stored_value = mapping.get(key)
                if (
                    stored_value is not None
                    and 0 <= int(stored_value) < len(header_names)
                ):
                    return int(stored_value)
                hints = HEADER_HINTS.get(key, [])
                for i, col in enumerate(header_names):
                    if any(p in col for p in hints):
                        return i
                return 0

            def clamp(idx: Any) -> int:
                try:
                    idx_int = int(idx)
                except (TypeError, ValueError):
                    idx_int = 0
                idx_int = max(0, min(idx_int, len(cols) - 1))
                return idx_int

            if use_minimal:
                c1, c2, c3, c4 = st.columns(4)
                with c1:
                    code_idx = st.selectbox(
                        "Sloupec: code",
                        cols,
                        format_func=lambda i: header_names[i] if i < len(header_names) else "",
                        index=clamp(pick_default("code")),
                        key=make_widget_key("map", section_key, sheet_key, "code"),
                    )
                with c2:
                    desc_idx = st.selectbox(
                        "Sloupec: description",
                        cols,
                        format_func=lambda i: header_names[i] if i < len(header_names) else "",
                        index=clamp(pick_default("description")),
                        key=make_widget_key("map", section_key, sheet_key, "description"),
                    )
                with c3:
                    total_idx = st.selectbox(
                        "Sloupec: total_price",
                        cols,
                        format_func=lambda i: header_names[i] if i < len(header_names) else "",
                        index=clamp(pick_default("total_price")),
                        key=make_widget_key("map", section_key, sheet_key, "total_price"),
                    )
                with c4:
                    summ_idx = st.selectbox(
                        "Sloupec: summary_total",
                        cols,
                        format_func=lambda i: header_names[i] if i < len(header_names) else "",
                        index=clamp(pick_default("summary_total")),
                        key=make_widget_key("map", section_key, sheet_key, "summary_total"),
                    )
                ui_mapping = {
                    "code": code_idx,
                    "description": desc_idx,
                    "unit": -1,
                    "quantity": -1,
                    "quantity_supplier": -1,
                    "unit_price_material": -1,
                    "unit_price_install": -1,
                    "total_price": total_idx,
                    "summary_total": summ_idx,
                    "item_id": -1,
                }
            else:
                c1, c2, c3, c4 = st.columns(4)
                with c1:
                    code_idx = st.selectbox(
                        "Sloupec: code",
                        cols,
                        format_func=lambda i: header_names[i] if i < len(header_names) else "",
                        index=clamp(pick_default("code")),
                        key=make_widget_key("map", section_key, sheet_key, "code"),
                    )
                with c2:
                    desc_idx = st.selectbox(
                        "Sloupec: description",
                        cols,
                        format_func=lambda i: header_names[i] if i < len(header_names) else "",
                        index=clamp(pick_default("description")),
                        key=make_widget_key("map", section_key, sheet_key, "description"),
                    )
                with c3:
                    unit_idx = st.selectbox(
                        "Sloupec: unit",
                        cols,
                        format_func=lambda i: header_names[i] if i < len(header_names) else "",
                        index=clamp(pick_default("unit")),
                        key=make_widget_key("map", section_key, sheet_key, "unit"),
                    )
                with c4:
                    qty_idx = st.selectbox(
                        "Sloupec: quantity",
                        cols,
                        format_func=lambda i: header_names[i] if i < len(header_names) else "",
                        index=clamp(pick_default("quantity")),
                        key=make_widget_key("map", section_key, sheet_key, "quantity"),
                    )
                c5, c6, c7 = st.columns(3)
                with c5:
                    qty_sup_idx = st.selectbox(
                        "Sloupec: quantity_supplier",
                        cols,
                        format_func=lambda i: header_names[i] if i < len(header_names) else "",
                        index=clamp(pick_default("quantity_supplier")),
                        key=make_widget_key("map", section_key, sheet_key, "quantity_supplier"),
                    )
                with c6:
                    upm_idx = st.selectbox(
                        "Sloupec: unit_price_material",
                        cols,
                        format_func=lambda i: header_names[i] if i < len(header_names) else "",
                        index=clamp(pick_default("unit_price_material")),
                        key=make_widget_key("map", section_key, sheet_key, "unit_price_material"),
                    )
                with c7:
                    upi_idx = st.selectbox(
                        "Sloupec: unit_price_install",
                        cols,
                        format_func=lambda i: header_names[i] if i < len(header_names) else "",
                        index=clamp(pick_default("unit_price_install")),
                        key=make_widget_key("map", section_key, sheet_key, "unit_price_install"),
                    )
                c8, c9, c10 = st.columns(3)
                with c8:
                    total_idx = st.selectbox(
                        "Sloupec: total_price",
                        cols,
                        format_func=lambda i: header_names[i] if i < len(header_names) else "",
                        index=clamp(pick_default("total_price")),
                        key=make_widget_key("map", section_key, sheet_key, "total_price"),
                    )
                with c9:
                    summ_idx = st.selectbox(
                        "Sloupec: summary_total",
                        cols,
                        format_func=lambda i: header_names[i] if i < len(header_names) else "",
                        index=clamp(pick_default("summary_total")),
                        key=make_widget_key("map", section_key, sheet_key, "summary_total"),
                    )
                with c10:
                    item_idx = st.selectbox(
                        "Sloupec: item_id",
                        cols,
                        format_func=lambda i: header_names[i] if i < len(header_names) else "",
                        index=clamp(pick_default("item_id")),
                        key=make_widget_key("map", section_key, sheet_key, "item_id"),
                    )

                ui_mapping = {
                    "code": code_idx,
                    "description": desc_idx,
                    "unit": unit_idx,
                    "quantity": qty_idx,
                    "quantity_supplier": qty_sup_idx,
                    "unit_price_material": upm_idx,
                    "unit_price_install": upi_idx,
                    "total_price": total_idx,
                    "summary_total": summ_idx,
                    "item_id": item_idx,
                }
            if isinstance(raw, pd.DataFrame):
                body = raw.iloc[header_row+1:].reset_index(drop=True)
                body.columns = [normalize_col(x) for x in raw.iloc[header_row].tolist()]
                table = build_normalized_table(body, ui_mapping)
            else:
                table = pd.DataFrame()

            wb.sheets[sheet]["mapping"] = ui_mapping
            wb.sheets[sheet]["header_row"] = header_row
            wb.sheets[sheet]["table"] = table
            mapping_changed = (ui_mapping != prev_mapping) or (header_row != prev_header)
            wb.sheets[sheet]["_changed"] = mapping_changed
            changed_any = changed_any or mapping_changed

            st.markdown("**Normalizovaná tabulka (náhled):**")
            show_df(table.head(50))
    return changed_any


def _build_join_lookup(df: pd.DataFrame) -> pd.DataFrame:
    """Return normalized join helpers grouped by ``__key__``."""

    if df is None or df.empty or "__key__" not in df.columns:
        return pd.DataFrame(columns=["__item_join__", "__fallback_join__"])

    df_local = df.copy()
    code_series = (
        df_local.get("code", pd.Series(index=df_local.index, dtype=object))
        .fillna("")
        .astype(str)
        .str.strip()
    )
    desc_series = (
        df_local.get("description", pd.Series(index=df_local.index, dtype=object))
        .fillna("")
        .astype(str)
        .str.strip()
    )

    key_df = pd.DataFrame({"code": code_series, "description": desc_series}, index=df_local.index)
    line_ids = key_df.groupby(["code", "description"], sort=False).cumcount()
    fallback = code_series + "||" + desc_series + "||" + line_ids.astype(str)

    if "item_id" in df_local.columns:
        item_series = normalize_identifier(df_local["item_id"]).fillna("").astype(str).str.strip()
    else:
        item_series = pd.Series(["" for _ in range(len(df_local))], index=df_local.index, dtype=object)

    lookup = pd.DataFrame(
        {
            "__key__": df_local["__key__"].astype(str),
            "__item_join__": item_series,
            "__fallback_join__": fallback,
        },
        index=df_local.index,
    )

    grouped = (
        lookup.groupby("__key__", sort=False)[["__item_join__", "__fallback_join__"]]
        .first()
        .copy()
    )
    return grouped


def _choose_join_columns(
    master_lookup: pd.DataFrame, supplier_lookup: pd.DataFrame, join_mode: str
) -> Tuple[pd.Series, pd.Series]:
    """Return matching join key series for master and supplier tables."""

    empty_master = master_lookup.empty or "__item_join__" not in master_lookup
    empty_supplier = supplier_lookup.empty or "__item_join__" not in supplier_lookup

    use_item_ids = False
    if join_mode != "code+description" and not empty_master and not empty_supplier:
        master_has_ids = master_lookup["__item_join__"].fillna("").astype(str).str.strip().ne("").any()
        supplier_has_ids = (
            supplier_lookup["__item_join__"].fillna("").astype(str).str.strip().ne("").any()
        )
        use_item_ids = master_has_ids and supplier_has_ids

    master_col = "__item_join__" if use_item_ids else "__fallback_join__"
    supplier_col = "__item_join__" if use_item_ids else "__fallback_join__"

    master_series = (
        master_lookup.get(master_col, pd.Series(dtype=object))
        if not master_lookup.empty
        else pd.Series(dtype=object)
    )
    supplier_series = (
        supplier_lookup.get(supplier_col, pd.Series(dtype=object))
        if not supplier_lookup.empty
        else pd.Series(dtype=object)
    )
    return master_series, supplier_series


def compare(master: WorkbookData, bids: Dict[str, WorkbookData], join_mode: str = "auto") -> Dict[str, pd.DataFrame]:
    """
    join_mode: "auto" (Item ID if detekováno, jinak code+description), nebo "code+description".
    """
    results = {}
    sheets = list(master.sheets.keys())
    for sheet in sheets:
        mobj = master.sheets.get(sheet, {})
        mtab = mobj.get("table", pd.DataFrame())
        if mtab is None or mtab.empty:
            continue
        summary_mask_master = is_summary_like_row(mtab)
        if summary_mask_master.any():
            mtab = mtab.loc[~summary_mask_master].copy()
        mtab = mtab[mtab["description"].astype(str).str.strip() != ""]
        base_cols = ["__key__", "code", "description", "unit", "quantity", "total_price"]
        if "item_id" in mtab.columns:
            base_cols.insert(1, "item_id")
        existing_base_cols = [col for col in base_cols if col in mtab.columns]
        base = mtab[existing_base_cols].copy()
        if "quantity" in base.columns:
            base["quantity"] = pd.to_numeric(base["quantity"], errors="coerce").fillna(0)
        else:
            base["quantity"] = 0
        if "total_price" in base.columns:
            base["total_price"] = pd.to_numeric(base["total_price"], errors="coerce").fillna(0)
        else:
            base["total_price"] = 0
        agg_mapping = {
            "code": "first",
            "description": "first",
            "unit": "first",
            "quantity": "sum",
            "total_price": "sum",
        }
        if "item_id" in base.columns:
            agg_mapping["item_id"] = "first"
        base_grouped = base.groupby("__key__", sort=False, as_index=False).agg(agg_mapping)
        master_lookup = _build_join_lookup(mtab)
        master_total_sum = base_grouped["total_price"].sum()
        base_grouped.rename(columns={"total_price": "Master total"}, inplace=True)
        comp = base_grouped.copy()

        supplier_totals: Dict[str, float] = {}
        for sup_name, wb in bids.items():
            tobj = wb.sheets.get(sheet, {})
            ttab = tobj.get("table", pd.DataFrame())
            if ttab is None or ttab.empty:
                comp[f"{sup_name} quantity"] = np.nan
                comp[f"{sup_name} unit_price"] = np.nan
                comp[f"{sup_name} total"] = np.nan
                continue
            summary_mask_supplier = is_summary_like_row(ttab)
            if summary_mask_supplier.any():
                ttab = ttab.loc[~summary_mask_supplier].copy()
            ttab = ttab[ttab["description"].astype(str).str.strip() != ""]
            # join by __key__ (manual mapping already built in normalized table)
            sup_qty_col = "quantity_supplier" if "quantity_supplier" in ttab.columns else "quantity"
            cols = [
                "__key__",
                sup_qty_col,
                "unit_price_material",
                "unit_price_install",
                "total_price",
                "unit",
            ]
            if "item_id" in ttab.columns:
                cols.append("item_id")
            existing_cols = [c for c in cols if c in ttab.columns]
            tt = ttab[existing_cols].copy()
            supplier_lookup = _build_join_lookup(ttab)
            tt[sup_qty_col] = pd.to_numeric(tt[sup_qty_col], errors="coerce")
            tt["total_price"] = pd.to_numeric(tt["total_price"], errors="coerce").fillna(0)
            price_cols = [c for c in ["unit_price_material", "unit_price_install"] if c in tt.columns]
            if price_cols:
                for col in price_cols:
                    tt[col] = pd.to_numeric(tt[col], errors="coerce")
                tt["unit_price_combined"] = tt[price_cols].sum(axis=1, min_count=1)
            else:
                tt["unit_price_combined"] = np.nan
            if "unit" in tt.columns:
                tt["unit"] = tt["unit"].astype(str).str.strip()
            supplier_totals[sup_name] = float(tt["total_price"].sum())
            first_price = (
                tt.groupby("__key__", sort=False)["unit_price_combined"].first().reset_index(name="first_unit_price")
            )
            def _sum_with_min_count(series: pd.Series) -> float:
                return series.sum(min_count=1)

            tt_grouped = tt.groupby("__key__", sort=False, as_index=False).agg(
                {
                    sup_qty_col: _sum_with_min_count,
                    "total_price": "sum",
                }
            )
            tt_grouped = tt_grouped.merge(first_price, on="__key__", how="left")
            if "unit" in tt.columns:
                unit_source = tt[["__key__", "unit"]].copy()
                unit_source = unit_source[unit_source["unit"].astype(str).str.strip() != ""]
                unit_grouped = (
                    unit_source.groupby("__key__", sort=False)["unit"].first().reset_index()
                )
                tt_grouped = tt_grouped.merge(unit_grouped, on="__key__", how="left")
            qty = tt_grouped[sup_qty_col]
            with np.errstate(divide="ignore", invalid="ignore"):
                qty_for_division = qty.where(qty != 0)
                tt_grouped["unit_price_combined"] = tt_grouped["total_price"] / qty_for_division
            mask = qty_for_division.isna()
            tt_grouped.loc[mask, "unit_price_combined"] = tt_grouped.loc[mask, "first_unit_price"]
            tt_grouped.drop(columns=["first_unit_price"], inplace=True)
            master_join_series, supplier_join_series = _choose_join_columns(
                master_lookup, supplier_lookup, join_mode
            )
            comp_join_keys = comp["__key__"].astype(str).map(master_join_series)
            tt_grouped["__join_key__"] = tt_grouped["__key__"].astype(str).map(supplier_join_series)
            comp["__join_key__"] = comp_join_keys
            merge_cols = ["__join_key__", sup_qty_col, "unit_price_combined", "total_price"]
            unit_merge_col: Optional[str] = None
            if "unit" in tt_grouped.columns:
                unit_merge_col = f"__{sup_name}__unit"
                tt_grouped.rename(columns={"unit": unit_merge_col}, inplace=True)
                merge_cols.append(unit_merge_col)
            comp = comp.merge(tt_grouped[merge_cols], on="__join_key__", how="left")
            comp.drop(columns=["__join_key__"], inplace=True, errors="ignore")
            rename_map = {
                sup_qty_col: f"{sup_name} quantity",
                "unit_price_combined": f"{sup_name} unit_price",
                "total_price": f"{sup_name} total",
            }
            if unit_merge_col:
                rename_map[unit_merge_col] = f"{sup_name} unit"
            comp.rename(columns=rename_map, inplace=True)
            comp[f"{sup_name} Δ qty"] = comp[f"{sup_name} quantity"] - comp["quantity"]

        for sup_name, total_sum in supplier_totals.items():
            col = f"{sup_name} total"
            if col not in comp.columns:
                continue
            mapped_series = pd.to_numeric(comp[col], errors="coerce")
            mapped_sum = mapped_series.sum(min_count=1)
            mapped_sum = float(mapped_sum) if pd.notna(mapped_sum) else 0.0
            diff = float(total_sum - mapped_sum)
            if math.isclose(diff, 0.0, rel_tol=1e-9, abs_tol=1e-6):
                continue
            extra_row: Dict[str, Any] = {c: np.nan for c in comp.columns}
            extra_row["__key__"] = f"__UNMAPPED__::{sup_name}"
            if "code" in extra_row:
                extra_row["code"] = ""
            if "description" in extra_row:
                extra_row["description"] = f"{UNMAPPED_ROW_LABEL} ({sup_name})"
            if "unit" in extra_row:
                extra_row["unit"] = ""
            if "quantity" in extra_row:
                extra_row["quantity"] = np.nan
            if "item_id" in extra_row:
                extra_row["item_id"] = ""
            extra_row[col] = diff
            comp = pd.concat([comp, pd.DataFrame([extra_row])], ignore_index=True)

        if supplier_totals:
            comp.attrs.setdefault("supplier_totals", {}).update(supplier_totals)

        total_cols = [c for c in comp.columns if c.endswith(" total") and c != "Master total"]
        if total_cols:
            comp["LOWEST total"] = comp[total_cols].min(axis=1, skipna=True)
            highest_total = comp[total_cols].max(axis=1, skipna=True)
            comp["MIDRANGE total"] = (comp["LOWEST total"] + highest_total) / 2
            for c in total_cols:
                comp[f"{c} Δ vs LOWEST"] = comp[c] - comp["LOWEST total"]

            def _valid_supplier_totals(row: pd.Series) -> Dict[str, Any]:
                values = {}
                for col in total_cols:
                    value = row[col]
                    if pd.notna(value):
                        values[col.replace(" total", "")] = value
                return values

            # Which supplier is the lowest per row?
            def lowest_supplier(row: pd.Series) -> Optional[str]:
                values = _valid_supplier_totals(row)
                if not values:
                    return None
                return min(values, key=values.get)

            def supplier_range(row: pd.Series) -> Optional[str]:
                values = _valid_supplier_totals(row)
                if not values:
                    return None
                lowest = min(values, key=values.get)
                highest = max(values, key=values.get)
                if lowest == highest:
                    return lowest
                return f"{lowest} – {highest}"

            comp["LOWEST supplier"] = comp.apply(lowest_supplier, axis=1)
            comp["MIDRANGE supplier range"] = comp.apply(supplier_range, axis=1)

        comp.attrs["master_total_sum"] = master_total_sum
        results[sheet] = comp
    return results

def summarize(results: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    rows = []
    for sheet, df in results.items():
        if df is None or df.empty:
            continue
        total_cols = [c for c in df.columns if c.endswith(" total")]
        df = df[df["description"].astype(str).str.strip() != ""]
        sums = {c: df[c].dropna().sum() for c in total_cols}
        row = {"sheet": sheet}
        row.update(sums)
        rows.append(row)
    out = pd.DataFrame(rows)
    return out


def rename_comparison_columns(df: pd.DataFrame, mapping: Dict[str, str]) -> pd.DataFrame:
    if df is None or df.empty or not mapping:
        return df
    rename_map: Dict[str, str] = {}
    for raw, alias in mapping.items():
        rename_map[f"{raw} quantity"] = f"{alias} quantity"
        rename_map[f"{raw} unit"] = f"{alias} unit"
        rename_map[f"{raw} unit_price"] = f"{alias} unit_price"
        rename_map[f"{raw} total"] = f"{alias} total"
        rename_map[f"{raw} Δ qty"] = f"{alias} Δ qty"
        rename_map[f"{raw} Δ vs LOWEST"] = f"{alias} Δ vs LOWEST"
    renamed = df.rename(columns=rename_map).copy()
    if "supplier_totals" in df.attrs:
        renamed_totals = {
            mapping.get(raw, raw): total for raw, total in df.attrs.get("supplier_totals", {}).items()
        }
        renamed.attrs["supplier_totals"] = renamed_totals
    if "LOWEST supplier" in renamed.columns or "MIDRANGE supplier range" in renamed.columns:

        def _map_supplier_name(value: Any) -> Any:
            if pd.isna(value):
                return value
            return mapping.get(value, value)

        if "LOWEST supplier" in renamed.columns:
            renamed["LOWEST supplier"] = renamed["LOWEST supplier"].apply(
                _map_supplier_name
            )

        if "MIDRANGE supplier range" in renamed.columns:

            def _map_supplier_range(value: Any) -> Any:
                if pd.isna(value):
                    return value
                if isinstance(value, (list, tuple)) and len(value) == 2:
                    low = _map_supplier_name(value[0])
                    high = _map_supplier_name(value[1])
                elif isinstance(value, str):
                    parts = [p.strip() for p in re.split(r"[–-]", value) if p.strip()]
                    if not parts:
                        return value
                    if len(parts) == 1:
                        return _map_supplier_name(parts[0])
                    low = _map_supplier_name(parts[0])
                    high = _map_supplier_name(parts[-1])
                else:
                    return value
                if low == high:
                    return low
                return f"{low} – {high}"

            renamed["MIDRANGE supplier range"] = renamed["MIDRANGE supplier range"].apply(
                _map_supplier_range
            )
    if "description" in renamed.columns:

        def _replace_unmapped_description(value: Any) -> Any:
            if not isinstance(value, str) or UNMAPPED_ROW_LABEL not in value:
                return value
            updated = value
            for raw, alias in mapping.items():
                updated = re.sub(
                    rf"\({re.escape(raw)}\)",
                    f"({alias})",
                    updated,
                )
            return updated

        renamed["description"] = renamed["description"].apply(_replace_unmapped_description)
    return renamed


def rename_total_columns(df: pd.DataFrame, mapping: Dict[str, str]) -> pd.DataFrame:
    if df is None or df.empty or not mapping:
        return df
    rename_map = {f"{raw} total": f"{alias} total" for raw, alias in mapping.items()}
    return df.rename(columns=rename_map)


def infer_section_group(code: Any, description: Any) -> str:
    """Return a heuristic section identifier based on code/description."""

    code_str = str(code if code is not None else "").strip()
    if code_str:
        cleaned = re.sub(r"[\u2013\u2014\u2012\u2010]", "-", code_str)
        cleaned = cleaned.replace("\\", ".").replace("/", ".")
        cleaned = re.sub(r"\s+", " ", cleaned)
        for sep in (".", "-", " "):
            if sep in cleaned:
                token = cleaned.split(sep)[0].strip()
                if token:
                    return token
        match = re.match(r"[A-Za-z]*\d+", cleaned)
        if match:
            return match.group(0)
        return cleaned

    desc_str = str(description if description is not None else "").strip()
    if desc_str:
        token = re.split(r"[\s/\-]+", desc_str)[0]
        token = re.sub(r"[^0-9A-Za-z]+", "", token)
        if token:
            return token.upper()
    return ""


def ensure_group_key(candidate: str, code: Any, description: Any, index: int) -> str:
    candidate = (candidate or "").strip()
    if candidate:
        return candidate

    code_str = str(code if code is not None else "").strip()
    if code_str:
        return re.sub(r"\s+", " ", code_str)

    desc_str = str(description if description is not None else "").strip()
    if desc_str:
        token = re.split(r"[\s/\-]+", desc_str)[0]
        token = re.sub(r"[^0-9A-Za-z]+", "", token)
        if token:
            return token.upper()
    return f"ODDIL_{index + 1}"


def build_group_label(key: str, description: Any) -> str:
    key = (key or "").strip()
    desc_str = str(description if description is not None else "").strip()
    if key and desc_str:
        if desc_str.lower().startswith(key.lower()):
            return desc_str
        return f"{key} — {desc_str}"
    if desc_str:
        return desc_str
    if key:
        return key
    return "Bez kódu"


def overview_comparison(
    master: WorkbookData, bids: Dict[str, WorkbookData], sheet_name: str
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Return tables for section totals, indirect costs, added costs,
    missing items and aggregated indirect totals."""
    mobj = master.sheets.get(sheet_name, {})
    mtab = mobj.get("table", pd.DataFrame())
    if (mtab is None or mtab.empty) and isinstance(mobj.get("raw"), pd.DataFrame):
        mapping, hdr, body = try_autodetect_mapping(mobj["raw"])
        if mapping:
            mtab = build_normalized_table(body, mapping)
    if mtab is None or mtab.empty:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
    if "is_summary" in mtab.columns:
        mtab = mtab[~mtab["is_summary"].fillna(False).astype(bool)]
    mtab = mtab.copy()
    if "__row_order__" not in mtab.columns:
        mtab["__row_order__"] = np.arange(len(mtab))
    mtab["total_for_sum"] = coerce_numeric(mtab.get("total_price", np.nan)).fillna(0)
    if "item_id" in mtab.columns:
        master_item_ids = normalize_identifier(mtab["item_id"])
    else:
        master_item_ids = pd.Series(["" for _ in range(len(mtab))], index=mtab.index, dtype=object)

    master_key_df = pd.DataFrame(
        {
            "__code_key__": mtab.get("code", pd.Series(index=mtab.index, dtype=object))
            .fillna("")
            .astype(str),
            "__desc_key__": mtab.get("description", pd.Series(index=mtab.index, dtype=object))
            .fillna("")
            .astype(str),
        },
        index=mtab.index,
    )
    mtab["__line_id__"] = (
        master_key_df.groupby(["__code_key__", "__desc_key__"], sort=False).cumcount()
    )
    fallback_join = (
        master_key_df["__code_key__"].astype(str).str.strip()
        + "||"
        + master_key_df["__desc_key__"].astype(str).str.strip()
        + "||"
        + mtab["__line_id__"].astype(str)
    )
    master_join_key = master_item_ids.astype(str).str.strip()
    master_join_key = master_join_key.where(master_join_key != "", fallback_join)

    base_columns = ["code", "description", "item_id", "__row_order__", "__line_id__", "total_for_sum"]
    base_existing = [col for col in base_columns if col in mtab.columns or col == "item_id"]
    df = mtab[[col for col in base_existing if col in mtab.columns]].copy()
    if "item_id" not in df.columns:
        df["item_id"] = master_item_ids.values
    else:
        df["item_id"] = master_item_ids.values
    if df["item_id"].astype(str).str.strip().eq("").all():
        df.drop(columns=["item_id"], inplace=True)
    df["__join_key__"] = master_join_key.values
    ordered_cols = ["code", "description"]
    if "item_id" in df.columns:
        ordered_cols.append("item_id")
    ordered_cols.extend(["__row_order__", "__line_id__", "total_for_sum", "__join_key__"])
    df = df[ordered_cols]
    df.rename(columns={"total_for_sum": "Master total"}, inplace=True)
    df["__row_status__"] = "master"

    combined_rows: Dict[str, Dict[str, Any]] = {}
    master_join_keys = set(df["__join_key__"].dropna().astype(str)) if "__join_key__" in df.columns else set()
    next_row_order = (
        float(pd.to_numeric(df["__row_order__"], errors="coerce").max())
        if "__row_order__" in df.columns and not df.empty
        else 0.0
    )

    for sup_name, wb in bids.items():
        tobj = wb.sheets.get(sheet_name, {})
        ttab = tobj.get("table", pd.DataFrame())
        if (ttab is None or ttab.empty) and isinstance(tobj.get("raw"), pd.DataFrame):
            mapping, hdr, body = try_autodetect_mapping(tobj["raw"])
            if mapping:
                ttab = build_normalized_table(body, mapping)
        if ttab is None or ttab.empty:
            df[f"{sup_name} total"] = np.nan
        else:
            if "is_summary" in ttab.columns:
                ttab = ttab[~ttab["is_summary"].fillna(False).astype(bool)]
            ttab = ttab.copy()
            ttab["total_for_sum"] = coerce_numeric(ttab.get("total_price", np.nan)).fillna(0)
            if "item_id" in ttab.columns:
                supplier_item_ids = normalize_identifier(ttab["item_id"])
            else:
                supplier_item_ids = pd.Series(["" for _ in range(len(ttab))], index=ttab.index, dtype=object)
            supplier_key_df = pd.DataFrame(
                {
                    "__code_key__": ttab.get("code", pd.Series(index=ttab.index, dtype=object))
                    .fillna("")
                    .astype(str),
                    "__desc_key__": ttab.get("description", pd.Series(index=ttab.index, dtype=object))
                    .fillna("")
                    .astype(str),
                },
                index=ttab.index,
            )
            ttab["__line_id__"] = (
                supplier_key_df.groupby(["__code_key__", "__desc_key__"], sort=False).cumcount()
            )
            fallback_join = (
                supplier_key_df["__code_key__"].astype(str).str.strip()
                + "||"
                + supplier_key_df["__desc_key__"].astype(str).str.strip()
                + "||"
                + ttab["__line_id__"].astype(str)
            )
            supplier_join_key = supplier_item_ids.astype(str).str.strip()
            supplier_join_key = supplier_join_key.where(supplier_join_key != "", fallback_join)
            ttab["__join_key__"] = supplier_join_key.values
            totals_grouped = (
                ttab.groupby("__join_key__", sort=False)["total_for_sum"].sum(min_count=1)
                if "total_for_sum" in ttab.columns
                else pd.Series(dtype=float)
            )
            display_agg: Dict[str, str] = {
                "code": "first",
                "description": "first",
            }
            if "item_id" in ttab.columns:
                display_agg["item_id"] = "first"
            if "__row_order__" in ttab.columns:
                display_agg["__row_order__"] = "min"
            if "__line_id__" in ttab.columns:
                display_agg["__line_id__"] = "first"
            supplier_display = (
                ttab.groupby("__join_key__", sort=False).agg(display_agg)
                if display_agg
                else pd.DataFrame()
            )
            tdf = ttab[["__join_key__", "total_for_sum"]].copy()
            tdf.rename(columns={"total_for_sum": f"{sup_name} total"}, inplace=True)
            df = df.merge(tdf, on="__join_key__", how="left")

            if not supplier_display.empty:
                for key, row_info in supplier_display.iterrows():
                    if pd.isna(key):
                        continue
                    key_str = str(key)
                    if key_str in master_join_keys:
                        continue
                    entry = combined_rows.get(key_str)
                    if entry is None:
                        entry = {
                            "__join_key__": key_str,
                            "code": row_info.get("code", ""),
                            "description": row_info.get("description", ""),
                            "Master total": np.nan,
                            "__row_status__": "supplier_only",
                        }
                        if "item_id" in df.columns:
                            entry["item_id"] = row_info.get("item_id", "")
                        if "__row_order__" in df.columns:
                            next_row_order += 1
                            entry["__row_order__"] = next_row_order
                        if "__line_id__" in df.columns:
                            entry["__line_id__"] = row_info.get("__line_id__", 0)
                    entry[f"{sup_name} total"] = totals_grouped.get(key, np.nan)
                    combined_rows[key_str] = entry

    total_cols = [c for c in df.columns if str(c).endswith(" total")]
    supplier_total_cols = [c for c in total_cols if c != "Master total"]

    if combined_rows:
        extra_df = pd.DataFrame.from_records(list(combined_rows.values()))
        for col in df.columns:
            if col not in extra_df.columns:
                extra_df[col] = np.nan
        extra_df = extra_df[df.columns]
        df = pd.concat([df, extra_df], ignore_index=True, sort=False)

    if "Master total" in df.columns:
        master_presence = df["Master total"].notna()
    else:
        master_presence = pd.Series(False, index=df.index)
    any_supplier_present = pd.Series(False, index=df.index, dtype=bool)
    if supplier_total_cols:
        all_supplier_present = pd.Series(True, index=df.index, dtype=bool)
    else:
        all_supplier_present = pd.Series(False, index=df.index, dtype=bool)

    for col in supplier_total_cols:
        supplier_presence = df[col].notna()
        any_supplier_present |= supplier_presence
        if supplier_total_cols:
            all_supplier_present &= supplier_presence

    if supplier_total_cols:
        partial_mask = master_presence & any_supplier_present & ~all_supplier_present
        df.loc[partial_mask, "__row_status__"] = "partial"
        matched_mask = master_presence & all_supplier_present
        df.loc[matched_mask, "__row_status__"] = "matched"

    master_only_mask = master_presence & ~any_supplier_present
    df.loc[master_only_mask, "__row_status__"] = "master_only"
    supplier_only_mask = ~master_presence & any_supplier_present
    df.loc[supplier_only_mask, "__row_status__"] = "supplier_only"

    base_view_cols = ["code", "description"]
    if "item_id" in df.columns:
        base_view_cols.append("item_id")
    base_view_cols.extend(["__row_order__", "__line_id__"])
    view_cols = base_view_cols + total_cols + ["__row_status__"]
    view = df[view_cols].copy()
    view["code"] = view["code"].fillna("").astype(str)
    view["description"] = view["description"].fillna("").astype(str)
    view = view[view["description"].str.strip() != ""]
    view = view.sort_values(by="__row_order__").reset_index(drop=True)

    for col in total_cols:
        present_col = f"__present__{col}"
        view[present_col] = view[col].notna()

    auto_keys: List[str] = []
    for idx, row in view.iterrows():
        candidate = infer_section_group(row.get("code"), row.get("description"))
        auto_keys.append(ensure_group_key(candidate, row.get("code"), row.get("description"), idx))
    view["auto_group_key"] = pd.Series(auto_keys, index=view.index).astype(str)

    ordered = view.sort_values("__row_order__")
    first_desc_map = ordered.groupby("auto_group_key")["description"].first().to_dict()
    view["auto_group_label"] = view.apply(
        lambda r: build_group_label(
            r["auto_group_key"], first_desc_map.get(r["auto_group_key"], r.get("description"))
        ),
        axis=1,
    )
    order_map = ordered.groupby("auto_group_key")["__row_order__"].min().to_dict()
    view["auto_group_order"] = view["auto_group_key"].map(order_map)
    view["auto_group_label"] = view["auto_group_label"].fillna(view["auto_group_key"])
    view.loc[view["auto_group_label"].astype(str).str.strip() == "", "auto_group_label"] = view[
        "auto_group_key"
    ]
    view["auto_group_order"] = pd.to_numeric(view["auto_group_order"], errors="coerce")
    view["auto_group_order"] = view["auto_group_order"].fillna(view["__row_order__"])

    indirect_mask = view["description"].str.contains("vedlej", case=False, na=False)
    added_mask = view["description"].str.contains("dodavat", case=False, na=False)
    sections_df = view[~(indirect_mask | added_mask)].copy()
    indirect_df = view[indirect_mask].copy()
    added_df = view[added_mask].copy()

    for df_part in (view, sections_df, indirect_df, added_df):
        if "__line_id__" in df_part.columns:
            df_part.drop(columns="__line_id__", inplace=True)

    # Missing items per supplier
    missing_rows: List[pd.DataFrame] = []
    for col in total_cols:
        if col == "Master total":
            continue
        missing_mask = sections_df[col].isna() & sections_df["Master total"].notna()
        if missing_mask.any():
            tmp = sections_df.loc[
                missing_mask,
                ["code", "description", "Master total", "__row_order__"],
            ].copy()
            tmp["missing_in"] = col.replace(" total", "")
            missing_rows.append(tmp)
    missing_df = (
        pd.concat(missing_rows, ignore_index=True)
        if missing_rows
        else pd.DataFrame(columns=["code", "description", "Master total", "missing_in"])
    )
    if not missing_df.empty and "__row_order__" in missing_df.columns:
        missing_df = missing_df.sort_values("__row_order__").reset_index(drop=True)

    # Aggregate indirect costs per supplier
    if indirect_df.empty:
        indirect_total = pd.DataFrame()
    else:
        sums = indirect_df[
            [c for c in indirect_df.columns if str(c).endswith(" total") and not str(c).startswith("__present__")]
        ].sum()
        indirect_total = sums.rename("total").to_frame().reset_index()
        indirect_total.rename(columns={"index": "supplier"}, inplace=True)
        indirect_total["supplier"] = indirect_total["supplier"].str.replace(" total", "", regex=False)

    if "__row_order__" in sections_df.columns:
        sections_df.rename(columns={"__row_order__": "source_order"}, inplace=True)
    if "source_order" in sections_df.columns:
        sections_df["source_order"] = pd.to_numeric(sections_df["source_order"], errors="coerce")
    if "auto_group_order" in sections_df.columns:
        sections_df["auto_group_order"] = pd.to_numeric(
            sections_df["auto_group_order"], errors="coerce"
        )
    if "auto_group_key" in sections_df.columns:
        sections_df["auto_group_key"] = sections_df["auto_group_key"].astype(str)
    if "auto_group_label" in sections_df.columns:
        sections_df["auto_group_label"] = sections_df["auto_group_label"].astype(str)

    for df_part in (indirect_df, added_df):
        if "__row_order__" in df_part.columns:
            df_part.drop(columns="__row_order__", inplace=True)
        for helper_col in ("auto_group_key", "auto_group_label", "auto_group_order"):
            if helper_col in df_part.columns:
                df_part.drop(columns=helper_col, inplace=True)
    if "__row_order__" in view.columns:
        view.drop(columns="__row_order__", inplace=True)
    if not missing_df.empty and "__row_order__" in missing_df.columns:
        missing_df.drop(columns="__row_order__", inplace=True)

    return sections_df, indirect_df, added_df, missing_df, indirect_total


def prepare_grouped_sections(
    df: pd.DataFrame, overrides: Dict[str, Dict[str, Any]]
) -> Tuple[pd.DataFrame, Dict[str, Dict[str, pd.DataFrame]]]:
    """Prepare grouped overview and per-group detail tables."""

    if df is None or df.empty:
        return pd.DataFrame(), {}

    working = df.copy()
    working["code"] = working["code"].fillna("").astype(str)
    working["description"] = working["description"].fillna("").astype(str)
    if "source_order" not in working.columns:
        working["source_order"] = np.arange(len(working))
    working = working.reset_index(drop=True)
    working["item_order"] = pd.to_numeric(working["source_order"], errors="coerce")
    if working["item_order"].isna().any():
        fallback_order = pd.Series(
            np.arange(len(working), dtype=float), index=working.index
        )
        working["item_order"] = working["item_order"].fillna(fallback_order)
    if "auto_group_key" not in working.columns:
        working["auto_group_key"] = working["code"]
    if "auto_group_label" not in working.columns:
        working["auto_group_label"] = working["auto_group_key"]
    if "auto_group_order" not in working.columns:
        working["auto_group_order"] = working["item_order"]
    working["auto_group_order"] = pd.to_numeric(working["auto_group_order"], errors="coerce")
    working["auto_group_order"] = working["auto_group_order"].fillna(working["item_order"])

    overrides = overrides or {}

    def row_group_info(row: pd.Series) -> pd.Series:
        override = overrides.get(str(row["code"]))
        manual_group = ""
        manual_order = np.nan
        manual_flag = False
        if override:
            manual_group = str(override.get("group", "") or "").strip()
            manual_order_raw = override.get("order")
            if manual_order_raw not in (None, ""):
                manual_order = pd.to_numeric(pd.Series([manual_order_raw]), errors="coerce").iloc[0]
            if manual_group:
                manual_flag = True
            if not manual_flag and not pd.isna(manual_order):
                manual_flag = True
        base_key = str(row.get("auto_group_key", "")).strip()
        if not base_key:
            base_key = ensure_group_key("", row.get("code"), row.get("description"), int(row.name))
        base_label = str(row.get("auto_group_label", "")).strip()
        if not base_label:
            base_label = build_group_label(base_key, row.get("description"))
        key = manual_group or base_key
        label = manual_group or base_label or key
        auto_order = row.get("auto_group_order")
        item_order = row.get("item_order")
        order = (
            manual_order
            if not pd.isna(manual_order)
            else auto_order
            if not pd.isna(auto_order)
            else item_order
        )
        return pd.Series(
            {
                "group_key": key,
                "group_label": label,
                "group_order": order,
                "manual_override": manual_flag,
            }
        )

    group_meta = working.apply(row_group_info, axis=1)
    working = pd.concat([working, group_meta], axis=1)
    working["group_order"] = pd.to_numeric(working["group_order"], errors="coerce")
    working["group_order"] = working["group_order"].fillna(working["item_order"])
    working["manual_override"] = working["manual_override"].fillna(False)

    total_cols = [
        c for c in working.columns if str(c).endswith(" total") and not str(c).startswith("__present__")
    ]
    agg_kwargs: Dict[str, Any] = {
        "Skupina": pd.NamedAgg(column="group_label", aggfunc="first"),
        "Referencni_kod": pd.NamedAgg(column="code", aggfunc="first"),
        "Referencni_popis": pd.NamedAgg(column="description", aggfunc="first"),
        "__group_order__": pd.NamedAgg(column="group_order", aggfunc="min"),
        "Rucni_seskupeni": pd.NamedAgg(column="manual_override", aggfunc="max"),
        "Pocet_polozek": pd.NamedAgg(column="code", aggfunc="count"),
    }
    rename_after = {
        "Referencni_kod": "Referenční kód",
        "Referencni_popis": "Referenční popis",
        "Rucni_seskupeni": "Ruční seskupení",
        "Pocet_polozek": "Počet položek",
    }
    for idx, col in enumerate(total_cols):
        key = f"value_{idx}"
        agg_kwargs[key] = pd.NamedAgg(column=col, aggfunc="sum")
        rename_after[key] = col

    grouped = (
        working.groupby("group_key", dropna=False).agg(**agg_kwargs).reset_index().rename(columns={"group_key": "__group_key__"})
    )
    grouped.rename(columns=rename_after, inplace=True)
    if "Ruční seskupení" in grouped.columns:
        grouped["Ruční seskupení"] = grouped["Ruční seskupení"].astype(bool)
    if "Počet položek" in grouped.columns:
        grouped["Počet položek"] = grouped["Počet položek"].astype(int)
    grouped = grouped.sort_values(["__group_order__", "Skupina"]).reset_index(drop=True)

    aggregated_display = grouped.drop(columns=["__group_key__", "__group_order__"], errors="ignore").copy()
    base_cols = [
        col
        for col in [
            "Skupina",
            "Referenční kód",
            "Referenční popis",
            "Počet položek",
            "Ruční seskupení",
        ]
        if col in aggregated_display.columns
    ]
    other_cols = [col for col in aggregated_display.columns if col not in base_cols]
    aggregated_display = aggregated_display[base_cols + other_cols]

    detail_groups: Dict[str, Dict[str, pd.DataFrame]] = {}
    working = working.sort_values(["group_key", "group_order", "item_order"]).reset_index(drop=True)
    for _, summary_row in grouped.iterrows():
        gkey = summary_row.get("__group_key__")
        label = summary_row.get("Skupina", str(gkey))
        detail_df = working[working["group_key"] == gkey].copy()
        detail_cols = [
            "code",
            "description",
            "auto_group_key",
            "auto_group_label",
            "group_label",
            "auto_group_order",
            "group_order",
            "manual_override",
        ]
        detail_cols.extend(total_cols)
        detail_cols_existing = [col for col in detail_cols if col in detail_df.columns]
        detail_display = detail_df[detail_cols_existing].copy()
        detail_display.rename(
            columns={
                "code": "Kód",
                "description": "Popis",
                "auto_group_key": "Návrh kódu skupiny",
                "auto_group_label": "Návrh popisu skupiny",
                "group_label": "Finální skupina",
                "auto_group_order": "Pořadí (návrh)",
                "group_order": "Pořadí (finální)",
                "manual_override": "Ruční změna",
            },
            inplace=True,
        )
        if "Ruční změna" in detail_display.columns:
            detail_display["Ruční změna"] = detail_display["Ruční změna"].astype(bool)
        for col in ("Pořadí (finální)", "Pořadí (návrh)"):
            if col in detail_display.columns:
                detail_display[col] = pd.to_numeric(detail_display[col], errors="coerce")
        summary_display = summary_row.drop(
            labels=[c for c in ["__group_key__", "__group_order__"] if c in summary_row.index]
        ).to_frame().T
        if "Ruční seskupení" in summary_display.columns:
            summary_display["Ruční seskupení"] = summary_display["Ruční seskupení"].astype(bool)
        if "Počet položek" in summary_display.columns:
            summary_display["Počet položek"] = summary_display["Počet položek"].astype(int)
        detail_groups[str(label)] = {"summary": summary_display, "data": detail_display}

    return aggregated_display, detail_groups


def convert_currency_df(
    df: pd.DataFrame, factor: float, skip: Optional[List[str]] = None
) -> pd.DataFrame:
    """Multiply numeric columns by ``factor`` while keeping helper columns intact."""

    if df is None:
        return pd.DataFrame()
    result = df.copy()
    if result.empty:
        return result
    skip_set = set(skip or [])
    numeric_cols = [
        col
        for col in result.select_dtypes(include=[np.number]).columns
        if col not in skip_set and not pd.api.types.is_bool_dtype(result[col])
    ]
    if not numeric_cols or factor == 1.0:
        return result
    for col in numeric_cols:
        result[col] = pd.to_numeric(result[col], errors="coerce") * factor
    return result


def convert_detail_groups(
    groups: Dict[str, Dict[str, pd.DataFrame]],
    factor: float,
    detail_skip: Optional[List[str]] = None,
    summary_skip: Optional[List[str]] = None,
) -> Dict[str, Dict[str, pd.DataFrame]]:
    """Apply currency conversion to grouped detail structures."""

    converted: Dict[str, Dict[str, pd.DataFrame]] = {}
    for label, payload in groups.items():
        data_df = payload.get("data", pd.DataFrame())
        summary_df = payload.get("summary", pd.DataFrame())
        converted[label] = {
            "data": convert_currency_df(data_df, factor, skip=detail_skip),
            "summary": convert_currency_df(summary_df, factor, skip=summary_skip),
        }
    return converted


def validate_totals(df: pd.DataFrame) -> float:
    """Return cumulative absolute difference between summaries and items.

    The check walks the table in order and compares each summary row with the
    sum of preceding item rows until the previous summary. If the last summary
    appears to be the grand total (i.e. it's the largest summary), it is also
    compared against the overall sum of all items. The absolute differences are
    accumulated and returned. If no summary rows exist, returns ``0``."""
    if df is None or df.empty:
        return np.nan
    if "is_summary" not in df.columns:
        return 0.0

    line_tp = coerce_numeric(df.get("total_price", 0)).fillna(0.0)
    sum_tp = coerce_numeric(df.get("summary_total", 0)).fillna(0.0)
    summaries = df["is_summary"].fillna(False).astype(bool).tolist()

    diffs: List[float] = []
    running = 0.0
    total_items = 0.0
    summary_vals: List[float] = []

    for line_val, sum_val, is_sum in zip(line_tp, sum_tp, summaries):
        if not is_sum:
            running += float(line_val)
            total_items += float(line_val)
        else:
            diffs.append(float(sum_val) - running)
            running = 0.0
            summary_vals.append(float(sum_val))

    # If the last summary is the largest, treat it as grand total and compare
    # against all items instead of the running section sum.
    if summary_vals:
        last_val = summary_vals[-1]
        if last_val == max(summary_vals):
            diffs[-1] = last_val - total_items

    return float(sum(abs(d) for d in diffs))

def qa_checks(master: WorkbookData, bids: Dict[str, WorkbookData]) -> Dict[str, Dict[str, pd.DataFrame]]:
    """Return {sheet: {supplier: {"missing": df, "extras": df, "duplicates": df, "total_diff": float}}}"""
    out: Dict[str, Dict[str, Dict[str, pd.DataFrame]]] = {}
    for sheet, mobj in master.sheets.items():
        mtab = mobj.get("table", pd.DataFrame())
        if mtab is None or mtab.empty:
            continue
        mtotal_diff = validate_totals(mtab)
        if "is_summary" in mtab.columns:
            mtab_clean = mtab[~mtab["is_summary"].fillna(False).astype(bool)]
        else:
            mtab_clean = mtab
        mkeys = set(mtab_clean["__key__"].dropna().astype(str))
        per_sheet: Dict[str, Dict[str, pd.DataFrame]] = {}
        # Include master total diff for reference
        per_sheet["Master"] = {
            "missing": pd.DataFrame(columns=["__key__"]),
            "extras": pd.DataFrame(columns=["__key__"]),
            "duplicates": pd.DataFrame(columns=["__key__", "cnt"]),
            "total_diff": mtotal_diff,
        }
        for sup, wb in bids.items():
            tobj = wb.sheets.get(sheet, {})
            ttab = tobj.get("table", pd.DataFrame())
            if ttab is None or ttab.empty:
                miss = pd.DataFrame({"__key__": sorted(mkeys)})
                ext = pd.DataFrame(columns=["__key__"])
                dupl = pd.DataFrame(columns=["__key__", "cnt"])
                total_diff = np.nan
            else:
                total_diff = validate_totals(ttab)
                if "is_summary" in ttab.columns:
                    ttab_clean = ttab[~ttab["is_summary"].fillna(False).astype(bool)]
                else:
                    ttab_clean = ttab
                tkeys_series = ttab_clean["__key__"].dropna().astype(str)
                tkeys = set(tkeys_series)
                miss = pd.DataFrame({"__key__": sorted(mkeys - tkeys)})
                ext = pd.DataFrame({"__key__": sorted(tkeys - mkeys)})
                # duplicates within supplier bid (same key appearing more than once)
                dupl_counts = tkeys_series.value_counts()
                dupl = dupl_counts[dupl_counts > 1].rename_axis("__key__").reset_index(name="cnt")
            per_sheet[sup] = {
                "missing": miss,
                "extras": ext,
                "duplicates": dupl,
                "total_diff": total_diff,
            }
        out[sheet] = per_sheet
    return out

# ------------- Sidebar Inputs -------------

offer_storage = OfferStorage()
stored_master_entries = offer_storage.list_master()
stored_bid_entries = offer_storage.list_bids()

st.sidebar.header("Vstupy")
st.sidebar.caption(
    "Nahrané soubory se automaticky ukládají pro další použití."
)

master_selection = ""
if stored_master_entries:
    master_display_map = {"": "— bez výběru —"}
    master_options = [""]
    for entry in stored_master_entries:
        name = entry["name"]
        timestamp = format_timestamp(entry.get("updated_at"))
        master_options.append(name)
        master_display_map[name] = (
            f"{name} ({timestamp})" if timestamp else name
        )
    master_selection = st.sidebar.selectbox(
        "Uložené Master soubory",
        master_options,
        format_func=lambda value: master_display_map.get(value, value),
    )

uploaded_master = st.sidebar.file_uploader(
    "Master BoQ (.xlsx/.xlsm)", type=["xlsx", "xlsm"], key="master"
)
if uploaded_master is not None:
    offer_storage.save_master(uploaded_master)
    master_file = uploaded_master
else:
    master_file = None
    if master_selection:
        try:
            master_file = offer_storage.load_master(master_selection)
        except FileNotFoundError:
            st.sidebar.warning(
                f"Uložený Master '{master_selection}' se nepodařilo načíst."
            )

bid_files: List[Any] = []
uploaded_bids = st.sidebar.file_uploader(
    "Nabídky dodavatelů (max 7)",
    type=["xlsx", "xlsm"],
    accept_multiple_files=True,
    key="bids",
)
if uploaded_bids:
    uploaded_bids = list(uploaded_bids)
    if len(uploaded_bids) > 7:
        st.sidebar.warning("Zpracuje se pouze prvních 7 souborů.")
        uploaded_bids = uploaded_bids[:7]
    for file_obj in uploaded_bids:
        offer_storage.save_bid(file_obj)
        bid_files.append(file_obj)

selected_stored_bids: List[str] = []
if stored_bid_entries:
    bid_display_map = {}
    bid_options: List[str] = []
    for entry in stored_bid_entries:
        bid_options.append(entry["name"])
        timestamp = format_timestamp(entry.get("updated_at"))
        bid_display_map[entry["name"]] = (
            f"{entry['name']} ({timestamp})" if timestamp else entry["name"]
        )
    selected_stored_bids = st.sidebar.multiselect(
        "Přidat uložené nabídky",
        bid_options,
        format_func=lambda value: bid_display_map.get(value, value),
    )
    for name in selected_stored_bids:
        try:
            bid_files.append(offer_storage.load_bid(name))
        except FileNotFoundError:
            st.sidebar.warning(
                f"Uloženou nabídku '{name}' se nepodařilo načíst."
            )

if len(bid_files) > 7:
    st.sidebar.warning("Bylo vybráno více než 7 nabídek, zpracuje se prvních 7.")
    bid_files = bid_files[:7]

currency = st.sidebar.text_input("Popisek měny", value="CZK")

stored_master_entries = offer_storage.list_master()
stored_bid_entries = offer_storage.list_bids()

with st.sidebar.expander("Správa uložených souborů"):
    st.caption(
        "Nahraj nový soubor se stejným názvem, aby se nahradil uložený."
    )
    if stored_master_entries:
        st.markdown("**Master**")
        for entry in stored_master_entries:
            label = entry["name"]
            timestamp = format_timestamp(entry.get("updated_at"))
            display = f"{label} — {timestamp}" if timestamp else label
            cols = st.columns([3, 1])
            cols[0].write(display)
            if cols[1].button(
                "Smazat",
                key=make_widget_key("delete_master", label),
            ):
                offer_storage.delete_master(label)
                st.experimental_rerun()
    else:
        st.caption("Žádný uložený Master soubor.")

    if stored_bid_entries:
        st.markdown("**Nabídky**")
        for entry in stored_bid_entries:
            label = entry["name"]
            timestamp = format_timestamp(entry.get("updated_at"))
            display = f"{label} — {timestamp}" if timestamp else label
            cols = st.columns([3, 1])
            cols[0].write(display)
            if cols[1].button(
                "Smazat",
                key=make_widget_key("delete_bid", label),
            ):
                offer_storage.delete_bid(label)
                st.experimental_rerun()
    else:
        st.caption("Žádné uložené nabídky.")

if not master_file:
    st.info("➡️ Nahraj Master BoQ v levém panelu nebo vyber uložený soubor.")
    st.stop()

# Determine sheet names without loading all sheets
master_xl = pd.ExcelFile(master_file)
all_sheets = master_xl.sheet_names

# User selections for comparison and overview
compare_sheets = st.sidebar.multiselect("Listy pro porovnání", all_sheets, default=all_sheets)
default_overview = (
    "Přehled_dílčí kapitoly"
    if "Přehled_dílčí kapitoly" in all_sheets
    else (all_sheets[0] if all_sheets else "")
)
overview_sheet = st.sidebar.selectbox(
    "List pro rekapitulaci",
    all_sheets,
    index=all_sheets.index(default_overview) if default_overview in all_sheets else 0,
)

# Read master only for selected comparison sheets
master_file.seek(0)
master_wb = read_workbook(master_file, limit_sheets=compare_sheets)

# If overview sheet not among comparison sheets, load separately
if overview_sheet in compare_sheets:
    master_overview_wb = WorkbookData(
        name=master_wb.name, sheets={overview_sheet: master_wb.sheets[overview_sheet]}
    )
else:
    master_file.seek(0)
    master_overview_wb = read_workbook(master_file, limit_sheets=[overview_sheet])

# Read bids for comparison sheets and overview sheet separately
bids_dict: Dict[str, WorkbookData] = {}
bids_overview_dict: Dict[str, WorkbookData] = {}
if bid_files:
    if len(bid_files) > 7:
        st.sidebar.warning("Zpracuje se pouze prvních 7 souborů.")
        bid_files = bid_files[:7]
    for i, f in enumerate(bid_files, start=1):
        name = getattr(f, "name", f"Bid{i}")
        f.seek(0)
        wb_comp = read_workbook(f, limit_sheets=compare_sheets)
        apply_master_mapping(master_wb, wb_comp)
        bids_dict[name] = wb_comp

        if overview_sheet in compare_sheets:
            wb_over = WorkbookData(
                name=wb_comp.name, sheets={overview_sheet: wb_comp.sheets.get(overview_sheet, {})}
            )
        else:
            f.seek(0)
            wb_over = read_workbook(f, limit_sheets=[overview_sheet])
            apply_master_mapping(master_overview_wb, wb_over)
        bids_overview_dict[name] = wb_over

# Manage supplier aliases and colors
display_names: Dict[str, str] = {}
color_map: Dict[str, str] = {}
if "supplier_metadata" not in st.session_state:
    st.session_state["supplier_metadata"] = {}
metadata: Dict[str, Dict[str, str]] = st.session_state["supplier_metadata"]
current_suppliers = list(bids_dict.keys())
for obsolete in list(metadata.keys()):
    if obsolete not in current_suppliers:
        metadata.pop(obsolete, None)

palette = (
    px.colors.qualitative.Plotly
    + px.colors.qualitative.Safe
    + px.colors.qualitative.Pastel
)

if current_suppliers:
    for idx, raw_name in enumerate(current_suppliers):
        entry = metadata.get(raw_name, {})
        if not entry.get("alias"):
            entry["alias"] = supplier_default_alias(raw_name)
        if not entry.get("color"):
            entry["color"] = palette[idx % len(palette)]
        metadata[raw_name] = entry

    with st.sidebar.expander("Alias a barvy dodavatelů", expanded=True):
        st.caption("Zkrácený název a barva se promítnou do tabulek a grafů.")
        for raw_name in current_suppliers:
            entry = metadata.get(raw_name, {})
            alias_value = st.text_input(
                f"Alias pro {raw_name}",
                value=entry.get("alias", supplier_default_alias(raw_name)),
                key=sanitize_key("alias", raw_name),
            )
            alias_clean = alias_value.strip() or supplier_default_alias(raw_name)
            color_default = entry.get("color", "#1f77b4")
            color_value = st.color_picker(
                f"Barva — {alias_clean}",
                value=color_default,
                key=sanitize_key("color", raw_name),
            )
            metadata[raw_name]["alias"] = alias_clean
            metadata[raw_name]["color"] = color_value or color_default

    display_names = {raw: metadata[raw]["alias"] for raw in current_suppliers}
    display_names = ensure_unique_aliases(display_names, RESERVED_ALIAS_NAMES)
    for raw, display_alias in display_names.items():
        metadata[raw]["alias_display"] = display_alias
    st.session_state["supplier_metadata"] = metadata
    color_map = {display_names[raw]: metadata[raw]["color"] for raw in current_suppliers}

chart_color_map = color_map.copy()
chart_color_map.setdefault("Master", "#636EFA")

default_threshold_range = st.session_state.get("compare_threshold_range", (-10.0, 10.0))
min_threshold, max_threshold = st.sidebar.slider(
    "Rozmezí odchylky vs Master (%)",
    min_value=-200.0,
    max_value=200.0,
    value=default_threshold_range,
    step=0.5,
    help="Rozsah procentuální odchylky, který se považuje za přijatelný. Hodnoty mimo rozsah budou zvýrazněny.",
)
st.session_state["compare_threshold_range"] = (min_threshold, max_threshold)

ensure_exchange_rate_state()

# ------------- Tabs -------------
tab_data, tab_compare, tab_summary, tab_rekap, tab_dashboard, tab_qa = st.tabs([
    "📑 Mapování",
    "⚖️ Porovnání",
    "📋 Celkový přehled",
    "📊 Rekapitulace",
    "📈 Dashboard",
    "🧪 QA kontroly",
])

with tab_data:
    master_changed = mapping_ui(
        "Master",
        master_wb,
        minimal_sheets=[overview_sheet] if overview_sheet in compare_sheets else None,
        section_id="master",
    )
    if master_changed:
        for wb in bids_dict.values():
            apply_master_mapping(master_wb, wb)
        if overview_sheet in compare_sheets:
            for wb in bids_overview_dict.values():
                apply_master_mapping(master_wb, wb)
    if overview_sheet not in compare_sheets:
        with st.expander("Mapování — Master rekapitulace", expanded=False):
            master_over_changed = mapping_ui(
                "Master rekapitulace", master_overview_wb, minimal=True, section_id="master_recap"
            )
        if master_over_changed:
            for wb in bids_overview_dict.values():
                apply_master_mapping(master_overview_wb, wb)
    if bids_dict:
        for sup_name, wb in bids_dict.items():
            alias = display_names.get(sup_name, sup_name)
            with st.expander(f"Mapování — {alias}", expanded=False):
                mapping_ui(
                    alias,
                    wb,
                    minimal_sheets=[overview_sheet] if overview_sheet in compare_sheets else None,
                    section_id=f"bid_{sup_name}",
                )
        if overview_sheet not in compare_sheets:
            for sup_name, wb in bids_overview_dict.items():
                alias = display_names.get(sup_name, sup_name)
                with st.expander(f"Mapování rekapitulace — {alias}", expanded=False):
                    mapping_ui(
                        f"{alias} rekapitulace",
                        wb,
                        minimal=True,
                        section_id=f"bid_recap_{sup_name}",
                    )
    st.success("Mapování připraveno. Přepni na záložku **Porovnání**.")

# Pre-compute comparison results for reuse in tabs (after mapping)
compare_results: Dict[str, pd.DataFrame] = {}
if bids_dict:
    raw_compare_results = compare(master_wb, bids_dict, join_mode="auto")
    compare_results = {
        sheet: rename_comparison_columns(df, display_names) for sheet, df in raw_compare_results.items()
    }

comparison_datasets: Dict[str, ComparisonDataset] = {}
if compare_results:
    comparison_datasets = build_comparison_datasets(compare_results)

# Pre-compute rekapitulace results to avoid repeated work in tabs (after mapping)
recap_results: Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame] = (
    pd.DataFrame(),
    pd.DataFrame(),
    pd.DataFrame(),
    pd.DataFrame(),
    pd.DataFrame(),
)
if bids_overview_dict:
    recap_results = overview_comparison(
        master_overview_wb, bids_overview_dict, overview_sheet
    )
    if display_names:
        recap_results = tuple(
            rename_total_columns(df, display_names) if i < 3 else df
            for i, df in enumerate(recap_results)
        )
        sections_df, indirect_df, added_df, missing_df, indirect_total = recap_results
        if not missing_df.empty and "missing_in" in missing_df.columns:
            missing_df["missing_in"] = missing_df["missing_in"].map(display_names).fillna(
                missing_df["missing_in"]
            )
        if not indirect_total.empty and "supplier" in indirect_total.columns:
            indirect_total["supplier"] = indirect_total["supplier"].map(display_names).fillna(
                indirect_total["supplier"]
            )
        recap_results = (sections_df, indirect_df, added_df, missing_df, indirect_total)

with tab_compare:
    if not bids_dict:
        st.info("Nahraj alespoň jednu nabídku dodavatele v levém panelu.")
    elif not comparison_datasets:
        st.info("Nebyla nalezena data pro porovnání. Zkontroluj mapování nebo vyber jiné listy.")
    else:
        available_sheets = [
            sheet for sheet, dataset in comparison_datasets.items() if not dataset.analysis_df.empty
        ]
        if not available_sheets:
            st.info("Listy určené k porovnání jsou prázdné. Zkontroluj zdrojová data.")
        else:
            default_sheet = available_sheets[0]
            selected_sheet = st.selectbox(
                "Vyber list pro analýzu",
                available_sheets,
                index=available_sheets.index(default_sheet) if default_sheet in available_sheets else 0,
                key="compare_sheet_select",
            )
            dataset = comparison_datasets.get(selected_sheet)
            if dataset is None or dataset.analysis_df.empty:
                st.warning("Vybraný list neobsahuje žádné položky k porovnání.")
            else:
                st.subheader(f"List: {selected_sheet}")
                threshold_min, threshold_max = st.session_state.get(
                    "compare_threshold_range", (-10.0, 10.0)
                )
                working = dataset.analysis_df.copy()

                if dataset.percent_columns:
                    pct_matrix = working[dataset.percent_columns]
                    above_mask = pct_matrix.gt(threshold_max)
                    below_mask = pct_matrix.lt(threshold_min)
                    working["__above_threshold__"] = above_mask.any(axis=1)
                    working["__below_threshold__"] = below_mask.any(axis=1)
                    working["__outside_threshold__"] = working["__above_threshold__"] | working["__below_threshold__"]
                else:
                    working["__above_threshold__"] = False
                    working["__below_threshold__"] = False
                    working["__outside_threshold__"] = False

                lowest_supplier_col = "LOWEST supplier" if "LOWEST supplier" in working.columns else None
                lowest_supplier_options = []
                if lowest_supplier_col:
                    lowest_supplier_options = sorted(
                        {
                            str(val)
                            for val in working[lowest_supplier_col].dropna().astype(str).tolist()
                            if str(val).strip()
                        },
                        key=natural_sort_key,
                    )

                search_key = make_widget_key("compare_search", selected_sheet)
                section_key = make_widget_key("compare_sections", selected_sheet)
                lowest_key = make_widget_key("compare_lowest", selected_sheet)
                limit_key = make_widget_key("compare_limit", selected_sheet)

                filter_top = st.container()
                with filter_top:
                    col_search, col_section, col_lowest, col_limit = st.columns([3, 3, 2, 1])
                    search_query = col_search.text_input(
                        "Vyhledávání (kód, popis, oddíl)",
                        key=search_key,
                        placeholder="např. 3.1 dveře",
                    )
                    section_selection = col_section.multiselect(
                        "Oddíly",
                        dataset.section_labels,
                        key=section_key,
                    )
                    lowest_selection = col_lowest.multiselect(
                        "Dodavatel s nejnižší cenou",
                        lowest_supplier_options,
                        key=lowest_key,
                    )
                    max_rows = col_limit.number_input(
                        "Limit řádků",
                        min_value=10,
                        max_value=5000,
                        value=200,
                        step=10,
                        key=limit_key,
                    )

                above_key = make_widget_key("compare_above_only", selected_sheet)
                below_key = make_widget_key("compare_below_only", selected_sheet)
                view_mode_key = make_widget_key("compare_view_mode", selected_sheet)

                filter_bottom = st.container()
                with filter_bottom:
                    col_above, col_below, col_view = st.columns([1, 1, 2])
                    show_above_only = col_above.checkbox(
                        "Jen nad horním limitem",
                        key=above_key,
                    )
                    show_below_only = col_below.checkbox(
                        "Jen pod dolním limitem",
                        key=below_key,
                    )
                    view_mode = col_view.radio(
                        "Rychlý přehled",
                        options=[
                            "Vše",
                            "Top 10 odchylek",
                            "Položky mimo rozsah",
                            "Položky bez nabídky",
                        ],
                        index=0,
                        horizontal=True,
                        key=view_mode_key,
                    )

                filtered = working.copy()
                if search_query:
                    normalized = normalize_text(search_query)
                    tokens = [token for token in re.split(r"\s+", normalized) if token]
                    if tokens:
                        search_series = filtered.get("__search_token__", pd.Series("", index=filtered.index))
                        mask = pd.Series(True, index=filtered.index, dtype=bool)
                        for token in tokens:
                            mask &= search_series.str.contains(re.escape(token), na=False)
                        filtered = filtered.loc[mask]

                if section_selection:
                    filtered = filtered[filtered["Oddíl"].isin(section_selection)]

                if lowest_selection and lowest_supplier_col:
                    filtered = filtered[filtered[lowest_supplier_col].isin(lowest_selection)]

                if show_above_only:
                    filtered = filtered[filtered["__above_threshold__"]]
                if show_below_only:
                    filtered = filtered[filtered["__below_threshold__"]]

                if view_mode == "Top 10 odchylek":
                    filtered = filtered.sort_values("__abs_diff_max__", ascending=False)
                    filtered = filtered.head(min(int(max_rows), 10))
                elif view_mode == "Položky mimo rozsah":
                    filtered = filtered[filtered["__outside_threshold__"]]
                elif view_mode == "Položky bez nabídky":
                    filtered = filtered[filtered["__missing_offer__"]]

                if view_mode != "Top 10 odchylek":
                    filtered = filtered.head(int(max_rows))

                if "__row_order__" in filtered.columns:
                    filtered = filtered.sort_values("__row_order__")

                visible = filtered.copy()
                visible_count = len(visible)

                metrics_cols = st.columns(4)
                outside_count = int(visible.get("__outside_threshold__", pd.Series(False, index=visible.index)).sum())
                outside_ratio = (outside_count / visible_count * 100.0) if visible_count else 0.0

                if dataset.percent_columns and not visible.empty:
                    pct_values = (
                        visible[dataset.percent_columns]
                        .replace([np.inf, -np.inf], np.nan)
                        .abs()
                        .to_numpy()
                        .ravel()
                    )
                    pct_values = pct_values[~np.isnan(pct_values)]
                else:
                    pct_values = np.array([], dtype=float)
                avg_abs_pct = float(np.mean(pct_values)) if pct_values.size else 0.0
                median_abs_pct = float(np.median(pct_values)) if pct_values.size else 0.0

                financial_series = pd.to_numeric(
                    visible.get("__abs_diff_sum__", pd.Series(0.0, index=visible.index)),
                    errors="coerce",
                ).fillna(0.0)
                financial_impact = float(financial_series.sum())

                missing_series = visible.get("__missing_any__", pd.Series(False, index=visible.index)).astype(bool)
                missing_count = int(missing_series.sum())
                missing_ratio = (missing_count / visible_count * 100.0) if visible_count else 0.0

                metrics_cols[0].metric(
                    "Položky mimo rozsah",
                    f"{outside_count}",
                    f"{outside_ratio:.1f}% výběru",
                )
                metrics_cols[1].metric(
                    "Průměrná | medián odchylka",
                    f"{avg_abs_pct:.2f} %",
                    f"medián {median_abs_pct:.2f} %",
                )
                metrics_cols[2].metric(
                    "Finanční dopad",
                    f"{format_number(financial_impact)} {currency}",
                )
                metrics_cols[3].metric(
                    "Neúplné položky",
                    f"{missing_ratio:.1f} %",
                    f"{missing_count} ks",
                )

                value_totals: List[Tuple[str, float]] = []
                for col in dataset.value_columns:
                    if col not in visible.columns:
                        continue
                    total_sum = pd.to_numeric(visible[col], errors="coerce").sum(skipna=True)
                    value_totals.append((col.replace(" total", ""), float(total_sum)))
                if value_totals:
                    totals_df = pd.DataFrame(value_totals, columns=["Dodavatel", "Součet"])
                    st.markdown("**Součty dle aktuálního filtru:**")
                    show_df(totals_df)
                    try:
                        fig_totals = px.bar(
                            totals_df,
                            x="Dodavatel",
                            y="Součet",
                            color="Dodavatel",
                            color_discrete_map=chart_color_map,
                            title=f"Součty dle filtru — {selected_sheet} ({currency})",
                        )
                        fig_totals.update_layout(showlegend=False)
                        fig_totals.update_yaxes(title=f"{currency}")
                        st.plotly_chart(fig_totals, use_container_width=True)
                    except Exception:
                        pass

                if visible.empty:
                    st.info("Žádná položka neodpovídá zadaným filtrům.")
                else:
                    hidden_cols = [col for col in visible.columns if str(col).startswith("__")]
                    display_base = visible.drop(columns=hidden_cols, errors="ignore")
                    display_base = display_base.drop(columns=["__key__"], errors="ignore")
                    display_ready = rename_value_columns_for_display(
                        display_base.copy(), f" — {currency}"
                    )
                    show_df(display_ready)

                    safe_sheet = re.sub(r"[^0-9A-Za-z]+", "_", selected_sheet).strip("_") or "porovnani"
                    export_csv = display_ready.to_csv(index=False).encode("utf-8-sig")
                    export_container = st.container()
                    with export_container:
                        col_csv, col_xlsx = st.columns(2)
                        col_csv.download_button(
                            "⬇️ Export CSV",
                            data=export_csv,
                            file_name=f"{safe_sheet}_filter.csv",
                            mime="text/csv",
                        )
                        excel_buffer = io.BytesIO()
                        with pd.ExcelWriter(excel_buffer, engine="openpyxl") as writer:
                            display_ready.to_excel(writer, index=False, sheet_name="Porovnani")
                        excel_buffer.seek(0)
                        col_xlsx.download_button(
                            "⬇️ Export XLSX",
                            data=excel_buffer.getvalue(),
                            file_name=f"{safe_sheet}_filter.xlsx",
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        )

                    with st.expander("Detail položky", expanded=True):
                        detail_map: Dict[Any, str] = {}
                        for idx, row in visible.iterrows():
                            code_text = str(row.get("code", "") or "").strip()
                            desc_text = str(row.get("description", "") or "").strip()
                            section_text = str(row.get("Oddíl", "") or "").strip()
                            label_parts = [part for part in [code_text, desc_text] if part]
                            label = " — ".join(label_parts) if label_parts else f"Položka {idx}"
                            if section_text:
                                label = f"{label} [{section_text}]"
                            detail_map[idx] = label

                        selected_index = st.selectbox(
                            "Vyber položku",
                            options=list(visible.index),
                            format_func=lambda idx: detail_map.get(idx, str(idx)),
                            key=f"compare_detail_{selected_sheet}",
                        )

                        detail_row = visible.loc[selected_index]
                        detail_key = detail_row.get("__key__", selected_index)
                        detail_long = dataset.long_df[dataset.long_df["__key__"] == detail_key].copy()

                        info_cols = st.columns(4)
                        info_cols[0].markdown(f"**Kód:** {detail_row.get('code', '—') or '—'}")
                        info_cols[1].markdown(f"**Oddíl:** {detail_row.get('Oddíl', '—') or '—'}")
                        info_cols[2].markdown(f"**Jednotka:** {detail_row.get('unit', '—') or '—'}")
                        qty_value = detail_row.get("quantity")
                        qty_numeric = pd.to_numeric(pd.Series([qty_value]), errors="coerce").iat[0]
                        qty_text = format_number(float(qty_numeric)) if pd.notna(qty_numeric) else "—"
                        info_cols[3].markdown(f"**Množství Master:** {qty_text}")

                        if lowest_supplier_col:
                            lowest_value = detail_row.get(lowest_supplier_col)
                            if pd.notna(lowest_value):
                                st.caption(f"Nejnižší cena: {lowest_value}")

                        detail_chart_df = detail_long.copy()
                        detail_chart_df["total"] = pd.to_numeric(
                            detail_chart_df["total"], errors="coerce"
                        )
                        detail_chart_df = detail_chart_df.dropna(subset=["total"])
                        if not detail_chart_df.empty:
                            if dataset.supplier_order:
                                detail_chart_df["supplier"] = pd.Categorical(
                                    detail_chart_df["supplier"],
                                    categories=dataset.supplier_order,
                                    ordered=True,
                                )
                                detail_chart_df = detail_chart_df.sort_values("supplier")
                            fig_detail = px.bar(
                                detail_chart_df,
                                x="supplier",
                                y="total",
                                color="supplier",
                                color_discrete_map=chart_color_map,
                                title=f"Cenové srovnání — {detail_map.get(selected_index, selected_sheet)}",
                            )
                            fig_detail.update_yaxes(title=f"Cena ({currency})")
                            fig_detail.update_layout(xaxis_title="Dodavatel")
                            st.plotly_chart(fig_detail, use_container_width=True)

                        detail_table = detail_long.copy()
                        detail_table["total"] = pd.to_numeric(detail_table["total"], errors="coerce")
                        detail_table["difference_vs_master"] = pd.to_numeric(
                            detail_table["difference_vs_master"], errors="coerce"
                        )
                        detail_table["pct_vs_master"] = pd.to_numeric(
                            detail_table["pct_vs_master"], errors="coerce"
                        )
                        detail_display = detail_table[[
                            "supplier",
                            "total",
                            "difference_vs_master",
                            "pct_vs_master",
                        ]].rename(
                            columns={
                                "supplier": "Dodavatel",
                                "total": f"Cena ({currency})",
                                "difference_vs_master": f"Rozdíl vs Master ({currency})",
                                "pct_vs_master": "Odchylka (%)",
                            }
                        )
                        for column in detail_display.columns:
                            if column == "Dodavatel":
                                continue
                            if column == "Odchylka (%)":
                                detail_display[column] = detail_display[column].apply(
                                    lambda value: f"{format_number(value)} %" if pd.notna(value) else ""
                                )
                            else:
                                detail_display[column] = detail_display[column].apply(format_number)
                        st.dataframe(detail_display, use_container_width=True, hide_index=True)

with tab_summary:
    if not bids_dict:
        st.info("Nahraj alespoň jednu nabídku dodavatele v levém panelu.")
    else:
        results = compare_results

        summary_df = summarize(results)
        if not summary_df.empty:
            st.markdown("### 📌 Souhrn po listech")
            ctrl_dir, ctrl_rate = st.columns([2, 1])
            with ctrl_dir:
                conversion_direction = st.radio(
                    "Směr konverze",
                    ["CZK → EUR", "EUR → CZK"],
                    index=0,
                    horizontal=True,
                )
            with ctrl_rate:
                rate_label = (
                    "Kurz (CZK za 1 EUR)"
                    if conversion_direction == "CZK → EUR"
                    else "Kurz (CZK za 1 EUR)"
                )
                exchange_rate = st.number_input(
                    rate_label,
                    min_value=0.0001,
                    value=float(st.session_state[EXCHANGE_RATE_STATE_KEY]),
                    step=0.1,
                    format="%.4f",
                    key=EXCHANGE_RATE_WIDGET_KEYS["summary"],
                )
                exchange_rate = update_exchange_rate_shared(exchange_rate)

            st.caption(
                "Tabulka zobrazuje původní hodnoty v CZK. Přepočet níže pracuje pouze se souhrnnými hodnotami pro rychlost."
            )
            show_df(summary_df)

            target_currency = "EUR" if conversion_direction == "CZK → EUR" else "CZK"
            conversion_factor = (1.0 / exchange_rate) if conversion_direction == "CZK → EUR" else exchange_rate
            value_cols = [c for c in summary_df.columns if c != "sheet"]
            summary_converted_df = summary_df.copy()
            for col in value_cols:
                summary_converted_df[col] = (
                    pd.to_numeric(summary_converted_df[col], errors="coerce") * conversion_factor
                )

            st.markdown(f"**Souhrn v {target_currency}:**")
            show_df(summary_converted_df)

            supplier_totals = {}
            for col in summary_df.columns:
                if str(col).endswith(" total") and not str(col).startswith("__present__"):
                    supplier = col.replace(" total", "")
                    supplier_totals[supplier] = pd.to_numeric(
                        summary_df[col], errors="coerce"
                    ).sum()
            grand_df = pd.DataFrame(
                {"supplier": list(supplier_totals.keys()), "grand_total": list(supplier_totals.values())}
            )
            grand_converted_df = grand_df.copy()
            if not grand_converted_df.empty:
                grand_converted_df["grand_total"] = (
                    pd.to_numeric(grand_converted_df["grand_total"], errors="coerce") * conversion_factor
                )

            base_totals_col, converted_totals_col = st.columns(2)
            with base_totals_col:
                st.markdown("**Celkové součty (CZK):**")
                show_df(grand_df)
            with converted_totals_col:
                st.markdown(f"**Celkové součty ({target_currency}):**")
                show_df(grand_converted_df)

            if not grand_df.empty:
                try:
                    fig = px.bar(
                        grand_df,
                        x="supplier",
                        y="grand_total",
                        color="supplier",
                        color_discrete_map=chart_color_map,
                        title=f"Celkové součty ({currency})",
                    )
                    fig.update_layout(showlegend=False)
                    st.plotly_chart(fig, use_container_width=True)
                except Exception:
                    show_df(grand_df)

with tab_rekap:
    if not bids_overview_dict:
        st.info("Nahraj alespoň jednu nabídku dodavatele v levém panelu.")
    else:
        sections_df, indirect_df, added_df, missing_df, indirect_total = recap_results
        if (
            sections_df.empty
            and indirect_df.empty
            and added_df.empty
            and missing_df.empty
        ):
            st.info(f"List '{overview_sheet}' neobsahuje data pro porovnání.")
        else:
            st.subheader(f"Souhrnný list: {overview_sheet}")

            ctrl_dir, ctrl_rate = st.columns([2, 1])
            with ctrl_dir:
                conversion_direction = st.radio(
                    "Směr přepočtu",
                    ["CZK → EUR", "EUR → CZK"],
                    index=0,
                    horizontal=True,
                )
            with ctrl_rate:
                exchange_rate = st.number_input(
                    "Kurz (CZK za 1 EUR)",
                    min_value=0.0001,
                    value=float(st.session_state[EXCHANGE_RATE_STATE_KEY]),
                    step=0.1,
                    format="%.4f",
                    key=EXCHANGE_RATE_WIDGET_KEYS["recap"],
                )
                exchange_rate = update_exchange_rate_shared(exchange_rate)

            base_currency = "CZK" if conversion_direction == "CZK → EUR" else "EUR"
            target_currency = "EUR" if conversion_direction == "CZK → EUR" else "CZK"
            conversion_factor = (
                1.0 / exchange_rate if conversion_direction == "CZK → EUR" else exchange_rate
            )
            st.caption(
                f"Hodnoty jsou nejprve zobrazeny v {base_currency}. Přepočet používá kurz 1 EUR = {exchange_rate:.4f} CZK a uplatňuje se pouze na první dvě tabulky."
            )

            # Combine hlavní, vedlejší i dodatečné položky pro interaktivní přehledy,
            # aby byly dostupné ve výběrové tabulce i v součtech dle kódů.
            section_frames: List[pd.DataFrame] = [
                df.copy()
                for df in (sections_df, indirect_df, added_df)
                if isinstance(df, pd.DataFrame) and not df.empty
            ]
            if section_frames:
                working_sections = (
                    pd.concat(section_frames, axis=0, ignore_index=False, sort=False)
                    .sort_index()
                    .reset_index(drop=True)
                )
            else:
                working_sections = sections_df.copy()
            if not working_sections.empty:
                working_sections["__code_token__"] = working_sections["code"].map(
                    extract_code_token
                )
                working_sections["__norm_desc__"] = working_sections["description"].map(
                    normalize_text
                )
            value_cols = [
                c
                for c in working_sections.columns
                if str(c).endswith(" total") and not str(c).startswith("__present__")
            ]

            def sum_for_mask(mask: pd.Series, absolute: bool = False) -> pd.Series:
                if value_cols and not working_sections.empty and mask.any():
                    subset = working_sections.loc[mask, value_cols].apply(
                        pd.to_numeric, errors="coerce"
                    )
                    if absolute:
                        subset = subset.abs()
                    return subset.sum(skipna=True)
                return pd.Series(0.0, index=value_cols, dtype=float)

            st.markdown("### Rekapitulace finančních nákladů stavby")
            main_detail = pd.DataFrame()
            main_detail_display_base = pd.DataFrame()
            main_detail_display_converted = pd.DataFrame()
            summary_display = pd.DataFrame()
            summary_display_converted = pd.DataFrame()
            chart_df = pd.DataFrame()
            fig_recap = None
            positive_tokens = {
                str(item.get("code_token", ""))
                for item in RECAP_CATEGORY_CONFIG
                if item.get("code_token") and not item.get("is_deduction")
            }
            deduction_tokens = {
                str(item.get("code_token", ""))
                for item in RECAP_CATEGORY_CONFIG
                if item.get("code_token") and item.get("is_deduction")
            }
            if not working_sections.empty and value_cols:
                working_sections["__canonical_desc__"] = (
                    working_sections.get("__norm_desc__", pd.Series("", index=working_sections.index))
                    .astype(str)
                    .map(lambda text: re.sub(r"[^0-9a-z]+", "", text))
                )

                def canonical_label(text: Any) -> str:
                    return re.sub(r"[^0-9a-z]+", "", normalize_text(text))

                available_mask = pd.Series(True, index=working_sections.index)
                recap_rows: List[Dict[str, Any]] = []
                for item in RECAP_CATEGORY_CONFIG:
                    code_token = str(item.get("code_token", "") or "").strip()
                    match_label = item.get("match_label", "")
                    fallback_label = item.get("fallback_label", match_label)
                    target_key = canonical_label(match_label)
                    mask = available_mask.copy()
                    if code_token:
                        mask = mask & (working_sections["__code_token__"] == code_token)
                    if target_key:
                        canon_series = working_sections["__canonical_desc__"].astype(str)
                        exact_mask = mask & (canon_series == target_key)
                        if exact_mask.any():
                            mask = exact_mask
                        else:
                            partial_mask = mask & canon_series.str.contains(target_key, na=False)
                            if partial_mask.any():
                                mask = partial_mask
                    if mask.any():
                        sums = sum_for_mask(mask)
                        available_mask.loc[mask] = False
                    else:
                        sums = pd.Series(np.nan, index=value_cols, dtype=float)
                    codes: List[str] = []
                    if mask.any() and "code" in working_sections.columns:
                        raw_codes = working_sections.loc[mask, "code"]
                        cleaned_codes: List[str] = []
                        for val in raw_codes:
                            text = str(val).strip()
                            if text and text.lower() != "nan":
                                cleaned_codes.append(text)
                        codes = sorted(set(cleaned_codes), key=natural_sort_key)
                    display_label = fallback_label
                    if mask.any() and "description" in working_sections.columns:
                        desc_series = working_sections.loc[mask, "description"].astype(str)
                        display_label = next(
                            (val.strip() for val in desc_series if val and val.strip()),
                            fallback_label,
                        )
                    recap_row: Dict[str, Any] = {
                        "č": ", ".join(codes),
                        "Položka": display_label,
                    }
                    for col in value_cols:
                        recap_row[col] = sums.get(col, np.nan)
                    recap_rows.append(recap_row)
                if recap_rows:
                    main_detail = pd.DataFrame(recap_rows)
                    for col in value_cols:
                        if col in main_detail.columns:
                            main_detail[col] = pd.to_numeric(main_detail[col], errors="coerce")
                    main_detail_display_base = rename_value_columns_for_display(
                        main_detail.copy(), f" — CELKEM {base_currency}"
                    )
                    show_df(main_detail_display_base)
                    converted_main = main_detail.copy()
                    for col in value_cols:
                        if col in converted_main.columns:
                            converted_main[col] = (
                                pd.to_numeric(converted_main[col], errors="coerce")
                                * conversion_factor
                            )
                    st.markdown(f"**Rekapitulace v {target_currency}:**")
                    main_detail_display_converted = rename_value_columns_for_display(
                        converted_main, f" — CELKEM {target_currency}"
                    )
                    show_df(main_detail_display_converted)
                else:
                    st.info("V datech se nepodařilo najít požadované položky rekapitulace.")
            else:
                st.info("Pro zobrazení rekapitulace finančních nákladů je potřeba načíst data z listu.")

            st.markdown("### Souhrn hlavních položek a vedlejších nákladů")

            def positive_recap_sum() -> pd.Series:
                if main_detail.empty:
                    if not positive_tokens:
                        return pd.Series(0.0, index=value_cols, dtype=float)
                    base_sum = sum_for_mask(
                        working_sections["__code_token__"].isin(positive_tokens)
                    )
                    return base_sum.reindex(value_cols, fill_value=0.0)
                numeric_cols = [col for col in value_cols if col in main_detail.columns]
                if not numeric_cols:
                    return pd.Series(0.0, index=value_cols, dtype=float)
                numeric_values = main_detail[numeric_cols].apply(
                    pd.to_numeric, errors="coerce"
                )
                result = numeric_values.clip(lower=0).sum(skipna=True)
                return result.reindex(value_cols, fill_value=0.0)

            plus_sum = positive_recap_sum()
            deduction_sum = sum_for_mask(
                working_sections["__code_token__"].isin(deduction_tokens), absolute=True
            )
            net_sum = plus_sum - deduction_sum

            indirect_sum = pd.Series(0.0, index=value_cols, dtype=float)
            if not indirect_df.empty and value_cols:
                for col in value_cols:
                    if col in indirect_df.columns:
                        indirect_sum[col] = pd.to_numeric(
                            indirect_df[col], errors="coerce"
                        ).sum()
            ratio_sum = pd.Series(np.nan, index=value_cols, dtype=float)
            for col in value_cols:
                base_val = net_sum.get(col)
                indirect_val = indirect_sum.get(col)
                if pd.notna(base_val) and base_val != 0:
                    ratio_sum[col] = (indirect_val / base_val) * 100 if pd.notna(indirect_val) else np.nan

            if deduction_tokens:
                formatted_tokens = ", ".join(
                    f"{token}." if str(token).isdigit() else str(token)
                    for token in sorted(deduction_tokens)
                )
                deduction_label = f"Součet odpočtů ({formatted_tokens})"
            else:
                deduction_label = "Součet odpočtů"
            summary_rows = [
                ("Součet kladných položek rekapitulace", "CZK", plus_sum),
                (deduction_label, "CZK", deduction_sum),
                ("Cena po odečtech", "CZK", net_sum),
                ("Vedlejší rozpočtové náklady", "CZK", indirect_sum),
                ("Podíl vedlejších nákladů (%)", "%", ratio_sum),
            ]
            summary_records: List[Dict[str, Any]] = []
            for label, unit, values in summary_rows:
                row: Dict[str, Any] = {"Ukazatel": label, "Jednotka": unit}
                if isinstance(values, pd.Series):
                    working_values = values.reindex(value_cols)
                else:
                    working_values = pd.Series(np.nan, index=value_cols, dtype=float)
                for col in value_cols:
                    row[col] = working_values.get(col, np.nan)
                summary_records.append(row)
            summary_base = pd.DataFrame(summary_records)
            if not summary_base.empty:
                summary_display = rename_value_columns_for_display(summary_base.copy(), "")
                show_df(summary_display)
                summary_converted = summary_base.copy()
                currency_mask = summary_converted["Jednotka"].str.upper() == "CZK"
                for col in value_cols:
                    summary_converted.loc[currency_mask, col] = (
                        pd.to_numeric(summary_converted.loc[currency_mask, col], errors="coerce")
                        * conversion_factor
                    )
                summary_converted.loc[currency_mask, "Jednotka"] = target_currency
                st.markdown(f"**Souhrn v {target_currency}:**")
                summary_display_converted = rename_value_columns_for_display(
                    summary_converted.copy(), ""
                )
                show_df(summary_display_converted)

                coordination_labels = [
                    "Koodinační přirážka Nominovaného subdodavatele",
                    "Koordinační přirážka Přímého dodavatele investora",
                    "Koordninační přirážka Nominovaného dodavatele standardů/koncových prvků",
                    "Doba výstavby",
                ]
                display_aliases = {str(alias) for alias in display_names.values()}
                supplier_aliases: List[str] = []
                for col in value_cols:
                    if is_master_column(col):
                        continue
                    base_name = col[:-len(" total")] if col.endswith(" total") else col
                    if base_name in display_aliases and base_name not in supplier_aliases:
                        supplier_aliases.append(base_name)
                if not supplier_aliases:
                    for col in value_cols:
                        if is_master_column(col):
                            continue
                        base_name = col[:-len(" total")] if col.endswith(" total") else col
                        cleaned = str(base_name).strip()
                        if cleaned and cleaned not in supplier_aliases:
                            supplier_aliases.append(cleaned)
                if supplier_aliases:
                    st.markdown("**Koordinační přirážky a další údaje:**")
                    storage_key = make_widget_key("recap", "coordination_table_state")
                    editor_key = make_widget_key("recap", "coordination_table_editor")
                    base_records: List[Dict[str, Any]] = []
                    for row_label in coordination_labels:
                        record: Dict[str, Any] = {"Položka": row_label}
                        for alias in supplier_aliases:
                            record[alias] = ""
                        base_records.append(record)
                    default_df = pd.DataFrame(base_records)
                    stored_records = st.session_state.get(storage_key)
                    if isinstance(stored_records, list):
                        try:
                            stored_df = pd.DataFrame(stored_records)
                        except Exception:
                            stored_df = pd.DataFrame()
                        if not stored_df.empty and "Položka" in stored_df.columns:
                            stored_df = stored_df.set_index("Položka")
                            stored_df = stored_df.reindex(coordination_labels)
                            stored_df = stored_df.reindex(columns=supplier_aliases, fill_value="")
                            stored_df = stored_df.fillna("")
                            stored_df.index.name = "Položka"
                            default_df = stored_df.reset_index()
                    column_config: Dict[str, Any] = {
                        "Položka": st.column_config.TextColumn("Položka", disabled=True)
                    }
                    for alias in supplier_aliases:
                        column_config[alias] = st.column_config.TextColumn(alias)
                    coordination_editor = st.data_editor(
                        default_df,
                        hide_index=True,
                        column_config=column_config,
                        num_rows="fixed",
                        key=editor_key,
                    )
                    if isinstance(coordination_editor, pd.DataFrame):
                        st.session_state[storage_key] = (
                            coordination_editor.fillna("").to_dict("records")
                        )
                else:
                    st.info(
                        "Pro zadání koordinačních přirážek je potřeba mít načtené nabídky dodavatelů."
                    )
            else:
                st.info("Souhrnná tabulka nedokázala zpracovat žádná čísla.")

            net_chart_series = net_sum.reindex(value_cols) if value_cols else pd.Series(dtype=float)
            if not net_chart_series.dropna().empty:
                chart_df = build_recap_chart_data(
                    value_cols,
                    net_chart_series,
                    currency_label=base_currency or "CZK",
                )
                if not chart_df.empty:
                    try:
                        fig_recap = px.bar(
                            chart_df,
                            x="Dodavatel",
                            y="Cena po odečtech",
                            color="Dodavatel",
                            color_discrete_map=chart_color_map,
                            title="Cena po odečtech hlavních položek",
                        )
                        fig_recap.update_traces(
                            text=chart_df["Popisek"],
                            textposition="outside",
                            texttemplate="%{text}",
                            customdata=np.column_stack(
                                [chart_df["Cena (text)"].fillna("–")]
                            ),
                            hovertemplate=(
                                "<b>%{x}</b><br>"
                                "Cena po odečtech: %{customdata[0]}<br>"
                                "Odchylka vs Master: %{text}<extra></extra>"
                            ),
                        )
                        fig_recap.update_layout(yaxis_title=f"{base_currency}", showlegend=False)
                        st.plotly_chart(fig_recap, use_container_width=True)
                    except Exception:
                        st.warning(
                            "Graf se nepodařilo vykreslit, zobrazují se hodnoty v tabulce."
                        )
                        show_df(chart_df)

            if (
                not main_detail_display_base.empty
                or not main_detail_display_converted.empty
                or not summary_display.empty
                or not summary_display_converted.empty
                or not chart_df.empty
            ):
                try:
                    pdf_bytes = generate_recap_pdf(
                        title=f"Rekapitulace — {overview_sheet}",
                        base_currency=base_currency,
                        target_currency=target_currency,
                        main_detail_base=main_detail_display_base,
                        main_detail_converted=main_detail_display_converted,
                        summary_base=summary_display,
                        summary_converted=summary_display_converted,
                        chart_df=chart_df,
                        chart_figure=fig_recap,
                    )
                    st.download_button(
                        "📄 Stáhnout rekapitulaci (PDF)",
                        data=pdf_bytes,
                        file_name="rekapitulace.pdf",
                        mime="application/pdf",
                    )
                except Exception:
                    st.warning("Export do PDF se nezdařil.")

            st.markdown("### Výběr položek pro vlastní součet")
            selection_state_key = make_widget_key("recap", "selection_state")
            if not working_sections.empty and value_cols:
                selection_columns = ["code", "description"] + value_cols
                selection_columns = [
                    col for col in selection_columns if col in working_sections.columns
                ]
                if selection_columns:
                    selection_source = working_sections.loc[:, selection_columns].copy()
                    selection_source.insert(0, "__selected__", False)
                    preselected_indices: List[int] = []
                    stored_prefill = st.session_state.get(selection_state_key, [])
                    for raw_idx in stored_prefill:
                        try:
                            idx_int = int(raw_idx)
                        except (TypeError, ValueError):
                            continue
                        if idx_int in selection_source.index:
                            preselected_indices.append(idx_int)
                    if preselected_indices:
                        selection_source.loc[preselected_indices, "__selected__"] = True
                    column_config: Dict[str, Any] = {
                        "__selected__": st.column_config.CheckboxColumn("Vybrat", default=False)
                    }
                    if "code" in selection_source.columns:
                        column_config["code"] = st.column_config.TextColumn("č.", disabled=True)
                    if "description" in selection_source.columns:
                        column_config["description"] = st.column_config.TextColumn(
                            "Položka", disabled=True
                        )
                    for col in value_cols:
                        if col in selection_source.columns:
                            column_config[col] = st.column_config.NumberColumn(
                                label=f"{col.replace(' total', '')} — CELKEM {base_currency}",
                                format="%.2f",
                                disabled=True,
                            )
                    selection_editor: Optional[pd.DataFrame] = None
                    submit_selection: bool = False
                    with st.form(key=make_widget_key("recap", "selection_form")):
                        selection_editor = st.data_editor(
                            selection_source,
                            hide_index=True,
                            column_config=column_config,
                            key=make_widget_key("recap", "selection_editor"),
                            use_container_width=True,
                        )
                        submit_selection = st.form_submit_button("Vytvořit tabulku z výběru")

                    selected_indices: List[int] = []
                    if (
                        isinstance(selection_editor, pd.DataFrame)
                        and "__selected__" in selection_editor.columns
                    ):
                        selected_flags = selection_editor["__selected__"].fillna(False)
                        for idx, flag in selected_flags.items():
                            if not bool(flag):
                                continue
                            try:
                                selected_indices.append(int(idx))
                            except (TypeError, ValueError):
                                continue
                    if submit_selection:
                        if selected_indices:
                            st.session_state[selection_state_key] = selected_indices
                        else:
                            st.session_state.pop(selection_state_key, None)
                            st.warning(
                                "Pro vytvoření souhrnu je potřeba vybrat alespoň jednu položku."
                            )

                    stored_indices = st.session_state.get(selection_state_key, [])
                    if stored_indices:
                        stored_mask = pd.Series(False, index=working_sections.index)
                        valid_indices: List[int] = []
                        for idx in stored_indices:
                            try:
                                idx_int = int(idx)
                            except (TypeError, ValueError):
                                continue
                            if idx_int in stored_mask.index:
                                valid_indices.append(idx_int)
                        if valid_indices:
                            stored_mask.loc[valid_indices] = True
                        if stored_mask.any():
                            selected_rows = working_sections.loc[stored_mask, selection_columns].copy()
                            detail_display = selected_rows.rename(
                                columns={"code": "č.", "description": "Položka"}
                            )
                            for col in value_cols:
                                if col in detail_display.columns:
                                    detail_display[col] = pd.to_numeric(
                                        detail_display[col], errors="coerce"
                                    )
                            st.markdown("**Vybrané položky:**")
                            show_df(
                                rename_value_columns_for_display(
                                    detail_display, f" — CELKEM {base_currency}"
                                )
                            )
                            totals = sum_for_mask(stored_mask)
                            summary_row = {
                                "Položka": "Součet vybraných položek",
                                "Jednotka": base_currency,
                            }
                            summary_row.update(
                                {col: totals.get(col, np.nan) for col in value_cols}
                            )
                            summary_df = pd.DataFrame([summary_row])
                            for col in value_cols:
                                if col in summary_df.columns:
                                    summary_df[col] = pd.to_numeric(
                                        summary_df[col], errors="coerce"
                                    )
                            st.markdown("**Součet vybraných položek:**")
                            show_df(
                                rename_value_columns_for_display(
                                    summary_df, f" — CELKEM {base_currency}"
                                )
                            )
                        else:
                            st.warning(
                                "Vybrané položky již nejsou v aktuálních datech k dispozici."
                            )
                            st.session_state.pop(selection_state_key, None)
                else:
                    st.info(
                        "Tabulka rekapitulace neobsahuje sloupce potřebné pro vytvoření výběru."
                    )
                    st.session_state.pop(selection_state_key, None)
            else:
                st.info("Pro výběr položek je potřeba načíst rekapitulaci s hodnotami.")
                st.session_state.pop(selection_state_key, None)

            st.markdown("### Interaktivní součet podsekcí")
            if not working_sections.empty and value_cols:
                tokens_df = working_sections[
                    working_sections["__code_token__"].astype(str).str.strip() != ""
                ][["__code_token__", "description"]]
                if tokens_df.empty:
                    st.info("V datech nejsou dostupné kódy podsekcí.")
                else:
                    desc_map = (
                        tokens_df.groupby("__code_token__")["description"]
                        .apply(
                            lambda series: next(
                                (str(val).strip() for val in series if str(val).strip()),
                                "",
                            )
                        )
                        .to_dict()
                    )
                    token_options = sorted(desc_map.keys(), key=natural_sort_key)
                    selected_token = st.selectbox(
                        "Vyber kód (např. 7.7) pro součet napříč celým rozpočtem",
                        options=token_options,
                        format_func=lambda token: (
                            f"{token} — {desc_map.get(token, '')}".strip(" —")
                        ),
                    )
                    selection_mask = working_sections["__code_token__"] == selected_token
                    if selection_mask.any():
                        label = desc_map.get(selected_token, "")
                        sum_values = sum_for_mask(selection_mask)
                        sum_row = {
                            "Položka": f"Součet pro {selected_token}",
                            "Jednotka": base_currency,
                        }
                        sum_row.update({col: sum_values.get(col, np.nan) for col in value_cols})
                        sum_df = pd.DataFrame([sum_row])
                        st.markdown("**Součet vybrané podsekce:**")
                        show_df(rename_value_columns_for_display(sum_df, ""))

                        detail_selection = working_sections.loc[
                            selection_mask, ["code", "description"] + value_cols
                        ].copy()
                        detail_selection.rename(
                            columns={"code": "č.", "description": "Položka"}, inplace=True
                        )
                        for col in value_cols:
                            if col in detail_selection.columns:
                                detail_selection[col] = pd.to_numeric(
                                    detail_selection[col], errors="coerce"
                                )
                        st.markdown("**Detail položek v rámci vybraného kódu:**")
                        show_df(
                            rename_value_columns_for_display(
                                detail_selection, f" — CELKEM {base_currency}"
                            )
                        )
                    else:
                        st.info("Pro zvolený kód nejsou k dispozici žádné položky.")
            else:
                st.info("Pro interaktivní součet je nutné mít načtené položky s kódy.")

            st.markdown("### Value Engineering (VE)")
            ve_tokens = ["VE", "A", "B", "C.1", "C.2", "C.3"]
            ve_rows: List[Dict[str, Any]] = []
            for token in ve_tokens:
                mask = working_sections["__code_token__"] == token if not working_sections.empty else pd.Series(False)
                if not working_sections.empty and mask.any():
                    desc_series = working_sections.loc[mask, "description"].astype(str)
                    desc_value = next((val.strip() for val in desc_series if val.strip()), "")
                    sums = sum_for_mask(mask)
                    row = {
                        "Kód": token,
                        "Popis": desc_value,
                        "Jednotka": base_currency,
                    }
                    row.update({col: sums.get(col, np.nan) for col in value_cols})
                    ve_rows.append(row)
            if ve_rows:
                ve_df = pd.DataFrame(ve_rows)
                show_df(rename_value_columns_for_display(ve_df, ""))
            else:
                st.info("V datech se nenachází žádné položky Value Engineering.")

            def aggregate_fixed_table(items: List[Dict[str, Any]]) -> pd.DataFrame:
                rows: List[Dict[str, Any]] = []
                for item in items:
                    codes = {
                        extract_code_token(code)
                        for code in item.get("codes", [])
                        if code is not None
                    }
                    keywords = [normalize_text(k) for k in item.get("keywords", []) if k]
                    match_all = bool(item.get("match_all_keywords"))
                    if working_sections.empty or not value_cols:
                        mask = pd.Series(False, index=working_sections.index)
                    else:
                        mask = pd.Series(False, index=working_sections.index)
                        if codes:
                            mask = mask | working_sections["__code_token__"].isin(codes)
                        if keywords:
                            norm_desc = working_sections.get("__norm_desc__", pd.Series("", index=working_sections.index))
                            if match_all:
                                keyword_mask = norm_desc.apply(
                                    lambda text: all(kw in text for kw in keywords)
                                )
                            else:
                                keyword_mask = norm_desc.apply(
                                    lambda text: any(kw in text for kw in keywords)
                                )
                            mask = mask | keyword_mask
                    sums = sum_for_mask(mask)
                    row = {
                        "Položka": item.get("label", ""),
                        "Jednotka": base_currency,
                    }
                    row.update({col: sums.get(col, np.nan) for col in value_cols})
                    rows.append(row)
                return pd.DataFrame(rows)

            fixed_tables: List[Tuple[str, List[Dict[str, Any]]]] = [
                (
                    "Vnitřní konstrukce",
                    [
                        {"label": "4 - Vnitřní konstrukce", "codes": ["4"]},
                        {"label": "4.1 - Příčky", "codes": ["4.1"]},
                        {"label": "4.2 - Dveře", "codes": ["4.2"]},
                        {
                            "label": "4.4 - Zámečnické a klempířské výrobky",
                            "codes": ["4.4"],
                        },
                    ],
                ),
                (
                    "Úpravy povrchů",
                    [
                        {"label": "5 - Úpravy povrchů", "codes": ["5"]},
                        {"label": "5.1 - Úpravy podlah", "codes": ["5.1"]},
                        {"label": "5.2 - Úpravy stropů", "codes": ["5.2"]},
                        {"label": "5.3 - Úpravy stěn", "codes": ["5.3"]},
                    ],
                ),
                (
                    "Vnitřní vybavení",
                    [
                        {"label": "6 - Vnitřní vybavení", "codes": ["6"]},
                        {"label": "6.1 - Vnitřní vybavení", "codes": ["6.1"]},
                        {
                            "label": "6.2 - Protipožární vybavení",
                            "codes": ["6.2"],
                        },
                    ],
                ),
                (
                    "Technické zařízení budov",
                    [
                        {"label": "7 - Technické zařízení budov", "codes": ["7"]},
                        {
                            "label": "7.1 - Kanalizace",
                            "keywords": ["kanalizace"],
                        },
                        {
                            "label": "7.2 - Vodovod",
                            "keywords": ["vodovod"],
                        },
                        {
                            "label": "7.3 - Zařizovací předměty",
                            "keywords": ["zarizovaci", "predmet"],
                            "match_all_keywords": True,
                        },
                        {
                            "label": "7.4 - Vytápění a chlazení",
                            "keywords": ["vytapeni", "chlazeni"],
                            "match_all_keywords": True,
                        },
                        {
                            "label": "7.6 - Vzduchotechnika",
                            "codes": ["7.6"],
                        },
                        {
                            "label": "7.7 - Měření a regulace",
                            "keywords": ["mereni", "regulace"],
                            "match_all_keywords": True,
                        },
                        {
                            "label": "7.8 - Silnoproud",
                            "keywords": ["silnoproud"],
                        },
                        {
                            "label": "7.9 - Slaboproud",
                            "keywords": ["slaboproud"],
                        },
                        {
                            "label": "7.10 - Stabilní hasicí zařízení - MHZ",
                            "keywords": ["stabilni hasi", "mhz"],
                            "match_all_keywords": True,
                        },
                        {
                            "label": "7.11 - Elektro slaboproud - EPS",
                            "keywords": ["elektro slaboproud", "eps"],
                            "match_all_keywords": True,
                        },
                        {
                            "label": "7.12 - Technologie gastro",
                            "keywords": ["technologie gastro"],
                        },
                        {
                            "label": "7.13 - Gastrovoz - chlazení",
                            "keywords": ["gastrovoz", "chlazeni"],
                            "match_all_keywords": True,
                        },
                    ],
                ),
            ]

            pdf_tables: List[Tuple[str, pd.DataFrame]] = []
            for title, items in fixed_tables:
                st.markdown(f"### {title}")
                table_df = aggregate_fixed_table(items)
                display_df = rename_value_columns_for_display(table_df.copy(), "")
                show_df(display_df)
                if not display_df.empty:
                    pdf_tables.append((title, display_df.copy()))

            available_tables = [(title, df) for title, df in pdf_tables if not df.empty]
            if available_tables:
                try:
                    themed_pdf = generate_tables_pdf(
                        title=f"Tematické tabulky — {overview_sheet}",
                        tables=available_tables,
                    )
                    st.download_button(
                        "📄 Stáhnout tematické tabulky (PDF)",
                        data=themed_pdf,
                        file_name="tematicke_tabulky.pdf",
                        mime="application/pdf",
                    )
                except Exception:
                    st.warning("Export tematických tabulek do PDF se nezdařil.")

            if not indirect_total.empty:
                st.markdown("### Vedlejší rozpočtové náklady — součty")
                indirect_display = indirect_total.copy()
                indirect_display.rename(
                    columns={
                        "supplier": "Dodavatel",
                        "total": f"Součet ({base_currency})",
                    },
                    inplace=True,
                )
                show_df(indirect_display)

            if not missing_df.empty:
                st.markdown("### Chybějící položky dle dodavatele — součet")
                missing_work = missing_df.copy()
                if "Master total" in missing_work.columns:
                    missing_work["Master total"] = pd.to_numeric(
                        missing_work["Master total"], errors="coerce"
                    )
                summary_missing = (
                    missing_work.groupby("missing_in")["Master total"].sum().reset_index()
                    if "missing_in" in missing_work.columns
                    else pd.DataFrame()
                )
                if not summary_missing.empty:
                    summary_missing.rename(
                        columns={
                            "missing_in": "Dodavatel",
                            "Master total": f"Součet chybějících položek ({base_currency})",
                        },
                        inplace=True,
                    )
                    show_df(summary_missing)
                detail_missing = missing_df.copy()
                detail_missing.rename(
                    columns={
                        "code": "č.",
                        "description": "Položka",
                        "missing_in": "Dodavatel",
                    },
                    inplace=True,
                )
                st.markdown("**Detail chybějících položek (v původní měně):**")
                show_df(detail_missing)

                with st.expander("Původní tabulka (detailní řádky)", expanded=False):
                    raw_display = sections_df.copy()
                    raw_display = raw_display.sort_values(
                        by="source_order" if "source_order" in raw_display.columns else "code"
                    )
                    raw_display = raw_display.rename(
                        columns={
                            "auto_group_key": "Návrh kódu skupiny",
                            "auto_group_label": "Návrh popisu skupiny",
                            "auto_group_order": "Pořadí (návrh)",
                            "source_order": "Původní pořadí",
                        }
                    )
                    show_df(raw_display)

            if not missing_df.empty:
                st.markdown(f"### Chybějící položky dle dodavatele ({base_currency})")
                show_df(missing_df)
            if not indirect_df.empty:
                st.markdown(f"### Vedlejší rozpočtové náklady ({base_currency})")
                indirect_detail_display = rename_value_columns_for_display(
                    indirect_df.copy(), f" — {base_currency}"
                )
                show_df(indirect_detail_display)
                if not indirect_total.empty:
                    st.markdown(f"**Součet vedlejších nákladů ({base_currency}):**")
                    show_df(
                        rename_value_columns_for_display(
                            indirect_total.copy(), f" — {base_currency}"
                        )
                    )
            if not added_df.empty:
                st.markdown(f"### Náklady přidané dodavatelem ({base_currency})")
                show_df(
                    rename_value_columns_for_display(added_df.copy(), f" — {base_currency}")
                )

with tab_dashboard:
    if not bids_dict:
        st.info("Nejdřív nahraj nabídky.")
    else:
        results = compare_results

        # Choose a sheet for detailed variance chart
        sheet_choices = list(results.keys())
        if sheet_choices:
            sel_sheet = st.selectbox("Vyber list pro detailní grafy", sheet_choices, index=0)
            df = results[sel_sheet]
            total_cols = [c for c in df.columns if c.endswith(" total")]
            if total_cols:
                st.markdown("**Součet za list (včetně Master):**")
                totals_chart_df = pd.DataFrame({"supplier": [c.replace(" total", "") for c in total_cols], "total": [df[c].sum() for c in total_cols]})
                try:
                    fig_tot = px.bar(
                        totals_chart_df,
                        x="supplier",
                        y="total",
                        color="supplier",
                        color_discrete_map=chart_color_map,
                        title=f"Součet za list: {sel_sheet} ({currency})",
                    )
                    fig_tot.update_layout(showlegend=False)
                    st.plotly_chart(fig_tot, use_container_width=True)
                except Exception:
                    show_df(totals_chart_df)

            # Heatmap-like chart: Δ vs LOWEST per supplier
            delta_cols = [c for c in df.columns if c.endswith(" Δ vs LOWEST")]
            if delta_cols:
                heat_df = df[["__key__"] + delta_cols].copy().set_index("__key__")
                # Rename columns to supplier names only
                heat_df.columns = [c.replace(" Δ vs LOWEST", "") for c in heat_df.columns]
                # aggregate top N worst deltas by sum
                sum_deltas = heat_df.sum().sort_values(ascending=False)
                sum_deltas_df = sum_deltas.rename_axis("supplier").reset_index(name="value")
                st.markdown("**Součet odchylek vs. nejnižší (vyšší = horší):**")
                try:
                    fig = px.bar(
                        sum_deltas_df,
                        x="supplier",
                        y="value",
                        color="supplier",
                        color_discrete_map=chart_color_map,
                        title="Součet Δ vs. nejnižší nabídku (po dodavatelích)",
                    )
                    fig.update_layout(showlegend=False)
                    st.plotly_chart(fig, use_container_width=True)
                except Exception:
                    show_df(sum_deltas_df)

                # Top položky podle rozdílu mezi nejlepší a vybraným dodavatelem
                st.markdown("**Top 20 položek s nejvyšší odchylkou od nejnižší ceny (součet přes dodavatele):**")
                item_abs = heat_df.abs()
                item_deltas = item_abs.sum(axis=1).sort_values(ascending=False).head(20)
                leading_supplier = item_abs.loc[item_deltas.index].idxmax(axis=1)
                item_chart_df = (
                    item_deltas.rename("value")
                    .rename_axis("item")
                    .to_frame()
                    .join(leading_supplier.rename("supplier"), how="left")
                    .reset_index()
                )
                try:
                    fig2 = px.bar(
                        item_chart_df,
                        x="item",
                        y="value",
                        color="supplier",
                        color_discrete_map=chart_color_map,
                        title="Top 20 položek podle absolutní Δ",
                    )
                    st.plotly_chart(fig2, use_container_width=True)
                except Exception:
                    show_df(item_chart_df)
            else:
                st.info("Pro zvolený list zatím nejsou k dispozici delty (nahraj nabídky a ověř mapování).")

with tab_qa:
    if not bids_dict:
        st.info("Nahraj nabídky, ať můžeme spustit kontroly.")
    else:
        qa = qa_checks(master_wb, bids_dict)
        for sheet, per_sup in qa.items():
            st.subheader(f"List: {sheet}")
            for sup, d in per_sup.items():
                alias = display_names.get(sup, sup)
                st.markdown(f"**Dodavatel:** {alias}")
                c1, c2, c3, c4 = st.columns(4)
                with c1:
                    st.markdown("**Chybějící položky**")
                    show_df(d["missing"].head(50))
                with c2:
                    st.markdown("**Nad rámec (navíc)**")
                    show_df(d["extras"].head(50))
                with c3:
                    st.markdown("**Duplicitní položky**")
                    show_df(d["duplicates"].head(50))
                with c4:
                    st.markdown("**Δ součtu vs. souhrn**")
                    diff = d.get("total_diff")
                    if diff is None or pd.isna(diff):
                        st.write("n/a")
                    else:
                        st.write(format_number(diff))

st.markdown("---")
st.caption("© 2025 BoQ Bid Studio — MVP. Doporučení: používat jednotné Item ID pro precizní párování.")

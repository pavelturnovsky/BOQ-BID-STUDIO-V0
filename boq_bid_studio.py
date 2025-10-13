
import hashlib
import logging
import io
import math
import re
import json
import time
import unicodedata
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple
from string import Template

import numpy as np
import pandas as pd
import plotly.express as px
import streamlit as st
import streamlit.components.v1 as components
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


MAIN_RECAP_TOKENS = ["0", "1", "2", "3", "4", "5"]


PERCENT_DIFF_SUFFIX = "_pct_diff"
PERCENT_DIFF_LABEL = " — ODCHYLKA VS MASTER (%)"
UNMAPPED_ROW_LABEL = "Nemapované položky"

SECTION_ONTOLOGY = {
    str(item.get("code_token", "")): item.get("fallback_label") or item.get("match_label", "")
    for item in RECAP_CATEGORY_CONFIG
    if item.get("code_token")
}
SECTION_ONTOLOGY.setdefault("", "Nezařazeno")


def aggregate_weighted_average_by_key(
    df: pd.DataFrame,
    value_col: str,
    weight_col: str,
    key_col: str = "__key__",
) -> pd.Series:
    """Return weighted averages of ``value_col`` grouped by ``key_col``.

    The helper prefers a weighted average using ``weight_col`` whenever both
    the value and weight are known. If weights are missing or zero, the first
    available value in the group is used as a fallback to avoid returning
    ``NaN`` for otherwise valid rows.
    """

    required = {value_col, weight_col, key_col}
    if not required.issubset(df.columns):
        return pd.Series(dtype=float)

    working = df[list(required)].copy()
    working[key_col] = working[key_col].astype(str)
    working[value_col] = pd.to_numeric(working[value_col], errors="coerce")
    working[weight_col] = pd.to_numeric(working[weight_col], errors="coerce")
    working = working.dropna(subset=[key_col])
    if working.empty:
        return pd.Series(dtype=float)

    grouped = working.groupby(key_col, sort=False)

    def _aggregate(group: pd.DataFrame) -> float:
        values = group[value_col]
        weights = group[weight_col]
        valid = values.notna() & weights.notna()
        if valid.any():
            total_weight = weights.loc[valid].sum(min_count=1)
            if pd.notna(total_weight) and total_weight != 0:
                weighted_sum = (values.loc[valid] * weights.loc[valid]).sum(min_count=1)
                if pd.notna(weighted_sum):
                    return float(weighted_sum / total_weight)
        for val in values:
            if pd.notna(val):
                return float(val)
        return float("nan")

    aggregated = grouped.apply(_aggregate)
    aggregated.index = aggregated.index.astype(str)
    return aggregated


def first_non_missing(series: pd.Series) -> Any:
    """Return the first non-missing/non-empty value from ``series``."""

    if series is None or not isinstance(series, pd.Series) or series.empty:
        return np.nan

    for value in series:
        if pd.isna(value):
            continue
        if isinstance(value, str) and value.strip() == "":
            continue
        return value
    return np.nan


def sum_preserving_na(series: pd.Series) -> float:
    """Return the numeric sum while keeping ``NaN`` when all values are missing."""

    if series is None or not isinstance(series, pd.Series) or series.empty:
        return float("nan")

    numeric = pd.to_numeric(series, errors="coerce")
    result = numeric.sum(min_count=1)
    if pd.isna(result):
        return float("nan")
    return float(result)

COMPARISON_METRICS_CONFIG = {
    "total": {
        "label": "Cena celkem",
        "master_columns": ["Master total"],
        "supplier_suffix": " total",
        "number_format": "currency",
        "help": "Porovnání celkové ceny položky.",
    },
    "quantity": {
        "label": "Množství",
        "master_columns": ["Master quantity", "quantity"],
        "supplier_suffix": " quantity",
        "number_format": "number",
        "help": "Srovnání vykázaných množství.",
    },
    "unit_price_material": {
        "label": "Jedn. cena materiál",
        "master_columns": ["Master unit_price_material", "unit_price_material"],
        "supplier_suffix": " unit_price_material",
        "number_format": "currency",
        "help": "Materiálová jednotková cena.",
    },
    "unit_price_install": {
        "label": "Jedn. cena montáž",
        "master_columns": ["Master unit_price_install", "unit_price_install"],
        "supplier_suffix": " unit_price_install",
        "number_format": "currency",
        "help": "Montážní jednotková cena.",
    },
}

COMPARISON_METRIC_ORDER = [
    "total",
    "quantity",
    "unit_price_material",
    "unit_price_install",
]

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


def normalize_join_value(value: Any) -> str:
    """Return a canonical representation suitable for joining rows."""

    if pd.isna(value):
        return ""
    text: str
    if isinstance(value, (int, np.integer)):
        text = str(int(value))
    elif isinstance(value, (float, np.floating)):
        float_val = float(value)
        if math.isfinite(float_val) and float_val.is_integer():
            text = str(int(float_val))
        else:
            text = str(float_val)
    else:
        text = str(value).strip()
    if not text:
        return ""
    normalized = unicodedata.normalize("NFKD", text)
    without_diacritics = "".join(ch for ch in normalized if not unicodedata.combining(ch))
    collapsed = re.sub(r"\s+", " ", without_diacritics)
    return collapsed.strip().lower()


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

    for master_helper in (
        "Master quantity",
        "Master unit_price_material",
        "Master unit_price_install",
    ):
        if master_helper in analysis_df.columns:
            analysis_df[master_helper] = coerce_numeric(
                analysis_df[master_helper]
            )

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
        master_series = coerce_numeric(analysis_df[master_column])
    else:
        master_series = pd.Series(np.nan, index=analysis_df.index, dtype=float)

    supplier_series: Dict[str, pd.Series] = {}
    for col, supplier in zip(supplier_columns, suppliers):
        supplier_series[supplier] = coerce_numeric(analysis_df[col])

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


def _series_or_default(df: pd.DataFrame, names: Any, default: Any) -> pd.Series:
    """Return the first matching column from ``df`` or a default-filled series."""

    if not isinstance(df, pd.DataFrame):
        return pd.Series(dtype=type(default) if default is not None else float)

    if not isinstance(names, (list, tuple, set)):
        names = [names]

    for name in names:
        if name and name in df.columns:
            series = df[name]
            if isinstance(series, pd.Series):
                return series

    if default is None:
        default = np.nan
    return pd.Series([default] * len(df), index=df.index)


def build_side_by_side_view(
    dataset: ComparisonDataset, supplier_alias: str
) -> pd.DataFrame:
    """Return a two-column comparison table for the provided supplier."""

    if dataset is None or dataset.analysis_df.empty or not supplier_alias:
        return pd.DataFrame(
            columns=
            [
                "Kód",
                "Popis",
                "Jednotka",
                "Cena master",
                "Cena dodavatel",
                "Jednotková cena montáž master",
                "Jednotková cena montáž dodavatel",
                "Jednotková cena materiál master",
                "Jednotková cena materiál dodavatel",
                "Množství master",
                "Množství dodavatel",
            ]
        )

    working = dataset.analysis_df.copy()

    code_series = _series_or_default(working, "code", "")
    description_series = _series_or_default(working, "description", "")
    unit_series = _series_or_default(working, "unit", "")
    master_total_series = _series_or_default(
        working, [dataset.master_column, "Master total"], np.nan
    )
    supplier_total_series = _series_or_default(
        working, f"{supplier_alias} total", np.nan
    )
    master_install_series = _series_or_default(
        working, "Master unit_price_install", np.nan
    )
    supplier_install_series = _series_or_default(
        working, f"{supplier_alias} unit_price_install", np.nan
    )
    master_material_series = _series_or_default(
        working, "Master unit_price_material", np.nan
    )
    supplier_material_series = _series_or_default(
        working, f"{supplier_alias} unit_price_material", np.nan
    )
    master_quantity_series = _series_or_default(
        working, ["Master quantity", "quantity"], np.nan
    )
    supplier_quantity_series = _series_or_default(
        working, f"{supplier_alias} quantity", np.nan
    )

    description_clean = description_series.astype(str).str.strip()
    has_description = description_clean.ne("") & ~description_clean.str.contains(
        UNMAPPED_ROW_LABEL, case=False, na=False
    )

    supplier_numeric_presence = pd.to_numeric(
        supplier_total_series, errors="coerce"
    ).notna()
    supplier_numeric_presence |= pd.to_numeric(
        supplier_quantity_series, errors="coerce"
    ).notna()
    supplier_numeric_presence |= pd.to_numeric(
        supplier_install_series, errors="coerce"
    ).notna()
    supplier_numeric_presence |= pd.to_numeric(
        supplier_material_series, errors="coerce"
    ).notna()

    valid_rows = has_description & supplier_numeric_presence
    if not valid_rows.any():
        return pd.DataFrame(
            columns=
            [
                "Kód",
                "Popis",
                "Jednotka",
                "Cena master",
                "Cena dodavatel",
                "Jednotková cena montáž master",
                "Jednotková cena montáž dodavatel",
                "Jednotková cena materiál master",
                "Jednotková cena materiál dodavatel",
                "Množství master",
                "Množství dodavatel",
            ]
        )

    filtered_index = working.index[valid_rows]

    result = pd.DataFrame(
        {
            "Kód": code_series.loc[filtered_index].reset_index(drop=True),
            "Popis": description_series.loc[filtered_index].reset_index(drop=True),
            "Jednotka": unit_series.loc[filtered_index].reset_index(drop=True),
            "Cena master": master_total_series.loc[filtered_index].reset_index(drop=True),
            "Cena dodavatel": supplier_total_series.loc[filtered_index].reset_index(
                drop=True
            ),
            "Jednotková cena montáž master": master_install_series.loc[
                filtered_index
            ].reset_index(drop=True),
            "Jednotková cena montáž dodavatel": supplier_install_series.loc[
                filtered_index
            ].reset_index(drop=True),
            "Jednotková cena materiál master": master_material_series.loc[
                filtered_index
            ].reset_index(drop=True),
            "Jednotková cena materiál dodavatel": supplier_material_series.loc[
                filtered_index
            ].reset_index(drop=True),
            "Množství master": master_quantity_series.loc[filtered_index].reset_index(
                drop=True
            ),
            "Množství dodavatel": supplier_quantity_series.loc[
                filtered_index
            ].reset_index(drop=True),
        }
    )
    return result


def build_master_supplier_table(
    dataset: ComparisonDataset, supplier_alias: str
) -> pd.DataFrame:
    """Return a combined view of Master and the selected supplier values."""

    if dataset is None or dataset.analysis_df.empty or not supplier_alias:
        return pd.DataFrame()

    working = dataset.analysis_df.copy()

    code_series = _series_or_default(working, "code", "")
    description_series = _series_or_default(working, "description", "")
    section_series = _series_or_default(working, "Oddíl", "")
    master_unit_series = _series_or_default(working, ["unit", "Master unit"], "")
    supplier_unit_series = _series_or_default(
        working, f"{supplier_alias} unit", ""
    )
    master_quantity_series = _series_or_default(
        working, ["Master quantity", "quantity"], np.nan
    )
    supplier_quantity_series = _series_or_default(
        working, f"{supplier_alias} quantity", np.nan
    )
    master_unit_price_series = _series_or_default(
        working, "Master unit_price", np.nan
    )
    supplier_unit_price_series = _series_or_default(
        working, f"{supplier_alias} unit_price", np.nan
    )
    master_unit_price_material_series = _series_or_default(
        working, "Master unit_price_material", np.nan
    )
    supplier_unit_price_material_series = _series_or_default(
        working, f"{supplier_alias} unit_price_material", np.nan
    )
    master_unit_price_install_series = _series_or_default(
        working, "Master unit_price_install", np.nan
    )
    supplier_unit_price_install_series = _series_or_default(
        working, f"{supplier_alias} unit_price_install", np.nan
    )
    master_total_series = _series_or_default(
        working, [dataset.master_column, "Master total"], np.nan
    )
    supplier_total_series = _series_or_default(
        working, f"{supplier_alias} total", np.nan
    )

    numeric_master_total = pd.to_numeric(master_total_series, errors="coerce")
    numeric_supplier_total = pd.to_numeric(supplier_total_series, errors="coerce")
    numeric_master_qty = pd.to_numeric(master_quantity_series, errors="coerce")
    numeric_supplier_qty = pd.to_numeric(supplier_quantity_series, errors="coerce")

    difference_series = numeric_supplier_total - numeric_master_total
    percent_series = compute_percent_difference(
        numeric_supplier_total, numeric_master_total
    )

    column_map: Dict[str, pd.Series] = {
        "Kód": code_series,
        "Popis": description_series,
        "Oddíl": section_series,
        "Jednotka — Master": master_unit_series,
        f"Jednotka — {supplier_alias}": supplier_unit_series,
        "Množství — Master": master_quantity_series,
        f"Množství — {supplier_alias}": supplier_quantity_series,
    }

    if not master_unit_price_series.isna().all():
        column_map["Jednotková cena — Master"] = master_unit_price_series
    if not supplier_unit_price_series.isna().all():
        column_map[f"Jednotková cena — {supplier_alias}"] = (
            supplier_unit_price_series
        )
    if not master_unit_price_material_series.isna().all():
        column_map["Jednotková cena materiál — Master"] = (
            master_unit_price_material_series
        )
    if not supplier_unit_price_material_series.isna().all():
        column_map[
            f"Jednotková cena materiál — {supplier_alias}"
        ] = supplier_unit_price_material_series
    if not master_unit_price_install_series.isna().all():
        column_map["Jednotková cena montáž — Master"] = (
            master_unit_price_install_series
        )
    if not supplier_unit_price_install_series.isna().all():
        column_map[
            f"Jednotková cena montáž — {supplier_alias}"
        ] = supplier_unit_price_install_series

    column_map["Cena — Master"] = master_total_series
    column_map[f"Cena — {supplier_alias}"] = supplier_total_series

    if not difference_series.isna().all():
        column_map[f"Rozdíl {supplier_alias} vs Master"] = difference_series
    if not percent_series.isna().all():
        column_map[f"Δ (%) {supplier_alias} vs Master"] = percent_series

    combined_df = pd.DataFrame(column_map)

    description_clean = description_series.astype(str).str.strip()
    has_description = description_clean.ne("") & ~description_clean.str.contains(
        UNMAPPED_ROW_LABEL, case=False, na=False
    )

    value_presence = (
        numeric_master_total.notna()
        | numeric_supplier_total.notna()
        | numeric_master_qty.notna()
        | numeric_supplier_qty.notna()
    )

    filtered_df = combined_df[has_description & value_presence]
    return filtered_df.reset_index(drop=True)


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
        header_text = str(col)
        header_length = len(header_text)
        effective_len = int(max_length or 0)
        if "%" not in header_text:
            effective_len = max(effective_len, header_length)
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

    normalized_cols = [str(col) for col in value_cols]
    if not isinstance(net_series, pd.Series):
        net_series = pd.Series(net_series)
    working_series = net_series.copy()
    try:
        working_series.index = working_series.index.astype(str)
    except Exception:
        working_series.index = working_series.index.map(str)
    aligned_values = working_series.reindex(normalized_cols)

    working_df = pd.DataFrame(
        {
            "source_column": normalized_cols,
            "Cena po odečtech": aligned_values.values,
        }
    )
    working_df["Dodavatel"] = working_df["source_column"].str.replace(
        " total", "", regex=False
    )

    supplier_order: List[str] = []
    for supplier in working_df["Dodavatel"].astype(str):
        if supplier not in supplier_order:
            supplier_order.append(supplier)

    def _first_numeric(series: pd.Series) -> float:
        numeric = pd.to_numeric(series, errors="coerce")
        numeric = numeric.dropna()
        if numeric.empty:
            return np.nan
        return float(numeric.iloc[0])

    collapsed = (
        working_df.groupby("Dodavatel", sort=False)["Cena po odečtech"]
        .apply(_first_numeric)
        .reindex(supplier_order)
    )

    chart_df = collapsed.reset_index()
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
    chart_df["Odchylka (text)"] = chart_df["Odchylka vs Master (%)"].apply(
        format_percent_label
    )
    chart_df["Cena (text)"] = [
        format_currency_label(value, currency_label)
        for value in chart_df["Cena po odečtech"]
    ]
    chart_df["Popisek"] = chart_df["Cena (text)"]
    return chart_df


def build_comparison_join_key(df: pd.DataFrame) -> pd.Series:
    """Return a deterministic join key for comparison tables.

    Primarily uses ``item_id`` when available and falls back to the
    combination of code/description. The helper mirrors the behaviour of the
    lookups used during workbook alignment so that aggregated data (e.g.
    rekapitulace) can be reliably matched back to detail rows in the
    comparison tab.
    """

    if df is None or df.empty:
        return pd.Series(dtype=str)

    index = df.index
    if "item_id" in df.columns:
        item_ids = df["item_id"].map(normalize_join_value)
    else:
        item_ids = pd.Series(["" for _ in range(len(index))], index=index, dtype=object)

    if "code" in df.columns:
        raw_codes = df["code"]
    else:
        raw_codes = pd.Series(["" for _ in range(len(index))], index=index, dtype=object)
    codes = raw_codes.map(normalize_join_value)

    if "description" in df.columns:
        raw_desc = df["description"]
    else:
        raw_desc = pd.Series(["" for _ in range(len(index))], index=index, dtype=object)
    descriptions = raw_desc.map(normalize_join_value)

    fallback = (codes + "||" + descriptions).str.strip()
    join_key = item_ids.astype(str).str.strip()
    join_key = join_key.where(join_key != "", fallback)
    return join_key.fillna("").astype(str)


def align_total_columns(
    base_df: pd.DataFrame,
    totals_df: pd.DataFrame,
    rename_map: Optional[Dict[str, str]] = None,
) -> pd.DataFrame:
    """Return ``base_df`` with value columns replaced by ``totals_df``.

    The helper ensures that detailed comparison tables reuse the same summed
    totals as the rekapitulace overview. It aligns rows via
    :func:`build_comparison_join_key` and overwrites numeric values whenever a
    matching aggregated value exists. Optional ``rename_map`` can be supplied
    to harmonise supplier aliases before matching.
    """

    if base_df is None or base_df.empty or totals_df is None or totals_df.empty:
        return base_df

    working_totals = totals_df.copy()
    if rename_map:
        working_totals = working_totals.rename(columns=rename_map)

    total_columns = [
        col
        for col in working_totals.columns
        if str(col).endswith(" total") and not str(col).startswith("__present__")
    ]
    if not total_columns:
        return base_df

    base_with_keys = base_df.copy()
    key_col = "__comparison_join_key__"
    base_with_keys[key_col] = build_comparison_join_key(base_with_keys)
    working_totals[key_col] = build_comparison_join_key(working_totals)

    if base_with_keys[key_col].empty:
        base_with_keys.drop(columns=[key_col], inplace=True, errors="ignore")
        return base_with_keys

    aggregated = (
        working_totals.groupby(key_col, sort=False)[total_columns]
        .sum(min_count=1)
        .dropna(how="all")
    )
    if aggregated.empty:
        base_with_keys.drop(columns=[key_col], inplace=True, errors="ignore")
        return base_with_keys

    base_with_keys = base_with_keys.set_index(key_col, drop=False)
    for col in total_columns:
        if col not in base_with_keys.columns:
            base_with_keys[col] = np.nan
        current = pd.to_numeric(base_with_keys[col], errors="coerce")
        updates = pd.to_numeric(aggregated.get(col), errors="coerce")
        if updates is None or updates.empty:
            continue
        updates_aligned = updates.reindex(current.index)
        valid_mask = updates_aligned.notna()
        if valid_mask.any():
            current.loc[valid_mask] = updates_aligned.loc[valid_mask]
        base_with_keys[col] = current
    base_with_keys.reset_index(drop=True, inplace=True)
    base_with_keys.drop(columns=[key_col], inplace=True, errors="ignore")
    return base_with_keys


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

    include_summary_other = summary_rows_included_as_items(df)
    if isinstance(include_summary_other, pd.Series) and not include_summary_other.empty:
        mask = mask & ~include_summary_other.reindex(index, fill_value=False)

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


def summary_rows_included_as_items(df: pd.DataFrame) -> pd.Series:
    """Return mask for summary rows that should still behave like regular items."""

    if df is None or not isinstance(df, pd.DataFrame) or df.empty:
        return pd.Series(dtype=bool)

    index = df.index
    summary_flag = df.get("is_summary")
    if isinstance(summary_flag, pd.Series):
        base = summary_flag.fillna(False).astype(bool)
    else:
        base = pd.Series(False, index=index, dtype=bool)

    summary_type_series = (
        df.get("summary_type", pd.Series("", index=index, dtype="object"))
        .fillna("")
        .astype(str)
        .str.lower()
    )

    summary_total_series = df.get("summary_total")
    if isinstance(summary_total_series, pd.Series):
        summary_total_numeric = coerce_numeric(summary_total_series)
        has_value = summary_total_series.notna()
        if summary_total_series.dtype == object:
            has_value = has_value & summary_total_series.astype(str).str.strip().ne("")
        non_zero = summary_total_numeric.notna() & summary_total_numeric.abs().gt(1e-9)
        effective = has_value & non_zero
        if effective.any():
            missing = ~effective
        else:
            missing = pd.Series(False, index=index, dtype=bool)
    else:
        missing = pd.Series(False, index=index, dtype=bool)

    return base & summary_type_series.eq("other") & missing

@st.cache_data
def build_normalized_table(
    df: pd.DataFrame,
    mapping: Dict[str, int],
    *,
    preserve_summary_totals: bool = False,
) -> pd.DataFrame:
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

    # Detect summary rows using centralized helper unless the caller explicitly
    # wants to preserve totals as-is (rekapitulace tables work with hard
    # numbers that must not be altered).
    if preserve_summary_totals:
        summary_mask = pd.Series(False, index=out.index, dtype=bool)
        out["is_summary"] = summary_mask
        out["summary_type"] = ""
    else:
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
    include_summary_other = summary_rows_included_as_items(out)
    adjustable_summary_mask = summary_mask & ~include_summary_other
    out.loc[
        adjustable_summary_mask & out["summary_total"].isna(), "summary_total"
    ] = out.loc[
        adjustable_summary_mask & out["summary_total"].isna(), "total_price"
    ]
    out.loc[adjustable_summary_mask, "total_price"] = np.nan

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
    if isinstance(include_summary_other, pd.Series):
        summary_col &= ~include_summary_other.reindex(out.index, fill_value=False)
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
    return f"{x:,.1f}".replace(",", " ").replace(".", ",")


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


def sanitize_filename(value: Any, default: str = "data") -> str:
    """Return a filesystem-friendly name derived from arbitrary text."""

    if value is None:
        return default
    text = str(value).strip()
    if not text:
        return default
    normalized = unicodedata.normalize("NFKD", text)
    without_diacritics = "".join(ch for ch in normalized if not unicodedata.combining(ch))
    safe = re.sub(r"[^0-9A-Za-z]+", "_", without_diacritics)
    safe = safe.strip("_")
    return safe or default


def prepare_preview_table(table: Any) -> pd.DataFrame:
    """Prepare a normalized table for preview/export by removing helper columns."""

    if not isinstance(table, pd.DataFrame) or table.empty:
        return pd.DataFrame()

    display = table.copy()
    display = display.reset_index(drop=True)
    helper_cols = [col for col in display.columns if str(col).startswith("__")]
    display = display.drop(columns=helper_cols, errors="ignore")
    display = display.reset_index(drop=True)
    return display


def _normalize_preview_value(value: Any) -> str:
    if pd.isna(value):
        return ""
    text = str(value).strip()
    if not text:
        return ""
    if text.lower() in {"nan", "none", "null"}:
        return ""
    return text


def _format_preview_row_order(value: Any) -> str:
    if pd.isna(value):
        return ""
    if isinstance(value, (int, np.integer)):
        return str(int(value))
    if isinstance(value, (float, np.floating)):
        float_value = float(value)
        if math.isfinite(float_value) and float_value.is_integer():
            return str(int(float_value))
        return str(float_value)
    text = str(value).strip()
    if not text:
        return ""
    return text


def extract_preview_row_keys(df: pd.DataFrame) -> List[str]:
    if not isinstance(df, pd.DataFrame) or df.empty:
        return []

    working = df.reset_index(drop=True)
    if "__row_order__" in working.columns:
        order_series = working["__row_order__"].apply(_format_preview_row_order)
    else:
        order_series = (
            pd.Series(np.arange(len(working)), index=working.index)
            .apply(_format_preview_row_order)
        )

    code_series = (
        working.get("code", pd.Series("", index=working.index))
        .map(_normalize_preview_value)
        .astype(str)
    )
    description_series = (
        working.get("description", pd.Series("", index=working.index))
        .map(_normalize_preview_value)
        .astype(str)
    )

    # Build stable identifiers for rows. Prefer code + description so that
    # reordering rows or inserting new ones does not falsely mark identical
    # items as missing/extra. Track occurrences of each pair to distinguish
    # duplicates while still ignoring the ``__row_order__`` helper column.
    occurrence_counts: Dict[Tuple[str, str], int] = {}
    keys: List[str] = []
    for idx in working.index:
        code_value = code_series.iloc[idx]
        desc_value = description_series.iloc[idx]
        has_identity = bool(code_value or desc_value)

        if has_identity:
            pair = (code_value, desc_value)
            occurrence = occurrence_counts.get(pair, 0) + 1
            occurrence_counts[pair] = occurrence
            payload = {
                "code": code_value,
                "description": desc_value,
                "occurrence": occurrence,
            }
        else:
            order_value = order_series.iloc[idx]
            payload = {"index": int(idx)}
            if order_value:
                payload["row_order"] = order_value

        key = json.dumps(payload, sort_keys=True, ensure_ascii=False)
        keys.append(key)
    return keys


def extract_preview_key_set(table: Any) -> Set[str]:
    if not isinstance(table, pd.DataFrame) or table.empty:
        return set()

    keys = extract_preview_row_keys(table)
    return {key for key in keys if key}


def format_preview_number(value: Any, decimals: int = 1) -> str:
    if value is None or (isinstance(value, str) and not value.strip()):
        return ""
    try:
        numeric_value = float(value)
    except (TypeError, ValueError):
        return str(value)
    if math.isnan(numeric_value):
        return ""

    format_spec = f",.{max(decimals, 0)}f"
    rounded = float(np.round(numeric_value, decimals)) if decimals > 0 else float(
        np.round(numeric_value)
    )
    formatted = format(rounded, format_spec)
    formatted = formatted.replace(",", "\u00a0")
    formatted = formatted.replace(".", ",")
    return formatted


def format_preview_numbers(
    display_df: pd.DataFrame, numeric_source: pd.DataFrame, numeric_cols: List[str]
) -> pd.DataFrame:
    if not isinstance(display_df, pd.DataFrame) or display_df.empty or not numeric_cols:
        return display_df

    formatted = display_df.copy()
    for col in numeric_cols:
        if col in numeric_source.columns:
            formatted[col] = numeric_source[col].apply(format_preview_number)
    return formatted


def build_preview_summary(
    numeric_source: pd.DataFrame, numeric_cols: List[str]
) -> pd.DataFrame:
    if not numeric_cols:
        return pd.DataFrame(columns=["Sloupec", "Součet"])

    rows: List[Dict[str, str]] = []
    for col in numeric_cols:
        if col not in numeric_source.columns:
            continue
        series = numeric_source[col]
        total = series.sum(min_count=1)
        if pd.isna(total):
            continue
        rows.append({"Sloupec": col, "Součet": format_preview_number(total)})

    if not rows:
        return pd.DataFrame(columns=["Sloupec", "Součet"])

    return pd.DataFrame(rows)


def describe_summary_columns(numeric_cols: List[str], currency_label: Optional[str]) -> str:
    if not numeric_cols:
        return ""

    column_list = ", ".join(f"`{col}`" for col in numeric_cols)
    currency_note = (
        f" U finančních sloupců je použita měna {currency_label}."
        if currency_label
        else ""
    )
    return (
        "Součty níže vycházejí z numerických sloupců: "
        f"{column_list}. Hodnoty jsou zaokrouhleny na jedno desetinné místo a zobrazeny s mezerami mezi tisíci."
        f"{currency_note}"
    )


def filter_table_by_keys(table: Any, keys: Set[str]) -> pd.DataFrame:
    if not isinstance(table, pd.DataFrame) or table.empty or not keys:
        return pd.DataFrame()

    working = table.reset_index(drop=True)
    row_keys = extract_preview_row_keys(working)
    if not row_keys:
        return pd.DataFrame()

    key_series = pd.Series(row_keys, index=working.index)
    mask = key_series.isin(keys)
    return working.loc[mask].reset_index(drop=True)


def count_rows_by_keys(table: Any, keys: Set[str]) -> int:
    """Return the number of rows in ``table`` matching ``keys`` including duplicates."""

    if not isinstance(table, pd.DataFrame) or table.empty or not keys:
        return 0

    subset = filter_table_by_keys(table, keys)
    return int(len(subset))


def describe_preview_rows(table: Any, keys: Set[str], max_items: int = 10) -> str:
    if not keys:
        return ""

    subset = filter_table_by_keys(table, keys)
    if subset.empty:
        return ""

    prepared = prepare_preview_table(subset)
    lines: List[str] = []
    code_col = "code" if "code" in prepared.columns else None
    desc_col = "description" if "description" in prepared.columns else None

    for idx, (_, row) in enumerate(prepared.iterrows()):
        if idx >= max_items:
            break
        parts: List[str] = []
        if code_col:
            code_val = str(row.get(code_col, "")).strip()
            if code_val and code_val.lower() != "nan":
                parts.append(f"**{code_val}**")
        if desc_col:
            desc_val = str(row.get(desc_col, "")).strip()
            if desc_val and desc_val.lower() != "nan":
                if parts:
                    parts[-1] = f"{parts[-1]} — {desc_val}"
                else:
                    parts.append(desc_val)
        if not parts:
            parts.append(str({k: v for k, v in row.items() if not str(k).startswith("__")}))
        lines.append(f"- {parts[0]}")

    remaining = len(keys) - min(len(keys), max(0, len(lines)))
    if remaining > 0:
        lines.append(f"- … a další {remaining} položek.")

    return "\n".join(lines)


def dataframe_to_excel_bytes(df: pd.DataFrame, sheet_name: str) -> bytes:
    """Serialize a dataframe into XLSX bytes for download widgets."""

    buffer = io.BytesIO()
    safe_sheet = sheet_name[:31] or "Data"
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name=safe_sheet)
    buffer.seek(0)
    return buffer.getvalue()


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
            wb.sheets[s] = {
                "raw": raw,
                "mapping": mapping,
                "header_row": header_row,
                "table": tbl,
                "header_names": list(body.columns) if hasattr(body, "columns") else [],
                "preserve_summary_totals": False,
            }
        except Exception as e:
            wb.sheets[s] = {
                "raw": None,
                "mapping": {},
                "header_row": -1,
                "table": pd.DataFrame(),
                "error": str(e),
                "header_names": [],
                "preserve_summary_totals": False,
            }
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
        preserve_totals = bool(mobj.get("preserve_summary_totals"))

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
            table = build_normalized_table(
                body,
                new_mapping,
                preserve_summary_totals=preserve_totals,
            )
        except Exception:
            continue

        target_sheet.update(
            {
                "mapping": new_mapping,
                "header_row": target_header_row,
                "table": table,
                "header_names": header,
                "preserve_summary_totals": preserve_totals,
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
                table = build_normalized_table(
                    body,
                    ui_mapping,
                    preserve_summary_totals=use_minimal,
                )
            else:
                table = pd.DataFrame()

            wb.sheets[sheet]["mapping"] = ui_mapping
            wb.sheets[sheet]["header_row"] = header_row
            wb.sheets[sheet]["table"] = table
            wb.sheets[sheet]["preserve_summary_totals"] = use_minimal
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
    if "code" in df_local.columns:
        raw_codes = df_local["code"].copy()
    else:
        raw_codes = pd.Series(["" for _ in range(len(df_local))], index=df_local.index, dtype=object)
    code_series = raw_codes.map(normalize_join_value)

    if "description" in df_local.columns:
        raw_descriptions = df_local["description"].copy()
    else:
        raw_descriptions = pd.Series(["" for _ in range(len(df_local))], index=df_local.index, dtype=object)
    desc_series = raw_descriptions.map(normalize_join_value)

    key_df = pd.DataFrame({"code": code_series, "description": desc_series}, index=df_local.index)
    line_ids = key_df.groupby(["code", "description"], sort=False).cumcount()
    fallback = code_series + "||" + desc_series + "||" + line_ids.astype(str)

    if "item_id" in df_local.columns:
        item_series = df_local["item_id"].map(normalize_join_value)
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
    grouped["__direct_join__"] = grouped.index.astype(str)
    return grouped


def _choose_join_columns(
    master_lookup: pd.DataFrame, supplier_lookup: pd.DataFrame, join_mode: str
) -> Tuple[pd.Series, pd.Series]:
    """Return matching join key series for master and supplier tables."""

    empty_master_lookup = master_lookup.empty
    empty_supplier_lookup = supplier_lookup.empty

    master_has_item_join = "__item_join__" in master_lookup.columns and not empty_master_lookup
    supplier_has_item_join = "__item_join__" in supplier_lookup.columns and not empty_supplier_lookup

    use_item_ids = False
    if (
        join_mode != "code+description"
        and master_has_item_join
        and supplier_has_item_join
    ):
        master_ids = master_lookup["__item_join__"].fillna("").astype(str).str.strip()
        supplier_ids = supplier_lookup["__item_join__"].fillna("").astype(str).str.strip()
        master_non_empty = master_ids[master_ids != ""]
        supplier_non_empty = supplier_ids[supplier_ids != ""]
        master_has_ids = not master_non_empty.empty
        supplier_has_ids = not supplier_non_empty.empty
        if master_has_ids and supplier_has_ids:
            master_coverage = len(master_non_empty) / max(len(master_ids), 1)
            supplier_coverage = len(supplier_non_empty) / max(len(supplier_ids), 1)
            master_duplicates = len(master_non_empty) - master_non_empty.nunique(dropna=True)
            supplier_duplicates = len(supplier_non_empty) - supplier_non_empty.nunique(dropna=True)
            master_duplicate_share = master_duplicates / max(len(master_non_empty), 1)
            supplier_duplicate_share = supplier_duplicates / max(len(supplier_non_empty), 1)
            if (
                master_coverage >= 0.6
                and supplier_coverage >= 0.6
                and master_duplicate_share <= 0.4
                and supplier_duplicate_share <= 0.4
            ):
                use_item_ids = True

    master_col = "__item_join__" if use_item_ids else "__fallback_join__"
    supplier_col = "__item_join__" if use_item_ids else "__fallback_join__"

    master_series = master_lookup.get(master_col, pd.Series(dtype=object)).copy()
    supplier_series = supplier_lookup.get(supplier_col, pd.Series(dtype=object)).copy()

    if not master_series.empty:
        master_series.index = master_series.index.astype(str)
    if not supplier_series.empty:
        supplier_series.index = supplier_series.index.astype(str)

    if (
        join_mode != "code+description"
        and "__direct_join__" in master_lookup.columns
        and "__direct_join__" in supplier_lookup.columns
        and not empty_master_lookup
        and not empty_supplier_lookup
    ):
        master_direct = master_lookup["__direct_join__"].copy()
        supplier_direct = supplier_lookup["__direct_join__"].copy()
        master_direct.index = master_direct.index.astype(str)
        supplier_direct.index = supplier_direct.index.astype(str)
        common_keys = master_direct.index.intersection(supplier_direct.index)
        if len(common_keys) > 0:
            if master_series.empty:
                master_series = master_direct.copy()
            else:
                master_series.loc[common_keys] = master_direct.loc[common_keys]
            if supplier_series.empty:
                supplier_series = supplier_direct.copy()
            else:
                supplier_series.loc[common_keys] = supplier_direct.loc[common_keys]

    return master_series, supplier_series


def _has_valid_mapping_index(mapping: Dict[str, Any], key: str) -> bool:
    """Return ``True`` if mapping ``key`` points to a usable column index."""

    if not isinstance(mapping, dict) or key not in mapping:
        return False

    value = mapping.get(key)

    if isinstance(value, (int, np.integer)):
        return int(value) >= 0
    if isinstance(value, (float, np.floating)):
        if math.isnan(float(value)):
            return False
        return int(value) == value and int(value) >= 0
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return False
        try:
            numeric = int(float(stripped))
        except ValueError:
            return False
        return numeric >= 0

    return False


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

        numeric_master_cols = [
            col
            for col in ("quantity", "total_price", "unit_price_material", "unit_price_install")
            if col in mtab.columns
        ]
        for col in numeric_master_cols:
            base[col] = coerce_numeric(mtab[col])

        if "__row_order__" in mtab.columns:
            base["__row_order__"] = mtab["__row_order__"]

        price_cols_master = [
            col for col in ("unit_price_material", "unit_price_install") if col in base.columns
        ]

        agg_mapping: Dict[str, Any] = {
            "code": first_non_missing,
            "description": first_non_missing,
            "unit": first_non_missing,
        }
        if "item_id" in base.columns:
            agg_mapping["item_id"] = first_non_missing
        if "__row_order__" in base.columns:
            agg_mapping["__row_order__"] = "min"
        if "quantity" in base.columns:
            agg_mapping["quantity"] = sum_preserving_na
        if "total_price" in base.columns:
            agg_mapping["total_price"] = sum_preserving_na
        for price_col in price_cols_master:
            agg_mapping[price_col] = first_non_missing

        base_grouped = base.groupby("__key__", sort=False, as_index=False).agg(agg_mapping)
        master_lookup = _build_join_lookup(mtab)

        master_total_series = base_grouped.get("total_price")
        if isinstance(master_total_series, pd.Series):
            master_total_sum_value = sum_preserving_na(master_total_series)
            master_total_sum = (
                float(master_total_sum_value)
                if pd.notna(master_total_sum_value)
                else 0.0
            )
        else:
            master_total_sum = 0.0

        base_grouped.rename(columns={"total_price": "Master total"}, inplace=True)
        for price_col in price_cols_master:
            if price_col in base_grouped.columns:
                base_grouped.rename(
                    columns={price_col: f"Master {price_col}"}, inplace=True
                )

        comp = base_grouped.copy()
        if "quantity" in comp.columns and "Master quantity" not in comp.columns:
            comp["Master quantity"] = comp["quantity"]

        supplier_totals: Dict[str, float] = {}
        for sup_name, wb in bids.items():
            tobj = wb.sheets.get(sheet, {})
            ttab = tobj.get("table", pd.DataFrame())
            if ttab is None or ttab.empty:
                comp[f"{sup_name} quantity"] = np.nan
                comp[f"{sup_name} total"] = np.nan
                continue
            summary_mask_supplier = is_summary_like_row(ttab)
            if summary_mask_supplier.any():
                ttab = ttab.loc[~summary_mask_supplier].copy()
            ttab = ttab[ttab["description"].astype(str).str.strip() != ""]
            # join by __key__ (manual mapping already built in normalized table)
            supplier_mapping = tobj.get("mapping", {}) or {}
            cols = [
                "__key__",
                "quantity",
                "quantity_supplier",
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

            numeric_supplier_cols = [
                col
                for col in (
                    "quantity",
                    "quantity_supplier",
                    "total_price",
                    "unit_price_material",
                    "unit_price_install",
                )
                if col in tt.columns
            ]
            for col in numeric_supplier_cols:
                tt[col] = coerce_numeric(tt[col])

            if "__row_order__" in ttab.columns:
                tt["__row_order__"] = ttab["__row_order__"]

            qty_supplier_series = tt.get("quantity_supplier")
            supplier_qty_has_data = (
                isinstance(qty_supplier_series, pd.Series)
                and qty_supplier_series.notna().any()
            )
            has_quantity_supplier_mapping = _has_valid_mapping_index(
                supplier_mapping, "quantity_supplier"
            )
            if supplier_qty_has_data or has_quantity_supplier_mapping:
                sup_qty_col = "quantity_supplier"
            else:
                sup_qty_col = "quantity"
                if sup_qty_col not in tt.columns:
                    tt[sup_qty_col] = np.nan

            total_series = tt.get("total_price")
            total_sum_value = (
                sum_preserving_na(total_series)
                if isinstance(total_series, pd.Series)
                else float("nan")
            )
            supplier_totals[sup_name] = (
                float(total_sum_value) if pd.notna(total_sum_value) else 0.0
            )

            agg_supplier: Dict[str, Any] = {}
            if sup_qty_col in tt.columns:
                agg_supplier[sup_qty_col] = sum_preserving_na
            if "total_price" in tt.columns:
                agg_supplier["total_price"] = sum_preserving_na
            if "unit" in tt.columns:
                agg_supplier["unit"] = first_non_missing
            if "item_id" in tt.columns:
                agg_supplier["item_id"] = first_non_missing
            if "__row_order__" in tt.columns:
                agg_supplier["__row_order__"] = "min"
            component_cols: List[str] = []
            for price_component in ("unit_price_material", "unit_price_install"):
                if price_component in tt.columns:
                    agg_supplier[price_component] = first_non_missing
                    component_cols.append(price_component)

            if not agg_supplier:
                comp[f"{sup_name} quantity"] = np.nan
                comp[f"{sup_name} total"] = np.nan
                continue

            tt_grouped = tt.groupby("__key__", sort=False, as_index=False).agg(agg_supplier)

            master_join_series, supplier_join_series = _choose_join_columns(
                master_lookup, supplier_lookup, join_mode
            )
            comp_join_keys = comp["__key__"].astype(str).map(master_join_series)
            tt_grouped["__join_key__"] = tt_grouped["__key__"].astype(str).map(
                supplier_join_series
            )
            comp["__join_key__"] = comp_join_keys

            qty_merge_col = sup_qty_col
            if sup_qty_col == "quantity":
                qty_merge_col = f"__{sup_name}__quantity"
                if sup_qty_col in tt_grouped.columns:
                    tt_grouped.rename(columns={sup_qty_col: qty_merge_col}, inplace=True)

            merge_cols = ["__join_key__"]
            if qty_merge_col in tt_grouped.columns:
                merge_cols.append(qty_merge_col)
            if "total_price" in tt_grouped.columns:
                merge_cols.append("total_price")

            unit_merge_col: Optional[str] = None
            if "unit" in tt_grouped.columns:
                unit_merge_col = f"__{sup_name}__unit"
                tt_grouped.rename(columns={"unit": unit_merge_col}, inplace=True)
                merge_cols.append(unit_merge_col)

            for component_name in component_cols:
                if component_name in tt_grouped.columns:
                    merge_cols.append(component_name)

            comp = comp.merge(tt_grouped[merge_cols], on="__join_key__", how="left")
            comp.drop(columns=["__join_key__"], inplace=True, errors="ignore")

            rename_map: Dict[str, str] = {}
            if qty_merge_col in comp.columns:
                rename_map[qty_merge_col] = f"{sup_name} quantity"
            if "total_price" in merge_cols:
                rename_map["total_price"] = f"{sup_name} total"
            for component_name in component_cols:
                if component_name in comp.columns:
                    rename_map[component_name] = f"{sup_name} {component_name}"
            if unit_merge_col:
                rename_map[unit_merge_col] = f"{sup_name} unit"

            comp.rename(columns=rename_map, inplace=True)

            qty_col = f"{sup_name} quantity"
            if qty_col in comp.columns and "quantity" in comp.columns:
                comp[f"{sup_name} Δ qty"] = comp[qty_col] - comp["quantity"]
            else:
                comp[f"{sup_name} Δ qty"] = np.nan

        for sup_name, total_sum in supplier_totals.items():
            col = f"{sup_name} total"
            if col not in comp.columns:
                continue
            mapped_series = coerce_numeric(comp[col])
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
        try:
            sections_view, _, _, _, _ = overview_comparison(master, bids, sheet)
        except Exception:
            sections_view = pd.DataFrame()
        if isinstance(sections_view, pd.DataFrame) and not sections_view.empty:
            comp = align_total_columns(comp, sections_view)
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
        rename_map[f"{raw} unit_price_material"] = f"{alias} unit_price_material"
        rename_map[f"{raw} unit_price_install"] = f"{alias} unit_price_install"
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
    master_preserve_totals = bool(mobj.get("preserve_summary_totals"))
    mtab = mobj.get("table", pd.DataFrame())
    if (mtab is None or mtab.empty) and isinstance(mobj.get("raw"), pd.DataFrame):
        mapping, hdr, body = try_autodetect_mapping(mobj["raw"])
        if mapping:
            mtab = build_normalized_table(
                body,
                mapping,
                preserve_summary_totals=master_preserve_totals,
            )
    if mtab is None or mtab.empty:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
    if "is_summary" in mtab.columns and not master_preserve_totals:
        summary_mask = mtab["is_summary"].fillna(False).astype(bool)
        include_summary_other = summary_rows_included_as_items(mtab)
        if isinstance(include_summary_other, pd.Series):
            summary_mask &= ~include_summary_other.reindex(mtab.index, fill_value=False)
        mtab = mtab[~summary_mask]
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
        supplier_preserve_totals = bool(tobj.get("preserve_summary_totals"))
        ttab = tobj.get("table", pd.DataFrame())
        if (ttab is None or ttab.empty) and isinstance(tobj.get("raw"), pd.DataFrame):
            mapping, hdr, body = try_autodetect_mapping(tobj["raw"])
            if mapping:
                ttab = build_normalized_table(
                    body,
                    mapping,
                    preserve_summary_totals=supplier_preserve_totals,
                )
        if ttab is None or ttab.empty:
            df[f"{sup_name} total"] = np.nan
        else:
            if "is_summary" in ttab.columns and not supplier_preserve_totals:
                summary_mask = ttab["is_summary"].fillna(False).astype(bool)
                include_summary_other = summary_rows_included_as_items(ttab)
                if isinstance(include_summary_other, pd.Series):
                    summary_mask &= ~include_summary_other.reindex(ttab.index, fill_value=False)
                ttab = ttab[~summary_mask]
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
    summary_mask = df["is_summary"].fillna(False).astype(bool)
    include_summary_other = summary_rows_included_as_items(df)
    if isinstance(include_summary_other, pd.Series):
        summary_mask &= ~include_summary_other.reindex(df.index, fill_value=False)
    summaries = summary_mask.tolist()

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
            summary_mask = mtab["is_summary"].fillna(False).astype(bool)
            include_summary_other = summary_rows_included_as_items(mtab)
            if isinstance(include_summary_other, pd.Series):
                summary_mask &= ~include_summary_other.reindex(mtab.index, fill_value=False)
            mtab_clean = mtab[~summary_mask]
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
                    summary_mask = ttab["is_summary"].fillna(False).astype(bool)
                    include_summary_other = summary_rows_included_as_items(ttab)
                    if isinstance(include_summary_other, pd.Series):
                        summary_mask &= ~include_summary_other.reindex(ttab.index, fill_value=False)
                    ttab_clean = ttab[~summary_mask]
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

ensure_exchange_rate_state()

# ------------- Tabs -------------
tab_data, tab_preview, tab_compare, tab_compare2, tab_summary, tab_rekap, tab_dashboard, tab_qa = st.tabs([
    "📑 Mapování",
    "🧾 Kontrola dat",
    "⚖️ Porovnání",
    "⚖️ Porovnání 2",
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

with tab_preview:
    st.subheader("Kontrola načtených tabulek")

    st.markdown(
        """
        <style>
        .preview-table-wrapper .stTabs [role="tablist"] {
            margin-bottom: 0.25rem;
        }
        .preview-table-wrapper .stTabs [role="tabpanel"] > div:first-child {
            padding-top: 0 !important;
        }
        .preview-table-wrapper {
            display: flex;
            flex-direction: column;
            gap: 0.5rem;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )

    preview_sheets = [sheet for sheet in compare_sheets if sheet in master_wb.sheets]
    if not preview_sheets:
        st.info("Vyber alespoň jeden list pro zobrazení v levém panelu.")
    else:
        default_preview = preview_sheets[0]
        selected_preview_sheet = st.selectbox(
            "List pro kontrolu",
            preview_sheets,
            index=preview_sheets.index(default_preview) if default_preview in preview_sheets else 0,
            key="preview_sheet_select",
        )

        def render_preview_table(
            df: pd.DataFrame,
            sheet_label: str,
            table_label: str,
            widget_suffix: str,
            *,
            highlight_keys: Optional[Set[str]] = None,
            highlight_color: str = "#FFE8CC",
            currency_label: Optional[str] = None,
            summary_title: Optional[str] = None,
        ) -> str:
            prepared = prepare_preview_table(df)
            wrapper_id = f"preview-wrapper-{widget_suffix}"
            label_slug = re.sub(r"[^0-9A-Za-z_-]+", "-", str(table_label).strip().lower()).strip("-")
            if not label_slug:
                label_slug = "table"
            wrapper_class = f"preview-table-wrapper preview-{label_slug}"
            wrapper_container = st.container()
            with wrapper_container:
                st.markdown(
                    f"<div id=\"{wrapper_id}\" class=\"{wrapper_class}\">",
                    unsafe_allow_html=True,
                )

                row_count = len(prepared)
                if row_count == 0:
                    st.info("Tabulka je prázdná nebo list neobsahuje položky.")

                height = min(900, 220 + max(row_count, 1) * 28)

                numeric_source = pd.DataFrame()
                numeric_cols: List[str] = []
                row_keys: List[str] = []
                if isinstance(df, pd.DataFrame) and not df.empty:
                    numeric_source = df.reset_index(drop=True)
                    numeric_cols = [
                        col
                        for col in prepared.columns
                        if col in numeric_source.columns
                        and pd.api.types.is_numeric_dtype(numeric_source[col])
                    ]
                    row_keys = extract_preview_row_keys(df)

                    def _normalize_summary_label(label: Any) -> str:
                        text = str(label or "").strip().lower()
                        text = text.replace("_", " ")
                        return re.sub(r"\s+", " ", text)

                    summary_targets = {"total price"}
                    preferred_cols = [
                        col
                        for col in numeric_cols
                        if _normalize_summary_label(col) in summary_targets
                    ]
                    if preferred_cols:
                        numeric_cols = preferred_cols

                display_df = format_preview_numbers(prepared, numeric_source, numeric_cols)

                highlight_set: Set[str] = set()
                if highlight_keys:
                    highlight_set = {str(key).strip() for key in highlight_keys if str(key).strip()}

                if not display_df.empty:
                    if highlight_set and row_keys:
                        row_index = pd.Series(row_keys[: len(display_df)], index=display_df.index)
                        highlight_mask = row_index.isin(highlight_set)
                        if highlight_mask.any():
                            highlight_styles = pd.DataFrame(
                                "",
                                index=display_df.index,
                                columns=display_df.columns,
                            )
                            highlight_styles.loc[highlight_mask, :] = (
                                f"background-color: {highlight_color}"
                            )

                            def apply_styles(_: pd.DataFrame) -> pd.DataFrame:
                                return highlight_styles

                            styler = display_df.style.apply(apply_styles, axis=None)
                        else:
                            styler = display_df.style
                        st.dataframe(styler, use_container_width=True, height=height)
                    else:
                        st.dataframe(display_df, use_container_width=True, height=height)
                else:
                    st.dataframe(display_df, use_container_width=True, height=height)

                st.caption(f"{row_count} řádků")

                summary_df = build_preview_summary(numeric_source, numeric_cols)
                if not summary_df.empty:
                    heading = summary_title or f"Součty — {table_label}"
                    st.markdown(f"**{heading}**")
                    summary_desc = describe_summary_columns(numeric_cols, currency_label)
                    if summary_desc:
                        st.caption(summary_desc)
                    st.dataframe(summary_df, use_container_width=True, height=160)

                file_stub = sanitize_filename(f"{table_label}_{sheet_label}")
                csv_bytes = prepared.to_csv(index=False).encode("utf-8-sig")
                excel_bytes = dataframe_to_excel_bytes(prepared, sheet_label)
                export_cols = st.columns(2)
                export_cols[0].download_button(
                    "⬇️ Export CSV",
                    data=csv_bytes,
                    file_name=f"{file_stub}.csv",
                    mime="text/csv",
                    key=f"{widget_suffix}_csv",
                )
                export_cols[1].download_button(
                    "⬇️ Export XLSX",
                    data=excel_bytes,
                    file_name=f"{file_stub}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    key=f"{widget_suffix}_xlsx",
                )

                st.markdown("</div>", unsafe_allow_html=True)

            return wrapper_id

        def inject_preview_scroll_sync(
            master_wrapper: str,
            target_wrapper: str,
            widget_suffix: str,
            enabled: bool,
        ) -> None:
            if not master_wrapper or not target_wrapper:
                return

            script_template = Template(
                """
<script>
(function() {
    const masterId = $master_id;
    const targetId = $target_id;
    const enabled = $enabled;
    const componentKey = $component_key;
    const parentWindow = window.parent || window;
    if (!parentWindow) {
        return;
    }
    const parentDocument = parentWindow.document || document;
    if (!parentDocument) {
        return;
    }
    const syncRegistry = parentWindow.__previewTableSync = parentWindow.__previewTableSync || {};
    const selectors = [
        '[data-testid="stDataFrameResizable"] [role="grid"]',
        '[data-testid="stDataFrame"] [role="grid"]',
        '[data-testid="stDataFrameResizable"] .stDataFrame [role="grid"]',
        '[data-testid="stDataFrame"] .stDataFrame [role="grid"]',
        '[data-testid="stDataFrameResizable"] .stDataFrame',
        '[data-testid="stDataFrame"] .stDataFrame',
        '[data-testid="stDataFrameResizable"] [data-baseweb="table"]',
        '[data-testid="stDataFrame"] [data-baseweb="table"]',
        '[data-testid="stDataFrame"] [data-testid="styled-dataframe"]',
        '[data-testid="stDataFrame"] table',
        '[data-testid="stDataFrame"] [class*="stDataFrame"]',
        '.stDataFrame [role="grid"]',
        '.fixed-table',
        '.ag-theme-streamlit'
    ];

    function resolveScrollable(element) {
        if (!element || !element.ownerDocument) {
            return null;
        }
        let current = element;
        const visited = new Set();
        while (current && current.ownerDocument && !visited.has(current)) {
            visited.add(current);
            if (
                (current.scrollHeight > current.clientHeight) ||
                (current.scrollWidth > current.clientWidth)
            ) {
                return current;
            }
            current = current.parentElement;
        }
        return null;
    }

    function collectScopes(root) {
        const scopes = [];
        if (!root) {
            return scopes;
        }
        scopes.push(root);
        if (root.querySelectorAll) {
            const frames = root.querySelectorAll('iframe');
            for (const frame of frames) {
                if (!frame) {
                    continue;
                }
                try {
                    const frameDoc = frame.contentDocument || (frame.contentWindow && frame.contentWindow.document);
                    if (frameDoc) {
                        scopes.push(frameDoc);
                        if (frameDoc.body) {
                            scopes.push(frameDoc.body);
                        }
                        frame.addEventListener('load', () => setup(0), { once: true });
                    }
                } catch (err) {
                    continue;
                }
            }
        }
        return scopes;
    }

    function findScrollable(rootId) {
        const wrapper = parentDocument.getElementById(rootId);
        if (!wrapper) {
            return null;
        }
        const queue = collectScopes(wrapper);
        const visited = new Set(queue);
        while (queue.length) {
            const scope = queue.shift();
            if (!scope || !scope.querySelector) {
                continue;
            }
            for (const selector of selectors) {
                let element = null;
                try {
                    element = scope.querySelector(selector);
                } catch (err) {
                    element = null;
                }
                if (!element) {
                    continue;
                }
                if (element.tagName && element.tagName.toLowerCase() === 'iframe') {
                    try {
                        const doc = element.contentDocument || (element.contentWindow && element.contentWindow.document);
                        if (doc && !visited.has(doc)) {
                            visited.add(doc);
                            queue.push(doc);
                            if (doc.body && !visited.has(doc.body)) {
                                visited.add(doc.body);
                                queue.push(doc.body);
                            }
                            element.addEventListener('load', () => setup(0), { once: true });
                        }
                    } catch (err) {
                        continue;
                    }
                    continue;
                }
                const scrollable = resolveScrollable(element);
                if (scrollable) {
                    return scrollable;
                }
            }
            if (scope.querySelectorAll) {
                const nestedFrames = scope.querySelectorAll('iframe');
                for (const frame of nestedFrames) {
                    if (!frame || visited.has(frame)) {
                        continue;
                    }
                    try {
                        const doc = frame.contentDocument || (frame.contentWindow && frame.contentWindow.document);
                        if (doc && !visited.has(doc)) {
                            visited.add(doc);
                            queue.push(doc);
                            if (doc.body && !visited.has(doc.body)) {
                                visited.add(doc.body);
                                queue.push(doc.body);
                            }
                            frame.addEventListener('load', () => setup(0), { once: true });
                        }
                    } catch (err) {
                        continue;
                    }
                }
            }
        }
        if (wrapper && wrapper.querySelectorAll) {
            const fallbackElements = wrapper.querySelectorAll('*');
            for (const element of fallbackElements) {
                const scrollable = resolveScrollable(element);
                if (scrollable) {
                    return scrollable;
                }
            }
        }
        return null;
    }

    function watchWrapper(wrapperId) {
        const root = parentDocument.getElementById(wrapperId);
        if (!root || !parentWindow.MutationObserver) {
            return null;
        }
        const observer = new parentWindow.MutationObserver(() => {
            observer.disconnect();
            window.setTimeout(() => setup(0), 0);
        });
        observer.observe(root, { childList: true, subtree: true });
        return observer;
    }

    function setup(attempt) {
        const masterEl = findScrollable(masterId);
        const targetEl = findScrollable(targetId);
        if (!masterEl || !targetEl) {
            if (attempt < 40) {
                window.setTimeout(() => setup(attempt + 1), 250);
            }
            return;
        }

        const existing = syncRegistry[componentKey];
        if (existing) {
            if (existing.masterEl && existing.masterHandler) {
                existing.masterEl.removeEventListener('scroll', existing.masterHandler);
            }
            if (existing.targetEl && existing.targetHandler) {
                existing.targetEl.removeEventListener('scroll', existing.targetHandler);
            }
            if (existing.observers) {
                for (const obs of existing.observers) {
                    try {
                        obs.disconnect();
                    } catch (err) {
                        continue;
                    }
                }
            }
            delete syncRegistry[componentKey];
        }

        if (!enabled) {
            return;
        }

        let syncing = false;
        const masterHandler = () => {
            if (syncing) {
                return;
            }
            syncing = true;
            targetEl.scrollTop = masterEl.scrollTop;
            targetEl.scrollLeft = masterEl.scrollLeft;
            syncing = false;
        };
        const targetHandler = () => {
            if (syncing) {
                return;
            }
            syncing = true;
            masterEl.scrollTop = targetEl.scrollTop;
            masterEl.scrollLeft = targetEl.scrollLeft;
            syncing = false;
        };
        masterEl.addEventListener('scroll', masterHandler, { passive: true });
        targetEl.addEventListener('scroll', targetHandler, { passive: true });

        const observers = [];
        const masterObserver = watchWrapper(masterId);
        if (masterObserver) {
            observers.push(masterObserver);
        }
        const targetObserver = watchWrapper(targetId);
        if (targetObserver) {
            observers.push(targetObserver);
        }

        syncRegistry[componentKey] = {
            masterHandler: masterHandler,
            targetHandler: targetHandler,
            masterEl: masterEl,
            targetEl: targetEl,
            observers: observers
        };
    }

    setup(0);
})();
</script>
"""
            )

            script = script_template.substitute(
                master_id=json.dumps(master_wrapper),
                target_id=json.dumps(target_wrapper),
                enabled=str(enabled).lower(),
                component_key=json.dumps(widget_suffix),
            )

            try:
                components.html(
                    script,
                    height=1,
                    key=f"preview_sync_script_{widget_suffix}",
                )
            except Exception as exc:  # pragma: no cover - guard against Streamlit quirks
                logging.getLogger(__name__).warning(
                    "Failed to initialize preview scroll sync for %s: %s",
                    widget_suffix,
                    exc,
                )

        master_sheet = master_wb.sheets.get(selected_preview_sheet, {})
        master_table = master_sheet.get("table", pd.DataFrame())

        sync_scroll_enabled = st.checkbox(
            "🔒 Zamknout společné rolování tabulek",
            key="preview_sync_scroll_enabled",
            help="Při zapnutí se Master a vybraná nabídka posouvají zároveň.",
        )

        master_keys_set = extract_preview_key_set(master_table)
        master_highlight_keys: Set[str] = set()
        supplier_missing_map: Dict[str, Set[str]] = {}
        supplier_extra_map: Dict[str, Set[str]] = {}
        discrepancy_frames: List[pd.DataFrame] = []

        def build_discrepancy_frame(
            source_table: pd.DataFrame,
            keys: Set[str],
            supplier_alias: str,
            diff_label: str,
        ) -> pd.DataFrame:
            if not isinstance(source_table, pd.DataFrame) or source_table.empty or not keys:
                return pd.DataFrame()

            subset = filter_table_by_keys(source_table, keys)
            if subset.empty:
                return pd.DataFrame()

            subset = subset.reset_index(drop=True)
            prepared_subset = prepare_preview_table(subset)
            if prepared_subset.empty:
                return pd.DataFrame()

            numeric_cols = [
                col
                for col in prepared_subset.columns
                if col in subset.columns and pd.api.types.is_numeric_dtype(subset[col])
            ]
            formatted_subset = format_preview_numbers(
                prepared_subset, subset, numeric_cols
            )
            formatted_subset.insert(0, "Typ rozdílu", diff_label)
            formatted_subset.insert(0, "Dodavatel", supplier_alias)
            formatted_subset.insert(0, "List", selected_preview_sheet)
            return formatted_subset

        if bids_dict:
            for sup_name, wb in bids_dict.items():
                alias = display_names.get(sup_name, sup_name)
                sheet_obj = wb.sheets.get(selected_preview_sheet)
                if sheet_obj is None:
                    continue
                supplier_table = sheet_obj.get("table", pd.DataFrame())
                supplier_keys = extract_preview_key_set(supplier_table)
                missing_keys = master_keys_set - supplier_keys
                extra_keys = supplier_keys - master_keys_set
                supplier_missing_map[alias] = missing_keys
                supplier_extra_map[alias] = extra_keys
                master_highlight_keys.update(missing_keys)

                missing_frame = build_discrepancy_frame(
                    master_table, missing_keys, alias, "Chybí v nabídce"
                )
                if not missing_frame.empty:
                    discrepancy_frames.append(missing_frame)

                extra_frame = build_discrepancy_frame(
                    supplier_table, extra_keys, alias, "Položka navíc"
                )
                if not extra_frame.empty:
                    discrepancy_frames.append(extra_frame)

        master_wrapper_id = ""
        cols_preview = st.columns(2)
        with cols_preview[0]:
            master_tab_label = f"Master — {selected_preview_sheet}"
            master_tab, = st.tabs([master_tab_label])
            with master_tab:
                master_widget_suffix = make_widget_key(
                    "preview", selected_preview_sheet, "master"
                )
                master_wrapper_id = render_preview_table(
                    master_table,
                    selected_preview_sheet,
                    "master",
                    master_widget_suffix,
                    highlight_keys=master_highlight_keys,
                    highlight_color="#FFE3E3",
                    currency_label=currency,
                    summary_title="Součty — Master",
                )
                if master_highlight_keys:
                    missing_lines = []
                    for alias, missing in supplier_missing_map.items():
                        if not missing:
                            continue
                        missing_count = count_rows_by_keys(master_table, missing)
                        missing_lines.append(f"- {alias}: {missing_count} řádků chybí")
                    if missing_lines:
                        st.caption(
                            "Červeně zvýrazněné řádky chybí v těchto nabídkách:\n"
                            + "\n".join(missing_lines)
                        )

        with cols_preview[1]:
            if not bids_dict:
                st.info("Nahraj alespoň jednu nabídku dodavatele v levém panelu.")
            else:
                supplier_tabs = st.tabs([display_names.get(name, name) for name in bids_dict.keys()])
                for tab, (sup_name, wb) in zip(supplier_tabs, bids_dict.items()):
                    alias = display_names.get(sup_name, sup_name)
                    with tab:
                        sheet_obj = wb.sheets.get(selected_preview_sheet)
                        if sheet_obj is None:
                            st.warning("Tento list nebyl v nabídce nalezen.")
                            continue
                        else:
                            supplier_table = sheet_obj.get("table", pd.DataFrame())
                            supplier_widget_suffix = make_widget_key(
                                "preview",
                                selected_preview_sheet,
                                alias,
                            )
                            supplier_wrapper_id = render_preview_table(
                                supplier_table,
                                selected_preview_sheet,
                                alias,
                                supplier_widget_suffix,
                                highlight_keys=supplier_extra_map.get(alias, set()),
                                highlight_color="#FFF0D6",
                                currency_label=currency,
                                summary_title=f"Součty — {alias}",
                            )
                            missing_keys = supplier_missing_map.get(alias, set())
                            extra_keys = supplier_extra_map.get(alias, set())
                            if missing_keys:
                                missing_count = count_rows_by_keys(
                                    master_table, missing_keys
                                )
                                st.error(
                                    f"Chybí {missing_count} řádků oproti Master."
                                )
                                missing_desc = describe_preview_rows(
                                    master_table, missing_keys
                                )
                                if missing_desc:
                                    st.markdown(missing_desc)
                            if extra_keys:
                                extra_count = count_rows_by_keys(
                                    supplier_table, extra_keys
                                )
                                st.info(
                                    f"Dodavatel obsahuje {extra_count} řádků navíc oproti Master."
                                )
                                extra_desc = describe_preview_rows(
                                    supplier_table, extra_keys
                                )
                                if extra_desc:
                                    st.markdown(extra_desc)
                            inject_preview_scroll_sync(
                                master_wrapper_id,
                                supplier_wrapper_id,
                                make_widget_key(
                                    "preview",
                                    selected_preview_sheet,
                                    alias,
                                    "sync",
                                ),
                                sync_scroll_enabled,
                            )

        discrepancy_table = (
            pd.concat(discrepancy_frames, ignore_index=True, sort=False)
            if discrepancy_frames
            else pd.DataFrame()
        )
        if not discrepancy_table.empty:
            base_columns = ["List", "Dodavatel", "Typ rozdílu"]
            ordered_columns = base_columns + [
                col for col in discrepancy_table.columns if col not in base_columns
            ]
            discrepancy_table = discrepancy_table.reindex(columns=ordered_columns)

            st.markdown("### Kompletní seznam rozdílů")
            st.caption(
                "Tabulka obsahuje všechny chybějící nebo přidané položky pro vybraný list."
            )
            diff_height = min(900, 220 + max(len(discrepancy_table), 1) * 28)
            st.dataframe(discrepancy_table, use_container_width=True, height=diff_height)

            diff_stub = sanitize_filename(f"rozdily_{selected_preview_sheet}")
            diff_csv = discrepancy_table.to_csv(index=False).encode("utf-8-sig")
            diff_xlsx = dataframe_to_excel_bytes(
                discrepancy_table, f"Rozdíly — {selected_preview_sheet}"
            )
            diff_cols = st.columns(2)
            diff_cols[0].download_button(
                "⬇️ Export rozdílů CSV",
                data=diff_csv,
                file_name=f"{diff_stub}.csv",
                mime="text/csv",
                key=f"{selected_preview_sheet}_diff_csv",
            )
            diff_cols[1].download_button(
                "⬇️ Export rozdílů XLSX",
                data=diff_xlsx,
                file_name=f"{diff_stub}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key=f"{selected_preview_sheet}_diff_xlsx",
            )
        elif bids_dict:
            st.info("Žádné rozdíly mezi Master a nabídkami nebyly nalezeny.")

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
                default_range = st.session_state.get("compare_threshold_range", (-10.0, 10.0))
                threshold_min, threshold_max = st.slider(
                    "Rozmezí odchylky vs Master (%)",
                    min_value=-200.0,
                    max_value=200.0,
                    value=default_range,
                    step=0.5,
                    help="Rozsah procentní odchylky, který se považuje za přijatelný. Hodnoty mimo rozsah budou zvýrazněny.",
                    key=make_widget_key("compare_threshold", selected_sheet),
                )
                st.session_state["compare_threshold_range"] = (threshold_min, threshold_max)
                analysis_df = dataset.analysis_df.copy()

                available_metric_keys = []
                for key in COMPARISON_METRIC_ORDER:
                    config = COMPARISON_METRICS_CONFIG.get(key)
                    if not config:
                        continue
                    master_col = next(
                        (col for col in config["master_columns"] if col in analysis_df.columns),
                        None,
                    )
                    if not master_col:
                        continue
                    supplier_available = False
                    for supplier_alias in dataset.suppliers:
                        supplier_col = f"{supplier_alias}{config['supplier_suffix']}"
                        if supplier_col in analysis_df.columns:
                            supplier_available = True
                            break
                    if supplier_available:
                        available_metric_keys.append(key)

                if not available_metric_keys:
                    st.info("Pro vybraný list nejsou k dispozici žádné srovnatelné parametry.")
                else:
                    selected_metrics_raw = st.multiselect(
                        "Parametry k porovnání",
                        options=available_metric_keys,
                        default=available_metric_keys,
                        format_func=lambda key: COMPARISON_METRICS_CONFIG[key]["label"],
                        key=make_widget_key("compare_metric_select", selected_sheet),
                    )
                    if not selected_metrics_raw:
                        st.warning("Vyber alespoň jeden parametr pro zobrazení.")
                    else:
                        selected_metrics = [
                            key
                            for key in COMPARISON_METRIC_ORDER
                            if key in selected_metrics_raw
                        ]
                        st.caption(
                            "Porovnání se provádí pouze na položkách s dostupnými a nenulovými hodnotami u Master i dodavatele."
                        )

                        supplier_aliases = [alias for alias in dataset.suppliers if alias]
                        if not supplier_aliases:
                            st.info("Žádný z dodavatelů neobsahuje data pro vybraný list.")
                        else:

                            def resolve_master_column(df: pd.DataFrame, candidates) -> Optional[str]:
                                for col in candidates:
                                    if col in df.columns:
                                        return col
                                return None

                            def build_supplier_view(supplier_alias: str) -> Dict[str, Any]:
                                metric_frames: List[pd.DataFrame] = []
                                metric_column_map: Dict[str, Dict[str, str]] = {}
                                used_metrics: List[str] = []
                                for metric_key in selected_metrics:
                                    config = COMPARISON_METRICS_CONFIG.get(metric_key)
                                    if not config:
                                        continue
                                    master_col = resolve_master_column(analysis_df, config["master_columns"])
                                    supplier_col = f"{supplier_alias}{config['supplier_suffix']}"
                                    if not master_col or supplier_col not in analysis_df.columns:
                                        continue
                                    master_values = coerce_numeric(analysis_df[master_col])
                                    supplier_values = coerce_numeric(analysis_df[supplier_col])
                                    diff_values = supplier_values - master_values
                                    pct_values = compute_percent_difference(supplier_values, master_values)
                                    label = config["label"]
                                    metric_frame = pd.DataFrame(
                                        {
                                            f"{label} — Master": master_values,
                                            f"{label} — {supplier_alias}": supplier_values,
                                            f"{label} — Rozdíl": diff_values,
                                            f"{label} — Δ (%)": pct_values,
                                        },
                                        index=analysis_df.index,
                                    )
                                    metric_frames.append(metric_frame)
                                    metric_column_map[metric_key] = {
                                        "master": f"{label} — Master",
                                        "supplier": f"{label} — {supplier_alias}",
                                        "diff": f"{label} — Rozdíl",
                                        "pct": f"{label} — Δ (%)",
                                    }
                                    used_metrics.append(metric_key)

                                if not used_metrics:
                                    return {"available": False, "message": "Dodavatel neobsahuje vybrané parametry."}

                                display_df = pd.DataFrame(index=analysis_df.index)
                                if "code" in analysis_df.columns:
                                    display_df["Kód"] = analysis_df["code"]
                                if "description" in analysis_df.columns:
                                    display_df["Popis"] = analysis_df["description"]
                                if "unit" in analysis_df.columns:
                                    display_df["Jednotka"] = analysis_df["unit"]
                                for frame in metric_frames:
                                    display_df = pd.concat([display_df, frame], axis=1)

                                relevant_mask = pd.Series(False, index=display_df.index, dtype=bool)
                                diff_mask = pd.Series(False, index=display_df.index, dtype=bool)
                                threshold_mask = pd.Series(False, index=display_df.index, dtype=bool)

                                for metric_key in used_metrics:
                                    cols = metric_column_map[metric_key]
                                    master_vals = coerce_numeric(display_df[cols["master"]])
                                    supplier_vals = coerce_numeric(display_df[cols["supplier"]])
                                    has_data = ~(master_vals.fillna(0).eq(0) & supplier_vals.fillna(0).eq(0))
                                    has_data &= ~(master_vals.isna() & supplier_vals.isna())
                                    relevant_mask |= has_data
                                    diff_vals = coerce_numeric(display_df[cols["diff"]])
                                    diff_mask |= diff_vals.fillna(0).abs() > 1e-9
                                    pct_vals = coerce_numeric(display_df[cols["pct"]])
                                    threshold_mask |= (pct_vals > threshold_max) | (pct_vals < threshold_min)

                                display_df = display_df.loc[relevant_mask].copy()
                                diff_mask = diff_mask.loc[display_df.index]
                                threshold_mask = threshold_mask.loc[display_df.index]
                                differences_df = display_df.loc[diff_mask].copy()

                                summary_stats: Dict[str, Any] = {
                                    "supplier": supplier_alias,
                                    "relevant_rows": int(len(display_df)),
                                    "missing_count": 0,
                                }

                                master_total_col = resolve_master_column(
                                    analysis_df,
                                    COMPARISON_METRICS_CONFIG["total"]["master_columns"],
                                ) if "total" in COMPARISON_METRICS_CONFIG else None
                                supplier_total_col = (
                                    f"{supplier_alias}{COMPARISON_METRICS_CONFIG['total']['supplier_suffix']}"
                                    if "total" in COMPARISON_METRICS_CONFIG
                                    else None
                                )
                                missing_df = pd.DataFrame()
                                if (
                                    master_total_col
                                    and supplier_total_col
                                    and master_total_col in analysis_df.columns
                                    and supplier_total_col in analysis_df.columns
                                ):
                                    master_totals = coerce_numeric(analysis_df[master_total_col])
                                    supplier_totals = coerce_numeric(analysis_df[supplier_total_col])
                                    missing_mask_all = master_totals.fillna(0).ne(0) & supplier_totals.isna()
                                    missing_count = int(missing_mask_all.sum())
                                    summary_stats["missing_count"] = missing_count
                                    if missing_count:
                                        keep_cols = ["code", "description", "Oddíl", master_total_col]
                                        existing_cols = [col for col in keep_cols if col in analysis_df.columns]
                                        missing_df = analysis_df.loc[missing_mask_all, existing_cols].copy()
                                        rename_map = {}
                                        if "code" in missing_df.columns:
                                            rename_map["code"] = "Kód"
                                        if "description" in missing_df.columns:
                                            rename_map["description"] = "Popis"
                                        if master_total_col in missing_df.columns:
                                            rename_map[master_total_col] = f"Master celkem ({currency})"
                                        missing_df.rename(columns=rename_map, inplace=True)

                                    if "total" in metric_column_map:
                                        total_cols = metric_column_map["total"]
                                        total_master = coerce_numeric(display_df[total_cols["master"]])
                                        total_supplier = coerce_numeric(display_df[total_cols["supplier"]])
                                        total_diff = coerce_numeric(display_df[total_cols["diff"]])
                                        total_pct = coerce_numeric(display_df[total_cols["pct"]])
                                    priced_mask = total_master.notna() & total_supplier.notna()
                                    priced_count = int(priced_mask.sum())
                                    expensive_mask = (total_diff > 0) & priced_mask
                                    cheaper_mask = (total_diff < 0) & priced_mask
                                    outside_mask = (priced_mask & ((total_pct > threshold_max) | (total_pct < threshold_min)))
                                    summary_stats.update(
                                        {
                                            "priced_count": priced_count,
                                            "expensive_count": int(expensive_mask.sum()),
                                            "cheaper_count": int(cheaper_mask.sum()),
                                            "outside_count": int(outside_mask.sum()),
                                            "cheaper_pct": float(cheaper_mask.mean() * 100) if priced_count else np.nan,
                                            "expensive_pct": float(expensive_mask.mean() * 100) if priced_count else np.nan,
                                            "outside_range_pct": float(outside_mask.mean() * 100) if priced_count else np.nan,
                                            "avg_pct": float(total_pct.loc[priced_mask].mean()) if priced_count else np.nan,
                                            "abs_diff_sum": float(total_diff.loc[priced_mask].abs().sum()) if priced_count else 0.0,
                                            "total_diff_sum": float(total_diff.loc[priced_mask].sum()) if priced_count else 0.0,
                                        }
                                    )
                                else:
                                    summary_stats.update(
                                        {
                                            "priced_count": 0,
                                            "expensive_count": 0,
                                            "cheaper_count": 0,
                                            "outside_count": 0,
                                            "cheaper_pct": np.nan,
                                            "expensive_pct": np.nan,
                                            "outside_range_pct": np.nan,
                                            "avg_pct": np.nan,
                                            "abs_diff_sum": 0.0,
                                            "total_diff_sum": 0.0,
                                        }
                                    )

                                return {
                                    "available": True,
                                    "display": display_df,
                                    "differences": differences_df,
                                    "metrics": used_metrics,
                                    "metric_columns": metric_column_map,
                                    "summary": summary_stats,
                                    "missing": missing_df,
                                    "threshold_mask": threshold_mask,
                                }

                            supplier_views: Dict[str, Dict[str, Any]] = {}
                            for alias in supplier_aliases:
                                view = build_supplier_view(alias)
                                if view.get("available"):
                                    supplier_views[alias] = view

                            if not supplier_views:
                                st.info("Dodavatelé neobsahují žádné položky odpovídající vybraným parametrům.")
                            else:
                                summary_rows: List[Dict[str, Any]] = []
                                supplier_tabs = st.tabs(list(supplier_views.keys()) + ["Souhrn dodavatelů"])

                                def _format_pct(value: Any) -> str:
                                    if pd.isna(value):
                                        return "—"
                                    return f"{value:.1f} %"

                                def _style_diff(value: Any) -> str:
                                    if pd.isna(value) or abs(float(value)) < 1e-9:
                                        return ""
                                    return "background-color: #ffe3e3" if value > 0 else "background-color: #e5f5e0"

                                def _style_pct(value: Any) -> str:
                                    if pd.isna(value):
                                        return ""
                                    if value > threshold_max:
                                        return "background-color: #ffe3e3"
                                    if value < threshold_min:
                                        return "background-color: #e5f5e0"
                                    return ""

                                for idx, (alias, view) in enumerate(supplier_views.items()):
                                    with supplier_tabs[idx]:
                                        display_df = view["display"]
                                        differences_df = view["differences"]
                                        metrics_used = view["metrics"]
                                        metric_column_map = view["metric_columns"]
                                        summary_stats = view["summary"]
                                        missing_df = view["missing"]
                                        summary_rows.append(summary_stats)

                                        metric_cols = st.columns(4)
                                        metric_cols[0].metric("Chybějící položky", str(summary_stats.get("missing_count", 0)))
                                        priced = summary_stats.get("priced_count", 0)
                                        metric_cols[1].metric(
                                            "Dražší než Master",
                                            f"{summary_stats.get('expensive_count', 0)} ({_format_pct(summary_stats.get('expensive_pct'))})",
                                        )
                                        metric_cols[2].metric(
                                            "Levnější než Master",
                                            f"{summary_stats.get('cheaper_count', 0)} ({_format_pct(summary_stats.get('cheaper_pct'))})",
                                        )
                                        metric_cols[3].metric(
                                            "Průměrná odchylka",
                                            _format_pct(summary_stats.get("avg_pct")),
                                        )

                                        column_config: Dict[str, Any] = {}
                                        for col in ["Kód", "Popis", "Jednotka", "Oddíl"]:
                                            if col in display_df.columns:
                                                column_config[col] = st.column_config.TextColumn(col, disabled=True)

                                        diff_columns: List[str] = []
                                        pct_columns: List[str] = []
                                        for metric_key in metrics_used:
                                            config = COMPARISON_METRICS_CONFIG.get(metric_key, {})
                                            columns = metric_column_map.get(metric_key, {})
                                            label = config.get("label", metric_key)
                                            number_format = config.get("number_format", "number")
                                            unit_note = f" ({currency})" if number_format == "currency" else ""
                                            fmt = "%.2f" if number_format == "currency" else "%.3f"
                                            master_col = columns.get("master")
                                            supplier_col = columns.get("supplier")
                                            diff_col = columns.get("diff")
                                            pct_col = columns.get("pct")
                                            if master_col in display_df.columns:
                                                column_config[master_col] = st.column_config.NumberColumn(
                                                    f"{label} — Master{unit_note}",
                                                    format=fmt,
                                                    help=config.get("help"),
                                                )
                                            if supplier_col in display_df.columns:
                                                column_config[supplier_col] = st.column_config.NumberColumn(
                                                    f"{label} — {alias}{unit_note}",
                                                    format=fmt,
                                                    help=config.get("help"),
                                                )
                                            if diff_col in display_df.columns:
                                                column_config[diff_col] = st.column_config.NumberColumn(
                                                    f"{label} — Rozdíl{unit_note}",
                                                    format=fmt,
                                                    help=f"Rozdíl hodnot dodavatele {alias} vůči Master.",
                                                )
                                                diff_columns.append(diff_col)
                                            if pct_col in display_df.columns:
                                                column_config[pct_col] = st.column_config.NumberColumn(
                                                    f"{label} — Δ (%)",
                                                    format="%.2f",
                                                    help="Procentní rozdíl oproti Master.",
                                                )
                                                pct_columns.append(pct_col)

                                        styled_display = display_df.style
                                        if diff_columns:
                                            styled_display = styled_display.applymap(_style_diff, subset=diff_columns)
                                        if pct_columns:
                                            styled_display = styled_display.applymap(_style_pct, subset=pct_columns)

                                        st.markdown("#### Kompletní přehled")
                                        st.dataframe(
                                            styled_display,
                                            use_container_width=True,
                                            hide_index=True,
                                            column_config=column_config,
                                        )

                                        st.markdown("#### Položky s rozdíly")
                                        if differences_df.empty:
                                            st.info("Všechny vybrané parametry odpovídají Master.")
                                        else:
                                            differences_styled = differences_df.style
                                            if diff_columns:
                                                differences_styled = differences_styled.applymap(_style_diff, subset=diff_columns)
                                            if pct_columns:
                                                differences_styled = differences_styled.applymap(_style_pct, subset=pct_columns)
                                            st.dataframe(
                                                differences_styled,
                                                use_container_width=True,
                                                hide_index=True,
                                                column_config=column_config,
                                            )
                                            export_stub = sanitize_filename(f"{selected_sheet}_{alias}_rozdily")
                                            export_bytes = dataframe_to_excel_bytes(
                                                differences_df.reset_index(drop=True),
                                                f"Rozdily — {alias}",
                                            )
                                            st.download_button(
                                                "⬇️ Export rozdílové tabulky XLSX",
                                                data=export_bytes,
                                                file_name=f"{export_stub}.xlsx",
                                                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                            )

                                        with st.expander("Chybějící položky v nabídce", expanded=False):
                                            if missing_df.empty:
                                                st.write("Dodavatel ocenil všechny položky Master pro vybrané parametry.")
                                            else:
                                                st.caption(
                                                    "Položky, které jsou uvedeny v Master BoQ, ale dodavatel je neocenil (nebo ponechal nulovou hodnotu)."
                                                )
                                                st.dataframe(
                                                    missing_df,
                                                    use_container_width=True,
                                                    hide_index=True,
                                                )

                                with supplier_tabs[-1]:
                                    st.markdown("### Souhrn napříč dodavateli")
                                    if not summary_rows:
                                        st.info("Žádné údaje k sumarizaci.")
                                    else:
                                        summary_df = pd.DataFrame(summary_rows)
                                        percent_cols = [
                                            "cheaper_pct",
                                            "expensive_pct",
                                            "outside_range_pct",
                                            "avg_pct",
                                        ]
                                        for col in percent_cols:
                                            if col in summary_df.columns:
                                                summary_df[col] = summary_df[col].apply(_format_pct)
                                        if "abs_diff_sum" in summary_df.columns:
                                            summary_df["abs_diff_sum"] = summary_df["abs_diff_sum"].apply(
                                                lambda v: format_number(v) if pd.notna(v) else "—"
                                            )
                                        if "total_diff_sum" in summary_df.columns:
                                            summary_df["total_diff_sum"] = summary_df["total_diff_sum"].apply(
                                                lambda v: format_number(v) if pd.notna(v) else "—"
                                            )
                                        rename_map = {
                                            "supplier": "Dodavatel",
                                            "relevant_rows": "Porovnávané položky",
                                            "missing_count": "Chybějící položky",
                                            "priced_count": "Oceněné položky",
                                            "expensive_count": "Dražší než Master",
                                            "cheaper_count": "Levnější než Master",
                                            "outside_count": f"Mimo toleranci ({threshold_min} až {threshold_max} %)",
                                            "cheaper_pct": "Levnější (%)",
                                            "expensive_pct": "Dražší (%)",
                                            "outside_range_pct": "Mimo toleranci (%)",
                                            "avg_pct": "Průměrná odchylka (%)",
                                            "abs_diff_sum": f"Součet abs. rozdílů ({currency})",
                                            "total_diff_sum": f"Součet rozdílů ({currency})",
                                        }
                                        summary_display = summary_df.rename(columns=rename_map)
                                        st.dataframe(
                                            summary_display,
                                            use_container_width=True,
                                            hide_index=True,
                                        )

                                        total_missing = sum(row.get("missing_count", 0) for row in summary_rows)
                                        total_abs = sum(
                                            float(row.get("abs_diff_sum", 0.0))
                                            for row in summary_rows
                                            if pd.notna(row.get("abs_diff_sum"))
                                        )
                                        total_outside = sum(row.get("outside_count", 0) for row in summary_rows)
                                        kpi_cols = st.columns(3)
                                        kpi_cols[0].metric("Celkem chybějících položek", str(total_missing))
                                        kpi_cols[1].metric(
                                            "Součet abs. rozdílů",
                                            f"{format_number(total_abs)} {currency}",
                                        )
                                        kpi_cols[2].metric(
                                            "Položky mimo toleranci",
                                            str(total_outside),
                                        )

                                        if summary_rows:
                                            chart_source = pd.DataFrame(
                                                {
                                                    "Dodavatel": [row.get("supplier") for row in summary_rows],
                                                    "Součet rozdílů": [
                                                        row.get("total_diff_sum", 0.0)
                                                        if pd.notna(row.get("total_diff_sum"))
                                                        else 0.0
                                                        for row in summary_rows
                                                    ],
                                                }
                                            )
                                            st.bar_chart(
                                                chart_source.set_index("Dodavatel"),
                                                use_container_width=True,
                                            )
with tab_compare2:
    if not bids_dict:
        st.info("Nahraj alespoň jednu nabídku dodavatele v levém panelu.")
    elif not comparison_datasets:
        st.info("Nebyla nalezena data pro porovnání. Zkontroluj mapování nebo vyber jiné listy.")
    else:
        available_sheets = [
            sheet
            for sheet, dataset in comparison_datasets.items()
            if dataset is not None and not dataset.analysis_df.empty
        ]
        if not available_sheets:
            st.info("Listy určené k porovnání jsou prázdné. Zkontroluj zdrojová data.")
        else:
            default_sheet = available_sheets[0]
            sheet_index = (
                available_sheets.index(default_sheet)
                if default_sheet in available_sheets
                else 0
            )
            selected_sheet = st.selectbox(
                "Vyber list pro zobrazení",
                available_sheets,
                index=sheet_index,
                key="compare2_sheet_select",
            )
            dataset = comparison_datasets.get(selected_sheet)
            if dataset is None or dataset.analysis_df.empty:
                st.warning("Vybraný list neobsahuje žádné položky k zobrazení.")
            else:
                supplier_aliases = [alias for alias in dataset.suppliers if alias]
                if not supplier_aliases:
                    st.info("Žádný z dodavatelů neobsahuje data pro vybraný list.")
                else:
                    supplier_index = 0 if len(supplier_aliases) else None
                    selected_supplier = st.selectbox(
                        "Vyber dodavatele",
                        supplier_aliases,
                        index=supplier_index,
                        key=make_widget_key("compare2_supplier_select", selected_sheet),
                    )
                    alias_lookup = {alias: raw for raw, alias in display_names.items()}
                    raw_supplier_name = alias_lookup.get(selected_supplier, selected_supplier)

                    def prepare_table_for_join(source_df: Any) -> pd.DataFrame:
                        if not isinstance(source_df, pd.DataFrame) or source_df.empty:
                            return pd.DataFrame()
                        if "description" not in source_df.columns:
                            return pd.DataFrame()
                        working = source_df.copy()
                        working["description"] = working["description"].astype(str)
                        working = working[working["description"].str.strip() != ""].copy()
                        if working.empty:
                            return pd.DataFrame()
                        working["__desc_key__"] = working["description"].map(normalize_text)
                        working["__desc_key__"] = working["__desc_key__"].fillna("")
                        working["__desc_order__"] = working.groupby("__desc_key__").cumcount()
                        working["__join_key__"] = (
                            working["__desc_key__"].astype(str)
                            + "#"
                            + working["__desc_order__"].astype(str)
                        )
                        if "__row_order__" in working.columns:
                            working["__sort_order__"] = working["__row_order__"]
                        else:
                            working["__sort_order__"] = np.arange(len(working))
                        return working

                    master_source = master_wb.sheets.get(selected_sheet, {}).get(
                        "table", pd.DataFrame()
                    )
                    master_table = (
                        master_source.copy()
                        if isinstance(master_source, pd.DataFrame)
                        else pd.DataFrame()
                    )
                    supplier_table = pd.DataFrame()
                    supplier_wb = bids_dict.get(raw_supplier_name)
                    if supplier_wb is not None:
                        supplier_source = supplier_wb.sheets.get(selected_sheet, {}).get(
                            "table", pd.DataFrame()
                        )
                        if isinstance(supplier_source, pd.DataFrame):
                            supplier_table = supplier_source.copy()

                    master_prepared = prepare_table_for_join(master_table)
                    supplier_prepared = prepare_table_for_join(supplier_table)

                    if master_prepared.empty and supplier_prepared.empty:
                        st.warning(
                            "Nepodařilo se najít položky s popisem pro Master ani vybraného dodavatele."
                        )
                    else:
                        join_suffix = (" — Master", f" — {selected_supplier}")
                        combined = pd.merge(
                            master_prepared,
                            supplier_prepared,
                            on="__join_key__",
                            how="outer",
                            suffixes=join_suffix,
                        )

                        sort_master_col = "__sort_order" + join_suffix[0]
                        sort_supplier_col = "__sort_order" + join_suffix[1]
                        combined["__sort_order__"] = combined.get(sort_master_col).combine_first(
                            combined.get(sort_supplier_col)
                        )
                        combined.sort_values(
                            by="__sort_order__", inplace=True, kind="stable"
                        )
                        combined.reset_index(drop=True, inplace=True)

                        desc_master_col = "description" + join_suffix[0]
                        desc_supplier_col = "description" + join_suffix[1]
                        combined["Popis"] = combined.get(desc_master_col).combine_first(
                            combined.get(desc_supplier_col)
                        )

                        drop_columns = [
                            "__join_key__",
                            "__desc_key__" + join_suffix[0],
                            "__desc_key__" + join_suffix[1],
                            "__desc_order__" + join_suffix[0],
                            "__desc_order__" + join_suffix[1],
                            sort_master_col,
                            sort_supplier_col,
                            "__sort_order__",
                        ]
                        combined.drop(columns=drop_columns, inplace=True, errors="ignore")
                        combined.drop(
                            columns=[c for c in (desc_master_col, desc_supplier_col) if c in combined],
                            inplace=True,
                            errors="ignore",
                        )

                        master_cols = [
                            col
                            for col in master_table.columns
                            if isinstance(col, str) and not col.startswith("__")
                        ]
                        supplier_cols = [
                            col
                            for col in supplier_table.columns
                            if isinstance(col, str) and not col.startswith("__")
                        ]

                        column_labels = {
                            "code": "Kód",
                            "item_id": "ID položky",
                            "unit": "Jednotka",
                            "quantity": "Množství",
                            "quantity_supplier": "Množství dodavatel",
                            "unit_price": "Jednotková cena",
                            "unit_price_material": "Jednotková cena materiál",
                            "unit_price_install": "Jednotková cena montáž",
                            "total_price": "Cena celkem",
                            "price": "Cena",
                            "subtotal": "Mezisoučet",
                        }

                        rename_map: Dict[str, str] = {}
                        for col in list(combined.columns):
                            if col == "Popis":
                                continue
                            if col.endswith(join_suffix[0]):
                                base = col[: -len(join_suffix[0])]
                                base_label = column_labels.get(
                                    base, base.replace("_", " ").strip().capitalize()
                                )
                                rename_map[col] = f"{base_label}{join_suffix[0]}"
                            elif col.endswith(join_suffix[1]):
                                base = col[: -len(join_suffix[1])]
                                base_label = column_labels.get(
                                    base, base.replace("_", " ").strip().capitalize()
                                )
                                rename_map[col] = f"{base_label}{join_suffix[1]}"
                            elif col.startswith("__"):
                                combined.drop(columns=[col], inplace=True)
                            else:
                                base_label = column_labels.get(
                                    col, col.replace("_", " ").strip().capitalize()
                                )
                                rename_map[col] = base_label

                        if rename_map:
                            combined.rename(columns=rename_map, inplace=True)

                        display_order: List[str] = []
                        if "Popis" in combined.columns:
                            display_order.append("Popis")

                        for col in master_cols:
                            if col == "description":
                                continue
                            base_label = column_labels.get(
                                col, col.replace("_", " ").strip().capitalize()
                            )
                            display_col = f"{base_label}{join_suffix[0]}"
                            if display_col in combined.columns and display_col not in display_order:
                                display_order.append(display_col)

                        for col in supplier_cols:
                            if col == "description":
                                continue
                            base_label = column_labels.get(
                                col, col.replace("_", " ").strip().capitalize()
                            )
                            display_col = f"{base_label}{join_suffix[1]}"
                            if display_col in combined.columns and display_col not in display_order:
                                display_order.append(display_col)

                        for col in combined.columns:
                            if col not in display_order:
                                display_order.append(col)

                        table_df = combined.reindex(columns=display_order)

                        if table_df.empty:
                            st.warning(
                                "Nebyly nalezeny spárované položky se stejným popisem pro Master i dodavatele."
                            )
                        else:
                            st.caption(
                                "Tabulka páruje Master a vybraného dodavatele podle shodného popisu položky bez dalších přepočtů."
                            )
                            st.dataframe(table_df, use_container_width=True, hide_index=True)
                            export_cols = st.columns(2)
                            csv_bytes = table_df.to_csv(index=False).encode("utf-8-sig")
                            excel_bytes = dataframe_to_excel_bytes(
                                table_df, f"Porovnání — {selected_sheet}"
                            )
                            export_cols[0].download_button(
                                "⬇️ Export CSV",
                                data=csv_bytes,
                                file_name=sanitize_filename(
                                    f"porovnani2_{selected_sheet}_{selected_supplier}"
                                )
                                + ".csv",
                                mime="text/csv",
                                key=make_widget_key(
                                    "compare2_csv", selected_sheet, selected_supplier
                                ),
                            )
                            export_cols[1].download_button(
                                "⬇️ Export XLSX",
                                data=excel_bytes,
                                file_name=sanitize_filename(
                                    f"porovnani2_{selected_sheet}_{selected_supplier}"
                                )
                                + ".xlsx",
                                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                key=make_widget_key(
                                    "compare2_xlsx", selected_sheet, selected_supplier
                                ),
                            )
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
                    summed = subset.sum(skipna=True, min_count=1)
                    return summed.reindex(value_cols, fill_value=0.0)
                return pd.Series(0.0, index=value_cols, dtype=float)

            def extract_values_for_mask(mask: pd.Series) -> pd.Series:
                """Return the first non-null value for each numeric column within ``mask``.

                Rekapitulace tabulky obsahují již agregovaná čísla (sloupec
                ``total price``), která potřebujeme převzít beze změny.
                Sčítání by v těchto případech vedlo k chybným výsledkům, proto
                vždy bereme první dostupnou hodnotu pro každý sloupec.
                """

                if value_cols and not working_sections.empty and mask.any():
                    subset = working_sections.loc[mask, value_cols].apply(
                        pd.to_numeric, errors="coerce"
                    )
                    first_values = subset.apply(
                        lambda col: col.dropna().iloc[0] if not col.dropna().empty else np.nan
                    )
                    return first_values.reindex(value_cols)
                return pd.Series(np.nan, index=value_cols, dtype=float)

            st.markdown("### Rekapitulace finančních nákladů stavby")
            main_detail = pd.DataFrame()
            main_detail_display_base = pd.DataFrame()
            main_detail_display_converted = pd.DataFrame()
            summary_display = pd.DataFrame()
            summary_display_converted = pd.DataFrame()
            chart_df = pd.DataFrame()
            fig_recap = None
            section_totals_by_token: Dict[str, pd.Series] = {}
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
                        sums = extract_values_for_mask(mask)
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
                    if code_token:
                        section_totals_by_token[code_token] = sums.reindex(
                            value_cols
                        )
                    elif target_key:
                        section_totals_by_token[target_key] = sums.reindex(value_cols)
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

            def sum_series_for_tokens(tokens: Iterable[str], absolute: bool = False) -> pd.Series:
                relevant: List[pd.Series] = []
                for token in tokens:
                    values = section_totals_by_token.get(str(token))
                    if values is None:
                        continue
                    series = values.apply(pd.to_numeric, errors="coerce")
                    if absolute:
                        series = series.abs()
                    relevant.append(series.reindex(value_cols))
                if relevant:
                    summed = pd.concat(relevant, axis=1).sum(
                        axis=1, skipna=True, min_count=1
                    )
                    return summed.reindex(value_cols, fill_value=0.0)
                return pd.Series(0.0, index=value_cols, dtype=float)

            plus_sum = sum_series_for_tokens(MAIN_RECAP_TOKENS)
            deduction_sum = sum_series_for_tokens(deduction_tokens, absolute=True)
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
                                [chart_df["Odchylka (text)"].fillna("–")]
                            ),
                            hovertemplate=(
                                "<b>%{x}</b><br>"
                                "Cena po odečtech: %{text}<br>"
                                "Odchylka vs Master: %{customdata[0]}<extra></extra>"
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





import re
import json
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import plotly.express as px
import streamlit as st
from workbook import WorkbookData

# ------------- App Config -------------
st.set_page_config(page_title="BoQ Bid Studio V.04", layout="wide")
st.title("🏗️ BoQ Bid Studio V.04")
st.caption("Jedna aplikace pro nahrání, mapování, porovnání nabídek a vizualizace — bez exportů do Excelu.")

# ------------- Helpers -------------

HEADER_HINTS = {
    "code": [
        "code",
        "item",
        "č.",
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
        "item id",
        "itemid",
        "id položky",
        "id polozky",
        "kod",
        "kód",
        "číslo položky",
        "cislo polozky",
        "regex:^id$",
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

@st.cache_data
def try_autodetect_mapping(df: pd.DataFrame) -> Tuple[Dict[str, int], int, pd.DataFrame]:
    """Autodetect header mapping using a sampled, vectorized search."""
    # probe size grows with the dataframe but is capped to keep things fast
    nprobe = min(len(df), max(10, min(50, len(df) // 100)))
    sample = df.head(nprobe).astype(str).applymap(normalize_col)

    regex_map = {}
    for key, hints in HEADER_HINTS.items():
        patterns: List[str] = []
        for h in hints:
            if h.startswith("regex:"):
                patterns.append(h[len("regex:"):])
            else:
                patterns.append(re.escape(h))
        regex_map[key] = "|".join(patterns)

    def detect_row(row: pd.Series) -> Dict[str, int]:
        mapping: Dict[str, int] = {}
        for key, regex in regex_map.items():
            matches = row.str.contains(regex, regex=True, na=False)
            if matches.any():
                mapping[key] = matches.idxmax()
        return mapping

    mappings = sample.apply(detect_row, axis=1)
    for header_row, mapping in mappings.items():
        if set(REQUIRED_KEYS).issubset(mapping.keys()):
            body = df.iloc[header_row + 1:].reset_index(drop=True)
            body.columns = [normalize_col(x) for x in df.iloc[header_row].tolist()]
            return mapping, header_row, body
    return {}, -1, df

def coerce_numeric(s: pd.Series) -> pd.Series:
    """Coerce various textual numeric formats into floats.

    Handles European formats like "1 234,56" by removing thousand
    separators (spaces/non‑breaking spaces) and converting decimal comma to
    a dot before calling ``pd.to_numeric``.
    """
    if not isinstance(s, pd.Series):
        s = pd.Series(s)
    s = s.astype(str).str.replace(r"\s+", "", regex=True)
    # If both comma and dot present, assume dot is thousands separator
    mask = s.str.contains(",") & s.str.contains(".")
    s = s.where(~mask, s.str.replace(".", "", regex=False))
    s = s.str.replace(",", ".", regex=False)
    return pd.to_numeric(s, errors="coerce")


def detect_summary_rows(df: pd.DataFrame) -> pd.Series:
    """Return boolean Series marking summary/subtotal rows.

    Detection combines textual patterns (e.g. "součet", "total") and
    structural hints such as empty code with zero quantity and unit price.
    """
    desc_str = df.get("description", "").fillna("").astype(str)
    summary_patterns = (
        r"(celkem za odd[ií]l|sou[cč]et za odd[ií]l|celkov[aá] cena za list|sou[cč]et za list|"
        r"sou[cč]et|souhrn|subtotal|total|celkem)"
    )
    code_blank = df.get("code", "").astype(str).str.strip() == ""
    qty_zero = coerce_numeric(df.get("quantity", 0)).fillna(0) == 0
    unit_price_combined = (
        coerce_numeric(df.get("unit_price_material", 0)).fillna(0)
        + coerce_numeric(df.get("unit_price_install", 0)).fillna(0)
    )
    up_zero = unit_price_combined == 0
    pattern_mask = desc_str.str.contains(summary_patterns, case=False, na=False)
    totals = coerce_numeric(df.get("summary_total", 0)).fillna(0) + coerce_numeric(
        df.get("total_price", 0)
    ).fillna(0)
    structural_mask = code_blank & qty_zero & up_zero
    return pattern_mask & structural_mask & (totals != 0)

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
    desc_str = out["description"].fillna("").astype(str)
    numeric_cols = out.select_dtypes(include=[np.number]).columns
    summary_col = out["is_summary"].fillna(False).astype(bool)
    out = out[~((out[numeric_cols].isna() | (out[numeric_cols] == 0)).all(axis=1) & ~summary_col)]
    # Canonical key (will be overridden if user picks dedicated Item ID)
    out["__key__"] = (
        out["code"].astype(str).str.strip() + " | " + desc_str.str.strip()
    ).str.strip(" |")

    # Preserve explicit ordering from mapping for later aggregations
    out["__row_order__"] = np.arange(len(out))

    # Reorder columns for clarity
    col_order = [
        "code",
        "description",
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


def show_df(df: pd.DataFrame) -> None:
    if not isinstance(df, pd.DataFrame):
        st.dataframe(df)
        return
    numeric_cols = df.select_dtypes(include=[np.number]).columns
    if len(numeric_cols) == 0:
        st.dataframe(df)
    else:
        st.dataframe(df.style.format({col: format_number for col in numeric_cols}))

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
    """Copy mapping and header row from master workbook into target workbook."""
    for sheet, mobj in master.sheets.items():
        if sheet not in target.sheets:
            continue
        raw = target.sheets[sheet].get("raw")
        mapping = mobj.get("mapping", {})
        header_row = mobj.get("header_row", -1)
        if not isinstance(raw, pd.DataFrame) or not mapping or header_row < 0:
            continue
        try:
            header = [normalize_col(x) for x in raw.iloc[header_row].astype(str).tolist()]
            body = raw.iloc[header_row+1:].reset_index(drop=True)
            body.columns = header
            table = build_normalized_table(body, mapping)
            target.sheets[sheet].update({
                "mapping": mapping,
                "header_row": header_row,
                "table": table,
                "header_names": header,
            })
        except Exception:
            continue

def mapping_ui(section_title: str, wb: WorkbookData, minimal: bool = False, minimal_sheets: Optional[List[str]] = None) -> bool:
    """Render mapping UI and return True if any mapping changed."""
    st.subheader(section_title)
    tabs = st.tabs(list(wb.sheets.keys()))
    changed_any = False
    for tab, (sheet, obj) in zip(tabs, wb.sheets.items()):
        use_minimal = minimal or (minimal_sheets is not None and sheet in minimal_sheets)
        with tab:
            st.markdown(f"**List:** `{sheet}`")
            raw = obj.get("raw")
            header_row = obj.get("header_row", -1)
            mapping = obj.get("mapping", {}).copy()
            prev_mapping = mapping.copy()
            prev_header = header_row
            hdr_preview = raw.head(10) if isinstance(raw, pd.DataFrame) else None
            if hdr_preview is not None:
                show_df(hdr_preview)
            # Header row selector
            header_row = st.number_input(f"Řádek s hlavičkou (0 = první řádek) — {sheet}", min_value=0, max_value=9, value=header_row if header_row >= 0 else 0, step=1, key=f"hdr_{section_title}_{sheet}")
            # Build header names for the selected row
            if isinstance(raw, pd.DataFrame) and header_row < len(raw):
                header_names = [normalize_col(x) for x in raw.iloc[header_row].astype(str).tolist()]
            else:
                header_names = obj.get("header_names", [])
            # Select boxes for mapping
            cols = list(range(len(header_names)))
            if not cols:
                cols = [0]

            def pick_default(key):
                hints = HEADER_HINTS.get(key, [])
                for i, col in enumerate(header_names):
                    if any(p in col for p in hints):
                        return i
                return mapping.get(key, 0)

            def clamp(idx: int) -> int:
                return max(0, min(idx, len(cols) - 1))

            if use_minimal:
                c1, c2, c3, c4 = st.columns(4)
                with c1:
                    code_idx = st.selectbox(
                        "Sloupec: code",
                        cols,
                        format_func=lambda i: header_names[i] if i < len(header_names) else "",
                        index=clamp(pick_default("code")),
                        key=f"map_code_{section_title}_{sheet}",
                    )
                with c2:
                    desc_idx = st.selectbox(
                        "Sloupec: description",
                        cols,
                        format_func=lambda i: header_names[i] if i < len(header_names) else "",
                        index=clamp(pick_default("description")),
                        key=f"map_desc_{section_title}_{sheet}",
                    )
                with c3:
                    total_idx = st.selectbox(
                        "Sloupec: total_price",
                        cols,
                        format_func=lambda i: header_names[i] if i < len(header_names) else "",
                        index=clamp(pick_default("total_price")),
                        key=f"map_total_{section_title}_{sheet}",
                    )
                with c4:
                    summ_idx = st.selectbox(
                        "Sloupec: summary_total",
                        cols,
                        format_func=lambda i: header_names[i] if i < len(header_names) else "",
                        index=clamp(pick_default("summary_total")),
                        key=f"map_sum_{section_title}_{sheet}",
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
                }
            else:
                c1, c2, c3, c4 = st.columns(4)
                with c1:
                    code_idx = st.selectbox(
                        "Sloupec: code",
                        cols,
                        format_func=lambda i: header_names[i] if i < len(header_names) else "",
                        index=clamp(pick_default("code")),
                        key=f"map_code_{section_title}_{sheet}",
                    )
                with c2:
                    desc_idx = st.selectbox(
                        "Sloupec: description",
                        cols,
                        format_func=lambda i: header_names[i] if i < len(header_names) else "",
                        index=clamp(pick_default("description")),
                        key=f"map_desc_{section_title}_{sheet}",
                    )
                with c3:
                    unit_idx = st.selectbox(
                        "Sloupec: unit",
                        cols,
                        format_func=lambda i: header_names[i] if i < len(header_names) else "",
                        index=clamp(pick_default("unit")),
                        key=f"map_unit_{section_title}_{sheet}",
                    )
                with c4:
                    qty_idx = st.selectbox(
                        "Sloupec: quantity",
                        cols,
                        format_func=lambda i: header_names[i] if i < len(header_names) else "",
                        index=clamp(pick_default("quantity")),
                        key=f"map_qty_{section_title}_{sheet}",
                    )
                c5, c6, c7, c8, c9 = st.columns(5)
                with c5:
                    qty_sup_idx = st.selectbox(
                        "Sloupec: quantity_supplier",
                        cols,
                        format_func=lambda i: header_names[i] if i < len(header_names) else "",
                        index=clamp(pick_default("quantity_supplier")),
                        key=f"map_qtysup_{section_title}_{sheet}",
                    )
                with c6:
                    upm_idx = st.selectbox(
                        "Sloupec: unit_price_material",
                        cols,
                        format_func=lambda i: header_names[i] if i < len(header_names) else "",
                        index=clamp(pick_default("unit_price_material")),
                        key=f"map_upm_{section_title}_{sheet}",
                    )
                with c7:
                    upi_idx = st.selectbox(
                        "Sloupec: unit_price_install",
                        cols,
                        format_func=lambda i: header_names[i] if i < len(header_names) else "",
                        index=clamp(pick_default("unit_price_install")),
                        key=f"map_upi_{section_title}_{sheet}",
                    )
                with c8:
                    total_idx = st.selectbox(
                        "Sloupec: total_price",
                        cols,
                        format_func=lambda i: header_names[i] if i < len(header_names) else "",
                        index=clamp(pick_default("total_price")),
                        key=f"map_total_{section_title}_{sheet}",
                    )
                with c9:
                    summ_idx = st.selectbox(
                        "Sloupec: summary_total",
                        cols,
                        format_func=lambda i: header_names[i] if i < len(header_names) else "",
                        index=clamp(pick_default("summary_total")),
                        key=f"map_sum_{section_title}_{sheet}",
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
        if "is_summary" in mtab.columns:
            mtab = mtab[~mtab["is_summary"].fillna(False).astype(bool)]
        mtab = mtab[mtab["description"].astype(str).str.strip() != ""]
        base = mtab[["__key__", "code", "description", "unit", "quantity", "total_price"]].copy()
        base["quantity"] = pd.to_numeric(base["quantity"], errors="coerce").fillna(0)
        base["total_price"] = pd.to_numeric(base["total_price"], errors="coerce").fillna(0)
        base_grouped = base.groupby("__key__", sort=False, as_index=False).agg(
            {
                "code": "first",
                "description": "first",
                "unit": "first",
                "quantity": "sum",
                "total_price": "sum",
            }
        )
        master_total_sum = base_grouped["total_price"].sum()
        base_grouped.rename(columns={"total_price": "Master total"}, inplace=True)
        comp = base_grouped.copy()

        for sup_name, wb in bids.items():
            tobj = wb.sheets.get(sheet, {})
            ttab = tobj.get("table", pd.DataFrame())
            if ttab is None or ttab.empty:
                comp[f"{sup_name} quantity"] = np.nan
                comp[f"{sup_name} unit_price"] = np.nan
                comp[f"{sup_name} total"] = np.nan
                continue
            if "is_summary" in ttab.columns:
                ttab = ttab[~ttab["is_summary"].fillna(False).astype(bool)]
            ttab = ttab[ttab["description"].astype(str).str.strip() != ""]
            # join by __key__ (manual mapping already built in normalized table)
            sup_qty_col = "quantity_supplier" if "quantity_supplier" in ttab.columns else "quantity"
            cols = [
                "__key__",
                sup_qty_col,
                "unit_price_material",
                "unit_price_install",
                "total_price",
            ]
            existing_cols = [c for c in cols if c in ttab.columns]
            tt = ttab[existing_cols].copy()
            tt[sup_qty_col] = pd.to_numeric(tt[sup_qty_col], errors="coerce")
            tt["total_price"] = pd.to_numeric(tt["total_price"], errors="coerce").fillna(0)
            price_cols = [c for c in ["unit_price_material", "unit_price_install"] if c in tt.columns]
            if price_cols:
                for col in price_cols:
                    tt[col] = pd.to_numeric(tt[col], errors="coerce")
                tt["unit_price_combined"] = tt[price_cols].sum(axis=1, min_count=1)
            else:
                tt["unit_price_combined"] = np.nan
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
            qty = tt_grouped[sup_qty_col]
            with np.errstate(divide="ignore", invalid="ignore"):
                qty_for_division = qty.where(qty != 0)
                tt_grouped["unit_price_combined"] = tt_grouped["total_price"] / qty_for_division
            mask = qty_for_division.isna()
            tt_grouped.loc[mask, "unit_price_combined"] = tt_grouped.loc[mask, "first_unit_price"]
            tt_grouped.drop(columns=["first_unit_price"], inplace=True)
            comp = comp.merge(
                tt_grouped[["__key__", sup_qty_col, "unit_price_combined", "total_price"]],
                on="__key__",
                how="left",
            )
            comp.rename(columns={
                sup_qty_col: f"{sup_name} quantity",
                "unit_price_combined": f"{sup_name} unit_price",
                "total_price": f"{sup_name} total",
            }, inplace=True)
            comp[f"{sup_name} Δ qty"] = comp[f"{sup_name} quantity"] - comp["quantity"]

        total_cols = [c for c in comp.columns if c.endswith(" total") and c != "Master total"]
        if total_cols:
            comp["LOWEST total"] = comp[total_cols].min(axis=1, skipna=True)
            for c in total_cols:
                comp[f"{c} Δ vs LOWEST"] = comp[c] - comp["LOWEST total"]
            # Which supplier is the lowest per row?
            def lowest_supplier(row):
                values = {c.replace(" total",""): row[c] for c in total_cols}
                # return supplier name with min value (ignore NaN)
                values = {k: v for k, v in values.items() if pd.notna(v)}
                if not values:
                    return None
                return min(values, key=values.get)
            comp["LOWEST supplier"] = comp.apply(lowest_supplier, axis=1)

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
        rename_map[f"{raw} unit_price"] = f"{alias} unit_price"
        rename_map[f"{raw} total"] = f"{alias} total"
        rename_map[f"{raw} Δ qty"] = f"{alias} Δ qty"
        rename_map[f"{raw} Δ vs LOWEST"] = f"{alias} Δ vs LOWEST"
    renamed = df.rename(columns=rename_map).copy()
    if "LOWEST supplier" in renamed.columns:
        renamed["LOWEST supplier"] = renamed["LOWEST supplier"].map(mapping).fillna(
            renamed["LOWEST supplier"]
        )
    return renamed


def rename_total_columns(df: pd.DataFrame, mapping: Dict[str, str]) -> pd.DataFrame:
    if df is None or df.empty or not mapping:
        return df
    rename_map = {f"{raw} total": f"{alias} total" for raw, alias in mapping.items()}
    return df.rename(columns=rename_map)

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
    mtab["total_for_sum"] = mtab["total_price"].fillna(0)

    df = (
        mtab[["code", "description", "__row_order__", "total_for_sum"]]
        .groupby(["code", "description"], as_index=False, dropna=False)
        .agg({"total_for_sum": "sum", "__row_order__": "min"})
        .rename(columns={"total_for_sum": "Master total"})
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
            ttab["total_for_sum"] = ttab["total_price"].fillna(0)
            tdf = (
                ttab[["code", "description", "total_for_sum"]]
                .groupby(["code", "description"], as_index=False, dropna=False)["total_for_sum"].sum()
            )
            df = df.merge(tdf, on=["code", "description"], how="left")
            df.rename(columns={"total_for_sum": f"{sup_name} total"}, inplace=True)

    total_cols = [c for c in df.columns if c.endswith(" total")]
    view_cols = ["code", "description", "__row_order__"] + total_cols
    view = df[view_cols].copy()
    view["code"] = view["code"].fillna("").astype(str)
    view["description"] = view["description"].fillna("").astype(str)
    view = view[view["description"].str.strip() != ""]
    view = view.sort_values(by="__row_order__").reset_index(drop=True)
    indirect_mask = view["description"].str.contains("vedlej", case=False, na=False)
    added_mask = view["description"].str.contains("dodavat", case=False, na=False)
    sections_df = view[~(indirect_mask | added_mask)].copy()
    indirect_df = view[indirect_mask].copy()
    added_df = view[added_mask].copy()

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
        sums = indirect_df[[c for c in indirect_df.columns if c.endswith(" total")]].sum()
        indirect_total = sums.rename("total").to_frame().reset_index()
        indirect_total.rename(columns={"index": "supplier"}, inplace=True)
        indirect_total["supplier"] = indirect_total["supplier"].str.replace(" total", "", regex=False)

    for df_part in (sections_df, indirect_df, added_df):
        if "__row_order__" in df_part.columns:
            df_part.drop(columns="__row_order__", inplace=True)
    if "__row_order__" in view.columns:
        view.drop(columns="__row_order__", inplace=True)
    if not missing_df.empty and "__row_order__" in missing_df.columns:
        missing_df.drop(columns="__row_order__", inplace=True)

    return sections_df, indirect_df, added_df, missing_df, indirect_total


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

st.sidebar.header("Vstupy")
master_file = st.sidebar.file_uploader(
    "Master BoQ (.xlsx/.xlsm)", type=["xlsx", "xlsm"], key="master"
)
bid_files = st.sidebar.file_uploader(
    "Nabídky dodavatelů (max 7)",
    type=["xlsx", "xlsm"],
    accept_multiple_files=True,
    key="bids",
)
currency = st.sidebar.text_input("Popisek měny", value="CZK")

if not master_file:
    st.info("➡️ Nahraj Master BoQ v levém panelu.")
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

    st.session_state["supplier_metadata"] = metadata
    display_names = {raw: metadata[raw]["alias"] for raw in current_suppliers}
    color_map = {display_names[raw]: metadata[raw]["color"] for raw in current_suppliers}

chart_color_map = color_map.copy()
chart_color_map.setdefault("Master", "#636EFA")

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
    )
    if master_changed:
        for wb in bids_dict.values():
            apply_master_mapping(master_wb, wb)
        if overview_sheet in compare_sheets:
            for wb in bids_overview_dict.values():
                apply_master_mapping(master_wb, wb)
    if overview_sheet not in compare_sheets:
        with st.expander("Mapování — Master rekapitulace", expanded=False):
            master_over_changed = mapping_ui("Master rekapitulace", master_overview_wb, minimal=True)
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
                )
        if overview_sheet not in compare_sheets:
            for sup_name, wb in bids_overview_dict.items():
                alias = display_names.get(sup_name, sup_name)
                with st.expander(f"Mapování rekapitulace — {alias}", expanded=False):
                    mapping_ui(f"{alias} rekapitulace", wb, minimal=True)
    st.success("Mapování připraveno. Přepni na záložku **Porovnání**.")

# Pre-compute comparison results for reuse in tabs (after mapping)
compare_results: Dict[str, pd.DataFrame] = {}
if bids_dict:
    raw_compare_results = compare(master_wb, bids_dict, join_mode="auto")
    compare_results = {
        sheet: rename_comparison_columns(df, display_names) for sheet, df in raw_compare_results.items()
    }

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
    else:
        results = compare_results

        # main per-sheet tables
        for sheet, df in results.items():
            st.subheader(f"List: {sheet}")
            # Totals per supplier for this sheet
            total_cols = [c for c in df.columns if c.endswith(" total")]
            if total_cols:
                def total_for_column(col: str) -> float:
                    if col == "Master total" and "master_total_sum" in df.attrs:
                        return df.attrs["master_total_sum"]
                    return df[col].sum()

                totals = [(col, total_for_column(col)) for col in total_cols]
                sums_df = pd.DataFrame(totals, columns=["Součet (sloupec)", "Hodnota"])
                with st.container():
                    c1, c2 = st.columns([2, 3])
                    with c1:
                        st.markdown("**Součty za list:**")
                        show_df(sums_df)
                    with c2:
                        chart_df = pd.DataFrame(
                            {
                                "supplier": [c.replace(" total", "") for c in total_cols],
                                "total": [total_for_column(c) for c in total_cols],
                            }
                        )
                        try:
                            fig = px.bar(
                                chart_df,
                                x="supplier",
                                y="total",
                                color="supplier",
                                color_discrete_map=chart_color_map,
                                title=f"Součet za list: {sheet} ({currency})",
                            )
                            fig.update_layout(showlegend=False)
                            st.plotly_chart(fig, use_container_width=True)
                        except Exception:
                            show_df(chart_df)
            show_df(df)

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
                    value=24.0,
                    step=0.1,
                    format="%.4f",
                )

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
                if col.endswith(" total"):
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
            if not sections_df.empty:
                st.markdown("### Celkové ceny oddílů")
                show_df(sections_df)
            if not missing_df.empty:
                st.markdown("### Chybějící položky dle dodavatele")
                show_df(missing_df)
            if not indirect_df.empty:
                st.markdown("### Vedlejší rozpočtové náklady")
                show_df(indirect_df)
                if not indirect_total.empty:
                    st.markdown("**Součet vedlejších nákladů:**")
                    show_df(indirect_total)
            if not added_df.empty:
                st.markdown("### Náklady přidané dodavatelem")
                show_df(added_df)

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
                item_chart_df = pd.DataFrame(
                    {
                        "item": item_deltas.index,
                        "value": item_deltas.values,
                        "supplier": leading_supplier.values,
                    }
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

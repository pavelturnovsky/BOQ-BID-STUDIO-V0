
import re
import json
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import plotly.express as px
import streamlit as st

# ------------- App Config -------------
st.set_page_config(page_title="BoQ Bid Studio V.01", layout="wide")
st.title("🏗️ BoQ Bid Studio V.01")
st.caption("Jedna aplikace pro nahrání, mapování, porovnání nabídek a vizualizace — bez exportů do Excelu.")

# ------------- Helpers -------------

HEADER_HINTS = {
    "code": ["code", "item", "číslo položky", "cislo polozky", "položka", "polozka", "id", "kód", "kod"],
    "description": ["description", "popis", "název", "nazev", "specifikace"],
    "unit": ["unit", "jm", "mj", "jednotka", "uom", "měrná jednotka", "merna jednotka"],
    "quantity": ["quantity", "qty", "množství", "mnozstvi", "q"],
    "unit_price": ["unit price", "u.p.", "cena/jedn", "cena za jednotku", "jedn. cena", "unitprice", "rate", "sazba", "jednotková cena", "jednotkova cena"],
    # optional extras commonly seen
    "item_id": ["item id", "itemid", "id položky", "id polozky", "kod", "kód", "číslo položky", "cislo polozky"]
}

REQUIRED_KEYS = ["code", "description", "quantity"]  # unit & unit_price can be optional at parse time

def normalize_col(c):
    if not isinstance(c, str):
        c = str(c)
    return re.sub(r"\s+", " ", c.strip().lower())

def try_autodetect_mapping(df: pd.DataFrame) -> Tuple[Dict[str, int], int, pd.DataFrame]:
    """
    Probe first 10 rows for a header line that includes required keys.
    Returns (mapping, header_row_index, body_df_with_named_columns) or ({}, -1, df) if not found.
    """
    nprobe = min(10, len(df))
    for header_row in range(nprobe):
        header = df.iloc[header_row].astype(str).apply(normalize_col).tolist()
        mapping = {}
        for k, patterns in HEADER_HINTS.items():
            for i, col in enumerate(header):
                if any(p in col for p in patterns):
                    mapping[k] = i
                    break
        if set(REQUIRED_KEYS).issubset(mapping.keys()):
            body = df.iloc[header_row + 1:].reset_index(drop=True)
            # assign the original header normalized as column names
            body.columns = [normalize_col(x) for x in df.iloc[header_row].tolist()]
            return mapping, header_row, body
    return {}, -1, df

def coerce_numeric(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s, errors="coerce")

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
        "unit_price": coerce_numeric(pick("unit_price", np.nan)),
    })
    # filter empty row heuristics
    mask = (out["code"].astype(str).str.strip() != "") | (out["description"].astype(str).str.strip() != "")
    out = out[mask].copy()
    # canonical key (will be overridden if user picks dedicated Item ID)
    out["__key__"] = out["code"].astype(str).str.strip() + " | " + out["description"].astype(str).str.strip()
    return out

@dataclass
class WorkbookData:
    name: str
    sheets: Dict[str, Dict] = field(default_factory=dict)  # sheet -> {"raw": df_raw, "mapping": dict, "header_row": int, "table": df_norm}

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

def mapping_ui(section_title: str, wb: WorkbookData) -> None:
    st.subheader(section_title)
    tabs = st.tabs(list(wb.sheets.keys()))
    for tab, (sheet, obj) in zip(tabs, wb.sheets.items()):
        with tab:
            st.markdown(f"**List:** `{sheet}`")
            raw = obj.get("raw")
            header_row = obj.get("header_row", -1)
            mapping = obj.get("mapping", {}).copy()
            hdr_preview = raw.head(10) if isinstance(raw, pd.DataFrame) else None
            if hdr_preview is not None:
                st.dataframe(hdr_preview)
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
                # try to find a column by heuristics
                hints = HEADER_HINTS.get(key, [])
                for i, col in enumerate(header_names):
                    if any(p in col for p in hints):
                        return i
                # fallback to existing mapping
                return mapping.get(key, 0)

            def clamp(idx: int) -> int:
                """Ensure index is within available column range."""
                return max(0, min(idx, len(cols) - 1))

            c1, c2, c3, c4, c5 = st.columns(5)
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
            with c5:
                up_idx = st.selectbox(
                    "Sloupec: unit_price",
                    cols,
                    format_func=lambda i: header_names[i] if i < len(header_names) else "",
                    index=clamp(pick_default("unit_price")),
                    key=f"map_up_{section_title}_{sheet}",
                )

            # Rebuild normalized table with UI mapping
            ui_mapping = {"code": code_idx, "description": desc_idx, "unit": unit_idx, "quantity": qty_idx, "unit_price": up_idx}
            if isinstance(raw, pd.DataFrame):
                body = raw.iloc[header_row+1:].reset_index(drop=True)
                body.columns = [normalize_col(x) for x in raw.iloc[header_row].tolist()]
                table = build_normalized_table(body, ui_mapping)
            else:
                table = pd.DataFrame()

            wb.sheets[sheet]["mapping"] = ui_mapping
            wb.sheets[sheet]["header_row"] = header_row
            wb.sheets[sheet]["table"] = table

            st.markdown("**Normalizovaná tabulka (náhled):**")
            st.dataframe(table.head(50))

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
        base = mtab[["__key__", "code", "description", "unit", "quantity"]].copy()
        base = base.drop_duplicates("__key__")
        comp = base.copy()

        for sup_name, wb in bids.items():
            tobj = wb.sheets.get(sheet, {})
            ttab = tobj.get("table", pd.DataFrame())
            if ttab is None or ttab.empty:
                comp[f"{sup_name} unit_price"] = np.nan
                comp[f"{sup_name} total"] = np.nan
                continue
            # join by __key__ (we keep auto mode for now; Item ID support can be added when present in normalized table)
            tt = ttab[["__key__", "unit_price"]].copy()
            tt = tt.groupby("__key__", as_index=False)["unit_price"].mean()
            comp = comp.merge(tt, on="__key__", how="left")
            comp.rename(columns={"unit_price": f"{sup_name} unit_price"}, inplace=True)
            comp[f"{sup_name} total"] = comp["quantity"] * comp[f"{sup_name} unit_price"]

        total_cols = [c for c in comp.columns if c.endswith(" total")]
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

        results[sheet] = comp
    return results

def summarize(results: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    rows = []
    for sheet, df in results.items():
        if df is None or df.empty:
            continue
        total_cols = [c for c in df.columns if c.endswith(" total")]
        sums = {c: df[c].sum(skipna=True) for c in total_cols}
        row = {"sheet": sheet}
        row.update(sums)
        rows.append(row)
    out = pd.DataFrame(rows)
    return out

def qa_checks(master: WorkbookData, bids: Dict[str, WorkbookData]) -> Dict[str, Dict[str, pd.DataFrame]]:
    """ Return {sheet: {"missing": df, "extras": df, "duplicates": df}} """
    out = {}
    for sheet, mobj in master.sheets.items():
        mtab = mobj.get("table", pd.DataFrame())
        if mtab is None or mtab.empty:
            continue
        mkeys = set(mtab["__key__"].dropna().astype(str))
        per_sheet = {}
        for sup, wb in bids.items():
            tobj = wb.sheets.get(sheet, {})
            ttab = tobj.get("table", pd.DataFrame())
            if ttab is None or ttab.empty:
                miss = pd.DataFrame({"__key__": sorted(mkeys)})
                ext = pd.DataFrame(columns=["__key__"])
                dupl = pd.DataFrame(columns=["__key__", "cnt"])
            else:
                tkeys_series = ttab["__key__"].dropna().astype(str)
                tkeys = set(tkeys_series)
                miss = pd.DataFrame({"__key__": sorted(mkeys - tkeys)})
                ext = pd.DataFrame({"__key__": sorted(tkeys - mkeys)})
                # duplicates within supplier bid (same key appearing more than once)
                dupl_counts = tkeys_series.value_counts()
                dupl = dupl_counts[dupl_counts > 1].rename_axis("__key__").reset_index(name="cnt")
            per_sheet[sup] = {"missing": miss, "extras": ext, "duplicates": dupl}
        out[sheet] = per_sheet
    return out

# ------------- Sidebar Inputs -------------

st.sidebar.header("Vstupy")
master_file = st.sidebar.file_uploader("Master BoQ (.xlsx/.xlsm)", type=["xlsx", "xlsm"], key="master")
bid_files = st.sidebar.file_uploader("Nabídky dodavatelů (max 7)", type=["xlsx", "xlsm"], accept_multiple_files=True, key="bids")
vat_rate = st.sidebar.number_input("DPH (%) — pouze pro zobrazení součtů", min_value=0.0, max_value=30.0, value=0.0, step=1.0)
currency = st.sidebar.text_input("Měna (zobrazit)", value="CZK")

if not master_file:
    st.info("➡️ Nahraj Master BoQ v levém panelu.")
    st.stop()

# Read master
master_wb = read_workbook(master_file)

# Confirm sheet selection (default all master sheets)
all_sheets = list(master_wb.sheets.keys())
selected_sheets = st.sidebar.multiselect("Které listy zahrnout", all_sheets, default=all_sheets)

# Filter master to selected sheets
master_wb.sheets = {s: master_wb.sheets[s] for s in selected_sheets}

# Read bids
bids_dict: Dict[str, WorkbookData] = {}
if bid_files:
    if len(bid_files) > 7:
        st.sidebar.warning("Zpracuje se pouze prvních 7 souborů.")
        bid_files = bid_files[:7]
    for i, f in enumerate(bid_files, start=1):
        name = getattr(f, "name", f"Bid{i}")
        wb = read_workbook(f, limit_sheets=selected_sheets)
        bids_dict[name] = wb

# ------------- Tabs -------------
tab_data, tab_compare, tab_dashboard, tab_qa = st.tabs(["📑 Mapování", "⚖️ Porovnání", "📈 Dashboard", "🧪 QA kontroly"])

with tab_data:
    mapping_ui("Master", master_wb)
    if bids_dict:
        for sup_name, wb in bids_dict.items():
            with st.expander(f"Mapování — {sup_name}", expanded=False):
                mapping_ui(sup_name, wb)
    st.success("Mapování připraveno. Přepni na záložku **Porovnání**.")

with tab_compare:
    if not bids_dict:
        st.info("Nahraj alespoň jednu nabídku dodavatele v levém panelu.")
    else:
        results = compare(master_wb, bids_dict, join_mode="auto")
        # main per-sheet tables
        for sheet, df in results.items():
            st.subheader(f"List: {sheet}")
            # Totals per supplier for this sheet
            total_cols = [c for c in df.columns if c.endswith(" total")]
            if total_cols:
                sums = df[total_cols].sum().rename("Sum")
                sums_df = sums.reset_index()
                sums_df.columns = ["Supplier total (col)", "Value"]
                with st.container():
                    c1, c2 = st.columns([2, 3])
                    with c1:
                        st.markdown("**Součty za list (bez DPH):**")
                        st.dataframe(sums_df)
                    with c2:
                        # bar chart per sheet
                        try:
                            chart_df = pd.DataFrame({"supplier": [c.replace(" total","") for c in total_cols], "total": [df[c].sum() for c in total_cols]})
                            fig = px.bar(chart_df, x="supplier", y="total", title=f"Součet za list: {sheet} ({currency} bez DPH)")
                            st.plotly_chart(fig, use_container_width=True)
                        except Exception:
                            pass
            st.dataframe(df)

        # global summary table
        summary_df = summarize(results)
        if not summary_df.empty:
            st.markdown("### 📌 Souhrn po listech")
            st.dataframe(summary_df)

            # grand totals per supplier
            supplier_totals = {}
            for col in summary_df.columns:
                if col.endswith(" total"):
                    supplier = col.replace(" total", "")
                    supplier_totals[supplier] = summary_df[col].sum()
            grand_df = pd.DataFrame({"supplier": list(supplier_totals.keys()), "grand_total": list(supplier_totals.values())})
            if vat_rate and vat_rate > 0:
                grand_df["grand_total_s_DPH"] = grand_df["grand_total"] * (1 + vat_rate/100.0)

            c1, c2 = st.columns([3, 2])
            with c1:
                st.markdown("**Celkové součty (napříč listy):**")
                st.dataframe(grand_df)
            with c2:
                try:
                    fig = px.bar(grand_df, x="supplier", y="grand_total", title=f"Celkové součty ({currency} bez DPH)")
                    st.plotly_chart(fig, use_container_width=True)
                except Exception:
                    pass

with tab_dashboard:
    if not bids_dict:
        st.info("Nejdřív nahraj nabídky.")
    else:
        results = compare(master_wb, bids_dict, join_mode="auto")
        # Choose a sheet for detailed variance chart
        sheet_choices = list(results.keys())
        if sheet_choices:
            sel_sheet = st.selectbox("Vyber list pro detailní grafy", sheet_choices, index=0)
            df = results[sel_sheet]
            # Heatmap-like chart: Δ vs LOWEST per supplier
            delta_cols = [c for c in df.columns if c.endswith(" Δ vs LOWEST")]
            if delta_cols:
                heat_df = df[["__key__"] + delta_cols].copy().set_index("__key__")
                # Rename columns to supplier names only
                heat_df.columns = [c.replace(" Δ vs LOWEST", "") for c in heat_df.columns]
                # aggregate top N worst deltas by sum
                sum_deltas = heat_df.sum().sort_values(ascending=False)
                st.markdown("**Součet odchylek vs. nejnižší (vyšší = horší):**")
                try:
                    fig = px.bar(sum_deltas, title="Součet Δ vs. LOWEST (po dodavatelích)")
                    st.plotly_chart(fig, use_container_width=True)
                except Exception:
                    st.dataframe(sum_deltas.to_frame("sum_delta"))

                # Top položky podle rozdílu mezi nejlepší a vybraným dodavatelem
                st.markdown("**Top 20 položek s nejvyšší odchylkou od nejnižší ceny (součet přes dodavatele):**")
                try:
                    item_deltas = heat_df.abs().sum(axis=1).sort_values(ascending=False).head(20)
                    fig2 = px.bar(item_deltas, title="Top 20 položek podle absolutní Δ")
                    st.plotly_chart(fig2, use_container_width=True)
                except Exception:
                    st.dataframe(item_deltas.to_frame("abs_sum_delta"))
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
                st.markdown(f"**Dodavatel:** {sup}")
                c1, c2, c3 = st.columns(3)
                with c1:
                    st.markdown("**Chybějící položky**")
                    st.dataframe(d["missing"].head(50))
                with c2:
                    st.markdown("**Nad rámec (navíc)**")
                    st.dataframe(d["extras"].head(50))
                with c3:
                    st.markdown("**Duplicitní položky**")
                    st.dataframe(d["duplicates"].head(50))

st.markdown("---")
st.caption("© 2025 BoQ Bid Studio — MVP. Doporučení: používat jednotné Item ID pro precizní párování.")

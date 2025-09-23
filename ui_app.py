"""Streamlit UI for the BOQ Bid Studio MVP."""
from __future__ import annotations

import io
import tempfile
from pathlib import Path
from typing import List

import pandas as pd
import plotly.express as px
import streamlit as st

from core import (
    assign_disciplines,
    build_wbs_index,
    export_to_xlsx,
    flag_outliers,
    hybrid_search,
    load_config,
    load_offers,
    match_items,
    normalize_currency,
    normalize_units,
    rollup_by_discipline,
    rollup_by_wbs,
    validate_totals,
)

st.set_page_config(page_title="BOQ Bid Studio", layout="wide")

CONFIG = load_config()
SAMPLE_FILES = [Path("sample_data/offer_A.csv"), Path("sample_data/offer_B.csv")]


def _persist_uploaded_files(files) -> List[Path]:
    paths: List[Path] = []
    for uploaded in files:
        suffix = Path(uploaded.name).suffix
        with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
            tmp.write(uploaded.getbuffer())
            paths.append(Path(tmp.name))
    return paths


def _run_pipeline(input_paths: List[Path]) -> pd.DataFrame:
    offers = load_offers(input_paths, CONFIG)
    offers = normalize_units(offers, CONFIG.get("unit_map", {}))
    currency_cfg = CONFIG.get("currency", {})
    offers = normalize_currency(
        offers,
        currency_cfg.get("base", "CZK"),
        currency_cfg.get("rates", {}),
        currency_cfg.get("default_vat", 0.21),
    )
    offers = validate_totals(offers)
    offers = assign_disciplines(offers, CONFIG)
    offers.reset_index(drop=True, inplace=True)
    offers["item_index"] = offers.index

    wbs_index = build_wbs_index(CONFIG)
    matches = match_items(offers, wbs_index, CONFIG)
    drop_cols = {"item_desc", "supplier", "sheet_name", "item_code"}
    offers = offers.merge(
        matches.drop(columns=[col for col in drop_cols if col in matches.columns]),
        on="item_index",
        how="left",
    )
    offers = flag_outliers(offers)
    return offers


def _display_heatmap(summary: pd.DataFrame) -> None:
    if summary.empty:
        st.info("No data available for heatmap")
        return
    pivot = summary.pivot(index="discipline", columns="supplier", values="total_diff").fillna(0)
    fig = px.imshow(
        pivot,
        text_auto=True,
        color_continuous_scale="RdBu",
        aspect="auto",
        origin="lower",
        title="Deviation heatmap vs baseline",
    )
    st.plotly_chart(fig, use_container_width=True)


st.title("BOQ Bid Studio")
st.write("Porovnávejte nabídky dodavatelů a vyhledávejte položky pomocí hybridního vyhledávání.")

with st.sidebar:
    st.header("Data")
    use_samples = st.checkbox("Použít ukázková data", value=True)
    uploaded = st.file_uploader(
        "Nahrajte nabídky", type=["xlsx", "xls", "xlsm", "csv"], accept_multiple_files=True
    )
    if st.button("Načíst data"):
        input_paths: List[Path] = []
        if use_samples:
            input_paths.extend(SAMPLE_FILES)
        if uploaded:
            input_paths.extend(_persist_uploaded_files(uploaded))
        if not input_paths:
            st.warning("Vyberte alespoň jeden soubor.")
        else:
            with st.spinner("Zpracovávám nabídky..."):
                offers = _run_pipeline(input_paths)
            st.session_state["offers"] = offers
            st.session_state["suppliers"] = sorted(offers["supplier"].dropna().unique().tolist())
            st.success("Data načtena")

offers = st.session_state.get("offers")
if offers is None:
    st.info("Načtěte data pomocí panelu vlevo.")
    st.stop()

suppliers = st.session_state.get("suppliers", [])
baseline_options = ["median"] + suppliers
baseline = st.sidebar.selectbox("Baseline", baseline_options, index=0)
discipline_filter = st.sidebar.multiselect(
    "Disciplíny", sorted(offers["primary_discipline"].dropna().unique().tolist())
)
wbs_prefix = st.sidebar.text_input("WBS prefix", "")
show_unmatched = st.sidebar.checkbox("Zobrazit nespárované", value=False)
only_outliers = st.sidebar.checkbox("Pouze odlehlé hodnoty", value=False)

filtered = offers.copy()
if discipline_filter:
    filtered = filtered[filtered["primary_discipline"].isin(discipline_filter)]
if wbs_prefix:
    filtered = filtered[filtered["matched_wbs_code"].fillna("").str.startswith(wbs_prefix)]
if not show_unmatched:
    filtered = filtered[filtered["match_status"] != "unmatched"]
if only_outliers:
    filtered = filtered[filtered["is_outlier"]]

st.subheader("Souhrn podle disciplíny")
discipline_summary = rollup_by_discipline(filtered, baseline=baseline)
st.dataframe(discipline_summary)
_display_heatmap(rollup_by_discipline(offers, baseline=baseline))

if wbs_prefix:
    st.subheader(f"Souhrn pro WBS {wbs_prefix}")
    wbs_summary = rollup_by_wbs(filtered, baseline=baseline, prefix=wbs_prefix)
    st.dataframe(wbs_summary)

st.subheader("Detail položek")
st.dataframe(
    filtered[
        [
            "supplier",
            "item_code",
            "item_desc",
            "unit",
            "qty",
            "net_unit_price",
            "net_total_price",
            "matched_wbs_code",
            "match_status",
            "match_score",
            "explain",
            "is_outlier",
        ]
    ]
)

st.subheader("Vyhledávání")
query = st.text_input("Zadejte dotaz (např. 'Kolik stojí vzduchotechnika?')")
if query:
    results = hybrid_search(offers, query, CONFIG)
    st.dataframe(results)

st.subheader("Export")
buffer = io.BytesIO()
export_to_xlsx(
    rollup_by_discipline(offers, baseline=baseline),
    offers,
    offers[offers["match_status"] == "unmatched"],
    buffer,
)
buffer.seek(0)
st.download_button(
    "Stáhnout report",
    data=buffer,
    file_name="comparison_report.xlsx",
    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
)

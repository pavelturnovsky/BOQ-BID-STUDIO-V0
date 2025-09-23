"""Item to WBS matching implementation."""
from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

import pandas as pd
from rapidfuzz import fuzz
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import linear_kernel

from .utils import build_wbs_table, clean_text, ensure_directories, log_match_decision, timestamp

_MATCH_LOG_PATH = Path("logs") / "matching.jsonl"


def build_wbs_index(config: Mapping[str, Any]) -> pd.DataFrame:
    """Create a dataframe representation of the configured WBS hierarchy."""

    rows = build_wbs_table(config)
    if not rows:
        raise ValueError("WBS configuration is empty")
    return pd.DataFrame(rows)


def _prepare_text(row: Mapping[str, Any]) -> str:
    parts = [row.get("wbs_name"), row.get("wbs_description"), " ".join(row.get("keywords", []))]
    return clean_text(" ".join(filter(None, parts)))


def _item_text(row: Mapping[str, Any]) -> str:
    parts = [row.get("item_code"), row.get("item_desc"), row.get("sheet_name"), row.get("supplier")]
    return clean_text(" ".join(filter(None, parts)))


def _discipline_boost(item_disciplines: Iterable[str], wbs_discipline: Optional[str]) -> float:
    if not wbs_discipline:
        return 1.0
    if not item_disciplines:
        return 1.0
    return 1.2 if wbs_discipline in set(item_disciplines) else 1.0


def _unit_check(item_unit: Optional[str], wbs_unit: Optional[str]) -> float:
    if not wbs_unit:
        return 1.0
    if not item_unit:
        return 0.0
    return 1.0 if clean_text(item_unit).lower() == clean_text(wbs_unit).lower() else 0.0


def _code_exact(item_code: Optional[str], wbs_code: Optional[str], wbs_discipline: Optional[str]) -> float:
    if not item_code:
        return 0.0
    cleaned_code = clean_text(item_code).replace("-", "").replace(" ", "").lower()
    score = 0.0
    if wbs_code:
        raw_code = clean_text(wbs_code)
        target = raw_code.replace(".", "").replace(" ", "").lower()
        if target and target in cleaned_code:
            score = max(score, 1.0)
        top_level = raw_code.split(".")[0].lower()
        if top_level and top_level in cleaned_code:
            score = max(score, 0.7)
    if wbs_discipline:
        discipline_tag = clean_text(wbs_discipline).lower()
        if discipline_tag and discipline_tag in cleaned_code:
            score = max(score, 0.7)
    return score


def match_items(
    df_items: pd.DataFrame,
    wbs_index: pd.DataFrame,
    config: Mapping[str, Any],
    log_path: Path | str = _MATCH_LOG_PATH,
    top_k: int = 5,
) -> pd.DataFrame:
    """Match offer items to WBS nodes and compute scoring signals."""

    if df_items.empty:
        raise ValueError("No items available for matching")
    vectorizer = TfidfVectorizer(ngram_range=(1, 2), min_df=1)
    wbs_texts = wbs_index.apply(_prepare_text, axis=1)
    wbs_matrix = vectorizer.fit_transform(wbs_texts)

    ensure_directories([Path(log_path).parent])

    match_rows: List[Dict[str, Any]] = []
    for idx, row in df_items.iterrows():
        item_text = _item_text(row)
        item_vec = vectorizer.transform([item_text])
        cosine_scores = linear_kernel(item_vec, wbs_matrix).flatten()
        candidate_indices = cosine_scores.argsort()[-top_k:][::-1]
        best_result: Optional[Dict[str, Any]] = None
        item_disciplines = row.get("disciplines", set())
        if isinstance(item_disciplines, list):
            item_disciplines = set(item_disciplines)
        for candidate_idx in candidate_indices:
            candidate = wbs_index.iloc[int(candidate_idx)]
            signals = {
                "code_exact": _code_exact(
                    row.get("item_code"), candidate.get("wbs_code"), candidate.get("discipline")
                ),
                "desc_fuzzy": fuzz.token_sort_ratio(
                    row.get("item_desc", ""),
                    " ".join(filter(None, [candidate.get("wbs_name"), candidate.get("wbs_description")])),
                )
                / 100,
                "tfidf_sim": float(cosine_scores[int(candidate_idx)]),
                "unit_check": _unit_check(row.get("unit"), candidate.get("unit")),
            }
            score = (
                0.5 * signals["code_exact"]
                + 0.25 * signals["desc_fuzzy"]
                + 0.2 * signals["tfidf_sim"]
                + 0.05 * signals["unit_check"]
            )
            boost = _discipline_boost(item_disciplines, candidate.get("discipline"))
            score *= boost
            signals["discipline_boost"] = boost
            status = "unmatched"
            if score >= 0.85:
                status = "auto_accepted"
            elif score >= 0.65:
                status = "needs_review"
            explain = (
                f"code_exact={signals['code_exact']:.2f}, desc_fuzzy={signals['desc_fuzzy']:.2f}, "
                f"tfidf={signals['tfidf_sim']:.2f}, unit={signals['unit_check']:.2f}, boost={boost:.2f}"
            )
            result = {
                "item_index": idx,
                "item_code": row.get("item_code"),
                "item_desc": row.get("item_desc"),
                "supplier": row.get("supplier"),
                "sheet_name": row.get("sheet_name"),
                "matched_wbs_code": candidate.get("wbs_code"),
                "matched_wbs_name": candidate.get("wbs_name"),
                "matched_wbs_desc": candidate.get("wbs_description"),
                "matched_wbs_discipline": candidate.get("discipline"),
                "match_score": score,
                "match_status": status,
                "signals": signals,
                "explain": explain,
            }
            if (best_result is None) or (result["match_score"] > best_result["match_score"]):
                best_result = result
        if best_result is None:
            continue
        log_match_decision(
            log_path,
            {
                "timestamp": timestamp(),
                "item_index": int(best_result["item_index"]),
                "supplier": best_result["supplier"],
                "sheet_name": best_result["sheet_name"],
                "item_code": best_result["item_code"],
                "matched_wbs_code": best_result["matched_wbs_code"],
                "score": best_result["match_score"],
                "status": best_result["match_status"],
                "signals": best_result["signals"],
                "explain": best_result["explain"],
            },
        )
        match_rows.append(best_result)

    matches = pd.DataFrame(match_rows)
    return matches


__all__ = ["match_items", "build_wbs_index"]

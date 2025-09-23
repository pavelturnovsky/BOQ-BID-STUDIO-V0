"""Hybrid TF-IDF and keyword search across offer items."""
from __future__ import annotations

import unicodedata
from typing import Iterable, Mapping, Optional, Sequence

import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import linear_kernel

from .disciplines import detect_disciplines
from .utils import clean_text


def _normalize_text(value: str) -> str:
    base = clean_text(value)
    normalized = unicodedata.normalize("NFKD", base)
    without_diacritics = "".join(ch for ch in normalized if not unicodedata.combining(ch))
    return without_diacritics.lower()


def _search_text(row: pd.Series) -> str:
    parts = [
        row.get("item_code"),
        row.get("item_desc"),
        row.get("matched_wbs_name"),
        row.get("matched_wbs_desc"),
        row.get("supplier"),
        row.get("sheet_name"),
    ]
    return _normalize_text(" ".join(filter(None, parts)))


def _collect_tokens(query: str) -> Sequence[str]:
    return [token for token in _normalize_text(query).split() if len(token) > 1]


def hybrid_search(
    df: pd.DataFrame,
    query: str,
    config: Mapping[str, Mapping[str, Iterable[str]]],
    top_n: int = 20,
) -> pd.DataFrame:
    """Return matching items ordered by relevance for the provided query."""

    if not query:
        raise ValueError("Query must not be empty")
    if df.empty:
        return pd.DataFrame(columns=["item_code", "item_desc", "supplier", "relevance", "reason"])

    if "disciplines" not in df.columns:
        df = df.copy()
        df["disciplines"] = [set() for _ in range(len(df))]

    tokens = _collect_tokens(query)
    disciplines = detect_disciplines(query, config)

    if disciplines:
        def _disc_filter(value: Iterable[str]) -> bool:
            if isinstance(value, set):
                return bool(value & disciplines)
            if isinstance(value, list):
                return bool(set(value) & disciplines)
            return False

        candidate_df = df[df["disciplines"].apply(_disc_filter)]
        if candidate_df.empty:
            candidate_df = df
    else:
        candidate_df = df

    search_space = candidate_df.apply(_search_text, axis=1)
    vectorizer = TfidfVectorizer(analyzer="char_wb", ngram_range=(3, 5), min_df=1)
    matrix = vectorizer.fit_transform(search_space)
    query_vec = vectorizer.transform([clean_text(query)])
    tfidf_scores = linear_kernel(query_vec, matrix).flatten()

    keyword_boost = []
    lower_tokens = [token.lower() for token in tokens]
    for text in search_space.str.lower():
        boost = 0.0
        reasons = []
        for token in lower_tokens:
            if token and token in text:
                boost += 0.1
                reasons.append(token)
        keyword_boost.append((boost, ", ".join(sorted(set(reasons)))))

    candidate_df = candidate_df.copy()
    relevance = []
    reasons_out = []
    for idx, tfidf_score in enumerate(tfidf_scores):
        boost, reason = keyword_boost[idx]
        relevance.append(tfidf_score + boost)
        reasons_out.append(reason or "tfidf")
    candidate_df["relevance"] = relevance
    candidate_df["reason"] = reasons_out
    candidate_df.sort_values("relevance", ascending=False, inplace=True)
    columns = [
        "item_code",
        "item_desc",
        "supplier",
        "relevance",
        "reason",
        "matched_wbs_code",
        "matched_wbs_name",
        "primary_discipline",
    ]
    for column in columns:
        if column not in candidate_df.columns:
            candidate_df[column] = None
    return candidate_df[columns].head(top_n).reset_index(drop=True)


__all__ = ["hybrid_search"]

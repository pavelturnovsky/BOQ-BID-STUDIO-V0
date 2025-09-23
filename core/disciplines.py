"""Discipline detection utilities."""
from __future__ import annotations

import re
from typing import Iterable, Mapping, Optional, Set

import pandas as pd

from .utils import clean_text


_DISCIPLINE_COLUMN = "disciplines"
_PRIMARY_DISCIPLINE = "primary_discipline"


def _compile_patterns(config: Mapping[str, Mapping[str, Iterable[str]]]) -> Mapping[str, Iterable[re.Pattern]]:
    compiled = {}
    for code, patterns in config.get("disciplines", {}).get("synonyms", {}).items():
        compiled[code] = [re.compile(pattern, re.IGNORECASE) for pattern in patterns]
    return compiled


def detect_disciplines(text: str, config: Mapping[str, Mapping[str, Iterable[str]]], extra_context: Optional[Iterable[str]] = None) -> Set[str]:
    """Return a set of discipline codes detected in the provided text."""

    compiled = _compile_patterns(config)
    context_parts = [text]
    if extra_context:
        context_parts.extend(extra_context)
    haystack = clean_text(" ".join(filter(None, context_parts)))
    matches: Set[str] = set()
    for discipline, patterns in compiled.items():
        if any(pattern.search(haystack) for pattern in patterns):
            matches.add(discipline)
    return matches


def assign_disciplines(df: pd.DataFrame, config: Mapping[str, Mapping[str, Iterable[str]]]) -> pd.DataFrame:
    """Annotate the dataframe with detected disciplines."""

    df = df.copy()
    compiled = _compile_patterns(config)

    def _detect(row: pd.Series) -> Set[str]:
        context = [row.get("discipline_hint"), row.get("sheet_name"), row.get("item_code")]
        haystack = clean_text(" ".join(filter(None, [row.get("item_desc", "")] + context)))
        matches = {disc for disc, patterns in compiled.items() if any(p.search(haystack) for p in patterns)}
        return matches

    df[_DISCIPLINE_COLUMN] = df.apply(_detect, axis=1)
    df[_PRIMARY_DISCIPLINE] = df[_DISCIPLINE_COLUMN].map(lambda s: sorted(s)[0] if s else "OTHER")
    return df


__all__ = ["detect_disciplines", "assign_disciplines"]

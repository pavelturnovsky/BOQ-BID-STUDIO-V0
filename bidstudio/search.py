"""Semantic search interfaces used within the Bid Studio pipeline."""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence

import numpy as np
import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import linear_kernel

logger = logging.getLogger(__name__)


@dataclass
class SearchResult:
    """A single hit returned by a :class:`SearchProvider`."""

    text: str
    score: float
    metadata: Dict[str, Any]


class SearchProvider(ABC):
    """Abstract base class that all search providers must implement."""

    def __init__(self) -> None:
        self._is_indexed = False

    @property
    def is_indexed(self) -> bool:
        return self._is_indexed

    @abstractmethod
    def index(
        self,
        frame: pd.DataFrame,
        text_columns: Sequence[str],
        metadata_columns: Optional[Sequence[str]] = None,
    ) -> None:
        """Build the internal index from the provided dataframe."""

    @abstractmethod
    def search(self, query: str, top_k: int = 5) -> List[SearchResult]:
        """Return the most relevant rows for ``query``."""


class TfidfSearchProvider(SearchProvider):
    """Local TF-IDF search implementation used as the offline fallback."""

    def __init__(self) -> None:
        super().__init__()
        self._vectorizer = TfidfVectorizer(stop_words="english")
        self._matrix = None
        self._documents: List[str] = []
        self._metadata: List[Dict[str, Any]] = []

    def index(
        self,
        frame: pd.DataFrame,
        text_columns: Sequence[str],
        metadata_columns: Optional[Sequence[str]] = None,
    ) -> None:
        if not text_columns:
            raise ValueError("At least one text column is required for indexing")

        logger.info("Indexing %d master rows using TF-IDF", len(frame))
        corpus = _combine_text_columns(frame, text_columns)
        self._matrix = self._vectorizer.fit_transform(corpus)
        self._documents = corpus

        if metadata_columns:
            available = [column for column in metadata_columns if column in frame.columns]
            if available:
                self._metadata = frame[available].to_dict(orient="records")
            else:
                self._metadata = [{} for _ in corpus]
        else:
            self._metadata = frame.to_dict(orient="records")

        self._is_indexed = True

    def search(self, query: str, top_k: int = 5) -> List[SearchResult]:
        if not self.is_indexed or self._matrix is None:
            raise RuntimeError("Search provider has not been indexed yet")

        if not query.strip():
            return []

        query_vector = self._vectorizer.transform([query])
        scores = linear_kernel(query_vector, self._matrix).flatten()
        best_indices = np.argsort(scores)[::-1]

        results: List[SearchResult] = []
        for index in best_indices[:top_k]:
            score = float(scores[index])
            if score <= 0:
                continue
            metadata = self._metadata[index] if index < len(self._metadata) else {}
            results.append(
                SearchResult(
                    text=self._documents[index],
                    score=score,
                    metadata=metadata,
                )
            )
        return results


def create_search_provider(provider_name: str) -> SearchProvider:
    """Instantiate a search provider by name, falling back to TF-IDF."""

    provider_name = (provider_name or "tfidf").lower()
    if provider_name in {"tfidf", "local", "fallback"}:
        return TfidfSearchProvider()

    logger.warning(
        "Unknown search provider '%s'; falling back to TF-IDF implementation",
        provider_name,
    )
    return TfidfSearchProvider()


def _combine_text_columns(frame: pd.DataFrame, columns: Sequence[str]) -> List[str]:
    combined: List[str] = []
    for _, row in frame[columns].fillna(" ").astype(str).iterrows():
        combined.append(" ".join(str(value) for value in row if value))
    return combined


__all__ = [
    "SearchProvider",
    "SearchResult",
    "TfidfSearchProvider",
    "create_search_provider",
]

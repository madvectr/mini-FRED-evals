"""TF-IDF based lexical retriever over Mini-FRED series cards."""

from __future__ import annotations

from pathlib import Path
from typing import Dict, List

from sklearn.feature_extraction.text import TfidfVectorizer  # type: ignore[import]
from sklearn.metrics.pairwise import linear_kernel  # type: ignore[import]


class TfidfRetriever:
    """Deterministic TF-IDF retriever for series card markdown files."""

    def __init__(self, cards_dir: Path):
        self.cards_dir = Path(cards_dir)
        self.vectorizer = TfidfVectorizer(
            stop_words="english",
            ngram_range=(1, 2),
            sublinear_tf=True,
            min_df=1,
        )
        self.doc_ids: List[str] = []
        self.documents: List[str] = []
        self.matrix = None

    def build(self) -> None:
        """Load all markdown cards and build the TF-IDF index."""
        if not self.cards_dir.exists():
            raise FileNotFoundError(
                f"Cards directory {self.cards_dir} not found. Generate cards first."
            )

        self.doc_ids.clear()
        self.documents.clear()

        for path in sorted(self.cards_dir.glob("*.md")):
            text = path.read_text(encoding="utf-8")
            doc_id = path.stem  # e.g., series_UNRATE
            self.doc_ids.append(doc_id)
            self.documents.append(text)

        if not self.documents:
            raise RuntimeError(
                f"No markdown files found in {self.cards_dir}; run build_series_cards."
            )

        self.matrix = self.vectorizer.fit_transform(self.documents)

    def retrieve(self, query: str, k: int = 3) -> List[Dict[str, object]]:
        """Return top-k documents ranked by cosine similarity."""
        if not query.strip():
            return []
        if not self.doc_ids or self.matrix is None:
            return []

        query_vec = self.vectorizer.transform([query])
        scores = linear_kernel(query_vec, self.matrix).ravel()
        ranked_indices = scores.argsort()[::-1][:k]

        results: List[Dict[str, object]] = []
        for idx in ranked_indices:
            score = float(scores[idx])
            if score <= 0:
                continue
            results.append(
                {
                    "doc_id": self.doc_ids[idx],
                    "score": score,
                    "text": self.documents[idx],
                }
            )
        return results

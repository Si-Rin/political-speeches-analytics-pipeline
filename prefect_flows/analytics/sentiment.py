"""
Sentiment classification — Stage B.

Independent of the shared spaCy Doc (see nlp_pipeline.py)

Interface:    transcript → sentiment → label + scores

Unlike labels.py's zero-shot multi-label scores (independent per-label presence, aggregated by MAX across chunks), this model outputs a single softmax distribution per chunk — negative/neutral/positive sum to 1, mutually exclusive
Aggregating those across chunks with MAX would break that invariant (you could not end up with a distribution that sums to 1 anymore)
So chunks are combined with a length-weighted AVERAGE instead — a document-level distribution that still sums to 1, dominated by its longer chunks rather than by whichever chunk happened to score highest on one label
"""
from collections import defaultdict
from typing import Dict, List

from prefect_flows.analytics.text_chunking import chunk_text

# cardiffnlp/twitter-roberta-base-sentiment-latest: 3-class (negative/neutral/positive) 
# A neutral class matters here since a lot of a political speech is factual/procedural rather than clearly polarized, unlike a binary pos/neg model
MODEL_NAME = "cardiffnlp/twitter-roberta-base-sentiment-latest"

CHUNK_WORD_BUDGET = 300

_pipeline = None


def get_pipeline():
    """Lazily load and cache the sentiment pipeline for this process."""
    global _pipeline
    if _pipeline is None:
        from transformers import pipeline
        _pipeline = pipeline(
            "text-classification",
            model=MODEL_NAME,
            top_k=None,  # return every class's score, not just the top one
        )
    return _pipeline


def analyze_sentiment(text: str) -> Dict:
    """
    Returns:
        {
          "label": "negative" | "neutral" | "positive" | None,
          "scores": {"negative": ..., "neutral": ..., "positive": ...},
        }
    scores sum to ~1 (length-weighted average of each chunk's softmax distribution)
    label is the argmax of scores, None/empty scores only if the text was empty
    """
    pipe = get_pipeline()
    chunks = chunk_text(text=text, tokenizer=pipe.tokenizer, model=pipe.model)

    weighted_scores: Dict[str, float] = defaultdict(float)
    total_words = 0
    for chunk in chunks:
        chunk_words = len(chunk.split())
        if chunk_words == 0:
            continue
        chunk_result = pipe(chunk, truncation=True)[0]  # list of {"label": ..., "score": ...} for every class
        for entry in chunk_result:
            weighted_scores[entry["label"].lower()] += entry["score"] * chunk_words
        total_words += chunk_words

    if total_words == 0:
        return {"label": None, "scores": {}}

    scores = {label: round(score / total_words, 4) for label, score in weighted_scores.items()}
    dominant_label = max(scores, key=scores.get)

    return {"label": dominant_label, "scores": scores}
"""
Emotion classification — Stage B.

Independent of the shared spaCy Doc (see nlp_pipeline.py)

Interface:    transcript → emotion → dominant emotion + scores

Same aggregation rationale as sentiment.py: this model outputs a single softmax distribution per chunk (the 7 classes sum to 1, mutually exclusive)
So chunks are combined with a length-weighted AVERAGE rather than labels.py's MAX 
That keeps the document-level result a valid probability distribution instead of an inflated, no-longer-summing-to-1 set of per-chunk maxima
"""
from collections import defaultdict
from typing import Dict

from prefect_flows.analytics.text_chunking import chunk_text

# j-hartmann/emotion-english-distilroberta-base: 7-class (anger, disgust, fear, joy, neutral, sadness, surprise), single softmax distribution
# Deliberately not SamLowe/roberta-base-go_emotions (28 fine-grained, multi-label/sigmoid emotions) — that's too granular for a single "dominant emotion" and isn't a probability distribution to average
MODEL_NAME = "j-hartmann/emotion-english-distilroberta-base"

CHUNK_WORD_BUDGET = 300

_pipeline = None


def get_pipeline():
    """Lazily load and cache the emotion pipeline for this process."""
    global _pipeline
    if _pipeline is None:
        from transformers import pipeline
        _pipeline = pipeline(
            "text-classification",
            model=MODEL_NAME,
            top_k=None,  # return every class's score, not just the top one
        )
    return _pipeline


def analyze_emotion(text: str) -> Dict:
    """
    Returns:
        {
          "dominant_emotion": "anger" | "disgust" | "fear" | "joy" | "neutral" | "sadness" | "surprise" | None,
          "scores": {"anger": ..., "disgust": ..., ...},
        }
    scores sum to ~1 (length-weighted average of each chunk's softmax distribution)
    dominant_emotion is the argmax of scores
    None/empty scores only if the text was empty
    """
    pipe = get_pipeline()
    chunks = chunk_text(text=text, tokenizer=pipe.tokenizer, model=pipe.model)

    weighted_scores: Dict[str, float] = defaultdict(float)
    total_words = 0
    for chunk in chunks:
        chunk_words = len(chunk.split())
        if chunk_words == 0:
            continue
        chunk_result = pipe(chunk)[0]  # list of {"label": ..., "score": ...} for every class
        for entry in chunk_result:
            weighted_scores[entry["label"].lower()] += entry["score"] * chunk_words
        total_words += chunk_words

    if total_words == 0:
        return {"dominant_emotion": None, "scores": {}}

    scores = {label: round(score / total_words, 4) for label, score in weighted_scores.items()}
    dominant_emotion = max(scores, key=scores.get)

    return {"dominant_emotion": dominant_emotion, "scores": scores}
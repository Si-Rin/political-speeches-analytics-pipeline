"""
Zero-shot multi-label classification — Stage B.

Independent of the shared spaCy Doc (see nlp_pipeline.py): this module works directly off raw transcript text through an NLI-based zero-shot classifier
It classifies each document against a FIXED policy-domain taxonomy (CANDIDATE_LABELS below) 
This is the "supervised classification" counterpart to topics.py's unsupervised/emergent topic modeling (see the distinction noted in init_schema.sql: "topics" = unsupervised, "labels" = supervised-style)
Populates gold.analytics.labels.

CANDIDATE_LABELS is the single most opinionated thing in this file — it's a starting taxonomy for US-style political speech, not a fixed fact. 
Tune it for the corpus; each label is used verbatim as an NLI hypothesis ("This text is about {label}."), so keep labels short, mutually distinguishable noun phrases rather than sentences.
"""
from typing import Dict, List

from prefect_flows.analytics.text_chunking import chunk_text

CANDIDATE_LABELS: List[str] = [
    "economy and jobs",
    "healthcare",
    "immigration",
    "foreign policy and national security",
    "education",
    "climate and environment",
    "taxation and government spending",
    "crime and justice",
    "civil rights and social issues",
    "elections and democracy",
    "energy",
    "technology and innovation",
]

# facebook/bart-large-mnli: the standard NLI-based zero-shot classifier — well-documented, robust default. 
# Swap for a smaller/faster model (e.g. MoritzLaurer/deberta-v3-base-zeroshot-v1.1-all-33) if batch latency becomes the bottleneck; the rest of this module doesn't change.
MODEL_NAME = "facebook/bart-large-mnli"

# A label is kept in the final "labels" list if its (max-aggregated, see classify_labels) score is at or above this. 
# Independent per-label thresholding is what "multi_label=True" buys us — scores don't have to sum to 1, so a speech can legitimately score high on several labels.
DEFAULT_THRESHOLD = 0.5

# bart-large-mnli's positional embeddings max out at 1024 tokens, and each candidate label adds its own hypothesis-template tokens on top of the premise (the chunk text) at inference time
# Budgeting chunks in words (roughly 1.3 tokens/word for English) rather than calling the tokenizer just to size them keeps this module import-cheap
# A conservative estimate, not an exact token count.
CHUNK_WORD_BUDGET = 300

_classifier = None


def get_classifier():
    """Lazily load and cache the zero-shot pipeline for this process."""
    global _classifier
    if _classifier is None:
        from transformers import pipeline
        _classifier = pipeline("zero-shot-classification", model=MODEL_NAME)
    return _classifier


def classify_labels(
    text: str,
    candidate_labels: List[str] = CANDIDATE_LABELS,
    threshold: float = DEFAULT_THRESHOLD,
) -> Dict:
    """
    Zero-shot multi-label classification of a document against candidate_labels.

    Long documents are split into sentence-bounded chunks (see text_chunking.chunk_text) and each chunk is scored independently
    A label's final score is the MAX across chunks — a document counts as "about" a label if any part of it strongly discusses it, not just on average across the whole speech. 
    Trade-off: a short, intense aside in an otherwise unrelated speech can surface a label on the strength of one sentence. 
    If that over-triggers in practice, switch the aggregation to mean or a length-weighted average instead — the chunk-scoring loop is the only place that needs to change.

    Returns:
        {
          "scores": {label: max_score, ...},    # every candidate label, sorted descending
          "labels": [label, ...],               # scores >= threshold; always includes the single best-scoring label even if it's
                                                # below threshold, so a document never ends up with an empty label set
          "threshold": threshold,
        }
    """
    classifier = get_classifier()
    chunks = chunk_text(text=text, tokenizer=classifier.tokenizer, model=classifier.model, reserved_special_tokens=classifier.tokenizer.num_special_tokens_to_add(pair=True))

    best_scores = {label: 0.0 for label in candidate_labels}
    for chunk in chunks:
        result = classifier(chunk, candidate_labels, multi_label=True, truncation=True)
        for label, score in zip(result["labels"], result["scores"]):
            if score > best_scores[label]:
                best_scores[label] = score

    ranked = sorted(best_scores.items(), key=lambda kv: kv[1], reverse=True)
    selected = [label for label, score in ranked if score >= threshold]
    if not selected and ranked:
        selected = [ranked[0][0]]

    return {
        "scores": {label: round(score, 4) for label, score in ranked},
        "labels": selected,
        "threshold": threshold,
    }
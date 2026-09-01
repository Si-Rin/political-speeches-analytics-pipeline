"""
Shared spaCy pipeline (Stage A only).

Loads the model once per worker process and exposes get_doc(text) so every Stage-A Gold module — lexical metrics, syntactic metrics, and NER — parses each document exactly once and reuses the same spaCy Doc, instead of each module re-tokenizing/re-tagging the same transcript from scratch.

en_core_web_sm's default pipeline already includes tagger, parser, lemmatizer AND ner — so doc.ents (entities) comes for free from the same get_doc(text) call used for lexical/syntactic metrics, no separate model or pass needed.

Stage B (sentiment/emotion) and Stage C (topics/themes) do NOT build on this shared Doc:
  - Stage B may optionally use doc.sents for sentence-level granularity, but its own model does its own tokenization internally — it isn't reading spaCy token/tag data the way Stage A modules do.
  - Stage C (BERTopic/embeddings) works directly off the raw transcript. Embedding models generally want natural text, not a lemmatized/spaCy-tokenized version of it, so it has no dependency on this Doc.

Usage in a flow:
    from prefect_flows.analytics.nlp_pipeline import get_doc

    doc = get_doc(transcript)                  # parsed once
    lex = compute_lexical_metrics(doc)
    syn = compute_syntactic_metrics(doc)
    entities = extract_entities(doc)            # reuses the same doc, doc.ents is already there
"""
import spacy

# en_core_web_sm: fast, no word vectors, CPU-friendly — the right default for a batch pipeline processing many documents. Swap to en_core_web_trf
# if NER/parsing quality becomes the bottleneck later; it's much slower and needs a transformer runtime, so start here.
MODEL_NAME = "en_core_web_sm"

_nlp = None


def get_nlp():
    """Lazily load and cache the spaCy pipeline for this process."""
    global _nlp
    if _nlp is None:
        _nlp = spacy.load(MODEL_NAME)
    return _nlp


def get_doc(text: str):
    """Parse text into a spaCy Doc using the shared Stage-A pipeline."""
    return get_nlp()(text or "")
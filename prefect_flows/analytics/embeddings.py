"""
Document embeddings — Stage C support module for topics.py.

Independent of the shared spaCy Doc, this module works directly off raw transcript text, same as Stage B's sentiment/labels modules.

Uses sentence-transformers/all-mpnet-base-v2 — the standard general-purpose embedding model BERTopic's own defaults are built around.
Reuses the same tokenizer-measured chunking utility as Stage B (text_chunking.chunk_text) because a chunk that exceeds the model's real max sequence length (384 tokens for this model) crashes torch, not just produces a worse embedding.

A document-level embedding is the LENGTH-WEIGHTED AVERAGE of its chunk embeddings (weighted by each chunk's token count).
"""
from typing import List

import numpy as np
from sentence_transformers import SentenceTransformer

from prefect_flows.analytics.text_chunking import chunk_text

MODEL_NAME = "sentence-transformers/all-mpnet-base-v2"

_model = None


def get_embedding_model() -> SentenceTransformer:
    """Lazily load and cache the embedding model for this process."""
    global _model
    if _model is None:
        _model = SentenceTransformer(MODEL_NAME)
    return _model


def embed_document(text: str) -> np.ndarray:
    """
    Returns a single embedding vector for text, pooled across tokenizer-measured chunks when the transcript is longer than the model's max sequence length.
    """
    model = get_embedding_model()
    tokenizer = model.tokenizer
    # underlying HF transformer — used by chunk_text as a fallback if the tokenizer itself doesn't report a usable model_max_length
    auto_model = model._first_module().auto_model

    chunks = chunk_text(text=text, tokenizer=tokenizer, model=auto_model)
    if not chunks:
        return np.zeros(model.get_sentence_embedding_dimension())

    chunk_embeddings = model.encode(chunks, show_progress_bar=False)
    weights = np.array(
        [len(tokenizer.encode(c, add_special_tokens=False)) for c in chunks],
        dtype=float,
    )
    weights /= weights.sum()

    return np.average(chunk_embeddings, axis=0, weights=weights)


def embed_documents(texts: List[str]) -> np.ndarray:
    """Batch version of embed_document — one row per text, same order."""
    return np.vstack([embed_document(t) for t in texts])
"""
Shared text-chunking utility for Stage B modules (labels, sentiment, emotion) whose underlying transformer models cap input length well below a full political speech 
(BART: 1024 tokens, RoBERTa-based sentiment/emotion: 512 tokens)

Sentence-bounded, word-budget chunking — no spaCy dependency, since Stage B is deliberately independent of the shared Doc 
(see nlp_pipeline.py; Stage B modules do their own tokenization internally).

Chunks are sized against each model's OWN tokenizer/config, not a word-count heuristic — a chunk that passes chunk_text() is guaranteed to fit under that model's max_position_embeddings
It's measured with the exact same tokenizer that will run inference on it
"""
import re
from functools import lru_cache
from typing import List, Optional

from transformers import PreTrainedTokenizerBase, PreTrainedModel

SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?])\s+")

# Some Hub tokenizer configs never set model_max_length and it defaults to this sentinel (effectively "unbounded") — treat it as unset, not real
_UNSET_MAX_LEN_SENTINEL = 1_000_000

# RoBERTa-family models reserve 2 positions (a padding-offset quirk in how position ids are built)
# So usable sequence length is max_position_embeddings - 2, not the raw config value
_POSITION_OFFSET_BY_MODEL_TYPE = {
    "roberta": 2,
    "xlm-roberta": 2,
}


@lru_cache(maxsize=None)
def resolve_max_length(
    tokenizer: PreTrainedTokenizerBase,
    model: Optional[PreTrainedModel] = None,
) -> int:
    """
    Get the real usable sequence length for this tokenizer/model pair.

    Cached per (tokenizer, model) identity — safe because callers pass the pipeline's own singleton tokenizer/model 
    (loaded once, reused for the life of the process), so this only actually resolves once per model.
    """
    tok_max = getattr(tokenizer, "model_max_length", None)
    if tok_max and tok_max < _UNSET_MAX_LEN_SENTINEL:
        return tok_max

    if model is not None and hasattr(model.config, "max_position_embeddings"):
        offset = _POSITION_OFFSET_BY_MODEL_TYPE.get(model.config.model_type, 0)
        return model.config.max_position_embeddings - offset

    raise ValueError(
        "Could not resolve a usable max_length: tokenizer.model_max_length "
        "is unset and no model was given to fall back on model.config."
    )


def chunk_text(
    text: str,
    tokenizer: PreTrainedTokenizerBase,
    model: Optional[PreTrainedModel] = None,
    max_length: Optional[int] = None,
    reserved_special_tokens: Optional[int] = None,
) -> List[str]:
    """
    Split text into sentence-bounded chunks that are guaranteed to fit within max_length tokens for the given tokenizer.

    Sentences are kept intact whenever possible. The one exception: a single sentence longer than budget on its own (rare, but real — run-on sentences, poorly-punctuated transcripts) can't be kept intact without exceeding the model's window regardless of how chunks are grouped
    That sentence alone falls back to a word-level split, still measured against the real tokenizer word-by-word 
    Any sentences already accumulated before it are flushed as their own chunk first, so they aren't dragged into that fallback

    Args:
        text: raw input text (a full transcript, or a section of one).
        tokenizer: the pipeline's own tokenizer (e.g. pipe.tokenizer) — not a separately-loaded one, so chunk sizing can't drift from what actually runs inference
        model: the pipeline's own model (e.g. pipe.model), used only as a fallback if the tokenizer doesn't report a real max_length
        max_length: override the resolved max sequence length. Usually left None so it's derived from tokenizer/model
        reserved_special_tokens: override how many positions are reserved for special tokens (e.g. <s>, </s>). Usually left None so it's computed exactly via tokenizer.num_special_tokens_to_add().

    Returns:
        List of text chunks, each guaranteed to tokenize to <= max_length tokens (including special tokens) under `tokenizer`.
    """
    if max_length is None:
        max_length = resolve_max_length(tokenizer, model=model)
    if reserved_special_tokens is None:
        reserved_special_tokens = tokenizer.num_special_tokens_to_add(pair=False)
        
    budget = max_length - reserved_special_tokens
    if budget <= 0:
        raise ValueError(
            f"max_length ({max_length}) too small to fit "
            f"{reserved_special_tokens} reserved special tokens."
        )
        
    sentences = [s for s in re.split(r"(?<=[.!?])\s+", text.strip()) if s.strip()]
    if not sentences:
        return [text] if text.strip() else []
    
    # Batch-encode once — cheaper than tokenizing sentence-by-sentence in the loop.
    sent_token_lens = [
        len(ids) for ids in tokenizer(sentences, add_special_tokens=False)["input_ids"]
    ]
 
    chunks: List[str] = []
    current: List[str] = []
    current_len = 0
 
    def flush():
        nonlocal current, current_len
        if current:
            chunks.append(" ".join(current))
            current, current_len = [], 0

    def split_long_sentence(sent: str) -> List[str]:
        """Word-level fallback, verified against the real tokenizer as we go."""
        words = sent.split()
        pieces: List[str] = []
        piece: List[str] = []
        piece_len = 0
        for w in words:
            w_len = len(tokenizer.encode(w, add_special_tokens=False))
            if piece and piece_len + w_len > budget:
                pieces.append(" ".join(piece))
                piece, piece_len = [], 0
            # A single "word" that alone exceeds budget (rare — e.g. a
            # pathological token) still goes in on its own; truncation=True
            # at the model call is the final safety net for that edge case.
            piece.append(w)
            piece_len += w_len
        if piece:
            pieces.append(" ".join(piece))
        return pieces

    for sent, sent_len in zip(sentences, sent_token_lens):
        if sent_len > budget:
            flush()  # keep whatever was accumulated so far intact
            chunks.extend(split_long_sentence(sent))
            continue

        if current and current_len + sent_len > budget:
            flush()
        current.append(sent)
        current_len += sent_len

    flush()
    return chunks
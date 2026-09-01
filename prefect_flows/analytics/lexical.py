"""
Lexical & rhetorical metrics — Stage A, operates on a shared spaCy Doc.

Parsing happens once, upstream, via nlp_pipeline.get_doc(); this module only reads token/lemma/sentence data off the Doc, it never re-tokenizes or re-tags text itself. 
See syntactic.py for the POS/tense/modality counterpart that reads the same Doc.

Computes:
  - pronoun_ratios: first/second/third-person pronoun usage per 1000 words (classic political-speech signal: heavy "we/us" framing vs "they/them")
  - top_content_words: most frequent non-stopword content words. This is a raw-frequency proxy, NOT true buzzword/slogan detection (real buzzword detector would need corpus-relative weighting (e.g. TF-IDF against a reference corpus of political speech in general) to separate "salient to this speech" from "just a common word".
  - repetitions: anaphora detection (repeated sentence-opening phrases)
  - oppositions: counts of predefined binary-framing word pairs (freedom vs tyranny, strength vs weakness, etc.)
  - bigrams / trigrams: frequent n-grams 
  - lexical_diversity: TTR and MTLD
"""
from collections import Counter
from typing import Dict, List, Tuple

# ---------------------------------------------------------------------------
# Pronoun categories (English). spaCy's tokenizer splits contractions ("we're" -> "we" + "'re", "you'll" -> "you" + "'ll"), so the bare pronoun alone is enough to catch contracted forms too 
# No need to list "we're", "we've", etc. separately the way a naive regex tokenizer would require
# ---------------------------------------------------------------------------
PRONOUN_CATEGORIES: Dict[str, set] = {
    "first_person_singular": {"i", "me", "my", "mine", "myself"},
    "first_person_plural": {"we", "us", "our", "ours", "ourselves"},
    "second_person": {"you", "your", "yours", "yourself", "yourselves"},
    "third_person_plural": {"they", "them", "their", "theirs", "themselves"},
}

# Common political binary framings — substantive words only, deliberately excluding pronouns (those are covered separately above)
OPPOSITION_PAIRS: List[Tuple[str, str]] = [
    ("freedom", "tyranny"),
    ("strength", "weakness"),
    ("winning", "losing"),
    ("truth", "lies"),
    ("safe", "dangerous"),
    ("prosperity", "decline"),
    ("hope", "fear"),
    ("unity", "division"),
    ("greatness", "failure"),
    ("peace", "war"),
    ("patriots", "radicals"),
    ("jobs", "unemployment"),
]

# Small, hardcoded stopword list — deliberately curated rather than using spaCy's built-in is_stop, so pronoun categories above stay excluded on purpose and the list stays predictable across spaCy model versions
STOPWORDS = {
    "a", "an", "the", "and", "or", "but", "if", "of", "at", "by", "for",
    "with", "about", "against", "between", "into", "through", "during",
    "before", "after", "above", "below", "to", "from", "up", "down", "in",
    "out", "on", "off", "over", "under", "again", "further", "then",
    "once", "is", "are", "was", "were", "be", "been", "being", "have",
    "has", "had", "having", "do", "does", "did", "doing", "will", "would",
    "should", "can", "could", "this", "that", "these", "those", "it",
    "its", "as", "so", "not", "no", "very", "just", "also", "there",
    "here", "what", "which", "who", "whom", "when", "where", "why", "how",
    "all", "each", "few", "more", "most", "other", "some", "such", "than",
    "too", "s", "t", "now",
    *PRONOUN_CATEGORIES["first_person_singular"],
    *PRONOUN_CATEGORIES["first_person_plural"],
    *PRONOUN_CATEGORIES["second_person"],
    *PRONOUN_CATEGORIES["third_person_plural"],
}

MTLD_MIN_TOKENS = 50


def tokens_from_doc(doc) -> List[str]:
    """
    Lowercase alphabetic tokens from a spaCy Doc. Punctuation and stray contraction fragments ("'re" in "we're" excluded by is_alpha since it contains an apostrophe) are dropped here

    Shared with syntactic.py (imported from there) so both modules agree on what counts as a "word" for their per-1000-word ratios
    """
    return [tok.lower_ for tok in doc if tok.is_alpha]


def pronoun_ratios(tokens: List[str]) -> Dict[str, float]:
    """Per-1000-word ratio for each pronoun category — normalized so speeches of different lengths are comparable."""
    total = len(tokens)
    if total == 0:
        return {cat: 0.0 for cat in PRONOUN_CATEGORIES}

    counts = Counter(tokens)
    return {
        category: round(sum(counts[w] for w in words) / total * 1000, 2)
        for category, words in PRONOUN_CATEGORIES.items()
    }


def top_ngrams(tokens: List[str], n: int, top_n: int = 15, min_count: int = 2) -> List[Dict]:
    """Frequent n-grams (phrases), excluding ones that are entirely stopwords (e.g. 'of the in' isn't a meaningful repeated phrase)."""
    ngrams = [tuple(tokens[i:i + n]) for i in range(len(tokens) - n + 1)]
    ngrams = [ng for ng in ngrams if not all(w in STOPWORDS for w in ng)]
    counts = Counter(ngrams)
    return [
        {"phrase": " ".join(ng), "count": count}
        for ng, count in counts.most_common(top_n)
        if count >= min_count
    ]


def calculate_ttr(tokens: List[str]) -> float:
    """Standard Type-Token Ratio (TTR): unique words / total words."""
    if not tokens:
        return 0.0
    return len(set(tokens)) / len(tokens)


def calculate_mtld(tokens: List[str], ttr_threshold: float = 0.720):
    """
    Measure of Textual Lexical Diversity (MTLD): average length of text segments needed to drop below a predefined TTR threshold (standard default 0.720)
    Returns None below MTLD_MIN_TOKENS rather than a misleading number on very short transcripts.
    """
    if len(tokens) < MTLD_MIN_TOKENS:
        return None

    def count_factors(token_list):
        factors = 0
        current_types = set()
        token_count = 0

        for token in token_list:
            current_types.add(token)
            token_count += 1
            current_ttr = len(current_types) / token_count
            if current_ttr < ttr_threshold:
                factors += 1
                current_types = set()
                token_count = 0

        if token_count > 0:
            final_ttr = len(current_types) / token_count
            if final_ttr < 1.0:
                factors += (1.0 - final_ttr) / (1.0 - ttr_threshold)

        return len(token_list) / factors if factors > 0 else len(token_list)

    forward_mtld = count_factors(tokens)
    backward_mtld = count_factors(list(reversed(tokens)))
    return (forward_mtld + backward_mtld) / 2.0


def top_content_words(tokens: List[str], top_n: int = 15) -> List[Dict[str, int]]:
    """Most frequent non-stopword tokens. Raw frequency, not semantic salience — see module docstring."""
    filtered = [t for t in tokens if t not in STOPWORDS and len(t) > 2]
    counts = Counter(filtered)
    return [{"word": word, "count": count} for word, count in counts.most_common(top_n)]


def anaphora_repetitions(doc, phrase_len: int = 3, min_repeats: int = 2) -> List[Dict]:
    """
    Detects anaphora: sentence-opening phrases repeated across the speech ('We will... We will... We will...')
    Uses spaCy's sentence segmentation (doc.sents) — the same segmentation syntactic.speech_stats() counts sentences with, so the two modules agree on sentence boundaries
    """
    openings = []
    for sent in doc.sents:
        words = [tok.lower_ for tok in sent if tok.is_alpha]
        if len(words) >= phrase_len:
            openings.append(" ".join(words[:phrase_len]))

    counts = Counter(openings)
    return [
        {"phrase": phrase, "count": count}
        for phrase, count in counts.most_common()
        if count >= min_repeats
    ]


def binary_oppositions(tokens: List[str]) -> List[Dict]:
    """Counts each side of predefined opposition pairs — surfaces us-vs-them / good-vs-bad framing common in political rhetoric."""
    counts = Counter(tokens)
    results = []
    for left, right in OPPOSITION_PAIRS:
        left_count = counts.get(left, 0)
        right_count = counts.get(right, 0)
        if left_count or right_count:
            results.append({"pair": [left, right], "counts": [left_count, right_count]})
    return results


def compute_lexical_metrics(doc) -> Dict:
    """Entry point — computes the gold.analytics.lex_metrics JSONB payload for one document from an already-parsed spaCy Doc."""
    tokens = tokens_from_doc(doc)
    return {
        "pronoun_ratios": pronoun_ratios(tokens),
        "top_content_words": top_content_words(tokens),
        "repetitions": anaphora_repetitions(doc),
        "oppositions": binary_oppositions(tokens),
        "bigrams": top_ngrams(tokens, n=2),
        "trigrams": top_ngrams(tokens, n=3),
        "lexical_diversity": {
            "ttr": calculate_ttr(tokens),
            "mtld": calculate_mtld(tokens) if len(tokens) >= MTLD_MIN_TOKENS else None,
        },
        "total_words": len(tokens),
    }
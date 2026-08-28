"""
Lexical & rhetorical metrics — pure Python/regex, no ML models.

Computes:
  - pronoun_ratios: first/second/third-person pronoun usage per 1000 words
    (classic political-speech signal: heavy "we/us" framing vs "they/them")
  - buzzwords: most frequent non-stopword content words
  - repetitions: anaphora detection (repeated sentence-opening phrases)
  - oppositions: counts of predefined binary-framing word pairs
    (freedom vs tyranny, strength vs weakness, etc.)
"""
import re
import string
from collections import Counter
from typing import Dict, List, Tuple

# ---------------------------------------------------------------------------
# Pronoun categories (English) — includes common contractions so "we're"
# counts the same as "we", not as a miss.
# ---------------------------------------------------------------------------
PRONOUN_CATEGORIES: Dict[str, set] = {
    "first_person_singular": {
        "i", "me", "my", "mine", "myself", "i'm", "i've", "i'll", "i'd",
    },
    "first_person_plural": {
        "we", "us", "our", "ours", "ourselves", "we're", "we've", "we'll", "we'd",
    },
    "second_person": {
        "you", "your", "yours", "yourself", "yourselves",
        "you're", "you've", "you'll", "you'd",
    },
    "third_person_plural": {
        "they", "them", "their", "theirs", "themselves",
        "they're", "they've", "they'll", "they'd",
    },
}

# Common political binary framings — substantive words only, deliberately
# excluding pronouns (those are covered separately above).
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

# Small, hardcoded stopword list — avoids pulling in nltk/spaCy just for
# this. Deliberately English-only, matching the project's single-language
# scope.
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
    "too", "s", "t", "don't", "now",
    *PRONOUN_CATEGORIES["first_person_singular"],
    *PRONOUN_CATEGORIES["first_person_plural"],
    *PRONOUN_CATEGORIES["second_person"],
    *PRONOUN_CATEGORIES["third_person_plural"],
}

WORD_RE = re.compile(r"[a-z]+(?:'[a-z]+)?")
MTLD_MIN_TOKENS = 50

def tokenize(text: str) -> List[str]:
    """Lowercase word tokenizer that keeps contractions as single tokens
    (e.g. 'we're' stays one token, not split into 'we' + 're')."""
    return WORD_RE.findall(text.lower())


def pronoun_ratios(tokens: List[str]) -> Dict[str, float]:
    """Per-1000-word ratio for each pronoun category — normalized so
    speeches of different lengths are comparable."""
    total = len(tokens)
    if total == 0:
        return {cat: 0.0 for cat in PRONOUN_CATEGORIES}

    counts = Counter(tokens)
    return {
        category: round(sum(counts[w] for w in words) / total * 1000, 2)
        for category, words in PRONOUN_CATEGORIES.items()
    }
    
def top_ngrams(tokens: List[str], n: int, top_n: int = 15, min_count: int = 2) -> List[Dict]:
    """Frequent n-grams (phrases), excluding ones that are entirely
    stopwords (e.g. 'of the in' isn't a meaningful repeated phrase)."""
    ngrams = [tuple(tokens[i:i + n]) for i in range(len(tokens) - n + 1)]
    ngrams = [ng for ng in ngrams if not all(w in STOPWORDS for w in ng)]
    counts = Counter(ngrams)
    return [
        {"phrase": " ".join(ng), "count": count}
        for ng, count in counts.most_common(top_n)
        if count >= min_count
    ]
    
def lexical_diversity(tokens: List[str]) -> float:
    """Type-token ratio: unique words / total words. Lower = more
    repetitive vocabulary (common in rally-style speeches with heavy
    sloganeering); higher = more varied word choice."""
    if not tokens:
        return 0.0
    return round(len(set(tokens)) / len(tokens), 4)

def calculate_ttr(tokens):
    """Calculates the standard Type-Token Ratio (TTR)."""
    if not tokens:
        return 0.0
    types = set(tokens)
    return len(types) / len(tokens)

def calculate_mtld(tokens, ttr_threshold=0.720):
    """
    Calculates the Measure of Textual Lexical Diversity (MTLD).
    MTLD computes the average length of text segments needed to drop 
    below a predefined TTR threshold (standard default is 0.720).
    """
    if len(tokens) < MTLD_MIN_TOKENS:
        return None  # explicit "not computed" rather than a misleading number
    
    if not tokens:
        return 0.0
        
    def count_factors(token_list):
        factors = 0
        current_types = set()
        token_count = 0
        
        for token in token_list:
            current_types.add(token)
            token_count += 1
            
            # Calculate current TTR
            current_ttr = len(current_types) / token_count
            
            # Check if TTR dropped below threshold
            if current_ttr < ttr_threshold:
                factors += 1
                current_types = set()
                token_count = 0
                
        # Handle the final incomplete segment
        if token_count > 0:
            # Linear interpolation for fractional factor
            final_ttr = len(current_types) / token_count
            if final_ttr < 1.0:
                fractional_factor = (1.0 - final_ttr) / (1.0 - ttr_threshold)
                factors += fractional_factor
            else:
                factors += 0.0  # Avoid division by zero if TTR is exactly 1.0
                
        return len(token_list) / factors if factors > 0 else len(token_list)

    # MTLD is calculated bi-directionally (forward and backward) and averaged
    forward_mtld = count_factors(tokens)
    backward_mtld = count_factors(list(reversed(tokens)))
    
    return (forward_mtld + backward_mtld) / 2.0

def buzzwords(tokens: List[str], top_n: int = 15) -> List[Dict[str, int]]:
    """Most frequent non-stopword tokens — a rough proxy for salient
    themes/slogans without needing a topic model."""
    filtered = [t for t in tokens if t not in STOPWORDS and len(t) > 2]
    counts = Counter(filtered)
    return [{"word": word, "count": count} for word, count in counts.most_common(top_n)]


def anaphora_repetitions(text: str, phrase_len: int = 3, min_repeats: int = 2) -> List[Dict]:
    """Detects anaphora: sentence-opening phrases repeated across the
    speech (e.g. 'We will... We will... We will...'). Naive sentence
    split on .!? — good enough for this signal, doesn't need spaCy."""
    sentences = re.split(r"(?<=[.!?])\s+", text.strip())
    openings = []
    for sentence in sentences:
        words = tokenize(sentence)
        if len(words) >= phrase_len:
            openings.append(" ".join(words[:phrase_len]))

    counts = Counter(openings)
    return [
        {"phrase": phrase, "count": count}
        for phrase, count in counts.most_common()
        if count >= min_repeats
    ]


def binary_oppositions(tokens: List[str]) -> List[Dict]:
    """Counts each side of predefined opposition pairs — surfaces
    us-vs-them / good-vs-bad framing common in political rhetoric."""
    counts = Counter(tokens)
    results = []
    for left, right in OPPOSITION_PAIRS:
        left_count = counts.get(left, 0)
        right_count = counts.get(right, 0)
        if left_count or right_count:
            results.append({"pair": [left, right], "counts": [left_count, right_count]})
    return results


def compute_lex_metrics(text: str) -> Dict:
    """Entry point — computes the full lex_metrics JSONB payload for one
    document's transcript."""
    tokens = tokenize(text)
    return {
        "pronoun_ratios": pronoun_ratios(tokens),
        "buzzwords": buzzwords(tokens),
        "repetitions": anaphora_repetitions(text),
        "oppositions": binary_oppositions(tokens),
        "bigrams": top_ngrams(tokens, n=2),
        "trigrams": top_ngrams(tokens, n=3),
        "lexical_diversity": {
            "ttr": calculate_ttr(tokens),
            "mtld": calculate_mtld(tokens) if len(tokens) >= MTLD_MIN_TOKENS else None,
        },
        "total_words": len(tokens),
    }
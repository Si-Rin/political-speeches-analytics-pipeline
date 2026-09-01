"""
Syntactic / rhetorical-structure metrics — Stage A, operates on the same shared spaCy Doc as lexical.py (see nlp_pipeline.get_doc)
Reuses lexical.tokens_from_doc() rather than re-deriving a token list, so both modules agree on what counts as a "word".

Computes:
  - speech_stats: basic speech-level counts (sentence count, avg sentence length, avg word length, question/exclamation counts)
  - modality: obligation / possibility / certainty markers per 1000 words.  - pos_distribution: coarse POS-tag distribution (% of tagged tokens)
  - verb_tense: past/present/future/non_finite breakdown (% of verbs)
"""
from collections import Counter
from typing import Dict, List, Tuple

from prefect_flows.analytics.lexical import tokens_from_doc

# Single-token modality markers, matched against token.lemma_ so contracted
# forms ("'ll" -> lemma "will") are caught without extra cases. Mixes modal
# verbs (must, could, will...) with adverbial hedges/boosters (possibly,
# definitely...) — both express the speaker's stance toward certainty of
# what they're saying, which is the actual concept being measured here.
MODAL_MARKERS: Dict[str, set] = {
    "obligation_modality": {"must", "should", "ought", "shall"},
    "possibility_modality": {"can", "could", "may", "might", "possibly", "perhaps", "maybe"},
    "certainty_modality": {
        "will", "definitely", "certainly", "surely", "undoubtedly",
        "clearly", "obviously", "always", "never",
    },
}

# Two-token modality phrases, matched as (first_token_lemma, second_token_text)
# on adjacent tokens — e.g. "have to" / "has to" / "had to" all share the
# lemma "have", "needs to" shares the lemma "need".
MODAL_PHRASES: Dict[str, List[Tuple[str, str]]] = {
    "obligation_modality": [
        ("have", "to"), ("need", "to"), ("require", "to"),
    ],
}

# Coarse POS buckets — spaCy's fine-grained token.tag_ is the Penn Treebank
# tagset.
POS_BUCKETS: Dict[str, set] = {
    "noun": {"NN", "NNS", "NNP", "NNPS"},
    "verb": {"VB", "VBD", "VBG", "VBN", "VBP", "VBZ"},
    "adjective": {"JJ", "JJR", "JJS"},
    "adverb": {"RB", "RBR", "RBS"},
    "pronoun": {"PRP", "PRP$"},
    "determiner": {"DT"},
    "preposition": {"IN"},
    "conjunction": {"CC"},
    "modal": {"MD"},
}


def speech_stats(doc, tokens: List[str]) -> Dict:
    """Basic speech-level counts: sentence count, average sentence/word
    length, and question/exclamation counts (rhetorical-question and
    exclamatory framing are common political-speech devices)."""
    sentences = list(doc.sents)
    sentence_count = len(sentences)
    words_per_sentence = [sum(1 for tok in sent if tok.is_alpha) for sent in sentences]
    avg_sentence_length = (
        round(sum(words_per_sentence) / sentence_count, 2) if sentence_count else 0.0
    )
    avg_word_length = round(sum(len(t) for t in tokens) / len(tokens), 2) if tokens else 0.0

    return {
        "sentence_count": sentence_count,
        "avg_sentence_length": avg_sentence_length,
        "avg_word_length": avg_word_length,
        "question_count": doc.text.count("?"),
        "exclamation_count": doc.text.count("!"),
    }


def modality_markers(doc, tokens: List[str]) -> Dict[str, float]:
    """Per-1000-word ratio of obligation / possibility / certainty
    modality markers. Single-token markers are matched by lemma (catching
    contracted forms like "'ll" -> "will" for free); two-token phrases
    ("have to", "need to"...) are matched on adjacent (lemma, text) pairs."""
    total = len(tokens)
    if total == 0:
        return {cat: 0.0 for cat in MODAL_MARKERS}

    doc_tokens = [tok for tok in doc if not tok.is_space]
    counts = {cat: 0 for cat in MODAL_MARKERS}

    for i, tok in enumerate(doc_tokens):
        lemma = tok.lemma_.lower()
        for category, lemmas in MODAL_MARKERS.items():
            if lemma in lemmas:
                counts[category] += 1

        if i + 1 < len(doc_tokens):
            next_text = doc_tokens[i + 1].lower_
            for category, phrases in MODAL_PHRASES.items():
                if (lemma, next_text) in phrases:
                    counts[category] += 1

    return {cat: round(n / total * 1000, 2) for cat, n in counts.items()}


def pos_distribution(tag_counts: Counter, total_tagged: int) -> Dict[str, float]:
    """Coarse POS-tag distribution as a percentage of tagged tokens."""
    if total_tagged == 0:
        return {bucket: 0.0 for bucket in POS_BUCKETS}
    return {
        bucket: round(sum(tag_counts[t] for t in tags) / total_tagged * 100, 2)
        for bucket, tags in POS_BUCKETS.items()
    }


def verb_tense(doc, tag_counts: Counter) -> Dict[str, float]:
    """past/present/future/non_finite breakdown, normalized against the
    verb count (not total words), since tense is only meaningful for verb
    tokens. 'future' is a periphrastic count (will/shall + a following
    verb, possibly with an adverb or subject in between, e.g. "will
    always fight", "Will we succeed?") layered on top of the raw tag
    counts, so the buckets are not expected to sum to 100."""
    verb_total = sum(tag_counts[t] for t in POS_BUCKETS["verb"])
    if verb_total == 0:
        return {"past": 0.0, "present": 0.0, "future": 0.0, "non_finite": 0.0}

    past = tag_counts["VBD"]
    present = tag_counts["VBP"] + tag_counts["VBZ"]
    non_finite = tag_counts["VBG"] + tag_counts["VBN"] + tag_counts["VB"]

    tagged = [(tok.lower_, tok.tag_) for tok in doc]
    future = 0
    for i, (word, tag) in enumerate(tagged):
        if tag == "MD" and word in {"will", "shall"}:
            for next_word, next_tag in tagged[i + 1:i + 5]:
                if next_tag in {".", ",", "?", "!"}:
                    break
                if next_tag.startswith("VB"):
                    future += 1
                    break

    return {
        "past": round(past / verb_total * 100, 2),
        "present": round(present / verb_total * 100, 2),
        "future": round(future / verb_total * 100, 2),
        "non_finite": round(non_finite / verb_total * 100, 2),
    }


def compute_syntactic_metrics(doc) -> Dict:
    """Entry point — computes the gold.analytics.syntactic_metrics JSONB
    payload for one document from an already-parsed spaCy Doc (the same
    Doc passed to lexical.compute_lexical_metrics())."""
    tokens = tokens_from_doc(doc)
    tag_counts = Counter(tok.tag_ for tok in doc)
    total_tagged = len(doc)

    return {
        "speech_stats": speech_stats(doc, tokens),
        "modality": modality_markers(doc, tokens),
        "pos_distribution": pos_distribution(tag_counts, total_tagged),
        "verb_tense": verb_tense(doc, tag_counts),
        "total_tagged_tokens": total_tagged,
    }
"""
Unsupervised topic modeling — Stage C.

Independent of the shared spaCy Doc and unlike every other Gold module, NOT a per-document function.
BERTopic clusters documents RELATIVE TO EACH OTHER (UMAP + HDBSCAN over the corpus' embeddings), the unit of work is the corpus, not the document.

BERTopic represents every topic by its own c-TF-IDF top terms.

KEYWORD QUALITY (why the vectorizer is configured the way it is)
BERTopic fits the vectorizer on ONE concatenated document PER TOPIC (the outlier topic -1 included), not per speech.
Consequences that shaped the settings below:
  - min_df=2 would require a word to appear in 2+ topics: it discards topic-specific words and keeps the ones shared by every topic
    (thank, great, going, know...). So min_df=1.
  - max_df is NOT used: with only a few topics (2-5 plus the outlier group) it deletes genuinely topical words that happen to appear in
    the outlier group and one topic. Generic words are filtered by DOCUMENT frequency across the speeches instead (corpus_stop_words),
    which does not depend on how many topics were found.
  - The token pattern keeps letters only (2+ chars): no numeric tokens such as "1130", but "UK", "EU", "UN" are kept.
Clustering (embeddings, UMAP, HDBSCAN) is untouched by the vectorizer: topic assignments are identical, only the keywords change.
"""
import logging
from typing import Dict, Iterable, List, Set

import numpy as np
from bertopic import BERTopic
from bertopic.representation import MaximalMarginalRelevance
from bertopic.vectorizers import ClassTfidfTransformer
from sklearn.feature_extraction.text import ENGLISH_STOP_WORDS, CountVectorizer
from umap import UMAP

from prefect_flows.analytics.embeddings import embed_documents, get_embedding_model

logger = logging.getLogger(__name__)

MIN_DOCS_FOR_TOPIC_MODELING = 20

# HDBSCAN's minimum cluster size
MIN_TOPIC_SIZE = 10

TOP_N_KEYWORDS = 10

# BERTopic's reserved label for not assigned documents to any cluster — "no dominant topic"
OUTLIER_TOPIC_ID = -1

# Letters only, 2+ chars: drops "1130", "2024", "10" and one-letter contraction fragments, keeps "uk", "eu", "un"
TOKEN_PATTERN = r"(?u)\b[^\W\d_]{2,}\b"

# Spoken-language filler and contraction fragments, on top of sklearn's English list.
# "make" / "great" are deliberately NOT forced here beyond "great": remove "great" from this set if "make america great again"-style phrases should surface.
FILLER_STOP_WORDS: Set[str] = {
    "thank", "thanks", "great", "going", "know", "like", "people", "just", "think",
    "said", "want", "really", "right", "got", "let", "yeah", "okay", "gonna",
    "ve", "ll", "re", "don", "didn", "doesn", "isn", "won", "ain",
}

# A word present in at least this share of the documents is generic FOR THIS CORPUS (computed per corpus: long-form and short-form separately).
# Short posts have low document frequencies, so the short-form corpus may need a lower value (0.15-0.2)
DEFAULT_MAX_DOC_FREQ = 0.3


def corpus_stop_words(texts: Iterable[str], max_doc_freq: float = DEFAULT_MAX_DOC_FREQ) -> Set[str]:
    """Words whose DOCUMENT frequency (share of documents containing them) is >= max_doc_freq."""
    texts = list(texts)
    counter = CountVectorizer(stop_words="english", token_pattern=TOKEN_PATTERN, binary=True)
    matrix = counter.fit_transform(texts)
    doc_freq = np.asarray(matrix.sum(axis=0)).ravel() / matrix.shape[0]
    return {word for word, freq in zip(counter.get_feature_names_out(), doc_freq) if freq >= max_doc_freq}


def build_stop_words(texts: Iterable[str], max_doc_freq: float = DEFAULT_MAX_DOC_FREQ) -> List[str]:
    generic = corpus_stop_words(texts, max_doc_freq)
    logger.info(
        "Corpus-specific stop words (document frequency >= %.0f%%): %s",
        max_doc_freq * 100, ", ".join(sorted(generic)) or "none",
    )
    return sorted(ENGLISH_STOP_WORDS | FILLER_STOP_WORDS | generic)


def fit_topics(doc_ids: List[int], texts: List[str], max_doc_freq: float = DEFAULT_MAX_DOC_FREQ) -> Dict[int, dict]:
    """
    Fits a fresh BERTopic model over the FULL current corpus and returns a per-document result:

        {
          doc_id: {
            "topic_id": int,
            "topic_keywords": [word, ...],   # topic's top c-TF-IDF terms (diversified with MMR)
            "probability": float,
          },
          ...
        }

    Topic boundaries and IDs can shift as the corpus grows -> re-running after ingesting new documents reassigns topic_ids for the WHOLE corpus, not just the new docs.
    Refit-from-scratch, not incremental.
    """
    if len(texts) < MIN_DOCS_FOR_TOPIC_MODELING:
        raise ValueError(
            f"Only {len(texts)} document(s) available; need at least "
            f"{MIN_DOCS_FOR_TOPIC_MODELING} for topic modeling to produce "
            f"meaningful clusters."
        )

    embeddings = embed_documents(texts)

    # ngram_range=(1, 2) enables one-word and two-word keywords extraction
    # min_df=1: the vectorizer sees one text per TOPIC
    vectorizer_model = CountVectorizer(
        stop_words=build_stop_words(texts, max_doc_freq),
        token_pattern=TOKEN_PATTERN,
        ngram_range=(1, 2),
        min_df=1,
    )

    # Down-weights words that are frequent across the whole corpus inside the class-based TF-IDF
    ctfidf_model = ClassTfidfTransformer(reduce_frequent_words=True, bm25_weighting=True)

    # Picks keywords that are relevant AND different from each other (fewer near-synonyms)
    representation_model = MaximalMarginalRelevance(diversity=0.3, top_n_words=TOP_N_KEYWORDS)

    # Seeding umap (dimentionality reduction technique) for reproducibility
    umap_model = UMAP(random_state=42)

    topic_model = BERTopic(
        umap_model=umap_model,
        min_topic_size=MIN_TOPIC_SIZE,
        calculate_probabilities=True,
        vectorizer_model=vectorizer_model,
        ctfidf_model=ctfidf_model,
        representation_model=representation_model,
        embedding_model=get_embedding_model(),   # MMR embeds candidate keywords
        verbose=False,
    )
    topic_ids, probabilities = topic_model.fit_transform(texts, embeddings=embeddings)

    results: Dict[int, dict] = {}
    for i, (doc_id, topic_id) in enumerate(zip(doc_ids, topic_ids)):
        if topic_id == OUTLIER_TOPIC_ID:
            keywords: List[str] = []
            probability = 0.0
        else:
            keywords = [word for word, _score in topic_model.get_topic(topic_id)[:TOP_N_KEYWORDS]]
            prob_row = probabilities[i]
            probability = float(prob_row[topic_id]) if hasattr(prob_row, "__len__") else float(prob_row)

        results[doc_id] = {
            "topic_id": int(topic_id),
            "topic_keywords": keywords,
            "probability": round(probability, 4),
        }

    return results
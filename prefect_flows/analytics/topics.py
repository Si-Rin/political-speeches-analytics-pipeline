"""
Unsupervised topic modeling — Stage C.

Independent of the shared spaCy Doc and unlike every other Gold module, NOT a per-document function.
BERTopic clusters documents RELATIVE TO EACH OTHER (UMAP + HDBSCAN over the corpus' embeddings), the unit of work is the corpus, not the document.

BERTopic represents every topic by its own c-TF-IDF top terms
"""
from typing import Dict, List

from bertopic import BERTopic
from umap import UMAP
from sklearn.feature_extraction.text import CountVectorizer

from prefect_flows.analytics.embeddings import embed_documents

MIN_DOCS_FOR_TOPIC_MODELING = 20

# HDBSCAN's minimum cluster size
MIN_TOPIC_SIZE = 10

TOP_N_KEYWORDS = 10

# BERTopic's reserved label for not assigned documents to any cluster — "no dominant topic"
OUTLIER_TOPIC_ID = -1


def fit_topics(doc_ids: List[int], texts: List[str]) -> Dict[int, dict]:
    """
    Fits a fresh BERTopic model over the FULL current corpus and returns a per-document result:

        {
          doc_id: {
            "topic_id": int,
            "topic_keywords": [word, ...],   # topic's top c-TF-IDF terms
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
    # min_df=2 drops n-grams that appear in only one document
    vectorizer_model = CountVectorizer(stop_words="english", ngram_range=(1, 2), min_df=2)

    # Seeding umap (dimentionality reduction technique) for reproducibility
    umap_model = UMAP(random_state=42)

    topic_model = BERTopic(
        umap_model=umap_model,
        min_topic_size=MIN_TOPIC_SIZE,
        calculate_probabilities=True,
        vectorizer_model=vectorizer_model,
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
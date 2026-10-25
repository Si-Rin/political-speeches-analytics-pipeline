"""Analytics models."""
from datetime import date
from typing import Dict, List, Optional

from pydantic import BaseModel


# ---------------------------------------------------------------------------
# Analytics: Overview
# ---------------------------------------------------------------------------
class SourceTypeCount(BaseModel):
    source_type: str
    count: int


class OriginCount(BaseModel):
    source_name: str
    count: int


class MonthCount(BaseModel):
    month: str  # "YYYY-MM"
    count: int


class WordCountStats(BaseModel):
    min: int
    max: int
    mean: float
    median: float
    stddev: float


class OverviewResponse(BaseModel):
    total_documents: int
    by_source_type: List[SourceTypeCount]
    by_origin: List[OriginCount]
    date_min: Optional[date] = None
    date_max: Optional[date] = None
    silver_success: int
    silver_failed: int
    word_count_stats: Optional[WordCountStats] = None
    documents_over_time: List[MonthCount]


# ---------------------------------------------------------------------------
# Analytics: Topics
# ---------------------------------------------------------------------------
class TopicSummary(BaseModel):
    corpus: str
    topic_id: int
    n_docs: int
    keywords: List[str]
    is_outlier: bool


class TopicValidation(BaseModel):
    corpus: str
    topic_id: int
    n_docs: int
    dominant_label: str
    purity: float
    lift: Optional[float] = None  # None when the label never appears in the corpus baseline


class TopicsResponse(BaseModel):
    topics: List[TopicSummary]
    validation: List[TopicValidation]


# ---------------------------------------------------------------------------
# Analytics: Labels
# ---------------------------------------------------------------------------
class LabelCount(BaseModel):
    label: str
    count: int
    pct: float


class LabelsResponse(BaseModel):
    long_form: List[LabelCount]
    short_form: List[LabelCount]


# ---------------------------------------------------------------------------
# Analytics: Sentiment & Emotion
# ---------------------------------------------------------------------------
class SentimentCount(BaseModel):
    label: str
    count: int
    pct: float


class SentimentOverTime(BaseModel):
    month: str
    positive: int
    negative: int
    neutral: int


class SentimentResponse(BaseModel):
    distribution: List[SentimentCount]
    over_time: List[SentimentOverTime]


class EmotionCount(BaseModel):
    emotion: str
    count: int
    pct: float


class EmotionsResponse(BaseModel):
    distribution: List[EmotionCount]


# ---------------------------------------------------------------------------
# Analytics: Entities
# ---------------------------------------------------------------------------
class EntityCount(BaseModel):
    text: str
    count: int


class EntitiesResponse(BaseModel):
    persons: List[EntityCount]
    locations: List[EntityCount]
    groups: List[EntityCount]


# ---------------------------------------------------------------------------
# Analytics: Corpus summary (lexical / rhetorical / affective aggregates over every analysed document)
# ---------------------------------------------------------------------------
class WordCount(BaseModel):
    word: str
    count: int


class PhraseCount(BaseModel):
    phrase: str
    count: int


class OppositionTotal(BaseModel):
    pair: str            # "word1 vs word2"
    left: str
    right: str
    left_count: int
    right_count: int


class CorpusSummaryResponse(BaseModel):
    documents_analyzed: int
    total_words: int
    avg_ttr: Optional[float] = None                   # mean type-token ratio
    avg_sentence_length: Optional[float] = None       # mean words / sentence
    sentiment_scores: Dict[str, float] = {}           # mean per-document score of each sentiment label
    emotion_scores: Dict[str, float] = {}             # mean per-document score of each emotion
    label_distribution: List[LabelCount] = []         # dominant zero-shot label per document, all analysed documents
    pronoun_ratios: Dict[str, float] = {}             # mean per 1,000 words
    top_words: List[WordCount] = []
    top_bigrams: List[PhraseCount] = []
    top_trigrams: List[PhraseCount] = []
    top_repetitions: List[PhraseCount] = []
    oppositions: List[OppositionTotal] = []

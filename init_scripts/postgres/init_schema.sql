-- Database Initialization

CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
CREATE SCHEMA IF NOT EXISTS collection;

-- Collection runs : metadata for collection runs orchestrated by Prefect (Bronze layer)
CREATE TABLE IF NOT EXISTS collection.runs (
    run_id                UUID PRIMARY KEY,
    prefect_flow_run_id   UUID,
    mode                  TEXT    NOT NULL CHECK (mode IN ('single', 'archive')),
    source_name           TEXT    NOT NULL,
    params                JSONB   NOT NULL,
    params_version        INT     NOT NULL DEFAULT 1,
    dry_run               BOOLEAN NOT NULL DEFAULT FALSE,
    effective_config      JSONB,        -- snapshot of internal policy applied
    status                TEXT    NOT NULL CHECK (status IN ('queued', 'running', 'completed', 'completed_with_errors', 'failed', 'cancelled')),
    listing_pages_visited INT     NOT NULL DEFAULT 0,
    error_message         TEXT,
    requested_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    started_at            TIMESTAMPTZ,
    finished_at           TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_runs_status_requested ON collection.runs (status, requested_at DESC);

-- Bronze Layer : metadata for raw files
CREATE TABLE IF NOT EXISTS bronze.documents (
    doc_id          SERIAL      PRIMARY KEY,   -- the PK is serial for easy insertion and auto-incrementing IDs
    first_run_id    UUID        REFERENCES collection.runs (run_id)     ON DELETE SET NULL,
    source_url      TEXT,
    source_type     TEXT        NOT NULL CHECK (source_type IN ('video', 'audio', 'text')),
    file_name       TEXT,
    file_size       BIGINT,
    mime_type       TEXT,
    excluded        BOOLEAN     NOT NULL DEFAULT FALSE,
    exclusion_reason TEXT,
    checksum        TEXT        NOT NULL,
    storage_path    TEXT        NOT NULL,
    raw_metadata    JSONB,      -- for YouTube video metadata,...
    ingestion_date  TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT unique_checksum UNIQUE (checksum)
);

CREATE INDEX IF NOT EXISTS idx_bronze_excluded ON bronze.documents(excluded);
CREATE INDEX IF NOT EXISTS idx_bronze_source_type ON bronze.documents(source_type);
CREATE INDEX IF NOT EXISTS idx_bronze_source_url ON bronze.documents (source_url);
CREATE INDEX IF NOT EXISTS idx_bronze_ingestion_date ON bronze.documents(ingestion_date);

-- Discovery log : metadata for discovered documents (Bronze layer)
CREATE TABLE IF NOT EXISTS bronze.discovery_log (
    id               BIGSERIAL PRIMARY KEY,
    run_id           UUID NOT NULL REFERENCES collection.runs (run_id) ON DELETE CASCADE,
    source_name      TEXT NOT NULL,
    source_url       TEXT NOT NULL,
    canonical_url    TEXT NOT NULL,
    listing_url      TEXT,
    title_hint       TEXT,
    status           TEXT NOT NULL CHECK (status IN ('prefilter_rejected', 'known_url_skipped', 'fetch_failed',
    'content_rejected', 'duplicate_content', 'ingested', 'ingest_failed', 'prefilter_passed')),
    reason_code      TEXT CHECK (reason_code IN ('not_a_document_link', 'url_pattern_excluded', 'out_of_date_range', 'robots_disallowed',
    'http_error', 'timeout', 'content_too_large', 'empty_text', 'no_keyword_match', 'extraction_failed', 'checksum_exists',
    'upload_failed', 'db_error', 'budget_exhausted', 'doc_type_excluded', 'access_restricted', 'format_unavailable')),
    detail           TEXT,
    matched_keywords JSONB,
    match_scope      TEXT CHECK (match_scope IN
        ('content', 'title', 'url', 'description', 'about', 'caption')),
    keywords_hash    TEXT,
    doc_id           INT REFERENCES bronze.documents (doc_id) ON DELETE SET NULL,
    decided_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_dlog_run_status ON bronze.discovery_log (run_id, status);
CREATE INDEX IF NOT EXISTS idx_dlog_url_kw     ON bronze.discovery_log (canonical_url, keywords_hash);

-- Silver Layer : processed data with extracted metadata
CREATE TABLE IF NOT EXISTS silver.text (
    doc_id              INT PRIMARY KEY REFERENCES bronze.documents(doc_id) ON DELETE CASCADE,
    title               TEXT,               -- optional
    speaker             VARCHAR(255),       -- optional
    publication_date    DATE,               -- optional
    language            VARCHAR(20),        -- optional
    transcript          TEXT,
    word_count          INT,
    status_processing   TEXT        NOT NULL DEFAULT 'pending' CHECK (status_processing IN ('pending', 'success', 'failed')),
    processing_date    TIMESTAMPTZ NOT NULL DEFAULT now(),
    error_message         TEXT
);

CREATE INDEX IF NOT EXISTS idx_silver_status_processing ON silver.text(status_processing);
CREATE INDEX IF NOT EXISTS idx_silver_publication_date ON silver.text(publication_date);
CREATE INDEX IF NOT EXISTS idx_silver_language ON silver.text(language);

-- Gold Layer : final analytics-ready data
CREATE TABLE IF NOT EXISTS gold.analytics (
    doc_id          INT PRIMARY KEY REFERENCES silver.text(doc_id) ON DELETE CASCADE,
    topics          JSONB,      -- label, score, ... (e.g., {"politics": 0.8, "sports": 0.1, "entertainment": 0.05})
    labels          JSONB,      -- zero-shot NLI-based classification against a predefined taxonomy ("topics" is unsupervised/emergent topic modeling; "labels" is classification against fixed categories, not conventional supervised learning)
    sentiment       JSONB,      -- label, score, ...
    emotions        JSONB,      -- {"joy": 0.6, "anger": 0.05, ...}
    entities        JSONB,      -- persons, organizations, locations, dates, etc.
    lex_metrics         JSONB,  -- pronoun ratios, top content words, repetitions, oppositions, n-grams, lexical diversity
    syntactic_metrics   JSONB,  -- speech stats, modality markers, POS distribution, verb tense
    keywords        JSONB,      -- c-TF-IDF keywords per topic
    analysis_date   TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Indexes for Gold Layer (fetch inside JSONB fields)
CREATE INDEX IF NOT EXISTS idx_gold_topics ON gold.analytics USING GIN (topics);
CREATE INDEX IF NOT EXISTS idx_gold_labels ON gold.analytics USING GIN (labels);
CREATE INDEX IF NOT EXISTS idx_gold_entities ON gold.analytics USING GIN (entities);
CREATE INDEX IF NOT EXISTS idx_gold_analysis_date ON gold.analytics(analysis_date);

-- Create a view to join all layers for easy access
CREATE OR REPLACE VIEW gold.v_documents_full AS
SELECT
    b.doc_id,
    b.source_url,
    b.source_type,
    b.ingestion_date,
    s.title,
    s.publication_date,
    s.word_count,
    s.status_processing,
    g.topics,
    g.labels,
    g.sentiment,
    g.emotions,
    g.entities,
    g.lex_metrics,
    g.syntactic_metrics,
    g.keywords,
    g.analysis_date
FROM bronze.documents b
LEFT JOIN silver.text s ON s.doc_id = b.doc_id
LEFT JOIN gold.analytics g ON g.doc_id = b.doc_id;
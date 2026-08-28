-- Database Initialization

CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

-- Bronze Layer : metadata for raw files
CREATE TABLE IF NOT EXISTS bronze.documents (
    doc_id          SERIAL      PRIMARY KEY,   -- the PK is serial for easy insertion and auto-incrementing IDs
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
CREATE INDEX IF NOT EXISTS idx_bronze_ingestion_date ON bronze.documents(ingestion_date);

-- Silver Layer : processed data with extracted metadata
CREATE TABLE IF NOT EXISTS silver.text (
    doc_id              INT PRIMARY KEY REFERENCES bronze.documents(doc_id) ON DELETE CASCADE,
    title               TEXT,               -- optional
    publication_date    DATE,               -- optional
    transcript          TEXT,
    word_count          INT,
    status_processing   TEXT        NOT NULL DEFAULT 'pending' CHECK (status_processing IN ('pending', 'success', 'failed')),
    processing_date    TIMESTAMPTZ NOT NULL DEFAULT now(),
    error_message         TEXT
);

CREATE INDEX IF NOT EXISTS idx_silver_status_processing ON silver.text(status_processing);
CREATE INDEX IF NOT EXISTS idx_silver_publication_date ON silver.text(publication_date);

-- Gold Layer : final analytics-ready data
CREATE TABLE IF NOT EXISTS gold.analytics (
    doc_id          INT PRIMARY KEY REFERENCES silver.text(doc_id) ON DELETE CASCADE,
    topics          JSONB,      -- label, score, ... (e.g., {"politics": 0.8, "sports": 0.1, "entertainment": 0.05})
    labels          JSONB,      -- zero-shot multi-label output ("topics" attribute is for unsupervised topic modeling, "labels" is for supervised classification)
    sentiment       JSONB,      -- label, score, ...
    emotions        JSONB,      -- {"joy": 0.6, "anger": 0.05, ...}
    entities        JSONB,      -- persons, organizations, locations, dates, etc.
    lex_metrics     JSONB,      -- pronoun ratios, buzzwords, repetitions, oppositions
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
    g.keywords,
    g.analysis_date
FROM bronze.documents b
LEFT JOIN silver.text s ON s.doc_id = b.doc_id
LEFT JOIN gold.analytics g ON g.doc_id = b.doc_id;
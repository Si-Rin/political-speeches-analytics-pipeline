-- Database Initialization

CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

-- Bronze Layer : metadata for raw files
CREATE TABLE IF NOT EXISTS bronze.documents (
    doc_id          SERIAL PRIMARY KEY,   -- was INT PRIMARY KEY: app relies on
                                           -- the DB auto-generating this via
                                           -- RETURNING doc_id, so it must be
                                           -- SERIAL (or GENERATED ALWAYS AS IDENTITY)
    source_url      TEXT,
    source_type     TEXT        NOT NULL CHECK (source_type IN ('video', 'audio', 'text')),
    file_name       TEXT,
    file_size       BIGINT,
    mime_type       TEXT,
    checksum        TEXT        NOT NULL,
    storage_path    TEXT        NOT NULL,
    ingestion_time  TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT unique_checksum UNIQUE (checksum)
);

CREATE INDEX IF NOT EXISTS idx_bronze_source_type ON bronze.documents(source_type);
CREATE INDEX IF NOT EXISTS idx_bronze_ingestion_time ON bronze.documents(ingestion_time);

-- Silver Layer : processed data with extracted metadata
CREATE TABLE IF NOT EXISTS silver.text (
    doc_id              INT PRIMARY KEY REFERENCES bronze.documents(doc_id) ON DELETE CASCADE,
    title               TEXT,               -- optional
    speaker             TEXT,               -- optional
    publication_date    DATE,               -- optional
    language            TEXT,
    transcript          TEXT,
    word_count          INT,
    status_processing   TEXT        NOT NULL DEFAULT 'pending' CHECK (status_processing IN ('pending', 'success', 'failed')),
    processing_time     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_silver_status_processing ON silver.text(status_processing);
CREATE INDEX IF NOT EXISTS idx_silver_publication_date ON silver.text(publication_date);
CREATE INDEX IF NOT EXISTS idx_silver_language ON silver.text(language);

-- Gold Layer : final analytics-ready data
CREATE TABLE IF NOT EXISTS gold.analytics (
    doc_id          INT PRIMARY KEY REFERENCES silver.text(doc_id) ON DELETE CASCADE,
    topics          JSONB,      -- label, score, ...
    labels          JSONB,      -- zero-shot multi-label output
    sentiment       JSONB,      -- label, score, ...
    emotions        JSONB,      -- {"joy": 0.6, "anger": 0.05, ...}
    entities        JSONB,      -- persons, organizations, locations, dates, etc.
    lex_metrics     JSONB,      -- pronoun ratios, buzzwords, repetitions, oppositions
    keywords        JSONB,      -- c-TF-IDF keywords per topic
    analysis_time   TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Indexes for Gold Layer (fetch inside JSONB fields)
CREATE INDEX IF NOT EXISTS idx_gold_topics ON gold.analytics USING GIN (topics);
CREATE INDEX IF NOT EXISTS idx_gold_labels ON gold.analytics USING GIN (labels);
CREATE INDEX IF NOT EXISTS idx_gold_entities ON gold.analytics USING GIN (entities);

CREATE INDEX IF NOT EXISTS idx_gold_analysis_time ON gold.analytics(analysis_time);

-- Create a view to join all layers for easy access
CREATE OR REPLACE VIEW gold.v_documents_full AS
SELECT
    b.doc_id,
    b.source_url,
    b.source_type,
    b.ingestion_time,
    s.title,
    s.speaker,
    s.language,
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
    g.analysis_time
FROM bronze.documents b
LEFT JOIN silver.text s ON s.doc_id = b.doc_id
LEFT JOIN gold.analytics g ON g.doc_id = b.doc_id;
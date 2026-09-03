-- 1. Bronze: rename ingestion_time -> ingestion_date, fix the index to match
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'bronze' AND table_name = 'documents' AND column_name = 'ingestion_time'
    ) THEN
        ALTER TABLE bronze.documents RENAME COLUMN ingestion_time TO ingestion_date;
    END IF;
END $$;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'silver' AND table_name = 'text' AND column_name = 'processing_time'
    ) THEN
        ALTER TABLE silver.text RENAME COLUMN processing_time TO processing_date;
    END IF;
END $$;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'gold' AND table_name = 'analytics' AND column_name = 'analysis_time'
    ) THEN
        ALTER TABLE gold.analytics RENAME COLUMN analysis_time TO analysis_date;
    END IF;
END $$;

DROP INDEX IF EXISTS bronze.idx_bronze_ingestion_time;
CREATE INDEX IF NOT EXISTS idx_bronze_ingestion_date ON bronze.documents(ingestion_date);

-- 2. Silver: add error_message if not already present
ALTER TABLE silver.text ADD COLUMN IF NOT EXISTS error_message TEXT;

-- 3. Gold view: drop the two nonexistent columns
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
# Political Speeches Analytics Pipeline

An automated multimodal data engineering and NLP pipeline for collecting, processing, structuring, and analyzing political speeches and related textual and audiovisual data.

The project is designed around a **Medallion Architecture** and uses **Prefect** to orchestrate workflows, **MinIO** for raw-object storage, **PostgreSQL** for structured data, **FastAPI** as the backend API, and **Streamlit** as the user-facing collection and monitoring interface.

## Project Overview

Political speech data is distributed across websites, video platforms, online archives, and other sources. Collecting and preparing this data manually is time-consuming and makes it difficult to maintain a consistent, reusable corpus for NLP analysis.

This project automates the main stages of the data lifecycle:

1. **Data acquisition** from multiple heterogeneous sources.
2. **Source classification and routing** to the appropriate ingestion adapter.
3. **Raw-data storage and deduplication** in the Bronze layer.
4. **Text extraction and transcription** in the Silver layer.
5. **Linguistic and NLP analytics** in the Gold layer.
6. Preparation of structured analytical results for downstream exploration and visualization.

The current use case focuses on building a corpus of **Donald Trump political speeches** and extracting corpus-level and document-level linguistic and semantic information from it.

## Architecture

```text
                          ┌─────────────────────────┐
                          │      User Interface      │
                          │       Streamlit          │
                          │ Collection / History /   │
                          │    Pipeline Status       │
                          └────────────┬────────────┘
                                       │
                                       ▼
                          ┌─────────────────────────┐
                          │       FastAPI API       │
                          │  validation / routing   │
                          │  ingestion submission   │
                          └────────────┬────────────┘
                                       │
                                       ▼
                          ┌─────────────────────────┐
                          │  Source Classification  │
                          │       & Routing         │
                          └────────────┬────────────┘
                                       │
                          ┌────────────┴────────────┐
                          ▼                         ▼
               ┌──────────────────┐       ┌──────────────────┐
               │  Source Adapters │       │  Generic Web     │
               │ Local / YouTube  │       │ Crawling / URLs  │
               │ Miller / UCSB /  │       │                  │
               │ Internet Archive │       │                  │
               └────────┬─────────┘       └────────┬─────────┘
                        └──────────────┬────────────┘
                                       ▼
                          ┌─────────────────────────┐
                          │   Bronze Ingestion      │
                          │        Prefect          │
                          │  SHA-256 / validation   │
                          └────────────┬────────────┘
                                       │
                       ┌───────────────┴───────────────┐
                       ▼                               ▼
              ┌─────────────────┐             ┌─────────────────┐
              │      MinIO      │             │   PostgreSQL    │
              │  Raw objects    │             │ Bronze metadata │
              └────────┬────────┘             └────────┬────────┘
                       └───────────────┬────────────────┘
                                       ▼
                          ┌─────────────────────────┐
                          │   Silver Processing     │
                          │        Prefect          │
                          │ extraction / Whisper    │
                          └────────────┬────────────┘
                                       ▼
                          ┌─────────────────────────┐
                          │      silver.text        │
                          │ normalized text +       │
                          │ document metadata       │
                          └────────────┬────────────┘
                                       │
                                       ▼
                    ┌─────────────────────────────────────┐
                    │             Gold Analytics           │
                    │                                     │
                    │ Stage A: spaCy linguistic analysis │
                    │  lexical / syntactic / NER         │
                    │                                     │
                    │ Stage B: sentiment / emotion /     │
                    │         zero-shot labels           │
                    │                                     │
                    │ Stage C: embeddings + BERTopic      │
                    │         corpus-level topics         │
                    └──────────────────┬──────────────────┘
                                       ▼
                          ┌─────────────────────────┐
                          │   Analytics / Future    │
                          │ Exploration & Dashboard │
                          └─────────────────────────┘
```

## Medallion Layers

### Bronze

The Bronze layer handles source acquisition and preserves raw data together with ingestion metadata.

Documents are streamed to temporary files while a **SHA-256 checksum** is computed. The checksum is used to detect duplicates before storage.

Raw objects are stored in MinIO in the `bronze` bucket. Corresponding metadata is registered in PostgreSQL in `bronze.documents`.

Typical metadata includes:

- source URL
- source type
- file name
- file size
- MIME type
- SHA-256 checksum
- source-specific metadata
- ingestion date
- MinIO storage path

The ingestion layer supports both direct resources and source-specific discovery/crawling adapters.

### Silver

The Silver layer transforms ingested resources into normalized, analysis-ready textual records.

Depending on the source, the pipeline can:

- extract meaningful text from HTML/web pages;
- extract text from documents where supported;
- download and process audio/video content;
- transcribe speech using **Whisper**;
- detect language and preserve document metadata;
- compute document-level text statistics such as speech length and word count.

The resulting records are stored in PostgreSQL, including the extracted/transcribed text and the metadata required by downstream analytics.

### Gold

The Gold layer contains derived analytical features and NLP results used for political-speech analysis.

The analytics pipeline is organized into three stages.

#### Stage A — Linguistic Analysis

A shared **spaCy** pipeline performs a single NLP pass over each transcript. The resulting `Doc` object is reused by several analytical modules instead of parsing the same text repeatedly.

Stage A currently covers:

- lexical metrics;
- syntactic metrics;
- named entity recognition (NER).

The default spaCy model is `en_core_web_sm`, selected for its CPU efficiency and suitability for the current corpus-processing workflow.

#### Stage B — Sentiment, Emotion and Labels

Stage B uses dedicated NLP models independently of the spaCy linguistic pass.

It currently includes:

- sentiment analysis;
- emotion analysis;
- zero-shot multi-label classification using a configurable political-topic label taxonomy.

These results are persisted as structured JSON analytics associated with each document.

#### Stage C — Corpus-Level Topic Modeling

Stage C performs **corpus-level topic extraction** rather than assigning topics independently to each document.

The current implementation combines:

- `sentence-transformers/all-mpnet-base-v2` for semantic embeddings;
- text chunking for long documents;
- length-weighted averaging of chunk embeddings to obtain a document representation;
- **BERTopic** for corpus-wide topic discovery and topic probabilities;
- c-TF-IDF-based topic keywords for topic interpretation.

The topic flow requires at least **20 eligible documents** before fitting a fresh corpus-level BERTopic model. This reflects the fact that topic discovery is meaningful only when enough documents are available to form a corpus.

## Supported Data Sources

The ingestion layer currently exposes adapters for:

- Local folders
- Single items / direct URLs
- YouTube
- Miller Center
- UCSB tweet/document archives
- Internet Archive
- Generic web scraping and crawling

The source adapters are implemented under `prefect_flows/sources/` and provide a common discovery interface before resources are staged and ingested.

### Generic Web Crawling

The web-crawling mode supports controlled discovery from seed URLs using parameters such as:

- seed URLs;
- keywords;
- allowed domains;
- crawl depth;
- page limits;
- pagination handling;
- duplicate protection through a visited-URL queue.

These controls are intended to keep automated collection focused and bounded rather than allowing unrestricted crawling.

### YouTube

YouTube ingestion supports both video downloads and an **audio-only mode**. The original YouTube source URL is retained as the document source URL rather than exposing the temporary local download path.

## User Interface

A Streamlit interface provides a non-technical entry point to the ingestion platform.

The collection interface allows users to provide a source and relevant metadata without interacting directly with Prefect, PostgreSQL, MinIO, or individual ingestion scripts.

Current interface pages include:

- **Collection** — submit URLs or local resources and configure collection options.
- **History** — inspect previously collected documents.
- **Pipeline Status** — monitor ingestion and processing activity.

For the current Donald Trump use case, the speaker is automatically associated with the collected corpus rather than requiring the user to enter it manually.

## Project Structure

```text
.
├── docker-compose.yml
├── requirements.txt
├── init_scripts/
│   └── postgres/
│       ├── 01-create-prefect-db.sql
│       └── init_schema.sql
├── backend/
│   ├── main.py
│   ├── pipeline_probe.py
│   ├── pipeline_runner.py
│   ├── schemas.py
│   └── routes/
│       ├── documents.py
│       └── status.py
├── frontend/
│   ├── app.py
│   ├── api_client.py
│   └── pages/
│       ├── collection.py
│       ├── history.py
│       └── pipeline_status.py
├── prefect_flows/
│   ├── analytics/
│   │   ├── embeddings.py
│   │   ├── emotion.py
│   │   ├── entities.py
│   │   ├── gold_db.py
│   │   ├── labels.py
│   │   ├── lexical.py
│   │   ├── nlp_pipeline.py
│   │   ├── sentiment.py
│   │   ├── syntactic.py
│   │   ├── text_chunking.py
│   │   └── topics.py
│   ├── extractors/
│   │   ├── audio_video_extractor.py
│   │   ├── base.py
│   │   ├── factory.py
│   │   ├── generic_html.py
│   │   ├── internet_archive.py
│   │   ├── miller_center.py
│   │   ├── ucsb_extractor.py
│   │   ├── utils.py
│   │   └── whisper_transcriber.py
│   ├── sources/
│   │   ├── base.py
│   │   ├── internet_archive.py
│   │   ├── local_folder.py
│   │   ├── miller_center.py
│   │   ├── single_item.py
│   │   ├── ucsb_tweets.py
│   │   ├── web_scraping.py
│   │   └── youtube.py
│   ├── bronze_ingest.py
│   ├── clients.py
│   ├── gold_labels.py
│   ├── gold_linguistic.py
│   ├── gold_sentiment.py
│   ├── gold_topics.py
│   ├── health_check.py
│   ├── silver_ingest.py
│   └── Dockerfile
├── ia_excluded_indices_example.json
├── ia_urls_example.txt
├── yt_urls_example.txt
├── export_data.py
├── recover_missing_metadata.py
└── url_transform.py
```

## Technology Stack

| Component | Technology |
|---|---|
| Programming language | Python |
| Workflow orchestration | Prefect 2 |
| Object storage | MinIO |
| Relational database | PostgreSQL 16 |
| Backend API | FastAPI |
| User interface | Streamlit |
| Video/audio downloading | yt-dlp |
| Audio/video processing | FFmpeg / pydub |
| Text extraction | Trafilatura / BeautifulSoup |
| Speech-to-text | Whisper |
| NLP pipeline | spaCy |
| Embeddings | Sentence Transformers (`all-mpnet-base-v2`) |
| Topic modeling | BERTopic |
| HTTP requests | Requests |
| Configuration | python-dotenv |
| Database driver | psycopg2 |

## Requirements

- Python 3.x
- Docker and Docker Compose
- Git
- FFmpeg available to the environment where media processing/transcription is performed

Python dependencies are listed in [`requirements.txt`](requirements.txt).

## Configuration

Create a `.env` file in the project root. Credentials are kept outside the source code and loaded through environment variables.

Example:

```env
MINIO_ROOT_USER=your_minio_user
MINIO_ROOT_PASSWORD=your_minio_password

POSTGRES_USER=your_postgres_user
POSTGRES_PASSWORD=your_postgres_password
POSTGRES_DB=political_speeches
POSTGRES_HOST=localhost
POSTGRES_PORT=5433

MINIO_ENDPOINT=localhost:9000
```

Do not commit real credentials or other secrets to the repository.

## Installation

### 1. Clone the repository

```bash
git clone https://github.com/Si-Rin/political-speeches-analytics-pipeline.git
cd political-speeches-analytics-pipeline
```

### 2. Create and activate a virtual environment

Windows:

```bash
python -m venv .venv
.venv\Scripts\activate
```

Linux/macOS:

```bash
python3 -m venv .venv
source .venv/bin/activate
```

### 3. Install Python dependencies

```bash
pip install -r requirements.txt
```

### 4. Start the infrastructure

```bash
docker compose up -d
```

The Docker Compose stack includes:

- **MinIO** on host port `9000`, with its web console on `9001`;
- **PostgreSQL 16** on host port `5433`;
- **Prefect Server** on `http://localhost:4200`;
- a **Prefect deployment/worker** used to execute pipeline flows;
- the **FastAPI backend**;
- the **Streamlit frontend**.

The PostgreSQL initialization scripts under `init_scripts/postgres/` are executed when the database volume is initialized.

## Running the Pipeline

### Health check

```bash
python prefect_flows/health_check.py
```

### Bronze ingestion

Local folder:

```bash
python prefect_flows/bronze_ingest.py \
  --source local \
  --folder /path/to/folder
```

YouTube URLs:

```bash
python prefect_flows/bronze_ingest.py \
  --source youtube \
  --urls "https://www.youtube.com/watch?v=VIDEO_ID"
```

YouTube audio-only mode:

```bash
python prefect_flows/bronze_ingest.py \
  --source youtube \
  --urls "https://www.youtube.com/watch?v=VIDEO_ID" \
  --audio-only
```

Miller Center crawl:

```bash
python prefect_flows/bronze_ingest.py \
  --source miller_center \
  --start-url "https://millercenter.org/president/kennedy/speeches"
```

Generic web crawl:

```bash
python prefect_flows/bronze_ingest.py \
  --source web_crawl \
  --seed-urls "https://example.com" \
  --keywords "politics" "speech" \
  --allowed-domains "example.com"
```

Internet Archive:

```bash
python prefect_flows/bronze_ingest.py \
  --source internet_archive \
  --urls "https://archive.org/..."
```

The ingestion flow stages each candidate, computes its checksum, skips duplicates, uploads non-duplicates to MinIO, and registers corresponding metadata in PostgreSQL.

## Gold Analytics Execution

Gold processing is split into independent Prefect flows so that each analytical stage can be rerun without repeating ingestion.

### Stage A — Linguistic analysis

```bash
python prefect_flows/gold_linguistic.py
```

This stage reuses one spaCy `Doc` per transcript for lexical, syntactic, and NER analysis.

### Stage B — Sentiment and emotion

```bash
python prefect_flows/gold_sentiment.py
```

### Stage B — Zero-shot labels

```bash
python prefect_flows/gold_labels.py
```

Optional document selection and limits are supported by the flow for controlled processing.

### Stage C — Corpus-level topics

```bash
python prefect_flows/gold_topics.py
```

Topic modeling operates on the eligible corpus as a whole and requires at least 20 documents before fitting BERTopic.

## Gold Analytics Schema

Gold analytics are stored in structured form and include fields such as:

- `doc_id`
- `topics`
- `labels`
- `sentiment`
- `emotions`
- `entities`
- `lex_metrics`
- `syntactic_metrics`
- `keywords`
- `analysis_date`

The JSON-based fields allow heterogeneous NLP outputs to be stored while retaining a stable document-level analytical record.

## Prefect Workflow Architecture

Prefect is used as the orchestration layer rather than embedding pipeline execution directly into the user interface.

The main separation is:

```text
Streamlit
    │
    ▼
FastAPI
    │
    ▼
Prefect Server API
    │
    ▼
Deployment / Work Pool
    │
    ▼
Prefect Worker
    │
    ├── Bronze ingestion
    ├── Silver processing
    └── Gold analytics
```

This architecture keeps the UI responsive and allows long-running ingestion, transcription, and NLP workloads to execute asynchronously through Prefect.

## Data Quality and Reliability

The pipeline includes several safeguards:

- **SHA-256 deduplication** to avoid storing the same file multiple times;
- **streaming downloads** to avoid loading complete remote files into memory;
- **temporary-file cleanup** after successful or failed processing;
- **Prefect retries** for selected transient operations;
- explicit handling of missing MinIO objects in downstream processing;
- UTF-8 database connections for text containing non-ASCII characters;
- bounded crawling through depth, page, domain, and duplicate controls;
- source-specific routing to avoid applying the wrong extractor to a resource.

## Authentication and Secrets

The application does not currently implement user-facing authentication such as OAuth, JWT, sessions, or bearer tokens.

Instead, it uses service-level credentials:

- **MinIO:** access key / secret key through `MINIO_ROOT_USER` and `MINIO_ROOT_PASSWORD`;
- **PostgreSQL:** username / password through the `POSTGRES_*` environment variables;
- **Prefect:** the local deployment is configured without an application-level authentication layer.

For local development, credentials are supplied through `.env` and injected into Docker Compose. Production deployments should use TLS, least-privilege service accounts, and a proper secret-management solution.

> **Security note:** the current Docker Compose configuration enables anonymous download access to the MinIO `bronze` bucket. This is suitable only when the stored corpus is intentionally public. It should be disabled for private data.

## Example Input Files

The repository includes example files for testing source-specific ingestion:

- `yt_urls_example.txt`
- `ia_urls_example.txt`
- `ia_excluded_indices_example.json`

These examples can be adapted to build a larger political-speech corpus.

## Current Status

### Implemented

- Multi-source Bronze ingestion
- Source classification and routing
- Generic web crawling with bounded discovery controls
- YouTube audio-only ingestion
- MinIO raw-object storage
- PostgreSQL metadata management
- SHA-256 deduplication
- Silver text extraction and audio/video transcription
- Prefect Server, deployment, work pool, and worker setup
- FastAPI backend
- Streamlit collection, history, and pipeline-status interface
- Gold Stage A linguistic metrics and NER
- Gold Stage B sentiment and emotion analysis
- Gold zero-shot multi-label classification
- Gold Stage C sentence embeddings and corpus-level BERTopic topic modeling

### Evolving

- Expansion and refinement of NLP features
- Corpus-level topic exploration and interpretation
- Downstream analytics and visualization
- Further production hardening and deployment configuration

## Repository

GitHub: https://github.com/Si-Rin/political-speeches-analytics-pipeline

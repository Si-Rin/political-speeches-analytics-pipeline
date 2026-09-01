# Political Speeches Analytics Pipeline

An automated data engineering and NLP pipeline for collecting, processing, and analyzing political speeches and related textual data.

The project is designed around a **Medallion Architecture** and uses **Prefect** to orchestrate ingestion and processing workflows, **MinIO** for raw-object storage, and **PostgreSQL** for structured metadata, extracted text, and analytics.

## Project Overview

Political speech data is distributed across websites, video platforms, online archives, and other sources. Collecting this data manually is time-consuming and makes it difficult to maintain a consistent, reusable corpus for analysis.

This project automates the main stages of the data lifecycle:

1. **Data acquisition** from multiple sources.
2. **Raw-data storage and deduplication** in the Bronze layer.
3. **Text extraction and transcription** in the Silver layer.
4. **NLP and statistical analysis** in the Gold layer.
5. Preparation of structured data for downstream exploration and visualization.

## Architecture

```text
                          ┌──────────────────────┐
                          │      Data Sources     │
                          │──────────────────────│
                          │ Local files           │
                          │ URLs / web pages      │
                          │ YouTube               │
                          │ Miller Center         │
                          │ UCSB tweet archives   │
                          │ Internet Archive      │
                          └──────────┬───────────┘
                                     │
                                     ▼
                          ┌──────────────────────┐
                          │   Bronze Ingestion   │
                          │      (Prefect)       │
                          └──────────┬───────────┘
                                     │
                         SHA-256 + deduplication
                                     │
                                     ▼
             ┌───────────────────────┴──────────────────────┐
             │                                              │
             ▼                                              ▼
     ┌─────────────────┐                            ┌─────────────────┐
     │      MinIO      │                            │   PostgreSQL    │
     │ Raw files       │                            │ Bronze metadata │
     └────────┬────────┘                            └────────┬────────┘
              │                                              │
              └──────────────────┬───────────────────────────┘
                                 ▼
                       ┌─────────────────────┐
                       │  Silver Processing  │
                       │      (Prefect)      │
                       └──────────┬──────────┘
                                  │
                       extraction / transcription
                                  │
                                  ▼
                       ┌─────────────────────┐
                       │      PostgreSQL     │
                       │  Silver structured  │
                       │        data         │
                       └──────────┬──────────┘
                                  │
                                  ▼
                       ┌─────────────────────┐
                       │   Gold Analytics    │
                       │ lexical / syntactic│
                       │ NER / NLP / models  │
                       └──────────┬──────────┘
                                  │
                                  ▼
                       ┌─────────────────────┐
                       │ Analytics / Dashboard│
                       │      (planned)      │
                       └─────────────────────┘
```

## Medallion Layers

### Bronze

The Bronze layer contains the ingested source files and their ingestion metadata.

Each document is streamed to a temporary file while a **SHA-256 checksum** is computed. The checksum is used to detect duplicates before the file is stored.

Raw objects are stored in the MinIO `bronze` bucket using an object key based on the checksum and original file extension. Corresponding metadata is registered in PostgreSQL in `bronze.documents`.

Typical metadata includes:

- source URL
- source type
- file name
- file size
- MIME type
- SHA-256 checksum
- source-specific metadata
- MinIO storage path

### Silver

The Silver layer transforms ingested files into normalized, analysis-ready textual records.

Depending on the source, the pipeline can extract text from web content or transcribe audio/video material. Metadata from Bronze is combined with the extracted content and additional document-level information.

### Gold

The Gold layer contains derived analytical features and NLP results used for political-speech analysis.

The current analytics work includes **lexical, syntactic, and named-entity-based features**, with additional NLP models being integrated as the analytics stage evolves.

## Supported Data Sources

The Bronze ingestion flow currently exposes adapters for:

- Local folders
- Direct URLs
- YouTube
- Miller Center
- UCSB tweet/document archives
- Internet Archive
- Generic web crawling

The source adapters are implemented under `prefect_flows/sources/` and provide a common discovery interface before files are staged and ingested.

## Project Structure

```text
.
├── docker-compose.yml
├── requirements.txt
├── init_scripts/
│   └── postgres/
│       └── init_schema.sql
├── prefect_flows/
│   ├── analytics/
│   │   └── lexical_metrics.py
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
│   │   ├── ucsb_tweets.py
│   │   ├── url_s.py
│   │   ├── web_scraping.py
│   │   └── youtube.py
│   ├── bronze_ingest.py
│   ├── clients.py
│   ├── gold_ingest.py
│   ├── health_check.py
│   └── silver_ingest.py
├── ia_excluded_indices_example.json
├── ia_urls_example.txt
└── yt_urls_example.txt
```

## Technology Stack

| Component | Technology |
|---|---|
| Programming language | Python |
| Workflow orchestration | Prefect 2 |
| Object storage | MinIO |
| Relational database | PostgreSQL 16 |
| Video/audio downloading | yt-dlp |
| Audio/video processing | FFmpeg / pydub |
| Text extraction | Trafilatura / BeautifulSoup |
| Speech-to-text | Whisper |
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

Create a `.env` file in the project root. Credentials are intentionally kept outside the source code and loaded through environment variables.

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

This starts:

- **MinIO** on `http://localhost:9000` with its web console on `http://localhost:9001`
- **PostgreSQL** on host port `5433`
- **Prefect server** on `http://localhost:4200`
- **Prefect worker** connected to the local Prefect server

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

The ingestion flow stages each candidate, computes its checksum, skips duplicates, uploads non-duplicates to MinIO, and registers the corresponding metadata in PostgreSQL.

## Workflow

The processing lifecycle is organized as separate Prefect flows:

```text
bronze_ingest
      │
      ▼
  bronze.documents + MinIO
      │
      ▼
silver_ingest
      │
      ▼
   silver.text
      │
      ▼
  gold_ingest / analytics
      │
      ▼
     Gold
```

This separation keeps raw ingestion, text preparation, and analytics logically independent and makes individual stages easier to rerun and troubleshoot.

## Data Quality and Reliability

The ingestion pipeline includes several safeguards:

- **SHA-256 deduplication** to avoid storing the same file multiple times.
- **Streaming downloads** to avoid loading complete remote files into memory.
- **Temporary-file cleanup** after successful or failed processing.
- **Prefect task retries** for selected transient operations.
- Explicit handling of missing MinIO objects in downstream processing.
- UTF-8 database connections for text containing non-ASCII characters.

## Authentication and Secrets

The application does not currently implement user-facing authentication such as OAuth, JWT, sessions, or bearer tokens.

Instead, it uses service-level credentials:

- **MinIO:** access key / secret key through `MINIO_ROOT_USER` and `MINIO_ROOT_PASSWORD`.
- **PostgreSQL:** username / password through the `POSTGRES_*` environment variables.
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

Implemented or in active development:

- Multi-source Bronze ingestion
- MinIO raw-object storage
- PostgreSQL metadata management
- SHA-256 deduplication
- Silver text extraction/transcription pipeline
- Prefect orchestration
- Gold lexical/syntactic/NLP analytics

Planned or evolving components include additional NLP models, richer analytical features, and a user-facing analytics/dashboard layer.

## Repository

GitHub: https://github.com/Si-Rin/political-speeches-analytics-pipeline

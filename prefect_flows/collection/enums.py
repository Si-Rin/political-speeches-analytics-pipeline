"""
Explicit vocabulary shared by the flow, the extractors and the database.
(str, Enum) keeps values JSON/psycopg2-friendly.
"""
from enum import Enum


class Mode(str, Enum):
    SINGLE = "single"
    ARCHIVE = "archive"


class SourceName(str, Enum):
    LOCAL_FILE = "local_file"
    LOCAL_FOLDER = "local_folder"          # dev/CLI only, never exposed by the backend/UI
    DIRECT_URL = "direct_url"
    YOUTUBE = "youtube"
    YOUTUBE_COLLECTION = "youtube_collection"
    INTERNET_ARCHIVE = "internet_archive"
    INTERNET_ARCHIVE_COLLECTION = "internet_archive_collection"
    MILLER_CENTER = "miller_center"
    UCSB = "ucsb"
    WEB_CRAWL = "web_crawl"


class ContentType(str, Enum):
    TEXT = "text"
    AUDIO = "audio"
    VIDEO = "video"


class RunStatus(str, Enum):
    QUEUED = "queued"
    RUNNING = "running"
    COMPLETED = "completed"
    COMPLETED_WITH_ERRORS = "completed_with_errors"
    FAILED = "failed"
    CANCELLED = "cancelled"


class CandidateStatus(str, Enum):
    PREFILTER_REJECTED = "prefilter_rejected"
    KNOWN_URL_SKIPPED = "known_url_skipped"
    FETCH_FAILED = "fetch_failed"
    CONTENT_REJECTED = "content_rejected"
    DUPLICATE_CONTENT = "duplicate_content"
    INGESTED = "ingested"
    INGEST_FAILED = "ingest_failed"
    PREFILTER_PASSED = "prefilter_passed"   # for dry-run


class ReasonCode(str, Enum):
    NOT_A_DOCUMENT_LINK = "not_a_document_link"
    URL_PATTERN_EXCLUDED = "url_pattern_excluded"
    OUT_OF_DATE_RANGE = "out_of_date_range"
    ROBOTS_DISALLOWED = "robots_disallowed"
    HTTP_ERROR = "http_error"
    TIMEOUT = "timeout"
    CONTENT_TOO_LARGE = "content_too_large"
    EMPTY_TEXT = "empty_text"
    NO_KEYWORD_MATCH = "no_keyword_match"
    EXTRACTION_FAILED = "extraction_failed"
    CHECKSUM_EXISTS = "checksum_exists"
    UPLOAD_FAILED = "upload_failed"
    DB_ERROR = "db_error"
    BUDGET_EXHAUSTED = "budget_exhausted"
    DOC_TYPE_EXCLUDED = "doc_type_excluded"
    ACCESS_RESTRICTED = "access_restricted"
    FORMAT_UNAVAILABLE = "format_unavailable"


class MatchScope(str, Enum):
    CONTENT = "content"
    TITLE = "title"
    URL = "url"
    DESCRIPTION = "description"   # YouTube
    ABOUT = "about"               # Miller Center
    CAPTION = "caption"           # Internet Archive

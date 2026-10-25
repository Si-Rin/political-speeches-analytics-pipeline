"""
Vocabulary shared with the Prefect side. The backend never imports prefect_flows, so these enums are copies;
tests/test_backend_contract.py fails if a value drifts from prefect_flows/collection/enums.py.
"""
from enum import Enum

PARAMS_VERSION = 1


class Mode(str, Enum):
    SINGLE = "single"
    ARCHIVE = "archive"


class SourceName(str, Enum):
    """Sources the flow can run today. Phase 4 adds miller_center, ucsb, youtube_collection, internet_archive_collection."""
    LOCAL_FILE = "local_file"
    DIRECT_URL = "direct_url"
    YOUTUBE = "youtube"
    INTERNET_ARCHIVE = "internet_archive"
    MILLER_CENTER = "miller_center"
    UCSB = "ucsb"
    WEB_CRAWL = "web_crawl"


class ContentType(str, Enum):
    TEXT = "text"
    AUDIO = "audio"
    VIDEO = "video"


class DownloadFormat(str, Enum):
    VIDEO = "video"
    AUDIO = "audio"


class RunStatus(str, Enum):
    QUEUED = "queued"
    RUNNING = "running"
    COMPLETED = "completed"
    COMPLETED_WITH_ERRORS = "completed_with_errors"
    FAILED = "failed"
    CANCELLED = "cancelled"


TERMINAL_STATUSES = frozenset({RunStatus.COMPLETED, RunStatus.COMPLETED_WITH_ERRORS, RunStatus.FAILED, RunStatus.CANCELLED})


class CandidateStatus(str, Enum):
    PREFILTER_REJECTED = "prefilter_rejected"
    KNOWN_URL_SKIPPED = "known_url_skipped"
    FETCH_FAILED = "fetch_failed"
    CONTENT_REJECTED = "content_rejected"
    DUPLICATE_CONTENT = "duplicate_content"
    INGESTED = "ingested"
    INGEST_FAILED = "ingest_failed"
    PREFILTER_PASSED = "prefilter_passed"


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
    DESCRIPTION = "description"
    ABOUT = "about"
    CAPTION = "caption"

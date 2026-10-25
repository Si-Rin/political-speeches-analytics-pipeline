"""
Request models for POST /runs: one model per source, discriminated on `source`. FastAPI routes a payload to exactly
one model, and a validation error names the model that failed. Site constants (Trump filter, UCSB term listings,
IA exclusions) never appear here: they live in the Prefect-side adapters.
"""
from datetime import date
from typing import Annotated, List, Literal, Optional, Union

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from backend.schemas.common import ContentType, DownloadFormat, Mode, SourceName


def _http_url(value: str) -> str:
    value = (value or "").strip()
    if not value.lower().startswith(("http://", "https://")):
        raise ValueError("must be an http(s) URL")
    return value


class _Request(BaseModel):
    model_config = ConfigDict(extra="forbid")
    dry_run: bool = Field(default=False, description="Preview: listing and cheap filters only, nothing fetched or stored")


class _SingleRequest(_Request):
    """Optional metadata typed by the researcher; it overrides what the extractors detect."""
    title: Optional[str] = Field(default=None, max_length=500)
    publication_date: Optional[date] = None
    language: Optional[str] = Field(default=None, max_length=40)
    notes: Optional[str] = Field(default=None, max_length=2000)


class LocalFileRequest(_SingleRequest):
    source: Literal["local_file"]
    staging_key: str = Field(min_length=3, max_length=300, description="Key returned by POST /uploads")
    content_type: ContentType

    @field_validator("staging_key")
    @classmethod
    def _safe_key(cls, v: str) -> str:
        if v.startswith("/") or ".." in v.split("/"):
            raise ValueError("invalid staging key")
        return v


class DirectUrlRequest(_SingleRequest):
    source: Literal["direct_url"]
    url: str
    content_type: ContentType
    _check_url = field_validator("url")(_http_url)


class YoutubeRequest(_SingleRequest):
    source: Literal["youtube"]
    url: str
    download_format: DownloadFormat = DownloadFormat.VIDEO
    _check_url = field_validator("url")(_http_url)


class InternetArchiveRequest(_SingleRequest):
    source: Literal["internet_archive"]
    url: str = Field(description="An item URL (archive.org/details/<identifier>)")
    _check_url = field_validator("url")(_http_url)


class WebCrawlRequest(_Request):
    source: Literal["web_crawl"]
    seed_urls: List[str] = Field(min_length=1, max_length=20)
    keywords: List[str] = Field(min_length=1, max_length=30)
    allowed_domains: Optional[List[str]] = None
    max_depth: int = Field(default=2, ge=2, le=5)
    max_pages: int = Field(default=50, ge=1, le=500, description="Pages visited, not documents collected")

    @field_validator("seed_urls")
    @classmethod
    def _seed_urls(cls, v: List[str]) -> List[str]:
        return [_http_url(u) for u in v]

    @field_validator("keywords")
    @classmethod
    def _keywords(cls, v: List[str]) -> List[str]:
        cleaned = [k.strip() for k in v if k and k.strip()]
        if not cleaned:
            raise ValueError("at least one non-empty keyword is required")
        return cleaned


class MillerCenterRequest(_Request):
    source: Literal["miller_center"]
    urls: Optional[List[str]] = Field(default=None, max_length=50, description="Specific speech pages")
    start_url: Optional[str] = Field(default=None, description="A listing page to crawl from")
    max_depth: int = Field(default=100, ge=1, le=1000)

    @field_validator("urls")
    @classmethod
    def _urls(cls, v):
        return [_http_url(u) for u in v] if v else None

    @field_validator("start_url")
    @classmethod
    def _start(cls, v):
        return _http_url(v) if v else None

    @model_validator(mode="after")
    def _needs_a_start(self):
        if not self.urls and not self.start_url:
            raise ValueError("urls or start_url is required")
        return self


class UcsbRequest(_Request):
    source: Literal["ucsb"]
    listing_url: str
    link_text_filter: str = Field(default="tweets of", min_length=1, max_length=200)
    max_documents: int = Field(default=1, ge=1, le=500)
    _check_url = field_validator("listing_url")(_http_url)


RunRequest = Annotated[ 
    Union[LocalFileRequest, DirectUrlRequest, YoutubeRequest, InternetArchiveRequest, WebCrawlRequest, MillerCenterRequest, UcsbRequest],
    Field(discriminator="source"),
]

# Mode is a property of the source, not something the client chooses.
SOURCE_MODE = {
    SourceName.LOCAL_FILE: Mode.SINGLE,
    SourceName.DIRECT_URL: Mode.SINGLE,
    SourceName.YOUTUBE: Mode.SINGLE,
    SourceName.INTERNET_ARCHIVE: Mode.SINGLE,
    SourceName.WEB_CRAWL: Mode.ARCHIVE,
    SourceName.MILLER_CENTER: Mode.ARCHIVE,
    SourceName.UCSB: Mode.ARCHIVE,
}
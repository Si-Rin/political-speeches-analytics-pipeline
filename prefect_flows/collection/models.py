"""
Flow-side parameter models, one per source.
extra="forbid" turns a drifted payload into an error.

"""
from typing import Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field, model_validator

from prefect_flows.collection.enums import ContentType


class _Params(BaseModel):
    model_config = ConfigDict(extra="forbid")


class LocalFolderParams(_Params):
    folder: str


class LocalFileParams(_Params):
    staging_key: Optional[str] = None  # staging_key is sent by the backend (parked upload in the MinIO staging bucket)
    path: Optional[str] = None
    content_type: ContentType
    raw_metadata: Optional[dict] = None
    user_provided: Optional[dict] = None

    @model_validator(mode="after")
    def _exactly_one_location(self):
        if bool(self.staging_key) == bool(self.path):
            raise ValueError("provide exactly one of staging_key or path")
        return self


class DirectUrlParams(_Params):
    url: str
    content_type: ContentType
    raw_metadata: Optional[dict] = None
    user_provided: Optional[dict] = None


class YoutubeParams(_Params):
    urls: List[str] = Field(min_length=1)
    audio_only: bool = False
    playlist_mode: bool = False
    max_downloads: Optional[int] = Field(default=None, ge=1)
    user_provided: Optional[dict] = None


class YoutubeCollectionParams(_Params):
    playlist_url: str
    audio_only: bool = False
    max_videos: Optional[int] = Field(default=None, ge=1)


class InternetArchiveParams(_Params):
    urls: List[str] = Field(min_length=1)
    excluded_indices: Dict[str, List[int]] = Field(default_factory=dict)
    user_provided: Optional[dict] = None


class MillerCenterParams(_Params):
    urls: Optional[List[str]] = None
    start_url: Optional[str] = None
    max_depth: int = Field(default=100, ge=1)
    crawl_delay: float = Field(default=5.0, ge=0)
    request_timeout: float = Field(default=10.0, gt=0)

    @model_validator(mode="after")
    def _needs_a_start(self):
        if not self.urls and not self.start_url:
            raise ValueError("urls or start_url is required")
        return self


class UcsbParams(_Params):
    listing_url: str
    link_text_filter: str = "tweets of"
    max_documents: int = Field(default=1, ge=1)
    crawl_delay: float = Field(default=1.0, ge=0)


class WebCrawlParams(_Params):
    seed_urls: List[str] = Field(min_length=1)
    keywords: List[str] = Field(min_length=1)
    allowed_domains: Optional[List[str]] = None
    max_depth: int = Field(default=2, ge=2, le=5)
    max_pages: int = Field(default=50, ge=1, le=500)

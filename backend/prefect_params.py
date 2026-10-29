"""
Backend -> Prefect translation. A pure function: user-level request in, (source_name, mode, params) out, where
`params` is the plain JSON dict the flow validates with its own models. It only renames user-level fields to flow
parameter names (download_format -> audio_only, typed metadata -> user_provided). No I/O, no site knowledge.
"""
from dataclasses import dataclass
from typing import Any, Dict, Optional

from backend.schemas.common import DownloadFormat, Mode, SourceName
from backend.schemas.sources import (
    SOURCE_MODE, DirectUrlRequest, InternetArchiveRequest, LocalFileRequest, 
     MillerCenterRequest, UcsbRequest, WebCrawlRequest, YoutubeRequest, YoutubeCollectionRequest
)


@dataclass(frozen=True)
class PrefectPayload:
    source_name: SourceName
    mode: Mode
    params: Dict[str, Any]


def _user_provided(req) -> Optional[Dict[str, Any]]:
    values = {
        "title": req.title,
        "publication_date": req.publication_date.isoformat() if req.publication_date else None,
        "language": req.language,
        "notes": req.notes,
    }
    values = {k: v for k, v in values.items() if v not in (None, "")}
    return values or None


def to_prefect_params(req) -> PrefectPayload:
    if isinstance(req, LocalFileRequest):
        name = SourceName.LOCAL_FILE
        params = {"staging_key": req.staging_key, "content_type": req.content_type.value}
    elif isinstance(req, DirectUrlRequest):
        name = SourceName.DIRECT_URL
        params = {"url": req.url, "content_type": req.content_type.value}
    elif isinstance(req, YoutubeRequest):
        name = SourceName.YOUTUBE
        params = {"urls": [req.url], "audio_only": req.download_format == DownloadFormat.AUDIO}
    elif isinstance(req, YoutubeCollectionRequest):
        name = SourceName.YOUTUBE_COLLECTION
        params = {"playlist_url": req.playlist_url, "audio_only": req.download_format == DownloadFormat.AUDIO, "max_videos": req.max_videos}
        return PrefectPayload(name, SOURCE_MODE[name], params)
    elif isinstance(req, InternetArchiveRequest):
        name = SourceName.INTERNET_ARCHIVE
        params = {"urls": [req.url]}
    elif isinstance(req, WebCrawlRequest):
        name = SourceName.WEB_CRAWL
        params = {"seed_urls": req.seed_urls, "keywords": req.keywords, "allowed_domains": req.allowed_domains,
                  "max_depth": req.max_depth, "max_pages": req.max_pages}
        return PrefectPayload(name, SOURCE_MODE[name], params)
    elif isinstance(req, MillerCenterRequest):
        name = SourceName.MILLER_CENTER
        params = {k: v for k, v in {"urls": req.urls, "start_url": req.start_url, "max_depth": req.max_depth}.items() if v is not None}
        return PrefectPayload(name, SOURCE_MODE[name], params)
    elif isinstance(req, UcsbRequest):
        name = SourceName.UCSB
        params = {"listing_url": req.listing_url, "link_text_filter": req.link_text_filter, "max_documents": req.max_documents}
        return PrefectPayload(name, SOURCE_MODE[name], params)
    else:
        raise TypeError(f"Unsupported request type: {type(req).__name__}")

    user_provided = _user_provided(req)
    if user_provided:
        params["user_provided"] = user_provided
    return PrefectPayload(name, SOURCE_MODE[name], params)

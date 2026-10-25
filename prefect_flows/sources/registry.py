"""
The only place that knows which sources exist. The flow asks the registry to validate params and build a source;
adding a source = one SourceSpec here (plus its adapter and params model).
Adapter imports are lazy so importing the registry stays light (yt-dlp, bs4...).
"""
from dataclasses import dataclass
from typing import Callable, Dict, List, Union

from pydantic import BaseModel

from prefect_flows.collection import models
from prefect_flows.collection.enums import Mode, SourceName


@dataclass(frozen=True)
class SourceSpec:
    name: SourceName
    mode: Mode
    params_model: type
    build: Callable      # (params: BaseModel, deps: Deps) -> BaseSource


_REGISTRY: Dict[SourceName, SourceSpec] = {}


def register(spec: SourceSpec) -> None:
    _REGISTRY[spec.name] = spec


def get(name: Union[str, SourceName]) -> SourceSpec:
    try:
        return _REGISTRY[SourceName(name)]
    except (ValueError, KeyError):
        raise ValueError(f"Unknown source '{name}'. Registered: {[n.value for n in _REGISTRY]}") from None


def names() -> List[str]:
    return [n.value for n in _REGISTRY]


def validate_params(name: Union[str, SourceName], params: dict) -> BaseModel:
    return get(name).params_model.model_validate(params or {})


# ---- builders (lazy imports) ----------------------------------------------------------------------

def _local_folder(p, deps):
    from prefect_flows.sources.local_folder import LocalFolderSource
    return LocalFolderSource(folder=p.folder)


def _local_file(p, deps):
    import os
    from prefect_flows.sources.single_item import SingleItemSource
    if p.staging_key:
        return SingleItemSource(location=p.staging_key, source_type=p.content_type.value, is_local=False,
                                raw_metadata=p.raw_metadata, staging_key=p.staging_key, staging_client=deps.store.client,
                                staging_bucket=os.environ.get("MINIO_STAGING_BUCKET", "staging"))
    return SingleItemSource(location=p.path, source_type=p.content_type.value, is_local=True, raw_metadata=p.raw_metadata)


def _direct_url(p, deps):
    from prefect_flows.sources.single_item import SingleItemSource
    return SingleItemSource(location=p.url, source_type=p.content_type.value, is_local=False, raw_metadata=p.raw_metadata)


def _youtube(p, deps):
    from prefect_flows.sources.youtube import YoutubeSource
    return YoutubeSource(urls=p.urls, audio_only=p.audio_only, playlist_mode=p.playlist_mode,
                         max_downloads=p.max_downloads, work=deps.work, known_urls=deps.known_urls, recorder=deps.recorder)


def _internet_archive(p, deps):
    from prefect_flows.sources.internet_archive import InternetArchiveSource
    return InternetArchiveSource(urls=p.urls, excluded_indices=p.excluded_indices)


def _miller_center(p, deps):
    from prefect_flows.sources.miller_center import MillerCenterSource
    return MillerCenterSource(urls=p.urls, start_url=p.start_url, max_depth=p.max_depth,
                              crawl_delay=p.crawl_delay, request_timeout=p.request_timeout, known_urls=deps.known_urls, recorder=deps.recorder)


def _ucsb(p, deps):
    from prefect_flows.sources.ucsb import UcsbSource
    return UcsbSource(listing_url=p.listing_url, link_text_filter=p.link_text_filter,
                      max_documents=p.max_documents, crawl_delay=p.crawl_delay, known_urls=deps.known_urls, recorder=deps.recorder)


def _web_crawl(p, deps):
    from prefect_flows.sources.web_scraping import WebCrawlSource
    return WebCrawlSource(seed_urls=p.seed_urls, keywords=p.keywords, allowed_domains=p.allowed_domains,
                          max_depth=p.max_depth, max_pages=p.max_pages, known_urls=deps.known_urls, recorder=deps.recorder)


for _spec in (
    SourceSpec(SourceName.LOCAL_FOLDER, Mode.SINGLE, models.LocalFolderParams, _local_folder),
    SourceSpec(SourceName.LOCAL_FILE, Mode.SINGLE, models.LocalFileParams, _local_file),
    SourceSpec(SourceName.DIRECT_URL, Mode.SINGLE, models.DirectUrlParams, _direct_url),
    SourceSpec(SourceName.YOUTUBE, Mode.SINGLE, models.YoutubeParams, _youtube),
    SourceSpec(SourceName.INTERNET_ARCHIVE, Mode.SINGLE, models.InternetArchiveParams, _internet_archive),
    SourceSpec(SourceName.MILLER_CENTER, Mode.ARCHIVE, models.MillerCenterParams, _miller_center),
    SourceSpec(SourceName.UCSB, Mode.ARCHIVE, models.UcsbParams, _ucsb),
    SourceSpec(SourceName.WEB_CRAWL, Mode.ARCHIVE, models.WebCrawlParams, _web_crawl),
):
    register(_spec)

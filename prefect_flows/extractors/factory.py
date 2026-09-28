from typing import Optional
from urllib.parse import urlparse

from prefect_flows.collection.enums import SourceName
from prefect_flows.extractors.audio_video_extractor import AudioVideoExtractor
from prefect_flows.extractors.base import BaseExtractor
from prefect_flows.extractors.generic_html import GenericHtmlExtractor
from prefect_flows.extractors.internet_archive import InternetArchiveExtractor
from prefect_flows.extractors.miller_center import MillerCenterExtractor
from prefect_flows.extractors.ucsb_extractor import UcsbExtractor

# Fallback when raw_metadata has no source_name (e.g. a page ingested through a generic path)
_DOMAIN_TO_SOURCE = {
    "millercenter.org": SourceName.MILLER_CENTER,
    "presidency.ucsb.edu": SourceName.UCSB,
    "archive.org": SourceName.INTERNET_ARCHIVE,
}


def _source_name(raw_metadata: dict, source_url: Optional[str]) -> Optional[SourceName]:
    """
    Exact SourceName contract: a stored source_name must equal an enum value (no substring matching).
    Missing or unknown values fall back to the URL's domain, then to None (generic extraction).
    """
    try:
        return SourceName((raw_metadata or {}).get("source_name"))
    except ValueError:
        pass
    host = (urlparse(source_url or "").hostname or "").lower()
    for domain, source in _DOMAIN_TO_SOURCE.items():
        if host == domain or host.endswith("." + domain):
            return source
    return None


def get_extractor(source_type: str, raw_metadata: dict, source_url: Optional[str] = None) -> BaseExtractor:
    source_name = _source_name(raw_metadata, source_url)
    if source_name in (SourceName.INTERNET_ARCHIVE, SourceName.INTERNET_ARCHIVE_COLLECTION):
        return InternetArchiveExtractor()
    if source_type in ("video", "audio"):
        return AudioVideoExtractor()
    if source_name == SourceName.MILLER_CENTER:
        return MillerCenterExtractor()
    if source_name == SourceName.UCSB:
        return UcsbExtractor()
    return GenericHtmlExtractor()

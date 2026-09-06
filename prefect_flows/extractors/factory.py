from prefect_flows.extractors.audio_video_extractor import AudioVideoExtractor
from prefect_flows.extractors.base import BaseExtractor
from prefect_flows.extractors.generic_html import GenericHtmlExtractor
from prefect_flows.extractors.internet_archive import InternetArchiveExtractor
from prefect_flows.extractors.miller_center import MillerCenterExtractor
from prefect_flows.extractors.ucsb_extractor import UcsbExtractor


def get_extractor(source_type: str, raw_metadata: dict) -> BaseExtractor:
    if raw_metadata.get("source_name") == "internet_archive":
        return InternetArchiveExtractor()
    if source_type in ("video", "audio"):
        return AudioVideoExtractor()
    if raw_metadata.get("source_name") == "miller_center":
        return MillerCenterExtractor()
    if raw_metadata.get("source_name") == "ucsb":
        return UcsbExtractor()
    return GenericHtmlExtractor()
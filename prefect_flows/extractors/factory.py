from prefect_flows.extractors.audio_video_extractor import AudioVideoExtractor
from prefect_flows.extractors.base import BaseExtractor
from prefect_flows.extractors.generic_html import GenericHtmlExtractor
from prefect_flows.extractors.miller_center import MillerCenterExtractor
from prefect_flows.extractors.ucsb_extractor import UcsbTweetsExtractor


def get_extractor(source_type: str, raw_metadata: dict) -> BaseExtractor:
    if source_type in ("video", "audio"):
        return AudioVideoExtractor()
    if raw_metadata.get("source_name") == "miller_center":
        return MillerCenterExtractor()
    if raw_metadata.get("source_name") == "ucsb_tweets":
        return UcsbTweetsExtractor()
    return GenericHtmlExtractor()
from typing import Optional
from prefect_flows.extractors.base import BaseExtractor
from prefect_flows.extractors.utils import parse_date
from prefect_flows.extractors.whisper_transcriber import transcribe


class AudioVideoExtractor(BaseExtractor):
    def extract_content(self, local_path: str, raw_metadata: dict) -> Optional[str]:
        return transcribe(local_path, raw_metadata)

    def extract_title(self, local_path: str, raw_metadata: dict) -> Optional[str]:
        return raw_metadata.get("title")

    def extract_pub_date(self, local_path: str, raw_metadata: dict) -> Optional[str]:
        return parse_date(raw_metadata.get("upload_date") or raw_metadata.get("publication_date"))
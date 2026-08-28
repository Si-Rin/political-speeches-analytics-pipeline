"""
Content Extractor: Generic HTML Extractor using Trafilatura

Trafilatura is an open source library designed to extract the core text, structure, and metadata from raw HTML while stripping away noise like ads, headers, and footers

This content extractor extracts specific data from a raw HTML file:
  - Title: 
  - Publication date: 
  - Content: 
"""

from typing import Any, Dict, Optional
import trafilatura

from prefect_flows.extractors.base import BaseExtractor
from prefect_flows.extractors.utils import parse_date, guess_title_from_html


class GenericHtmlExtractor(BaseExtractor):
    
    def __init__(self):
        super().__init__()
        self._current_path: Optional[str] = None
        self._cached_html: Optional[str] = None
        self._cached_extracted_dict: Optional[Dict[str, Any]] = None
        
    def _load_and_extract(self, local_path: str) -> Dict[str, Any]:
        """Load the file and extract the content and metadata"""
        if self._current_path != local_path:
            self._current_path = local_path
            with open(local_path, "r", encoding="utf-8", errors="replace") as f:
                self._cached_html = f.read()
            
            # All in one extraction (content + metadata)
            extracted = trafilatura.bare_extraction(
                self._cached_html, 
                with_metadata=True,
                favor_recall=True
            )
            # Convert the returned Document object into a python dict
            self._cached_extracted_dict = extracted.as_dict() if extracted else {}
        return self._cached_extracted_dict

    def extract_content(self, local_path: str, raw_metadata: dict) -> Optional[str]:
        data = self._load_and_extract(local_path)
        return data.get("text") if data else None

    def extract_title(self, local_path: str, raw_metadata: dict) -> Optional[str]:
        data = self._load_and_extract(local_path) 
        return data.get("title") or guess_title_from_html(self._cached_html) if data else None

    def extract_pub_date(self, local_path: str, raw_metadata: dict) -> Optional[str]:
        data = self._load_and_extract(local_path) 
        print(f"trafilatura date field structure : {data.get('date')}")
        return parse_date(data.get("date")) if data else None
    
    
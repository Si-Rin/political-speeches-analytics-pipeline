"""
Content Extractor: Miller Center presidential speech pages.

This content extractor extracts specific data from a Miller Center HTML page:
  - Title: div#id="block-mainpagecontent" -> h2.class="presidential-speeches--title" -> span
  - Publication date: div#id="block-mainpagecontent" -> p.class="episode-date"
  - Content: div#id="block-mainpagecontent" -> div#id="dp-expandable-text" -> div.class="transcript-inner" -> every p
"""
 
from typing import Any, Dict, Optional
from bs4 import BeautifulSoup

from prefect_flows.extractors.base import BaseExtractor
from prefect_flows.extractors.utils import parse_date

class MillerCenterExtractor(BaseExtractor):
    
    def __init__(self):
        super().__init__()
        self._current_path: Optional[str] = None
        self._cached_html: Optional[str] = None 
        self._cached_soup: Optional[str] = None
        
    def _load_html(self, local_path: str) -> BeautifulSoup:
        """Load the HTML file"""
        if self._current_path != local_path:
            self._current_path = local_path
            with open(local_path, "r", encoding="utf-8", errors="replace") as f:
                self._cached_html = f.read()   
            self._cached_soup = BeautifulSoup(self._cached_html, "html.parser")       
        return self._cached_soup      
    
    def _container(self, local_path: str):
        return self._load_html(local_path).find("div", id="block-mainpagecontent")
    
    def extract_content(self, local_path: str, raw_metadata: dict) -> Optional[str]:
        container = self._container(local_path)
        if not container:
            return None
        transcript_container = container.find("div", id="dp-expandable-text")
        if not transcript_container:
            return None
        inner = transcript_container.find("div", class_="transcript-inner")
        if not inner:
            return None
        paragraphs = [p.getText(strip=True) for p in inner.find_all("p")]
        paragraphs = [p for p in paragraphs if p]
        return "\n\n".join(paragraphs) if paragraphs else None
    
    def extract_title(self, local_path: str, raw_metadata: dict) -> Optional[str]:
        container = self._container(local_path)
        if not container:
            return None
        title_inner = container.find("h2", class_="presidential-speeches--title")
        return title_inner.find("span").get_text(strip=True) if title_inner else None

    def extract_pub_date(self, local_path: str, raw_metadata: dict) -> Optional[str]:
        container = self._container(local_path)
        if not container:
            return None
        date_tag = container.find("p", class_="episode-date")
        return parse_date(date_tag.get_text(strip=True)) if date_tag else None 
    
    
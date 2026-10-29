"""
Base interface for Silver-layer content extraction.

extract() is a template method, not abstract — it's shared logic (call the three hooks, validate, assemble the result) that every subclass inherits unchanged.
Subclasses only implement the three small extract_* hooks below, which keeps each concrete extractor short and focused instead of one long extract() reimplemented per subclass.
"""
from abc import abstractmethod, ABC
from dataclasses import dataclass
from typing import Optional

from prefect_flows.extractors.utils import detect_language, normalize_language_code, parse_date

@dataclass 
class ExtractedContent :
    content: Optional[str] = None
    title: Optional[str] = None
    publication_date: Optional[str] = None
    language: Optional[str] = None
   
@dataclass
class SilverRecord:
    doc_id: int
    title: Optional[str] = None
    publication_date: Optional[str] = None
    transcript: Optional[str] = None
    status_processing: str = "pending"
    error_message: Optional[str] = None

    @property
    def word_count(self) -> Optional[int]:
        return len(self.transcript.split()) if self.transcript else None

    @classmethod
    def success(cls, doc_id: int, content: ExtractedContent) -> "SilverRecord":
        return cls(doc_id=doc_id, title=content.title, publication_date=content.publication_date, language=content.language,
                    transcript=content.content, status_processing="success")

    @classmethod
    def failure(cls, doc_id: int, error_message: str) -> "SilverRecord":
        return cls(doc_id=doc_id, status_processing="failed", error_message=error_message)   
 
class BaseExtractor(ABC):
    """Subclassed for each new type of content extractor"""
    
    def extract(self, local_path: str, raw_metadata: dict) -> ExtractedContent:
        """Template method, not to be overrided, calls the hooks and validates the result"""
        content = self.extract_content(local_path, raw_metadata)
        if not content or not content.strip():
            raise ValueError(f"{self.__class__.__name__}: extraction produced empty content")
        title = self.extract_title(local_path, raw_metadata)
        pub_date = self.extract_pub_date(local_path, raw_metadata)
        # Values typed by the researcher win over detected ones
        user = (raw_metadata or {}).get("user_provided") or {}
        title = (user.get("title") or "").strip() or title
        pub_date = parse_date(user.get("publication_date")) or pub_date
        language = normalize_language_code(user.get("language")) or self.extract_language(local_path, raw_metadata, content)
        return ExtractedContent(content=content, title=title, publication_date=pub_date)

    @abstractmethod
    def extract_language(self, local_path: str, raw_metadata: dict, content: str) -> Optional[str]:
    """Deetect it from the text. Overridden where the language is known (audio/video: the language Whisper was told to use)."""
    return detect_language(content)

    @abstractmethod
    def extract_content(self, local_path: str, raw_metadata: dict) -> Optional[str]:
        raise NotImplementedError
    
    @abstractmethod
    def extract_title(self, local_path: str, raw_metadata: dict) -> Optional[str]:
        raise NotImplementedError
    
    @abstractmethod
    def extract_pub_date(self, local_path: str, raw_metadata: dict) -> Optional[str]:
        raise NotImplementedError
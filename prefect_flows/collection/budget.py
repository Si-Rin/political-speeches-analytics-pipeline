"""
Limits for one collection run.
The budget tracks usage and enforces limits.
"""
from dataclasses import dataclass
from typing import Optional


@dataclass
class CollectionBudget:
    max_listing_pages: Optional[int] = None
    hard_max_documents: Optional[int] = None
    listing_pages_used: int = 0
    documents_used: int = 0

    def take_listing_page(self) -> bool:
        if self.max_listing_pages is not None and self.listing_pages_used >= self.max_listing_pages:
            return False
        self.listing_pages_used += 1
        return True

    def take_document(self) -> bool:
        if self.hard_max_documents is not None and self.documents_used >= self.hard_max_documents:
            return False
        self.documents_used += 1
        return True

    @property
    def exhausted(self) -> bool:
        return (
            (self.max_listing_pages is not None and self.listing_pages_used >= self.max_listing_pages)
            or (self.hard_max_documents is not None and self.documents_used >= self.hard_max_documents)
        )

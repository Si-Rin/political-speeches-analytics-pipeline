"""
Cheap filter, before any fetch.

Small composable predicates over a listing link.
Never looks at keywords.
"""
import re
from dataclasses import dataclass, field
from datetime import date
from typing import Optional, Sequence

from prefect_flows.collection.enums import ReasonCode


@dataclass
class Link:
    url: str
    canonical_url: str
    anchor_text: str = ""
    title_hint: Optional[str] = None
    listing_url: Optional[str] = None
    listing_date: Optional[date] = None
    extra: dict = field(default_factory=dict)


@dataclass(frozen=True)
class Decision:
    ok: bool
    reason: Optional[ReasonCode] = None
    detail: Optional[str] = None


ACCEPT = Decision(True)


class DateRange:
    """Rejects links whose listing date is known and outside [date_from, date_to]. Unknown dates pass."""

    def __init__(self, date_from: Optional[date] = None, date_to: Optional[date] = None):
        self.date_from, self.date_to = date_from, date_to

    def __call__(self, link: Link) -> Decision:
        d = link.listing_date
        if d is None:
            return ACCEPT
        if (self.date_from and d < self.date_from) or (self.date_to and d > self.date_to):
            return Decision(False, ReasonCode.OUT_OF_DATE_RANGE, d.isoformat())
        return ACCEPT


class UrlPattern:
    """Optional include regex (must match) and exclude regexes (must not match) on the canonical URL."""

    def __init__(self, include: Optional[str] = None, exclude: Sequence[str] = ()):
        self.include = re.compile(include) if include else None
        self.exclude = [re.compile(p) for p in exclude]

    def __call__(self, link: Link) -> Decision:
        if self.include and not self.include.search(link.canonical_url):
            return Decision(False, ReasonCode.NOT_A_DOCUMENT_LINK, "url does not match include pattern")
        for pattern in self.exclude:
            if pattern.search(link.canonical_url):
                return Decision(False, ReasonCode.URL_PATTERN_EXCLUDED, pattern.pattern)
        return ACCEPT


class AnchorPattern:
    """The anchor text must match (e.g. UCSB: 'Tweets of ...'); otherwise the link is not a document link."""

    def __init__(self, pattern: str):
        self.pattern = re.compile(pattern, re.IGNORECASE)

    def __call__(self, link: Link) -> Decision:
        if self.pattern.search(link.anchor_text or ""):
            return ACCEPT
        return Decision(False, ReasonCode.NOT_A_DOCUMENT_LINK, "anchor text does not match")


class Prefilter:
    def __init__(self, *predicates):
        self.predicates = predicates

    def check(self, link: Link) -> Decision:
        for predicate in self.predicates:
            decision = predicate(link)
            if not decision.ok:
                return decision
        return ACCEPT

"""
Keyword matching for the content filter.

The filter decides *whether* text matches
case- / accent-insensitive, whole-word/phrase, any/all.
"""
import hashlib
import re
import unicodedata
from dataclasses import dataclass
from typing import Iterable, Optional, Sequence

from prefect_flows.collection.enums import MatchScope


def normalize_text(text: str) -> str:
    """Accent-fold, casefold, unify apostrophes and collapse whitespace."""
    text = (text or "").replace("\u2019", "'").replace("\u2018", "'")
    decomposed = unicodedata.normalize("NFKD", text)
    stripped = "".join(ch for ch in decomposed if not unicodedata.combining(ch))
    return re.sub(r"\s+", " ", stripped.casefold()).strip()


@dataclass(frozen=True)
class KeywordMatch:
    keywords: tuple          # the (original) keywords that matched, in the order given
    scope: MatchScope


class KeywordFilter:
    def __init__(self, keywords: Sequence[str], mode: str = "any"):
        cleaned = [k.strip() for k in keywords if k and k.strip()]
        if not cleaned:
            raise ValueError("KeywordFilter needs at least one non-empty keyword")
        if mode not in ("any", "all"):
            raise ValueError("match mode must be 'any' or 'all'")
        self.keywords = tuple(dict.fromkeys(cleaned))
        self.mode = mode
        self._patterns = {k: self._compile(k) for k in self.keywords}

    @staticmethod
    def _compile(keyword: str) -> "re.Pattern":
        words = [re.escape(w) for w in normalize_text(keyword).split(" ")]
        return re.compile(r"(?<!\w)" + r"\s+".join(words) + r"(?!\w)")

    @property
    def keywords_hash(self) -> str:
        """Stable id of (keywords, mode): the rejection cache key. Order- and case-insensitive."""
        canonical = self.mode + "|" + "|".join(sorted(normalize_text(k) for k in self.keywords))
        return hashlib.sha1(canonical.encode("utf-8")).hexdigest()[:16]

    def matched_keywords(self, text: str) -> list:
        norm = normalize_text(text)
        return [k for k in self.keywords if self._patterns[k].search(norm)]

    def match(self, text: str, scope: MatchScope = MatchScope.CONTENT) -> Optional[KeywordMatch]:
        found = self.matched_keywords(text)
        ok = bool(found) if self.mode == "any" else len(found) == len(self.keywords)
        return KeywordMatch(tuple(found), scope) if ok else None

    def match_first(self, texts: Iterable) -> Optional[KeywordMatch]:
        """texts: (MatchScope, text) pairs tried in order; the first scope that satisfies the filter wins."""
        for scope, text in texts:
            if text and (m := self.match(text, scope)):
                return m
        return None

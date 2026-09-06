"""
Content Extractor: UCSB "Tweets of <date>" (1st term) and "Truth Social
Posts of <date>" (2nd term) compilation pages — Trump switched platforms
between terms, so UCSB compiles each under a different title and a
different table row layout. One class, dispatching internally on
raw_metadata["source_name"] ("ucsb_tweets" vs "ucsb_truths"), since
title/pub_date extraction is IDENTICAL between the two formats and only
extract_content's row-parsing differs.

- ucsb_tweets pages: flat 2-cell rows (timestamp, content ending in
  "Retweets: N" / "Favorites: N" labels). Retweets matched via
  RT_PATTERN, which handles "RT @user: text" (2016+) and bare pre-2016
  "@user: text" quote-style retweets.
- ucsb_truths pages: PAIRED rows — a 2-cell content row (timestamp,
  one-or-more <p> text) immediately followed by a 1-cell permalink row.
  No Retweets:/Favorites: labels at all. Reposts ("ReTruths") have no
  text prefix to match on, so they're identified via the permalink's
  handle (truthsocial.com/@<handle>/posts/...) instead — confirmed
  against a real sample day containing an actual repost (permalink
  pointed to @DonaldTrumpLiveNews, not @realDonaldTrump).

Both formats: link-only entries (bare URL, no real words) are excluded,
and @mentions/URLs are stripped from the final content regardless of
where in the text they appear — a bare handle or link isn't
natural-language rhetoric, and previously slipped through when only the
retweet PREFIX was checked (see git history: "realdonaldtrump" was
leaking into keywords/entities from a mid-tweet mention before this).
"""
import re
from typing import Optional
from bs4 import BeautifulSoup

from prefect_flows.extractors.base import BaseExtractor

RT_PATTERN = re.compile(r'^["\u201c]?(RT\s+)?@\w+[:\-]?\s*')
MENTION_PATTERN = re.compile(r"@\w+")
URL_PATTERN = re.compile(r"https?://\S+")
PERMALINK_PATTERN = re.compile(r"truthsocial\.com/@([\w.]+)/posts/", re.IGNORECASE)

OWN_HANDLE = "realdonaldtrump"


class UcsbExtractor(BaseExtractor):
    def _soup(self, local_path: str) -> BeautifulSoup:
        if not hasattr(self, "_cached_soup"):
            with open(local_path, "r", encoding="utf-8", errors="replace") as f:
                self._cached_soup = BeautifulSoup(f.read(), "html.parser")
        return self._cached_soup

    def _tbody(self, local_path: str):
        soup = self._soup(local_path)
        content_div = soup.find("div", class_="field-docs-content")
        table = content_div.find("table") if content_div else None
        return table.find("tbody") if table else None

    def _clean(self, text: str) -> str:
        """Strips URLs and @mentions from anywhere in the text, then
        collapses whitespace left behind. Shared by both formats."""
        cleaned = URL_PATTERN.sub("", text)
        cleaned = MENTION_PATTERN.sub("", cleaned)
        return re.sub(r"\s+", " ", cleaned).strip()

    # ---- ucsb_tweets: flat 2-cell rows ----
    def _parse_tweets(self, tbody) -> list[dict]:
        tweets = []
        for row in tbody.find_all("tr"):
            cells = row.find_all("td")
            if len(cells) != 2:
                continue  # skips the "data as of <date>" footer row (colspan=2)

            _, content_cell = cells
            # separator="\n" so "Retweets:"/"Favorites:" labels land on
            # their own line regardless of the <br> tags between them
            cell_text = content_cell.get_text(separator="\n", strip=True)
            tweet_text = cell_text.split("Retweets:")[0]
            tweet_text = re.sub(r"\s*\n\s*", " ", tweet_text).strip()
            if not tweet_text:
                continue

            without_urls = URL_PATTERN.sub("", tweet_text).strip()
            tweets.append({
                "text": tweet_text,
                "is_retweet": bool(RT_PATTERN.match(tweet_text)),
                "is_substantive": len(without_urls) >= 3,
            })
        return tweets

    def _extract_tweets_content(self, local_path: str) -> Optional[str]:
        tbody = self._tbody(local_path)
        if not tbody:
            return None
        tweets = self._parse_tweets(tbody)
        original = []
        for t in tweets:
            if t["is_retweet"] or not t["is_substantive"]:
                continue
            cleaned = self._clean(t["text"])
            if cleaned:
                original.append(cleaned)
        return "\n\n".join(original) if original else None

    # ---- ucsb_truths: paired rows (content row + permalink row) ----
    def _parse_truths(self, tbody) -> list[dict]:
        rows = tbody.find_all("tr", recursive=False)
        posts = []
        i = 0
        while i < len(rows):
            cells = rows[i].find_all("td")
            if len(cells) != 2:
                i += 1  # not a content row (e.g. an orphaned permalink row) — skip just this one
                continue

            timestamp_cell, text_cell = cells
            paragraphs = text_cell.find_all("p")
            text = (
                " ".join(p.get_text(strip=True) for p in paragraphs)
                if paragraphs else text_cell.get_text(strip=True)
            )

            handle = OWN_HANDLE
            if i + 1 < len(rows):
                link = rows[i + 1].find("a", href=True)
                if link:
                    match = PERMALINK_PATTERN.search(link["href"])
                    if match:
                        handle = match.group(1).lower()
                i += 2
            else:
                i += 1  # last row on the page, no permalink to check — default to own handle

            if text:
                posts.append({"text": text, "is_repost": handle != OWN_HANDLE})
        return posts

    def _extract_truths_content(self, local_path: str) -> Optional[str]:
        tbody = self._tbody(local_path)
        if not tbody:
            return None
        posts = self._parse_truths(tbody)
        original = []
        for p in posts:
            if p["is_repost"]:
                continue
            cleaned = self._clean(p["text"])
            if len(cleaned) >= 3:
                original.append(cleaned)
        return "\n\n".join(original) if original else None

    # ---- BaseExtractor interface ----
    def extract_content(self, local_path: str, raw_metadata: dict) -> Optional[str]:
        if raw_metadata.get("source_name") == "ucsb_truths":
            return self._extract_truths_content(local_path)
        return self._extract_tweets_content(local_path)

    def extract_title(self, local_path: str, raw_metadata: dict) -> Optional[str]:
        soup = self._soup(local_path)
        title_div = soup.find("div", class_="field-ds-doc-title")
        h1 = title_div.find("h1") if title_div else None
        return h1.get_text(strip=True) if h1 else None

    def extract_pub_date(self, local_path: str, raw_metadata: dict) -> Optional[str]:
        soup = self._soup(local_path)
        date_div = soup.find("div", class_="field-docs-start-date-time")
        date_span = date_div.find("span", attrs={"property": "dc:date"}) if date_div else None
        if date_span and date_span.get("content"):
            return date_span["content"][:10]  # "2015-06-16T00:00:00+00:00" -> "2015-06-16"
        return None
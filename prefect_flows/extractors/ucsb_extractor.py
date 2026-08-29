"""
Content Extractor: UCSB "Tweets of <date>" compilation pages.

Each page lists all of Trump's tweets for one calendar day, including
retweets of other accounts and link-only/media-attachment tweets with no
real text. Since this project analyzes his own rhetoric, both are
excluded from `content`:

- Retweets: matched via RT_PATTERN, which must handle two historical
  formats — "RT @user: text" (2016+) and bare "@user: text" (pre-2016
  quote-style retweets, no RT prefix).
- Link-only tweets: entries that are just a bare t.co URL with no actual
  words (media/image attachments) — these add tokenizer noise
  (https, t, co) without contributing any real language.
"""
import re
from typing import Optional
from bs4 import BeautifulSoup

from prefect_flows.extractors.base import BaseExtractor

RT_PATTERN = re.compile(r"^(RT\s+)?@\w+:\s*")
URL_PATTERN = re.compile(r"https?://\S+")


class UcsbTweetsExtractor(BaseExtractor):
    def _soup(self, local_path: str) -> BeautifulSoup:
        if not hasattr(self, "_cached_soup"):
            with open(local_path, "r", encoding="utf-8", errors="replace") as f:
                self._cached_soup = BeautifulSoup(f.read(), "html.parser")
        return self._cached_soup

    def _tweets(self, local_path: str) -> list[dict]:
        if not hasattr(self, "_cached_tweets"):
            self._cached_tweets = self._parse_tweets(self._soup(local_path))
        return self._cached_tweets

    def _is_substantive(self, text: str) -> bool:
        """False for tweets that are just a bare link/media attachment
        with no actual words."""
        without_urls = URL_PATTERN.sub("", text).strip()
        return len(without_urls) >= 3

    def _parse_tweets(self, soup: BeautifulSoup) -> list[dict]:
        content_div = soup.find("div", class_="field-docs-content")
        table = content_div.find("table") if content_div else None
        tbody = table.find("tbody") if table else None
        if not tbody:
            return []

        tweets = []
        for row in tbody.find_all("tr"):
            cells = row.find_all("td")
            if len(cells) != 2:
                continue  # skips the "data as of <date>" footer row (colspan=2)

            timestamp_cell, content_cell = cells
            timestamp_text = timestamp_cell.get_text(separator=" ", strip=True)

            # separator="\n" so "Retweets:"/"Favorites:" labels land on
            # their own line regardless of the <br> tags between them
            cell_text = content_cell.get_text(separator="\n", strip=True)

            retweets_match = re.search(r"Retweets:\s*\n?\s*(\d+)", cell_text)
            favorites_match = re.search(r"Favorites:\s*\n?\s*(\d+)", cell_text)

            tweet_text = cell_text.split("Retweets:")[0]
            tweet_text = re.sub(r"\s*\n\s*", " ", tweet_text).strip()
            if not tweet_text:
                continue

            tweets.append({
                "timestamp": timestamp_text,
                "text": tweet_text,
                "retweets": int(retweets_match.group(1)) if retweets_match else None,
                "favorites": int(favorites_match.group(1)) if favorites_match else None,
                "is_retweet": bool(RT_PATTERN.match(tweet_text)),
                "is_substantive": self._is_substantive(tweet_text),
            })
        return tweets

    def extract_content(self, local_path: str, raw_metadata: dict) -> Optional[str]:
        tweets = self._tweets(local_path)
        original = [
            t["text"] for t in tweets
            if not t["is_retweet"] and t["is_substantive"]
        ]
        return "\n\n".join(original) if original else None

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
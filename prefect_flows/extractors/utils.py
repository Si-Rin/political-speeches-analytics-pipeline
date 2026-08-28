from datetime import datetime
import html
import re
from typing import Optional

from bs4 import BeautifulSoup

def parse_date(date_str: str) -> Optional[str]:
    """
    Date normalization to YYYY-MM-DD 
    Returns None on anything unparseable rather than raising — a bad date shouldn't fail extraction    
    """
    if not date_str:
        return None
    date_str = date_str.strip()
    for fmt in ("%B %d, %Y", "%Y-%m-%d", "%Y%m%d"):     # %Y = 4-digit year, %B = full month name, %m = numeric month, %d = day of the month
        try:
            return datetime.strptime(date_str, fmt).date().isoformat()
        except ValueError:
            continue
    m = re.search(r"(\d{4})-(\d{2})-(\d{2})", date_str)
    return f"{m.group(1)}-{m.group(2)}-{m.group(3)}" if m else None

def guess_title_from_html(raw_html: str) -> Optional[str]:
    """Analyze standard meta tags - title tag and the first H1 tag"""
    if not raw_html:
        return None
    soup = BeautifulSoup(raw_html, "lxml")
    
    # 1 - meta tags (Twitter or Open Graph (og)): property="og:title"/attrs="twitter:title"
    for attr, val in [("property", "og:title"), ("name", "twitter:title"), ("name", "title")]:
        meta_tag = soup.find("meta", attrs={attr: val})
        if meta_tag and meta_tag.get("content"):
            title = html.unescape(meta_tag["content"]).strip()
            if title:
                return title
        
    # 2 - Standard HTML tags <title>
    title_tag = soup.find("title")
    if title_tag:
        raw_title = title_tag.get_text(strip=True)
        if raw_title:
            return clean_site_name_from_title(html.unescape(raw_title))
        
    # 3 - First <h1> title
    h1_tag = soup.find("h1")
    if h1_tag:
        # Make a copy to not lose the original soup
        h1_copy = BeautifulSoup(str(h1_tag), 'lxml').find("h1")
        # On supprime les éléments souvent invisibles ou parasites
        for junk in h1_copy.find_all(["span", "small", "time", "a"]):
            junk.decompose()
        cleaned_h1 = h1_tag.get_text().strip()
        # Skip empty or short <h1>
        if len(cleaned_h1) > 2:
            return cleaned_h1
    return None

def clean_site_name_from_title(title: str) -> str:
    """
    Cleans the title by extracting the site name in the end/start 
    Avoids cutting long titles having dashes
    """
    # Replaces multiple spaces with one space
    title = re.sub(r'\s+', ' ', title).strip()
    
    # Separate the title basing on those delimiters: |, -, —, ·
    separators = r'\s+[|\-—·•]\s+'
    parts = re.split(separators, title)
    
    if len(parts)>1:
        # If the last part is very short or represents less than 25% of the total length then it will be deleted
        last_part = parts[-1].strip()
        if len(last_part) < 20 or (len(last_part) / len(title)) < 0.25:
            # Rebuild the title without the last part
            return " - ".join(p.strip() for p in parts[:-1])
            
        # Same process if the site name is in the start
        first_part = parts[0].strip()
        if len(first_part) < 20 or (len(first_part) / len(title)) < 0.25:
            return " - ".join(p.strip() for p in parts[1:])
            
    return title
from datetime import date

import pytest

from prefect_flows.collection.enums import MatchScope, ReasonCode
from prefect_flows.collection.keyword_filter import KeywordFilter, normalize_text
from prefect_flows.collection.prefilter import AnchorPattern, DateRange, Link, Prefilter, UrlPattern
from prefect_flows.collection.urls import canonicalize_url


@pytest.mark.parametrize("raw, expected", [
    ("HTTPS://Example.COM/a/b/?b=2&a=1#frag", "https://example.com/a/b?a=1&b=2"),
    ("https://example.com/x?utm_source=t&id=7&fbclid=zz", "https://example.com/x?id=7"),
    ("https://example.com:443/x/", "https://example.com/x"),
    ("http://example.com:8080/x", "http://example.com:8080/x"),
    ("https://example.com", "https://example.com/"),
    ("/data/speeches/a.mp3", "/data/speeches/a.mp3"),
])
def test_canonicalize(raw, expected):
    assert canonicalize_url(raw) == expected


def test_normalize_text_folds_accents_case_and_apostrophes():
    assert normalize_text("  Café\u2019s  TARIFFS ") == "cafe's tariffs"


def test_keywords_any_all_word_boundaries():
    kf_any = KeywordFilter(["tariff", "border wall"], "any")
    assert kf_any.match("New TARIFF plan").keywords == ("tariff",)
    assert kf_any.match("tariffs everywhere") is None            # whole word only
    assert kf_any.match("the  border\nwall") is not None         # phrase, whitespace-insensitive
    kf_all = KeywordFilter(["tariff", "border wall"], "all")
    assert kf_all.match("tariff only") is None
    assert kf_all.match("tariff and border wall").keywords == ("tariff", "border wall")


def test_keyword_scope_and_match_first():
    kf = KeywordFilter(["carnage"])
    m = kf.match_first([(MatchScope.ABOUT, "populism and America First"), (MatchScope.CONTENT, "This American carnage stops")])
    assert m.scope == MatchScope.CONTENT
    assert kf.match_first([(MatchScope.ABOUT, "nothing"), (MatchScope.CONTENT, "")]) is None


def test_keywords_hash_is_order_and_case_insensitive_and_mode_sensitive():
    assert KeywordFilter(["Tariff", "NATO"]).keywords_hash == KeywordFilter(["nato", "tariff"]).keywords_hash
    assert KeywordFilter(["tariff", "nato"], "any").keywords_hash != KeywordFilter(["tariff", "nato"], "all").keywords_hash


def test_keyword_filter_rejects_bad_input():
    with pytest.raises(ValueError):
        KeywordFilter([" ", ""])
    with pytest.raises(ValueError):
        KeywordFilter(["x"], "some")


def _link(url="https://x.org/a", anchor="Tweets of June 16, 2015", d=None):
    return Link(url=url, canonical_url=canonicalize_url(url), anchor_text=anchor, listing_date=d)


def test_prefilter_predicates_and_order():
    pf = Prefilter(AnchorPattern(r"tweets of|truth social"), DateRange(date(2016, 1, 1), date(2016, 12, 31)),
                   UrlPattern(exclude=[r"/print/"]))
    assert pf.check(_link(d=date(2016, 5, 1))).ok
    assert pf.check(_link(d=None)).ok                                              # unknown date passes
    assert pf.check(_link(anchor="Press Release")).reason == ReasonCode.NOT_A_DOCUMENT_LINK
    assert pf.check(_link(d=date(2015, 6, 16))).reason == ReasonCode.OUT_OF_DATE_RANGE
    assert pf.check(_link(url="https://x.org/print/1", d=date(2016, 5, 1))).reason == ReasonCode.URL_PATTERN_EXCLUDED

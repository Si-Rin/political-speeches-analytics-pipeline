import pytest

from prefect_flows.extractors.base import BaseExtractor
from prefect_flows.extractors.factory import get_extractor
from prefect_flows.extractors.generic_html import GenericHtmlExtractor
from prefect_flows.extractors.internet_archive import InternetArchiveExtractor
from prefect_flows.extractors.miller_center import MillerCenterExtractor
from prefect_flows.extractors.ucsb_extractor import UcsbExtractor
from prefect_flows.extractors.whisper_transcriber import resolve_language


def test_factory_missing_source_name_does_not_crash():
    assert isinstance(get_extractor("text", {}), GenericHtmlExtractor)
    assert isinstance(get_extractor("text", {"source_name": None}), GenericHtmlExtractor)


def test_factory_domain_fallback():
    url = "https://millercenter.org/the-presidency/presidential-speeches/january-20-2017-inaugural-address"
    assert isinstance(get_extractor("text", {}, url), MillerCenterExtractor)
    assert isinstance(get_extractor("text", {}, "https://www.presidency.ucsb.edu/documents/x"), UcsbExtractor)
    assert isinstance(get_extractor("text", {}, "https://example.com/a"), GenericHtmlExtractor)


@pytest.mark.parametrize("name, cls", [
    ("ucsb", UcsbExtractor),
    ("miller_center", MillerCenterExtractor),
    ("internet_archive", InternetArchiveExtractor),
    ("internet_archive_collection", InternetArchiveExtractor),
])
def test_factory_exact_source_name(name, cls):
    assert isinstance(get_extractor("text", {"source_name": name}, "https://example.com"), cls)


def test_factory_enum_contract_is_exact_not_substring():
    # "ucsb_tweets" is not a SourceName: it must not match by substring any more.
    assert isinstance(get_extractor("text", {"source_name": "ucsb_tweets"}, "https://example.com"), GenericHtmlExtractor)
    # ...but an unknown name still falls back to the domain, like a missing one.
    assert isinstance(
        get_extractor("text", {"source_name": "ucsb_tweets"}, "https://www.presidency.ucsb.edu/documents/x"),
        UcsbExtractor,
    )


class _Fake(BaseExtractor):
    def extract_content(self, p, m): return "some text"
    def extract_title(self, p, m): return "Detected title"
    def extract_pub_date(self, p, m): return "2017-01-20"


def test_user_provided_overrides_detected():
    out = _Fake().extract("x", {"user_provided": {"title": "My title", "publication_date": "2020-05-01"}})
    assert (out.title, out.publication_date) == ("My title", "2020-05-01")


def test_detected_values_kept_without_user_input():
    out = _Fake().extract("x", {})
    assert (out.title, out.publication_date) == ("Detected title", "2017-01-20")


def test_unparseable_user_date_falls_back_to_detected():
    out = _Fake().extract("x", {"user_provided": {"publication_date": "not a date"}})
    assert out.publication_date == "2017-01-20"


@pytest.mark.parametrize("value, expected", [
    (None, "en"), ("", "en"), ("fr", "fr"), ("FR", "fr"), ("  French ", "fr"), ("Arabic", "ar"),
    ("en-US", "en"), ("zh_CN", "zh"), ("Ukrainian", "uk"), ("hi", "hi"), ("ko", "ko"),
    ("xx", "en"),          # well-formed but not in the allow-list
    ("ur", "en"),          # real language, not supported
    ("klingon!!", "en"),
])
def test_resolve_language(value, expected):
    meta = {"user_provided": {"language": value}} if value is not None else {}
    assert resolve_language(meta) == expected

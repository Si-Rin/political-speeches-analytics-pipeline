from pathlib import Path

from bs4 import BeautifulSoup

from prefect_flows.collection.urls import safe_file_name
from prefect_flows.collection.workarea import Scope
from prefect_flows.sources.miller_center import MillerCenterSource, is_listing_url

# The URL that crashed with "OSError: File name too long"
LONG = ("https://millercenter.org/the-presidency/presidential-speeches?field_full_node_value=&field_president_target_id%5B8396%5D=8396"
        "&viewsreference%5Bcompressed%5D=" + "eJxlkN0KgzAMhd8l173YD1PWlynZGrtAWkXrQMR3X4pzju2itIfvnJzQGTxmBDtD5iwE9mCgbZqBMtg0ihh4EHrqNyUc" * 3)


def test_listing_url_gives_a_short_safe_name_without_the_query():
    name = safe_file_name(LONG)
    assert name.startswith("presidential-speeches_") and name.endswith(".html")
    assert len(name) < 60 and "?" not in name and "%" not in name and "=" not in name


def test_simple_speech_urls_keep_their_readable_name():
    assert safe_file_name("https://millercenter.org/the-presidency/presidential-speeches/january-20-2017-inaugural-address") \
        == "january-20-2017-inaugural-address.html"


def test_different_filters_do_not_collide():
    a = safe_file_name("https://x.org/speeches?president=1")
    b = safe_file_name("https://x.org/speeches?president=2")
    assert a != b


def test_scope_never_exceeds_the_filesystem_limit(tmp_path):
    scope = Scope(tmp_path / "c-1")
    target = scope.path_for("a" * 400 + ".html")
    target.write_text("ok")                      # would raise OSError: File name too long before
    assert target.suffix == ".html" and len(target.name.encode()) <= 255


def test_is_listing_url():
    assert is_listing_url(LONG)
    assert is_listing_url("https://millercenter.org/the-presidency/presidential-speeches/")
    assert not is_listing_url("https://millercenter.org/the-presidency/presidential-speeches/january-20-2017-inaugural-address")


def _page(slugs, next_href=None):
    links = "".join(f'<div class="views-row"><a href="/the-presidency/presidential-speeches/{s}">{s}</a></div>' for s in slugs)
    pager = f'<li class="pager__item pager__item--next"><a href="{next_href}">Next</a></li>' if next_href else ""
    return BeautifulSoup(f"<html><body>{links}<a href='/about'>About</a><ul>{pager}</ul></body></html>", "html.parser")


def _source(pages, **kw):
    src = MillerCenterSource(start_url="https://millercenter.org/the-presidency/presidential-speeches?field_president_target_id%5B8396%5D=8396",
                             crawl_delay=0, **kw)
    fetched = []
    def fake_fetch(url):
        fetched.append(url)
        return pages.get(url)
    src._fetch = fake_fetch
    return src, fetched


def test_listing_is_expanded_into_speech_candidates_across_pages():
    base = "https://millercenter.org/the-presidency/presidential-speeches?field_president_target_id%5B8396%5D=8396"
    pages = {base: _page(["speech-a", "speech-b"], next_href=base + "&page=1"),
             base + "&page=1": _page(["speech-c"])}              # no pager link on the last page -> page=2 is tried and is empty
    src, fetched = _source(pages)
    cands = list(src.candidates())
    assert [c.source_url for c in cands] == [f"https://millercenter.org/the-presidency/presidential-speeches/{s}" for s in ("speech-a", "speech-b", "speech-c")]
    assert all(c.source_type == "text" and len(c.file_name) < 60 for c in cands)
    assert not any(is_listing_url(c.source_url) for c in cands)       # the listing page itself is never stored
    assert len(fetched) == 3                                           # 2 listing pages + 1 empty probe: speech pages are NOT fetched here


def test_max_depth_caps_the_number_of_speeches():
    base = "https://millercenter.org/the-presidency/presidential-speeches?field_president_target_id%5B8396%5D=8396"
    src, _ = _source({base: _page([f"s{i}" for i in range(10)])}, max_depth=4)
    assert len(list(src.candidates())) == 4


def test_unrecognised_layout_stops_instead_of_looping():
    base = "https://millercenter.org/the-presidency/presidential-speeches?field_president_target_id%5B8396%5D=8396"
    src, fetched = _source({base: BeautifulSoup("<html><body>nothing</body></html>", "html.parser")})
    assert list(src.candidates()) == [] and len(fetched) == 1


def test_speech_page_chain_mode_is_unchanged():
    speech = "https://millercenter.org/the-presidency/presidential-speeches/a"
    nxt = BeautifulSoup('<div class="next"><a href="/the-presidency/presidential-speeches/b">n</a></div>', "html.parser")
    end = BeautifulSoup("<html></html>", "html.parser")
    src = MillerCenterSource(start_url=speech, crawl_delay=0)
    src._fetch = lambda u: {speech: nxt, "https://millercenter.org/the-presidency/presidential-speeches/b": end}.get(u)
    assert [c.source_url for c in src.candidates()] == [speech, "https://millercenter.org/the-presidency/presidential-speeches/b"]


# ---- Markup copied from the real listing (Drupal views, infinite scroll wrapper, absolute links, target=_blank) ----
REAL = [
    ("september-22-2026-address-81st-united-nations-general-assembly", "September 22, 2026: Address before the 81st United Nations General Assembly", False),
    ("july-16-2026-address-nation-election-security", "July 16, 2026: Address to the Nation on Election Security", False),
    ("april-1-2026-address-nation-iran", "April 1, 2026: Address to the Nation on Iran", False),
    ("february-28-2026-remarks-us-military-operations-against-iran", "February 28, 2026: Remarks on the U.S. Military Operations Against Iran", False),
    ("february-24-2026-state-union-address", "February 24, 2026: State of the Union Address", False),
    ("january-3-2026-statement-us-action-venezuela", "January 3, 2026: Statement on U.S. Action in Venezuela", False),
    ("january-20-2025-inaugural-address", "January 20, 2025: Inaugural Address", True),
    ("october-26-2020-swearing-ceremony-honorable-amy-coney-barrett", "October 26, 2020: Swearing-In Ceremony of the Honorable Amy Coney Barrett to the US Supreme Court ", True),
    ("february-4-2020-state-union-address", "February 4, 2020: State of the Union Address", True),
]
BASE = "https://millercenter.org/the-presidency/presidential-speeches?field_president_target_id%5B8396%5D=8396"


def _real_row(slug, title, video):
    icons = ('<span class="media-video">video icon</span>\n                ' if video else "") + \
            '<span class="media-audio">audio icon</span>\n                <span class="media-transcript">transcript icon</span>'
    return (f'<div class="views-row"><div class="views-field views-field-title"><span class="field-content">'
            f'<a href="https://millercenter.org/the-presidency/presidential-speeches/{slug}" target="_blank">{title}</a></span></div>'
            f'<div class="views-field views-field-presidential-media-icons"><span class="field-content"><section class="speech-icons-column">'
            f'{icons}</section></span></div></div>')


def _real_page(rows, extra="", pager=""):
    return BeautifulSoup('<div data-drupal-views-infinite-scroll-content-wrapper="" class="views-infinite-scroll-content-wrapper clearfix">'
                         + "".join(_real_row(*r) for r in rows) + "</div>" + extra + pager, "html.parser")


def test_real_markup_is_parsed_with_titles_in_page_order():
    src, _ = _source({BASE: _real_page(REAL)})
    cands = list(src.candidates())
    assert [c.source_url.rsplit("/", 1)[-1] for c in cands] == [r[0] for r in REAL]
    assert all(c.source_url.startswith("https://millercenter.org/the-presidency/presidential-speeches/") and "?" not in c.source_url for c in cands)
    assert cands[0].title_hint == "September 22, 2026: Address before the 81st United Nations General Assembly"
    assert cands[7].title_hint.endswith("US Supreme Court")                 # trailing space of the source title is stripped


def test_links_outside_the_result_rows_are_ignored():
    featured = '<aside><a href="/the-presidency/presidential-speeches/featured-other-speech">Featured</a></aside>'
    src, _ = _source({BASE: _real_page(REAL[:3], extra=featured)})
    assert len(list(src.candidates())) == 3


def test_bare_page_param_in_the_infinite_scroll_pager_keeps_the_president_filter():
    pager = '<ul class="js-pager__items pager" data-drupal-views-infinite-scroll-pager=""><li class="pager__item"><a class="button" href="?page=1" rel="next">Load more</a></li></ul>'
    page2 = BASE + "&page=1"
    src, fetched = _source({BASE: _real_page(REAL[:3], pager=pager), page2: _real_page(REAL[3:6])})
    assert len(list(src.candidates())) == 6
    assert fetched[1] == page2 and "field_president_target_id%5B8396%5D=8396" in fetched[1]    # filter NOT dropped


# ---- Regression: the full page also shows "Most Read Speeches" (other presidents); they must never be collected ----
MOST_READ = """<div class="dynamic-content"><header><h2>Most Read Speeches</h2></header><div class="grid-container">
<div class="grid-item"><a class="wrapper-link" href="https://millercenter.org/the-presidency/presidential-speeches/march-12-1933-fireside-chat-1-banking-crisis"><h6>March 12, 1933</h6></a></div>
<div class="grid-item"><a class="wrapper-link" href="https://millercenter.org/the-presidency/presidential-speeches/august-6-1945-statement-president-announcing-use-bomb"><h6>August 6, 1945</h6></a></div>
</div></div>"""


def _full_page(rows, pager=""):
    body = _real_page(rows, pager=pager)
    return BeautifulSoup(MOST_READ + str(body), "html.parser")


def test_most_read_block_is_ignored_on_a_normal_page():
    src, _ = _source({BASE: _full_page(REAL[:2])})
    assert [c.source_url.rsplit("/", 1)[-1] for c in src.candidates()] == [REAL[0][0], REAL[1][0]]


def test_page_past_the_end_of_the_listing_ends_it_instead_of_collecting_most_read_speeches():
    pager = '<ul class="js-pager__items pager" data-drupal-views-infinite-scroll-pager=""><li class="pager__item"><a class="button" href="?page=1" rel="next"></a></li></ul>'
    last_page = BASE + "&page=1"
    empty_results = BeautifulSoup(MOST_READ + '<div class="views-infinite-scroll-content-wrapper"></div>', "html.parser")   # rows gone, sidebar still there
    src, fetched = _source({BASE: _full_page(REAL[:3], pager=pager), last_page: _full_page(REAL[3:5]),
                            last_page.replace("page=1", "page=2"): empty_results}, max_depth=100)
    got = [c.source_url.rsplit("/", 1)[-1] for c in src.candidates()]
    assert got == [r[0] for r in REAL[:5]]
    assert not any("1933" in g or "1945" in g for g in got)

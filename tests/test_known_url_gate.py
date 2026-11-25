"""Limits ("max N") must count only NEW documents: known URLs are skipped while discovering and paging continues."""
from bs4 import BeautifulSoup

from prefect_flows.collection.enums import CandidateStatus as S
from prefect_flows.collection.recorder import DiscoveryRecorder
from prefect_flows.sources.base import KnownUrlGate
from prefect_flows.sources.miller_center import MillerCenterSource
from prefect_flows.sources.ucsb import UcsbSource

BASE = "https://millercenter.org/the-presidency/presidential-speeches?field_president_target_id%5B8396%5D=8396"
SPEECH = "https://millercenter.org/the-presidency/presidential-speeches/{}"


class Known:
    def __init__(self, *urls):
        self.urls = set(urls)
        self.asked = []
    def known_url(self, canonical_url, source_type=None):
        self.asked.append(canonical_url)
        return canonical_url in self.urls


def listing(slugs, next_href=None):
    rows = "".join(f'<div class="views-row"><div class="views-field views-field-title"><a href="{SPEECH.format(s)}">Title {s}</a></div></div>' for s in slugs)
    pager = f'<li class="pager__item pager__item--next"><a href="{next_href}">Next</a></li>' if next_href else ""
    return BeautifulSoup(f"<div>{rows}</div><ul>{pager}</ul>", "html.parser")


def miller(pages, known, max_depth, **kw):
    rec = DiscoveryRecorder("r")
    src = MillerCenterSource(start_url=BASE, max_depth=max_depth, crawl_delay=0, known_urls=known, recorder=rec, **kw)
    src._fetch = lambda url: pages.get(url)
    return src, rec


def slugs_of(cands):
    return [c.source_url.rsplit("/", 1)[-1] for c in cands]


def test_known_speeches_do_not_use_up_the_limit_and_paging_continues():
    pages = {BASE: listing(["a", "b", "c"], next_href="?page=1"), BASE + "&page=1": listing(["d", "e", "f"])}
    known = Known(SPEECH.format("a"), SPEECH.format("b"), SPEECH.format("c"))          # the whole first page was collected before
    src, rec = miller(pages, known, max_depth=2)
    assert slugs_of(src.candidates()) == ["d", "e"]                                    # limit 2 reached with NEW speeches only
    assert [e.status for e in rec.entries].count(S.KNOWN_URL_SKIPPED) == 3
    assert all(e.title_hint and e.listing_url for e in rec.entries)


def test_everything_known_ends_cleanly_with_nothing_to_ingest():
    pages = {BASE: listing(["a", "b"], next_href="?page=1"), BASE + "&page=1": listing(["c"])}
    known = Known(*(SPEECH.format(s) for s in "abc"))
    src, rec = miller(pages, known, max_depth=5)
    assert list(src.candidates()) == []                                               # no endless loop
    assert rec.counts[S.KNOWN_URL_SKIPPED] == 3


def test_partially_new_listing_ingests_what_exists_when_the_listing_runs_out():
    pages = {BASE: listing(["a", "b", "c"])}                                          # asks for 10 but only 1 is new
    src, _ = miller(pages, Known(SPEECH.format("a"), SPEECH.format("b")), max_depth=10)
    assert slugs_of(src.candidates()) == ["c"]


def test_without_a_known_port_nothing_is_skipped():
    src, rec = miller({BASE: listing(["a", "b"])}, None, max_depth=5)
    assert slugs_of(src.candidates()) == ["a", "b"] and not rec.entries


def test_miller_chain_mode_traverses_known_pages_without_counting_them():
    a, b, c = (SPEECH.format(x) for x in "abc")
    def page(nxt):
        return BeautifulSoup(f'<div class="next"><a href="{nxt}">n</a></div>' if nxt else "<html></html>", "html.parser")
    rec = DiscoveryRecorder("r")
    src = MillerCenterSource(start_url=a, max_depth=2, crawl_delay=0, known_urls=Known(a), recorder=rec)
    pages = {a: page(b), b: page(c), c: page(None)}
    src._fetch = lambda u: pages.get(u)
    assert [x.source_url for x in src.candidates()] == [b, c]                         # a is known: traversed, not counted
    assert rec.counts[S.KNOWN_URL_SKIPPED] == 1


def test_ucsb_max_documents_counts_only_new_pages():
    doc = lambda n: f"https://www.presidency.ucsb.edu/documents/tweets-{n}"
    html = "".join(f'<a href="{doc(n)}">Tweets of June {n}, 2015</a>' for n in range(1, 6)) + "<a href='/about'>About</a>"
    rec = DiscoveryRecorder("r")
    src = UcsbSource("https://www.presidency.ucsb.edu/x", max_documents=2, crawl_delay=0, known_urls=Known(doc(1), doc(2), doc(3)), recorder=rec)
    src._fetch = lambda url: BeautifulSoup(html, "html.parser")
    assert [c.source_url for c in src.candidates()] == [doc(4), doc(5)]
    assert rec.counts[S.KNOWN_URL_SKIPPED] == 3


def test_gate_ignores_non_http_sources():
    rec = DiscoveryRecorder("r")
    assert KnownUrlGate(Known("/tmp/a.txt"), rec).skip("/tmp/a.txt") is False and not rec.entries


# ---- web crawl ----
def test_web_crawl_known_pages_are_not_counted_but_their_links_are_followed(monkeypatch):
    from prefect_flows.sources import web_scraping
    from prefect_flows.sources.web_scraping import WebCrawlSource

    pages = {"https://x.org/": '<title>t</title>tariff <a href="/p1">1</a><a href="/p2">2</a><a href="/p3">3</a>',
             "https://x.org/p1": "tariff page one", "https://x.org/p2": "tariff page two", "https://x.org/p3": "tariff page three"}

    class Resp:
        def __init__(self, text): self.text, self.headers = text, {"Content-Type": "text/html"}
        def raise_for_status(self): pass

    monkeypatch.setattr(web_scraping.requests, "get", lambda url, **kw: Resp(pages[url]))
    rec = DiscoveryRecorder("r")
    src = WebCrawlSource(["https://x.org/"], ["tariff"], max_depth=2, max_pages=2, crawl_delay=0,
                         known_urls=Known("https://x.org/", "https://x.org/p1"), recorder=rec)
    src._robots_allowed = lambda u: True
    urls = [c.source_url for c in src.candidates()]
    assert urls == ["https://x.org/p2", "https://x.org/p3"]       # the known seed and p1 did not use up max_pages=2
    assert rec.counts[S.KNOWN_URL_SKIPPED] == 2


# ---- youtube playlist ----
def test_youtube_max_downloads_counts_only_new_videos(tmp_path, monkeypatch):
    from prefect_flows.collection.workarea import WorkArea
    from prefect_flows.sources import youtube

    ids = [f"{c}" * 11 for c in "abcde"]
    watch = lambda i: f"https://www.youtube.com/watch?v={i}"

    class FakeYDL:
        def __init__(self, opts): self.opts = opts
        def __enter__(self): return self
        def __exit__(self, *a): pass
        def extract_info(self, url, download=False):
            if not download:
                return {"entries": [{"url": watch(i)} for i in ids]}
            vid = url.rsplit("=", 1)[-1]
            self.path = self.opts["outtmpl"].replace("%(id)s", vid).replace("%(ext)s", "webm")
            open(self.path, "wb").write(b"x")
            return {"id": vid}
        def prepare_filename(self, info): return self.path

    monkeypatch.setattr(youtube.yt_dlp, "YoutubeDL", FakeYDL)
    rec = DiscoveryRecorder("r")
    with WorkArea("r", root=tmp_path / "w") as work:
        src = youtube.YoutubeSource(["https://www.youtube.com/playlist?list=PL"], work=work, playlist_mode=True, max_downloads=2,
                                    audio_only=True, delay_seconds=0, known_urls=Known(watch(ids[0]), watch(ids[1])), recorder=rec)
        got = []
        for cand in src.candidates():
            got.append(cand.source_url)
            cand.handle.release()
    assert got == [watch(ids[2]), watch(ids[3])]                  # a, b known: skipped, then 2 NEW videos taken
    assert rec.counts[S.KNOWN_URL_SKIPPED] == 2

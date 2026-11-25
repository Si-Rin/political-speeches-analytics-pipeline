from prefect_flows.sources.youtube import canonical_youtube_url


def test_canonical_forms_collapse_to_one_url():
    want = "https://www.youtube.com/watch?v=l1infKsQLr0"
    for u in ["https://www.youtube.com/watch?v=l1infKsQLr0&t=30s", "https://youtu.be/l1infKsQLr0?si=x",
              "https://m.youtube.com/watch?v=l1infKsQLr0", "https://www.youtube.com/shorts/l1infKsQLr0"]:
        assert canonical_youtube_url(u) == want


def test_non_youtube_url_unchanged():
    assert canonical_youtube_url("https://example.org/a.mp4") == "https://example.org/a.mp4"

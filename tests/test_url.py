from ogame_stats.utils_url import join_public_url


def test_join_public_url_slashes():
    assert join_public_url("https://x/y", "latest.html") == "https://x/y/latest.html"
    assert join_public_url("https://x/y/", "latest.html") == "https://x/y/latest.html"
    assert join_public_url("https://x/y/", "/latest.html") == "https://x/y/latest.html"
    assert join_public_url("https://x/y", "/latest.html") == "https://x/y/latest.html"


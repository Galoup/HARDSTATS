from __future__ import annotations


def join_public_url(public_base_url: str, path: str) -> str:
    """
    Join a base URL + relative path robustly.

    - tolerates base with/without trailing slash
    - tolerates path with/without leading slash
    - does not attempt to normalize/encode; caller should pass safe filenames
    """
    base = (public_base_url or "").strip()
    rel = (path or "").strip()
    if not base:
        return rel
    if not rel:
        return base
    if not base.endswith("/"):
        base += "/"
    rel = rel.lstrip("/")
    return base + rel


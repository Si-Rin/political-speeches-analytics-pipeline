"""URL normalization shared by dedup, the discovery log and the known-URL check."""
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

_TRACKING_PARAMS = {"fbclid", "gclid", "mc_cid", "mc_eid", "igshid", "ref", "ref_src"}
_DEFAULT_PORTS = {"http": 80, "https": 443}


def canonicalize_url(url: str) -> str:
    """
    Lowercase scheme and host, default ports and tracking parameters (utm_*, fbclid, ...), sort the remaining query parameters,
    and trim a trailing slash from the path (the root path stays "/").
    Local filesystem paths and anything without a scheme are returned unchanged (stripped).
    """
    url = (url or "").strip()
    parts = urlsplit(url) # split the url into its components (scheme/protocol, netloc, hostname, port, username, path, query/requestparams)
    if not parts.scheme or not parts.netloc:
        return url
    host = (parts.hostname or "").lower()
    if parts.port and parts.port != _DEFAULT_PORTS.get(parts.scheme.lower()):
        host = f"{host}:{parts.port}"
    userinfo = f"{parts.username}@" if parts.username else "" # @ could be used for basic auth but rarely
    path = parts.path.rstrip("/") or "/"
    query = urlencode(sorted(
        (k, v) for k, v in parse_qsl(parts.query, keep_blank_values=True)
        if not k.lower().startswith("utm_") and k.lower() not in _TRACKING_PARAMS
    ))
    return urlunsplit((parts.scheme.lower(), userinfo + host, path, query, ""))

"""URL normalization shared by dedup, the discovery log and the known-URL check."""
import hashlib
import re
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

def safe_file_name(url: str, ext: str = ".html", max_stem: int = 80) -> str:
    """
    Extract the last path segment of a URL and convert it to a filesystem-safe file name, with an optional extension.

    If the last path segment is too long or if the URL has a query string, append a short hash of the full URL to avoid collisions.
    
    The resulting file name is limited to 255 bytes to avoid filesystem errors.
    """
    parts = urlsplit((url or "").strip())
    segment = parts.path.rstrip("/").rsplit("/", 1)[-1]
    stem = re.sub(r"[^A-Za-z0-9._-]+", "-", segment).strip("-.") or "index"
    if stem.lower().endswith(ext.lower()):
        stem = stem[: -len(ext)] or "index"
    cut = len(stem) > max_stem
    if cut or parts.query:
        stem = f"{stem[:max_stem].rstrip('-.') or 'index'}_{hashlib.sha1(canonicalize_url(url).encode('utf-8')).hexdigest()[:8]}"
    return f"{stem}{ext}"

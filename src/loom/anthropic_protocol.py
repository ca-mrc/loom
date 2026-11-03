"""The versioned API root shared by Anthropic discovery and inference."""

from __future__ import annotations

from urllib.parse import unquote, urlsplit, urlunsplit


def anthropic_api_base_url(base_url: str) -> str:
    """Accept an origin/proxy prefix with an optional terminal ``/v1``.

    This performs path normalization, not network authorization. Hosted callers
    must still apply their existing SSRF and connection-access checks.
    """
    if any(ord(char) < 33 for char in base_url) or "\\" in base_url:
        raise ValueError("Anthropic base URL contains invalid characters")
    parts = urlsplit(base_url)
    if (parts.scheme not in {"http", "https"} or not parts.hostname or parts.username
            or parts.password or parts.query or parts.fragment):
        raise ValueError("Anthropic base URL must be HTTP(S), without credentials, query or fragment")
    # Accessing port also validates malformed/non-numeric ports.
    if parts.port is not None and not 0 < parts.port <= 65535:
        raise ValueError("Anthropic base URL has an invalid port")
    decoded = unquote(parts.path)
    if decoded != parts.path or "//" in decoded or any(p in {".", ".."} for p in decoded.split("/")):
        raise ValueError("Anthropic base URL has an ambiguous path")
    path = parts.path.rstrip("/")
    if not path.endswith("/v1"):
        path += "/v1"
    return urlunsplit(parts._replace(path=path))

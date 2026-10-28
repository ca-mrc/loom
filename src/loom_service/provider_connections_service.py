"""Provider-connection service layer.

The route handlers are thin orchestrators; this module owns the
business logic:

- URL parsing + upstream-host derivation.
- DNS resolution + SSRF IP classification (RFC1918, ULA, loopback,
  link-local, multicast, broadcast, unspecified). The team-level
  `allow_private_endpoints` flag relaxes RFC1918 + ULA; loopback +
  link-local stay rejected unconditionally because no legitimate
  provider hosts on those.

Separated from the routes so each piece is testable without a
FastAPI request fixture; the routes file is a thin orchestrator.
"""

from __future__ import annotations

import asyncio
import ipaddress
import socket
from dataclasses import dataclass
from typing import TYPE_CHECKING
from urllib.parse import quote, urlparse

from loom.security.redaction import redact_text

if TYPE_CHECKING:
    import httpx

# Public IP classifications that are always SSRF targets — rejected
# even when team.allow_private_endpoints=True. These are network
# constructs that are never legitimate provider hosts on any topology.
_UNCONDITIONALLY_REJECTED_IPS = (
    # IPv4
    ipaddress.IPv4Network("0.0.0.0/8"),       # this network / unspecified
    ipaddress.IPv4Network("127.0.0.0/8"),     # loopback
    ipaddress.IPv4Network("169.254.0.0/16"),  # link-local
    ipaddress.IPv4Network("224.0.0.0/4"),     # multicast
    ipaddress.IPv4Network("240.0.0.0/4"),     # reserved (incl. broadcast)
    # IPv6
    ipaddress.IPv6Network("::1/128"),         # loopback
    ipaddress.IPv6Network("::/128"),          # unspecified
    ipaddress.IPv6Network("fe80::/10"),       # link-local
    ipaddress.IPv6Network("ff00::/8"),        # multicast
)

# Private IP classifications — rejected by default but permitted when
# team.allow_private_endpoints=True. Operators with on-prem providers
# (vLLM in the lab, hosted Llama on internal subnet) need the opt-in.
_PRIVATE_IPS = (
    # IPv4 RFC1918
    ipaddress.IPv4Network("10.0.0.0/8"),
    ipaddress.IPv4Network("172.16.0.0/12"),
    ipaddress.IPv4Network("192.168.0.0/16"),
    # IPv4 CGNAT (RFC6598)
    ipaddress.IPv4Network("100.64.0.0/10"),
    # IPv6 ULA
    ipaddress.IPv6Network("fc00::/7"),
)


class InvalidBaseUrlError(Exception):
    """Base URL fails parsing, scheme check, or has no resolvable host."""


class SsrfRejectedError(Exception):
    """The resolved IPs include addresses the SSRF policy forbids."""

    def __init__(self, message: str, *, ips: list[str]) -> None:
        super().__init__(message)
        self.ips = ips



@dataclass(frozen=True)
class ResolvedUpstream:
    """A `base_url` validated + resolved for provider_connection storage."""

    base_url: str
    upstream_host: str
    resolved_ips: list[str]


def derive_upstream_host(base_url: str) -> str:
    """Parse `base_url` and return the hostname portion.

    Used both for the `provider_connections.upstream_host` column (the
    string the egress proxy validates SNI against) and for the DNS
    resolution that populates `resolved_egress_ips`. Raises on missing
    scheme or host.
    """
    if not base_url:
        raise InvalidBaseUrlError("base_url must be non-empty")
    parsed = urlparse(base_url)
    if parsed.scheme not in ("http", "https"):
        raise InvalidBaseUrlError(
            f"base_url scheme must be http or https; got {parsed.scheme!r}",
        )
    if not parsed.hostname:
        raise InvalidBaseUrlError(
            f"base_url has no hostname: {base_url!r}",
        )
    return parsed.hostname


def classify_ip(
    ip: ipaddress.IPv4Address | ipaddress.IPv6Address,
    *,
    allow_private: bool,
) -> str | None:
    """Return None if `ip` is allowed; otherwise a human-readable reason
    naming the rejection category. The `allow_private` flag relaxes the
    `_PRIVATE_IPS` set; the `_UNCONDITIONALLY_REJECTED_IPS` set stays
    rejected.
    """
    for net in _UNCONDITIONALLY_REJECTED_IPS:
        if ip.version == net.version and ip in net:
            return f"address {ip} is in {net} (never legitimate as a provider host)"
    if not allow_private:
        for net in _PRIVATE_IPS:
            if ip.version == net.version and ip in net:
                return (
                    f"address {ip} is in private range {net}; set "
                    f"team.allow_private_endpoints=True to permit on-prem providers"
                )
    return None


def resolve_and_validate(
    base_url: str, *, allow_private: bool,
    _resolver: object = None,
) -> ResolvedUpstream:
    """Parse + DNS-resolve `base_url`; validate every resolved IP against
    the SSRF policy.

    `_resolver`: optional override for testability. Must be a callable
    matching `socket.getaddrinfo(host, port=None)` — tests inject a
    stub to avoid hitting real DNS. Default uses stdlib `socket`.
    """
    upstream_host = derive_upstream_host(base_url)
    resolver = _resolver if _resolver is not None else socket.getaddrinfo

    try:
        # type=SOCK_STREAM filters down to TCP results so we don't see
        # the same address twice (UDP + TCP duplicates).
        infos = resolver(  # type: ignore[operator]
            upstream_host, None, type=socket.SOCK_STREAM,
        )
    except socket.gaierror as e:
        raise InvalidBaseUrlError(
            f"DNS resolution failed for {upstream_host!r}: {e}",
        ) from e

    ips: list[ipaddress.IPv4Address | ipaddress.IPv6Address] = []
    seen: set[str] = set()
    for info in infos:
        sockaddr = info[4]
        ip_str = sockaddr[0]
        # IPv6 results can include zone-id suffix (e.g. "fe80::1%eth0");
        # strip before parsing.
        ip_str_clean = ip_str.split("%", 1)[0]
        if ip_str_clean in seen:
            continue
        seen.add(ip_str_clean)
        ips.append(ipaddress.ip_address(ip_str_clean))

    if not ips:
        raise InvalidBaseUrlError(
            f"DNS resolution for {upstream_host!r} returned no addresses",
        )

    rejected_reasons: list[str] = []
    for ip in ips:
        reason = classify_ip(ip, allow_private=allow_private)
        if reason is not None:
            rejected_reasons.append(reason)
    if rejected_reasons:
        raise SsrfRejectedError(
            "; ".join(rejected_reasons),
            ips=[str(ip) for ip in ips],
        )

    return ResolvedUpstream(
        base_url=base_url,
        upstream_host=upstream_host,
        resolved_ips=[str(ip) for ip in ips],
    )


# ──────────────────────────────────────────────────────────────────────
# Connection probe (used by /test route)
# ──────────────────────────────────────────────────────────────────────

# Probe timeout — short on purpose; legitimate provider models endpoints
# answer in under a second from anywhere on the public internet. Stretching
# this only makes the route easier to use as a slow-loris vector against
# loom_service itself.
_PROBE_TIMEOUT_SEC = 5.0

# The model preflight is a real (one-token) generation, not a /models
# listing: cold or large models routinely need more than the listing
# probe's 5s. Still bounded; a timeout is recorded as inconclusive.
_GENERATION_PROBE_TIMEOUT_SEC = 20.0


def _redact_secret(s: str, *secrets: str) -> str:
    """Replace every occurrence of each non-empty secret in `s` with
    ``[REDACTED]``. Used on upstream-body excerpts + URLs before they
    land in DB columns (``last_validation_error``) or response bodies
    that downstream operators can read.

    Real concern: some upstreams echo the auth header / `?key=` query
    parameter in 4xx error bodies (debug pages, dev OpenAI-compatible
    servers). Without redaction, a single bad probe leaks the key
    into `provider_connections.last_validation_error`, where any
    team-scoped GET surfaces it.

    Two-character minimum on each secret avoids the degenerate case
    where an empty or 1-char secret would replace half the string.
    """
    out = s
    for secret in secrets:
        if secret and len(secret) >= 4:
            out = out.replace(secret, "[REDACTED]")
    return redact_text(out)


@dataclass(frozen=True)
class ProbeResult:
    """Outcome of a connection probe. `http_status` is None when the
    request never reached a server (DNS/connect/timeout); otherwise the
    upstream's response code."""

    status: str  # 'valid' | 'invalid'
    http_status: int | None
    error: str | None


@dataclass(frozen=True)
class ModelPreflightResult:
    """Outcome of a single model entitlement probe.

    `status='valid'` means the provider accepted a minimal non-streaming
    generation request for this exact model id. `status='failed'` means the
    connection/key/model combination could not be used. Error fields are safe
    to persist and return to operators; secrets are redacted before return.
    """

    status: str  # 'valid' | 'failed'
    http_status: int | None
    error_code: str | None
    error_message: str | None


# A failed preflight is either a *rejection* (the upstream refused this
# key/model: 401/403, unknown model, egress policy) or *inconclusive* (the
# probe never got a definitive answer: timeout, network error, 408/429/5xx).
# Only rejections may block batch submission (#948); an inconclusive probe
# is not evidence the model is unusable, and blocking on it indefinitely
# strands a valid connection until someone happens to re-run preflight.
_INCONCLUSIVE_PREFLIGHT_ERROR_CODES = frozenset(
    {"timeout", "request-error", "unexpected-error"},
)


def classify_preflight_failure(
    error_code: str | None, http_status: int | None,
) -> str:
    """Return ``'inconclusive'`` or ``'rejected'`` for a failed preflight.

    Unknown codes without a transient HTTP status stay ``'rejected'`` so
    genuine auth/model incompatibility remains fail-closed.
    """
    if error_code in _INCONCLUSIVE_PREFLIGHT_ERROR_CODES:
        return "inconclusive"
    if error_code == "model-not-found":
        # The upstream names the model as unroutable, whatever the status:
        # OpenAI-compatible relays answer 503 when no channel serves it.
        return "rejected"
    if http_status is not None and (http_status in (408, 429) or http_status >= 500):
        return "inconclusive"
    return "rejected"


def preflight_failure_kind(
    status: str | None, error_code: str | None, http_status: int | None,
) -> str | None:
    """Failure kind for a cached preflight row; ``None`` unless it failed."""
    if status != "failed":
        return None
    return classify_preflight_failure(error_code, http_status)


def _probe_url_and_headers(
    provider_type: str, base_url: str, api_key: str,
) -> tuple[str, dict[str, str]]:
    """Map (type, base_url, api_key) → (probe URL, headers). Every probe
    is GET; bodies stay zero-byte so a misconfigured provider can't bill
    us for tokens.

    - openai-compatible / custom: `GET <base>/models` with Bearer
    - anthropic: `GET <base>/models` with `x-api-key` + version header
    - google: `GET <base>/models?key=<API_KEY>` (Google API style)
    """
    base = base_url.rstrip("/")
    if provider_type == "anthropic":
        return (
            f"{base}/models",
            {
                "x-api-key": api_key,
                "anthropic-version": "2023-06-01",
            },
        )
    if provider_type == "google":
        return (
            f"{base}/models?key={api_key}",
            {},
        )
    # openai-compatible + custom (defensible default)
    return (
        f"{base}/models",
        {"Authorization": f"Bearer {api_key}"},
    )


def _preflight_base_path_headers_body(
    provider_type: str, base_url: str, api_key: str, model_id: str,
) -> tuple[str, str, dict[str, str], dict[str, object]]:
    """Map a provider connection + model id to a minimal generation call.

    Discovery uses `/models`; preflight intentionally hits the generation
    surface so entitlement failures such as 403 "model not enabled" are
    caught before a batch is submitted.
    """
    base = base_url.rstrip("/")
    if provider_type == "anthropic":
        return (
            base,
            "/messages",
            {
                "x-api-key": api_key,
                "anthropic-version": "2023-06-01",
            },
            {
                "model": model_id,
                "messages": [{"role": "user", "content": "ping"}],
                "max_tokens": 1,
            },
        )
    if provider_type == "google":
        quoted_model = quote(model_id, safe="")
        return (
            base,
            f"/models/{quoted_model}:generateContent?key={api_key}",
            {},
            {
                "contents": [{"parts": [{"text": "ping"}]}],
                "generationConfig": {"maxOutputTokens": 1},
            },
        )
    return (
        base,
        "/chat/completions",
        {"Authorization": f"Bearer {api_key}"},
        {
            "model": model_id,
            "messages": [{"role": "user", "content": "ping"}],
            "max_tokens": 1,
        },
    )


async def probe_connection(
    provider_type: str, base_url: str, api_key: str,
    *, _client_factory: object = None,
) -> ProbeResult:
    """Probe a provider connection's reachability + credential validity.

    Issues a single GET to the provider's `/models` listing endpoint
    with a short timeout, no redirects (a 3xx to an internal host
    would re-introduce SSRF). 2xx → valid; anything else → invalid
    with the http_status + a 200-char body excerpt for diagnosis.

    `_client_factory` is a test seam — must be a callable returning an
    `httpx.AsyncClient`-like object exposing `.get(url, headers=...)`.
    Default constructs a fresh AsyncClient per call so settings stay
    explicit (timeout + no redirects).
    """
    import httpx

    url, headers = _probe_url_and_headers(provider_type, base_url, api_key)

    if _client_factory is None:
        client_cm = httpx.AsyncClient(
            timeout=_PROBE_TIMEOUT_SEC, follow_redirects=False,
        )
    else:
        client_cm = _client_factory()  # type: ignore[operator]

    try:
        async with client_cm as client:
            try:
                resp = await client.get(url, headers=headers)
            except httpx.TimeoutException as e:
                return ProbeResult(
                    status="invalid", http_status=None,
                    error=f"timeout after {_PROBE_TIMEOUT_SEC}s: {e}",
                )
            except httpx.RequestError as e:
                return ProbeResult(
                    status="invalid", http_status=None,
                    error=f"{type(e).__name__}: {e}",
                )
    except Exception as e:
        # Some test doubles raise at __aenter__ instead of .get; surface
        # as invalid rather than 500 the route.
        return ProbeResult(
            status="invalid", http_status=None,
            error=f"{type(e).__name__}: {e}",
        )

    if 200 <= resp.status_code < 300:
        return ProbeResult(
            status="valid", http_status=resp.status_code, error=None,
        )
    excerpt = (resp.text or "")[:200]
    # Redact: upstream may echo the auth header / `?key=` query param
    # in 4xx debug pages, and this error string is persisted to
    # `provider_connections.last_validation_error` (readable via
    # GET /provider-connections/{id}).
    safe_url = _redact_secret(url, api_key)
    safe_excerpt = _redact_secret(excerpt, api_key)
    return ProbeResult(
        status="invalid",
        http_status=resp.status_code,
        error=(
            f"HTTP {resp.status_code} from {safe_url}; "
            f"body excerpt: {safe_excerpt!r}"
        ),
    )


def _upstream_error_code(resp: httpx.Response) -> str | None:
    """`error.code` from an OpenAI-shaped error body, if there is one."""
    try:
        body = resp.json()
    except ValueError:
        return None
    error = body.get("error") if isinstance(body, dict) else None
    code = error.get("code") if isinstance(error, dict) else None
    return code if isinstance(code, str) else None


async def preflight_model(
    provider_type: str, base_url: str, api_key: str, model_id: str,
    *, _client_factory: object = None,
) -> ModelPreflightResult:
    """Probe whether this connection/key can call one concrete model.

    This is deliberately single-model and bounded: no catalog-wide probing,
    no streaming, one token max. Any 2xx counts as valid; provider-specific
    response validation is intentionally out of scope because entitlement is
    already proven when the upstream accepts the generation request.
    """
    import httpx

    base, path, headers, body = _preflight_base_path_headers_body(
        provider_type, base_url, api_key, model_id,
    )
    if _client_factory is None:
        client_cm = httpx.AsyncClient(
            base_url=base,
            timeout=_GENERATION_PROBE_TIMEOUT_SEC, follow_redirects=False,
        )
    else:
        client_cm = _client_factory(base_url=base)  # type: ignore[operator]

    try:
        async with client_cm as client:
            try:
                # Routes re-run resolve_and_validate() immediately before
                # invoking this helper and block forbidden DNS/IP results.
                # Runtime egress policy also gates the stored resolved IP set.
                # codeql[py/full-ssrf]
                # HTTPX timeouts bound each connect/read/write step, not the
                # whole request: an upstream dripping bytes inside the read
                # timeout could hold the probe open indefinitely. The total
                # deadline makes the probe genuinely bounded (#948). Caller
                # cancellation still propagates as CancelledError.
                async with asyncio.timeout(_GENERATION_PROBE_TIMEOUT_SEC):
                    resp = await client.post(
                        path, headers=headers, json=body,
                    )
            except (httpx.TimeoutException, TimeoutError) as e:
                detail = str(e) or "total deadline exceeded"
                return ModelPreflightResult(
                    status="failed",
                    http_status=None,
                    error_code="timeout",
                    error_message=_redact_secret(
                        f"timeout after {_GENERATION_PROBE_TIMEOUT_SEC}s: {detail}",
                        api_key,
                    ),
                )
            except httpx.RequestError as e:
                return ModelPreflightResult(
                    status="failed",
                    http_status=None,
                    error_code="request-error",
                    error_message=_redact_secret(f"{type(e).__name__}: {e}", api_key),
                )
    except Exception as e:
        return ModelPreflightResult(
            status="failed",
            http_status=None,
            error_code="unexpected-error",
            error_message=_redact_secret(f"{type(e).__name__}: {e}", api_key),
        )

    if 200 <= resp.status_code < 300:
        return ModelPreflightResult(
            status="valid",
            http_status=resp.status_code,
            error_code=None,
            error_message=None,
        )

    excerpt = (resp.text or "")[:200]
    safe_url = _redact_secret(f"{base}{path}", api_key)
    safe_excerpt = _redact_secret(excerpt, api_key)
    if resp.status_code in (401, 403):
        error_code = "access-denied"
    elif _upstream_error_code(resp) == "model_not_found":
        error_code = "model-not-found"
    else:
        error_code = "upstream-http-error"
    return ModelPreflightResult(
        status="failed",
        http_status=resp.status_code,
        error_code=error_code,
        error_message=(
            f"HTTP {resp.status_code} from {safe_url}; "
            f"body excerpt: {safe_excerpt!r}"
        ),
    )


# ──────────────────────────────────────────────────────────────────────
# Upstream models fetch (used by /models/refresh route)
# ──────────────────────────────────────────────────────────────────────


class UpstreamModelFetchError(Exception):
    """Upstream `/models` listing call failed or returned an unparseable
    shape. Includes the http_status when one was received.

    ``error_code`` is a stable operator-facing classifier (e.g.
    ``upstream_non_json``) so the SPA can render a typed message without
    embedding raw upstream HTML bodies.
    """

    def __init__(
        self,
        message: str,
        *,
        http_status: int | None = None,
        error_code: str = "upstream_models_fetch_failed",
    ) -> None:
        super().__init__(message)
        self.http_status = http_status
        self.error_code = error_code


def _parse_upstream_models(
    provider_type: str, payload: object,
) -> list[str]:
    """Map a provider's /models response body to a flat list of model
    ids. Shapes covered:

    - openai-compatible / custom / anthropic: `{"data": [{"id": "..."}]}`
      (Anthropic's /models endpoint matches the OpenAI shape since 2024)
    - google: `{"models": [{"name": "models/<id>"}]}` — strip the
      `models/` prefix (Google's convention; the user-visible id is
      the suffix).

    Unknown shapes raise UpstreamModelFetchError so the route returns
    a 502-ish error to the operator rather than a silently-empty cache.
    """
    if not isinstance(payload, dict):
        raise UpstreamModelFetchError(
            "Models unavailable for this connection: upstream /models "
            "returned a malformed catalog.",
            error_code="upstream_malformed_catalog",
        )
    ids: list[str] = []
    if provider_type == "google":
        raw_list = payload.get("models")
        if not isinstance(raw_list, list):
            raise UpstreamModelFetchError(
                "Models unavailable for this connection: upstream /models "
                "catalog is missing a models list.",
                error_code="upstream_malformed_catalog",
            )
        for entry in raw_list:
            if not isinstance(entry, dict):
                continue
            name = entry.get("name")
            if not isinstance(name, str):
                continue
            # Google returns `name: "models/<id>"`. Strip the prefix
            # so the stored id matches what users type in --model.
            ids.append(name.removeprefix("models/"))
    else:
        # openai-compatible / custom / anthropic
        raw_list = payload.get("data")
        if not isinstance(raw_list, list):
            raise UpstreamModelFetchError(
                "Models unavailable for this connection: upstream /models "
                "catalog is missing a data list.",
                error_code="upstream_malformed_catalog",
            )
        for entry in raw_list:
            if not isinstance(entry, dict):
                continue
            mid = entry.get("id")
            if isinstance(mid, str) and mid:
                ids.append(mid)
    if not ids:
        raise UpstreamModelFetchError(
            "Models unavailable for this connection: upstream /models "
            "returned no parseable model entries.",
            error_code="upstream_malformed_catalog",
        )
    # Dedup preserving order — operators see the upstream's preferred
    # ordering, but the same id repeated would be a UNIQUE-violation
    # at upsert.
    seen: set[str] = set()
    out: list[str] = []
    for mid in ids:
        if mid not in seen:
            seen.add(mid)
            out.append(mid)
    return out


async def fetch_upstream_models(
    provider_type: str, base_url: str, api_key: str,
    *, _client_factory: object = None,
) -> list[str]:
    """Issue the same `/models` GET that probe_connection uses, then
    parse the response body into a flat list of model ids.

    Raises UpstreamModelFetchError on network errors, non-2xx
    responses, or unparseable bodies. Successful return is always
    a non-empty list (parser raises if zero ids).
    """
    import httpx

    url, headers = _probe_url_and_headers(provider_type, base_url, api_key)
    if _client_factory is None:
        client_cm = httpx.AsyncClient(
            timeout=_PROBE_TIMEOUT_SEC, follow_redirects=False,
        )
    else:
        client_cm = _client_factory()  # type: ignore[operator]

    try:
        async with client_cm as client:
            try:
                resp = await client.get(url, headers=headers)
            except httpx.TimeoutException as e:
                raise UpstreamModelFetchError(
                    f"Models unavailable for this connection: upstream "
                    f"/models timed out after {_PROBE_TIMEOUT_SEC}s.",
                    error_code="upstream_timeout",
                ) from e
            except httpx.RequestError as e:
                raise UpstreamModelFetchError(
                    "Models unavailable for this connection: could not "
                    "reach upstream /models.",
                    error_code="upstream_transport",
                ) from e
    except UpstreamModelFetchError:
        raise
    except Exception as e:
        raise UpstreamModelFetchError(
            "Models unavailable for this connection: upstream /models "
            "fetch failed.",
            error_code="upstream_models_fetch_failed",
        ) from e

    if not (200 <= resp.status_code < 300):
        excerpt = (resp.text or "")[:200]
        # Same redaction rationale as probe_connection — this string
        # surfaces via the /models/refresh 502 detail body which the
        # CLI prints back to operators.
        safe_url = _redact_secret(url, api_key)
        safe_excerpt = _redact_secret(excerpt, api_key)
        raise UpstreamModelFetchError(
            f"Models unavailable for this connection: upstream /models "
            f"returned HTTP {resp.status_code} from {safe_url}; "
            f"body excerpt: {safe_excerpt!r}",
            http_status=resp.status_code,
            error_code="upstream_http_error",
        )
    try:
        payload = resp.json()
    except ValueError as e:
        # Do not embed the raw body or parser text — yibuapi (and similar)
        # may return HTML with HTTP 200 (#918).
        raise UpstreamModelFetchError(
            "Models unavailable for this connection: upstream returned a "
            "non-JSON /models response. Add a model manually if you know "
            "the ID.",
            http_status=resp.status_code,
            error_code="upstream_non_json",
        ) from e
    return _parse_upstream_models(provider_type, payload)

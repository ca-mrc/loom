"""Unit tests for the provider-connection service layer.

These cover the pure-function pieces (URL parsing, IP classification,
model probing). The DNS resolution + route integration tests live
in tests/integration/test_provider_connections_routes.py and need a
real Postgres.
"""

from __future__ import annotations

import asyncio
import ipaddress
import json
import socket

import pytest

from loom_service.provider_connections_service import (
    InvalidBaseUrlError,
    ResolvedUpstream,
    SsrfRejectedError,
    _preflight_base_path_headers_body,
    classify_ip,
    derive_upstream_host,
    resolve_and_validate,
)

# ──────────────────────────────────────────────────────────────────────
# derive_upstream_host
# ──────────────────────────────────────────────────────────────────────


def test_derive_upstream_host_happy_https() -> None:
    assert derive_upstream_host("https://api.openai.com/v1") == "api.openai.com"


def test_derive_upstream_host_happy_http() -> None:
    assert derive_upstream_host("http://vllm.lab.local:8000/v1") == "vllm.lab.local"


def test_derive_upstream_host_strips_port_and_path() -> None:
    assert derive_upstream_host("https://gw.example.com:8443/openai/v1/chat") == "gw.example.com"


def test_derive_upstream_host_rejects_empty() -> None:
    with pytest.raises(InvalidBaseUrlError, match="non-empty"):
        derive_upstream_host("")


def test_derive_upstream_host_rejects_non_http_scheme() -> None:
    with pytest.raises(InvalidBaseUrlError, match="scheme must be http"):
        derive_upstream_host("ftp://example.com/")


def test_derive_upstream_host_rejects_missing_scheme() -> None:
    with pytest.raises(InvalidBaseUrlError, match="scheme must be http"):
        derive_upstream_host("api.openai.com/v1")


def test_derive_upstream_host_rejects_missing_host() -> None:
    with pytest.raises(InvalidBaseUrlError, match="has no hostname"):
        derive_upstream_host("https:///path-only")


# ──────────────────────────────────────────────────────────────────────
# classify_ip — IPv4
# ──────────────────────────────────────────────────────────────────────


@pytest.mark.parametrize("ip_str,allow_private", [
    ("127.0.0.1", False),
    ("127.0.0.1", True),       # loopback rejected even with flag on
    ("127.255.255.255", True),
    ("169.254.169.254", False),  # AWS / GCP metadata IP
    ("169.254.169.254", True),
    ("0.0.0.0", False),
    ("0.1.2.3", True),
    ("224.0.0.1", True),       # multicast
    ("255.255.255.255", True), # broadcast (in 240.0.0.0/4)
])
def test_classify_ip_v4_unconditionally_rejected(
    ip_str: str, allow_private: bool,
) -> None:
    ip = ipaddress.IPv4Address(ip_str)
    reason = classify_ip(ip, allow_private=allow_private)
    assert reason is not None, f"{ip_str} should be rejected"
    assert "never legitimate" in reason


@pytest.mark.parametrize("ip_str", [
    "10.0.0.1",        # RFC1918 /8
    "10.255.255.255",
    "172.16.0.1",      # RFC1918 /12
    "172.31.255.255",
    "192.168.0.1",     # RFC1918 /16
    "192.168.1.100",
    "100.64.0.1",      # CGNAT /10
])
def test_classify_ip_v4_private_rejected_without_flag(ip_str: str) -> None:
    ip = ipaddress.IPv4Address(ip_str)
    reason = classify_ip(ip, allow_private=False)
    assert reason is not None
    assert "private range" in reason
    assert "allow_private_endpoints" in reason


@pytest.mark.parametrize("ip_str", [
    "10.0.0.1",
    "172.16.0.1",
    "192.168.1.100",
    "100.64.0.1",
])
def test_classify_ip_v4_private_allowed_with_flag(ip_str: str) -> None:
    ip = ipaddress.IPv4Address(ip_str)
    assert classify_ip(ip, allow_private=True) is None


@pytest.mark.parametrize("ip_str", [
    "1.1.1.1",
    "8.8.8.8",
    "104.18.0.1",      # cloudflare-fronted
    "199.7.83.42",
])
def test_classify_ip_v4_public_always_allowed(ip_str: str) -> None:
    ip = ipaddress.IPv4Address(ip_str)
    assert classify_ip(ip, allow_private=False) is None
    assert classify_ip(ip, allow_private=True) is None


# ──────────────────────────────────────────────────────────────────────
# classify_ip — IPv6
# ──────────────────────────────────────────────────────────────────────


@pytest.mark.parametrize("ip_str,allow_private", [
    ("::1", False),
    ("::1", True),
    ("fe80::1", False),
    ("fe80::1", True),
    ("ff02::1", True),
])
def test_classify_ip_v6_unconditionally_rejected(
    ip_str: str, allow_private: bool,
) -> None:
    ip = ipaddress.IPv6Address(ip_str)
    reason = classify_ip(ip, allow_private=allow_private)
    assert reason is not None
    assert "never legitimate" in reason


@pytest.mark.parametrize("ip_str", [
    "fc00::1",         # ULA
    "fdab:cd00::1",    # ULA
])
def test_classify_ip_v6_ula_rejected_without_flag(ip_str: str) -> None:
    ip = ipaddress.IPv6Address(ip_str)
    reason = classify_ip(ip, allow_private=False)
    assert reason is not None
    assert "private range" in reason


def test_classify_ip_v6_ula_allowed_with_flag() -> None:
    assert classify_ip(
        ipaddress.IPv6Address("fc00::1"), allow_private=True,
    ) is None


def test_classify_ip_v6_public_always_allowed() -> None:
    # 2606:4700:4700::1111 = cloudflare DNS
    ip = ipaddress.IPv6Address("2606:4700:4700::1111")
    assert classify_ip(ip, allow_private=False) is None


# ──────────────────────────────────────────────────────────────────────
# resolve_and_validate — uses _resolver override
# ──────────────────────────────────────────────────────────────────────


def _fake_resolver_returning(*ips: str):
    """Build a getaddrinfo-shaped fake resolver from a list of IP strings."""
    def _resolver(host, port, type=None):
        infos = []
        for ip in ips:
            if ":" in ip:
                infos.append((socket.AF_INET6, socket.SOCK_STREAM, 0, "", (ip, 0, 0, 0)))
            else:
                infos.append((socket.AF_INET, socket.SOCK_STREAM, 0, "", (ip, 0)))
        return infos
    return _resolver


def test_resolve_and_validate_happy_public_ipv4() -> None:
    result = resolve_and_validate(
        "https://api.openai.com/v1",
        allow_private=False,
        _resolver=_fake_resolver_returning("104.18.0.1", "104.18.0.2"),
    )
    assert isinstance(result, ResolvedUpstream)
    assert result.upstream_host == "api.openai.com"
    assert sorted(result.resolved_ips) == ["104.18.0.1", "104.18.0.2"]


def test_resolve_and_validate_happy_public_ipv6() -> None:
    result = resolve_and_validate(
        "https://api.openai.com/v1",
        allow_private=False,
        _resolver=_fake_resolver_returning("2606:4700:4700::1111"),
    )
    assert result.resolved_ips == ["2606:4700:4700::1111"]


def test_resolve_and_validate_deduplicates_repeated_ips() -> None:
    """getaddrinfo may return the same IP multiple times (TCP + UDP);
    even though we filter to SOCK_STREAM, the deduplication is
    defensive. Test asserts the contract."""
    result = resolve_and_validate(
        "https://api.example.com/",
        allow_private=False,
        _resolver=_fake_resolver_returning("1.1.1.1", "1.1.1.1", "2.2.2.2"),
    )
    assert sorted(result.resolved_ips) == ["1.1.1.1", "2.2.2.2"]


def test_resolve_and_validate_strips_ipv6_zone_id() -> None:
    """IPv6 results may include zone-id suffix; we strip before parsing
    so 'fe80::1%eth0' classifies correctly as link-local (not as a
    malformed address that bypasses checks)."""
    with pytest.raises(SsrfRejectedError):
        resolve_and_validate(
            "https://localhost.example/",
            allow_private=False,
            _resolver=_fake_resolver_returning("fe80::1%eth0"),
        )


def test_resolve_and_validate_rejects_mixed_public_private(
) -> None:
    """If ANY resolved IP is private (without flag), the whole URL is
    rejected — defense against split-DNS attacks where the public IP
    is resolved on the API side but private on the gateway side."""
    with pytest.raises(SsrfRejectedError, match="private range"):
        resolve_and_validate(
            "https://attacker.example/",
            allow_private=False,
            _resolver=_fake_resolver_returning("1.1.1.1", "10.0.0.1"),
        )


def test_resolve_and_validate_allows_private_with_flag(
) -> None:
    result = resolve_and_validate(
        "https://vllm.lab.local:8000/v1",
        allow_private=True,
        _resolver=_fake_resolver_returning("10.0.5.42"),
    )
    assert result.resolved_ips == ["10.0.5.42"]


def test_resolve_and_validate_still_rejects_loopback_with_flag(
) -> None:
    """allow_private opens RFC1918 + ULA only — loopback stays
    rejected because nothing legitimate runs as a provider on
    127.0.0.0/8 (vLLM-on-localhost case is single-box compose mode
    where the SDK dials gateway-on-bridge, not the host)."""
    with pytest.raises(SsrfRejectedError, match=r"127\.0\.0"):
        resolve_and_validate(
            "https://api.localhost/",
            allow_private=True,
            _resolver=_fake_resolver_returning("127.0.0.1"),
        )


def test_resolve_and_validate_dns_failure_raises(
) -> None:
    def _failing(host, port, type=None):
        raise socket.gaierror(-2, "Name or service not known")

    with pytest.raises(InvalidBaseUrlError, match="DNS resolution failed"):
        resolve_and_validate(
            "https://nonexistent.example/",
            allow_private=False,
            _resolver=_failing,
        )


def test_resolve_and_validate_empty_resolution_raises(
) -> None:
    def _empty(host, port, type=None):
        return []

    with pytest.raises(InvalidBaseUrlError, match="returned no addresses"):
        resolve_and_validate(
            "https://api.example/",
            allow_private=False,
            _resolver=_empty,
        )


import httpx  # noqa: E402

import loom_service.provider_connections_service as pcs  # noqa: E402
from loom_service.provider_connections_service import (  # noqa: E402
    classify_preflight_failure,
    preflight_failure_kind,
    preflight_model,
    probe_connection,
)


def _client_factory(transport: httpx.MockTransport) -> object:
    """Return a callable producing an AsyncClient bound to the supplied
    MockTransport. Probe/fetch call it with no args; preflight passes a
    base_url so request methods can use relative endpoint paths."""

    def _factory(*, base_url: str | None = None) -> httpx.AsyncClient:
        if base_url is not None:
            return httpx.AsyncClient(base_url=base_url, transport=transport)
        return httpx.AsyncClient(transport=transport)

    return _factory


async def test_probe_openai_compatible_uses_bearer_and_models_path() -> None:
    captured: list[httpx.Request] = []

    def _handler(request: httpx.Request) -> httpx.Response:
        captured.append(request)
        return httpx.Response(
            200, json={"data": [{"id": "gpt-4o"}, {"id": "gpt-3.5"}]},
        )

    result = await probe_connection(
        "openai-compatible", "https://api.openai.com/v1", "sk-XYZ",
        _client_factory=_client_factory(httpx.MockTransport(_handler)),
    )
    assert result.status == "valid"
    assert result.http_status == 200
    assert result.error is None
    assert len(captured) == 1
    req = captured[0]
    assert req.method == "GET"
    assert str(req.url) == "https://api.openai.com/v1/models"
    assert req.headers["Authorization"] == "Bearer sk-XYZ"


async def test_probe_anthropic_uses_xapikey_and_version_header() -> None:
    captured: list[httpx.Request] = []

    def _handler(request: httpx.Request) -> httpx.Response:
        captured.append(request)
        return httpx.Response(200, json={"data": []})

    result = await probe_connection(
        "anthropic", "https://api.anthropic.com", "ant-XYZ",
        _client_factory=_client_factory(httpx.MockTransport(_handler)),
    )
    assert result.status == "valid"
    req = captured[0]
    assert str(req.url) == "https://api.anthropic.com/models"
    assert req.headers["x-api-key"] == "ant-XYZ"
    assert req.headers["anthropic-version"] == "2023-06-01"
    assert "Authorization" not in req.headers


async def test_probe_google_uses_query_string_api_key() -> None:
    captured: list[httpx.Request] = []

    def _handler(request: httpx.Request) -> httpx.Response:
        captured.append(request)
        return httpx.Response(200, json={"models": []})

    result = await probe_connection(
        "google", "https://generativelanguage.googleapis.com/v1beta", "g-XYZ",
        _client_factory=_client_factory(httpx.MockTransport(_handler)),
    )
    assert result.status == "valid"
    req = captured[0]
    assert "key=g-XYZ" in str(req.url)
    assert "/models" in str(req.url)


async def test_probe_custom_falls_back_to_openai_shape() -> None:
    captured: list[httpx.Request] = []

    def _handler(request: httpx.Request) -> httpx.Response:
        captured.append(request)
        return httpx.Response(200, json={})

    result = await probe_connection(
        "custom", "https://gw.example.com/v1", "k-XYZ",
        _client_factory=_client_factory(httpx.MockTransport(_handler)),
    )
    assert result.status == "valid"
    assert captured[0].headers["Authorization"] == "Bearer k-XYZ"


async def test_probe_401_marks_invalid_with_excerpt() -> None:
    def _handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            401, text='{"error": {"message": "Invalid API key"}}',
        )

    result = await probe_connection(
        "openai-compatible", "https://api.openai.com/v1", "wrong",
        _client_factory=_client_factory(httpx.MockTransport(_handler)),
    )
    assert result.status == "invalid"
    assert result.http_status == 401
    assert "HTTP 401" in (result.error or "")
    assert "Invalid API key" in (result.error or "")


async def test_probe_500_truncates_body_excerpt_to_200_chars() -> None:
    long_body = "x" * 2000

    def _handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500, text=long_body)

    result = await probe_connection(
        "openai-compatible", "https://api.openai.com/v1", "k",
        _client_factory=_client_factory(httpx.MockTransport(_handler)),
    )
    assert result.status == "invalid"
    # Repr-quoted excerpt → ≤ 200 chars of x's. Sanity-check the
    # truncation didn't pull the full 2000 chars.
    assert result.error is not None
    assert len(result.error) < 500


async def test_probe_timeout_returns_invalid_with_no_http_status() -> None:
    def _handler(request: httpx.Request) -> httpx.Response:
        raise httpx.TimeoutException("connect timeout")

    result = await probe_connection(
        "openai-compatible", "https://api.openai.com/v1", "k",
        _client_factory=_client_factory(httpx.MockTransport(_handler)),
    )
    assert result.status == "invalid"
    assert result.http_status is None
    assert "timeout" in (result.error or "").lower()


async def test_probe_connect_error_returns_invalid() -> None:
    def _handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("connection refused")

    result = await probe_connection(
        "openai-compatible", "https://api.openai.com/v1", "k",
        _client_factory=_client_factory(httpx.MockTransport(_handler)),
    )
    assert result.status == "invalid"
    assert result.http_status is None
    assert "ConnectError" in (result.error or "")


async def test_probe_strips_trailing_slash_in_base_url() -> None:
    captured: list[httpx.Request] = []

    def _handler(request: httpx.Request) -> httpx.Response:
        captured.append(request)
        return httpx.Response(200, json={})

    await probe_connection(
        "openai-compatible", "https://api.openai.com/v1/", "k",
        _client_factory=_client_factory(httpx.MockTransport(_handler)),
    )
    # No `//models` — single slash.
    assert str(captured[0].url) == "https://api.openai.com/v1/models"


# ──────────────────────────────────────────────────────────────────────
# preflight_model (uses httpx.MockTransport — no real network)
# ──────────────────────────────────────────────────────────────────────


def test_preflight_endpoint_uses_relative_path_under_validated_base() -> None:
    base, path, headers, body = _preflight_base_path_headers_body(
        "openai-compatible", "https://api.openai.com/v1/", "sk-XYZ",
        "gpt-4o-mini",
    )

    assert base == "https://api.openai.com/v1"
    assert path == "/chat/completions"
    assert headers == {"Authorization": "Bearer sk-XYZ"}
    assert body == {
        "model": "gpt-4o-mini",
        "messages": [{"role": "user", "content": "ping"}],
        "max_tokens": 1,
    }


async def test_preflight_openai_model_posts_minimal_chat_completion() -> None:
    captured: list[httpx.Request] = []

    async def _read_json(request: httpx.Request) -> dict[str, object]:
        return json.loads((await request.aread()).decode())

    async def _handler(request: httpx.Request) -> httpx.Response:
        captured.append(request)
        body = await _read_json(request)
        assert body == {
            "model": "gpt-4o-mini",
            "messages": [{"role": "user", "content": "ping"}],
            "max_tokens": 1,
        }
        return httpx.Response(200, json={"id": "chatcmpl-ok"})

    result = await preflight_model(
        "openai-compatible", "https://api.openai.com/v1", "sk-XYZ",
        "gpt-4o-mini",
        _client_factory=_client_factory(httpx.MockTransport(_handler)),
    )

    assert result.status == "valid"
    assert result.http_status == 200
    assert result.error_code is None
    assert result.error_message is None
    assert len(captured) == 1
    req = captured[0]
    assert req.method == "POST"
    assert str(req.url) == "https://api.openai.com/v1/chat/completions"
    assert req.headers["Authorization"] == "Bearer sk-XYZ"


async def test_preflight_openai_403_marks_access_denied_and_redacts_key() -> None:
    real_key = "sk-LIVE-model-preflight-secret"

    def _handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            403,
            text=f"model blocked for Authorization: Bearer {real_key}",
        )

    result = await preflight_model(
        "openai-compatible", "https://api.openai.com/v1", real_key,
        "gpt-private",
        _client_factory=_client_factory(httpx.MockTransport(_handler)),
    )

    assert result.status == "failed"
    assert result.http_status == 403
    assert result.error_code == "access-denied"
    assert result.error_message is not None
    assert "HTTP 403" in result.error_message
    assert real_key not in result.error_message
    assert "[REDACTED]" in result.error_message


async def test_preflight_timeout_is_recorded_as_inconclusive() -> None:
    def _handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ReadTimeout("read timeout")

    result = await preflight_model(
        "openai-compatible", "https://api.openai.com/v1", "sk-XYZ",
        "slow-model",
        _client_factory=_client_factory(httpx.MockTransport(_handler)),
    )

    assert result.status == "failed"
    assert result.error_code == "timeout"
    assert "timeout after 20.0s" in (result.error_message or "")
    assert classify_preflight_failure(
        result.error_code, result.http_status,
    ) == "inconclusive"


@pytest.mark.parametrize(
    ("status", "body", "kind"),
    [
        # An OpenAI-compatible relay with no channel for the model (#2054).
        (503, {"error": {"code": "model_not_found", "message": "No available channel"}}, "rejected"),
        (404, {"error": {"code": "model_not_found"}}, "rejected"),
        (503, {"error": {"code": "overloaded"}}, "inconclusive"),
        (503, "upstream unavailable", "inconclusive"),
    ],
)
async def test_preflight_unroutable_model_is_a_rejection_whatever_the_status(
    status: int, body: object, kind: str,
) -> None:
    def _handler(request: httpx.Request) -> httpx.Response:
        if isinstance(body, str):
            return httpx.Response(status, text=body)
        return httpx.Response(status, json=body)

    result = await preflight_model(
        "openai-compatible", "https://api.openai.com/v1", "sk-XYZ", "gpt-unrouted",
        _client_factory=_client_factory(httpx.MockTransport(_handler)),
    )

    assert result.status == "failed"
    assert result.http_status == status
    assert classify_preflight_failure(result.error_code, result.http_status) == kind


def _dripping_response(chunks: int, interval: float) -> httpx.Response:
    """200 whose body keeps making progress: one byte every `interval`s.

    Each chunk arrives well inside any per-read timeout, so only a total
    deadline can stop it.
    """

    class _Drip(httpx.AsyncByteStream):
        async def __aiter__(self):  # type: ignore[override]
            for _ in range(chunks):
                await asyncio.sleep(interval)
                yield b" "

    return httpx.Response(200, stream=_Drip())


async def test_preflight_total_deadline_stops_slow_dripping_response(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """#948 review: per-read timeouts alone let a dripping body run long."""
    monkeypatch.setattr(pcs, "_GENERATION_PROBE_TIMEOUT_SEC", 0.05)

    def _handler(request: httpx.Request) -> httpx.Response:
        return _dripping_response(chunks=40, interval=0.01)  # ~0.4s total

    loop = asyncio.get_running_loop()
    started = loop.time()
    result = await preflight_model(
        "openai-compatible", "https://api.openai.com/v1", "sk-XYZ",
        "drip-model",
        _client_factory=_client_factory(httpx.MockTransport(_handler)),
    )
    elapsed = loop.time() - started

    assert result.status == "failed"
    assert result.error_code == "timeout"
    assert "timeout after 0.05s" in (result.error_message or "")
    assert classify_preflight_failure(
        result.error_code, result.http_status,
    ) == "inconclusive"
    assert elapsed < 0.3


async def test_preflight_total_deadline_allows_response_within_budget(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(pcs, "_GENERATION_PROBE_TIMEOUT_SEC", 1.0)

    def _handler(request: httpx.Request) -> httpx.Response:
        return _dripping_response(chunks=3, interval=0.01)

    result = await preflight_model(
        "openai-compatible", "https://api.openai.com/v1", "sk-XYZ",
        "quick-model",
        _client_factory=_client_factory(httpx.MockTransport(_handler)),
    )

    assert result.status == "valid"
    assert result.http_status == 200


async def test_preflight_propagates_caller_cancellation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Caller cancellation must not be swallowed into a timeout result."""
    monkeypatch.setattr(pcs, "_GENERATION_PROBE_TIMEOUT_SEC", 5.0)

    def _handler(request: httpx.Request) -> httpx.Response:
        return _dripping_response(chunks=500, interval=0.01)

    task = asyncio.create_task(preflight_model(
        "openai-compatible", "https://api.openai.com/v1", "sk-XYZ",
        "slow-model",
        _client_factory=_client_factory(httpx.MockTransport(_handler)),
    ))
    await asyncio.sleep(0.05)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


@pytest.mark.parametrize(
    ("error_code", "http_status", "expected"),
    [
        # #948: probes that never got a definitive answer must not block.
        ("timeout", None, "inconclusive"),
        ("request-error", None, "inconclusive"),
        ("unexpected-error", None, "inconclusive"),
        ("upstream-http-error", 408, "inconclusive"),
        ("upstream-http-error", 429, "inconclusive"),
        ("upstream-http-error", 502, "inconclusive"),
        ("upstream-http-error", 503, "inconclusive"),
        # Genuine auth/model incompatibility stays fail-closed.
        ("access-denied", 401, "rejected"),
        ("access-denied", 403, "rejected"),
        ("upstream-http-error", 400, "rejected"),
        ("upstream-http-error", 404, "rejected"),
        ("egress-policy-rejected", None, "rejected"),
        # Unknown/legacy codes without a transient status: fail closed.
        ("upstream_404", None, "rejected"),
        (None, None, "rejected"),
    ],
)
def test_classify_preflight_failure(
    error_code: str | None, http_status: int | None, expected: str,
) -> None:
    assert classify_preflight_failure(error_code, http_status) == expected


def test_preflight_failure_kind_is_none_unless_failed() -> None:
    assert preflight_failure_kind(None, None, None) is None
    assert preflight_failure_kind("valid", None, 200) is None
    assert preflight_failure_kind("failed", "timeout", None) == "inconclusive"
    assert preflight_failure_kind("failed", "access-denied", 403) == "rejected"


# ──────────────────────────────────────────────────────────────────────
# fetch_upstream_models + _parse_upstream_models
# ──────────────────────────────────────────────────────────────────────


from loom_service.provider_connections_service import (  # noqa: E402
    UpstreamModelFetchError,
    _parse_upstream_models,
    fetch_upstream_models,
)


def test_parse_openai_compatible_extracts_data_id() -> None:
    body = {"data": [
        {"id": "gpt-4o", "object": "model"},
        {"id": "gpt-3.5-turbo"},
    ], "object": "list"}
    assert _parse_upstream_models("openai-compatible", body) == [
        "gpt-4o", "gpt-3.5-turbo",
    ]


def test_parse_anthropic_uses_same_data_id_shape() -> None:
    body = {"data": [
        {"id": "claude-opus-4-7"},
        {"id": "claude-sonnet-4-6"},
    ]}
    assert _parse_upstream_models("anthropic", body) == [
        "claude-opus-4-7", "claude-sonnet-4-6",
    ]


def test_parse_google_strips_models_prefix() -> None:
    body = {"models": [
        {"name": "models/gemini-2.5-pro"},
        {"name": "models/gemini-2.5-flash"},
    ]}
    assert _parse_upstream_models("google", body) == [
        "gemini-2.5-pro", "gemini-2.5-flash",
    ]


def test_parse_dedup_preserves_order() -> None:
    body = {"data": [{"id": "x"}, {"id": "y"}, {"id": "x"}]}
    assert _parse_upstream_models("openai-compatible", body) == ["x", "y"]


def test_parse_rejects_non_dict_body() -> None:
    with pytest.raises(UpstreamModelFetchError, match="malformed catalog"):
        _parse_upstream_models("openai-compatible", [{"id": "x"}])


def test_parse_rejects_missing_data_field() -> None:
    with pytest.raises(UpstreamModelFetchError, match="missing a data list"):
        _parse_upstream_models("openai-compatible", {"models": [{"id": "x"}]})


def test_parse_rejects_missing_models_field_for_google() -> None:
    with pytest.raises(UpstreamModelFetchError, match="missing a models list"):
        _parse_upstream_models("google", {"data": [{"name": "models/x"}]})


def test_parse_rejects_empty_result() -> None:
    """A 200 with no recognized entries → error so the cache doesn't
    silently get wiped on a parser-shape regression."""
    with pytest.raises(UpstreamModelFetchError, match="no parseable model"):
        _parse_upstream_models("openai-compatible", {"data": [{"foo": "bar"}]})


def test_parse_skips_non_string_ids() -> None:
    body = {"data": [{"id": "gpt-4o"}, {"id": 123}, {"id": None}]}
    assert _parse_upstream_models("openai-compatible", body) == ["gpt-4o"]


async def test_fetch_upstream_models_happy_path() -> None:
    def _handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"data": [
            {"id": "gpt-4o"}, {"id": "gpt-3.5-turbo"},
        ]})

    out = await fetch_upstream_models(
        "openai-compatible", "https://api.openai.com/v1", "k",
        _client_factory=_client_factory(httpx.MockTransport(_handler)),
    )
    assert out == ["gpt-4o", "gpt-3.5-turbo"]


async def test_fetch_upstream_models_401_raises_with_status() -> None:
    def _handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(401, json={"error": "bad key"})

    with pytest.raises(UpstreamModelFetchError) as exc:
        await fetch_upstream_models(
            "openai-compatible", "https://api.openai.com/v1", "k",
            _client_factory=_client_factory(httpx.MockTransport(_handler)),
        )
    assert exc.value.http_status == 401
    assert "HTTP 401" in str(exc.value)


async def test_fetch_upstream_models_timeout() -> None:
    def _handler(request: httpx.Request) -> httpx.Response:
        raise httpx.TimeoutException("connect timeout")

    with pytest.raises(UpstreamModelFetchError, match="timed out") as exc:
        await fetch_upstream_models(
            "openai-compatible", "https://api.openai.com/v1", "k",
            _client_factory=_client_factory(httpx.MockTransport(_handler)),
        )
    assert exc.value.error_code == "upstream_timeout"


async def test_fetch_upstream_models_non_json_body() -> None:
    def _handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, text="<html>nginx</html>")

    with pytest.raises(UpstreamModelFetchError, match="non-JSON") as exc:
        await fetch_upstream_models(
            "openai-compatible", "https://api.openai.com/v1", "k",
            _client_factory=_client_factory(httpx.MockTransport(_handler)),
        )
    assert exc.value.http_status == 200
    assert exc.value.error_code == "upstream_non_json"
    # Must not leak upstream HTML into the operator-facing message.
    assert "<html>" not in str(exc.value)


# ──────────────────────────────────────────────────────────────────────
# api_key redaction — regression guard for the audit fix
# ──────────────────────────────────────────────────────────────────────


async def test_probe_redacts_api_key_in_error_excerpt() -> None:
    """Some upstream debug pages echo the Authorization header in the
    body. The error string is persisted to `last_validation_error`
    (readable via GET /provider-connections/{id}); it MUST NOT contain
    the api_key."""
    real_key = "sk-LIVE-supersecret123456"

    def _handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            401,
            text=(
                "Unauthorized. You sent Authorization: Bearer "
                f"{real_key}"
            ),
        )

    result = await probe_connection(
        "openai-compatible", "https://api.openai.com/v1", real_key,
        _client_factory=_client_factory(httpx.MockTransport(_handler)),
    )
    assert result.status == "invalid"
    assert result.error is not None
    assert real_key not in result.error
    assert "[REDACTED]" in result.error


async def test_probe_redacts_api_key_when_echoed_in_url() -> None:
    """Google's `?key=<API_KEY>` style — the URL itself contains the
    secret and lands in the error string. Redaction must scrub it too."""
    real_key = "AIza-supersecret-google-key-1234"

    def _handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(403, text="invalid api key")

    result = await probe_connection(
        "google", "https://generativelanguage.googleapis.com/v1beta",
        real_key,
        _client_factory=_client_factory(httpx.MockTransport(_handler)),
    )
    assert result.status == "invalid"
    assert result.error is not None
    assert real_key not in result.error
    assert "[REDACTED]" in result.error


async def test_probe_redacts_signed_urls_and_internal_endpoints() -> None:
    def _handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            500,
            text=(
                "debug artifact=http://minio.internal:9000/a/b?"
                "X-Amz-Signature=abc123 via "
                "http://loom-control-plane:8080/trials"
            ),
        )

    result = await probe_connection(
        "openai-compatible", "https://api.openai.com/v1", "sk-test-key",
        _client_factory=_client_factory(httpx.MockTransport(_handler)),
    )

    assert result.status == "invalid"
    assert result.error is not None
    assert "minio.internal" not in result.error
    assert "X-Amz-Signature=abc123" not in result.error
    assert "loom-control-plane" not in result.error
    assert "[REDACTED" in result.error


async def test_fetch_upstream_models_redacts_api_key_in_502_detail() -> None:
    """The 502 raised on non-2xx surfaces via the route's HTTPException
    detail to operators — same redaction requirement as probe."""
    real_key = "sk-LIVE-fetch-supersecret789"

    def _handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            401, text=f"Bad key={real_key}",
        )

    with pytest.raises(UpstreamModelFetchError) as exc:
        await fetch_upstream_models(
            "openai-compatible", "https://api.openai.com/v1", real_key,
            _client_factory=_client_factory(httpx.MockTransport(_handler)),
        )
    msg = str(exc.value)
    assert real_key not in msg
    assert "[REDACTED]" in msg


async def test_fetch_upstream_models_redacts_signed_urls_and_internal_endpoints() -> None:
    def _handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            502,
            text=(
                "cache=http://minio:9000/models?"
                "X-Amz-Credential=minio via http://llm-gateway:9100"
            ),
        )

    with pytest.raises(UpstreamModelFetchError) as exc:
        await fetch_upstream_models(
            "openai-compatible", "https://api.openai.com/v1", "sk-test-key",
            _client_factory=_client_factory(httpx.MockTransport(_handler)),
        )

    msg = str(exc.value)
    assert "minio:9000" not in msg
    assert "X-Amz-Credential=minio" not in msg
    assert "llm-gateway" not in msg
    assert "[REDACTED" in msg


def test_redact_secret_helper_minimum_length_guard() -> None:
    """A 1-3 char "secret" should NOT trigger redaction — that would
    over-redact common substrings ("a", "b", "to"). 4-char minimum
    keeps the helper safe with degenerate inputs."""
    from loom_service.provider_connections_service import _redact_secret
    assert _redact_secret("hello world", "") == "hello world"
    assert _redact_secret("hello world", "ab") == "hello world"
    assert _redact_secret("hello world", "abc") == "hello world"
    assert _redact_secret("hello world", "world") == "hello [REDACTED]"


def test_redact_secret_helper_multiple_secrets() -> None:
    from loom_service.provider_connections_service import _redact_secret
    out = _redact_secret(
        "key=sk-AAA url=sk-BBB",
        "sk-AAA", "sk-BBB",
    )
    assert "sk-AAA" not in out
    assert "sk-BBB" not in out
    assert out == "key=[REDACTED] url=[REDACTED]"

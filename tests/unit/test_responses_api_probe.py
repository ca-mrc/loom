"""Tests for `loom_llm_gateway.responses_probe`.

Two exported entry points:

- `classify_probe_status(status_code)` — pure classifier. 200/400/401 →
  supported; 404/501/5xx → unsupported; other 4xx → unknown (surfaces
  to caller as "keep the existing behaviour, don't dispatch to translator").
- `probe_responses_api(upstream_url, api_key, transport)` — runs one
  outbound POST with a sentinel-model JSON body; returns
  `(supported: bool | None, error_detail: str | None)`.

Reference: docs/architecture/responses-api.md
"""
from __future__ import annotations

from uuid import uuid4

import httpx
import pytest

from loom_llm_gateway.attempt_deadline import (
    AttemptDeadlineReachedError,
    GatewayAttemptDeadline,
)
from loom_llm_gateway.responses_probe import (
    ProbeBackoff,
    ProbeOutcome,
    classify_probe_status,
    probe_responses_api,
)


@pytest.mark.parametrize("code", [200, 400, 401])
def test_classify_supported_codes(code: int) -> None:
    assert classify_probe_status(code) is True


@pytest.mark.parametrize("code", [404, 501])
def test_classify_unsupported_codes(code: int) -> None:
    assert classify_probe_status(code) is False


@pytest.mark.parametrize("code", [403, 429, 422, 500, 502, 503, 504])
def test_classify_ambiguous_codes_return_none(code: int) -> None:
    """4xx that isn't 400/401/404 — the endpoint exists but our probe
    can't tell whether it's a real Responses handler or a proxy that
    rejected the request for policy reasons. Leave as None so the
    caller decides whether to retry or fall back."""
    assert classify_probe_status(code) is None


def _mock_transport(status: int) -> httpx.MockTransport:
    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(status, json={})
    return httpx.MockTransport(handler)


def _raising_transport(exc: type[BaseException]) -> httpx.MockTransport:
    async def handler(request: httpx.Request) -> httpx.Response:
        raise exc("simulated")
    return httpx.MockTransport(handler)


async def test_probe_returns_supported_on_400() -> None:
    outcome = await probe_responses_api(
        upstream_url="http://mock.example/v1/responses",
        api_key="tok",
        transport=_mock_transport(400),
    )
    assert outcome == ProbeOutcome(supported=True, error_detail=None)


@pytest.mark.parametrize("code", [500, 502, 503, 504])
async def test_probe_reports_5xx_as_transient(code: int) -> None:
    outcome = await probe_responses_api(
        upstream_url="http://mock.example/v1/responses",
        api_key="tok",
        transport=_mock_transport(code),
    )
    assert outcome == ProbeOutcome(
        supported=None, error_detail=f"upstream_{code}", transient=True,
    )


async def test_probe_returns_unsupported_on_501() -> None:
    outcome = await probe_responses_api(
        upstream_url="http://mock.example/v1/responses",
        api_key="tok",
        transport=_mock_transport(501),
    )
    assert outcome == ProbeOutcome(supported=False, error_detail="upstream_501")


async def test_probe_reports_timeout_as_transient() -> None:
    outcome = await probe_responses_api(
        upstream_url="http://mock.example/v1/responses",
        api_key="tok",
        transport=_raising_transport(httpx.ReadTimeout),
    )
    assert outcome.supported is None
    assert outcome.transient is True
    assert outcome.error_detail is not None
    assert "timeout" in outcome.error_detail.lower()


async def test_probe_reports_connect_error_as_transient() -> None:
    outcome = await probe_responses_api(
        upstream_url="http://mock.example/v1/responses",
        api_key="tok",
        transport=_raising_transport(httpx.ConnectError),
    )
    assert outcome.supported is None
    assert outcome.transient is True
    assert outcome.error_detail is not None


async def test_probe_returns_none_on_ambiguous_4xx() -> None:
    outcome = await probe_responses_api(
        upstream_url="http://mock.example/v1/responses",
        api_key="tok",
        transport=_mock_transport(403),
    )
    assert outcome.supported is None
    assert outcome.transient is False
    assert outcome.error_detail == "ambiguous_403"


async def test_probe_posts_sentinel_model_body_to_upstream() -> None:
    import json

    captured: dict[str, object] = {}

    async def handler(request: httpx.Request) -> httpx.Response:
        captured["method"] = request.method
        captured["url"] = str(request.url)
        captured["auth"] = request.headers.get("Authorization")
        captured["body"] = request.content
        return httpx.Response(400)

    transport = httpx.MockTransport(handler)
    await probe_responses_api(
        upstream_url="http://mock.example/v1/responses",
        api_key="tok-xyz",
        transport=transport,
    )
    assert captured["method"] == "POST"
    assert captured["url"] == "http://mock.example/v1/responses"
    assert captured["auth"] == "Bearer tok-xyz"
    # Body carries a sentinel model + minimal input. Not empty — an
    # empty body would let reverse-proxied endpoints 400 at the edge
    # without ever attempting to route (which is what we're trying to
    # test with the probe).
    body = json.loads(captured["body"])  # type: ignore[arg-type]
    assert "model" in body
    assert body["model"].startswith("loom-probe")
    assert body.get("input") is not None
    assert body.get("max_output_tokens") == 1


async def test_probe_caps_all_httpx_phases_to_attempt_remaining() -> None:
    captured: dict[str, object] = {}

    async def handler(request: httpx.Request) -> httpx.Response:
        captured.update(request.extensions["timeout"])
        return httpx.Response(400)

    deadline = GatewayAttemptDeadline(2.0, clock=lambda: 0.0)
    outcome = await probe_responses_api(
        upstream_url="http://mock.example/v1/responses",
        api_key="tok",
        transport=httpx.MockTransport(handler),
        deadline=deadline,
    )

    assert outcome.supported is True
    assert captured == {
        "connect": 2.0,
        "read": 2.0,
        "write": 2.0,
        "pool": 2.0,
    }


async def test_expired_probe_deadline_dispatches_zero_times() -> None:
    calls = 0

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(400)

    with pytest.raises(AttemptDeadlineReachedError):
        await probe_responses_api(
            upstream_url="http://mock.example/v1/responses",
            api_key="tok",
            transport=httpx.MockTransport(handler),
            deadline=GatewayAttemptDeadline(0.0, clock=lambda: 0.0),
        )
    assert calls == 0


def test_probe_backoff_expires_and_clears() -> None:
    now = [100.0]
    backoff = ProbeBackoff(clock=lambda: now[0])
    connection_id = uuid4()

    assert backoff.active(connection_id) is False
    backoff.record(connection_id)
    assert backoff.active(connection_id) is True
    now[0] += 59
    assert backoff.active(connection_id) is True
    now[0] += 2
    assert backoff.active(connection_id) is False

    backoff.record(connection_id)
    backoff.clear(connection_id)
    assert backoff.active(connection_id) is False

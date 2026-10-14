"""Real provider clients belong to one run and close on every exit path."""

from __future__ import annotations

from typing import Any

import httpx
import pytest
from google import genai

from loom_cli.run_cmd import _build_sdk_clients


@pytest.mark.parametrize("fail_run", [False, True])
async def test_google_sdk_construction_and_cleanup(
    monkeypatch: pytest.MonkeyPatch, fail_run: bool,
) -> None:
    # A local API key must select the Gemini Developer API even when the shell
    # was previously configured for Vertex. Keep the actual SDK constructor.
    monkeypatch.setenv("GOOGLE_GENAI_USE_VERTEXAI", "true")
    real_client = genai.Client
    sync_http = httpx.Client(transport=httpx.MockTransport(lambda _: httpx.Response(200)))
    async_http = httpx.AsyncClient(transport=httpx.MockTransport(lambda _: httpx.Response(200)))

    def construct(**kwargs: Any) -> genai.Client:
        return real_client(
            **kwargs,
            http_options={"httpx_client": sync_http, "httpx_async_client": async_http},
        )

    monkeypatch.setattr(genai, "Client", construct)
    try:
        try:
            async with _build_sdk_clients({"google": "synthetic-google-key"}) as clients:
                anthropic_client, openai_client, google_client = clients
                assert anthropic_client is None
                assert openai_client is None
                assert isinstance(google_client, real_client)
                assert google_client.vertexai is False
                assert not sync_http.is_closed
                assert not async_http.is_closed
                if fail_run:
                    raise RuntimeError("synthetic run failure")
        except RuntimeError as exc:
            assert fail_run and str(exc) == "synthetic run failure"
        assert sync_http.is_closed
        assert async_http.is_closed
    finally:
        await async_http.aclose()
        sync_http.close()


async def test_missing_credentials_do_not_construct_clients() -> None:
    async with _build_sdk_clients({}) as clients:
        assert clients == (None, None, None)


async def test_partial_construction_failure_closes_prior_client(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import anthropic

    http = httpx.AsyncClient(transport=httpx.MockTransport(lambda _: httpx.Response(200)))
    real_anthropic = anthropic.AsyncAnthropic

    def construct_anthropic(**kwargs: Any) -> anthropic.AsyncAnthropic:
        return real_anthropic(**kwargs, http_client=http)

    def fail_google(**kwargs: Any) -> None:
        raise RuntimeError("synthetic construction failure")

    monkeypatch.setattr(anthropic, "AsyncAnthropic", construct_anthropic)
    monkeypatch.setattr(genai, "Client", fail_google)
    try:
        with pytest.raises(RuntimeError, match="construction failure"):
            async with _build_sdk_clients({"anthropic": "fake-ant", "google": "fake-g"}):
                pytest.fail("client construction should have failed")
        assert http.is_closed
    finally:
        await http.aclose()

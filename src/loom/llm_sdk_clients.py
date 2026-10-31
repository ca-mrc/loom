"""Explicit provider SDK clients over owned, unbounded HTTP/1.1 transports."""

from __future__ import annotations

import asyncio
import os
from collections.abc import Awaitable, Callable, Iterable
from typing import Any

import httpx
from litellm.llms.custom_httpx.http_handler import AsyncHTTPHandler

from loom.llm_http_client import create_model_http_client


async def _close_all(closes: Iterable[Awaitable[None]]) -> None:
    results = await asyncio.gather(*closes, return_exceptions=True)
    errors = [result for result in results if isinstance(result, BaseException)]
    if len(errors) == 1:
        raise errors[0]
    if errors:
        raise BaseExceptionGroup("model transport cleanup failed", errors)


class _ModelHTTPHandler(AsyncHTTPHandler):
    """Use LiteLLM's public generic-provider client hook with an owned transport."""

    def __init__(self, factory: Callable[[], httpx.AsyncClient], *, timeout_sec: float) -> None:
        self._model_factory = factory
        self._transports: set[httpx.AsyncClient] = set()
        self._closed = False
        super().__init__(timeout=timeout_sec)

    def create_client(self, *args: Any, **kwargs: Any) -> httpx.AsyncClient:
        # LiteLLM creates and closes a temporary client on connection/protocol
        # errors. It must not close the handler's shared primary transport.
        if self._closed:
            raise RuntimeError("model HTTP handler is closed")
        self._transports = {client for client in self._transports if not client.is_closed}
        client = self._model_factory()
        self._transports.add(client)
        if kwargs.get("timeout") is not None:
            client.timeout = kwargs["timeout"]
        if kwargs.get("event_hooks") is not None:
            client.event_hooks = kwargs["event_hooks"]
        return client

    async def close(self) -> None:
        self._closed = True
        transports = list(self._transports)
        self._transports.clear()
        await _close_all(client.aclose() for client in transports)


class ModelSDKClients:
    """Own explicit SDK clients for one app or agent attempt, never global SDK state.

    LiteLLM's default aiohttp transport caps total/per-host connections at
    1000/500; disabling it falls back to HTTPX's 100. Supported ``client=``
    injection bypasses both without changing retry or request timeout policy.
    Separate credential/base identities retain separate SDK and cookie state.
    """

    def __init__(self, *, timeout_sec: float) -> None:
        self.timeout_sec = timeout_sec
        self._clients: dict[tuple[str, str | None, str | None], Any] = {}
        self._closed = False

    def client_for(self, *, model: str, api_key: str | None, api_base: str | None) -> Any:
        import litellm
        from litellm.llms.custom_httpx.http_handler import (
            get_default_headers,
            get_ssl_configuration,
        )
        from litellm.secret_managers.main import str_to_bool
        from openai import AsyncOpenAI

        if self._closed:
            raise RuntimeError("model SDK clients are closed")
        _, provider, resolved_key, resolved_base = litellm.get_llm_provider(
            model=model, api_key=api_key, api_base=api_base,
        )
        generic = provider in {"anthropic", "gemini", "vertex_ai"}
        if not generic:
            api_base = (api_base or resolved_base or litellm.api_base
                or litellm.get_secret("OPENAI_BASE_URL") or litellm.get_secret("OPENAI_API_BASE")
                or "https://api.openai.com/v1")
            api_key = (api_key or resolved_key or litellm.api_key or litellm.openai_key
                or litellm.get_secret("OPENAI_API_KEY"))
        identity = provider, api_key, api_base
        cached = self._clients.get(identity)
        if cached is not None:
            # LiteLLM mutates OpenAI retry/organization options on an injected
            # client. Per-call views share transport, never mutable call policy.
            return cached.with_options() if not generic else cached
        # Match the selected SDK transport's existing environment-proxy policy.
        trust_env = (not AsyncHTTPHandler._should_use_aiohttp_transport()
            or bool(litellm.aiohttp_trust_env)
            or str_to_bool(os.environ.get("AIOHTTP_TRUST_ENV", "False")) is True)
        transport_options: dict[str, Any] = dict(verify=get_ssl_configuration(),
            cert=os.environ.get("SSL_CERTIFICATE", litellm.ssl_certificate),
            follow_redirects=True, trust_env=trust_env, headers=get_default_headers())

        def transport_factory() -> httpx.AsyncClient:
            if self._closed:
                raise RuntimeError("model SDK clients are closed")
            return create_model_http_client(timeout_sec=self.timeout_sec, **transport_options)

        client = (_ModelHTTPHandler(transport_factory, timeout_sec=self.timeout_sec) if generic else AsyncOpenAI(
            api_key=api_key, base_url=api_base, http_client=transport_factory(),
        ))
        self._clients[identity] = client
        return client if isinstance(client, _ModelHTTPHandler) else client.with_options()

    async def aclose(self) -> None:
        self._closed = True
        clients = list(self._clients.values())
        self._clients.clear()
        await _close_all(client.close() for client in clients)

    async def __aenter__(self) -> ModelSDKClients:
        return self

    async def __aexit__(self, *args: Any) -> None:
        await self.aclose()

"""Owned HTTP/1.1 model transports without provider-key admission queues."""

from __future__ import annotations

from typing import Any

import httpx


def create_model_http_client(
    *, timeout_sec: float | httpx.Timeout | None, base_url: str = "", proxy: httpx.Proxy | None = None,
    **kwargs: Any,
) -> httpx.AsyncClient:
    """Bound idle reuse only; active calls never wait for an SDK pool slot."""
    return httpx.AsyncClient(
        base_url=base_url,
        timeout=timeout_sec,
        proxy=proxy,
        limits=httpx.Limits(max_connections=None, max_keepalive_connections=20),
        **kwargs,
    )

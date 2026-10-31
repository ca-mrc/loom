"""Per-connection httpx client pool for the egress-proxy chain (#190 PR-C2).

When `LOOM_GW_EGRESS_PROXY_URL` is set, every upstream HTTP call
from this gateway must route through the Envoy egress proxy on that
URL AND carry `x-loom-connection-id: <connection_id>` on the
proxy request. HTTPS upstreams carry it on the CONNECT request; HTTP
upstreams carry it on the forward-proxy request, and Envoy strips it
before forwarding upstream.

httpx exposes proxy-header injection via `httpx.Proxy(headers=...)`,
which is per-Proxy = per-client. So we maintain a small pool keyed on
`connection_id`, lazily building a client per connection and reusing it
for all of that connection's requests. The pool is bounded by the
number of `provider_connections` rows (≤ team count × per-team
connections, currently <1k in practice), so no LRU eviction is needed.

When the env var is empty (default), `get_egress_client_for` falls
through to the shared `upstream_client` and no per-connection
clients are built. Lets operators flip the proxy off via env without
a code change.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

import httpx

if TYPE_CHECKING:
    from uuid import UUID

# Header Envoy's RDS routes match on. Must equal
# `loom_egress_xds.envoy_translator.CONNECTION_ID_HEADER`.
_CONNECTION_ID_HEADER = "x-loom-connection-id"


def create_upstream_client(
    *, timeout_sec: float, proxy: httpx.Proxy | None = None,
) -> httpx.AsyncClient:
    """Keep idle reuse bounded without limiting active provider requests."""
    return httpx.AsyncClient(
        timeout=timeout_sec,
        proxy=proxy,
        limits=httpx.Limits(max_connections=None, max_keepalive_connections=20),
    )


@dataclass
class EgressClientPool:
    """Lazily-built httpx clients keyed on `connection_id`.

    The shared `upstream_client` (no proxy) is the fallback path used
    by routes that don't have a connection_id OR when egress mode is
    off. The pool only owns clients it created.
    """

    upstream_client: httpx.AsyncClient
    proxy_url: str
    upstream_timeout_sec: float
    _clients: dict[str, httpx.AsyncClient] = field(default_factory=dict)
    _lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    async def get(self, connection_id: UUID | str | None) -> httpx.AsyncClient:
        """Return the client a request with this `connection_id`
        should use.

        - Egress mode off (`proxy_url == ""`) → shared upstream client.
        - No connection_id supplied → shared upstream client. The
          route layer is responsible for rejecting requests that
          require a connection_id but didn't carry one; this method
          isn't an authz boundary.
        - Egress mode on + connection_id → pooled per-connection
          client with the CONNECT header set.
        """
        if not self.proxy_url or connection_id is None:
            return self.upstream_client
        key = str(connection_id)
        # Fast path: already pooled.
        cached = self._clients.get(key)
        if cached is not None:
            return cached
        # Slow path: build under lock so two concurrent first-touches
        # don't both build + leak one.
        async with self._lock:
            cached = self._clients.get(key)
            if cached is not None:
                return cached
            client = create_upstream_client(
                proxy=httpx.Proxy(
                    url=self.proxy_url,
                    headers={_CONNECTION_ID_HEADER: key},
                ),
                timeout_sec=self.upstream_timeout_sec,
            )
            self._clients[key] = client
            return client

    async def aclose(self) -> None:
        """Close every pooled client. The shared `upstream_client` is
        owned by the lifespan, not this pool — caller closes it."""
        async with self._lock:
            clients = list(self._clients.values())
            self._clients.clear()
        for c in clients:
            try:
                await c.aclose()
            except Exception:
                pass

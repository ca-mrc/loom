"""Gzip complete JSON responses; leave every other response byte-for-byte.

The ingress routes `/api/v1` straight to this service, and nothing in
between compresses, so a large JSON read crosses the network at full size
(the 13,089-call Batch detail is 950 KB, 58 KB gzipped; #2771). Starlette's
`GZipMiddleware` would also compress streamed downloads, object reads and
Trial bundles, which rely on `Content-Length`, ranges and their own encoding,
so this middleware is deliberately narrower:

- **Media type:** only `application/json` and `application/*+json`.
- **One body message:** a streamed response passes through unchanged.
- **Untouched headers:** a response that already sets `Content-Encoding`,
  `Content-Range` or `ETag` is left alone, so validators and ranges keep
  describing the bytes sent.
- **Size floor:** bodies under `minimum_size` aren't worth the header bytes.

Every eligible response carries `Vary: Accept-Encoding`, compressed or not,
so a shared cache never serves the gzip form to a client that didn't ask for it.

BREACH needs a response that reflects attacker-chosen input next to a
secret, under an attacker-observable size. API JSON responses aren't a
place where secrets and reflected input are meant to share a body; a route
that ever must should set `Content-Encoding: identity` to opt out.
"""

from __future__ import annotations

import gzip

from starlette.datastructures import Headers, MutableHeaders
from starlette.types import ASGIApp, Message, Receive, Scope, Send

_UNCOMPRESSIBLE_STATUS = frozenset({204, 206, 304})


def accepts_gzip(header: str) -> bool:
    """Whether an `Accept-Encoding` value allows gzip (RFC 9110 §12.5.3)."""
    gzip_q: float | None = None
    star_q: float | None = None
    for item in header.split(","):
        coding, _, params = item.strip().partition(";")
        coding = coding.strip().lower()
        if coding not in {"gzip", "x-gzip", "*"}:
            continue
        q = 1.0
        for param in params.split(";"):
            name, _, value = param.strip().partition("=")
            if name.strip().lower() == "q":
                try:
                    q = float(value)
                except ValueError:
                    q = 0.0
        if coding == "*":
            star_q = q
        else:
            gzip_q = q
    if gzip_q is not None:
        return gzip_q > 0
    return star_q is not None and star_q > 0


def _is_json(content_type: str) -> bool:
    media_type = content_type.split(";", 1)[0].strip().lower()
    return media_type == "application/json" or (
        media_type.startswith("application/") and media_type.endswith("+json")
    )


def _eligible(start: Message) -> bool:
    if start["status"] < 200 or start["status"] in _UNCOMPRESSIBLE_STATUS:
        return False
    headers = Headers(raw=start["headers"])
    return (
        _is_json(headers.get("content-type", ""))
        and "content-encoding" not in headers
        and "content-range" not in headers
        and "etag" not in headers
    )


class JsonCompressionMiddleware:
    def __init__(self, app: ASGIApp, *, minimum_size: int = 4096, compresslevel: int = 5) -> None:
        self.app = app
        self.minimum_size = minimum_size
        # Level 5 compresses the 950 KB Batch detail in about 3 ms to within
        # 4% of level 9, which takes about 8 ms.
        self.compresslevel = compresslevel

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        compress = accepts_gzip(Headers(scope=scope).get("accept-encoding", ""))
        held: Message | None = None
        decided = False

        async def send_wrapper(message: Message) -> None:
            nonlocal held, decided
            if decided:
                await send(message)
                return
            if message["type"] == "http.response.start":
                if _eligible(message):
                    held = message
                    return
                decided = True
                await send(message)
                return
            decided = True
            if held is None:  # pragma: no cover - ASGI always starts first
                await send(message)
                return
            if message["type"] == "http.response.body":
                headers = MutableHeaders(raw=held["headers"])
                headers.add_vary_header("Accept-Encoding")
                body = message.get("body", b"")
                if (
                    compress
                    and not message.get("more_body", False)
                    and len(body) >= self.minimum_size
                ):
                    compressed = gzip.compress(body, compresslevel=self.compresslevel, mtime=0)
                    if len(compressed) < len(body):
                        headers["Content-Encoding"] = "gzip"
                        headers["Content-Length"] = str(len(compressed))
                        message = {**message, "body": compressed}
            await send(held)
            await send(message)

        await self.app(scope, receive, send_wrapper)

"""Gzip only complete JSON responses; every other response is byte-for-byte (#2771)."""

from __future__ import annotations

import asyncio
import gzip
import json

import pytest
from starlette.applications import Starlette
from starlette.responses import JSONResponse, Response, StreamingResponse
from starlette.routing import Route

from loom_service.app import create_app
from loom_service.config import LoomServiceSettings
from loom_service.json_compression import JsonCompressionMiddleware, accepts_gzip

PAYLOAD = {"items": [{"id": i, "state": "succeeded", "note": "x" * 40} for i in range(400)]}
BODY = bytes(JSONResponse(PAYLOAD).body)


async def _json(_request):
    return JSONResponse(PAYLOAD)


async def _small(_request):
    return JSONResponse({"ok": True})


async def _problem(_request):
    return Response(BODY, media_type="application/problem+json", status_code=422)


async def _stream(_request):
    async def chunks():
        yield BODY[:100]
        yield BODY[100:]

    return StreamingResponse(chunks(), media_type="application/json")


async def _octets(_request):
    return Response(BODY, media_type="application/octet-stream")


async def _events(_request):
    return Response(BODY, media_type="text/event-stream")


async def _identity(_request):
    return Response(BODY, media_type="application/json", headers={"Content-Encoding": "identity"})


async def _etag(_request):
    return Response(BODY, media_type="application/json", headers={"ETag": '"v1"'})


async def _range(_request):
    return Response(
        BODY,
        status_code=206,
        media_type="application/json",
        headers={"Content-Range": f"bytes 0-{len(BODY) - 1}/{len(BODY)}"},
    )


APP = JsonCompressionMiddleware(
    Starlette(
        routes=[
            Route("/json", _json),
            Route("/small", _small),
            Route("/problem", _problem),
            Route("/stream", _stream),
            Route("/octets", _octets),
            Route("/events", _events),
            Route("/identity", _identity),
            Route("/etag", _etag),
            Route("/range", _range),
        ]
    )
)


async def _get(app, path, accept_encoding=b"gzip, deflate, br", method="GET"):
    """Return the raw start message and the raw (undecoded) body."""
    sent = []
    headers = [(b"host", b"testserver")]
    if accept_encoding is not None:
        headers.append((b"accept-encoding", accept_encoding))
    scope = {
        "type": "http",
        "asgi": {"version": "3.0"},
        "http_version": "1.1",
        "method": method,
        "scheme": "https",
        "path": path,
        "raw_path": path.encode(),
        "query_string": b"",
        "root_path": "",
        "headers": headers,
        "server": ("testserver", 443),
        "client": ("127.0.0.1", 1),
    }
    done = False

    async def receive():
        nonlocal done
        if not done:
            done = True
            return {"type": "http.request", "body": b"", "more_body": False}
        await asyncio.Future()

    async def send(message):
        sent.append(message)

    await app(scope, receive, send)
    start = next(m for m in sent if m["type"] == "http.response.start")
    body = b"".join(m.get("body", b"") for m in sent if m["type"] == "http.response.body")
    return {k.decode().lower(): v.decode() for k, v in start["headers"]}, start["status"], body


@pytest.mark.parametrize("path", ["/json", "/problem"])
async def test_large_json_is_gzipped_with_matching_length(path):
    headers, status, body = await _get(APP, path)
    assert headers["content-encoding"] == "gzip"
    assert headers["vary"] == "Accept-Encoding"
    assert int(headers["content-length"]) == len(body)
    assert gzip.decompress(body) == BODY
    assert len(body) < len(BODY) // 5
    assert status in {200, 422}


@pytest.mark.parametrize(
    "accept_encoding", [None, b"identity", b"br", b"gzip;q=0", b"*;q=0", b"gzip;q=0, *"]
)
async def test_json_stays_plain_unless_gzip_is_acceptable_but_still_varies(accept_encoding):
    headers, _, body = await _get(APP, "/json", accept_encoding)
    assert "content-encoding" not in headers
    assert headers["vary"] == "Accept-Encoding"
    assert body == BODY
    assert int(headers["content-length"]) == len(BODY)


@pytest.mark.parametrize("path", ["/small", "/stream"])
async def test_small_and_streamed_json_pass_through(path):
    headers, _, body = await _get(APP, path)
    assert "content-encoding" not in headers
    assert body == (b'{"ok":true}' if path == "/small" else BODY)


@pytest.mark.parametrize("path", ["/octets", "/events", "/identity", "/etag", "/range"])
async def test_downloads_validators_and_explicit_encodings_are_untouched(path):
    headers, _, body = await _get(APP, path)
    assert headers.get("content-encoding") in {None, "identity"}
    assert "vary" not in headers
    assert body == BODY


async def test_head_reports_the_same_representation_as_get():
    """The server drops a HEAD body; its headers must still match GET's."""
    head, _, _ = await _get(APP, "/json", method="HEAD")
    get, _, _ = await _get(APP, "/json")
    assert {k: head[k] for k in ("content-encoding", "content-length", "vary")} == {
        k: get[k] for k in ("content-encoding", "content-length", "vary")
    }


@pytest.mark.parametrize(
    "value,expected",
    [
        ("gzip", True),
        ("GZIP", True),
        ("x-gzip", True),
        ("deflate, gzip;q=0.5", True),
        ("*", True),
        ("gzip;q=0", False),
        ("gzip; q=0.0", False),
        ("*;q=0", False),
        ("gzip;q=0, *", False),
        ("br, *;q=0.1", True),
        ("", False),
        ("identity", False),
        ("gzip;q=bad", False),
    ],
)
def test_accept_encoding_parsing(value, expected):
    assert accepts_gzip(value) is expected


async def test_service_app_compresses_its_json():
    """The real app compresses its large JSON (the OpenAPI document).

    `@app.middleware` layers re-stream every body, so this fails if the
    middleware is registered outside them rather than next to the routes.
    """
    app = create_app(
        LoomServiceSettings(
            **{
                "_env_file": None,
                "service_mode": "management",
                "db_url": "postgresql+psycopg://u:p@localhost/db",
            }
        )
    )
    headers, status, body = await _get(app, "/openapi.json")
    assert status == 200
    assert headers["content-encoding"] == "gzip"
    assert json.loads(gzip.decompress(body))["info"]["title"] == "Loom Service"

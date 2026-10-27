from __future__ import annotations

import hashlib
import http.server
import importlib.util
import io
import json
import re
import socket
import ssl
import threading
import time
from http.client import IncompleteRead, RemoteDisconnected
from pathlib import Path
from types import ModuleType
from urllib.error import HTTPError, URLError
from urllib.parse import unquote
from urllib.request import urlopen

import pytest

PAYLOAD = b"checksum-locked guest package"
URL = "https://packages.example.invalid/archive?token=do-not-log"


@pytest.fixture
def payload_builder() -> ModuleType:
    path = Path(__file__).resolve().parents[2] / "deploy/guest-runtime/build-payload.py"
    spec = importlib.util.spec_from_file_location("guest_payload_builder", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def locked_archive() -> dict[str, object]:
    return {"url": URL, "bytes": len(PAYLOAD), "sha256": hashlib.sha256(PAYLOAD).hexdigest()}


@pytest.mark.parametrize("status", [500, 502, 503, 504])
@pytest.mark.parametrize("failures", [1, 2])
def test_transient_http_response_recovers_with_verified_bytes(
    payload_builder, locked_archive, tmp_path, monkeypatch, capsys, status, failures,
):
    requests = []
    sleeps = []
    error_bodies = []

    def open_archive(url, *, timeout):
        requests.append((url, timeout))
        if len(requests) <= failures:
            error_body = io.BytesIO(b"upstream error body must not be logged")
            error_bodies.append(error_body)
            raise HTTPError(url, status, "private response reason", {}, error_body)
        return io.BytesIO(PAYLOAD)

    monkeypatch.setattr(payload_builder, "urlopen", open_archive)
    monkeypatch.setattr(time, "sleep", sleeps.append)
    destination = payload_builder.download(locked_archive, tmp_path)

    assert requests == [(URL, 120)] * (failures + 1)
    assert sleeps == list(range(1, failures + 1))
    assert all(body.closed for body in error_bodies)
    assert destination.read_bytes() == PAYLOAD
    assert destination.name == locked_archive["sha256"]
    assert list(tmp_path.iterdir()) == [destination]
    diagnostic = capsys.readouterr().err
    assert str(locked_archive["sha256"]) in diagnostic
    assert f"HTTP {status}" in diagnostic and "1/3" in diagnostic
    assert "do-not-log" not in diagnostic and "private" not in diagnostic
    assert "upstream error body" not in diagnostic


@pytest.mark.parametrize("status", [500, 502, 503, 504])
def test_transient_http_failures_stop_after_three_attempts(
    payload_builder, locked_archive, tmp_path, monkeypatch, capsys, status,
):
    bodies = []
    sleeps = []
    partial = tmp_path / f"{locked_archive['sha256']}.partial"
    partial.write_bytes(b"previous interrupted download")

    def open_archive(url, *, timeout):
        assert (url, timeout) == (URL, 120)
        body = io.BytesIO(b"private error")
        bodies.append(body)
        raise HTTPError(url, status, "private response reason", {}, body)

    monkeypatch.setattr(payload_builder, "urlopen", open_archive)
    monkeypatch.setattr(time, "sleep", sleeps.append)
    with pytest.raises(HTTPError) as error:
        payload_builder.download(locked_archive, tmp_path)

    assert error.value.code == status
    assert len(bodies) == 3 and all(body.closed for body in bodies)
    assert sleeps == [1, 2]
    assert not list(tmp_path.iterdir())
    diagnostic = capsys.readouterr().err
    assert "3/3" in diagnostic and "do-not-log" not in diagnostic


def test_mixed_server_errors_use_one_bounded_attempt_budget(
    payload_builder, locked_archive, tmp_path, monkeypatch,
):
    responses = iter([502, 500, PAYLOAD])
    requests = []
    sleeps = []

    def open_archive(url, *, timeout):
        requests.append((url, timeout))
        response = next(responses)
        if isinstance(response, int):
            raise HTTPError(url, response, "upstream server error", {}, io.BytesIO())
        return io.BytesIO(response)

    monkeypatch.setattr(payload_builder, "urlopen", open_archive)
    monkeypatch.setattr(time, "sleep", sleeps.append)
    destination = payload_builder.download(locked_archive, tmp_path)
    assert destination.read_bytes() == PAYLOAD
    assert destination.name == locked_archive["sha256"]
    assert requests == [(URL, 120)] * 3
    assert sleeps == [1, 2]


@pytest.mark.parametrize("status", [400, 401, 403, 404, 429])
def test_other_http_errors_fail_without_retry(
    payload_builder, locked_archive, tmp_path, monkeypatch, status,
):
    requests = []
    body = io.BytesIO(b"private error")
    sleeps = []

    def open_archive(url, *, timeout):
        requests.append((url, timeout))
        raise HTTPError(url, status, "error", {}, body)

    monkeypatch.setattr(payload_builder, "urlopen", open_archive)
    monkeypatch.setattr(time, "sleep", sleeps.append)
    with pytest.raises(HTTPError) as error:
        payload_builder.download(locked_archive, tmp_path)

    assert error.value.code == status
    assert requests == [(URL, 120)] and sleeps == []
    assert body.closed and not list(tmp_path.iterdir())


@pytest.mark.parametrize("content", [b"short", b"x" * len(PAYLOAD)])
@pytest.mark.parametrize("status", [500, 504])
def test_content_mismatch_after_retry_is_not_retried_or_cached(
    payload_builder, locked_archive, tmp_path, monkeypatch, content, status,
):
    requests = []
    sleeps = []

    def open_archive(url, *, timeout):
        requests.append((url, timeout))
        if len(requests) == 1:
            raise HTTPError(url, status, "upstream server error", {}, io.BytesIO())
        return io.BytesIO(content)

    monkeypatch.setattr(payload_builder, "urlopen", open_archive)
    monkeypatch.setattr(time, "sleep", sleeps.append)
    with pytest.raises(ValueError, match="content does not match source lock"):
        payload_builder.download(locked_archive, tmp_path)
    assert requests == [(URL, 120)] * 2 and sleeps == [1]
    assert not list(tmp_path.iterdir())


@pytest.mark.parametrize("failures", [1, 2, 3])
@pytest.mark.parametrize("phase", ["body", "wrapped-body", "connection", "wrapped-connection"])
@pytest.mark.parametrize("error_type", [TimeoutError, ConnectionResetError, RemoteDisconnected, ssl.SSLEOFError])
def test_transport_interruptions_restart_within_the_existing_attempt_budget(
    payload_builder, locked_archive, tmp_path, monkeypatch, capsys, failures, phase, error_type,
):
    errors = []

    def interruption():
        error = error_type("private transfer interrupted")
        if phase.startswith("wrapped-"):
            error = URLError(error)
        errors.append(error)
        return error

    class InterruptedBody(io.BytesIO):
        def read(self, size=-1):
            if self.tell():
                raise interruption()
            return super().read(size)

    requests = []
    sleeps = []
    bodies = []

    def open_archive(url, *, timeout):
        requests.append((url, timeout))
        if phase.endswith("connection") and len(requests) <= failures:
            raise interruption()
        body = (InterruptedBody(b"partial") if len(requests) <= failures
                else io.BytesIO(PAYLOAD))
        bodies.append(body)
        return body

    monkeypatch.setattr(payload_builder, "urlopen", open_archive)
    monkeypatch.setattr(time, "sleep", sleeps.append)
    if failures == 3:
        with pytest.raises(URLError if phase.startswith("wrapped-") else error_type,
                           match="transfer interrupted") as failure:
            payload_builder.download(locked_archive, tmp_path)
        assert failure.value is errors[-1]
        assert not list(tmp_path.iterdir())
    else:
        destination = payload_builder.download(locked_archive, tmp_path)
        assert destination.read_bytes() == PAYLOAD
        assert destination.name == locked_archive["sha256"]
        assert list(tmp_path.iterdir()) == [destination]
    assert requests == [(URL, 120)] * min(failures + 1, 3)
    assert sleeps == list(range(1, min(failures, 2) + 1))
    assert all(body.closed for body in bodies)
    diagnostic = capsys.readouterr().err
    category = "body" if phase.endswith("body") else "connection"
    assert f"{category} {'timeout' if error_type is TimeoutError else 'interrupted'}" in diagnostic
    assert "1/3" in diagnostic
    assert str(locked_archive["sha256"]) in diagnostic
    assert "do-not-log" not in diagnostic and "transfer interrupted" not in diagnostic


@pytest.mark.parametrize("failures", [1, 2, 3])
@pytest.mark.parametrize("wrapped", [False, True])
def test_incomplete_body_restarts_without_using_the_fallback(
    payload_builder, locked_archive, tmp_path, monkeypatch, failures, wrapped,
):
    requests, sleeps, bodies, errors = [], [], [], []

    class InterruptedBody(io.BytesIO):
        def read(self, size=-1):
            if self.tell():
                error = IncompleteRead(b"private partial bytes", 10)
                if wrapped:
                    error = URLError(error)
                errors.append(error)
                raise error
            return super().read(size)

    def open_archive(url, *, timeout):
        requests.append((url, timeout))
        body = InterruptedBody(b"partial") if len(requests) <= failures else io.BytesIO(PAYLOAD)
        bodies.append(body)
        return body

    monkeypatch.setattr(payload_builder, "urlopen", open_archive)
    monkeypatch.setattr(time, "sleep", sleeps.append)
    item = {**locked_archive, "fallback_url": FALLBACK}
    if failures == 3:
        with pytest.raises(URLError if wrapped else IncompleteRead) as failure:
            payload_builder.download(item, tmp_path)
        assert failure.value is errors[-1] and not list(tmp_path.iterdir())
    else:
        destination = payload_builder.download(item, tmp_path)
        assert destination.read_bytes() == PAYLOAD
        assert list(tmp_path.iterdir()) == [destination]
    assert requests == [(URL, 120)] * min(failures + 1, 3)
    assert sleeps == list(range(1, min(failures, 2) + 1))
    assert all(body.closed for body in bodies)


@pytest.mark.parametrize("phase", ["body", "connection"])
@pytest.mark.parametrize("error_type", [TimeoutError, ConnectionResetError, RemoteDisconnected, ssl.SSLEOFError])
@pytest.mark.parametrize("recover", [False, True])
def test_http_and_transport_interruptions_share_one_attempt_budget(
    payload_builder, locked_archive, tmp_path, monkeypatch, phase, error_type, recover,
):
    class InterruptedBody(io.BytesIO):
        def read(self, size=-1):
            raise error_type("body transfer interrupted")

    requests = []
    sleeps = []
    bodies = []

    def open_archive(url, *, timeout):
        requests.append((url, timeout))
        if len(requests) == 1:
            body = io.BytesIO()
            bodies.append(body)
            raise HTTPError(url, 503, "private upstream response", {}, body)
        interrupted = len(requests) == 2 or not recover
        if interrupted and phase == "connection":
            raise URLError(error_type("private handshake interrupted"))
        body = InterruptedBody() if interrupted else io.BytesIO(PAYLOAD)
        bodies.append(body)
        return body

    monkeypatch.setattr(payload_builder, "urlopen", open_archive)
    monkeypatch.setattr(time, "sleep", sleeps.append)
    if recover:
        destination = payload_builder.download(locked_archive, tmp_path)
        assert destination.read_bytes() == PAYLOAD
        assert list(tmp_path.iterdir()) == [destination]
    else:
        with pytest.raises(URLError if phase == "connection" else error_type):
            payload_builder.download(locked_archive, tmp_path)
        assert not list(tmp_path.iterdir())
    assert requests == [(URL, 120)] * 3 and sleeps == [1, 2]
    assert all(body.closed for body in bodies)


@pytest.mark.parametrize("reason", [
    "connection failed", ssl.SSLCertVerificationError("certificate rejected"),
    ssl.SSLError("TLS configuration rejected"),
])
@pytest.mark.parametrize("phase", ["connection", "body"])
def test_other_transport_error_is_not_retried(
    payload_builder, locked_archive, tmp_path, monkeypatch, reason, phase,
):
    requests = []
    sleeps = []

    class RejectedBody(io.BytesIO):
        def read(self, size=-1):
            raise URLError(reason)

    def open_archive(url, *, timeout):
        requests.append((url, timeout))
        if phase == "body":
            return RejectedBody()
        raise URLError(reason)

    monkeypatch.setattr(payload_builder, "urlopen", open_archive)
    monkeypatch.setattr(time, "sleep", sleeps.append)
    with pytest.raises(URLError):
        payload_builder.download(locked_archive, tmp_path)
    assert requests == [(URL, 120)] and sleeps == []
    assert not list(tmp_path.iterdir())


@pytest.mark.parametrize("wrapped", [False, True])
def test_incomplete_read_at_connection_is_not_retried(
    payload_builder, locked_archive, tmp_path, monkeypatch, wrapped,
):
    requests, sleeps = [], []
    error = IncompleteRead(b"unexpected before response", 10)
    if wrapped:
        error = URLError(error)

    def open_archive(url, *, timeout):
        requests.append((url, timeout))
        raise error

    monkeypatch.setattr(payload_builder, "urlopen", open_archive)
    monkeypatch.setattr(time, "sleep", sleeps.append)
    with pytest.raises(type(error)) as failure:
        payload_builder.download({**locked_archive, "fallback_url": FALLBACK}, tmp_path)
    assert failure.value is error
    assert requests == [(URL, 120)] and sleeps == []
    assert not list(tmp_path.iterdir())


@pytest.mark.parametrize("phase", ["connection", "body", "wrapped-body"])
@pytest.mark.parametrize("error_type", [ssl.SSLCertVerificationError, ssl.SSLError])
def test_fatal_tls_errors_are_not_retried_or_sent_to_fallback(
    payload_builder, locked_archive, tmp_path, monkeypatch, phase, error_type,
):
    requests, sleeps, bodies = [], [], []
    error = error_type("private TLS rejection")
    if phase == "wrapped-body":
        error = URLError(error)

    class RejectedBody(io.BytesIO):
        def read(self, size=-1):
            raise error

    def open_archive(url, *, timeout):
        requests.append((url, timeout))
        if phase == "connection":
            raise error
        body = RejectedBody()
        bodies.append(body)
        return body

    monkeypatch.setattr(payload_builder, "urlopen", open_archive)
    monkeypatch.setattr(time, "sleep", sleeps.append)
    with pytest.raises(type(error)) as failure:
        payload_builder.download({**locked_archive, "fallback_url": FALLBACK}, tmp_path)
    assert failure.value is error
    assert requests == [(URL, 120)] and sleeps == []
    assert all(body.closed for body in bodies) and not list(tmp_path.iterdir())


@pytest.mark.parametrize("error_type", [ConnectionResetError, RemoteDisconnected, ssl.SSLEOFError])
def test_exhausted_connection_interruptions_do_not_use_the_fallback(
    payload_builder, locked_archive, tmp_path, monkeypatch, error_type,
):
    requests, sleeps = [], []
    error = error_type("private interrupted connection")

    def open_archive(url, *, timeout):
        requests.append((url, timeout))
        raise error

    monkeypatch.setattr(payload_builder, "urlopen", open_archive)
    monkeypatch.setattr(time, "sleep", sleeps.append)
    with pytest.raises(error_type) as failure:
        payload_builder.download({**locked_archive, "fallback_url": FALLBACK}, tmp_path)
    assert failure.value is error
    assert requests == [(URL, 120)] * 3 and sleeps == [1, 2]
    assert not list(tmp_path.iterdir())


@pytest.mark.parametrize("error_type", [PermissionError, TimeoutError, ConnectionResetError, ssl.SSLEOFError])
@pytest.mark.parametrize("stage", ["open", "write", "close"])
def test_local_cache_error_is_not_retried(
    payload_builder, locked_archive, tmp_path, monkeypatch, error_type, stage,
):
    requests = []
    sleeps = []
    body = io.BytesIO(PAYLOAD)
    original_open = Path.open

    class CacheWriter:
        def __init__(self, handle):
            self.handle = handle

        def __enter__(self):
            return self

        def write(self, content):
            if stage == "write":
                raise error_type("cache write denied")
            return self.handle.write(content)

        def __exit__(self, *args):
            self.handle.close()
            if stage == "close":
                raise error_type("cache write denied")

    def open_archive(url, *, timeout):
        requests.append((url, timeout))
        return body

    def deny_partial_write(path, *args, **kwargs):
        if path.suffix == ".partial":
            if stage == "open":
                raise error_type("cache write denied")
            return CacheWriter(original_open(path, *args, **kwargs))
        return original_open(path, *args, **kwargs)

    monkeypatch.setattr(payload_builder, "urlopen", open_archive)
    monkeypatch.setattr(Path, "open", deny_partial_write)
    monkeypatch.setattr(time, "sleep", sleeps.append)
    with pytest.raises(error_type, match="cache write denied"):
        payload_builder.download(locked_archive, tmp_path)
    assert requests == [(URL, 120)] and sleeps == []
    assert body.closed and not list(tmp_path.iterdir())


@pytest.mark.parametrize("cached", [PAYLOAD, b"invalid cache"])
def test_only_verified_cached_bytes_are_reused(
    payload_builder, locked_archive, tmp_path, monkeypatch, cached,
):
    destination = tmp_path / str(locked_archive["sha256"])
    destination.write_bytes(cached)
    requests = []

    def open_archive(url, *, timeout):
        requests.append((url, timeout))
        return io.BytesIO(PAYLOAD)

    monkeypatch.setattr(payload_builder, "urlopen", open_archive)
    assert payload_builder.download(locked_archive, tmp_path) == destination
    assert destination.read_bytes() == PAYLOAD
    assert requests == ([] if cached == PAYLOAD else [(URL, 120)])


FALLBACK = "https://files.example.invalid/permanent?token=do-not-log"


def test_mirror_404_falls_back_to_the_permanent_file(
    payload_builder, locked_archive, tmp_path, monkeypatch, capsys,
):
    requests = []
    sleeps = []

    def open_archive(url, *, timeout):
        requests.append((url, timeout))
        if url == URL:
            raise HTTPError(url, 404, "superseded", {}, io.BytesIO())
        return io.BytesIO(PAYLOAD)

    monkeypatch.setattr(payload_builder, "urlopen", open_archive)
    monkeypatch.setattr(time, "sleep", sleeps.append)
    destination = payload_builder.download({**locked_archive, "fallback_url": FALLBACK}, tmp_path)

    assert destination.read_bytes() == PAYLOAD
    assert requests == [(URL, 120), (FALLBACK, 120)] and sleeps == []
    assert "do-not-log" not in capsys.readouterr().err


@pytest.mark.parametrize("status", [500, 502, 503, 504])
def test_unavailable_mirror_falls_back_after_its_retries(
    payload_builder, locked_archive, tmp_path, monkeypatch, status,
):
    requests = []
    sleeps = []

    def open_archive(url, *, timeout):
        requests.append((url, timeout))
        if url == URL:
            raise HTTPError(url, status, "unavailable", {}, io.BytesIO())
        return io.BytesIO(PAYLOAD)

    monkeypatch.setattr(payload_builder, "urlopen", open_archive)
    monkeypatch.setattr(time, "sleep", sleeps.append)
    destination = payload_builder.download({**locked_archive, "fallback_url": FALLBACK}, tmp_path)

    assert destination.read_bytes() == PAYLOAD
    assert requests == [(URL, 120)] * 3 + [(FALLBACK, 120)]
    assert sleeps == [1, 2]


@pytest.mark.parametrize("status", [400, 401, 403, 429])
def test_other_mirror_errors_do_not_fall_back(
    payload_builder, locked_archive, tmp_path, monkeypatch, status,
):
    requests = []

    def open_archive(url, *, timeout):
        requests.append((url, timeout))
        raise HTTPError(url, status, "error", {}, io.BytesIO())

    monkeypatch.setattr(payload_builder, "urlopen", open_archive)
    with pytest.raises(HTTPError) as error:
        payload_builder.download({**locked_archive, "fallback_url": FALLBACK}, tmp_path)
    assert error.value.code == status and requests == [(URL, 120)]


def test_fallback_bytes_are_verified_like_the_mirror(
    payload_builder, locked_archive, tmp_path, monkeypatch,
):
    def open_archive(url, *, timeout):
        if url == URL:
            raise HTTPError(url, 404, "superseded", {}, io.BytesIO())
        return io.BytesIO(b"x" * len(PAYLOAD))

    monkeypatch.setattr(payload_builder, "urlopen", open_archive)
    with pytest.raises(ValueError, match="does not match source lock"):
        payload_builder.download({**locked_archive, "fallback_url": FALLBACK}, tmp_path)
    assert not list(tmp_path.iterdir())


def test_locked_packages_pair_the_mirror_with_a_permanent_launchpad_file():
    # archive.ubuntu.com is fast but drops a pool file once Ubuntu supersedes
    # its version. Every entry keeps the mirror URL for speed and the permanent
    # Launchpad Librarian file as its 404 fallback, both naming the exact .deb.
    lock = json.loads((Path(__file__).resolve().parents[2] / "deploy/guest-runtime/sources.lock.json").read_text())
    for package in lock["packages"]:
        version = str(package["version"]).split(":", 1)[-1]
        filename = f"{package['name']}_{version}_{package['architecture']}.deb"
        mirror = re.fullmatch(r"https://archive\.ubuntu\.com/ubuntu/pool/[a-z]+/[^/]+/[^/]+/([^/]+)", str(package["url"]))
        assert mirror is not None and unquote(mirror.group(1)) == filename, package["url"]
        fallback = re.fullmatch(r"https://launchpadlibrarian\.net/[0-9]+/([^/]+)", str(package.get("fallback_url")))
        assert fallback is not None and fallback.group(1) == filename, package.get("fallback_url")


def test_actual_http_peer_disconnect_recovers_verified_archive(
    payload_builder, locked_archive, tmp_path, monkeypatch,
):
    requests = []
    sleeps = []

    class Handler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            requests.append(self.path)
            if len(requests) < 3:
                self.connection.shutdown(socket.SHUT_RDWR)
                self.connection.close()
                return
            self.send_response(200)
            self.send_header("Content-Length", str(len(PAYLOAD)))
            self.end_headers()
            self.wfile.write(PAYLOAD)

        def log_message(self, *args):
            pass

    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()

    def open_loopback_fixture(url, *, timeout):
        assert url == URL
        # Keep the public HTTPS source contract; only the fixture's transport
        # is replaced with a real local HTTP peer that closes before headers.
        return urlopen(f"http://127.0.0.1:{server.server_port}/archive", timeout=timeout)

    monkeypatch.setattr(payload_builder, "urlopen", open_loopback_fixture)
    monkeypatch.setattr(time, "sleep", sleeps.append)
    try:
        destination = payload_builder.download(locked_archive, tmp_path)
        assert destination.read_bytes() == PAYLOAD
        assert destination.name == locked_archive["sha256"]
        assert list(tmp_path.iterdir()) == [destination]
        assert requests == ["/archive"] * 3
        assert sleeps == [1, 2]
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)
        assert not thread.is_alive()

from __future__ import annotations

import hashlib
import importlib.util
import io
import time
from pathlib import Path
from types import ModuleType
from urllib.error import HTTPError, URLError

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


@pytest.mark.parametrize("status", [502, 503, 504])
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


@pytest.mark.parametrize("status", [502, 503, 504])
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


@pytest.mark.parametrize("status", [400, 401, 403, 404, 429, 500])
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
def test_content_mismatch_after_retry_is_not_retried_or_cached(
    payload_builder, locked_archive, tmp_path, monkeypatch, content,
):
    requests = []
    sleeps = []

    def open_archive(url, *, timeout):
        requests.append((url, timeout))
        if len(requests) == 1:
            raise HTTPError(url, 504, "gateway timeout", {}, io.BytesIO())
        return io.BytesIO(content)

    monkeypatch.setattr(payload_builder, "urlopen", open_archive)
    monkeypatch.setattr(time, "sleep", sleeps.append)
    with pytest.raises(ValueError, match="content does not match source lock"):
        payload_builder.download(locked_archive, tmp_path)
    assert requests == [(URL, 120)] * 2 and sleeps == [1]
    assert not list(tmp_path.iterdir())


def test_interrupted_body_is_cleaned_without_retry(
    payload_builder, locked_archive, tmp_path, monkeypatch,
):
    class InterruptedBody(io.BytesIO):
        def read(self, size=-1):
            if self.tell():
                raise TimeoutError("body transfer interrupted")
            return super().read(size)

    requests = []
    sleeps = []
    body = InterruptedBody(b"partial")

    def open_archive(url, *, timeout):
        requests.append((url, timeout))
        return body

    monkeypatch.setattr(payload_builder, "urlopen", open_archive)
    monkeypatch.setattr(time, "sleep", sleeps.append)
    with pytest.raises(TimeoutError, match="body transfer interrupted"):
        payload_builder.download(locked_archive, tmp_path)
    assert requests == [(URL, 120)] and sleeps == []
    assert body.closed and not list(tmp_path.iterdir())


def test_non_http_transport_error_is_not_retried(
    payload_builder, locked_archive, tmp_path, monkeypatch,
):
    requests = []
    sleeps = []

    def open_archive(url, *, timeout):
        requests.append((url, timeout))
        raise URLError("connection failed")

    monkeypatch.setattr(payload_builder, "urlopen", open_archive)
    monkeypatch.setattr(time, "sleep", sleeps.append)
    with pytest.raises(URLError):
        payload_builder.download(locked_archive, tmp_path)
    assert requests == [(URL, 120)] and sleeps == []
    assert not list(tmp_path.iterdir())


def test_local_write_error_is_not_retried(
    payload_builder, locked_archive, tmp_path, monkeypatch,
):
    requests = []
    sleeps = []
    body = io.BytesIO(PAYLOAD)
    original_open = Path.open

    def open_archive(url, *, timeout):
        requests.append((url, timeout))
        return body

    def deny_partial_write(path, *args, **kwargs):
        if path.suffix == ".partial":
            raise PermissionError("cache write denied")
        return original_open(path, *args, **kwargs)

    monkeypatch.setattr(payload_builder, "urlopen", open_archive)
    monkeypatch.setattr(Path, "open", deny_partial_write)
    monkeypatch.setattr(time, "sleep", sleeps.append)
    with pytest.raises(PermissionError, match="cache write denied"):
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

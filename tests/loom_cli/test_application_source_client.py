"""The authenticated application client transports the exact captured archive."""
from __future__ import annotations

import json
from uuid import UUID

import httpx
import pytest

from loom.application_source_upload import ApplicationSourceUploadV1
from loom_cli import server_client
from loom_cli.application_client import ApplicationClient
from loom_cli.application_source import package_application_source
from loom_cli.config import LoomConfig, save_config
from tests.loom_cli.test_application_source import commit
from tests.loom_cli.test_application_source import repo as repo

UPLOAD = "40000000-0000-4000-8000-000000000001"
FOREIGN = "40000000-0000-4000-8000-000000000002"


@pytest.fixture
def source_http(monkeypatch, tmp_xdg_home):
    save_config(LoomConfig(server_url="https://manage.example.com", auth_token="management-secret"))
    handlers, requests = {}, []
    authed = server_client.authed_client

    def handle(request):
        requests.append(request)
        assert request.url.host == "manage.example.com"
        return handlers[request.method, request.url.path](request)

    monkeypatch.setattr(server_client, "authed_client", lambda cfg: authed(cfg, transport=httpx.MockTransport(handle)))
    return handlers, requests


def receipt(source, **changes):
    return {"source_digest": source.manifest.digest, "archive_sha256": source.archive_sha256.removeprefix("sha256:"),
        "archive_size_bytes": source.archive_size_bytes, "base_commit": source.base_commit,
        "upload_id": UPLOAD, "phase": "awaiting_source", "expires_at": "2026-12-01T00:00:00Z", **changes}


def test_source_client_sends_frozen_dirty_archive_and_exact_replay_identity(repo, source_http):
    handlers, requests = source_http
    (repo / "app.py").write_bytes(b"committed")
    commit(repo)
    (repo / "app.py").write_bytes(b"dirty")
    (repo / "untracked").write_bytes(b"feature")
    with package_application_source(repo) as source, ApplicationClient() as client:
        expected = source.archive.read()
        pending = receipt(source)
        verified = {**pending, "phase": "source_verified"}
        handlers["POST", "/api/v1/application-sources"] = lambda _: httpx.Response(201, json=pending)
        handlers["PUT", f"/api/v1/application-sources/{UPLOAD}/content"] = lambda _: httpx.Response(200, json=verified)
        handlers["GET", f"/api/v1/application-sources/{UPLOAD}"] = lambda _: httpx.Response(200, json=verified)
        # Mutating the checkout after capture cannot change the already frozen upload.
        (repo / "app.py").write_bytes(b"newer edit")
        intent = client.create_source_upload(source, idempotency_key="frozen-source")
        result = client.upload_source(intent, source)
        assert result.phase == "source_verified" and result.upload_id == UUID(UPLOAD)
        assert client.source_upload_status(UUID(UPLOAD)) == result
        assert not source.archive.closed
    assert source.archive.closed
    assert [request.method for request in requests] == ["POST", "PUT", "GET"]
    assert requests[0].headers["Idempotency-Key"] == "frozen-source"
    assert json.loads(requests[0].content) == {name: pending[name] for name in (
        "source_digest", "archive_sha256", "archive_size_bytes", "base_commit")}
    assert requests[1].content == expected
    assert requests[1].headers["Content-Length"] == str(len(expected))
    assert requests[1].headers["Content-Type"] == "application/octet-stream"
    assert all(request.headers["Authorization"] == "Bearer management-secret" for request in requests)


@pytest.mark.parametrize("stage", ["intent", "upload", "status"])
def test_source_client_never_retries_an_uncertain_request(repo, source_http, stage):
    handlers, requests = source_http
    (repo / "app").write_bytes(b"feature")
    def lost(_):
        raise httpx.ReadTimeout("uncertain result")
    with package_application_source(repo) as source, ApplicationClient() as client:
        pending = ApplicationSourceUploadV1.model_validate(receipt(source))
        handlers["POST", "/api/v1/application-sources"] = lost
        handlers["PUT", f"/api/v1/application-sources/{UPLOAD}/content"] = lost
        handlers["GET", f"/api/v1/application-sources/{UPLOAD}"] = lost
        with pytest.raises(httpx.ReadTimeout):
            if stage == "intent":
                client.create_source_upload(source, idempotency_key="same-after-unknown")
            elif stage == "upload":
                client.upload_source(pending, source)
            else:
                client.source_upload_status(UUID(UPLOAD))
    assert len(requests) == 1


@pytest.mark.parametrize("damage", [
    {"source_digest": "sha256:" + "a" * 64}, {"archive_sha256": "a" * 64},
    {"archive_size_bytes": 20480}, {"base_commit": "a" * 40},
])
def test_mismatched_intent_response_cannot_authorize_bytes(repo, source_http, damage):
    handlers, requests = source_http
    (repo / "app").write_bytes(b"feature")
    with package_application_source(repo) as source, ApplicationClient() as client:
        handlers["POST", "/api/v1/application-sources"] = lambda _: httpx.Response(201, json=receipt(source, **damage))
        with pytest.raises(ValueError, match=r"source.*mismatch"):
            client.create_source_upload(source, idempotency_key="bound-source")
        with pytest.raises(ValueError, match=r"source.*mismatch"):
            client.upload_source(ApplicationSourceUploadV1.model_validate(receipt(source, **damage)), source)
    assert len(requests) == 1


@pytest.mark.parametrize("damage", [{"upload_id": FOREIGN}, {"phase": "awaiting_source"}, {"archive_sha256": "a" * 64}])
def test_upload_response_must_verify_the_same_source_and_upload(repo, source_http, damage):
    handlers, requests = source_http
    (repo / "app").write_bytes(b"feature")
    with package_application_source(repo) as source, ApplicationClient() as client:
        handlers["PUT", f"/api/v1/application-sources/{UPLOAD}/content"] = lambda _: httpx.Response(200,
            json=receipt(source, phase="source_verified") | damage)
        with pytest.raises(ValueError, match=r"source.*mismatch"):
            client.upload_source(ApplicationSourceUploadV1.model_validate(receipt(source)), source)
    assert len(requests) == 1


def test_status_cannot_substitute_another_upload(repo, source_http):
    handlers, requests = source_http
    (repo / "app").write_bytes(b"feature")
    with package_application_source(repo) as source, ApplicationClient() as client:
        handlers["GET", f"/api/v1/application-sources/{UPLOAD}"] = lambda _: httpx.Response(200, json=receipt(source, upload_id=FOREIGN))
        with pytest.raises(ValueError, match=r"source.*mismatch"):
            client.source_upload_status(UUID(UPLOAD))
    assert len(requests) == 1


def test_only_known_csrf_rejection_replays_full_identical_stream(repo, source_http):
    handlers, requests = source_http
    save_config(LoomConfig(server_url="https://manage.example.com", auth_session_cookie="fixture-session",
        auth_session_cookie_name="__Host-loom_session", auth_csrf_token="fixture-csrf"))
    (repo / "app").write_bytes(b"feature")
    uploads = []
    with package_application_source(repo) as source, ApplicationClient() as client:
        expected = source.archive.read()
        handlers["GET", "/api/v1/auth/me"] = lambda _: httpx.Response(200, json={"csrf_token": "fixture-csrf"})
        def upload(request):
            uploads.append(request.content)
            if len(uploads) == 1:
                return httpx.Response(403, json={"detail": "csrf rejected"})
            return httpx.Response(200, json=receipt(source, phase="source_verified"))
        handlers["PUT", f"/api/v1/application-sources/{UPLOAD}/content"] = upload
        result = client.upload_source(ApplicationSourceUploadV1.model_validate(receipt(source)), source)
    assert result.phase == "source_verified" and uploads == [expected, expected]
    assert [request.method for request in requests] == ["GET", "PUT", "GET", "PUT"]
    assert all(request.headers["Cookie"] == "__Host-loom_session=fixture-session" for request in requests)

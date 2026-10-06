from __future__ import annotations

import hashlib
from uuid import uuid4

import pytest
from fastapi import HTTPException, Request

from loom.auth import AuthContext
from loom_service.config import LoomServiceSettings
from loom_service.session_auth import (
    browser_origin_allowed,
    hash_secret,
    session_cookie_options,
    session_csrf_token,
    verify_csrf,
)


def _session_ctx(csrf_raw: str = "csrf-token") -> AuthContext:
    return AuthContext(
        token_hash=b"",
        type="user",
        scopes=["read:own"],
        team_id=uuid4(),
        expires_at=None,
        role="viewer",
        session_hash=hash_secret("session-token"),
        csrf_hash=hash_secret(csrf_raw),
        auth_kind="session",
    )


def test_hash_secret_matches_sha256_bytes() -> None:
    assert hash_secret("abc") == hashlib.sha256(b"abc").digest()


def test_verify_csrf_accepts_matching_session_header() -> None:
    verify_csrf(_session_ctx(), "csrf-token")


def test_read_proof_is_stable_cookie_bound_and_invalidated_by_nonce_rotation() -> None:
    ctx = _session_ctx()
    proof = session_csrf_token(ctx, "session-token")
    assert proof == session_csrf_token(ctx, "session-token")
    verify_csrf(ctx, proof, raw_session="session-token")
    verify_csrf(ctx, "csrf-token", raw_session="session-token")
    for invalid_ctx, cookie, token in (
        (ctx, "another-cookie", proof),
        (_session_ctx("rotated-token"), "session-token", proof),
        (ctx, None, proof),
        (ctx, "session-token", "invalid-非ascii"),
    ):
        with pytest.raises(HTTPException) as exc:
            verify_csrf(invalid_ctx, token, raw_session=cookie)
        assert exc.value.status_code == 403


def test_verify_csrf_rejects_missing_session_header() -> None:
    with pytest.raises(HTTPException) as ei:
        verify_csrf(_session_ctx(), None)
    assert ei.value.status_code == 403
    assert "CSRF" in ei.value.detail


def test_verify_csrf_rejects_mismatched_session_header() -> None:
    with pytest.raises(HTTPException) as ei:
        verify_csrf(_session_ctx(), "wrong-token")
    assert ei.value.status_code == 403


def test_verify_csrf_skips_bearer_contexts() -> None:
    ctx = AuthContext(
        token_hash=hash_secret("team-token"),
        type="team",
        scopes=["submit"],
        team_id=uuid4(),
        expires_at=None,
        auth_kind="bearer",
    )
    verify_csrf(ctx, None)


@pytest.mark.parametrize("environment", ["development", "staging", "production", ""])
def test_hosted_cookie_is_host_scoped_in_every_environment(monkeypatch, environment):
    monkeypatch.setenv("LOOM_ENV", environment)
    monkeypatch.delenv("LOOM_SVC_AUTH_LOCAL_HTTP", raising=False)
    settings = LoomServiceSettings(
        _env_file=None, db_url="postgresql+psycopg://u:p@localhost/loom",
        minio_access_key="x", minio_secret_key="y",
    )
    options = session_cookie_options(settings)
    assert options["key"] == "__Host-loom_session"
    assert options["secure"] and options["httponly"]
    assert options["path"] == "/"
    assert "domain" not in options


def test_explicit_local_http_uses_legacy_cookie(monkeypatch):
    monkeypatch.setenv("LOOM_ENV", "development")
    monkeypatch.setenv("LOOM_SVC_AUTH_LOCAL_HTTP", "true")
    settings = LoomServiceSettings(
        _env_file=None, db_url="postgresql+psycopg://u:p@localhost/loom",
        minio_access_key="x", minio_secret_key="y",
    )
    options = session_cookie_options(settings)
    assert options["key"] == "loom_session"
    assert options["secure"] is False


@pytest.mark.parametrize("environment,public_url", [
    ("production", None), ("development", "https://alice.dev.example.com"),
])
def test_local_http_cannot_downgrade_hosted_or_production(monkeypatch, environment, public_url):
    monkeypatch.setenv("LOOM_ENV", environment)
    monkeypatch.setenv("LOOM_SVC_AUTH_LOCAL_HTTP", "true")
    settings = LoomServiceSettings(
        _env_file=None, db_url="postgresql+psycopg://u:p@localhost/loom",
        minio_access_key="x", minio_secret_key="y", public_base_url=public_url,
    )
    options = session_cookie_options(settings)
    assert options["key"] == "__Host-loom_session"
    assert options["secure"] is True


def test_host_prefixed_custom_cookie_never_loses_secure(monkeypatch):
    monkeypatch.setenv("LOOM_SVC_AUTH_LOCAL_HTTP", "true")
    settings = LoomServiceSettings(
        _env_file=None, db_url="postgresql+psycopg://u:p@localhost/loom",
        minio_access_key="x", minio_secret_key="y", auth_session_cookie_name="__Host-custom",
    )
    assert session_cookie_options(settings)["secure"] is True


def test_legacy_canonical_https_origin_cannot_be_downgraded(monkeypatch):
    monkeypatch.setenv("LOOM_SVC_AUTH_LOCAL_HTTP", "true")
    monkeypatch.setenv("LOOM_PUBLIC_BASE_URL", "https://alice.dev.example.com/prod")
    settings = LoomServiceSettings(
        _env_file=None, db_url="postgresql+psycopg://u:p@localhost/loom",
        minio_access_key="x", minio_secret_key="y", public_base_url=None,
    )
    options = session_cookie_options(settings)
    assert options["secure"] is True
    assert options["key"] == "__Host-loom_session"


@pytest.mark.parametrize("origin,allowed", [
    ("https://alice.dev.example.com", True),
    ("https://alice.dev.example.com:443", True),
    ("https://alice.dev.example.com:0", False),
    ("https://bob.dev.example.com", False),
    ("http://alice.dev.example.com", False),
    ("null", False),
    ("https://alice.dev.example.com/", False),
])
def test_hosted_origin_matches_canonical_origin_not_proxy_headers(monkeypatch, origin, allowed):
    monkeypatch.setenv("LOOM_SVC_AUTH_LOCAL_HTTP", "false")
    settings = LoomServiceSettings(
        _env_file=None, db_url="postgresql+psycopg://u:p@localhost/loom",
        minio_access_key="x", minio_secret_key="y",
        public_base_url="https://alice.dev.example.com",
    )
    request = Request({"type": "http", "method": "POST", "scheme": "http", "path": "/login",
                       "server": ("internal-service", 8090), "headers": [
                           (b"origin", origin.encode()),
                           (b"x-forwarded-host", b"bob.dev.example.com"),
                       ]})
    assert browser_origin_allowed(request, settings) is allowed

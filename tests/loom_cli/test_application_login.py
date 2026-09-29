"""Application login preserves management authority and current shared membership."""

from __future__ import annotations

import copy
import json
from uuid import UUID

import httpx
import pytest

from loom_cli.__main__ import main
from loom_cli.config import LoomConfig, config_path, load_config, save_config
from loom_cli.contexts import selected_context
from tests.loom_cli.test_application_cmd import (
    APPLICATION,
    OPERATION,
    RELEASE,
    operation,
    registration,
)

CONTEXT = "app-alice-" + UUID(APPLICATION).hex
ORIGIN = "https://alice.example.com"
PROOF = "loom_app_login_" + UUID(OPERATION).hex + "_" + "a" * 43


@pytest.fixture
def application_login_http(monkeypatch, tmp_xdg_home):
    from loom_cli import server_client

    save_config(LoomConfig(server_url="https://manage.example.com", auth_token="management-secret",
                          tokens={"openai": "management-provider-secret"}))
    state = {"status": {"registration": registration(), "operation": operation("completed")},
             "proof": {"application_id": APPLICATION, "incarnation": OPERATION,
                       "owner_user_id": APPLICATION, "owner_team_id": OPERATION,
                       "deployment_generation": 2, "access_generation": 2,
                       "origin": ORIGIN, "login_token": PROOF, "expires_in": 90},
             "session": {"user": {"id": APPLICATION, "username": "alice", "email": None,
                                   "display_name": "Alice", "is_platform_admin": False},
                         "teams": [{"id": OPERATION, "name": "Team", "role": "member"}],
                         "current_team": {"id": OPERATION, "name": "Team", "role": "member"},
                         "role": "member", "scopes": ["read:own", "submit"],
                         "is_platform_admin": False, "csrf_token": "child-csrf"},
             "cookie": "__Host-loom_session=child-session; Secure; HttpOnly; Path=/; SameSite=Lax",
             "child_status": 200}
    requests = []
    original_client = httpx.Client
    authed = server_client.authed_client

    def handle(request):
        requests.append(request)
        row = state["status"]["registration"]
        if request.url.host == "manage.example.com":
            assert request.headers["authorization"] == "Bearer management-secret"
            if request.method == "GET":
                assert request.url.path == f"/api/v1/applications/{row['application_id']}"
                return httpx.Response(200, json=copy.deepcopy(state["status"]))
            assert request.url.path == f"/api/v1/applications/{row['application_id']}/login"
            proof = copy.deepcopy(state["proof"])
            if sum(r.method == "POST" and r.url.host == "manage.example.com" for r in requests) > 1:
                proof["login_token"] = proof["login_token"][:-43] + "b" * 43
            return httpx.Response(200, json=proof)
        assert str(request.url) == "https://" + row["public_host"] + "/api/v1/auth/login/complete"
        assert not any(key in request.headers for key in ("authorization", "cookie", "x-loom-csrf"))
        token = state["proof"]["login_token"]
        assert json.loads(request.content)["token"] in (token, token[:-43] + "b" * 43)
        return httpx.Response(state["child_status"], json=state["session"], headers={
            "set-cookie": state["cookie"], "location": "https://foreign.example.com",
        })

    # Keep actual authenticated-client construction and fresh child-client flags;
    # only replace external HTTP transport, never credential parsing or saving.
    def client_factory(*args, **kwargs):
        kwargs["transport"] = httpx.MockTransport(handle)
        return original_client(*args, **kwargs)

    monkeypatch.setattr(httpx, "Client", client_factory)
    monkeypatch.setattr(server_client, "authed_client", lambda cfg: authed(cfg, transport=httpx.MockTransport(handle)))
    return state, requests


@pytest.mark.parametrize("role", ["owner", "member", "viewer"])
def test_application_login_saves_only_separate_child_credentials(application_login_http, capsys, role):
    state, requests = application_login_http
    state["session"]["role"] = role
    state["session"]["current_team"]["role"] = role
    original = config_path().read_bytes()
    assert main(["dev", "app", "login", APPLICATION]) == 0
    assert config_path().read_bytes() == original
    with selected_context(CONTEXT):
        cfg = load_config()
        assert cfg.server_url == ORIGIN
        assert cfg.auth_session_cookie == "child-session" and cfg.auth_csrf_token == "child-csrf"
        assert cfg.auth_session_cookie_name == "__Host-loom_session"
        assert cfg.auth_token is None and not cfg.tokens and not cfg.local_providers
        assert cfg.managed_environment is None
        assert cfg.managed_application.application_id == APPLICATION
        assert cfg.managed_application.incarnation == OPERATION
        assert config_path().stat().st_mode & 0o777 == 0o600
    assert [r.url.host for r in requests] == ["manage.example.com", "manage.example.com", "alice.example.com"]
    output = capsys.readouterr()
    assert "loom --context " + CONTEXT in output.out
    for secret in (PROOF, "child-session", "child-csrf", "management-secret"):
        assert secret not in output.out + output.err


@pytest.mark.parametrize("field,value", [
    ("application_id", RELEASE), ("incarnation", RELEASE), ("owner_user_id", RELEASE), ("owner_team_id", RELEASE),
    ("origin", "https://foreign.example.com"), ("deployment_generation", 1), ("access_generation", 1),
    ("deployment_generation", 2.0), ("access_generation", True), ("expires_in", True),
    ("login_token", "loom_env_login_" + "a" * 43),
    ("login_token", "loom_app_login_" + UUID(RELEASE).hex + "_" + "a" * 43),
])
def test_wrong_application_proof_never_reaches_child(application_login_http, field, value):
    state, requests = application_login_http
    state["proof"][field] = value
    assert main(["dev", "app", "login", APPLICATION]) == 1
    assert all(r.url.host == "manage.example.com" for r in requests)
    with selected_context(CONTEXT):
        assert not config_path().exists()


@pytest.mark.parametrize("fault", ["stopped", "pending", "old-deployment", "old-access", "foreign-operation", "no-operation"])
def test_nonready_application_never_requests_proof(application_login_http, fault):
    state, requests = application_login_http
    if fault == "stopped":
        state["status"]["registration"]["desired_state"] = "suspended"
    elif fault == "pending":
        state["status"]["operation"]["phase"] = "pending"
    elif fault == "old-deployment":
        state["status"]["operation"]["deployment_generation"] = 1
    elif fault == "old-access":
        state["status"]["operation"]["access_generation"] = 1
    elif fault == "foreign-operation":
        state["status"]["operation"]["application_id"] = RELEASE
    else:
        state["status"]["operation"] = None
    assert main(["dev", "app", "login", APPLICATION]) == 1
    assert len(requests) == 1 and requests[0].method == "GET"


@pytest.mark.parametrize("fault", ["redirect", "foreign-user", "foreign-team", "admin", "nested-admin",
                                   "bad-role", "missing-csrf", "insecure-cookie", "domain-cookie", "oversized"])
def test_invalid_child_response_never_saves_credentials(application_login_http, fault):
    state, requests = application_login_http
    if fault == "redirect":
        state["child_status"] = 302
    elif fault == "foreign-user":
        state["session"]["user"]["id"] = RELEASE
    elif fault == "foreign-team":
        state["session"]["current_team"]["id"] = RELEASE
    elif fault == "admin":
        state["session"]["is_platform_admin"] = True
    elif fault == "nested-admin":
        state["session"]["user"]["is_platform_admin"] = True
    elif fault == "bad-role":
        state["session"]["role"] = "admin"
    elif fault == "missing-csrf":
        state["session"].pop("csrf_token")
    elif fault == "insecure-cookie":
        state["cookie"] = "__Host-loom_session=insecure; Path=/"
    elif fault == "domain-cookie":
        state["cookie"] += "; Domain=alice.example.com"
    else:
        state["session"]["oversized"] = "x" * 16385
    assert main(["dev", "app", "login", APPLICATION]) == 1
    assert len(requests) == 3
    with selected_context(CONTEXT):
        assert not config_path().exists()


def test_repeat_login_preserves_child_settings_across_generation_change(application_login_http):
    state, _ = application_login_http
    assert main(["dev", "app", "login", APPLICATION]) == 0
    with selected_context(CONTEXT):
        cfg = load_config()
        cfg.tokens["openai"] = "child-explicit-key"
        save_config(cfg)
    for generation in ("deployment_generation", "access_generation"):
        state["status"]["registration"][generation] = 3
        state["status"]["operation"][generation] = 3
        state["proof"][generation] = 3
    assert main(["dev", "app", "login", APPLICATION]) == 0
    with selected_context(CONTEXT):
        assert load_config().tokens == {"openai": "child-explicit-key"}
    assert load_config().tokens == {"openai": "management-provider-secret"}


def test_existing_unbound_context_is_not_replaced(application_login_http):
    _, requests = application_login_http
    with selected_context(CONTEXT):
        save_config(LoomConfig(server_url=ORIGIN, auth_token="unbound-secret"))
        original = config_path().read_bytes()
    assert main(["dev", "app", "login", APPLICATION]) == 1
    assert len(requests) == 1
    with selected_context(CONTEXT):
        assert config_path().read_bytes() == original


def test_two_owner_logins_keep_distinct_contexts(application_login_http):
    state, _ = application_login_http
    assert main(["dev", "app", "login", APPLICATION]) == 0
    with selected_context(CONTEXT):
        alice_bytes = config_path().read_bytes()
    row = state["status"]["registration"]
    row.update(application_id=RELEASE, incarnation=APPLICATION, owner_user_id=RELEASE,
               owner_team_id=RELEASE, slug="bob", application_namespace="loom-dev-bob", public_host="bob.example.com")
    state["status"]["operation"]["application_id"] = RELEASE
    state["proof"].update(application_id=RELEASE, incarnation=APPLICATION, owner_user_id=RELEASE,
                          owner_team_id=RELEASE, origin="https://bob.example.com",
                          login_token="loom_app_login_" + UUID(RELEASE).hex + "_" + "a" * 43)
    state["session"]["user"]["id"] = RELEASE
    state["session"]["current_team"]["id"] = RELEASE
    state["cookie"] = state["cookie"].replace("child-session", "bob-session")
    assert main(["dev", "app", "login", RELEASE]) == 0
    with selected_context("app-bob-" + UUID(RELEASE).hex):
        cfg = load_config()
        assert cfg.auth_session_cookie == "bob-session" and cfg.server_url == "https://bob.example.com"
        assert cfg.managed_application.application_id == RELEASE
    with selected_context(CONTEXT):
        assert config_path().read_bytes() == alice_bytes
    assert load_config().auth_token == "management-secret"


def test_failed_login_save_keeps_existing_credentials_and_redacts_error(application_login_http, monkeypatch, capsys):
    from loom_cli import application_login

    assert main(["dev", "app", "login", APPLICATION]) == 0
    original_management = config_path().read_bytes()
    with selected_context(CONTEXT):
        original_child = config_path().read_bytes()

    def fail_save(cfg):
        raise OSError("private-error-" + PROOF)

    monkeypatch.setattr(application_login, "save_config", fail_save)
    assert main(["dev", "app", "login", APPLICATION]) == 1
    assert config_path().read_bytes() == original_management
    with selected_context(CONTEXT):
        assert config_path().read_bytes() == original_child
    output = capsys.readouterr()
    assert "private-error" not in output.out + output.err and PROOF not in output.out + output.err


@pytest.mark.parametrize("opened", [True, False])
def test_browser_login_issues_distinct_fragment_proof(application_login_http, monkeypatch, capsys, opened):
    import webbrowser
    from urllib.parse import parse_qs, urlsplit

    launched = []
    monkeypatch.setattr(webbrowser, "open", lambda url, **kwargs: launched.append(url) or opened)
    assert main(["dev", "app", "login", APPLICATION, "--browser"]) == (0 if opened else 1)
    assert len(launched) == 1
    url = urlsplit(launched[0])
    assert url.scheme == "https" and url.netloc == "alice.example.com" and url.path == "/auth/managed"
    assert not url.query
    assert parse_qs(url.fragment) == {"token": [PROOF[:-43] + "b" * 43]}
    output = capsys.readouterr()
    assert "loom_app_login_" not in output.out + output.err

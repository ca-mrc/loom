"""`loom providers {create,list,show,update,delete}` against a mocked
server (httpx MockTransport). The route layer's behavior is exercised
by `tests/integration/test_provider_connections_routes.py`; this file
exercises the CLI surface: argparse, payload shape, output format,
error paths."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import httpx
import pytest

from loom_cli.__main__ import main


@pytest.fixture(autouse=True)
def _isolated_logged_in_config(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    """Isolated config + already-logged-in. Tests that want to test the
    not-logged-in path clear via `loom auth logout`."""
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path))
    monkeypatch.setenv("MY_TOK", "loom_admin_test123456")
    main([
        "auth", "login",
        "--server", "https://loom.test",
        "--token", "env:MY_TOK",
    ])


class MockServer:
    """Wraps the MockTransport state so tests can both adjust canned
    responses (via .canned[(method, path)] = httpx.Response(...)) AND
    inspect the recorded requests (via iterating or indexing)."""

    def __init__(self) -> None:
        self.requests: list[httpx.Request] = []
        self.canned: dict[tuple[str, str], httpx.Response] = {}

    def __getitem__(self, idx: int) -> httpx.Request:
        return self.requests[idx]

    def __len__(self) -> int:
        return len(self.requests)

    def __eq__(self, other: object) -> bool:
        return self.requests == other


@pytest.fixture
def mock_server(
    monkeypatch: pytest.MonkeyPatch,
) -> MockServer:
    """Patch `loom_cli.providers_cmd.authed_client` to return an
    httpx.Client backed by a MockTransport. Returned MockServer records
    every outgoing request + lets tests inject canned responses."""
    server = MockServer()

    def _handler(request: httpx.Request) -> httpx.Response:
        server.requests.append(request)
        key = (request.method, request.url.path)
        if key in server.canned:
            return server.canned[key]
        return httpx.Response(404, json={"detail": f"no mock for {key}"})

    transport = httpx.MockTransport(_handler)

    def _patched_authed_client(cfg: Any, *, timeout: float = 30.0) -> httpx.Client:
        return httpx.Client(
            base_url=cfg.server_url,
            headers={"Authorization": f"Bearer {cfg.auth_token}"},
            transport=transport,
            timeout=timeout,
        )

    monkeypatch.setattr(
        "loom_cli.providers_cmd.authed_client", _patched_authed_client,
    )
    return server


def _make_connection(
    *, name: str = "openai-prod", type_: str = "openai-compatible",
    pricing_mode: str = "usage_only",
    custom_pricing: dict | None = None,
    catalog_id: str | None = "openai",
    status: str = "pending",
    allowed_models: list[str] | None = None,
) -> dict[str, Any]:
    return {
        "id": "00000000-0000-0000-0000-000000000001",
        "team_id": "00000000-0000-0000-0000-000000000000",
        "name": name,
        "type": type_,
        "base_url": "https://api.openai.com/v1",
        "upstream_host": "api.openai.com",
        "resolved_egress_ips": ["104.18.0.1"],
        "allowed_models": allowed_models,
        "status": status,
        "last_validated_at": None,
        "last_validation_error": None,
        "pricing_mode": pricing_mode,
        "custom_pricing": custom_pricing,
        "catalog_id": catalog_id,
        "created_by": "admin:abc12345",
        "created_at": "2026-06-16T00:00:00Z",
        "updated_at": "2026-06-16T00:00:00Z",
    }


# ──────────────────────────────────────────────────────────────────────
# create
# ──────────────────────────────────────────────────────────────────────


def test_create_happy_path_posts_correct_payload(
    monkeypatch: pytest.MonkeyPatch,
    mock_server: MockServer,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setenv("OPENAI_KEY", "sk-test-XXXX")
    mock_server.canned[("POST", "/api/v1/provider-connections")] = httpx.Response(
        201, json=_make_connection(name="openai-prod"),
    )

    rc = main([
        "providers", "create",
        "--name", "openai-prod",
        "--type", "openai-compatible",
        "--base-url", "https://api.openai.com/v1",
        "--api-key", "env:OPENAI_KEY",
    ])
    assert rc == 0
    out = capsys.readouterr().out
    assert "Created provider connection 'openai-prod'" in out
    assert "name:          openai-prod" in out
    assert "Next steps:" in out
    assert "loom providers test openai-prod" in out
    assert "loom providers models openai-prod --refresh" in out

    # Verify the request shape.
    assert len(mock_server) == 1
    req = mock_server[0]
    assert req.method == "POST"
    body = json.loads(req.content)
    assert body == {
        "name": "openai-prod",
        "type": "openai-compatible",
        "base_url": "https://api.openai.com/v1",
        "api_key": "sk-test-XXXX",  # resolved from env
    }
    # Authorization header carried.
    assert req.headers["Authorization"] == "Bearer loom_admin_test123456"


@pytest.mark.parametrize(("provider_type", "noted"), [
    ("openai-compatible", False), ("custom", False), ("anthropic", True), ("google", True),
])
def test_create_notes_native_types_are_not_for_agent_runs(
    monkeypatch: pytest.MonkeyPatch,
    mock_server: MockServer,
    capsys: pytest.CaptureFixture[str],
    provider_type: str,
    noted: bool,
) -> None:
    monkeypatch.setenv("K", "k123")
    mock_server.canned[("POST", "/api/v1/provider-connections")] = httpx.Response(
        201, json=_make_connection(name="n", type_=provider_type),
    )
    rc = main([
        "providers", "create", "--name", "n", "--type", provider_type,
        "--base-url", "https://api.example.com", "--api-key", "env:K",
    ])
    assert rc == 0
    assert ("agent batches and trials can't use this connection" in capsys.readouterr().err) is noted


def test_create_with_operator_pricing_sends_custom_pricing(
    monkeypatch: pytest.MonkeyPatch,
    mock_server: MockServer,
) -> None:
    monkeypatch.setenv("K", "k123")
    mock_server.canned[("POST", "/api/v1/provider-connections")] = httpx.Response(
        201, json=_make_connection(
            pricing_mode="custom",
            custom_pricing={"input_usd_per_1m": 2.5, "output_usd_per_1m": 10.0},
        ),
    )
    rc = main([
        "providers", "create",
        "--name", "n", "--type", "openai-compatible",
        "--base-url", "https://api.openai.com/v1",
        "--api-key", "env:K",
        "--price-model", "model-a",
        "--input-usd-per-1m", "2.5",
        "--output-usd-per-1m", "10.0",
    ])
    assert rc == 0
    body = json.loads(mock_server[0].content)
    assert body["pricing_mode"] == "custom"
    assert body["custom_pricing"] == {"model-a": {
        "input_usd_per_1m": 2.5,
        "output_usd_per_1m": 10.0,
        "cache_read_usd_per_1m": None,
        "cache_write_usd_per_1m": None,
    }}


def test_create_with_catalog_id_sends_field(
    monkeypatch: pytest.MonkeyPatch,
    mock_server: MockServer,
) -> None:
    monkeypatch.setenv("K", "k123")
    mock_server.canned[("POST", "/api/v1/provider-connections")] = httpx.Response(
        201, json=_make_connection(catalog_id="together"),
    )

    rc = main([
        "providers", "create",
        "--name", "together-prod", "--type", "openai-compatible",
        "--base-url", "https://api.together.xyz/v1",
        "--api-key", "env:K",
        "--catalog-id", "together",
    ])
    assert rc == 0
    body = json.loads(mock_server[0].content)
    assert body["catalog_id"] == "together"


def test_create_half_set_pricing_rejected(
    monkeypatch: pytest.MonkeyPatch,
    mock_server: MockServer,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """--input-usd-per-1m without --output-usd-per-1m must fail at the
    CLI layer, BEFORE the HTTP call goes out."""
    monkeypatch.setenv("K", "k123")
    rc = main([
        "providers", "create",
        "--name", "n", "--type", "openai-compatible",
        "--base-url", "https://api.openai.com/v1",
        "--api-key", "env:K",
        "--price-model", "model-a",
        "--input-usd-per-1m", "2.5",  # but no --output-usd-per-1m
    ])
    assert rc == 2
    assert "output_usd_per_1m" in capsys.readouterr().err
    # No HTTP call was made.
    assert mock_server == []


def test_create_api_key_literal_rejected_at_argparse(
    capsys: pytest.CaptureFixture[str],
) -> None:
    with pytest.raises(SystemExit) as exc:
        main([
            "providers", "create",
            "--name", "n", "--type", "openai-compatible",
            "--base-url", "https://api.openai.com/v1",
            "--api-key", "sk-literal",  # rejected
        ])
    assert exc.value.code == 2
    assert "literal values are rejected" in capsys.readouterr().err


def test_create_server_error_surfaces_detail(
    monkeypatch: pytest.MonkeyPatch,
    mock_server: MockServer,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setenv("K", "k1234567")
    mock_server.canned[("POST", "/api/v1/provider-connections")] = httpx.Response(
        409, json={"detail": "a provider_connection named 'n' already exists"},
    )
    rc = main([
        "providers", "create",
        "--name", "n", "--type", "openai-compatible",
        "--base-url", "https://api.openai.com/v1",
        "--api-key", "env:K",
    ])
    assert rc == 1
    err = capsys.readouterr().err
    assert "HTTP 409" in err
    assert "already exists" in err


# ──────────────────────────────────────────────────────────────────────
# list
# ──────────────────────────────────────────────────────────────────────


def test_list_empty(
    mock_server: MockServer, capsys: pytest.CaptureFixture[str],
) -> None:
    mock_server.canned[("GET", "/api/v1/provider-connections")] = httpx.Response(
        200, json={"items": []},
    )
    rc = main(["providers", "list"])
    assert rc == 0
    out = capsys.readouterr().out
    assert "no provider connections" in out


def test_list_table_format(
    mock_server: MockServer, capsys: pytest.CaptureFixture[str],
) -> None:
    mock_server.canned[("GET", "/api/v1/provider-connections")] = httpx.Response(
        200, json={"items": [
            _make_connection(name="openai-prod"),
            _make_connection(name="anthropic-dev", type_="anthropic",
                             pricing_mode="catalog"),
        ]},
    )
    rc = main(["providers", "list"])
    assert rc == 0
    out = capsys.readouterr().out
    assert "openai-prod" in out
    assert "anthropic-dev" in out
    assert "openai-compatible" in out
    assert "anthropic" in out


def test_list_json_format(
    mock_server: MockServer, capsys: pytest.CaptureFixture[str],
) -> None:
    items = [_make_connection(name="x")]
    mock_server.canned[("GET", "/api/v1/provider-connections")] = httpx.Response(
        200, json={"items": items},
    )
    rc = main(["providers", "list", "--format", "json"])
    assert rc == 0
    parsed = json.loads(capsys.readouterr().out)
    assert parsed == items


# ──────────────────────────────────────────────────────────────────────
# show / update / delete — exercise the _resolve_by_name lookup path
# ──────────────────────────────────────────────────────────────────────


def test_show_resolves_by_name_then_prints(
    mock_server: MockServer, capsys: pytest.CaptureFixture[str],
) -> None:
    conn = _make_connection(name="openai-prod")
    mock_server.canned[("GET", "/api/v1/provider-connections")] = httpx.Response(
        200, json={"items": [conn]},
    )
    rc = main(["providers", "show", "openai-prod"])
    assert rc == 0
    out = capsys.readouterr().out
    assert "name:          openai-prod" in out
    assert "catalog:       openai" in out


def test_show_unknown_name_returns_1_with_helpful_message(
    mock_server: MockServer, capsys: pytest.CaptureFixture[str],
) -> None:
    mock_server.canned[("GET", "/api/v1/provider-connections")] = httpx.Response(
        200, json={"items": []},
    )
    rc = main(["providers", "show", "nope"])
    assert rc == 1
    err = capsys.readouterr().err
    assert "no provider connection named 'nope'" in err
    assert "loom providers list" in err


def test_update_patches_by_resolved_id(
    monkeypatch: pytest.MonkeyPatch,
    mock_server: MockServer,
) -> None:
    conn = _make_connection(name="openai-prod")
    mock_server.canned[("GET", "/api/v1/provider-connections")] = httpx.Response(
        200, json={"items": [conn]},
    )
    mock_server.canned[
        ("PATCH", f"/api/v1/provider-connections/{conn['id']}")
    ] = httpx.Response(200, json=conn)

    rc = main([
        "providers", "update", "openai-prod",
        "--base-url", "https://api.openai.com/v2",
    ])
    assert rc == 0
    # First call is the GET (resolution), second is the PATCH.
    assert mock_server[0].method == "GET"
    assert mock_server[1].method == "PATCH"
    patch_body = json.loads(mock_server[1].content)
    assert patch_body == {"base_url": "https://api.openai.com/v2"}


def test_update_catalog_id_patches_by_resolved_id(
    mock_server: MockServer,
) -> None:
    conn = _make_connection(name="openai-prod")
    updated = _make_connection(name="openai-prod", catalog_id="fireworks")
    mock_server.canned[("GET", "/api/v1/provider-connections")] = httpx.Response(
        200, json={"items": [conn]},
    )
    mock_server.canned[
        ("PATCH", f"/api/v1/provider-connections/{conn['id']}")
    ] = httpx.Response(200, json=updated)

    rc = main([
        "providers", "update", "openai-prod",
        "--catalog-id", "fireworks",
    ])
    assert rc == 0
    patch_body = json.loads(mock_server[1].content)
    assert patch_body == {"catalog_id": "fireworks"}


def test_update_with_admin_actor_sends_audit_header(
    mock_server: MockServer,
) -> None:
    conn = _make_connection(name="openai-prod")
    mock_server.canned[("GET", "/api/v1/provider-connections")] = httpx.Response(
        200, json={"items": [conn]},
    )
    mock_server.canned[
        ("PATCH", f"/api/v1/provider-connections/{conn['id']}")
    ] = httpx.Response(200, json=conn)

    rc = main([
        "providers", "update", "openai-prod",
        "--pricing-mode", "catalog",
        "--catalog-id", "openai",
        "--admin-actor", "release-operator",
    ])

    assert rc == 0
    assert mock_server[1].headers["X-Loom-Admin-Actor"] == "release-operator"


def test_update_pricing_mode_rate_card_patches_by_resolved_id(
    mock_server: MockServer,
) -> None:
    conn = _make_connection(name="yibuapi-prod")
    updated = _make_connection(
        name="yibuapi-prod",
        pricing_mode="catalog",
        catalog_id="yibuapi",
    )
    mock_server.canned[("GET", "/api/v1/provider-connections")] = httpx.Response(
        200, json={"items": [conn]},
    )
    mock_server.canned[
        ("PATCH", f"/api/v1/provider-connections/{conn['id']}")
    ] = httpx.Response(200, json=updated)

    rc = main([
        "providers", "update", "yibuapi-prod",
        "--pricing-mode", "catalog",
        "--catalog-id", "yibuapi",
    ])

    assert rc == 0
    patch_body = json.loads(mock_server[1].content)
    assert patch_body == {
        "pricing_mode": "catalog",
        "catalog_id": "yibuapi",
    }


def test_update_with_no_changes_errors(
    capsys: pytest.CaptureFixture[str],
) -> None:
    rc = main(["providers", "update", "n"])
    assert rc == 2
    err = capsys.readouterr().err
    assert "requires at least one of" in err


def test_update_api_key_resolves_via_source(
    monkeypatch: pytest.MonkeyPatch,
    mock_server: MockServer,
    tmp_path: Path,
) -> None:
    key_file = tmp_path / "new-key.txt"
    key_file.write_text("new-secret-key\n")
    conn = _make_connection(name="x")
    mock_server.canned[("GET", "/api/v1/provider-connections")] = httpx.Response(
        200, json={"items": [conn]},
    )
    mock_server.canned[
        ("PATCH", f"/api/v1/provider-connections/{conn['id']}")
    ] = httpx.Response(200, json=conn)
    rc = main([
        "providers", "update", "x",
        "--api-key", f"file:{key_file}",
    ])
    assert rc == 0
    patch_body = json.loads(mock_server[1].content)
    assert patch_body == {"api_key": "new-secret-key"}


def test_delete_resolves_then_calls_delete(
    mock_server: MockServer, capsys: pytest.CaptureFixture[str],
) -> None:
    conn = _make_connection(name="openai-prod")
    mock_server.canned[("GET", "/api/v1/provider-connections")] = httpx.Response(
        200, json={"items": [conn]},
    )
    mock_server.canned[
        ("DELETE", f"/api/v1/provider-connections/{conn['id']}")
    ] = httpx.Response(204)
    rc = main(["providers", "delete", "openai-prod"])
    assert rc == 0
    out = capsys.readouterr().out
    assert "Soft-deleted provider connection 'openai-prod'" in out
    assert mock_server[1].method == "DELETE"


# ──────────────────────────────────────────────────────────────────────
# share / unshare
# ──────────────────────────────────────────────────────────────────────


def test_share_resolves_then_posts_target_team(
    mock_server: MockServer, capsys: pytest.CaptureFixture[str],
) -> None:
    conn = _make_connection(name="yibuapi-prod")
    target_team = "11111111-1111-1111-1111-111111111111"
    mock_server.canned[("GET", "/api/v1/provider-connections")] = httpx.Response(
        200, json={"items": [conn]},
    )
    mock_server.canned[
        ("POST", f"/api/v1/provider-connections/{conn['id']}/shares")
    ] = httpx.Response(201, json={
        "provider_connection_id": conn["id"],
        "provider_name": "yibuapi-prod",
        "provider_owner_team_id": conn["team_id"],
        "target_team_id": target_team,
    })

    rc = main([
        "providers", "share", "yibuapi-prod",
        "--target-team-id", target_team,
        "--admin-actor", "release-operator",
    ])

    assert rc == 0
    out = capsys.readouterr().out
    assert "Shared provider connection 'yibuapi-prod'" in out
    assert target_team in out
    assert "api_key" not in out
    assert mock_server[1].method == "POST"
    assert mock_server[1].url.path == (
        f"/api/v1/provider-connections/{conn['id']}/shares"
    )
    assert json.loads(mock_server[1].content) == {
        "target_team_id": target_team,
    }
    assert mock_server[1].headers["X-Loom-Admin-Actor"] == "release-operator"


def test_unshare_resolves_then_deletes_target_team(
    mock_server: MockServer, capsys: pytest.CaptureFixture[str],
) -> None:
    conn = _make_connection(name="yibuapi-prod")
    target_team = "11111111-1111-1111-1111-111111111111"
    mock_server.canned[("GET", "/api/v1/provider-connections")] = httpx.Response(
        200, json={"items": [conn]},
    )
    mock_server.canned[
        (
            "DELETE",
            f"/api/v1/provider-connections/{conn['id']}/shares/{target_team}",
        )
    ] = httpx.Response(204)

    rc = main([
        "providers", "unshare", "yibuapi-prod",
        "--target-team-id", target_team,
    ])

    assert rc == 0
    out = capsys.readouterr().out
    assert "Unshared provider connection 'yibuapi-prod'" in out
    assert target_team in out
    assert "api_key" not in out
    assert mock_server[1].method == "DELETE"
    assert mock_server[1].url.path == (
        f"/api/v1/provider-connections/{conn['id']}/shares/{target_team}"
    )


# ──────────────────────────────────────────────────────────────────────
# test
# ──────────────────────────────────────────────────────────────────────


def test_test_valid_returns_0(
    mock_server: MockServer, capsys: pytest.CaptureFixture[str],
) -> None:
    """A valid probe response → rc=0 with status printed."""
    conn = _make_connection(name="openai-prod")
    mock_server.canned[("GET", "/api/v1/provider-connections")] = httpx.Response(
        200, json={"items": [conn]},
    )
    mock_server.canned[
        ("POST", f"/api/v1/provider-connections/{conn['id']}/test")
    ] = httpx.Response(200, json={
        "connection_id": conn["id"],
        "status": "valid",
        "http_status": 200,
        "last_validation_error": None,
        "last_validated_at": "2026-06-16T12:00:00Z",
    })
    rc = main(["providers", "test", "openai-prod"])
    assert rc == 0
    out = capsys.readouterr().out
    assert "status:                valid" in out
    assert "http_status:           200" in out
    # 1 GET (resolution) + 1 POST (test).
    assert mock_server[1].method == "POST"
    assert mock_server[1].url.path == (
        f"/api/v1/provider-connections/{conn['id']}/test"
    )


def test_test_invalid_returns_1_and_prints_error(
    mock_server: MockServer, capsys: pytest.CaptureFixture[str],
) -> None:
    """`invalid` status → rc=1 + error printed; CI-greppable."""
    conn = _make_connection(name="openai-prod")
    mock_server.canned[("GET", "/api/v1/provider-connections")] = httpx.Response(
        200, json={"items": [conn]},
    )
    mock_server.canned[
        ("POST", f"/api/v1/provider-connections/{conn['id']}/test")
    ] = httpx.Response(200, json={
        "connection_id": conn["id"],
        "status": "invalid",
        "http_status": 401,
        "last_validation_error": "HTTP 401 from .../models; body excerpt: 'invalid api key'",
        "last_validated_at": "2026-06-16T12:00:00Z",
    })
    rc = main(["providers", "test", "openai-prod"])
    assert rc == 1
    out = capsys.readouterr().out
    assert "status:                invalid" in out
    assert "http_status:           401" in out
    assert "invalid api key" in out


def test_test_unreachable_omits_http_status(
    mock_server: MockServer, capsys: pytest.CaptureFixture[str],
) -> None:
    """When the probe never reached an HTTP server, http_status=None;
    rendering should NOT print a `http_status:` line."""
    conn = _make_connection(name="openai-prod")
    mock_server.canned[("GET", "/api/v1/provider-connections")] = httpx.Response(
        200, json={"items": [conn]},
    )
    mock_server.canned[
        ("POST", f"/api/v1/provider-connections/{conn['id']}/test")
    ] = httpx.Response(200, json={
        "connection_id": conn["id"],
        "status": "invalid",
        "http_status": None,
        "last_validation_error": "timeout after 5.0s",
        "last_validated_at": "2026-06-16T12:00:00Z",
    })
    rc = main(["providers", "test", "openai-prod"])
    assert rc == 1
    out = capsys.readouterr().out
    assert "http_status:" not in out
    assert "timeout" in out


def test_test_unknown_name_returns_1(
    mock_server: MockServer, capsys: pytest.CaptureFixture[str],
) -> None:
    mock_server.canned[("GET", "/api/v1/provider-connections")] = httpx.Response(
        200, json={"items": []},
    )
    rc = main(["providers", "test", "nope"])
    assert rc == 1
    err = capsys.readouterr().err
    assert "no provider connection named 'nope'" in err


def test_test_tolerates_malformed_server_response(
    mock_server: MockServer, capsys: pytest.CaptureFixture[str],
) -> None:
    """Server returned 200 but with fields missing — handler must NOT
    crash with KeyError. Renders status='unknown' and returns 1 (not
    valid)."""
    conn = _make_connection(name="openai-prod")
    mock_server.canned[("GET", "/api/v1/provider-connections")] = httpx.Response(
        200, json={"items": [conn]},
    )
    mock_server.canned[
        ("POST", f"/api/v1/provider-connections/{conn['id']}/test")
    ] = httpx.Response(200, json={"connection_id": conn["id"]})  # no status
    rc = main(["providers", "test", "openai-prod"])
    assert rc == 1
    out = capsys.readouterr().out
    assert "status:                unknown" in out


# ──────────────────────────────────────────────────────────────────────
# models
# ──────────────────────────────────────────────────────────────────────


def _make_model(
    *, model_id: str, visible: bool = True,
    hidden_reason: str | None = None, upstream_present: bool = True,
    last_preflight_status: str | None = None,
    last_preflight_http_status: int | None = None,
    last_preflight_error_code: str | None = None,
    last_preflight_error_message: str | None = None,
    last_preflight_failure_kind: str | None = None,
) -> dict[str, Any]:
    return {
        "model_id": model_id,
        "family": None,
        "context_length": None,
        "capabilities": {},
        "visible": visible,
        "hidden_reason": hidden_reason,
        "last_seen_at": "2026-06-16T00:00:00Z",
        "upstream_present": upstream_present,
        "last_preflight_status": last_preflight_status,
        "last_preflight_at": (
            "2026-06-16T00:01:00Z" if last_preflight_status else None
        ),
        "last_preflight_http_status": last_preflight_http_status,
        "last_preflight_error_code": last_preflight_error_code,
        "last_preflight_error_message": last_preflight_error_message,
        "last_preflight_failure_kind": last_preflight_failure_kind,
    }


def test_models_empty_list_hints_refresh(
    mock_server: MockServer, capsys: pytest.CaptureFixture[str],
) -> None:
    conn = _make_connection(name="openai-prod")
    mock_server.canned[("GET", "/api/v1/provider-connections")] = httpx.Response(
        200, json={"items": [conn]},
    )
    mock_server.canned[
        ("GET", f"/api/v1/provider-connections/{conn['id']}/models")
    ] = httpx.Response(200, json={"items": []})
    rc = main(["providers", "models", "openai-prod"])
    assert rc == 0
    out = capsys.readouterr().out
    assert "no models cached" in out
    assert "--refresh" in out


def test_models_table_renders_visible_and_hidden(
    mock_server: MockServer, capsys: pytest.CaptureFixture[str],
) -> None:
    conn = _make_connection(name="openai-prod")
    mock_server.canned[("GET", "/api/v1/provider-connections")] = httpx.Response(
        200, json={"items": [conn]},
    )
    mock_server.canned[
        ("GET", f"/api/v1/provider-connections/{conn['id']}/models")
    ] = httpx.Response(200, json={"items": [
        _make_model(model_id="gpt-4o", last_preflight_status="valid",
                    last_preflight_http_status=200),
        _make_model(model_id="gpt-3.5", visible=False,
                    hidden_reason="operator-hidden"),
        _make_model(model_id="gpt-stale", visible=False,
                    hidden_reason="missing-upstream",
                    upstream_present=False),
        _make_model(model_id="gpt-private", last_preflight_status="failed",
                    last_preflight_http_status=403,
                    last_preflight_error_code="access-denied"),
    ]})
    rc = main(["providers", "models", "openai-prod"])
    assert rc == 0
    out = capsys.readouterr().out
    assert "gpt-4o" in out
    assert "[visible,preflight=valid]" in out
    assert "preflight=valid" in out
    assert "gpt-3.5" in out
    assert "operator-hidden" in out
    assert "gpt-stale" in out
    assert "missing-upstream" in out
    assert "gpt-private" in out
    assert "preflight=failed" in out
    assert "access-denied" in out


def test_models_json_format(
    mock_server: MockServer, capsys: pytest.CaptureFixture[str],
) -> None:
    conn = _make_connection(name="openai-prod")
    items = [_make_model(model_id="gpt-4o")]
    mock_server.canned[("GET", "/api/v1/provider-connections")] = httpx.Response(
        200, json={"items": [conn]},
    )
    mock_server.canned[
        ("GET", f"/api/v1/provider-connections/{conn['id']}/models")
    ] = httpx.Response(200, json={"items": items})
    rc = main(["providers", "models", "openai-prod", "--format", "json"])
    assert rc == 0
    parsed = json.loads(capsys.readouterr().out)
    assert parsed == items


def test_models_refresh_posts_then_lists(
    mock_server: MockServer, capsys: pytest.CaptureFixture[str],
) -> None:
    conn = _make_connection(name="openai-prod")
    mock_server.canned[("GET", "/api/v1/provider-connections")] = httpx.Response(
        200, json={"items": [conn]},
    )
    items = [_make_model(model_id="gpt-4o")]
    mock_server.canned[(
        "POST", f"/api/v1/provider-connections/{conn['id']}/models/refresh",
    )] = httpx.Response(200, json={
        "added": 1, "refreshed": 0, "missing": 0, "items": items,
    })
    mock_server.canned[
        ("GET", f"/api/v1/provider-connections/{conn['id']}/models")
    ] = httpx.Response(200, json={"items": items})

    rc = main(["providers", "models", "openai-prod", "--refresh"])
    assert rc == 0
    out = capsys.readouterr().out
    assert "Refreshed: +1 new" in out
    assert "gpt-4o" in out
    # POST refresh comes before GET list (after the GET-to-resolve-name).
    methods = [(r.method, r.url.path) for r in mock_server.requests]
    assert methods == [
        ("GET", "/api/v1/provider-connections"),
        ("POST", f"/api/v1/provider-connections/{conn['id']}/models/refresh"),
        ("GET", f"/api/v1/provider-connections/{conn['id']}/models"),
    ]


def test_models_refresh_with_admin_actor_sends_audit_header(
    mock_server: MockServer,
) -> None:
    conn = _make_connection(name="openai-prod")
    mock_server.canned[("GET", "/api/v1/provider-connections")] = httpx.Response(
        200, json={"items": [conn]},
    )
    refresh_path = f"/api/v1/provider-connections/{conn['id']}/models/refresh"
    mock_server.canned[("POST", refresh_path)] = httpx.Response(
        200,
        json={"added": 0, "refreshed": 0, "missing": 0, "items": []},
    )
    mock_server.canned[
        ("GET", f"/api/v1/provider-connections/{conn['id']}/models")
    ] = httpx.Response(200, json={"items": []})

    rc = main([
        "providers", "models", "openai-prod", "--refresh",
        "--admin-actor", "release-operator",
    ])

    assert rc == 0
    refresh_request = next(
        request for request in mock_server.requests
        if request.method == "POST" and request.url.path == refresh_path
    )
    assert refresh_request.headers["X-Loom-Admin-Actor"] == "release-operator"


def test_models_preflight_timeout_reports_inconclusive_non_blocking(
    mock_server: MockServer, capsys: pytest.CaptureFixture[str],
) -> None:
    """#948: a timed-out probe is labelled inconclusive, not a rejection."""
    conn = _make_connection(name="openai-prod")
    mock_server.canned[("GET", "/api/v1/provider-connections")] = httpx.Response(
        200, json={"items": [conn]},
    )
    timed_out = _make_model(
        model_id="slow-reasoner",
        last_preflight_status="failed",
        last_preflight_http_status=None,
        last_preflight_error_code="timeout",
        last_preflight_error_message="timeout after 20.0s: read timeout",
        last_preflight_failure_kind="inconclusive",
    )
    mock_server.canned[(
        "POST",
        f"/api/v1/provider-connections/{conn['id']}/models/slow-reasoner/preflight",
    )] = httpx.Response(200, json=timed_out)
    mock_server.canned[
        ("GET", f"/api/v1/provider-connections/{conn['id']}/models")
    ] = httpx.Response(200, json={"items": [timed_out]})

    rc = main([
        "providers", "models", "openai-prod", "--preflight", "slow-reasoner",
    ])

    assert rc == 0
    out = capsys.readouterr().out
    assert "does not block batch submission" in out
    assert "preflight=failed,timeout,inconclusive" in out


def test_models_preflight_posts_then_lists(
    mock_server: MockServer, capsys: pytest.CaptureFixture[str],
) -> None:
    conn = _make_connection(name="openai-prod")
    mock_server.canned[("GET", "/api/v1/provider-connections")] = httpx.Response(
        200, json={"items": [conn]},
    )
    failed = _make_model(
        model_id="gpt-private",
        last_preflight_status="failed",
        last_preflight_http_status=403,
        last_preflight_error_code="access-denied",
        last_preflight_error_message="HTTP 403 from upstream: [REDACTED]",
    )
    mock_server.canned[(
        "POST",
        f"/api/v1/provider-connections/{conn['id']}/models/gpt-private/preflight",
    )] = httpx.Response(200, json=failed)
    mock_server.canned[
        ("GET", f"/api/v1/provider-connections/{conn['id']}/models")
    ] = httpx.Response(200, json={"items": [failed]})

    rc = main([
        "providers", "models", "openai-prod", "--preflight", "gpt-private",
    ])

    assert rc == 0
    out = capsys.readouterr().out
    assert "Preflighted model 'gpt-private': failed" in out
    assert "access-denied" in out
    methods = [(r.method, r.url.path) for r in mock_server.requests]
    assert methods == [
        ("GET", "/api/v1/provider-connections"),
        (
            "POST",
            f"/api/v1/provider-connections/{conn['id']}/models/gpt-private/preflight",
        ),
        ("GET", f"/api/v1/provider-connections/{conn['id']}/models"),
    ]


def test_models_preflight_with_admin_actor_sends_audit_header(
    mock_server: MockServer,
) -> None:
    conn = _make_connection(name="openai-prod")
    mock_server.canned[("GET", "/api/v1/provider-connections")] = httpx.Response(
        200, json={"items": [conn]},
    )
    preflight_path = (
        f"/api/v1/provider-connections/{conn['id']}/models/gpt-private/preflight"
    )
    model = _make_model(model_id="gpt-private", last_preflight_status="valid")
    mock_server.canned[("POST", preflight_path)] = httpx.Response(200, json=model)
    mock_server.canned[
        ("GET", f"/api/v1/provider-connections/{conn['id']}/models")
    ] = httpx.Response(200, json={"items": [model]})

    rc = main([
        "providers", "models", "openai-prod", "--preflight", "gpt-private",
        "--admin-actor", "release-operator",
    ])

    assert rc == 0
    preflight_request = next(
        request for request in mock_server.requests
        if request.method == "POST" and request.url.path == preflight_path
    )
    assert preflight_request.headers["X-Loom-Admin-Actor"] == "release-operator"


def test_models_hide_posts_hide_then_lists(
    mock_server: MockServer, capsys: pytest.CaptureFixture[str],
) -> None:
    conn = _make_connection(name="openai-prod")
    mock_server.canned[("GET", "/api/v1/provider-connections")] = httpx.Response(
        200, json={"items": [conn]},
    )
    mock_server.canned[(
        "POST", f"/api/v1/provider-connections/{conn['id']}/models/gpt-3.5/hide",
    )] = httpx.Response(200, json=_make_model(
        model_id="gpt-3.5", visible=False, hidden_reason="operator-hidden",
    ))
    mock_server.canned[
        ("GET", f"/api/v1/provider-connections/{conn['id']}/models")
    ] = httpx.Response(200, json={"items": []})

    rc = main([
        "providers", "models", "openai-prod", "--hide", "gpt-3.5",
    ])
    assert rc == 0
    out = capsys.readouterr().out
    assert "Hid model 'gpt-3.5'" in out
    methods = [(r.method, r.url.path) for r in mock_server.requests]
    assert ("POST",
            f"/api/v1/provider-connections/{conn['id']}/models/gpt-3.5/hide",
            ) in methods


def test_models_unhide_posts_unhide_then_lists(
    mock_server: MockServer, capsys: pytest.CaptureFixture[str],
) -> None:
    conn = _make_connection(name="openai-prod")
    mock_server.canned[("GET", "/api/v1/provider-connections")] = httpx.Response(
        200, json={"items": [conn]},
    )
    mock_server.canned[(
        "POST",
        f"/api/v1/provider-connections/{conn['id']}/models/gpt-3.5/unhide",
    )] = httpx.Response(200, json=_make_model(
        model_id="gpt-3.5", visible=True,
    ))
    mock_server.canned[
        ("GET", f"/api/v1/provider-connections/{conn['id']}/models")
    ] = httpx.Response(200, json={"items": []})

    rc = main([
        "providers", "models", "openai-prod", "--unhide", "gpt-3.5",
    ])
    assert rc == 0
    out = capsys.readouterr().out
    assert "Unhid model 'gpt-3.5'" in out


def test_models_refresh_and_hide_combine(
    mock_server: MockServer, capsys: pytest.CaptureFixture[str],
) -> None:
    """Order is refresh → hide → unhide → list. Both flags fire."""
    conn = _make_connection(name="openai-prod")
    mock_server.canned[("GET", "/api/v1/provider-connections")] = httpx.Response(
        200, json={"items": [conn]},
    )
    mock_server.canned[(
        "POST", f"/api/v1/provider-connections/{conn['id']}/models/refresh",
    )] = httpx.Response(200, json={
        "added": 0, "refreshed": 1, "missing": 0, "items": [],
    })
    mock_server.canned[(
        "POST", f"/api/v1/provider-connections/{conn['id']}/models/gpt-3.5/hide",
    )] = httpx.Response(200, json=_make_model(
        model_id="gpt-3.5", visible=False, hidden_reason="operator-hidden",
    ))
    mock_server.canned[
        ("GET", f"/api/v1/provider-connections/{conn['id']}/models")
    ] = httpx.Response(200, json={"items": []})

    rc = main([
        "providers", "models", "openai-prod",
        "--refresh", "--hide", "gpt-3.5",
    ])
    assert rc == 0
    methods = [(r.method, r.url.path) for r in mock_server.requests]
    # Expected order: resolve, refresh, hide, list.
    assert methods == [
        ("GET", "/api/v1/provider-connections"),
        ("POST", f"/api/v1/provider-connections/{conn['id']}/models/refresh"),
        ("POST",
         f"/api/v1/provider-connections/{conn['id']}/models/gpt-3.5/hide"),
        ("GET", f"/api/v1/provider-connections/{conn['id']}/models"),
    ]


def test_models_refresh_upstream_502_surfaces_error(
    mock_server: MockServer, capsys: pytest.CaptureFixture[str],
) -> None:
    conn = _make_connection(name="openai-prod")
    mock_server.canned[("GET", "/api/v1/provider-connections")] = httpx.Response(
        200, json={"items": [conn]},
    )
    mock_server.canned[(
        "POST", f"/api/v1/provider-connections/{conn['id']}/models/refresh",
    )] = httpx.Response(502, json={
        "detail": {
            "code": "upstream_http_error",
            "message": (
                "Models unavailable for this connection: upstream /models "
                "returned HTTP 401"
            ),
            "upstream_http_status": 401,
        },
    })

    rc = main([
        "providers", "models", "openai-prod", "--refresh",
    ])
    assert rc == 1
    err = capsys.readouterr().err
    assert "HTTP 502" in err
    assert "upstream_http_status" in err or "HTTP 401" in err


# ──────────────────────────────────────────────────────────────────────
# not-logged-in path
# ──────────────────────────────────────────────────────────────────────


def test_create_not_logged_in_returns_2(
    capsys: pytest.CaptureFixture[str],
) -> None:
    # Clear auth via logout.
    main(["auth", "logout"])
    capsys.readouterr()  # drain logout output

    rc = main([
        "providers", "create",
        "--name", "n", "--type", "openai-compatible",
        "--base-url", "https://api.openai.com/v1",
        "--api-key", "env:NEVER_SET",
    ])
    assert rc == 2
    err = capsys.readouterr().err
    assert "not logged in" in err
    assert "loom auth login" in err


# ──────────────────────────────────────────────────────────────────────
# rotate-key (#80 slice D)
# ──────────────────────────────────────────────────────────────────────


def test_rotate_key_patches_then_tests(
    monkeypatch: pytest.MonkeyPatch,
    mock_server: MockServer, capsys: pytest.CaptureFixture[str],
) -> None:
    """Happy path: PATCH writes new key, immediate POST /test probes
    the new key, both 200 → rc=0."""
    monkeypatch.setenv("NEW_KEY", "sk-new-rotated")
    conn = _make_connection(name="openai-prod")
    cid = conn["id"]
    mock_server.canned[("GET", "/api/v1/provider-connections")] = httpx.Response(
        200, json={"items": [conn]},
    )
    mock_server.canned[
        ("PATCH", f"/api/v1/provider-connections/{cid}")
    ] = httpx.Response(200, json=conn)
    mock_server.canned[
        ("POST", f"/api/v1/provider-connections/{cid}/test")
    ] = httpx.Response(200, json={"status": "valid"})

    rc = main([
        "providers", "rotate-key", "openai-prod",
        "--api-key", "env:NEW_KEY",
    ])
    assert rc == 0
    methods = [r.method for r in mock_server.requests]
    paths = [r.url.path for r in mock_server.requests]
    assert methods == ["GET", "PATCH", "POST"]
    assert paths[1] == f"/api/v1/provider-connections/{cid}"
    assert paths[2] == f"/api/v1/provider-connections/{cid}/test"
    # PATCH payload sends ONLY api_key, not other fields.
    patch_body = json.loads(mock_server[1].content)
    assert patch_body == {"api_key": "sk-new-rotated"}
    out = capsys.readouterr().out
    assert "Rotated api_key" in out
    assert "Post-rotation test: status='valid'" in out


def test_rotate_key_post_rotation_invalid_returns_1(
    monkeypatch: pytest.MonkeyPatch,
    mock_server: MockServer, capsys: pytest.CaptureFixture[str],
) -> None:
    """Rotation succeeded but the new key doesn't probe valid — operator
    needs a clear nonzero exit + diagnostic."""
    monkeypatch.setenv("NEW_KEY", "sk-wrong")
    conn = _make_connection(name="openai-prod")
    cid = conn["id"]
    mock_server.canned[("GET", "/api/v1/provider-connections")] = httpx.Response(
        200, json={"items": [conn]},
    )
    mock_server.canned[
        ("PATCH", f"/api/v1/provider-connections/{cid}")
    ] = httpx.Response(200, json=conn)
    mock_server.canned[
        ("POST", f"/api/v1/provider-connections/{cid}/test")
    ] = httpx.Response(
        200, json={
            "status": "invalid",
            "last_validation_error": "HTTP 401 from upstream",
        },
    )
    rc = main([
        "providers", "rotate-key", "openai-prod",
        "--api-key", "env:NEW_KEY",
    ])
    assert rc == 1
    out = capsys.readouterr()
    assert "status='invalid'" in out.out
    assert "HTTP 401" in out.out
    assert "rotation succeeded" in out.err.lower()


def test_rotate_key_skip_test(
    monkeypatch: pytest.MonkeyPatch,
    mock_server: MockServer, capsys: pytest.CaptureFixture[str],
) -> None:
    """--skip-test: PATCH only, no POST /test call. Useful when the
    upstream provider hasn't propagated the new key yet."""
    monkeypatch.setenv("NEW_KEY", "sk-new")
    conn = _make_connection(name="openai-prod")
    cid = conn["id"]
    mock_server.canned[("GET", "/api/v1/provider-connections")] = httpx.Response(
        200, json={"items": [conn]},
    )
    mock_server.canned[
        ("PATCH", f"/api/v1/provider-connections/{cid}")
    ] = httpx.Response(200, json=conn)

    rc = main([
        "providers", "rotate-key", "openai-prod",
        "--api-key", "env:NEW_KEY", "--skip-test",
    ])
    assert rc == 0
    methods = [r.method for r in mock_server.requests]
    assert "POST" not in methods   # /test was NOT called
    assert "Post-rotation test skipped" in capsys.readouterr().out


def test_rotate_key_patch_failure_propagates(
    monkeypatch: pytest.MonkeyPatch,
    mock_server: MockServer, capsys: pytest.CaptureFixture[str],
) -> None:
    """If the PATCH itself fails, exit 1 and don't even attempt /test."""
    monkeypatch.setenv("NEW_KEY", "sk-new")
    conn = _make_connection(name="openai-prod")
    cid = conn["id"]
    mock_server.canned[("GET", "/api/v1/provider-connections")] = httpx.Response(
        200, json={"items": [conn]},
    )
    mock_server.canned[
        ("PATCH", f"/api/v1/provider-connections/{cid}")
    ] = httpx.Response(403, json={"detail": "scope admin:tokens required"})
    rc = main([
        "providers", "rotate-key", "openai-prod",
        "--api-key", "env:NEW_KEY",
    ])
    assert rc == 1
    # No /test call after PATCH failed.
    methods = [r.method for r in mock_server.requests]
    assert methods.count("POST") == 0


def test_rotate_key_unknown_name_returns_1(
    monkeypatch: pytest.MonkeyPatch,
    mock_server: MockServer, capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setenv("NEW_KEY", "sk-new")
    mock_server.canned[("GET", "/api/v1/provider-connections")] = httpx.Response(
        200, json={"items": []},
    )
    rc = main([
        "providers", "rotate-key", "nope", "--api-key", "env:NEW_KEY",
    ])
    assert rc == 1
    assert "no provider connection named 'nope'" in capsys.readouterr().err


def test_rotate_key_literal_api_key_rejected_at_argparse(
    mock_server: MockServer, capsys: pytest.CaptureFixture[str],
) -> None:
    """Same argv-hygiene rule as create/update: --api-key accepts only
    env:VAR / file:PATH / -. Literals leak via shell history."""
    with pytest.raises(SystemExit):
        main([
            "providers", "rotate-key", "openai-prod",
            "--api-key", "sk-literal-no-good",
        ])


@pytest.mark.parametrize("edit", ["mode", "model", "file"])
def test_custom_price_update_preserves_unselected_models(
    mock_server: MockServer, tmp_path: Path, edit: str,
) -> None:
    existing = {"other-model": {"input_usd_per_1m": 1, "output_usd_per_1m": 2}}
    conn = _make_connection(pricing_mode="custom", custom_pricing=existing, catalog_id=None)
    mock_server.canned[("GET", "/api/v1/provider-connections")] = httpx.Response(
        200, json={"items": [conn]},
    )
    mock_server.canned[("PATCH", f"/api/v1/provider-connections/{conn['id']}")] = (
        httpx.Response(200, json=conn)
    )
    argv = ["providers", "update", conn["name"], "--pricing-mode", "custom"]
    if edit == "model":
        argv += ["--price-model", "new-model", "--input-usd-per-1m", "0",
                 "--output-usd-per-1m", "3"]
    elif edit == "file":
        path = tmp_path / "prices.json"
        path.write_text('[{"model":"new-model","input_usd_per_1m":0,"output_usd_per_1m":3}]')
        argv += ["--price-file", str(path)]
    assert main(argv) == 0
    payload = json.loads(mock_server[1].content)
    if edit == "mode":
        assert "custom_pricing" not in payload
    else:
        assert payload["custom_pricing"]["new-model"]["input_usd_per_1m"] == 0
        assert ("other-model" in payload["custom_pricing"]) == (edit == "model")

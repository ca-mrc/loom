"""Consume a managed proof with a fresh client and save only child credentials."""

from __future__ import annotations

import re
from typing import Any
from uuid import UUID

from loom.nebius_environment_contract import EnvironmentRegistrationV1
from loom_cli.config import LoomConfig, config_path, load_config, save_config
from loom_cli.contexts import ManagedEnvironmentBinding, https_origin, selected_context
from loom_cli.environment_client import EnvironmentClient
from loom_cli.managed_login import child_http_client, consume_personal_session


def _verify_proof(row: EnvironmentRegistrationV1, proof: dict[str, Any]) -> str:
    expected = {"environment_id": str(row.environment_id), "incarnation": str(row.incarnation),
                "owner_user_id": str(row.owner_user_id), "owner_team_id": str(row.owner_team_id),
                "origin": "https://" + row.public_host}
    token = proof.get("login_token")
    if (set(proof) != set(expected) | {"login_token", "expires_in"}
            or any(proof.get(key) != value for key, value in expected.items())
            or type(proof.get("expires_in")) is not int or proof["expires_in"] != 90
            or not isinstance(token, str) or re.fullmatch(r"loom_env_login_[A-Za-z0-9_-]{43}", token) is None):
        raise ValueError("invalid personal login proof")
    return token


def _consume(row: EnvironmentRegistrationV1, token: str) -> tuple[str, str, str]:
    if row.owner_user_id is None:
        raise ValueError("personal environment requires an owner")
    return consume_personal_session(child_http_client("https://" + row.public_host), token,
                                    user_id=row.owner_user_id, team_id=row.owner_team_id, required_role="owner")


def login_environment(client: EnvironmentClient, environment_id: UUID) -> str:
    status = client.status(environment_id)
    row = status.registration
    if (row.environment_id != environment_id or row.scope != "personal" or row.owner_user_id is None
            or row.desired_state != "active" or status.operation is None or status.operation.phase != "completed"):
        raise ValueError("personal environment is not ready")
    binding = ManagedEnvironmentBinding(
        environment_id=str(row.environment_id), incarnation=str(row.incarnation),
        management_origin=https_origin(str(client.http.base_url).rstrip("/")), child_origin="https://" + row.public_host,
    )
    name = "dev-" + row.slug + "-" + row.environment_id.hex
    with selected_context(name):
        if config_path().exists() and load_config().managed_environment != binding:
            raise ValueError("existing context binding conflicts with personal environment")
    token = _verify_proof(row, client.login(environment_id))
    cookie_name, cookie, csrf = _consume(row, token)
    with selected_context(name):
        # A repeated login rotates credentials, not the child's deliberately
        # configured local providers/tokens. Never initialize from management.
        cfg = load_config() if config_path().exists() else LoomConfig()
        if cfg.managed_environment not in (None, binding):
            raise ValueError("existing context binding conflicts with personal environment")
        cfg.server_url, cfg.managed_environment = binding.child_origin, binding
        cfg.auth_token = None
        cfg.auth_session_cookie_name, cfg.auth_session_cookie = cookie_name, cookie
        cfg.auth_csrf_token = csrf
        save_config(cfg)
    return name


def open_environment_browser(client: EnvironmentClient, environment_id: UUID) -> bool:
    import webbrowser
    from urllib.parse import urlencode

    # A second one-use proof: the CLI's proof has already been consumed. Read
    # status again so a concurrent lifecycle change cannot select a stale host.
    row = client.status(environment_id).registration
    if row.environment_id != environment_id or row.scope != "personal" or row.desired_state != "active":
        raise ValueError("personal environment is not ready")
    origin = https_origin("https://" + row.public_host)
    token = _verify_proof(row, client.login(environment_id))
    # No proof in query parameters, stdout, exception text or config storage.
    return webbrowser.open(origin + "/auth/managed#" + urlencode({"token": token}), new=2)

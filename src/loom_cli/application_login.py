"""Generation-bound personal application login into a separate CLI context."""

from __future__ import annotations

import re
from typing import Any
from uuid import UUID

from loom.nebius_application_contract import ApplicationRegistrationV1
from loom_cli.application_client import ApplicationClient
from loom_cli.config import LoomConfig, config_path, load_config, save_config
from loom_cli.contexts import ManagedApplicationBinding, https_origin, selected_context
from loom_cli.managed_login import child_http_client, consume_personal_session


def _ready(client: ApplicationClient, identity: UUID) -> ApplicationRegistrationV1:
    status = client.status(identity)
    row, operation = status.registration, status.operation
    if (row.application_id != identity or row.desired_state != "active" or operation is None
            or operation.application_id != identity or operation.phase != "completed"
            or operation.deployment_generation != row.deployment_generation
            or operation.access_generation != row.access_generation):
        raise ValueError("personal application is not ready")
    return row


def _verify_proof(row: ApplicationRegistrationV1, proof: dict[str, Any]) -> str:
    expected = {"application_id": str(row.application_id), "incarnation": str(row.incarnation),
                "owner_user_id": str(row.owner_user_id), "owner_team_id": str(row.owner_team_id),
                "origin": "https://" + row.public_host,
                "deployment_generation": row.deployment_generation, "access_generation": row.access_generation}
    token = proof.get("login_token")
    if (set(proof) != set(expected) | {"login_token", "expires_in"}
            or any(proof.get(key) != value for key, value in expected.items())
            or any(type(proof.get(key)) is not int for key in ("deployment_generation", "access_generation", "expires_in"))
            or proof["expires_in"] != 90 or not isinstance(token, str)
            or re.fullmatch(r"loom_app_login_" + row.owner_team_id.hex + r"_[A-Za-z0-9_-]{43}", token) is None):
        raise ValueError("invalid application login proof")
    return token


def login_application(client: ApplicationClient, application_id: UUID) -> str:
    row = _ready(client, application_id)
    binding = ManagedApplicationBinding(
        application_id=str(row.application_id), incarnation=str(row.incarnation),
        management_origin=https_origin(str(client.http.base_url).rstrip("/")), child_origin="https://" + row.public_host,
    )
    name = "app-" + row.slug + "-" + row.application_id.hex
    with selected_context(name):
        if config_path().exists() and load_config().managed_application != binding:
            raise ValueError("existing context binding conflicts with application")
    token = _verify_proof(row, client.login(application_id))
    cookie_name, cookie, csrf = consume_personal_session(child_http_client(binding.child_origin), token,
                                                       user_id=row.owner_user_id, team_id=row.owner_team_id)
    with selected_context(name):
        cfg = load_config() if config_path().exists() else LoomConfig()
        if cfg.managed_environment is not None or cfg.managed_application not in (None, binding):
            raise ValueError("existing context binding conflicts with application")
        cfg.server_url, cfg.managed_application = binding.child_origin, binding
        cfg.auth_token = None
        cfg.auth_session_cookie_name, cfg.auth_session_cookie = cookie_name, cookie
        cfg.auth_csrf_token = csrf
        save_config(cfg)
    return name


def open_application_browser(client: ApplicationClient, application_id: UUID) -> bool:
    import webbrowser
    from urllib.parse import urlencode

    # Separate one-use proof, issued after fresh status/generation qualification.
    row = _ready(client, application_id)
    origin = https_origin("https://" + row.public_host)
    token = _verify_proof(row, client.login(application_id))
    return webbrowser.open(origin + "/auth/managed#" + urlencode({"token": token}), new=2)

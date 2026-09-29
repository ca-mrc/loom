"""Shared, bounded personal session consumption; never reuse management HTTP state."""

from __future__ import annotations

import json
from uuid import UUID

import httpx

from loom_cli.contexts import https_origin
from loom_cli.server_client import response_session_cookie


def child_http_client(origin: str) -> httpx.Client:
    return httpx.Client(base_url=https_origin(origin), trust_env=False, follow_redirects=False, timeout=30)


def consume_personal_session(http: httpx.Client, token: str, *, user_id: UUID, team_id: UUID,
                             required_role: str | None = None) -> tuple[str, str, str]:
    with http, http.stream("POST", "/api/v1/auth/login/complete", json={"token": token}, follow_redirects=False) as response:
        if response.status_code != 200:
            raise ValueError("personal login rejected; request a fresh proof")
        content = bytearray()
        for chunk in response.iter_bytes():
            content.extend(chunk)
            if len(content) > 16384:
                raise ValueError("invalid personal login response")
        cookie = response_session_cookie(response, current_name="__Host-loom_session")
        try:
            data = json.loads(content)
            csrf = data["csrf_token"]
            if (cookie is None or not isinstance(csrf, str) or not csrf or len(csrf) > 4096
                    or data["user"]["id"] != str(user_id) or data["current_team"]["id"] != str(team_id)
                    or data["role"] not in {"owner", "member", "viewer"}
                    or (required_role is not None and data["role"] != required_role)
                    or data["is_platform_admin"] is not False or data["user"]["is_platform_admin"] is not False):
                raise ValueError
        except (ValueError, KeyError, TypeError):
            raise ValueError("invalid personal login response") from None
        return cookie[0], cookie[1], csrf

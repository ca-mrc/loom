"""Only the registered HTTPS child receives its scoped bootstrap credential."""

from __future__ import annotations

import json
from uuid import uuid4

import httpx
import pytest

from loom.nebius_environment_contract import new_environment_registration
from tests.support.nebius_platform import platform_inputs as platform_inputs
from tests.unit.test_nebius_environment_contract import foundation_from


def binding(platform_inputs):
    return new_environment_registration(foundation_from(platform_inputs[0]), environment_id=uuid4(),
                                        incarnation=uuid4(), owner_user_id=uuid4(), owner_team_id=uuid4(), slug="alice")


def owner_body(row):
    return {"environment_id": str(row.environment_id), "incarnation": str(row.incarnation),
            "owner_user_id": str(row.owner_user_id), "owner_team_id": str(row.owner_team_id),
            "origin": "https://" + row.public_host}


async def test_login_request_uses_only_child_authority_and_validates_exact_reply(platform_inputs):
    from loom_service.environment_management.child_client import ChildEnvironmentClient

    row = binding(platform_inputs)

    def child(request):
        assert str(request.url) == "https://" + row.public_host + "/api/v1/admin/managed-environment/login"
        assert request.headers["authorization"] == "Bearer child-only-admin"
        assert not request.headers.get("cookie")
        assert json.loads(request.content) == {"environment_id": str(row.environment_id), "incarnation": str(row.incarnation)}
        return httpx.Response(200, json={**owner_body(row), "login_token": "loom_env_login_" + "a" * 43, "expires_in": 90})

    async with httpx.AsyncClient(transport=httpx.MockTransport(child), cookies={"management_session": "must-not-leak"}) as http:
        ticket = await ChildEnvironmentClient(http).login(row, admin_token="child-only-admin")
        assert ticket["origin"] == "https://" + row.public_host
        assert ticket["login_token"] == "loom_env_login_" + "a" * 43


@pytest.mark.parametrize("change", ["owner_user_id", "owner_team_id", "environment_id", "incarnation", "origin", "extra", "redirect", "oversized"])
async def test_child_reply_cannot_redirect_or_rebind_the_owner_environment_or_origin(platform_inputs, change):
    from loom_service.environment_management.child_client import ChildEnvironmentClient
    from loom_service.environment_management.provider import ProviderBlockedError

    row = binding(platform_inputs)
    body = {**owner_body(row), "login_token": "loom_env_login_" + "a" * 43, "expires_in": 90}
    if change in {"owner_user_id", "owner_team_id", "environment_id", "incarnation"}:
        body[change] = str(uuid4())
    elif change == "origin":
        body["origin"] = "https://bob.example.com"
    elif change == "extra":
        body["admin_token"] = "upstream-secret"

    def child(request):
        if change == "redirect":
            return httpx.Response(302, headers={"Location": "https://foreign.example.com/"})
        if change == "oversized":
            return httpx.Response(200, content=b"x" * 20000)
        return httpx.Response(200, json=body)

    async with httpx.AsyncClient(transport=httpx.MockTransport(child)) as http:
        with pytest.raises(ProviderBlockedError) as caught:
            await ChildEnvironmentClient(http).login(row, admin_token="child-only-admin")
        assert "upstream-secret" not in str(caught.value)

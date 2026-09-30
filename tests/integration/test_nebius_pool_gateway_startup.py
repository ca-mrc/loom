"""Run the actual gateway process against SQL and a Kubernetes HTTP boundary."""
from __future__ import annotations

import asyncio
import hashlib
import ssl
from types import SimpleNamespace
from uuid import uuid4

import httpx
import pytest
from sqlalchemy import insert, update

from loom.db.nebius_pool_schema import NebiusPoolMachineCredential, NebiusPoolRequest
from loom.db.schema import Token
from tests.integration.test_nebius_pool_cleanup_journal import begin_cleanup
from tests.integration.test_nebius_pool_kubernetes import provider
from tests.integration.test_nebius_pool_pod_inventory import InventoryAPI
from tests.integration.test_nebius_pool_registry import sessions as sessions


async def settings(sessions, tmp_path, principal):
    from loom_service.pool_management.__main__ import PoolGatewaySettings

    raw = "gateway_" + uuid4().hex
    hashed = hashlib.sha256(raw.encode()).digest()
    async with sessions.begin() as session:
        old = await session.get(Token, principal.token_hash)
        await session.execute(insert(Token).values(token_hash=hashed, type="pool_machine", scopes=[],
            issued_at=old.issued_at, expires_at=old.expires_at))
        await session.execute(insert(NebiusPoolMachineCredential).values(token_hash=hashed,
            machine_id=principal.machine_id, credential_epoch=principal.credential_epoch))
    file = tmp_path / "gateway-token"
    file.write_text(raw)
    file.chmod(0o600)
    return PoolGatewaySettings(_env_file=None, db_url=sessions.kw["bind"].url.render_as_string(hide_password=False),
        pool_id=principal.pool_id, installation_id=principal.installation_id, machine_id=principal.machine_id,
        admission_epoch=principal.pool_epoch, bearer_token_file=file, poll_seconds=0.1,
        kubernetes={"kind": "projected_service_account", "endpoint": "https://kubernetes.example",
            "ca_file": tmp_path / "ca", "token_file": tmp_path / "kubernetes-token"}), hashed


@pytest.mark.parametrize("build", [False, True])
async def test_actual_gateway_entrypoint_creates_releases_and_stops_with_server(sessions, tmp_path, monkeypatch, build):
    from loom_service.pool_management import __main__ as entrypoint

    _, api, principal, receipt, original = await provider(sessions, build=build)
    await original.aclose()
    configured, _ = await settings(sessions, tmp_path, principal)
    stopped, seen = asyncio.Event(), {}
    clients, credentials, auth_headers = [], [], []
    real_client = httpx.AsyncClient

    class Credentials:
        def __init__(self, connection):
            assert connection == configured.kubernetes
            self.ssl_context, self.closed, self.calls = ssl.create_default_context(), False, 0
            credentials.append(self)

        async def get_token(self):
            assert not self.closed
            self.calls += 1
            return "projected_" + str(self.calls)

        async def close(self):
            self.closed = True

    inventory = InventoryAPI(api, [])

    def boundary(request):
        auth_headers.append(request.headers["Authorization"])
        assert request.url.host == "kubernetes.example"
        return inventory(request)

    def client(**kwargs):
        assert kwargs["trust_env"] is False and kwargs["follow_redirects"] is False
        result = real_client(**kwargs, transport=httpx.MockTransport(boundary))
        clients.append(result)
        return result

    async def serve():
        await stopped.wait()

    def server(config):
        seen["health"] = config.app
        return SimpleNamespace(serve=serve)

    monkeypatch.setattr(entrypoint, "PoolGatewaySettings", lambda: configured)
    monkeypatch.setattr(entrypoint, "ProjectedKubernetesCredentials", Credentials)
    monkeypatch.setattr(entrypoint.httpx, "AsyncClient", client)
    monkeypatch.setattr(entrypoint.uvicorn, "Server", server)
    task = asyncio.create_task(entrypoint._run())

    async def phase(wanted):
        async with asyncio.timeout(8):
            while True:
                if task.done():
                    await task
                    raise AssertionError("gateway stopped before processing work")
                async with sessions() as session:
                    if (await session.get(NebiusPoolRequest, receipt.reservation_id)).phase == wanted:
                        return
                await asyncio.sleep(0.02)

    try:
        await phase("observed")
        await begin_cleanup(sessions, receipt.reservation_id)
        await phase("released")
        async with real_client(transport=httpx.ASGITransport(app=seen["health"]), base_url="http://health") as health:
            assert (await health.get("/readyz")).status_code == 200
        stopped.set()  # Normal Uvicorn shutdown must also stop the background loop.
        await asyncio.wait_for(asyncio.shield(task), timeout=5)
    finally:
        stopped.set()
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
    assert len(api.writes) == len(api.deletes) == (2 if build else 1)
    assert not api.objects
    assert all(item.is_closed for item in clients) and all(item.closed for item in credentials)
    assert len(set(auth_headers)) == len(auth_headers)  # Renew projected identity per HTTP request.


@pytest.mark.parametrize("damage", ["pool", "machine", "installation", "epoch", "revoked"])
async def test_gateway_startup_refuses_wrong_machine_binding_before_kubernetes(sessions, tmp_path, monkeypatch, damage):
    from loom_service.pool_management import __main__ as entrypoint

    _, _, principal, _, http = await provider(sessions)
    await http.aclose()
    configured, hashed = await settings(sessions, tmp_path, principal)
    if damage == "revoked":
        from datetime import UTC, datetime

        async with sessions.begin() as session:
            await session.execute(update(Token).where(Token.token_hash == hashed).values(revoked_at=datetime.now(UTC)))
    else:
        field = {"pool": "pool_id", "machine": "machine_id", "installation": "installation_id", "epoch": "admission_epoch"}[damage]
        configured = configured.model_copy(update={field: 999 if damage == "epoch" else uuid4()})

    def forbidden(*_a, **_kw):
        raise AssertionError("unqualified gateway opened Kubernetes credentials")

    monkeypatch.setattr(entrypoint, "PoolGatewaySettings", lambda: configured)
    monkeypatch.setattr(entrypoint, "ProjectedKubernetesCredentials", forbidden)
    with pytest.raises(ValueError, match="pool_gateway_identity_unavailable"):
        await entrypoint._run()

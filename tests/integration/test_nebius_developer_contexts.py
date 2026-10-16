"""CLI contexts compose real management/child APIs and three independent DBs.

Only HTTP transport is in-process; authentication, one-use challenges, ownership,
registry transactions, cookies and config persistence are the product code.
"""

from __future__ import annotations

import asyncio
import base64
import json
from contextlib import ExitStack, contextmanager

import httpx
import pytest
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom.admin_secret import AdminSecretVerifier
from loom.db.schema import TeamMembership, User
from loom_cli import auth_cmd, environment_client, environment_login
from loom_cli.__main__ import main
from loom_cli.config import LoomConfig, config_path, load_config, save_config
from loom_cli.contexts import selected_context
from loom_cli.server_client import authed_client
from loom_service.app import create_app
from loom_service.config import LoomServiceSettings
from loom_service.environment_management.child_client import ChildEnvironmentClient
from loom_service.environment_management.manager import EnvironmentManager, EnvironmentPlanFactory
from loom_service.password_auth import hash_password
from tests.integration.conftest import _isolated_migration_database
from tests.integration.test_nebius_environment_management import (
    environment_registry as environment_registry,
)
from tests.support.nebius_platform import ROOT
from tests.support.nebius_platform import platform_inputs as platform_inputs
from tests.unit.test_nebius_environment_contract import foundation_from


@pytest.fixture
def personal_databases(migration_template_postgres_url):
    with ExitStack() as stack:
        yield [stack.enter_context(contextmanager(_isolated_migration_database)(
            migration_template_postgres_url,
            template_name=make_url(migration_template_postgres_url).database,
            prepare_template=False,
        )) for _ in range(2)]


async def test_two_owners_enter_real_child_apis_without_sharing_management_credentials(
    environment_registry, personal_databases, platform_inputs, tmp_path, monkeypatch, capsys,
):
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path / "config"))
    monkeypatch.setenv("LOOM_SECRET_STORE_MASTER_KEY", base64.b64encode(b"m" * 32).decode())
    monkeypatch.delenv("LOOM_SECRET_STORE_MASTER_KEYS", raising=False)
    registry, factory, principals, prepare = environment_registry
    management_origin = "https://manage.example.com"
    settings = LoomServiceSettings(_env_file=None, service_mode="management", public_base_url=management_origin,
                                   db_url=factory.kw["bind"].url.render_as_string(hide_password=False))
    management = create_app(settings)
    management.state.settings, management.state.session_factory = settings, factory
    apps = {"manage.example.com": management}
    requests, proofs = [], []

    class ApplicationTransport(httpx.AsyncBaseTransport):
        async def handle_async_request(self, request):
            requests.append(request)
            response = await httpx.ASGITransport(app=apps[request.url.host]).handle_async_request(request)
            await response.aread()
            if request.url.path.endswith("/login") and "managed-environment" in request.url.path and response.status_code == 200:
                proofs.append(response.json()["login_token"])
            return response

    loop = asyncio.get_running_loop()
    transport = ApplicationTransport()

    class CliTransport(httpx.BaseTransport):
        def handle_request(self, request):
            response = asyncio.run_coroutine_threadsafe(transport.handle_async_request(request), loop).result(timeout=20)
            return httpx.Response(response.status_code, headers=response.headers, content=response.content)

    monkeypatch.setattr(environment_client, "authed_client", lambda cfg: authed_client(cfg, transport=CliTransport()))
    monkeypatch.setattr(auth_cmd, "authed_client", lambda cfg: authed_client(cfg, transport=CliTransport()))
    monkeypatch.setattr(environment_login, "child_http_client", lambda origin: httpx.Client(
        base_url=origin, transport=CliTransport(), follow_redirects=False, trust_env=False,
    ))

    class NoCatalog:
        async def resolve(self, candidate_id):
            pytest.fail("login must not resolve a new publication")

    engines, rows, configs = [], [], []
    try:
        async with httpx.AsyncClient(transport=transport) as remote:
            child_control = ChildEnvironmentClient(remote)
            management.state.environment_manager = EnvironmentManager(registry, EnvironmentPlanFactory(
                foundation_from(platform_inputs[0]), NoCatalog(), keyring={}, repo_root=ROOT,
            ), child=child_control)
            for name, principal, database in zip(("alice", "bob"), principals, personal_databases, strict=True):
                prepared = prepare(name, principal)
                row = prepared.registration
                rows.append(row)
                path = tmp_path / (name + ".json")
                path.write_text(json.dumps({"schema_version": "loom.nebius-managed-environment.v1",
                    "registration": row.model_dump(mode="json"), "namespace": row.application_namespace,
                    "public_host": row.public_host}))
                child_settings = LoomServiceSettings(_env_file=None, db_url=database, minio_access_key="test",
                    minio_secret_key="test", public_base_url="https://" + row.public_host, managed_environment_config_file=path)
                app = create_app(child_settings)
                engine = create_async_engine(database)
                engines.append(engine)
                app.state.settings = child_settings
                app.state.session_factory = async_sessionmaker(engine, expire_on_commit=False)
                admin = "loom_admin_" + name[0] * 43
                app.state.admin_secret_verifier = AdminSecretVerifier.from_token(admin)
                apps[row.public_host] = app
                await child_control.enroll(row, admin_token=admin)
                operation = await registry.create(principal=principal, idempotency_key=name, prepared=prepared)
                lease = await registry.claim(operation.operation_id)
                # Provisioning has its own tests. Qualify the CLI from a ready
                # registry with genuine child admin material and auth databases.
                while (step := await registry.next_step(lease)) is not None:
                    if step.key == "credentials:material":
                        await registry.store_material(lease, step.key, {"loom-admin-secret": {
                            "secrets.toml": '[admin]\ntoken="' + admin + '"\n',
                        }})
                    else:
                        await registry.confirm_step(lease, step.key, provider_identity="test:" + step.key)
                await registry.complete(lease)
                async with factory.begin() as session:
                    user = await session.get(User, principal.user_id)
                    user.password_hash = hash_password("owner-passphrase")
                    session.add(TeamMembership(user_id=principal.user_id, team_id=principal.team_id, role="member"))
                async with httpx.AsyncClient(transport=transport, base_url=management_origin) as login:
                    response = await login.post("/api/v1/auth/login", json={"username": name, "password": "owner-passphrase"})
                    assert response.status_code == 200, response.text
                    configs.append(LoomConfig(server_url=management_origin,
                        auth_session_cookie=login.cookies.get("__Host-loom_session"), auth_session_cookie_name="__Host-loom_session",
                        auth_csrf_token=response.json()["csrf_token"], tokens={"openai": "management-only-key"}))

            async def cli(*args):
                result = await asyncio.to_thread(main, list(args))
                captured = capsys.readouterr()
                assert not any(proof in captured.out + captured.err for proof in proofs)
                return result, captured

            children = []
            for index, (row, cfg) in enumerate(zip(rows, configs, strict=True)):
                save_config(cfg)
                assert (await cli("dev", "login", str(rows[1 - index].environment_id)))[0] == 1
                assert (await cli("dev", "login", str(row.environment_id)))[0] == 0
                # The existing management client refreshes its own CSRF token
                # before unsafe requests. That is not child credential copying.
                retained = load_config()
                assert retained.server_url == management_origin
                assert retained.auth_session_cookie == cfg.auth_session_cookie
                assert retained.auth_csrf_token and retained.managed_environment is None
                assert retained.tokens == {"openai": "management-only-key"}
                before = config_path().read_bytes()
                context = "dev-" + row.slug + "-" + row.environment_id.hex
                with selected_context(context):
                    child = load_config()
                    assert child.tokens == {} and child.auth_token is None
                    assert child.auth_session_cookie not in {item.auth_session_cookie for item in configs}
                    assert child.managed_environment.incarnation == str(row.incarnation)
                    children.append(child)
                code, output = await cli("--context", context, "auth", "whoami", "--format", "json")
                assert code == 0, output.err
                assert json.loads(output.out)["user_id"] == str(row.owner_user_id)
                assert config_path().read_bytes() == before

            assert children[0].auth_session_cookie != children[1].auth_session_cookie
            async with httpx.AsyncClient(transport=transport) as check:
                for row, proof in zip(rows, proofs, strict=True):
                    assert (await check.post("https://" + row.public_host + "/api/v1/auth/login/complete",
                                             json={"token": proof})).status_code == 400
                for row, foreign in ((rows[0], children[1]), (rows[1], children[0])):
                    response = await check.get("https://" + row.public_host + "/api/v1/auth/whoami",
                        headers={"Cookie": "__Host-loom_session=" + foreign.auth_session_cookie})
                    assert response.status_code == 401
            for request in requests:
                if request.url.host != "manage.example.com":
                    assert all(cfg.auth_session_cookie not in request.headers.get("cookie", "") for cfg in configs)
                    assert "management-only-key" not in str(request.headers)
    finally:
        for engine in engines:
            await engine.dispose()

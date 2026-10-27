"""Gateway completion audit under the actual bootstrapped TLS database role."""

from __future__ import annotations

import hashlib
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock
from uuid import uuid4

import httpx
import psycopg
import pytest
from fastapi import FastAPI
from sqlalchemy import create_engine, insert
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom import nebius_platform_bootstrap as bootstrap
from loom.db.schema import (
    RateCard,
    ServiceExecutionTarget,
    Task,
    TaskImageMaterialization,
    Team,
    Token,
    Trial,
    TrialTaskImageMaterialization,
)
from loom.execution_runtime_contract import RuntimeTaskInputV1
from loom.pipeline.keys import canonical_digest, digest_bytes
from loom.service_execution_materialization import (
    ServiceExecutionInputFileV1,
    ServiceExecutionInputManifestV1,
)
from loom.trajectory.storage import FakeObjectStore
from loom_control_plane.service_execution_output import (
    VerifiedExecutionPod,
    resolve_service_execution_input,
)
from loom_llm_gateway.app import create_app
from loom_llm_gateway.config import GatewaySettings
from loom_llm_gateway.rate_card import RateCardCache
from loom_llm_gateway.routes import service_execution
from tests.integration.test_nebius_platform_bootstrap import platform_database  # noqa: F401
from tests.integration.test_service_execution_leases import (
    _reserve,
    _runtime_contract,
    _seed_ready_trial,
)

pytestmark = pytest.mark.docker


@pytest.fixture
def gateway_database(
    request: pytest.FixtureRequest,
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[str, str]:
    database_url = request.getfixturevalue("platform_database")
    monkeypatch.setattr(bootstrap, "database_url", lambda _value, _namespace: database_url)
    monkeypatch.setenv("LOOM_DB_URL", database_url)
    monkeypatch.setenv("LOOM_COLLECTOR_TOKEN", "loom_ecc_" + "f" * 64)
    monkeypatch.setenv("LOOM_BATCH_RUNNER_TOKEN", "loom_br_" + "b" * 64)
    password = "gateway-test-password-" + "x" * 32
    for role in ("SERVICE", "CONTROL_PLANE", "GATEWAY", "ACTUATOR"):
        monkeypatch.setenv("LOOM_DB_" + role + "_PASSWORD", password)
    monkeypatch.setenv("LOOM_ENV", "development")
    monkeypatch.setenv("LOOM_NAMESPACE", "gateway-grants-test")
    bootstrap.bootstrap_database({"namespace": "loom-nebius-platform"})
    # A repeated migration/bootstrap must preserve these restricted grants.
    bootstrap.bootstrap_database({"namespace": "loom-nebius-platform"})
    return database_url, password


async def test_gateway_completion_records_usage_with_restricted_tls_role(
    gateway_database: tuple[str, str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database_url, password = gateway_database

    team_id, trial_id = uuid4(), uuid4()
    task_id = "gateway-grants-" + uuid4().hex
    token = "loom_team_" + uuid4().hex
    issued_at = datetime.now(UTC)
    admin_engine = create_engine(make_url(database_url).set(drivername="postgresql+psycopg"))
    try:
        with admin_engine.begin() as db:
            db.execute(insert(Team).values(id=team_id, name="gateway-grants"))
            db.execute(
                insert(Token).values(
                    token_hash=hashlib.sha256(token.encode()).digest(),
                    type="team",
                    scopes=["llm:call"],
                    team_id=team_id,
                    issued_at=issued_at,
                )
            )
            db.execute(insert(Task).values(id=task_id, checksum="0" * 64, config={}, source="test"))
            db.execute(
                insert(Trial).values(
                    id=trial_id,
                    team_id=team_id,
                    task_id=task_id,
                    config={},
                    requires_caps={},
                    state="running",
                    submitted_at=issued_at,
                )
            )
            db.execute(
                insert(RateCard).values(
                    id="gateway-grants",
                    captured_at=issued_at,
                    table={
                        "id": "gateway-grants",
                        "entries": [
                            {
                                "provider": "openai",
                                "model": "mock-model",
                                "input_per_mtok": 1,
                                "output_per_mtok": 2,
                                "cache_read_per_mtok": 0,
                                "cache_write_per_mtok": 0,
                            }
                        ],
                    },
                )
            )
    finally:
        admin_engine.dispose()

    gateway_url = make_url(database_url).set(username="loom_gateway", password=password)
    monkeypatch.setenv(
        "LOOM_GW_DB_URL",
        gateway_url.set(drivername="postgresql+psycopg").render_as_string(hide_password=False),
    )
    monkeypatch.setenv("LOOM_GW_OPENAI_API_KEY", "mock-upstream-key")
    settings = GatewaySettings(_env_file=None)
    app = create_app(settings)
    engine = create_async_engine(str(settings.db_url))
    app.state.settings = settings
    app.state.session_factory = async_sessionmaker(engine, expire_on_commit=False)
    app.state.rate_card_cache = RateCardCache(
        session_factory=app.state.session_factory, ttl_sec=settings.rate_card_cache_ttl_sec
    )
    upstream_calls = 0

    async def upstream(**kwargs: Any) -> dict[str, Any]:
        nonlocal upstream_calls
        upstream_calls += 1
        return {
            "id": "mock-response",
            "model": kwargs["model"],
            "choices": [
                {"message": {"role": "assistant", "content": "done"}, "finish_reason": "stop"}
            ],
            "usage": {"prompt_tokens": 2, "completion_tokens": 1},
        }

    monkeypatch.setattr("loom_llm_gateway.litellm_wrapper.acompletion", upstream)
    try:
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app, raise_app_exceptions=False),
            base_url="http://gateway.test",
        ) as client:
            authorities = None
            # First call lazily binds the pre-migration Trial; the second must
            # verify/reuse existing authorities without UPDATE or DELETE.
            for count in (1, 2):
                response = await client.post(
                    "/v1/chat/completions",
                    headers={"Authorization": "Bearer " + token},
                    json={
                        "model": "openai/mock-model",
                        "messages": [{"role": "user", "content": "hello"}],
                        "loom": {
                            "team_id": str(team_id),
                            "trial_id": str(trial_id),
                            "step_id": "main",
                        },
                    },
                )
                assert upstream_calls == count  # A 500 here is AFTER the provider returned.
                assert response.status_code == 200, response.text
                assert response.json()["choices"][0]["message"]["content"] == "done"
                assert response.json()["loom"]["input_tokens"] == 2
                with psycopg.connect(database_url) as db:
                    calls = db.execute(
                        "SELECT input_tokens, output_tokens, cost_usd, lifecycle_authority_id "
                        "FROM llm_calls WHERE trial_id=%s",
                        (trial_id,),
                    ).fetchall()
                    assert len(calls) == count
                    assert all(row[0:2] == (2, 1) and row[2] > 0 and row[3] for row in calls)
                    current = db.execute(
                        "SELECT * FROM data_lifecycle_authorities WHERE owner_id=%s ORDER BY data_class",
                        (str(trial_id),),
                    ).fetchall()
                    assert len(current) == 2
                    if authorities is not None:
                        assert current == authorities
                    authorities = current
                    assert db.execute(
                        "SELECT lifecycle_authority_id IS NOT NULL FROM trials WHERE id=%s",
                        (trial_id,),
                    ).fetchone() == (True,)
    finally:
        await engine.dispose()

    with psycopg.connect(gateway_url.render_as_string(hide_password=False)) as db:
        assert db.execute(
            "SELECT current_user, ssl FROM pg_stat_ssl WHERE pid=pg_backend_pid()"
        ).fetchone() == ("loom_gateway", True)
        for statement in (
            "UPDATE data_lifecycle_authorities SET pinned=true",
            "UPDATE data_lifecycle_authorities SET expires_at=NULL",
            "UPDATE data_lifecycle_authorities SET state='deleted'",
            "DELETE FROM data_lifecycle_authorities",
            "UPDATE execution_admission_policies SET max_concurrent=1000",
            "DELETE FROM execution_budget_policies",
            "UPDATE tokens SET scopes=ARRAY['admin:tokens']",
            "DELETE FROM users",
            "CREATE ROLE gateway_escape",
        ):
            with pytest.raises(psycopg.errors.InsufficientPrivilege):
                db.execute(statement)
            db.rollback()


async def test_gateway_reads_frozen_task_input_without_materialization_write_privileges(
    gateway_database: tuple[str, str],
) -> None:
    database_url, password = gateway_database
    team_id, trial_id, snapshot_id = uuid4(), uuid4(), uuid4()
    task_id = "gateway-snapshot-" + uuid4().hex
    body = b"frozen instruction\n"
    plan = _runtime_contract(now=datetime.now(UTC))
    manifest = ServiceExecutionInputManifestV1(
        task_revision_sha256=plan.task_revision_sha256,
        files=(ServiceExecutionInputFileV1(
            relative_path="instruction.md", size_bytes=len(body), sha256=digest_bytes(body), mode="0644",
        ),),
    )
    manifest_body = manifest.canonical_bytes()
    manifest_digest = digest_bytes(manifest_body)
    plan = plan.model_copy(update={
        "task_image_materialization_id": snapshot_id,
        "agent_image_ref": plan.task_image_ref,
        "task_input": RuntimeTaskInputV1(manifest_sha256=manifest_digest, file_count=1, total_bytes=len(body)),
    })
    contract = plan.canonical_payload()
    lease = SimpleNamespace(
        trial_id=trial_id, team_id=team_id,
        runtime_contract_json=contract, runtime_contract_sha256=canonical_digest(contract),
    )
    admin_engine = create_engine(make_url(database_url).set(drivername="postgresql+psycopg"))
    try:
        with admin_engine.begin() as db:
            db.execute(insert(Team).values(id=team_id, name="gateway-snapshot"))
            # The current Task no longer describes the source authorized by the lease.
            db.execute(insert(Task).values(
                id=task_id, checksum="9" * 64, config={"new_revision": True},
                source="s3://artifacts/new/task/", source_provenance={},
            ))
            db.execute(insert(Trial).values(
                id=trial_id, team_id=team_id, task_id=task_id, config={}, requires_caps={},
                state="running", submitted_at=datetime.now(UTC),
            ))
            db.execute(insert(TaskImageMaterialization).values(
                id=snapshot_id, materialization_key=uuid4().hex * 2, task_id=task_id,
                task_checksum=plan.task_revision_sha256.removeprefix("sha256:"), cpu_arch="x86_64",
                task_config={"original_revision": True}, task_source="s3://artifacts/original/task/",
                task_source_provenance={"service_execution_input": {
                    "schema_version": "loom.service-execution-input.v1",
                    "manifest_uri": "s3://artifacts/original/manifest.json",
                    "manifest_sha256": manifest_digest, "file_count": 1, "total_bytes": len(body),
                }},
                state="retiring", registry_images={},
            ))
            db.execute(insert(TrialTaskImageMaterialization).values(trial_id=trial_id, materialization_id=snapshot_id))
    finally:
        admin_engine.dispose()

    gateway_url = make_url(database_url).set(username="loom_gateway", password=password)
    engine = create_async_engine(gateway_url.set(drivername="postgresql+psycopg"))
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    store = FakeObjectStore()
    store.objects[("artifacts", "original/manifest.json")] = manifest_body
    try:
        async with sessions() as session:
            resolved = await resolve_service_execution_input(
                session, lease=lease, store=store, artifacts_bucket="artifacts",
            )
            assert resolved.prefix == "original/task/"
            assert resolved.manifest_body == manifest_body
    finally:
        await engine.dispose()

    with psycopg.connect(gateway_url.render_as_string(hide_password=False)) as db:
        assert db.execute(
            "SELECT current_user, ssl FROM pg_stat_ssl WHERE pid=pg_backend_pid()"
        ).fetchone() == ("loom_gateway", True)
        for table, update in (
            ("task_image_materializations", "state='failed'"),
            ("trial_task_image_materializations", "materialization_id=materialization_id"),
        ):
            assert db.execute(
                "SELECT has_table_privilege(current_user, %s, 'SELECT'), "
                "has_table_privilege(current_user, %s, 'INSERT'), "
                "has_table_privilege(current_user, %s, 'UPDATE'), "
                "has_table_privilege(current_user, %s, 'DELETE')",
                (table, table, table, table),
            ).fetchone() == (True, False, False, False)
            for statement in (f"UPDATE {table} SET {update}", f"DELETE FROM {table}"):
                with pytest.raises(psycopg.errors.InsufficientPrivilege):
                    db.execute(statement)
                db.rollback()


async def test_gateway_role_can_reclaim_and_checkpoint_terminus_execution(
    gateway_database: tuple[str, str],
) -> None:
    """Hosted STS writes these tables through the Gateway, not the control plane."""
    database_url, password = gateway_database
    now = datetime.now(UTC)
    admin = create_async_engine(make_url(database_url).set(drivername="postgresql+psycopg"))
    admin_sessions = async_sessionmaker(admin, expire_on_commit=False)
    try:
        async with admin_sessions() as session:
            trial_id, target = await _seed_ready_trial(session, now=now)
            lease = await _reserve(session, trial_id=trial_id, target=target, now=now)
            lease.pod_uid = "bound-pod"
            lease.pod_ip = "10.42.0.12"
            lease.observed_state = "running"
            target_row = await session.get(ServiceExecutionTarget, target.target_id)
            assert target_row is not None
            target_row.spec_json = {
                **target_row.spec_json,
                "cluster_scope_id": "primary",
                "pod_identity_audience": "loom-execution",
            }
            trial = await session.get(Trial, trial_id)
            assert trial is not None
            trial.state = "running"
            await session.commit()
            lease_id = lease.id
            generation = lease.generation
            namespace = lease.namespace_name
            assert lease.resource_generation == generation
    finally:
        await admin.dispose()

    gateway_url = make_url(database_url).set(username="loom_gateway", password=password)
    with psycopg.connect(gateway_url.render_as_string(hide_password=False)) as db:
        for table in (
            "terminus_agent_executions",
            "terminus_agent_run_attempts",
            "episode_checkpoints",
        ):
            assert db.execute(
                "SELECT has_table_privilege(current_user, %s, 'SELECT'), "
                "has_table_privilege(current_user, %s, 'INSERT'), "
                "has_table_privilege(current_user, %s, 'UPDATE'), "
                "has_table_privilege(current_user, %s, 'DELETE')",
                (table, table, table, table),
            ).fetchone() == (True, True, True, True)

    engine = create_async_engine(gateway_url.set(drivername="postgresql+psycopg"))
    app = FastAPI()
    app.state.session_factory = async_sessionmaker(engine, expire_on_commit=False)
    app.include_router(service_execution.router)
    app.state.execution_pod_reviewer = SimpleNamespace(
        review=AsyncMock(
            return_value=VerifiedExecutionPod(
                cluster_scope_id="primary",
                namespace=namespace,
                service_account="loom-execution-attempt",
                audience="loom-execution",
                pod_uid="bound-pod",
            ),
        ),
    )
    identity = {
        "lease_id": str(lease_id),
        "generation": generation,
        "execution_role": "attempt",
    }
    try:
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app, raise_app_exceptions=False),
            base_url="http://gateway.test",
        ) as client:
            reclaimed = await client.post(
                "/internal/service-execution/terminus/reclaim",
                headers={"Authorization": "Bearer pod-token"},
                json={
                    **identity,
                    "schema_version": "loom.service-execution-terminus-reclaim.v1",
                },
            )
            assert reclaimed.status_code == 200, reclaimed.text
            recovery = reclaimed.json()
            assert recovery["recovery"] == "fresh"
            checkpoint = await client.post(
                "/internal/service-execution/terminus/episode-checkpoints",
                headers={"Authorization": "Bearer pod-token"},
                json={
                    **identity,
                    "schema_version": "loom.service-execution-terminus-checkpoint.v1",
                    "execution_id": recovery["agent_execution_id"],
                    "run_attempt_id": recovery["agent_run_attempt_id"],
                    "episode": 1,
                    "active_role": "student",
                    "last_call_ordinal": 0,
                    "last_seq": 0,
                    "tmux_session_id": None,
                    "attempt_deadline_wall_clock": (now + timedelta(hours=1)).isoformat(),
                },
            )
            assert checkpoint.status_code == 200, checkpoint.text
            assert checkpoint.json()["version"] == 1
    finally:
        await engine.dispose()

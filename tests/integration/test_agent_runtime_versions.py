"""Real PostgreSQL and authenticated API registration, submission, and frozen reruns."""

import base64
import json
from copy import deepcopy
from datetime import UTC, datetime
from types import SimpleNamespace
from uuid import UUID, uuid4

import httpx
import pytest
from cryptography.hazmat.primitives.serialization import Encoding, PublicFormat
from fastapi import FastAPI
from sqlalchemy import delete, select

from loom.admin_secret import AdminSecretVerifier
from loom.agent_runtime_registry import register_agent_runtime, resolve_agent_runtimes
from loom.db.schema import Agent, Batch, ServiceExecutionClass, ServiceExecutionTarget, Task, Trial
from loom.execution_contract import NEBIUS_CPU_EXECUTION_CLASS_V1
from loom.pipeline.keys import canonical_digest
from loom_cli.catalog_provision import AgentRow, CatalogRows, PostgresCatalogStore
from loom_control_plane.routes.admin import router as admin_router
from loom_service.batch_runner import _materialize_trial_config
from tests.integration.test_service_batches_crud import (
    RAW_ADMIN_TOKEN,
    _automatic_service_execution_task_config,
    _seed_connection,
    _service_execution_runtime_profile,
    _service_execution_task_config,
)
from tests.integration.test_service_batches_crud import camp_setup as camp_setup
from tests.support.agent_runtime import release
from tests.support.execution_image_admission import _PRIVATE_KEY, IMAGE_ADMISSION_KEYRING


def _cp(app: FastAPI) -> FastAPI:
    cp = FastAPI()
    cp.include_router(admin_router)
    cp.state.session_factory = app.state.session_factory
    cp.state.admin_secret_verifier = AdminSecretVerifier.from_token(RAW_ADMIN_TOKEN)
    cp.state.settings = SimpleNamespace(
        execution_image_admission_public_keys_json=json.dumps(
            {
                "schema_version": 1,
                "keys": [
                    {
                        "signing_key_id": "test-builder",
                        "public_key_base64": base64.b64encode(
                            _PRIVATE_KEY.public_key().public_bytes(Encoding.Raw, PublicFormat.Raw)
                        ).decode(),
                    }
                ],
            }
        )
    )
    return cp


async def test_registration_real_auth_idempotency_rebind_and_public_catalog(
    camp_setup, postgres_url
):
    app, user_token, _ = camp_setup
    item = release("api-" + uuid4().hex)
    path = f"/admin/agents/terminus-2/versions/{item.agent_version}"
    try:
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=_cp(app)), base_url="http://cp"
        ) as client:
            payload = item.model_dump(mode="json")
            denied = await client.put(
                path, json=payload, headers={"Authorization": f"Bearer {user_token}"}
            )
            assert denied.status_code == 403
            headers = {"Authorization": f"Bearer {RAW_ADMIN_TOKEN}"}
            for _ in range(2):
                response = await client.put(path, json=payload, headers=headers)
                assert response.status_code == 200, response.text
            other = release(item.agent_version, "9")
            response = await client.put(path, json=other.model_dump(mode="json"), headers=headers)
            assert response.status_code == 409, response.text
            bad = item.model_dump(mode="json")
            bad["image_admission"]["signature_base64"] = "A" * 88
            response = await client.put(path, json=bad, headers=headers)
            assert response.status_code == 400
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://svc"
        ) as client:
            response = await client.get(
                "/api/v1/agents", headers={"Authorization": f"Bearer {user_token}"}
            )
            assert response.status_code == 200
            versions = next(
                x["versions"] for x in response.json()["items"] if x["name"] == "terminus-2"
            )
            assert item.public_metadata() in versions
            assert all("agent_image_ref" not in x for x in versions)
        store = PostgresCatalogStore(postgres_url)
        await store.upsert_rows(
            CatalogRows(
                benchmarks=[],
                tasks=[],
                agents=[AgentRow("terminus-2", item.agent_version, "native-runtime", payload)],
            )
        )
        with pytest.raises(ValueError, match="overwrite"):
            await store.upsert_rows(
                CatalogRows(
                    benchmarks=[],
                    tasks=[],
                    agents=[AgentRow("terminus-2", item.agent_version, "builtin", {})],
                )
            )
        async with app.state.session_factory() as session:
            resolved = await resolve_agent_runtimes(session, [("terminus-2", item.agent_version)])
            assert resolved == (item,)
            with pytest.raises(ValueError, match="unknown"):
                await resolve_agent_runtimes(session, [("terminus-2", "missing")])
    finally:
        async with app.state.session_factory() as session, session.begin():
            await session.execute(delete(Agent).where(Agent.version == item.agent_version))


@pytest.mark.parametrize("combinations", [False, True])
@pytest.mark.parametrize("runtime_mode", ["frozen", "current", "unavailable", "no_controller", "prebound", "legacy", "other_agent", "incompatible", "incompatible_frozen"])
async def test_public_batch_freezes_versions_and_rerun_keeps_snapshot(
    camp_setup, postgres_url, combinations, runtime_mode,
):
    app, user_token, team = camp_setup
    a, b = release("a-" + uuid4().hex), release("b-" + uuid4().hex, "9")
    old_bridge = "44dbda72dff90fde5c29b094227db6c5ee03389b"
    if runtime_mode == "incompatible":
        a = a.model_copy(update={"publisher_source_revision": old_bridge})
    profile = _service_execution_runtime_profile()
    app.state.settings = app.state.settings.model_copy(
        update={
            "service_execution_runtime_profile_json": profile.model_dump_json(),
        }
    )
    task_id = "local/version-" + uuid4().hex
    target_id = "version-target-" + uuid4().hex
    class_id = NEBIUS_CPU_EXECUTION_CLASS_V1.class_id
    now = datetime.now(UTC)
    spec = NEBIUS_CPU_EXECUTION_CLASS_V1.model_dump(mode="json")
    created_class = False
    async with app.state.session_factory() as session, session.begin():
        for item in (a, b):
            await register_agent_runtime(session, item, keyring=IMAGE_ADMISSION_KEYRING)
        if await session.get(ServiceExecutionClass, class_id) is None:
            session.add(
                ServiceExecutionClass(
                    id=class_id,
                    schema_version=NEBIUS_CPU_EXECUTION_CLASS_V1.schema_version,
                    spec_json=spec,
                    spec_sha256=canonical_digest(spec),
                    enabled=True,
                )
            )
            created_class = True
            await session.flush()
        session.add(
            ServiceExecutionTarget(
                id=target_id,
                logical_pool_id="nebius-cpu",
                execution_class_id=class_id,
                schema_version="loom.execution-target.v1",
                spec_json={"health_stale_after_seconds": 600},
                spec_sha256="sha256:" + "e" * 64,
                environment="development",
                provider="nebius",
                region="eu-north1",
                failure_domain="north",
                data_residency="eu",
                desired_state="active",
                observed_state="ready",
                health_status="healthy",
                health_observed_at=now,
            )
        )
        task_config = _automatic_service_execution_task_config(task_id)
        task_config["environment"].pop("docker_image")
        task_config["environment"]["dockerfile"] = "Dockerfile"
        session.add(
            Task(
                id=task_id,
                checksum="c" * 64,
                config=task_config,
                source="s3://artifacts/task/",
                source_provenance={
                    "service_execution_input": {
                        "schema_version": "loom.service-execution-input.v1",
                        "manifest_uri": "s3://artifacts/task.json",
                        "manifest_sha256": "sha256:" + "d" * 64,
                        "file_count": 3,
                        "total_bytes": 4096,
                    }
                },
                license="MIT",
            )
        )
    try:
        model = {"provider": "openai", "name": "gpt-5", "source": "api"}
        selected = [
            {"agent_name": "terminus-2", "agent_version": x.agent_version, "agent_model": model}
            for x in (a, b)
        ]
        payload = {
            "name": "version freeze",
            "purpose": "evaluation",
            "provider_connection_id": _seed_connection(postgres_url, team, "gpt-5"),
            "task_filter": {"task_ids": [task_id], "subset_kind": "explicit"},
            "backend": "nebius",
            "trial_config": {
                "retry": {"max_attempts": 1, "retry_on": []},
                "override_agent_timeout_sec": 120,
                **({} if combinations else selected[0]),
            },
        }
        if combinations:
            payload["combinations"] = selected
        headers = {"Authorization": f"Bearer {user_token}"}
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://svc"
        ) as client:
            rejected = await client.post(
                "/api/v1/batches", json={**payload, "backend": "docker"}, headers=headers
            )
            assert rejected.status_code == 400 and "agent_version" in rejected.text
            unknown = {**payload, "trial_config": {**selected[0], "agent_version": "missing"}}
            unknown.pop("combinations", None)
            rejected = await client.post("/api/v1/batches", json=unknown, headers=headers)
            assert (
                rejected.status_code == 400 and "unknown published agent version" in rejected.text
            )
            response = await client.post("/api/v1/batches", json=payload, headers=headers)
            if runtime_mode == "incompatible":
                assert response.status_code == 400, response.text
                assert a.agent_version in response.text
                assert "instance_id" in response.text
                assert "choose a compatible published version" in response.text
                catalog = await client.get("/api/v1/agents", headers=headers)
                versions = next(
                    x["versions"] for x in catalog.json()["items"] if x["name"] == "terminus-2"
                )
                assert a.public_metadata() in versions
                assert b.public_metadata() in versions
                async with app.state.session_factory() as session:
                    batches = (await session.scalars(select(Batch).where(
                        Batch.resolved_task_ids.contains([task_id]),
                    ))).all()
                    assert batches == []
                    trials = (await session.scalars(select(Trial).where(
                        Trial.task_id == task_id,
                    ))).all()
                    assert trials == []
                return
            assert response.status_code == 201, response.text
            batch_id = UUID(response.json()["batch_id"])
            async with app.state.session_factory() as session, session.begin():
                batch = await session.get(Batch, batch_id)
                frozen = deepcopy(batch.service_execution_runtime_profile)
                if runtime_mode == "incompatible_frozen":
                    old_profile = deepcopy(batch.service_execution_runtime_profile)
                    old_profile["agent_runtime_bindings"][0]["publisher_source_revision"] = old_bridge
                    batch.service_execution_runtime_profile = old_profile
                if runtime_mode == "legacy":
                    batch.backend = "docker"
                    batch.service_execution_runtime_profile = None
                if runtime_mode == "other_agent":
                    if combinations:
                        batch.combinations = [{**item, "agent_name": "oracle"}
                                              for item in batch.combinations]
                    else:
                        batch.trial_config = {**batch.trial_config, "agent_name": "oracle"}
                parent_profile = deepcopy(batch.service_execution_runtime_profile)
                original_config = deepcopy(batch.trial_config)
                original_combinations = deepcopy(batch.combinations)
                assert len(frozen["agent_runtime_bindings"]) == (2 if combinations else 1)
                assert frozen["agent_runtime_bindings"][0]["agent_image_ref"] == a.agent_image_ref
                batch.state = "finished"
                batch.result_status = "all_failed"
                batch.finished_at = now
                for index, combo in enumerate(batch.combinations or [None]):
                    cfg = _materialize_trial_config(batch.trial_config, combo)
                    session.add(
                        Trial(
                            id=uuid4(),
                            task_id=task_id,
                            team_id=team,
                            state="failed",
                            failure_reason="gateway_error",
                            failure_message="gateway 503",
                            config=cfg,
                            requires_caps={
                                "backend": "nebius",
                                "worker_pool": "nebius-cpu",
                                "cpu_arch": "x86_64",
                            },
                            submitted_at=now,
                            batch_id=batch_id,
                            combination_idx=index,
                            sample_idx=0,
                        )
                    )
            if runtime_mode == "prebound":
                async with app.state.session_factory() as session, session.begin():
                    task_row = await session.get(Task, task_id)
                    task_row.config = {
                        **task_row.config,
                        "service_execution": _service_execution_task_config(task_id)["service_execution"],
                    }
            current_profile = profile.model_copy(update={
                "candidate_sha": "2" * 40,
                "agent_image_ref": b.agent_image_ref,
                "image_admission": profile.image_admission.model_copy(update={
                    "admissions": (*profile.image_admission.admissions, b.image_admission),
                }),
            })
            if runtime_mode == "no_controller":
                current_profile = current_profile.model_copy(update={"agent_image_ref": None})
            app.state.settings = app.state.settings.model_copy(update={
                "service_execution_runtime_profile_json": (
                    current_profile.model_dump_json()
                    if runtime_mode not in {"frozen", "unavailable"} else "{}"
                ),
            })
            async with app.state.session_factory() as session, session.begin():
                await session.execute(
                    delete(Agent).where(Agent.version.in_([a.agent_version, b.agent_version]))
                )
            rerun = await client.post(
                f"/api/v1/batches/{batch_id}/rerun-failed", headers=headers,
                json={"use_current_runtime": runtime_mode not in {"frozen", "incompatible_frozen"}},
            )
            if runtime_mode not in {"frozen", "current"}:
                assert rerun.status_code == 400, rerun.text
                if runtime_mode == "incompatible_frozen":
                    assert "instance_id" in rerun.text
                    assert a.agent_version in rerun.text
                async with app.state.session_factory() as session:
                    children = (await session.scalars(select(Batch).where(
                        Batch.rerun_of_batch_id == batch_id,
                    ))).all()
                    assert children == []
                    parent = await session.get(Batch, batch_id)
                    assert parent.trial_config == original_config
                    assert parent.combinations == original_combinations
                    assert parent.service_execution_runtime_profile == parent_profile
                return
            assert rerun.status_code == 201, rerun.text
            async with app.state.session_factory() as session:
                row = await session.get(Batch, UUID(rerun.json()["batch_id"]))
                parent = await session.get(Batch, batch_id)
                assert parent.trial_config == original_config
                assert parent.combinations == original_combinations
                assert parent.service_execution_runtime_profile == parent_profile
                assert row.rerun_of_batch_id == batch_id
                assert row.trial_config["retry"] == original_config["retry"]
                assert row.trial_config["override_agent_timeout_sec"] == 120
                assert row.provider_connection_id == parent.provider_connection_id
                assert row.provider_model_id == parent.provider_model_id
                assert row.resolved_task_ids == [task_id]
                if runtime_mode == "current":
                    assert row.service_execution_runtime_profile == current_profile.model_dump(mode="json")
                    selections = row.combinations or [row.trial_config]
                    assert all(x.get("agent_version") is None for x in selections)
                    expected_config = deepcopy(original_config)
                    expected_config.pop("agent_version", None)
                    expected_config["model_switch_plan_mode"] = "inherit"
                    expected_combinations = deepcopy(original_combinations)
                    for item in expected_combinations:
                        item.pop("agent_version", None)
                    assert row.trial_config == expected_config
                    assert row.combinations == expected_combinations
                    assert [(x["sample_idx"], x["combination_idx"]) for x in row.rerun_targets] == (
                        [(0, 0), (0, 1)] if combinations else [(0, 0)]
                    )
                else:
                    assert row.service_execution_runtime_profile == frozen
                actual = (
                    await session.scalars(select(Trial).where(Trial.batch_id == batch_id))
                ).all()
                assert {t.config["agent_version"] for t in actual} == (
                    {a.agent_version, b.agent_version} if combinations else {a.agent_version}
                )
    finally:
        async with app.state.session_factory() as session, session.begin():
            await session.execute(
                delete(ServiceExecutionTarget).where(ServiceExecutionTarget.id == target_id)
            )
            if created_class:
                await session.execute(
                    delete(ServiceExecutionClass).where(ServiceExecutionClass.id == class_id)
                )
            await session.execute(
                delete(Agent).where(Agent.version.in_([a.agent_version, b.agent_version]))
            )

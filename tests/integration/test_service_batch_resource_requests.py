"""Ordinary batch request overrides remain source-bound, scoped and immutable."""
from __future__ import annotations

from collections.abc import AsyncIterator
from copy import deepcopy
from datetime import UTC, datetime
from typing import Any
from uuid import UUID, uuid4

import httpx
import pytest
from sqlalchemy import create_engine, delete, func, select
from sqlalchemy.orm import sessionmaker

from loom.db.schema import Batch, ServiceExecutionClass, ServiceExecutionTarget, Task, Trial, Worker
from loom.execution_contract import NEBIUS_CPU_EXECUTION_CLASS_V1
from loom.execution_runtime_contract import runtime_pod_resources
from loom.models.task import TaskConfig
from loom.models.trial import TrialConfig
from loom.pipeline.keys import canonical_digest
from loom.service_execution_materialization import (
    ServiceExecutionRuntimeProfileV1,
    compile_service_execution_plan,
)
from tests.integration.test_service_batches_crud import (
    _automatic_service_execution_task_config,
    _service_execution_runtime_profile,
    camp_setup,  # noqa: F401
)
from tests.support.execution_image_admission import signed_image_admission_bundle
from tests.unit.test_execution_resource_requests import _render


@pytest.fixture
async def native_resource_batch(camp_setup, postgres_url: str) -> AsyncIterator[dict[str, Any]]:  # noqa: F811
    app, raw, team_id = camp_setup
    profile = _service_execution_runtime_profile()
    controller = "registry.example/controller@sha256:" + "9" * 64
    profile = profile.model_copy(update={
        "agent_image_ref": controller,
        "image_admission": signed_image_admission_bundle((
            profile.task_image_ref, profile.runtime_image_ref, controller,
        )),
    })
    app.state.settings = app.state.settings.model_copy(update={
        "service_execution_runtime_profile_json": profile.model_dump_json(),
    })
    engine = create_engine(postgres_url)
    sessions = sessionmaker(engine)
    task_ids = [f"local/request-{uuid4().hex}" for _ in range(2)]
    target_id = "nebius-requests-" + uuid4().hex
    spec = NEBIUS_CPU_EXECUTION_CLASS_V1.model_dump(mode="json")
    with sessions() as s:
        s.execute(delete(Worker))
        s.add(ServiceExecutionClass(
            id=NEBIUS_CPU_EXECUTION_CLASS_V1.class_id,
            schema_version=NEBIUS_CPU_EXECUTION_CLASS_V1.schema_version,
            spec_json=spec, spec_sha256=canonical_digest(spec), enabled=True,
        ))
        s.add(ServiceExecutionTarget(
            id=target_id, logical_pool_id="nebius-cpu",
            execution_class_id=NEBIUS_CPU_EXECUTION_CLASS_V1.class_id,
            schema_version="loom.execution-target.v1", spec_json={"health_stale_after_seconds": 60},
            spec_sha256="sha256:" + "e" * 64, environment="development", provider="nebius",
            region="eu-north1", failure_domain="eu-north1-a", data_residency="eu",
            desired_state="active", observed_state="ready", health_status="healthy",
            health_observed_at=datetime.now(UTC),
        ))
        for task_id in task_ids:
            s.add(Task(
                id=task_id, checksum="c" * 64,
                config=_automatic_service_execution_task_config(task_id),
                source="s3://artifacts/task-inputs/task/", license="MIT",
                source_provenance={"service_execution_input": {
                    "schema_version": "loom.service-execution-input.v1",
                    "manifest_uri": "s3://artifacts/task-inputs/task.json",
                    "manifest_sha256": "sha256:" + "d" * 64, "file_count": 3, "total_bytes": 4096,
                }},
            ))
        s.commit()
    payload = {
        "task_filter": {"task_ids": task_ids}, "backend": "nebius", "n_per_task": 1,
        "purpose": "evaluation",
        "trial_config": {"agent_name": "terminus-2", "agent_model": {
            "provider": "openai", "name": "gpt-5", "source": "api",
        }, "retry": {"max_attempts": 1, "retry_on": []}},
        "task_resource_requests": {task_ids[0]: {
            "task_revision_sha256": "sha256:" + "c" * 64,
            "requests": {"controller": {
                "cpu_millis": 1000, "memory_mib": 1024, "ephemeral_storage_mib": 512,
            }},
        }},
    }
    try:
        yield dict(app=app, raw=raw, team_id=team_id, sessions=sessions,
                   profile=profile, task_ids=task_ids, payload=payload)
    finally:
        with sessions() as s:
            s.execute(delete(ServiceExecutionTarget).where(ServiceExecutionTarget.id == target_id))
            s.execute(delete(ServiceExecutionClass).where(ServiceExecutionClass.id == spec["class_id"]))
            s.commit()
        engine.dispose()


@pytest.mark.parametrize("with_combinations", [False, True])
async def test_resource_requests_are_batch_scoped_without_task_mutation(native_resource_batch, with_combinations):
    f = native_resource_batch
    if with_combinations:
        f["payload"]["combinations"] = [{"agent_name": "terminus-2", "agent_model": f["payload"]["trial_config"]["agent_model"], "n_per_task": 1, "label": label} for label in ("a", "b")]
        f["payload"]["trial_config"].pop("agent_name")
        f["payload"]["trial_config"].pop("agent_model")
    original_profile = f["app"].state.settings.service_execution_runtime_profile_json
    with f["sessions"]() as s:
        before = {t.id: (t.checksum, deepcopy(t.config)) for t in s.scalars(
            select(Task).where(Task.id.in_(f["task_ids"])))}
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=f["app"]), base_url="http://svc") as c:
        headers = {"Authorization": "Bearer " + f["raw"]}
        response = await c.post("/api/v1/batches", headers=headers, json=f["payload"])
        assert response.status_code == 201, response.text
        batch_id = response.json()["batch_id"]
        detail = await c.get("/api/v1/batches/" + batch_id, headers=headers)
        assert detail.json()["task_resource_requests"] == f["payload"]["task_resource_requests"]
        default = deepcopy(f["payload"])
        default.pop("task_resource_requests")
        response = await c.post("/api/v1/batches", headers=headers, json=default)
        assert response.status_code == 201, response.text
        default_id = response.json()["batch_id"]
    assert f["app"].state.settings.service_execution_runtime_profile_json == original_profile
    with f["sessions"]() as s:
        batch = s.get(Batch, UUID(batch_id))
        assert batch.service_execution_runtime_profile["task_resource_requests"] == f["payload"]["task_resource_requests"]
        assert not s.get(Batch, UUID(default_id)).service_execution_runtime_profile.get("task_resource_requests")
        assert {t.id: (t.checksum, t.config) for t in s.scalars(
            select(Task).where(Task.id.in_(f["task_ids"])))} == before


@pytest.mark.parametrize("explicit_override", [False, True])
async def test_ordinary_submission_freezes_deployment_requests_through_pod_render(
    native_resource_batch, explicit_override: bool,
):
    f = native_resource_batch
    requests = {
        "controller": {"cpu_millis": 250, "memory_mib": 512, "ephemeral_storage_mib": 100},
        "task_sandbox": {"cpu_millis": 500, "memory_mib": 1024, "ephemeral_storage_mib": 300},
        "verifier_sandbox": {"cpu_millis": 250, "memory_mib": 512, "ephemeral_storage_mib": 100},
    }
    policy = {task_id: {"task_revision_sha256": "sha256:" + "c" * 64,
                        "requests": deepcopy(requests)} for task_id in f["task_ids"]}
    # An environment can configure other catalog tasks without admitting them here.
    policy["unselected/task"] = deepcopy(policy[f["task_ids"][0]])
    profile = ServiceExecutionRuntimeProfileV1.model_validate({
        **f["profile"].model_dump(mode="json"), "task_resource_requests": policy,
    })
    f["app"].state.settings = f["app"].state.settings.model_copy(update={
        "service_execution_runtime_profile_json": profile.model_dump_json(),
    })
    payload = deepcopy(f["payload"])
    payload.pop("task_resource_requests")
    expected = {task_id: policy[task_id] for task_id in f["task_ids"]}
    if explicit_override:
        selected = f["task_ids"][0]
        expected[selected] = deepcopy(policy[selected])
        expected[selected]["requests"]["controller"]["cpu_millis"] = 500
        payload["task_resource_requests"] = {selected: expected[selected]}
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=f["app"]), base_url="http://svc") as c:
        headers = {"Authorization": "Bearer " + f["raw"]}
        response = await c.post("/api/v1/batches", headers=headers, json=payload)
        assert response.status_code == 201, response.text
        batch_id = response.json()["batch_id"]
        detail = await c.get("/api/v1/batches/" + batch_id, headers=headers)
        assert detail.json()["task_resource_requests"] == expected
    with f["sessions"]() as session:
        batch = session.get(Batch, UUID(batch_id))
        frozen = ServiceExecutionRuntimeProfileV1.model_validate(batch.service_execution_runtime_profile)
        for task_id in f["task_ids"]:
            row = session.get(Task, task_id)
            task = TaskConfig.model_validate(row.config)
            plan = compile_service_execution_plan(
                task=task, trial=TrialConfig.model_validate(payload["trial_config"]),
                task_id=task_id, task_revision_sha256="sha256:" + row.checksum,
                source_provenance=row.source_provenance, profile=frozen,
            )
            totals = runtime_pod_resources(plan)
            assert totals.model_dump() == {
                "cpu_millis": 1000 if explicit_override and task_id == f["task_ids"][0] else 750,
                "memory_mib": 1536, "ephemeral_storage_mib": 400,
            }
            pod = _render(plan, task)
            assert pod["containers"][0]["resources"]["requests"]["ephemeral-storage"] == "100Mi"
            assert row.config == _automatic_service_execution_task_config(task_id)


_NEBIUS_DEFAULT_REQUESTS = {
    "controller": {"cpu_millis": 200, "memory_mib": 512, "ephemeral_storage_mib": 512},
    "task_sandbox": {"cpu_millis": 600, "memory_mib": 1024, "ephemeral_storage_mib": 1024},
    "verifier_sandbox": {"cpu_millis": 200, "memory_mib": 512, "ephemeral_storage_mib": 512},
}


def _set_default_requests(f, *, requests=None, task_requests=None):
    profile = ServiceExecutionRuntimeProfileV1.model_validate({
        **f["profile"].model_dump(mode="json"),
        "default_task_resource_requests": requests or deepcopy(_NEBIUS_DEFAULT_REQUESTS),
        "task_resource_requests": task_requests or {},
    })
    f["app"].state.settings = f["app"].state.settings.model_copy(update={
        "service_execution_runtime_profile_json": profile.model_dump_json(),
    })


@pytest.mark.parametrize("with_combinations", [False, True])
async def test_new_catalog_tasks_and_revisions_receive_default_requests_through_render(
    native_resource_batch, with_combinations: bool,
):
    f = native_resource_batch
    _set_default_requests(f)
    payload = deepcopy(f["payload"])
    payload.pop("task_resource_requests")
    trial_config = deepcopy(payload["trial_config"])
    if with_combinations:
        payload["combinations"] = [{"agent_name": "terminus-2",
            "agent_model": trial_config["agent_model"], "n_per_task": 1, "label": "default"}]
        payload["trial_config"].pop("agent_name")
        payload["trial_config"].pop("agent_model")
    # Both task IDs are newly generated, with no task-map entry. A newly
    # published revision must inherit the baseline without policy reapproval.
    with f["sessions"]() as session:
        session.get(Task, f["task_ids"][1]).checksum = "e" * 64
        session.commit()
        before = {row.id: (row.checksum, deepcopy(row.config)) for row in session.scalars(
            select(Task).where(Task.id.in_(f["task_ids"])),
        )}
    expected = {task_id: {"task_revision_sha256": "sha256:" + revision,
                         "requests": _NEBIUS_DEFAULT_REQUESTS}
                for task_id, (revision, _) in before.items()}
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=f["app"]), base_url="http://svc") as c:
        headers = {"Authorization": "Bearer " + f["raw"]}
        response = await c.post("/api/v1/batches", headers=headers, json=payload)
        assert response.status_code == 201, response.text
        batch_id = response.json()["batch_id"]
        detail = await c.get("/api/v1/batches/" + batch_id, headers=headers)
        assert detail.json()["task_resource_requests"] == expected
    with f["sessions"]() as session:
        batch = session.get(Batch, UUID(batch_id))
        frozen = ServiceExecutionRuntimeProfileV1.model_validate(batch.service_execution_runtime_profile)
        for task_id, (revision, raw_config) in before.items():
            row = session.get(Task, task_id)
            assert (row.checksum, row.config) == (revision, raw_config)
            task = TaskConfig.model_validate(row.config)
            plan = compile_service_execution_plan(
                task=task, trial=TrialConfig.model_validate(trial_config), profile=frozen,
                task_id=task_id, task_revision_sha256="sha256:" + row.checksum,
                source_provenance=row.source_provenance,
            )
            assert runtime_pod_resources(plan).model_dump() == {
                "cpu_millis": 800, "memory_mib": 1536, "ephemeral_storage_mib": 1536,
            }
            pod = _render(plan, task)
            containers = {row["name"]: row for row in
                          [*pod["containers"], *pod["initContainers"][1:]]}
            for name, role in (("execution", "controller"), ("task-sandbox", "task_sandbox")):
                requested = _NEBIUS_DEFAULT_REQUESTS[role]
                assert containers[name]["resources"]["requests"] == {
                    "cpu": f"{requested['cpu_millis']}m",
                    "memory": f"{requested['memory_mib']}Mi",
                    "ephemeral-storage": f"{requested['ephemeral_storage_mib']}Mi",
                }
                assert containers[name]["resources"]["limits"]["ephemeral-storage"] == "2048Mi"


@pytest.mark.parametrize("explicit_override", [False, True])
async def test_task_policy_and_explicit_override_take_precedence_over_default(
    native_resource_batch, explicit_override: bool,
):
    f = native_resource_batch
    task_id = f["task_ids"][0]
    configured = {"task_revision_sha256": "sha256:" + "c" * 64,
                  "requests": deepcopy(_NEBIUS_DEFAULT_REQUESTS)}
    configured["requests"]["controller"]["cpu_millis"] = 250
    _set_default_requests(f, task_requests={task_id: configured})
    payload = deepcopy(f["payload"])
    payload.pop("task_resource_requests")
    expected = deepcopy(configured)
    if explicit_override:
        expected["requests"]["controller"]["cpu_millis"] = 350
        payload["task_resource_requests"] = {task_id: expected}
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=f["app"]), base_url="http://svc") as c:
        headers = {"Authorization": "Bearer " + f["raw"]}
        response = await c.post("/api/v1/batches", headers=headers, json=payload)
        assert response.status_code == 201, response.text
        detail = await c.get("/api/v1/batches/" + response.json()["batch_id"], headers=headers)
    assert detail.json()["task_resource_requests"] == {
        task_id: expected, f["task_ids"][1]: {
            "task_revision_sha256": "sha256:" + "c" * 64, "requests": _NEBIUS_DEFAULT_REQUESTS,
        },
    }


@pytest.mark.parametrize("current_runtime", [False, True])
async def test_default_policy_changes_do_not_rewrite_frozen_batch_or_rerun_requests(
    native_resource_batch, current_runtime: bool,
):
    f = native_resource_batch
    _set_default_requests(f)
    payload = deepcopy(f["payload"])
    payload.pop("task_resource_requests")
    payload["task_filter"]["task_ids"] = [f["task_ids"][0]]
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=f["app"]), base_url="http://svc") as c:
        headers = {"Authorization": "Bearer " + f["raw"]}
        response = await c.post("/api/v1/batches", headers=headers, json=payload)
        assert response.status_code == 201, response.text
        parent_id = response.json()["batch_id"]
        with f["sessions"]() as session:
            frozen_before = deepcopy(session.get(Batch, UUID(parent_id)).service_execution_runtime_profile)
            session.add(Trial(id=uuid4(), task_id=f["task_ids"][0], team_id=f["team_id"],
                batch_id=UUID(parent_id), state="failed", failure_reason="gateway_error",
                config=payload["trial_config"], requires_caps={}, sample_idx=0, combination_idx=0,
                submitted_at=datetime.now(UTC), finished_at=datetime.now(UTC)))
            session.commit()
        changed = deepcopy(_NEBIUS_DEFAULT_REQUESTS)
        changed["controller"]["cpu_millis"] = 400
        _set_default_requests(f, requests=changed)
        rerun = await c.post("/api/v1/batches/" + parent_id + "/rerun-failed", headers=headers,
                             json={"use_current_runtime": current_runtime})
        assert rerun.status_code == 201, rerun.text
        detail = await c.get("/api/v1/batches/" + rerun.json()["batch_id"], headers=headers)
        assert detail.json()["task_resource_requests"] == frozen_before["task_resource_requests"]
        fresh = await c.post("/api/v1/batches", headers=headers, json=payload)
        assert fresh.status_code == 201, fresh.text
        detail = await c.get("/api/v1/batches/" + fresh.json()["batch_id"], headers=headers)
        assert detail.json()["task_resource_requests"][f["task_ids"][0]]["requests"] == changed
    with f["sessions"]() as session:
        assert session.get(Batch, UUID(parent_id)).service_execution_runtime_profile == frozen_before


async def test_default_requests_reject_task_with_lower_limits_before_creating_batch(native_resource_batch):
    f = native_resource_batch
    _set_default_requests(f)
    with f["sessions"]() as session:
        task = session.get(Task, f["task_ids"][0])
        config = deepcopy(task.config)
        config["environment"]["storage_mb"] = 512
        task.config = config
        session.commit()
        before = (session.scalar(select(func.count()).select_from(Batch)),
                  session.scalar(select(func.count()).select_from(Trial)))
    payload = deepcopy(f["payload"])
    payload.pop("task_resource_requests")
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=f["app"]), base_url="http://svc") as c:
        response = await c.post("/api/v1/batches", headers={"Authorization": "Bearer " + f["raw"]},
                                json=payload)
    assert response.status_code == 400, response.text
    assert "hard limits" in response.text
    with f["sessions"]() as session:
        assert (session.scalar(select(func.count()).select_from(Batch)),
                session.scalar(select(func.count()).select_from(Trial))) == before
        assert session.get(Task, f["task_ids"][0]).config == config


@pytest.mark.parametrize("invalid", ["stale_revision", "exceeds_limit"])
async def test_deployment_requests_cannot_apply_to_changed_task_limits_or_revision(
    native_resource_batch, invalid: str,
):
    f = native_resource_batch
    policy = deepcopy(f["payload"]["task_resource_requests"])
    entry = policy[f["task_ids"][0]]
    if invalid == "stale_revision":
        entry["task_revision_sha256"] = "sha256:" + "0" * 64
    else:
        entry["requests"]["controller"]["memory_mib"] = 100_000
    profile = ServiceExecutionRuntimeProfileV1.model_validate({
        **f["profile"].model_dump(mode="json"), "task_resource_requests": policy,
    })
    f["app"].state.settings = f["app"].state.settings.model_copy(update={
        "service_execution_runtime_profile_json": profile.model_dump_json(),
    })
    payload = deepcopy(f["payload"])
    payload.pop("task_resource_requests")
    with f["sessions"]() as session:
        before = session.scalar(select(func.count()).select_from(Batch))
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=f["app"]), base_url="http://svc") as c:
        response = await c.post("/api/v1/batches", headers={"Authorization": "Bearer " + f["raw"]}, json=payload)
    assert response.status_code == 400, response.text
    assert ("source revision" if invalid == "stale_revision" else "hard limits") in response.text
    with f["sessions"]() as session:
        assert session.scalar(select(func.count()).select_from(Batch)) == before



@pytest.mark.parametrize("invalid", ["unknown_task", "revision", "oversized", "nonpositive", "empty_roles", "unknown_role", "non_terminus", "docker", "bool", "fraction"])
async def test_invalid_resource_requests_create_no_batch(native_resource_batch, invalid: str):
    f = native_resource_batch
    payload = deepcopy(f["payload"])
    entry = payload["task_resource_requests"][f["task_ids"][0]]
    if invalid == "unknown_task":
        payload["task_resource_requests"] = {"unselected/task": entry}
    elif invalid == "revision":
        entry["task_revision_sha256"] = "sha256:" + "0" * 64
    elif invalid == "oversized":
        entry["requests"]["controller"]["memory_mib"] = 100_000
    elif invalid == "nonpositive":
        entry["requests"]["controller"]["ephemeral_storage_mib"] = 0
    elif invalid == "bool":
        entry["requests"]["controller"]["cpu_millis"] = True
    elif invalid == "fraction":
        entry["requests"]["controller"]["cpu_millis"] = 100.5
    elif invalid == "empty_roles":
        entry["requests"] = {}
    elif invalid == "unknown_role":
        entry["requests"]["extra"] = entry["requests"].pop("controller")
    elif invalid == "non_terminus":
        payload["trial_config"]["agent_name"] = "direct-completion"
    elif invalid == "docker":
        payload["backend"] = "docker"
    with f["sessions"]() as s:
        before = (s.scalar(select(func.count()).select_from(Batch)), s.scalar(select(func.count()).select_from(Trial)))
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=f["app"]), base_url="http://svc") as c:
        r = await c.post("/api/v1/batches", headers={"Authorization": "Bearer " + f["raw"]}, json=payload)
    assert r.status_code in (400, 422), r.text
    with f["sessions"]() as s:
        assert (s.scalar(select(func.count()).select_from(Batch)), s.scalar(select(func.count()).select_from(Trial))) == before


@pytest.mark.parametrize("with_combinations", [False, True])
@pytest.mark.parametrize("current_runtime", [False, True])
async def test_rerun_preserves_requests_and_drops_unselected_entries(native_resource_batch, current_runtime: bool, with_combinations: bool):
    f = native_resource_batch
    payload = deepcopy(f["payload"])
    if with_combinations:
        payload["combinations"] = [{"agent_name": "terminus-2", "agent_model": payload["trial_config"]["agent_model"], "n_per_task": 1, "label": "rerun"}]
        payload["trial_config"].pop("agent_name")
        payload["trial_config"].pop("agent_model")
    payload["task_resource_requests"][f["task_ids"][1]] = deepcopy(next(iter(payload["task_resource_requests"].values())))
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=f["app"]), base_url="http://svc") as c:
        headers = {"Authorization": "Bearer " + f["raw"]}
        r = await c.post("/api/v1/batches", headers=headers, json=payload)
        assert r.status_code == 201, r.text
        parent_id = r.json()["batch_id"]
        with f["sessions"]() as s:
            for index, task_id in enumerate(f["task_ids"]):
                s.add(Trial(id=uuid4(), task_id=task_id, team_id=f["team_id"],
                    batch_id=UUID(parent_id), state="failed" if index == 0 else "succeeded",
                    failure_reason="gateway_error" if index == 0 else None,
                    result={"reward": 1.0} if index else None,
                    config={**payload["trial_config"], **({k: payload["combinations"][0][k] for k in ("agent_name", "agent_model")} if with_combinations else {})}, requires_caps={}, sample_idx=0, combination_idx=0,
                    submitted_at=datetime.now(UTC), finished_at=datetime.now(UTC)))
            s.commit()
        r = await c.post("/api/v1/batches/" + parent_id + "/rerun-failed", headers=headers,
                         json={"use_current_runtime": current_runtime})
        assert r.status_code == 201, r.text
        child_id = r.json()["batch_id"]
        detail = await c.get("/api/v1/batches/" + child_id, headers=headers)
        assert detail.json()["task_resource_requests"] == {
            f["task_ids"][0]: payload["task_resource_requests"][f["task_ids"][0]],
        }


@pytest.mark.parametrize("node_share", [False, True])
async def test_node_share_submission_does_not_reject_small_task_with_retired_default(
    native_resource_batch, node_share,
):
    f = native_resource_batch
    _set_default_requests(f)
    profile = ServiceExecutionRuntimeProfileV1.model_validate_json(
        f["app"].state.settings.service_execution_runtime_profile_json,
    )
    if node_share:
        profile = profile.model_copy(update={"resource_allocation_policy": "node-share-v1"})
    f["app"].state.settings = f["app"].state.settings.model_copy(update={
        "service_execution_runtime_profile_json": profile.model_dump_json(),
    })
    with f["sessions"]() as session:
        for task_id in f["task_ids"]:
            task = session.get(Task, task_id)
            config = deepcopy(task.config)
            config["environment"]["memory_mb"] = 256
            task.config = config
        session.commit()
    payload = deepcopy(f["payload"])
    payload.pop("task_resource_requests")
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=f["app"]), base_url="http://svc") as client:
        response = await client.post("/api/v1/batches", json=payload,
            headers={"Authorization": "Bearer " + f["raw"]})
    if not node_share:
        assert response.status_code == 400 and "exceed hard limits" in response.text
        return
    assert response.status_code == 201, response.text
    with f["sessions"]() as session:
        batch = session.get(Batch, UUID(response.json()["batch_id"]))
        frozen = ServiceExecutionRuntimeProfileV1.model_validate(batch.service_execution_runtime_profile)
        assert frozen.task_resource_requests == {}
        task = session.get(Task, f["task_ids"][0])
        plan = compile_service_execution_plan(task=TaskConfig.model_validate(task.config),
            trial=TrialConfig.model_validate(payload["trial_config"]), profile=frozen,
            task_id=task.id, task_revision_sha256="sha256:" + task.checksum,
            source_provenance=task.source_provenance)
        assert plan.task_resources.memory_mib == 256

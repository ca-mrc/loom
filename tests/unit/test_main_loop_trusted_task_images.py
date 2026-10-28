"""The real worker preparation path must preserve signed image/start bindings."""

import asyncio
import importlib
import json
from datetime import timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import UUID

import pytest

from loom_worker import main_loop as ml
from loom_worker.runner_pool import RunnerPool
from tests.unit.test_main_loop_cleanup import _FakeCPClient, _FakeSettings
from tests.unit.test_task_image_publication_signing import NOW
from tests.unit.test_worker_claim_loop import _healthy_setup_node
from tests.unit.test_worker_task_image_execution import (
    accepting,
    evidence,
    refreshed,
    signed_evidence,
)


def delivery(kwargs):
    return dict(
        schema="loom.task-image-execution-delivery/v2",
        claim=kwargs["expected_claim"].model_dump(mode="json", exclude_none=True),
        grant_envelope=kwargs["wire"].decode(), frozen_plan=kwargs["plan_wire"].decode(),
        publications=[item.decode() for item in kwargs["publication_wires"]],
        keyset=kwargs["keyset_wire"].decode(),
    )


@pytest.mark.parametrize("case", ["valid", "slow-pull", "no_root", "wrong_worker", "wrong_task", "layered", "denied", "cancelled"])
async def test_signed_claim_preparation_and_start_stay_bound(tmp_path, monkeypatch, case):
    task_dir = tmp_path / "source"
    task_dir.mkdir()
    grant, private, kwargs = signed_evidence(task_dir)
    now = NOW
    cp, settings, captured = _FakeCPClient(), _FakeSettings(), {}
    cp.consume_task_image_execution_start = accepting()
    from loom_task_image_authority.execution_delivery import TaskImageExecutionDelivery

    cp.refresh_task_image_execution = AsyncMock(return_value=TaskImageExecutionDelivery.model_validate(delivery(kwargs)))
    if case == "slow-pull":
        async def accept(request):
            from loom_task_image_authority.execution_start import ExecutionStartReceipt

            return ExecutionStartReceipt.model_validate(dict(
                schema="loom.task-image-execution-start-receipt/v1", start_id=grant["grant_id"],
                request_sha256=request.digest, consumed_at=now.strftime("%Y-%m-%dT%H:%M:%SZ"),
                expires_at=(now + timedelta(seconds=20)).strftime("%Y-%m-%dT%H:%M:%SZ"),
            ))
        cp.consume_task_image_execution_start = AsyncMock(side_effect=accept)
        cp.refresh_task_image_execution.return_value = refreshed(grant, private, kwargs, NOW + timedelta(seconds=121))
    if case == "denied":
        cp.consume_task_image_execution_start = AsyncMock(side_effect=ConnectionError("offline"))
    elif case == "cancelled":
        cp.consume_task_image_execution_start = AsyncMock(side_effect=asyncio.CancelledError)
    m = importlib.import_module("loom_worker.task_image_execution")
    assert hasattr(m, "WorkerExecutionTrust"), "main-loop release trust adapter missing"
    trust = m.WorkerExecutionTrust(
        root=kwargs["trust_root"], purpose="production", shadow_campaign_id=None,
        clock=lambda: now,
    )
    payload = dict(
        trial_id=grant["claim"]["trial_id"], team_id=grant["claim"]["team_id"],
        task_id=json.loads(grant["canonical_task_config"])["task"]["id"],
        attempt_count=grant["claim"]["trial_attempt_count"],
        config={"agent_name": "oracle", "agent_model": None},
        task_image_execution=delivery(kwargs),
    )
    worker_id = UUID(grant["claim"]["worker_id"])
    if case == "wrong_worker":
        worker_id = UUID("99999999-9999-4999-8999-999999999999")
    if case == "wrong_task":
        payload["task_id"] = "different-task"
    expected_image = grant["components"][0]["image"]
    materialize = AsyncMock(return_value=task_dir)
    resolve = AsyncMock(return_value="unsigned:layer" if case == "layered" else expected_image)
    if case == "slow-pull":
        async def slow_pull(**_):
            nonlocal now
            now += timedelta(seconds=121)
            return expected_image
        resolve.side_effect = slow_pull
    layer = AsyncMock(side_effect=AssertionError("must not derive an unsigned layer"))
    monkeypatch.setattr(ml, "_materialize_task_dir", materialize)
    monkeypatch.setattr(ml, "resolve_task_image", resolve)
    monkeypatch.setattr(ml, "_resolve_layered_trial_image", layer)
    monkeypatch.setattr(ml, "_host_cpu_arch", lambda: "x86_64")

    class Runner:
        def __init__(self, **values):
            captured.update(values)

        async def run(self):
            assert await captured["start_authorization"]() is True
            captured["ran"] = True

        async def interrupt_attempt(self):
            pass

    runner = Mock(side_effect=Runner)
    monkeypatch.setattr(ml, "LocalTrialRunner", runner)
    pool = RunnerPool(max_concurrent=1)
    await ml._spawn_trial(
        pool=pool, settings=settings, cp_client=cp, gateway_client=None,
        object_store=None, worker_id=worker_id, payload=payload,
        execution_trust=None if case == "no_root" else trust,
    )
    await pool.wait_all(timeout=2)
    assert cp.bundle_requests == 0
    layer.assert_not_called()
    if case in {"valid", "slow-pull"}:
        assert captured["ran"]
        assert captured["start_authorization"] is not None
        assert resolve.call_args.kwargs["registry_image"] == expected_image
        assert resolve.call_args.kwargs["build_if_missing"] is False
        assert captured["sidecar_runtime_factory"]().registry_images == {
            item["component"]: item["image"] for item in grant["components"]
        }
        cp.consume_task_image_execution_start.assert_awaited_once()
        assert cp.refresh_task_image_execution.await_count == (2 if case == "slow-pull" else 1)
        assert not task_dir.exists()
    else:
        assert not captured.get("ran")
        if case == "cancelled":
            assert cp.patch_calls == []
        else:
            assert cp.patch_calls[-1]["state"] == "failed"
        if case not in {"denied", "cancelled"}:
            cp.consume_task_image_execution_start.assert_not_called()
        if case in {"no_root", "wrong_worker", "wrong_task"}:
            materialize.assert_not_called()
            runner.assert_not_called()
        else:
            assert not task_dir.exists()


@pytest.mark.parametrize("pipeline", [True, False])
async def test_shared_claim_parser_preserves_signed_wire_and_trusted_context(tmp_path, monkeypatch, pipeline):
    grant, kwargs = evidence(tmp_path)
    raw_delivery = delivery(kwargs)
    payload = dict(
        trial_id=grant["claim"]["trial_id"], team_id=grant["claim"]["team_id"],
        task_id=json.loads(grant["canonical_task_config"])["task"]["id"],
        attempt_count=grant["claim"]["trial_attempt_count"], config={}, requires_caps={},
        provider_connection_id=None, family_key=None, family_state_uri=None,
        family_run_spec=None, state="claimed", task_image_execution=raw_delivery,
    )
    cp = _FakeCPClient()
    cp.claim_work = AsyncMock(side_effect=[dict(
        schema_version="loom.work-claim.v1", work_kind="trial", payload=payload,
    ), None])
    spawn = AsyncMock()
    monkeypatch.setattr(ml, "_spawn_trial", spawn)
    m = importlib.import_module("loom_worker.task_image_execution")
    trust = m.WorkerExecutionTrust(
        root=kwargs["trust_root"], purpose="production", shadow_campaign_id=None,
    )
    settings = _FakeSettings()
    settings.max_concurrent = 1
    assert await ml._claim_available_work(
        pool=RunnerPool(max_concurrent=1), settings=settings, cp_client=cp,
        gateway_client=None, object_store=None, worker_id=UUID(grant["claim"]["worker_id"]),
        capability_snapshot_digest="sha256:" + "1" * 64,
        pipeline_run=AsyncMock() if pipeline else None,
        read_setup_health=_healthy_setup_node, execution_trust=trust,
    ) == 1
    assert spawn.call_args.kwargs["payload"]["task_image_execution"] == raw_delivery
    assert spawn.call_args.kwargs["execution_trust"] is trust
    assert cp.claim_work.call_args.kwargs["supported_work_kinds"] == (["trial", "execution_attempt"] if pipeline else ["trial"])


async def test_release_trust_cannot_enable_legacy_body_capability_claims(tmp_path):
    _, kwargs = evidence(tmp_path)
    m = importlib.import_module("loom_worker.task_image_execution")
    trust = m.WorkerExecutionTrust(
        root=kwargs["trust_root"], purpose="production", shadow_campaign_id=None,
    )
    cp = Mock()
    with pytest.raises(ValueError, match="authenticated shared-queue"):
        await ml._claim_available_work(
            pool=RunnerPool(max_concurrent=1), settings=_FakeSettings(), cp_client=cp,
            gateway_client=None, object_store=None, worker_id=UUID(kwargs["expected_claim"].worker_id),
            capability_snapshot_digest=None, pipeline_run=None,
            execution_trust=trust,
        )
    assert cp.mock_calls == []


async def test_release_trust_rejects_http_before_worker_registration(tmp_path, monkeypatch):
    _, kwargs = evidence(tmp_path)
    m = importlib.import_module("loom_worker.task_image_execution")
    trust = m.WorkerExecutionTrust(
        root=kwargs["trust_root"], purpose="production", shadow_campaign_id=None,
    )
    effects = Mock(side_effect=AssertionError("worker assembly must not start"))
    monkeypatch.setattr(ml, "install_signal_handlers", effects)
    with pytest.raises(ValueError, match="HTTPS"):
        await ml.run_worker(
            SimpleNamespace(control_plane_url="http://cp.example"), execution_trust=trust,
        )
    effects.assert_not_called()


@pytest.mark.parametrize("case", ["v2", "v1", "no-pipeline", "http", "protected"])
async def test_registration_advertises_execution_reader_only_with_explicit_trust(tmp_path, monkeypatch, case):
    from loom.models.worker_capabilities import WorkerCapabilitySnapshotV1

    _, kwargs = evidence(tmp_path)
    m = importlib.import_module("loom_worker.task_image_execution")
    trust = m.WorkerExecutionTrust(root=kwargs["trust_root"], purpose="production", shadow_campaign_id=None)
    settings = SimpleNamespace(hostname="worker", max_concurrent=1, pool_name="fixture",
        executor_worker_credential="present" if case == "protected" else None,
        control_plane_url="http://cp.example" if case == "http" else "https://cp.example")
    snapshot = dict(schema_version="loom.worker-capabilities.v1", cpu_arch="x86_64", cpu_cores=2,
        memory_bytes=1024, scratch_bytes=1024, network_profiles=["gateway"],
        container_runtime_features=["loom-secret-tmpfs-v1"], gpu_devices=[],
        input_cache_capacity_bytes=0, input_cache_reserved_bytes=0, input_cache_ready_bytes=0)
    monkeypatch.setattr(ml, "_pipeline_registration_payload", lambda _: {"capability_snapshot": snapshot})
    monkeypatch.setattr(ml, "_trial_execution_registration_payload", lambda _: {"capability_snapshot": snapshot}, raising=False)
    cp = SimpleNamespace(register=AsyncMock(return_value={"worker_id": "fixture"}))
    call = dict(cp_client=cp, settings=settings, pipeline_enabled=case != "no-pipeline",
                execution_trust=trust if case != "v1" else None)
    if case in {"http", "protected"}:
        with pytest.raises(ValueError):
            await ml._register_worker_with_retry(**call)
        cp.register.assert_not_called()
    else:
        await ml._register_worker_with_retry(**call)
        registered = cp.register.call_args.kwargs
        checked = WorkerCapabilitySnapshotV1.model_validate_json(json.dumps(registered["capability_snapshot"]))
        assert ("task-image-execution-v2" in checked.container_runtime_features) is (case != "v1")
        if case != "v1":
            assert registered["capability_snapshot_digest"] == checked.digest
        assert registered["supported_work_kinds"] == (["trial"] if case == "no-pipeline" else ["trial", "execution_attempt"])


def test_trial_only_reader_measures_native_host_without_pipeline_capabilities(tmp_path, monkeypatch):
    monkeypatch.setattr(ml, "_host_cpu_arch", lambda: "arm64")
    monkeypatch.setattr(ml, "_pipeline_registration_payload", Mock(side_effect=AssertionError("Pipeline must remain disabled")))
    values = ml._trial_execution_registration_payload(SimpleNamespace(trajectory_cache_dir=tmp_path, pool_name="local_gpu"))
    snapshot = values["capability_snapshot"]
    assert snapshot["cpu_arch"] == "arm64"
    assert snapshot["cpu_cores"] > 0 and snapshot["memory_bytes"] > 0 and snapshot["scratch_bytes"] > 0
    assert snapshot["container_runtime_features"] == []  # Added only after release trust/HTTPS validation.
    assert snapshot["gpu_devices"] == []
    assert snapshot["input_cache_capacity_bytes"] == 0
    assert values["capabilities"][0]["cpu_arch"] == "arm64"


async def test_trial_only_reader_rejects_an_unrequested_pipeline_attempt(tmp_path):
    from loom.pipeline.work_protocol import ExecutionAttemptClaimV1
    from loom_worker.task_image_execution import WorkerExecutionTrust
    from tests.unit.test_pipeline_work_protocol import attempt_claim

    _, kwargs = evidence(tmp_path)
    trust = WorkerExecutionTrust(root=kwargs["trust_root"], purpose="production", shadow_campaign_id=None)
    cp = SimpleNamespace(claim_work=AsyncMock(return_value=dict(
        schema_version="loom.work-claim.v1", work_kind="execution_attempt", payload=ExecutionAttemptClaimV1.model_validate(attempt_claim()).model_dump(mode="json"))))
    pool = RunnerPool(max_concurrent=1)
    settings = _FakeSettings()
    settings.max_concurrent = 1
    with pytest.raises(RuntimeError, match="mismatched Pipeline claim"):
        await ml._claim_available_work(pool=pool, settings=settings, cp_client=cp, gateway_client=None,
            object_store=None, worker_id=UUID(kwargs["expected_claim"].worker_id), capability_snapshot_digest="sha256:" + "1" * 64,
            pipeline_run=None, read_setup_health=_healthy_setup_node,
            execution_trust=trust)
    assert pool.in_flight == 0

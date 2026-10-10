"""Actual active Pod disruption reaches the fenced PostgreSQL outcome once."""
from __future__ import annotations

import asyncio
import os
import subprocess
import tempfile
import time
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid4

import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom.db.schema import (
    ExecutionProvisioningAuthorization,
    ServiceExecutionEvent,
    ServiceExecutionLease,
    ServiceExecutionTarget,
    Trial,
)
from loom.execution_contract import WorkloadRequirementsV1
from loom.execution_runtime_contract import ExecutionRuntimePlanV1, ProcessPhaseV1
from loom_control_plane.service_execution import (
    ServiceExecutionConflict,
    ServiceExecutionFenceError,
    record_execution_event,
    record_kubernetes_observation,
    set_execution_target_health,
)
from loom_execution_actuator.controller import ExecutionActuator
from loom_execution_actuator.kubernetes_api import InClusterKubernetesJobApi
from loom_execution_actuator.renderer import ExecutionTargetRuntime
from tests.integration.test_execution_actuator_k3s import (
    _build_image,
    _docker_platform,
    _executable_lease,
    _import_image,
    _load_client,
    _pod_probe,
    _runtime_binary_digest,
    _start_k3s,
)
from tests.integration.test_service_execution_leases import (
    _cleanup_service_execution_test_rows,  # noqa: F401
    _reserve,
    _runtime_result_payload,
    _seed_ready_trial,
)

pytestmark = pytest.mark.skipif(
    os.environ.get("LOOM_RUN_DISPOSABLE_K3S") != "1",
    reason="set LOOM_RUN_DISPOSABLE_K3S=1 for disposable Kubernetes maintenance qualification",
)


@pytest.mark.timeout(360)
async def test_active_attempt_loss_while_draining_is_fenced_and_cleaned(
    postgres_url: str, record_property: Callable[[str, object], None],
) -> None:
    """A real active Pod loss cannot reopen admission, retry work or accept late success.

    The actual runtime runs the native fixture's HTTP server, whose response
    proves the agent phase started. There is intentionally no output authority:
    the loss must close as unavailable, never fabricate a committed result.
    The existing local runc RuntimeClass does not qualify deployed isolation.
    """
    from kubernetes import client

    suffix = uuid4().hex[:10]
    runtime_tag = f"docker.io/library/loom-maintenance-runtime:{suffix}"
    fixture_tag = f"docker.io/library/loom-maintenance-fixture:{suffix}"
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    container = None
    try:
        platform = await asyncio.to_thread(_docker_platform)
        for tag, dockerfile in (
            (runtime_tag, "deploy/Dockerfile.execution-runtime"),
            (fixture_tag, "tests/fixtures/execution_runtime_fixture/Dockerfile"),
        ):
            await asyncio.to_thread(_build_image, tag=tag, dockerfile=dockerfile, platform=platform)
        with tempfile.TemporaryDirectory(prefix="loom-maintenance-k3s-") as temporary:
            root = Path(temporary)
            runtime_digest = await asyncio.to_thread(_runtime_binary_digest, runtime_tag, root, platform)
            container = await asyncio.to_thread(_start_k3s)
            client_module, core, batch = await asyncio.to_thread(_load_client, container)
            runtime_image = await asyncio.to_thread(
                _import_image, container, tag=runtime_tag, root=root, ordinal=1,
            )
            task_image = await asyncio.to_thread(
                _import_image, container, tag=fixture_tag, root=root, ordinal=2,
            )
            record_property("runtime_image", runtime_image)
            record_property("task_image", task_image)
            record_property("runtime_binary_sha256", runtime_digest)
            now = datetime.now(UTC)
            async with sessions() as session:
                trial_id, target = await _seed_ready_trial(session, now=now)
                fixture = _executable_lease(
                    target.namespace_name, task_image_ref=task_image,
                    runtime_image_ref=runtime_image, runtime_binary_sha256=runtime_digest,
                )
                requirements = WorkloadRequirementsV1.model_validate(
                    {**fixture.workload_requirements_json, "sidecar_count": 0},
                )
                contract = ExecutionRuntimePlanV1.model_validate({
                    **fixture.runtime_contract_json,
                    "setup": [], "sidecars": [],
                    "main": ProcessPhaseV1(
                        role="agent", argv=("/fixture", "server", "8080"),
                        working_directory="/workspace", timeout_seconds=300,
                    ).model_dump(mode="json"),
                })
                lease = await _reserve(
                    session, trial_id=trial_id, target=target, now=now,
                    requirements=requirements, runtime_contract=contract,
                )
                trial = await session.get(Trial, trial_id)
                assert trial is not None
                waiting_trial = Trial(
                    id=uuid4(), team_id=trial.team_id, task_id=trial.task_id,
                    config=trial.config, requires_caps=trial.requires_caps,
                    state="queued", attempt_count=0,
                )
                session.add(waiting_trial)
                await session.commit()
            namespace = target.namespace_name
            await asyncio.to_thread(
                core.create_namespace, client.V1Namespace(metadata=client.V1ObjectMeta(name=namespace)),
            )
            await asyncio.to_thread(
                core.create_namespaced_service_account, namespace,
                client.V1ServiceAccount(
                    metadata=client.V1ObjectMeta(name="loom-execution-attempt"),
                    automount_service_account_token=False,
                ),
            )
            await asyncio.to_thread(
                client.NodeV1Api().create_runtime_class,
                client.V1RuntimeClass(metadata=client.V1ObjectMeta(name="loom-sandbox"), handler="runc"),
            )
            api = InClusterKubernetesJobApi(client_module=client_module, batch_api=batch, core_api=core)
            target_runtime = ExecutionTargetRuntime(
                target_id=target.target_id, namespace=namespace,
                runtime_class_name="loom-sandbox",
                credential_broker_url="http://127.0.0.1:1/internal/service-execution",
            )
            actuator = ExecutionActuator(
                sessions=sessions, kubernetes=api, target=target_runtime,
                controller_id=f"maintenance-{suffix}", command_lease_seconds=5,
            )
            assert await actuator.run_commands_once(now=now) == 1
            selector = f"loom.openai.com/lease-id={lease.id}"
            deadline = time.monotonic() + 90
            observation = None
            pod = None
            while time.monotonic() < deadline:
                observation = await api.get_job(namespace=namespace, job_name=lease.job_name)
                if observation is not None and observation.started_at is not None:
                    pods = await asyncio.to_thread(core.list_namespaced_pod, namespace, label_selector=selector)
                    assert len(pods.items) == 1
                    pod = pods.items[0]
                    response = await asyncio.to_thread(
                        _pod_probe, core, namespace, pod.metadata.name, "http://127.0.0.1:8080",
                    )
                    if "exit:0" in response:
                        break
                await asyncio.sleep(0.5)
            else:
                raise AssertionError(f"native agent phase did not start: {observation}")
            assert pod is not None and observation is not None and observation.job_uid is not None
            original_pod_uid = pod.metadata.uid
            original_job_uid = observation.job_uid
            record_property("job_uid", original_job_uid)
            record_property("pod_uid", original_pod_uid)
            record_property("agent_http_probe", response)
            await actuator.reconcile_full_once()
            async with sessions() as session:
                current = await session.get(ServiceExecutionLease, lease.id)
                trial = await session.get(Trial, trial_id)
                assert current is not None and trial is not None
                assert current.pod_uid == original_pod_uid and current.pod_started_at is not None
                assert trial.state == "running" and trial.attempt_count == 1
                await set_execution_target_health(
                    session, target_id=target.target_id, desired_state="draining",
                    observed_state="ready", health_status="healthy", observed_at=datetime.now(UTC),
                )
                await session.commit()
            async with sessions() as session:
                with pytest.raises(ServiceExecutionConflict, match="execution target is not ready"):
                    await _reserve(
                        session, trial_id=waiting_trial.id, target=target, now=datetime.now(UTC),
                        requirements=requirements, runtime_contract=contract,
                    )
                await session.rollback()

            # Maintenance blocks replacement placement before disrupting the
            # exact active incarnation; the Job renderer remains unmodified.
            await asyncio.to_thread(core.patch_node, pod.spec.node_name, {"spec": {"unschedulable": True}})
            await asyncio.to_thread(
                core.create_namespaced_pod_eviction, pod.metadata.name, namespace,
                client.V1Eviction(
                    metadata=client.V1ObjectMeta(name=pod.metadata.name, namespace=namespace),
                    delete_options=client.V1DeleteOptions(
                        preconditions=client.V1Preconditions(uid=original_pod_uid),
                    ),
                ),
            )
            deadline = time.monotonic() + 90
            while time.monotonic() < deadline:
                await actuator.reconcile_full_once()
                async with sessions() as session:
                    current = await session.get(ServiceExecutionLease, lease.id)
                    assert current is not None
                    if current.observed_state == "failed":
                        break
                await asyncio.sleep(0.5)
            else:
                observation = await api.get_job(namespace=namespace, job_name=lease.job_name)
                raise AssertionError(f"actual Pod eviction never reached durable native failure: {observation}")

            async with sessions() as session:
                failure = await session.scalar(select(ServiceExecutionEvent).where(
                    ServiceExecutionEvent.lease_id == lease.id,
                    ServiceExecutionEvent.event_kind == "kubernetes_observed",
                    ServiceExecutionEvent.payload_json["normalized_state"].astext.in_(
                        ("failed", "oom_killed", "evicted", "node_lost", "deadline_exceeded"),
                    ),
                ).order_by(ServiceExecutionEvent.ordinal))
                assert failure is not None
                first_failure_at = failure.observed_at
                record_property("native_failure_state", failure.payload_json["normalized_state"])
                record_property("native_failure_reason", failure.payload_json.get("reason"))
                record_property("first_failure_at", first_failure_at.isoformat())
                event_count = await session.scalar(select(func.count()).select_from(ServiceExecutionEvent).where(
                    ServiceExecutionEvent.lease_id == lease.id,
                ))
                # Replay the actual persisted API observation, never an invented
                # terminal fixture. Delivery time cannot change identity/deadline.
                for _ in range(2):
                    replay, duplicate = await record_kubernetes_observation(
                        session, lease_id=lease.id, generation=lease.generation,
                        payload=failure.payload_json,
                        observed_at=first_failure_at + timedelta(minutes=4, seconds=59),
                    )
                    assert duplicate and replay.id == failure.id
                    assert replay.observed_at == first_failure_at
                assert await session.scalar(select(func.count()).select_from(ServiceExecutionEvent).where(
                    ServiceExecutionEvent.lease_id == lease.id,
                )) == event_count
                await session.commit()
            # A new controller and duplicate terminal observations must retain
            # the first failure deadline, not grant another late-output window.
            actuator = ExecutionActuator(
                sessions=sessions, kubernetes=api, target=target_runtime,
                controller_id=f"maintenance-restarted-{suffix}", command_lease_seconds=5,
            )
            before_deadline = first_failure_at + timedelta(minutes=4, seconds=59)
            for _ in range(2):
                await actuator.reconcile_full_once(now=before_deadline)
            async with sessions() as session:
                current = await session.get(ServiceExecutionLease, lease.id)
                trial = await session.get(Trial, trial_id)
                target_row = await session.get(ServiceExecutionTarget, target.target_id)
                waiting = await session.get(Trial, waiting_trial.id)
                assert current is not None and trial is not None and target_row is not None and waiting is not None
                assert target_row.desired_state == "draining"
                assert waiting.state == "queued" and waiting.attempt_count == 0
                assert current.revoked_at is None and current.output_commit_state == "not_started"
                assert current.deleted_at is None and trial.state == "running"
            assert await api.get_job(namespace=namespace, job_name=lease.job_name) is not None
            observation = await api.get_job(namespace=namespace, job_name=lease.job_name)
            assert observation is not None and observation.job_uid == original_job_uid
            remaining = await asyncio.to_thread(core.list_namespaced_pod, namespace, label_selector=selector)
            for item in remaining.items:
                if item.metadata.uid != original_pod_uid:
                    assert not item.spec.node_name
                    assert not item.status.container_statuses

            after_deadline = first_failure_at + timedelta(minutes=5, seconds=1)
            await actuator.reconcile_full_once(now=after_deadline)
            await actuator.run_commands_once(now=after_deadline)
            deadline = time.monotonic() + 30
            while time.monotonic() < deadline:
                await actuator.reconcile_full_once(now=after_deadline)
                await actuator.run_commands_once(now=after_deadline + timedelta(seconds=10))
                remaining = await asyncio.to_thread(core.list_namespaced_pod, namespace, label_selector=selector)
                if await api.get_job(namespace=namespace, job_name=lease.job_name) is None and not remaining.items:
                    break
                await asyncio.sleep(0.5)
            else:
                raise AssertionError("native cleanup did not remove the exact Job and its Pods")
            await actuator.reconcile_full_once(now=after_deadline + timedelta(seconds=11))
            async with sessions() as session:
                current = await session.get(ServiceExecutionLease, lease.id)
                assert current is not None
                late_result = _runtime_result_payload(lease, started_at=now)
                with pytest.raises(ServiceExecutionFenceError, match="not authoritative"):
                    await record_execution_event(
                        session, lease_id=lease.id, generation=lease.generation,
                        ordinal=current.last_event_ordinal + 1, event_kind="result_reported",
                        payload=late_result, observed_at=after_deadline,
                    )
                await session.rollback()
            for _ in range(2):
                await actuator.reconcile_full_once(now=after_deadline + timedelta(seconds=12))
            async with sessions() as session:
                current = await session.get(ServiceExecutionLease, lease.id)
                trial = await session.get(Trial, trial_id)
                assert current is not None and trial is not None
                assert trial.state == "failed" and trial.attempt_count == 1
                assert trial.failure_reason == "native_execution_failed"
                assert current.output_commit_state == "unavailable" and current.cleanup_state == "complete"
                assert current.deleted_at is not None and current.revoked_at is not None
                assert current.job_uid == original_job_uid
                assert await session.scalar(select(func.count()).select_from(ServiceExecutionLease).where(
                    ServiceExecutionLease.trial_id == trial_id,
                )) == 1
                events = list((await session.scalars(select(ServiceExecutionEvent).where(
                    ServiceExecutionEvent.lease_id == lease.id,
                ))).all())
                assert len([event for event in events if event.event_kind == "failed"]) == 1
                assert not any(event.event_kind == "result_reported" for event in events)
                observations = [event for event in events if event.event_kind == "kubernetes_observed"]
                assert len({event.idempotency_key for event in observations}) == len(observations)
                assert min(event.observed_at for event in observations if event.payload_json.get(
                    "normalized_state",
                ) in {"failed", "oom_killed", "evicted", "node_lost", "deadline_exceeded"}) == first_failure_at
                reservation = await session.scalar(select(ExecutionProvisioningAuthorization).where(
                    ExecutionProvisioningAuthorization.lease_id == lease.id,
                ))
                assert reservation is not None and reservation.state == "released"
    finally:
        if container is not None:
            await asyncio.to_thread(container.stop)
        await engine.dispose()
        await asyncio.to_thread(
            subprocess.run, ["docker", "image", "rm", "--force", runtime_tag, fixture_tag],
            capture_output=True, check=False,
        )

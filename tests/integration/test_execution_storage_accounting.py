"""The native Pod request remains the storage envelope across admission states."""

import asyncio
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom.db.schema import ExecutionCapacityPolicy, ExecutionCostReservation, Trial
from loom.execution_runtime_contract import (
    ContainerResourcesV1,
    ExecutionRuntimePlanV1,
    ProbeV1,
    SidecarContainerV1,
)
from loom_control_plane.execution_capacity import ExecutionProvisioningBlockedError
from loom_execution_actuator.renderer import ExecutionTargetRuntime, render_execution_job
from loom_execution_capacity_collector.kubernetes import _pod_request
from tests.execution_placement_fixtures import placement_fixture
from tests.integration.test_execution_capacity_placement import _record
from tests.integration.test_service_execution_leases import (
    _cleanup_service_execution_test_rows,  # noqa: F401 -- shared database cleanup
    _requirements,
    _reserve,
    _runtime_contract,
    _seed_ready_trial,
)
from tests.support.execution_image_admission import signed_image_admission_bundle


def _plan(now):
    plan = _runtime_contract(now=now)
    resources = ContainerResourcesV1(
        cpu_millis=1000, memory_mib=2048, ephemeral_storage_mib=10240
    )
    probe = ProbeV1(kind="tcp", port=9000)
    agent_image = "registry.example/controller@sha256:" + "e" * 64
    return ExecutionRuntimePlanV1.model_validate({
        **plan.canonical_payload(),
        "agent_image_ref": agent_image,
        "image_admission": signed_image_admission_bundle(
            (plan.task_image_ref, plan.runtime_image_ref, agent_image), now=now,
        ),
        "task_resources": resources,
        "workspace_mib": 10240,
        "sidecars": tuple(SidecarContainerV1(
            role_name=role, image_ref=plan.task_image_ref, argv=("/sandbox",),
            private_sandbox=True, resources=resources,
            startup_probe=probe, readiness_probe=probe,
        ) for role in ("task-sandbox", "verifier-sandbox")),
    })


def _request(pod):
    def container(value):
        return SimpleNamespace(
            resources=SimpleNamespace(requests=value["resources"]["requests"]),
            restart_policy=value.get("restartPolicy"),
        )
    return _pod_request(SimpleNamespace(spec=SimpleNamespace(
        containers=[container(row) for row in pod["containers"]],
        init_containers=[container(row) for row in pod["initContainers"]], overhead={},
    )))


async def test_two_trials_share_one_node_without_losing_storage_limits(postgres_url):
    """Concurrent CP admission and observed Pods must charge the same 30 GiB."""
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime.now(UTC)
    plan = _plan(now)
    requirements = _requirements().model_copy(update={
        "cpu_millis": 1000, "memory_mib": 2048, "ephemeral_storage_mib": 10240,
    })
    try:
        async with sessions() as session, session.begin():
            pairs = [await _seed_ready_trial(session, now=now) for _ in range(3)]
            target = pairs[0][1]
            for trial_id, _ in pairs:
                trial = await session.get(Trial, trial_id)
                trial.config = {"agent": {"name": "terminus-2"}}
            policy = await session.get(ExecutionCapacityPolicy, target.target_id)
            policy.max_nodes = 1
            placement = placement_fixture(
                target_id=target.target_id, node_cpu=16000, node_memory=65536,
                node_storage=69432, raw_storage=81920, quota_nodes=1,
            )
            placement["node_group"]["max_nodes"] = 1
            await _record(session, target.target_id, now + timedelta(seconds=1), placement)

        async def claim(trial_id):
            async with sessions() as session, session.begin():
                return await _reserve(
                    session, trial_id=trial_id, target=target,
                    now=now + timedelta(seconds=4), runtime_contract=plan,
                    requirements=requirements,
                )

        leases = await asyncio.gather(*(claim(pair[0]) for pair in pairs[:2]))
        requests = []
        for lease in leases:
            pod = render_execution_job(
                lease, target=ExecutionTargetRuntime(
                    target_id=target.target_id, namespace=lease.namespace_name,
                ), now=now + timedelta(seconds=3),
            )["spec"]["template"]["spec"]
            request = _request(pod)
            assert request.storage_mib == 30720
            assert (request.cpu_millis, request.memory_mib) == (3000, 6144)
            requests.append(request)
            # Both real sandbox disk limits and all volume caps remain intact.
            for container in [*pod["containers"], *pod["initContainers"][1:]]:
                assert container["resources"]["requests"]["ephemeral-storage"] == "10240Mi"
                assert container["resources"]["limits"]["ephemeral-storage"] == "10240Mi"
            volumes = {row["name"]: row["emptyDir"]["sizeLimit"] for row in pod["volumes"]}
            assert volumes["workspace"] == "10240Mi"
            assert volumes["runtime"] == f"{plan.runtime_volume_mib}Mi"
            assert volumes["task-sandbox-socket"] == volumes["verifier-sandbox-socket"] == "1Mi"
            # 1024 MiB artifacts + four 10 MiB phase streams + 2 MiB for
            # atomic result replacement + a 4 KiB termination summary.
            assert volumes["output"] == "1067Mi"
            async with sessions() as session:
                cost = await session.scalar(select(ExecutionCostReservation).where(
                    ExecutionCostReservation.lease_id == lease.id,
                ))
                assert cost.requested_ephemeral_storage_mib == request.storage_mib

        # Two full reservations fill the node; a third is refused before and
        # after the collector observes the exact same execution Pods.
        for observed in (False, True):
            if observed:
                node = placement["nodes"][0]
                node["requested"] = {
                    key: sum(getattr(row, key) for row in requests)
                    for key in ("cpu_millis", "memory_mib", "storage_mib")
                }
                node["used_pod_slots"] = 2
                node["managed_pods"] = [{
                    "uid": str(lease.id), "lease_id": str(lease.id),
                    "generation": lease.resource_generation,
                    "requests": request.model_dump(),
                } for lease, request in zip(leases, requests, strict=True)]
                async with sessions() as session, session.begin():
                    await _record(session, target.target_id, now + timedelta(seconds=3), placement)
            with pytest.raises(ExecutionProvisioningBlockedError, match="execution_capacity_max_nodes_exceeded"):
                await claim(pairs[2][0])
            async with sessions() as session:
                refused = await session.get(Trial, pairs[2][0])
                assert (refused.state, refused.attempt_count) == ("queued", 0)
    finally:
        await engine.dispose()

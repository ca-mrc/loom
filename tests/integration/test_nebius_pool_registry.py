"""Global execution prepare serializes real management transactions before Pods."""
from __future__ import annotations

import asyncio
import hashlib
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from sqlalchemy import func, insert, select, text, update

from loom.db.nebius_pool_schema import (
    NebiusPoolBinding,
    NebiusPoolMachine,
    NebiusPoolMachineCredential,
    NebiusPoolParticipant,
    NebiusPoolRequest,
)
from loom.db.schema import Token
from loom.execution_contract import nebius_cpu_execution_class
from loom.nebius_pool_workload import PoolExecutionPrepareV1
from loom.pipeline.keys import canonical_digest
from loom_execution_actuator.renderer import ExecutionTargetRuntime
from loom_execution_capacity_collector.contracts import (
    CapacityPlacement,
    ManagedPodPlacement,
    ResourceTotals,
)
from loom_service.pool_management.auth import resolve_pool_machine
from loom_service.pool_management.render import PoolExecutionProfile
from tests.execution_placement_fixtures import placement_fixture
from tests.integration.test_nebius_pool_observation_registry import (
    capture_scope,
    publish,
    snapshots,
)
from tests.integration.test_nebius_pool_observation_registry import (
    sessions as sessions,  # shared real PostgreSQL fixture, explicit re-export
)
from tests.support.execution_image_admission import IMAGE_ADMISSION_KEYRING
from tests.unit.test_nebius_pool_execution_render import inputs


async def machine(sessions, pool_id, participant_id=None, *, role=None, workload_scope="environment"):
    raw = "loom_pool_" + uuid4().hex + uuid4().hex
    token_hash = hashlib.sha256(raw.encode()).digest()
    machine_id = uuid4()
    async with sessions.begin() as session:
        await session.execute(insert(NebiusPoolMachine).values(
            machine_id=machine_id, pool_id=pool_id, participant_id=participant_id,
            role=role or ("participant" if participant_id else "observer"), workload_scope=workload_scope,
            credential_epoch=1, phase="active"))
        await session.execute(insert(Token).values(token_hash=token_hash, type="pool_machine", scopes=[],
            issued_at=datetime.now(UTC), expires_at=datetime.now(UTC) + timedelta(hours=1)))
        await session.execute(insert(NebiusPoolMachineCredential).values(
            token_hash=token_hash, machine_id=machine_id, credential_epoch=1))
    async with sessions() as session:
        return await resolve_pool_machine(session, "Bearer " + raw)


async def setup(sessions, *, occupied_cpu=0, max_nodes=1, group_id="pool-test",
                parent_id=None, quota_nodes=None, environment_classes=("development", "development"),
                pinned=True, memory_quota=True, data_environment_id=None, cluster_id="cluster-1", namespace_uids=None,
                installation_id=None,
                workload_kinds=("trial", "verifier", "task_image_build")):
    placement = CapacityPlacement.model_validate(placement_fixture(
        target_id=group_id, parent_id=parent_id, node_cpu=3000, node_memory=8192, node_storage=32768,
        requested_cpu=occupied_cpu, quota_nodes=quota_nodes or max_nodes, used_nodes=1,
    ))
    placement = placement.model_copy(update={"node_group": placement.node_group.model_copy(update={"max_nodes": max_nodes})})
    if not memory_quota:
        placement = placement.model_copy(update={"quota_resources": {
            key: quota for key, quota in placement.quota_resources.items() if key != "memory"}})
    first, body = inputs()
    if installation_id is not None:
        first = first.model_copy(update={"installation_id": installation_id})
    policy = {"observation_max_age_seconds": 60, "max_create_per_minute": 10,
              "max_pending_jobs": 10, "max_unschedulable_jobs": 0,
              "max_image_pull_backoff_jobs": 0, "build_concurrency_limit": 2}
    selector = {"loom.nebius/role": "execution", "loom.nebius/node-os": "linux", "loom.nebius/node-arch": "amd64"}
    if pinned:
        selector["nebius.com/node-group-id"] = group_id
    binding = {"node_selector": selector, "admission": policy,
               "quota_identities": {name: [quota.parent_id, quota.region, quota.service, quota.name, quota.unit]
                                    for name, quota in placement.quota_resources.items()}}
    async with sessions.begin() as session:
        await session.execute(insert(NebiusPoolBinding).values(
            pool_id=first.pool_id, installation_id=first.installation_id, cluster_id=cluster_id,
            node_group_id=group_id, policy_revision=1, admission_epoch=2, mode="global",
            binding_json=binding, binding_sha256=canonical_digest(binding).removeprefix("sha256:")))
    participants = []
    principals = []
    profiles = {}
    for index in range(2):
        participant = first.model_copy(update={
            "participant_id": first.participant_id if index == 0 else uuid4(),
            "environment_id": (data_environment_id or first.environment_id) if index == 0 else uuid4(),
            "incarnation": first.incarnation if index == 0 else uuid4(),
            "environment_class": environment_classes[index],
            "execution_namespace": first.execution_namespace.model_copy(update={"name": f"{group_id}-execution-{index}",
                "uid": (namespace_uids or {}).get(f"{group_id}-execution-{index}", uuid4())}),
            "build_namespace": first.build_namespace.model_copy(update={"name": f"{group_id}-build-{index}",
                "uid": (namespace_uids or {}).get(f"{group_id}-build-{index}", uuid4())}),
            "targets": (first.targets[0].model_copy(update={"profile_id": uuid4(),
                "workload_kinds": workload_kinds}),),
        })
        async with sessions.begin() as session:
            await session.execute(insert(NebiusPoolParticipant).values(
                participant_id=participant.participant_id, pool_id=first.pool_id,
                environment_id=participant.environment_id, incarnation=participant.incarnation,
                binding_revision=1, admission_epoch=2, phase="active", binding_json=participant.model_dump(mode="json"),
                binding_sha256=canonical_digest(participant).removeprefix("sha256:")))
        participants.append(participant)
        principals.append(await machine(sessions, first.pool_id, participant.participant_id))
        profiles[participant.targets[0].profile_id] = PoolExecutionProfile(
            profile_id=participant.targets[0].profile_id,
            runtime=ExecutionTargetRuntime(target_id="native", namespace=participant.execution_namespace.name,
                                           node_selector=binding["node_selector"]),
            candidate_sha="1" * 40, execution_class_id="linux-amd64-cpu-pod-v1",
            runtime_image_ref="registry.example/runtime@sha256:" + "b" * 64,
            runtime_binary_sha256="sha256:" + "c" * 64,
            execution_class=nebius_cpu_execution_class(), image_admission_keyring=IMAGE_ADMISSION_KEYRING,
        )
    observer = await machine(sessions, first.pool_id)
    await publish_placement(sessions, observer, placement)
    requests = []
    for participant in participants:
        # Independent environment DBs may reuse their local work UUID.
        requests.append(PoolExecutionPrepareV1.model_validate(body | {
            "key": body["key"] | {"participant_id": participant.participant_id},
            "origin": body["origin"] | {"data_environment_id": participant.environment_id},
        }))
    return participants, principals, requests, profiles, observer


async def publish_placement(sessions, observer, placement):
    capture = await capture_scope(sessions, observer)
    provider, kubernetes = snapshots(capture)
    totals = {"nodes": "nodes", "vcpu": "vcpu_millis", "memory": "memory_mib", "storage": "storage_mib"}
    provider = provider.model_copy(update={"node_group": placement.node_group,
        "node_count": placement.node_group.node_count, "target_node_count": placement.node_group.node_count,
        "ready_node_count": sum(node.ready for node in placement.nodes), "quota_resources": placement.quota_resources,
        **{f"{prefix}_{totals[name]}": getattr(quota, attr) for name, quota in placement.quota_resources.items()
           for prefix, attr in (("quota", "limit"), ("used", "used"))}})
    def total(field):
        return ResourceTotals(**{key: sum(getattr(getattr(node, field), key) for node in placement.nodes)
                                 for key in ("cpu_millis", "memory_mib", "storage_mib")})
    kubernetes = kubernetes.model_copy(update={"nodes": placement.nodes, "active_nodes": len(placement.nodes),
        "ready_nodes": sum(node.ready for node in placement.nodes), "allocatable": total("allocatable"),
        "requested": total("requested"), "provisioned": total("allocatable"),
        "pending_jobs": len(placement.pending_pods), "pending_pods": placement.pending_pods,
        "template_samples": placement.template_samples, "daemonsets": placement.daemonsets})
    await publish(sessions, observer, capture, provider=provider, kubernetes=kubernetes)


async def prepare(sessions, principal, request, profiles):
    from loom_service.pool_management.registry import PoolProfiles, prepare_execution

    async with sessions.begin() as session:
        return await prepare_execution(session, principal, request,
            profiles=profiles if isinstance(profiles, PoolProfiles) else PoolProfiles(execution=profiles))


async def test_concurrent_environment_sessions_cannot_overbook_before_pods_exist(sessions):
    _, principals, requests, profiles, _ = await setup(sessions)
    third = requests[0].model_copy(update={"key": requests[0].key.model_copy(update={"local_work_id": uuid4()})})
    results = await asyncio.wait_for(asyncio.gather(
        prepare(sessions, principals[0], requests[0], profiles),
        prepare(sessions, principals[1], requests[1], profiles),
        prepare(sessions, principals[0], third, profiles),
    ), timeout=15)
    assert sorted(result.phase for result in results) == ["reserved", "reserved", "waiting"]
    assert len({result.reservation_id for result in results if result.phase == "reserved"}) == 2
    async with sessions() as session:
        rows = list((await session.scalars(select(NebiusPoolRequest))).all())
        assert len(rows) == 3 and sum(row.cpu_millis for row in rows if row.phase == "reserved") == 3000
        assert all(row.job_uid is None and row.plan_json is None for row in rows)
        assert all((row.granted_at is not None) == (row.phase == "reserved") for row in rows)


async def test_same_body_replay_returns_one_grant_and_changed_body_conflicts(sessions):
    from loom_service.pool_management.registry import PoolAdmissionError

    _, principals, requests, profiles, _ = await setup(sessions)
    first = await prepare(sessions, principals[0], requests[0], profiles)
    replay = await prepare(sessions, principals[0], requests[0], profiles)
    assert first.reservation_id == replay.reservation_id
    assert first.request_sha256 == replay.request_sha256
    changed = requests[0].model_copy(update={"deadline_at": requests[0].deadline_at + timedelta(seconds=1)})
    with pytest.raises(PoolAdmissionError, match="conflict"):
        await prepare(sessions, principals[0], changed, profiles)
    async with sessions() as session:
        assert await session.scalar(select(func.count()).select_from(NebiusPoolRequest)) == 1


@pytest.mark.parametrize("damage", ["closed", "fenced", "deadline", "profile", "foreign-owner", "missing-observation",
                                    "wrong-pool-profile", "unpinned-pool-profile"])
async def test_prepare_has_no_grant_on_unqualified_authority_or_work(sessions, damage):
    from loom_service.pool_management.registry import PoolAdmissionError

    participants, principals, requests, profiles, _ = await setup(sessions)
    request = requests[0]
    async with sessions.begin() as session:
        if damage == "closed":
            await session.execute(update(NebiusPoolBinding).where(NebiusPoolBinding.pool_id == participants[0].pool_id).values(mode="closed"))
        elif damage == "fenced":
            await session.execute(update(NebiusPoolParticipant).where(
                NebiusPoolParticipant.participant_id == participants[0].participant_id).values(phase="fenced"))
        elif damage == "missing-observation":
            # New binding identity invalidates the old capture, not its stored history.
            await session.execute(update(NebiusPoolBinding).where(
                NebiusPoolBinding.pool_id == participants[0].pool_id).values(policy_revision=2))
    if damage == "deadline":
        request = request.model_copy(update={"deadline_at": datetime.now(UTC) - timedelta(seconds=1)})
    elif damage == "profile":
        profiles = {}
    elif damage == "foreign-owner":
        request = requests[1]
    elif damage in {"wrong-pool-profile", "unpinned-pool-profile"}:
        profile_id = participants[0].targets[0].profile_id
        profile = profiles[profile_id]
        profiles[profile_id] = replace(profile, runtime=replace(profile.runtime,
            node_selector=None if damage == "unpinned-pool-profile" else {"loom.nebius/role": "unrelated"}))
    with pytest.raises(PoolAdmissionError):
        await prepare(sessions, principals[0], request, profiles)
    async with sessions() as session:
        assert await session.scalar(select(func.count()).select_from(NebiusPoolRequest).where(NebiusPoolRequest.phase == "reserved")) == 0


async def test_waiting_renews_without_reordering_or_fabricating_a_grant(sessions):
    _, principals, requests, profiles, _ = await setup(sessions, occupied_cpu=3000)
    waiting = await prepare(sessions, principals[0], requests[0], profiles)
    assert waiting.phase == "waiting" and not hasattr(waiting, "reservation_id")
    async with sessions() as session:
        original = (await session.scalars(select(NebiusPoolRequest))).one()
        created_at, renewed_at = original.created_at, original.renewed_at
    await prepare(sessions, principals[0], requests[0], profiles)
    async with sessions() as session:
        current = (await session.scalars(select(NebiusPoolRequest))).one()
        assert current.created_at == created_at and current.renewed_at > renewed_at
        assert current.phase == "waiting" and current.granted_at is None and current.priority == 2


@pytest.mark.parametrize("memory_quotas", [(True, True), (False, False), (True, False)])
async def test_distinct_physical_pools_cannot_double_spend_shared_provider_quota(sessions, memory_quotas):
    first = await setup(sessions, occupied_cpu=3000, max_nodes=2, quota_nodes=3, parent_id="shared", memory_quota=memory_quotas[0])
    second = await setup(sessions, occupied_cpu=3000, max_nodes=2, quota_nodes=3,
                         parent_id="shared", group_id="other-pool", memory_quota=memory_quotas[1])
    profiles = first[3] | second[3]
    results = await asyncio.wait_for(asyncio.gather(
        prepare(sessions, first[1][0], first[2][0], profiles),
        prepare(sessions, second[1][0], second[2][0], profiles),
    ), timeout=15)
    # Each pool has one full native node. Only one additional node fits the
    # shared quota of three, even though each provider snapshot reported one.
    assert sorted(result.phase for result in results) == ["reserved", "waiting"]


@pytest.mark.parametrize("classes,winner", [(("development", "production"), 1),
                                           (("development", "staging"), 1),
                                           (("development", "development"), 0)])
async def test_fitting_waits_keep_class_then_age_priority(sessions, classes, winner):
    _, principals, requests, profiles, observer = await setup(sessions, occupied_cpu=3000,
                                                            environment_classes=classes)
    for index in range(2):
        assert (await prepare(sessions, principals[index], requests[index], profiles)).phase == "waiting"
    placement = CapacityPlacement.model_validate(placement_fixture(
        target_id="pool-test", node_cpu=3000, node_memory=8192, node_storage=32768,
        requested_cpu=1500, quota_nodes=1, used_nodes=1))
    await publish_placement(sessions, observer, placement)
    loser = 1 - winner
    assert (await prepare(sessions, principals[loser], requests[loser], profiles)).phase == "waiting"
    assert (await prepare(sessions, principals[winner], requests[winner], profiles)).phase == "reserved"


async def test_replay_does_not_rerender_an_existing_grant_or_renew_its_lifetime(sessions):
    _, principals, requests, profiles, _ = await setup(sessions)
    first = await prepare(sessions, principals[0], requests[0], profiles)
    async with sessions() as session:
        before = await session.get(NebiusPoolRequest, first.reservation_id)
    replay = await prepare(sessions, principals[0], requests[0], {})
    async with sessions() as session:
        after = await session.get(NebiusPoolRequest, first.reservation_id)
    assert first == replay
    assert (before.created_at, before.renewed_at, before.granted_at, before.deadline_at) == (
        after.created_at, after.renewed_at, after.granted_at, after.deadline_at)


async def test_a_generic_execution_label_is_not_a_physical_node_group_binding(sessions):
    from loom_service.pool_management.registry import PoolAdmissionError

    _, principals, requests, profiles, _ = await setup(sessions, pinned=False)
    with pytest.raises(PoolAdmissionError):
        await prepare(sessions, principals[0], requests[0], profiles)


async def test_admission_rejects_a_transaction_that_cannot_see_committed_peer_grants(sessions):
    from loom_service.pool_management.registry import (
        PoolAdmissionError,
        PoolProfiles,
        prepare_execution,
    )

    _, principals, requests, profiles, _ = await setup(sessions)
    async with sessions.begin() as session:
        await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"))
        with pytest.raises(PoolAdmissionError):
            await prepare_execution(session, principals[0], requests[0], profiles=PoolProfiles(execution=profiles))


async def test_prepare_never_commits_the_callers_transaction(sessions):
    from loom_service.pool_management.registry import PoolProfiles, prepare_execution

    _, principals, requests, profiles, _ = await setup(sessions)
    async with sessions() as session:
        assert (await prepare_execution(session, principals[0], requests[0], profiles=PoolProfiles(execution=profiles))).phase == "reserved"
        await session.rollback()
    async with sessions() as session:
        assert await session.scalar(select(func.count()).select_from(NebiusPoolRequest)) == 0


async def test_observed_exact_receipt_is_counted_once_but_later_grants_still_charge(sessions):
    from loom_service.pool_management.render import prepare_pool_execution

    participants, principals, requests, profiles, observer = await setup(sessions)
    first = await prepare(sessions, principals[0], requests[0], profiles)
    prepared = prepare_pool_execution(requests[0], participant=participants[0],
        profile=profiles[participants[0].targets[0].profile_id], reservation_id=first.reservation_id, now=datetime.now(UTC))
    plan = {"job": prepared.job}
    async with sessions.begin() as session:
        await session.execute(update(NebiusPoolRequest).where(NebiusPoolRequest.request_id == first.reservation_id).values(
            phase="create_intent", plan_json=plan, plan_sha256=canonical_digest(plan).removeprefix("sha256:")))
        await session.execute(update(NebiusPoolRequest).where(NebiusPoolRequest.request_id == first.reservation_id).values(
            phase="observed", job_uid=uuid4()))
    payload = placement_fixture(target_id="pool-test", node_cpu=3000, node_memory=8192, node_storage=32768,
                                requested_cpu=1500, requested_memory=2048, requested_storage=4096, quota_nodes=1)
    payload["nodes"][0]["used_pod_slots"] = 1
    payload["nodes"][0]["managed_pods"] = [{"uid": "pod-1", "lease_id": f"reservation:{first.reservation_id}",
        "generation": 1, "requests": {"cpu_millis": 1500, "memory_mib": 2048, "storage_mib": 4096}}]
    await publish_placement(sessions, observer, CapacityPlacement.model_validate(payload))
    assert (await prepare(sessions, principals[1], requests[1], profiles)).phase == "reserved"
    third = requests[0].model_copy(update={"key": requests[0].key.model_copy(update={"local_work_id": uuid4()})})
    assert (await prepare(sessions, principals[0], third, profiles)).phase == "waiting"
    # No new scope after the second grant. Cleanup intent on the first remains
    # charged as observed occupancy, never as automatically released capacity.
    async with sessions.begin() as session:
        await session.execute(update(NebiusPoolRequest).where(NebiusPoolRequest.request_id == first.reservation_id).values(phase="cleanup_intent"))
    assert (await prepare(sessions, principals[0], third, profiles)).phase == "waiting"


async def test_foreign_pending_pod_cannot_be_discarded_to_make_a_grant_fit(sessions):
    _, principals, requests, profiles, observer = await setup(sessions)
    payload = placement_fixture(target_id="pool-test", node_cpu=3000, node_memory=8192,
                                node_storage=32768, quota_nodes=1, requested_cpu=1500)
    placement = CapacityPlacement.model_validate(payload).model_copy(update={"pending_pods": [ManagedPodPlacement(
        uid="foreign-1", lease_id="foreign-pod:foreign-1", generation=1,
        requests=ResourceTotals(cpu_millis=1500, memory_mib=2048, storage_mib=4096))]})
    await publish_placement(sessions, observer, placement)
    assert (await prepare(sessions, principals[0], requests[0], profiles)).phase == "waiting"


async def test_impossible_earlier_wait_does_not_starve_a_fitting_request(sessions):
    _, principals, requests, profiles, _ = await setup(sessions)
    larger = requests[0].model_copy(update={"execution": requests[0].execution.model_copy(update={
        "requirements": requests[0].execution.requirements.model_copy(update={"cpu_millis": 4000}),
        "runtime": requests[0].execution.runtime.model_copy(update={
            "task_resources": requests[0].execution.runtime.task_resources.model_copy(update={"cpu_millis": 4000})}),
    })})
    assert (await prepare(sessions, principals[0], larger, profiles)).phase == "waiting"
    assert (await prepare(sessions, principals[1], requests[1], profiles)).phase == "reserved"


async def seed_historical_request(sessions, participant, request, *, phase, age, expired=False):
    """Retained clock states without sleeping or weakening immutable SQL guards."""
    request_id = uuid4()
    now = datetime.now(UTC)
    timestamp = now - timedelta(seconds=age)
    if expired:
        request = request.model_copy(update={"deadline_at": now - timedelta(seconds=30)})
    payload = request.model_dump(mode="json")
    async with sessions.begin() as session:
        await session.execute(insert(NebiusPoolRequest).values(
            request_id=request_id, pool_id=participant.pool_id, participant_id=participant.participant_id,
            namespace_uid=participant.execution_namespace.uid, workload_kind=request.key.workload_kind,
            local_work_id=request.key.local_work_id, generation=request.key.generation,
            admission_epoch=request.admission_epoch, target_id=request.target_id,
            request_sha256=canonical_digest(payload).removeprefix("sha256:"), request_json=payload,
            deadline_at=request.deadline_at, cpu_millis=1500, memory_mib=2048, ephemeral_storage_mib=4096,
            pod_slots=1, priority=2, created_at=timestamp, renewed_at=timestamp,
            phase="waiting" if phase == "waiting" else "reserved", granted_at=None if phase == "waiting" else timestamp))
        if phase in {"create_intent", "cleanup_intent"}:
            plan = {"job": {"metadata": {"name": f"loom-pool-{request_id.hex}", "namespace": participant.execution_namespace.name}}}
            await session.execute(update(NebiusPoolRequest).where(NebiusPoolRequest.request_id == request_id).values(
                phase="create_intent", plan_json=plan, plan_sha256=canonical_digest(plan).removeprefix("sha256:")))
            if phase == "cleanup_intent":
                await session.execute(update(NebiusPoolRequest).where(NebiusPoolRequest.request_id == request_id).values(phase=phase))
    return request_id


@pytest.mark.parametrize("phase", ["reserved", "create_intent", "cleanup_intent"])
async def test_expired_or_fenced_caller_does_not_free_an_unobserved_grant(sessions, phase):
    participants, principals, requests, profiles, observer = await setup(sessions, occupied_cpu=1500)
    retained = await seed_historical_request(sessions, participants[0], requests[0], phase=phase, age=180, expired=True)
    async with sessions.begin() as session:
        await session.execute(update(NebiusPoolParticipant).where(
            NebiusPoolParticipant.participant_id == participants[0].participant_id).values(phase="fenced"))
    # Requalify the changed registration while retaining the old reservation.
    await publish_placement(sessions, observer, CapacityPlacement.model_validate(placement_fixture(
        target_id="pool-test", node_cpu=3000, node_memory=8192, node_storage=32768,
        requested_cpu=1500, quota_nodes=1)))
    assert (await prepare(sessions, principals[1], requests[1], profiles)).phase == "waiting"
    async with sessions() as session:
        assert (await session.get(NebiusPoolRequest, retained)).phase == phase


@pytest.mark.parametrize("age,expected", [(90, "waiting"), (180, "reserved")])
async def test_only_renewed_fitting_waits_protect_capacity(sessions, age, expected):
    participants, principals, requests, profiles, _ = await setup(sessions, occupied_cpu=1500)
    await seed_historical_request(sessions, participants[0], requests[0], phase="waiting", age=age)
    assert (await prepare(sessions, principals[1], requests[1], profiles)).phase == expected


async def test_long_scale_zero_retains_the_last_compatible_measured_template(sessions):
    _, principals, requests, profiles, observer = await setup(sessions)
    empty = placement_fixture(target_id="pool-test", node_cpu=3000, node_memory=8192,
                               node_storage=32768, nodes=0, used_nodes=0, quota_nodes=1)
    empty["template_samples"] = []
    placement = CapacityPlacement.model_validate(empty)
    for _ in range(105):
        await publish_placement(sessions, observer, placement)
    assert (await prepare(sessions, principals[0], requests[0], profiles)).phase == "reserved"

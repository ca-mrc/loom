"""Node-share compilation uses qualified global capacity, never local totals."""
from __future__ import annotations

from datetime import UTC, datetime, timedelta
from uuid import uuid4

import httpx
import pytest
from sqlalchemy import func, select, update

from loom.db.nebius_pool_outbox_schema import NebiusPoolExecutionOutbox
from loom.db.schema import Batch, ExecutionCapacityPolicy, ServiceExecutionLease, Task, Trial
from loom_execution_actuator.pool_client import PoolRequestUnconfirmedError
from tests.integration.test_nebius_pool_execution_activation import selected
from tests.integration.test_nebius_pool_execution_controller import another_trial
from tests.integration.test_nebius_pool_observation_registry import sessions as sessions
from tests.integration.test_nebius_pool_participant_http import client
from tests.integration.test_nebius_pool_registry import machine, publish_placement


def scope(outbox, target_id):
    from loom.nebius_pool_allocation import PoolNodeAllocationRequestV1

    participant = outbox.participant
    return PoolNodeAllocationRequestV1(pool_id=participant.pool_id, participant_id=participant.participant_id,
        admission_epoch=participant.admission_epoch, participant_revision=participant.binding_revision,
        target_id=target_id)


async def configure_node_share(sessions, trial_id):
    from loom.service_execution_materialization import build_nebius_runtime_profile
    from tests.integration.test_service_execution_leases import _runtime_contract

    async with sessions.begin() as session:
        trial = await session.get(Trial, trial_id)
        original = _runtime_contract(now=datetime.now(UTC))
        profile = build_nebius_runtime_profile(candidate_sha=original.candidate_sha,
            task_image_ref=original.task_image_ref, runtime_image_ref=original.runtime_image_ref,
            runtime_binary_sha256=original.runtime_binary_sha256, image_admission=original.image_admission)
        profile = profile.model_copy(update={"resource_allocation_policy": "node-share-v1"})
        batch = await session.get(Batch, trial.batch_id)
        batch.service_execution_runtime_profile = profile.model_dump(mode="json")
        trial.config = {"schema_version": "1", "agent_name": "direct-completion",
            "agent_model": {"provider": "openai", "name": "test", "source": "api"}}
        task = await session.get(Task, trial.task_id)
        config = {key: value for key, value in task.config.items() if key != "service_execution"}
        config["environment"] = {key: value for key, value in config["environment"].items() if key != "tmpfs"}
        config.update(agent={"name": "direct-completion"},
            verifier={"name": "script", "args": {"script_path": "verifier/check.sh"}},
            steps=[{"name": "main", "instruction_file": "instruction.md", "artifacts": ["answer.txt"]}])
        task.config = config
        task.source_provenance = {"service_execution_input": {
            "schema_version": "loom.service-execution-input.v1", "manifest_uri": "s3://artifacts/task-inputs/task.json",
            "manifest_sha256": "sha256:" + "d" * 64, "file_count": 3, "total_bytes": 4096}}


@pytest.mark.parametrize("cold", [False, True])
async def test_global_allocation_uses_measured_node_minus_daemonsets_not_free_resources(sessions, tmp_path, cold):
    from loom_execution_capacity_collector.contracts import CapacityPlacement
    from tests.execution_placement_fixtures import placement_fixture

    outbox, _, proposal, app, token = await selected(sessions, tmp_path)
    raw = placement_fixture(target_id="group-1", node_cpu=3000, node_memory=8192, node_storage=32768,
        requested_cpu=2000, requested_memory=6000, quota_nodes=1, used_nodes=1)
    raw["template_samples"][0]["daemonset_requests"] = {"cpu_millis": 250, "memory_mib": 512, "storage_mib": 1024}
    observer = await machine(sessions, proposal.request.pool_id)
    await publish_placement(sessions, observer, CapacityPlacement.model_validate(raw))
    if cold:
        raw["node_group"]["node_count"] = 0
        raw.update(nodes=[], template_samples=[])
        await publish_placement(sessions, observer, CapacityPlacement.model_validate(raw))
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app)) as http:
        allocation = await client(http, token).node_allocation(scope(outbox, proposal.request.target_id))
    assert allocation.usable_node.model_dump() == {
        "cpu_millis": 2750, "memory_mib": 7680, "ephemeral_storage_mib": 31744}
    assert allocation.observed_at <= datetime.now(UTC) < allocation.valid_until
    assert allocation.observation_id.int and allocation.scope == scope(outbox, proposal.request.target_id)


@pytest.mark.parametrize("damage", ["participant", "epoch", "revision", "target"])
async def test_global_allocation_requires_exact_registered_participant_scope(sessions, tmp_path, damage):
    outbox, _, proposal, app, token = await selected(sessions, tmp_path)
    request = scope(outbox, proposal.request.target_id)
    field, value = {"participant": ("participant_id", uuid4()), "epoch": ("admission_epoch", 99),
        "revision": ("participant_revision", 99), "target": ("target_id", "foreign")}[damage]
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app)) as http:
        with pytest.raises(PoolRequestUnconfirmedError):
            await client(http, token).node_allocation(request.model_copy(update={field: value}))


async def test_node_share_selection_freezes_global_evidence_with_local_allocator_disabled(sessions, tmp_path):
    from loom_execution_actuator.pool_execution_selection import PoolExecutionSelector

    outbox, original, proposal, app, token = await selected(sessions, tmp_path)
    following = await another_trial(sessions, original)
    await configure_node_share(sessions, following)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app)) as http:
        selected_work = await PoolExecutionSelector(outbox=outbox, allocation_reader=client(http, token)).select_next()
    assert selected_work is not None and selected_work.lease_id is None
    allocation = selected_work.request.execution.node_allocation
    assert allocation.scope == scope(outbox, proposal.request.target_id)
    assert allocation.usable_node.memory_mib == 8192  # The legacy local sample says 262144 MiB.
    runtime = selected_work.request.execution.runtime
    assert runtime.node_resource_allocation.usable_node == allocation.usable_node
    async with sessions() as session:
        assert (await session.get(NebiusPoolExecutionOutbox, selected_work.request.key.local_work_id)).trial_id == following
        assert await session.scalar(select(func.count()).select_from(ServiceExecutionLease)) == 0
        assert (await session.get(Trial, following)).attempt_count == 0


async def test_missing_global_evidence_never_falls_back_to_enabled_local_allocation(sessions, tmp_path):
    from loom_execution_actuator.pool_execution_selection import PoolExecutionSelector

    outbox, original, _, _, _ = await selected(sessions, tmp_path)
    following = await another_trial(sessions, original)
    await configure_node_share(sessions, following)
    async with sessions.begin() as session:
        await session.execute(update(ExecutionCapacityPolicy).values(enabled=True))
    assert await PoolExecutionSelector(outbox=outbox).select_next() is None
    async with sessions() as session:
        assert await session.scalar(select(func.count()).select_from(NebiusPoolExecutionOutbox).where(
            NebiusPoolExecutionOutbox.trial_id == following)) == 0


@pytest.mark.parametrize("damage", ["expired", "future", "target"])
async def test_global_evidence_is_requalified_at_local_proposal(sessions, tmp_path, damage):
    from loom_execution_actuator.pool_outbox import PoolHandoffError

    outbox, original, proposal, app, token = await selected(sessions, tmp_path)
    following = await another_trial(sessions, original)
    await configure_node_share(sessions, following)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app)) as http:
        allocation = await client(http, token).node_allocation(scope(outbox, proposal.request.target_id))
    if damage == "target":
        allocation = allocation.model_copy(update={"scope": allocation.scope.model_copy(update={"target_id": "foreign"})})
    else:
        offset = timedelta(hours=-1 if damage == "expired" else 1)
        allocation = allocation.model_copy(update={"observed_at": allocation.observed_at + offset,
            "valid_until": allocation.valid_until + offset})
    with pytest.raises(PoolHandoffError):
        await outbox.propose(trial_id=following, target_id=proposal.request.target_id, node_allocation=allocation)

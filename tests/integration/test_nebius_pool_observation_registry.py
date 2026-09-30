"""Real management SQL binds physical observations to server-issued capture scopes."""
from __future__ import annotations

from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from sqlalchemy import delete, func, insert, select, update
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom.db.nebius_pool_schema import NebiusPoolBinding, NebiusPoolParticipant, NebiusPoolRequest
from loom.pipeline.keys import canonical_digest
from loom_execution_capacity_collector.contracts import (
    KubernetesCapacitySnapshot,
    NodeGroupPlacement,
    ProviderCapacitySnapshot,
    ResourceTotals,
)
from tests.integration.test_nebius_pool_auth import credential
from tests.unit.test_nebius_pool_execution_render import inputs


@pytest.fixture
async def sessions(isolated_migration_postgres_url):
    engine = create_async_engine(isolated_migration_postgres_url)
    try:
        yield async_sessionmaker(engine, expire_on_commit=False)
    finally:
        await engine.dispose()


async def setup(sessions):
    from loom_service.pool_management.auth import resolve_pool_machine

    participant, _ = inputs()
    participant = participant.model_copy(update={"admission_epoch": 1})
    raw, pool_id, _, machine = await credential(sessions, role="observer", participant_config=participant)
    payload = {"node_selector": {"loom.nebius/role": "execution"}}
    async with sessions.begin() as session:
        await session.execute(update(NebiusPoolBinding).where(NebiusPoolBinding.pool_id == pool_id).values(
            policy_revision=2, binding_json=payload, binding_sha256=canonical_digest(payload).removeprefix("sha256:")))
    async with sessions() as session:
        principal = await resolve_pool_machine(session, "Bearer " + raw)
    return participant, principal, machine


def snapshots(capture):
    from loom_execution_capacity_collector.pool import PoolPodClassifier

    zero = ResourceTotals(cpu_millis=0, memory_mib=0, storage_mib=0)
    provider = ProviderCapacitySnapshot(
        source_versions={"node_group": "v1"}, provider_capacity_state="available", autoscaler_state="ready",
        quota_nodes=2, quota_vcpu_millis=8000, quota_memory_mib=16384, quota_storage_mib=65536,
        used_nodes=0, used_vcpu_millis=0, used_memory_mib=0, used_storage_mib=0,
        node_count=0, target_node_count=0, ready_node_count=0,
        node_group=NodeGroupPlacement(id="group-1", max_nodes=2, node_count=0, template={},
            raw_node=ResourceTotals(cpu_millis=4000, memory_mib=8192, storage_mib=32768)),
    )
    kubernetes = KubernetesCapacitySnapshot(
        source_versions={"pool_scope": PoolPodClassifier(capture.scope).fingerprint,
                         "nodes": "v1", "pods": "v1", "daemonsets": "v1"},
        active_nodes=0, ready_nodes=0, provisioned=zero, allocatable=zero, requested=zero,
        pending_jobs=0, unschedulable_jobs=0, image_pull_backoff_jobs=0, pending_reasons={},
    )
    return provider, kubernetes


async def capture_scope(sessions, principal):
    from loom_service.pool_management.observations import issue_pool_capture

    async with sessions.begin() as session:
        return await issue_pool_capture(session, principal)


async def publish(sessions, principal, capture, *, observed_at=None, provider=None, kubernetes=None):
    from loom_service.pool_management.observations import record_pool_observation

    initial_provider, initial_kubernetes = snapshots(capture)
    async with sessions.begin() as session:
        return await record_pool_observation(session, principal, capture_id=capture.capture_id,
            observed_at=observed_at or datetime.now(UTC),
            provider=provider or initial_provider, kubernetes=kubernetes or initial_kubernetes)


async def test_capture_is_server_derived_and_observation_replay_is_exact(sessions):
    from loom.db.nebius_pool_schema import NebiusPoolCapture, NebiusPoolObservation

    participant, principal, _ = await setup(sessions)
    capture = await capture_scope(sessions, principal)
    assert capture.scope.environments[0].target_ids == ("native",)
    assert capture.scope.environments[0].environment_id == participant.environment_id
    assert capture.scope.jobs == ()
    observed_at = datetime.now(UTC)
    first = await publish(sessions, principal, capture, observed_at=observed_at)
    replay = await publish(sessions, principal, capture, observed_at=observed_at)
    assert first.observation_id == replay.observation_id
    async with sessions() as session:
        assert await session.scalar(select(func.count()).select_from(NebiusPoolCapture)) == 1
        assert await session.scalar(select(func.count()).select_from(NebiusPoolObservation)) == 1
        assert await session.scalar(select(func.count()).select_from(NebiusPoolRequest)) == 0


async def test_new_unobserved_request_does_not_invalidate_capture_or_release_its_charge(sessions):
    participant, principal, _ = await setup(sessions)
    capture = await capture_scope(sessions, principal)
    request_id = uuid4()
    async with sessions.begin() as session:
        await session.execute(insert(NebiusPoolRequest).values(
            request_id=request_id, pool_id=participant.pool_id, participant_id=participant.participant_id,
            namespace_uid=participant.execution_namespace.uid, workload_kind="trial", local_work_id=uuid4(),
            generation=1, admission_epoch=1, target_id="native", request_sha256="c" * 64,
            request_json={"typed": True}, deadline_at=datetime.now(UTC) + timedelta(minutes=10),
            phase="reserved", cpu_millis=1000, memory_mib=1024, ephemeral_storage_mib=1024, pod_slots=1,
        ))
    await publish(sessions, principal, capture)
    async with sessions() as session:
        assert (await session.get(NebiusPoolRequest, request_id)).phase == "reserved"


@pytest.mark.parametrize("damage", ["participant", "pool", "credential", "scope", "provider", "future", "before-capture"])
async def test_observation_cannot_reuse_drifted_authority_or_wrong_physical_capture(sessions, damage):
    from loom.db.nebius_pool_schema import NebiusPoolMachine, NebiusPoolObservation
    from loom_service.pool_management.observations import PoolObservationError

    participant, principal, machine = await setup(sessions)
    capture = await capture_scope(sessions, principal)
    provider, kubernetes = snapshots(capture)
    now = datetime.now(UTC)
    async with sessions.begin() as session:
        if damage == "participant":
            binding = participant.model_copy(update={"binding_revision": 2}).model_dump(mode="json")
            await session.execute(update(NebiusPoolParticipant).where(
                NebiusPoolParticipant.participant_id == participant.participant_id).values(
                    binding_revision=2, binding_json=binding,
                    binding_sha256=canonical_digest(binding).removeprefix("sha256:")))
        elif damage == "pool":
            await session.execute(update(NebiusPoolBinding).where(
                NebiusPoolBinding.pool_id == participant.pool_id).values(admission_epoch=2))
        elif damage == "credential":
            await session.execute(update(NebiusPoolMachine).where(NebiusPoolMachine.machine_id == machine).values(phase="revoked"))
    if damage == "scope":
        kubernetes = kubernetes.model_copy(update={"source_versions": {"pool_scope": "sha256:" + "f" * 64}})
    elif damage == "provider":
        provider = provider.model_copy(update={"node_group": provider.node_group.model_copy(update={"id": "foreign"})})
    elif damage == "future":
        now += timedelta(minutes=2)
    elif damage == "before-capture":
        now -= timedelta(minutes=2)
    with pytest.raises(PoolObservationError):
        await publish(sessions, principal, capture, observed_at=now, provider=provider, kubernetes=kubernetes)
    async with sessions() as session:
        assert await session.scalar(select(func.count()).select_from(NebiusPoolObservation)) == 0


async def test_changed_replay_is_not_an_observation_refresh(sessions):
    from loom_service.pool_management.observations import PoolObservationError

    _, principal, _ = await setup(sessions)
    capture = await capture_scope(sessions, principal)
    observed_at = datetime.now(UTC)
    await publish(sessions, principal, capture, observed_at=observed_at)
    with pytest.raises(PoolObservationError):
        await publish(sessions, principal, capture, observed_at=observed_at + timedelta(seconds=1))


@pytest.mark.parametrize("mutation", ["capture-update", "capture-delete", "observation-update", "observation-delete"])
async def test_database_preserves_issued_capture_and_observation_evidence(sessions, mutation):
    from loom.db.nebius_pool_schema import NebiusPoolCapture, NebiusPoolObservation

    _, principal, _ = await setup(sessions)
    capture = await capture_scope(sessions, principal)
    await publish(sessions, principal, capture)
    model = NebiusPoolCapture if mutation.startswith("capture") else NebiusPoolObservation
    async with sessions.begin() as session:
        with pytest.raises(DBAPIError):
            async with session.begin_nested():
                await session.execute(delete(model) if mutation.endswith("delete") else update(model).values(
                    **({"scope_json": {}} if model is NebiusPoolCapture else {"observation_json": {}})))


async def test_participant_machine_cannot_issue_or_publish_observer_scope(sessions):
    from loom_service.pool_management.auth import resolve_pool_machine
    from loom_service.pool_management.observations import PoolObservationError, issue_pool_capture

    raw, _, _, _ = await credential(sessions)
    async with sessions.begin() as session:
        principal = await resolve_pool_machine(session, "Bearer " + raw)
        with pytest.raises(PoolObservationError):
            await issue_pool_capture(session, principal)

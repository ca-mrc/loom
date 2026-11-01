"""Exact cleanup receipts reconcile immutable, lagging Pod observations."""
# ruff: noqa: F811 -- imported shared pytest fixtures

import asyncio
import inspect
from copy import deepcopy
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from uuid import uuid4

import pytest
from sqlalchemy import select

from loom.db.schema import (
    ExecutionCapacityPolicy,
    ExecutionProvisioningAuthorization,
    ServiceExecutionEvent,
    ServiceExecutionLease,
    TaskImageMaterializationAttempt,
)
from loom.pipeline.keys import canonical_digest
from loom_control_plane.execution_capacity import (
    _CAPACITY_ADMISSION_LOCK,
    ExecutionProvisioningBlockedError,
    create_execution_capacity_observation,
)
from loom_control_plane.execution_capacity_reconciliation import reconcile_capacity_placement
from loom_control_plane.service_execution import (
    enqueue_execution_transition,
    record_kubernetes_observation,
)
from loom_control_plane.task_image_capacity import reserve_native_task_image_capacity
from loom_execution_actuator.contracts import (
    KubernetesApiError,
    KubernetesJobObservation,
    NormalizedJobState,
)
from loom_execution_actuator.controller import ExecutionActuator
from loom_execution_actuator.renderer import ExecutionTargetRuntime
from loom_execution_capacity_collector.contracts import CapacityPlacement
from tests.execution_placement_fixtures import placement_fixture
from tests.integration.test_execution_capacity_placement import _record
from tests.integration.test_nebius_task_image_capacity import (
    _native,
    native_setup,  # noqa: F401
)
from tests.integration.test_service_execution_leases import (
    IMAGE_ADMISSION_KEYRING,
    _cleanup_service_execution_test_rows,  # noqa: F401
    _delete_lease,
    _reserve,
    _seed_ready_trial,
    _separate_attempt_awaiting_verifier,
    _stub_verifier_plan,  # noqa: F401
    reserve_next_verifier_executions,
)


@pytest.mark.parametrize("proof", ["valid", "legacy", "wrong_uid", "wrong_epoch", "wrong_target", "future"])
async def test_exact_native_cleanup_receipt_reuses_stale_observed_capacity(native_setup, proof):
    sessions, owned = native_setup
    now = datetime.now(UTC)
    async with sessions() as session, session.begin():
        pair = await _seed_ready_trial(session, now=now)
        previous, materialization = await _native(session, owned, pair, now)
        attempt = await session.get(TaskImageMaterializationAttempt, previous)
        confirmed = now + timedelta(seconds=2)
        attempt.native_build = {
            **attempt.native_build,
            "job_uid": "deleted-job", "pod_uid": "deleted-pod",
            "state": "released", "capacity_reserved_at": now.isoformat(),
            "capacity_released_at": confirmed.isoformat(),
            "capacity_release_receipt": {
                "pod_uid": "deleted-pod", "job_uid": "deleted-job",
                "target_id": pair[1].target_id, "lease_epoch": 1,
                "confirmed_at": confirmed.isoformat(), "pods_absent": True,
            },
        }
        native = dict(attempt.native_build)
        receipt = dict(native["capacity_release_receipt"])
        if proof == "legacy":
            native.pop("capacity_release_receipt")
        else:
            if proof == "wrong_uid":
                receipt["pod_uid"] = "replacement-pod"
            if proof == "wrong_epoch":
                receipt["lease_epoch"] = 2
            if proof == "wrong_target":
                receipt["target_id"] = "another-owner"
            if proof == "future":
                receipt["confirmed_at"] = (now + timedelta(seconds=4)).isoformat()
                native["capacity_released_at"] = receipt["confirmed_at"]
            native["capacity_release_receipt"] = receipt
        attempt.native_build = native
        placement = placement_fixture(
            target_id=pair[1].target_id, quota_nodes=1,
            requested_cpu=64000, requested_memory=1024, requested_storage=2048,
        )
        placement["nodes"][0].update(used_pod_slots=1, managed_pods=[{
            "uid": "deleted-pod", "lease_id": f"task-image:{materialization}",
            "generation": 1,
            "requests": {"cpu_millis": 64000, "memory_mib": 1024, "storage_mib": 2048},
        }])
        await _record(session, pair[1].target_id, now + timedelta(seconds=1), placement)
        incoming, _ = await _native(session, owned, pair, now, cpu=8000, checksum="3" * 64)
    async with sessions() as session, session.begin():
        try:
            decision = await reserve_native_task_image_capacity(
                session, attempt_id=incoming, now=now + timedelta(seconds=3),
            )
        except ExecutionProvisioningBlockedError as error:
            assert proof != "valid"
            assert error.reason == "execution_capacity_provider_quota_nodes_exceeded"
        else:
            assert proof == "valid"
            assert decision["capacity_reason"] == "existing_allocatable"


@pytest.mark.parametrize("classification", ["pending", "unschedulable", "image_pull_backoff"])
@pytest.mark.parametrize("metadata", [True, False])
async def test_confirmed_pending_pod_release_does_not_exhaust_the_pending_limit(native_setup, classification, metadata):
    sessions, owned = native_setup
    now = datetime.now(UTC)
    async with sessions() as session, session.begin():
        pair = await _seed_ready_trial(session, now=now)
        previous, materialization = await _native(session, owned, pair, now, cpu=8000)
        old = await session.get(TaskImageMaterializationAttempt, previous)
        at = (now + timedelta(seconds=2)).isoformat()
        old.native_build = {**old.native_build, "state": "released", "job_uid": "old-job", "pod_uid": "old-pod",
            "capacity_reserved_at": now.isoformat(), "capacity_released_at": at,
            "capacity_release_receipt": {"pod_uid": "old-pod", "job_uid": "old-job", "pods_absent": True,
                "target_id": pair[1].target_id, "lease_epoch": 1, "confirmed_at": at}}
        policy = await session.get(ExecutionCapacityPolicy, pair[1].target_id)
        policy.max_pending_jobs = 1 if classification == "pending" else 128
        policy.max_unschedulable_jobs = 1 if classification == "unschedulable" else 128
        policy.max_image_pull_backoff_jobs = 0
        raw = placement_fixture(target_id=pair[1].target_id, quota_nodes=1)
        raw["pending_pods"] = [{"uid": "old-pod", "lease_id": f"task-image:{materialization}", "generation": 1,
            "requests": {"cpu_millis": 8000, "memory_mib": 1024, "storage_mib": 2048},
            "pending_counts": {"pending": True, "unschedulable": False, "image_pull_backoff": False}}]
        raw["pending_pods"][0]["pending_counts"][classification] = True
        if not metadata:
            raw["pending_pods"][0].pop("pending_counts")
        initial, _ = await _record(session, pair[1].target_id, now + timedelta(seconds=1), raw)
        names = inspect.signature(create_execution_capacity_observation).parameters
        payload = {key: deepcopy(value) for key, value in initial.observation_json.items() if key in names}
        payload.update(observed_at=now + timedelta(seconds=2), source_version=str(uuid4()), pending_jobs=1,
                       unschedulable_jobs=int(classification == "unschedulable"),
                       image_pull_backoff_jobs=int(classification == "image_pull_backoff"))
        await create_execution_capacity_observation(session, **payload)
        incoming, _ = await _native(session, owned, pair, now, cpu=8000, checksum="3" * 64)
    async with sessions() as session, session.begin():
        try:
            decision = await reserve_native_task_image_capacity(session, attempt_id=incoming, now=now + timedelta(seconds=3))
        except ExecutionProvisioningBlockedError as error:
            assert not metadata
            assert error.reason == f"execution_capacity_{'pending' if classification == 'pending' else classification}_limit_exceeded"
        else:
            assert metadata and decision["capacity_reason"] == "existing_allocatable"


@pytest.mark.parametrize("role", ["attempt", "verifier"])
@pytest.mark.parametrize("proof", ["valid", "legacy", "wrong_uid", "wrong_generation", "future"])
async def test_execution_cleanup_receipts_require_exact_pod_and_generation(
    native_setup, _stub_verifier_plan, role, proof,
):
    sessions, _ = native_setup
    now = datetime.now(UTC)
    if role == "verifier":
        _, parent, target = await _separate_attempt_awaiting_verifier(sessions, now=now)
        async with sessions() as session, session.begin():
            children = await reserve_next_verifier_executions(
                session, pool_id=target.logical_pool_id,
                image_admission_keyring=IMAGE_ADMISSION_KEYRING, now=now + timedelta(seconds=1),
            )
            lease = next(row for row in children if row.parent_lease_id == parent.id)
    else:
        async with sessions() as session, session.begin():
            trial, target = await _seed_ready_trial(session, now=now)
            lease = await _reserve(session, trial_id=trial, target=target, now=now)
    async with sessions() as session, session.begin():
        retained = await session.get(ServiceExecutionLease, lease.id)
        retained.job_uid, retained.pod_uid = "deleted-job", "deleted-pod"
    await _delete_lease(sessions, lease_id=lease.id, at=now + timedelta(seconds=2))
    async with sessions() as session, session.begin():
        retained = await session.get(ServiceExecutionLease, lease.id)
        receipt = {"pod_uid": "deleted-pod", "job_uid": "deleted-job", "pods_absent": True,
                   "resource_generation": retained.resource_generation,
                   "confirmed_at": (now + timedelta(seconds=2)).isoformat()}
        if proof == "wrong_uid":
            receipt["pod_uid"] = "another-pod"
        if proof == "wrong_generation":
            receipt["resource_generation"] = retained.resource_generation + 1
        if proof == "future":
            receipt["confirmed_at"] = (now + timedelta(seconds=10)).isoformat()
        payload = {"normalized_state": "deleted", "target_id": target.target_id}
        if proof != "legacy":
            payload["cleanup_receipt"] = receipt
        session.add(ServiceExecutionEvent(
            id=uuid4(), lease_id=lease.id, generation=retained.generation,
            ordinal=retained.last_event_ordinal + 1, event_kind="kubernetes_observed",
            idempotency_key=str(uuid4()), payload_json=payload,
            payload_sha256=canonical_digest(payload), observed_at=now + timedelta(seconds=2),
        ))
    raw = placement_fixture(target_id=target.target_id, requested_cpu=1000,
                            requested_memory=1024, requested_storage=2048)
    raw["nodes"][0].update(used_pod_slots=1, managed_pods=[{
        "uid": "deleted-pod", "lease_id": str(lease.id), "generation": lease.resource_generation,
        "requests": {"cpu_millis": 1000, "memory_mib": 1024, "storage_mib": 2048},
    }])
    original = CapacityPlacement.model_validate(raw)
    async with sessions() as session:
        corrected = await reconcile_capacity_placement(
            session, original, target_ids=frozenset({target.target_id}), now=now + timedelta(seconds=3),
        )
        assert corrected.nodes[0].requested.cpu_millis == (0 if proof == "valid" else 1000)
        # An unrelated owner must never spend another target's cleanup receipt.
        assert await reconcile_capacity_placement(
            session, original, target_ids=frozenset({"foreign-owner"}), now=now + timedelta(seconds=3),
        ) == original


async def test_release_and_lagging_collection_race_never_spends_freed_slot_twice(native_setup):
    sessions, owned = native_setup
    now = datetime.now(UTC)
    async with sessions() as session, session.begin():
        pair = await _seed_ready_trial(session, now=now)
        previous, materialization = await _native(session, owned, pair, now)
        old = await session.get(TaskImageMaterializationAttempt, previous)
        old.native_build = {**old.native_build, "job_uid": "deleted-job", "pod_uid": "deleted-pod",
                            "capacity_reserved_at": now.isoformat()}
        raw = placement_fixture(target_id=pair[1].target_id, quota_nodes=1,
                                requested_cpu=64000, requested_memory=1024, requested_storage=2048)
        raw["nodes"][0].update(used_pod_slots=1, managed_pods=[{
            "uid": "deleted-pod", "lease_id": f"task-image:{materialization}", "generation": 1,
            "requests": {"cpu_millis": 64000, "memory_mib": 1024, "storage_mib": 2048},
        }])
        await _record(session, pair[1].target_id, now + timedelta(seconds=1), raw)
        contenders = [(await _native(session, owned, pair, now, cpu=40000, checksum=str(i) * 64))[0]
                      for i in (3, 4)]
    release_pending, commit_release = asyncio.Event(), asyncio.Event()

    async def release():
        async with sessions() as session, session.begin():
            await session.execute(_CAPACITY_ADMISSION_LOCK)
            old = await session.get(TaskImageMaterializationAttempt, previous)
            at = (now + timedelta(seconds=2)).isoformat()
            old.native_build = {**old.native_build, "state": "released", "capacity_released_at": at,
                "capacity_release_receipt": {"pod_uid": "deleted-pod", "job_uid": "deleted-job",
                    "target_id": pair[1].target_id, "lease_epoch": 1,
                    "confirmed_at": at, "pods_absent": True}}
            # A collector may finish publishing an older Pod inventory even
            # after the deletion check; exact UID proof remains applicable.
            await _record(session, pair[1].target_id, now + timedelta(seconds=3), raw)
            await session.flush()
            release_pending.set()
            await commit_release.wait()

    async def claim(attempt):
        async with sessions() as session, session.begin():
            try:
                result = await reserve_native_task_image_capacity(
                    session, attempt_id=attempt, now=now + timedelta(seconds=4),
                )
            except ExecutionProvisioningBlockedError as error:
                return error.reason
            return result["capacity_reason"]

    releasing = asyncio.create_task(release())
    try:
        await asyncio.wait_for(release_pending.wait(), timeout=10)
        # Other readers cannot consume an uncommitted deletion acknowledgement.
        async with sessions() as session:
            original = CapacityPlacement.model_validate(raw)
            assert await reconcile_capacity_placement(
                session, original, target_ids=frozenset({pair[1].target_id}),
                now=now + timedelta(seconds=4),
            ) == original
        claims = [asyncio.create_task(claim(attempt)) for attempt in contenders]
    finally:
        commit_release.set()
    await asyncio.wait_for(releasing, timeout=10)
    results = await asyncio.wait_for(asyncio.gather(*claims), timeout=10)
    assert sorted(results) == ["execution_capacity_provider_quota_nodes_exceeded", "existing_allocatable"]


async def test_terminal_authorization_keeps_unobserved_pod_charge_until_cleanup(native_setup):
    sessions, owned = native_setup
    now = datetime.now(UTC)
    async with sessions() as session, session.begin():
        trial, target = await _seed_ready_trial(session, now=now)
        lease = await _reserve(session, trial_id=trial, target=target, now=now)
        authorization = await session.scalar(select(ExecutionProvisioningAuthorization).where(
            ExecutionProvisioningAuthorization.lease_id == lease.id,
        ))
        lease.observed_state = "failed"
        await session.flush()
        await session.refresh(authorization)
        assert authorization.state == "released" and lease.cleanup_state != "complete"
        raw = placement_fixture(target_id=target.target_id, quota_nodes=1)
        await _record(session, target.target_id, now + timedelta(seconds=1), raw)
        incoming, _ = await _native(session, owned, (trial, target), now,
                                    cpu=64000 - authorization.requested_cpu_millis + 1)
    async with sessions() as session, session.begin():
        try:
            await reserve_native_task_image_capacity(session, attempt_id=incoming, now=now + timedelta(seconds=2))
        except ExecutionProvisioningBlockedError as error:
            assert error.reason == "execution_capacity_provider_quota_nodes_exceeded"
        else:
            pytest.fail("terminal state is not physical Pod deletion")


@pytest.mark.parametrize("role", ["attempt", "verifier"])
@pytest.mark.parametrize("legacy_first", [False, True])
async def test_actuator_requires_exact_dependent_absence_before_recording_cleanup_receipt(
    native_setup, _stub_verifier_plan, role, legacy_first,
):
    sessions, _ = native_setup
    now = datetime.now(UTC)
    if role == "verifier":
        _, parent, target = await _separate_attempt_awaiting_verifier(sessions, now=now)
        async with sessions() as session, session.begin():
            children = await reserve_next_verifier_executions(
                session, pool_id=target.logical_pool_id,
                image_admission_keyring=IMAGE_ADMISSION_KEYRING, now=now,
            )
            lease = next(row for row in children if row.parent_lease_id == parent.id)
    else:
        async with sessions() as session, session.begin():
            trial, target = await _seed_ready_trial(session, now=now)
            lease = await _reserve(session, trial_id=trial, target=target, now=now)
    async with sessions() as session, session.begin():
        lease = await session.get(ServiceExecutionLease, lease.id)
        lease.pod_uid, lease.job_uid = "old-pod", "old-job"
        await session.flush()
        await enqueue_execution_transition(session, lease_id=lease.id, expected_generation=lease.generation,
                                           desired_state="cancel", now=now)
        lease.cleanup_state = "in_progress"
        lease.output_commit_state, lease.output_generation = "unavailable", lease.resource_generation
        lease.output_unavailable_reason = "test_cleanup"
    absent = False
    checked = []

    async def confirm(**identity):
        checked.append(identity)
        return absent

    actuator = ExecutionActuator(sessions=sessions, kubernetes=SimpleNamespace(confirm_pod_absence=confirm),
        target=ExecutionTargetRuntime(target_id=target.target_id, namespace=target.namespace_name),
        controller_id="exact-cleanup-test")
    observation = KubernetesJobObservation(namespace=lease.namespace_name, job_name=lease.job_name,
        lease_id=str(lease.id), execution_unit_key=str(lease.execution_unit_key),
        resource_generation=lease.resource_generation, target_id=target.target_id,
        job_uid=lease.job_uid, normalized_state=NormalizedJobState.DELETED)
    with pytest.raises(KubernetesApiError, match="dependent Pod"):
        await actuator._persist_observation(lease, observation, now=now)
    async with sessions() as session:
        assert (await session.get(ServiceExecutionLease, lease.id)).cleanup_state == "in_progress"
    absent = True
    if legacy_first:
        old_actor = ExecutionActuator(sessions=sessions, kubernetes=SimpleNamespace(),
            target=ExecutionTargetRuntime(target_id=target.target_id, namespace=target.namespace_name),
            controller_id="legacy-cleanup-test")
        await old_actor._persist_observation(lease, observation, now=now)
    await actuator._persist_observation(lease, observation, now=now)
    # A second replica's later absence probe must reuse the first durable
    # receipt under the unchanged observation idempotency key.
    await actuator._persist_observation(lease, observation, now=now)
    async with sessions() as session:
        retained = await session.get(ServiceExecutionLease, lease.id)
        assert retained.cleanup_state == "complete"
        events = (await session.scalars(select(ServiceExecutionEvent).where(
            ServiceExecutionEvent.lease_id == lease.id,
        ))).all()
        if legacy_first:
            assert all("cleanup_receipt" not in event.payload_json for event in events)
            return  # New actors must not manufacture evidence into an old event.
        receipt = next(event.payload_json["cleanup_receipt"] for event in events
                       if "cleanup_receipt" in event.payload_json)
        assert receipt["pod_uid"] == lease.pod_uid and receipt["job_uid"] == lease.job_uid
        assert receipt["resource_generation"] == lease.resource_generation
        assert receipt["pods_absent"] is True
        assert datetime.fromisoformat(receipt["confirmed_at"]) <= datetime.now(UTC)
        event = next(event for event in events if "cleanup_receipt" in event.payload_json)
        # Two replicas may probe t1 < t2 but acquire the event lock t2, t1.
        # Whichever committed first remains the immutable replay authority.
        earlier_probe = {**event.payload_json, "cleanup_receipt": {
            **receipt, "confirmed_at": (datetime.fromisoformat(receipt["confirmed_at"])
                                        - timedelta(seconds=1)).isoformat(),
        }}
        replayed, duplicate = await record_kubernetes_observation(
            session, lease_id=lease.id, generation=retained.generation,
            payload=earlier_probe, observed_at=now,
        )
        assert duplicate and replayed.payload_json == event.payload_json
        legacy_payload = {key: value for key, value in event.payload_json.items() if key != "cleanup_receipt"}
        legacy_replay, duplicate = await record_kubernetes_observation(
            session, lease_id=lease.id, generation=retained.generation,
            payload=legacy_payload, observed_at=now,
        )
        assert duplicate and legacy_replay.payload_json == event.payload_json
        await session.commit()
    assert checked[-1] == {"namespace": lease.namespace_name, "pod_uid": lease.pod_uid, "job_uid": lease.job_uid}

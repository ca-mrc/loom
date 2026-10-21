"""Deferred grading uses a fresh global grant after exact parent cleanup."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import func, select

from loom.db.nebius_pool_outbox_schema import NebiusPoolExecutionOutbox
from loom.db.schema import ExecutionProvisioningAuthorization, ServiceExecutionLease, Task, Trial
from loom_execution_actuator.pool_execution_selection import PoolExecutionSelector
from tests.integration import test_nebius_pool_execution_activation as activation
from tests.integration.test_nebius_pool_execution_controller import connected, lease, start
from tests.integration.test_nebius_pool_observation_registry import sessions as sessions


@pytest.fixture
async def separate_setup(monkeypatch):
    from loom.db.schema import Batch
    from tests.unit.test_native_oracle import _oracle
    from tests.unit.test_service_execution_materialization import _provenance
    from tests.unit.test_service_execution_terminus_plan import _inputs

    original = activation.setup

    async def setup(sessions):
        outbox, trial_id, target = await original(sessions)
        task_config, _, profile = _inputs()
        async with sessions.begin() as session:
            trial = await session.get(Trial, trial_id)
            task = await session.get(Task, trial.task_id)
            raw = task_config.model_dump(mode="json")
            raw["task"]["id"] = task.id
            task.config, task.source_provenance = raw, _provenance()
            trial.config = _oracle(verifier_env_mode="separate").model_dump(mode="json")
            batch = await session.get(Batch, trial.batch_id)
            batch.service_execution_runtime_profile = profile.model_dump(mode="json")
        return outbox, trial_id, target

    monkeypatch.setattr(activation, "setup", setup)


async def commit_output(sessions, current, store, *, reward=None):
    from uuid import UUID, uuid4

    from loom.execution_runtime_contract import ExecutionRuntimePlanV1
    from loom.pipeline.artifact_commit import ArtifactCommitService, PartReceiptV1
    from loom.pipeline.keys import canonical_document, digest_bytes
    from loom.service_execution_oracle import oracle_usage
    from loom_control_plane.artifact_commit_runtime import SqlArtifactCommitRepository
    from loom_control_plane.service_execution import _runtime_container_roles
    from loom_control_plane.service_execution_output import (
        ServiceExecutionOutputFileV1,
        ServiceExecutionOutputPrepareV1,
        ServiceExecutionOutputRouteService,
    )
    from tests.integration.test_service_execution_leases import _runtime_result_payload
    from tests.unit.test_native_oracle import _trace

    plan = ExecutionRuntimePlanV1.model_validate(current.runtime_contract_json)
    result = _runtime_result_payload(current, started_at=datetime.now(UTC))
    result["container_roles"] = _runtime_container_roles(plan)
    from copy import deepcopy

    phase_template = result["phases"][0]
    result["phases"] = []
    for index, runtime_phase in enumerate((*plan.setup, plan.main), 1):
        phase = deepcopy(phase_template)
        phase.update(ordinal=index, role=runtime_phase.role)
        for stream in ("stdout", "stderr"):
            phase[stream]["path"] = f"{index:02d}-{runtime_phase.role}.{stream}"
        result["phases"].append(phase)
    files = (
        {
            "trajectory/events.jsonl": _trace(current.trial_id)[1],
            "accounting/usage.json": canonical_document(oracle_usage()),
            "artifacts/workspace.tar": b"committed workspace",
        }
        if current.execution_role == "attempt"
        else {"verifier/output.json": canonical_document({"rewards": {"passed": reward}})}
    )
    result["verifier_rewards"] = {"passed": reward} if reward is not None else None
    result["outputs"] = [
        {
            **declaration.model_dump(mode="json"),
            "state": "captured",
            "size_bytes": len(files[declaration.relative_path]),
            "sha256": digest_bytes(files[declaration.relative_path]),
        }
        if declaration.relative_path in files
        else {**declaration.model_dump(mode="json"), "state": "missing"}
        for declaration in plan.output_declarations
    ]
    for phase in result["phases"]:
        for stream in ("stdout", "stderr"):
            body = b"ok\n" if stream == "stdout" else b""
            phase[stream].update(
                sha256=digest_bytes(body), bytes_seen=len(body), bytes_saved=len(body)
            )
            files[phase[stream]["path"]] = body
    files = {"result.json": canonical_document(result), **dict(sorted(files.items()))}
    route = ServiceExecutionOutputRouteService(
        session_factory=sessions,
        service=ArtifactCommitService(
            store=store,
            bucket="artifacts",
            repository=SqlArtifactCommitRepository(
                session_factory=sessions, store=store, bucket="artifacts"
            ),
        ),
    )
    grant = await route.prepare(
        lease=current,
        request=ServiceExecutionOutputPrepareV1(
            schema_version="loom.service-execution-output-prepare.v1",
            request_id=uuid4(),
            lease_id=current.id,
            generation=current.resource_generation,
            execution_role=current.execution_role,
            files=tuple(
                ServiceExecutionOutputFileV1(
                    relative_path=path,
                    media_type="application/octet-stream",
                    size_bytes=len(body),
                    sha256=digest_bytes(body),
                )
                for path, body in files.items()
            ),
        ),
    )
    upload_id, token = UUID(grant["upload_session_id"]), grant["upload_token"]
    for index, body in enumerate(files.values()):

        async def stream(body=body):
            yield body

        receipt = PartReceiptV1.model_validate(
            await route.put_part(
                lease=current,
                session_id=upload_id,
                file_index=index,
                part_number=1,
                content_length=len(body),
                content_sha256=digest_bytes(body),
                upload_token=token,
                body=stream(),
            )
        )
        await route.complete_file(
            lease=current,
            session_id=upload_id,
            file_index=index,
            ordered_parts=(receipt,),
            upload_token=token,
        )
    return await route.commit(lease=current, session_id=upload_id, upload_token=token)


async def complete_execution(sessions, case, current, store, *, reward=None):
    await commit_output(sessions, current, store, reward=reward)
    case.sdk.pods[0]["status"] = {"phase": "Succeeded"}
    await case.actuator.reconcile_full_once()
    await case.actuator.reconcile_full_once()
    await case.worker.run_once()
    case.sdk.pods.clear()
    await case.worker.run_once()
    await case.actuator.reconcile_full_once()
    async with sessions() as session:
        done = await session.get(ServiceExecutionLease, current.id)
        assert done.cleanup_state == "complete"
    assert (await case.outbox.get(case.resources.key(done))).phase == "released"
    return done


async def finish_parent(sessions, case):
    from loom.trajectory.storage import FakeObjectStore

    await start(sessions, case)
    case.store = FakeObjectStore()
    return await complete_execution(sessions, case, await lease(sessions, case), case.store)


@pytest.mark.parametrize("node_share", [False, True])
async def test_global_verifier_proposal_and_grant_preserve_parent_attempt(
    sessions,
    tmp_path,
    separate_setup,
    node_share,
):
    async with connected(sessions, tmp_path) as case:
        if node_share:
            from loom.db.schema import Batch

            await case.outbox.request_cancel(case.proposal.request.key)
            assert (await case.driver.advance(case.proposal.request.key)).phase == "cancelled"
            async with sessions.begin() as session:
                trial = await session.get(Trial, case.trial_id)
                batch = await session.get(Batch, trial.batch_id)
                batch.service_execution_runtime_profile = {
                    **batch.service_execution_runtime_profile,
                    "resource_allocation_policy": "node-share-v1",
                }
            case.proposal = await PoolExecutionSelector(
                outbox=case.outbox, allocation_reader=case.driver.management
            ).select_next()
            assert case.proposal.request.execution.node_allocation is not None
        parent = await finish_parent(sessions, case)
        selections = await asyncio.gather(
            *(PoolExecutionSelector(outbox=case.outbox).select_next() for _ in range(2))
        )
        (selected,) = [item for item in selections if item is not None]
        assert selected is not None, "a pending verifier must select a global proposal"
        assert selected.request.key.workload_kind == "verifier"
        assert selected.request.execution.parent_lease_id == parent.id
        assert (
            selected.request.execution.node_allocation
            == case.proposal.request.execution.node_allocation
        )
        assert selected.request.execution.runtime.node_resource_allocation == (
            case.proposal.request.execution.runtime.node_resource_allocation
        )
        if node_share:
            from uuid import uuid4

            from loom.nebius_pool_workload import PoolExecutionPrepareV1
            from loom_execution_actuator.pool_client import PoolRequestUnconfirmedError

            allocation = selected.request.execution.node_allocation
            changed = selected.request.model_copy(
                update={
                    "execution": selected.request.execution.model_copy(
                        update={
                            "node_allocation": allocation.model_copy(
                                update={"observation_id": uuid4()}
                            )
                        }
                    )
                }
            )
            with pytest.raises(PoolRequestUnconfirmedError):
                await case.driver.management.prepare(changed)
            # Historical sizing keeps its old scope after a binding revision;
            # it never supplies authority for the new request's admission epoch.
            changed = PoolExecutionPrepareV1.model_validate(
                selected.request.model_dump()
                | {
                    "admission_epoch": selected.request.admission_epoch + 1,
                    "participant_revision": selected.request.participant_revision + 1,
                }
            )
            assert changed.execution.node_allocation == allocation
            with pytest.raises(PoolRequestUnconfirmedError):
                await case.driver.management.prepare(changed)
        # A lost prepare reply reuses its exact grant; concurrent attachment
        # consumes local admission/cost once and never increments the attempt.
        receipt = await case.driver.management.prepare(selected.request)
        assert await case.driver.management.prepare(selected.request) == receipt
        attachments = await asyncio.gather(
            *(case.outbox.accept_grant(selected.request.key, receipt) for _ in range(2))
        )
        assert attachments[0] == attachments[1]
        pending = await case.outbox.begin_activation(selected.request.key)
        # Commit activation but drop its reply. Driver restart reads it back.
        activated = await case.driver.management.activate(pending.activation)
        active = await case.driver.advance(selected.request.key)
        assert active.phase == "active" and active.activated == activated
        async with sessions() as session:
            child = await session.get(ServiceExecutionLease, active.lease_id)
            trial = await session.get(Trial, case.trial_id)
            assert (child.execution_role, child.parent_lease_id, child.attempt) == (
                "verifier",
                parent.id,
                1,
            )
            assert trial.state == "running" and trial.attempt_count == 1
            assert (
                await session.scalar(
                    select(func.count()).select_from(ExecutionProvisioningAuthorization)
                )
                == 0
            )
            assert (
                await session.scalar(select(func.count()).select_from(NebiusPoolExecutionOutbox))
                == 2 + node_share
            )
        assert case.resources.key(child) == selected.request.key
        assert await PoolExecutionSelector(outbox=case.outbox).select_next() is None
        from loom.db.schema import ExecutionAdmissionReservation, ExecutionCostReservation
        from loom_control_plane.service_execution_materializer import ServiceExecutionMaterializer
        from tests.integration.test_nebius_pool_pod_inventory import pod

        await case.actuator.run_commands_once()
        await case.worker.run_once()
        (job,) = case.api.objects.values()
        child_pod = pod(job)
        child_pod["spec"]["nodeName"] = "execution-node"
        child_pod["status"].update(
            podIP="10.24.7.19",
            containerStatuses=[
                {
                    "name": "execution",
                    "image": "test",
                    "imageID": "test",
                    "ready": True,
                    "restartCount": 0,
                    "state": {"running": {"startedAt": datetime.now(UTC).isoformat()}},
                }
            ],
        )
        case.sdk.pods.append(child_pod)
        await case.actuator.reconcile_full_once()
        async with sessions() as session:
            child = await session.get(ServiceExecutionLease, child.id)
            assert child.job_uid == job["metadata"]["uid"]
        child = await complete_execution(sessions, case, child, case.store, reward=0)
        materializer = ServiceExecutionMaterializer(
            session_factory=sessions,
            source_store=case.store,
            source_bucket="artifacts",
            canonical_store=case.store,
            artifacts_bucket="artifacts",
            trajectories_bucket="trajectories",
        )
        assert await materializer.run_once() == 1
        async with sessions() as session:
            trial = await session.get(Trial, case.trial_id)
            parent = await session.get(ServiceExecutionLease, parent.id)
            child = await session.get(ServiceExecutionLease, child.id)
            assert trial.state == "succeeded" and trial.result["reward"] == {"passed": 0}
            assert trial.result["aggregate_reward"] == 0
            assert parent.materialization_state == child.materialization_state == "committed"
            assert parent.canonical_trajectory_sha256 == child.canonical_trajectory_sha256
            assert parent.canonical_atif_sha256 == child.canonical_atif_sha256
            assert child.created_at >= parent.deleted_at
            costs = list(await session.scalars(select(ExecutionCostReservation)))
            assert sorted(cost.execution_role for cost in costs) == ["attempt", "verifier"]
            assert all(cost.state != "reserved" for cost in costs)
            admissions = list(await session.scalars(select(ExecutionAdmissionReservation)))
            assert len(admissions) == 2 and all(item.state == "released" for item in admissions)
        assert len(case.api.writes) == len(case.api.deletes) == 2


@pytest.mark.parametrize("damage", ["full", "cancel", "config"])
async def test_pending_global_verifier_does_not_claim_or_hide_other_trials(
    sessions,
    tmp_path,
    separate_setup,
    damage,
):
    from loom.db.schema import ExecutionCostReservation
    from loom_execution_capacity_collector.contracts import CapacityPlacement
    from tests.execution_placement_fixtures import placement_fixture
    from tests.integration.test_nebius_pool_execution_controller import another_trial
    from tests.integration.test_nebius_pool_registry import machine, publish_placement

    async with connected(sessions, tmp_path) as case:
        parent = await finish_parent(sessions, case)
        selected = await PoolExecutionSelector(outbox=case.outbox).select_next()
        if damage == "full":
            observer = await machine(sessions, selected.request.pool_id)
            placement = CapacityPlacement.model_validate(
                placement_fixture(
                    target_id="group-1",
                    node_cpu=3000,
                    node_memory=8192,
                    node_storage=32768,
                    requested_cpu=3000,
                    quota_nodes=1,
                    used_nodes=1,
                )
            )
            await publish_placement(sessions, observer, placement)
            assert (await case.driver.advance(selected.request.key)).phase == "selected"
            following = await another_trial(sessions, case.trial_id)
            next_work = await PoolExecutionSelector(outbox=case.outbox).select_next()
            async with sessions() as session:
                assert (
                    await session.get(
                        NebiusPoolExecutionOutbox, next_work.request.key.local_work_id
                    )
                ).trial_id == following
        async with sessions.begin() as session:
            trial = await session.get(Trial, case.trial_id)
            if damage in {"full", "cancel"}:
                trial.cancellation_requested_at = datetime.now(UTC)
            else:
                trial.config = {**trial.config, "override_verifier_timeout_sec": 45}
        assert (await case.driver.advance(selected.request.key)).phase == "cancelled"
        async with sessions() as session:
            assert (
                await session.scalar(select(func.count()).select_from(ServiceExecutionLease)) == 1
            )
            assert (
                await session.scalar(select(func.count()).select_from(ExecutionCostReservation))
                == 1
            )
            trial = await session.get(Trial, case.trial_id)
            assert trial.attempt_count == parent.attempt == 1
            assert (
                await session.scalar(
                    select(func.count()).select_from(ExecutionProvisioningAuthorization)
                )
                == 0
            )


@pytest.mark.parametrize("after_activation", [False, True])
async def test_global_verifier_lost_claim_retains_cleanup_and_retries_only_the_child(
    sessions,
    tmp_path,
    separate_setup,
    after_activation,
):
    from loom.nebius_pool_contract import PoolReceiptV1
    from loom_control_plane.service_execution import mark_execution_output_unavailable

    async with connected(sessions, tmp_path) as case:
        parent = await finish_parent(sessions, case)
        selected = await PoolExecutionSelector(outbox=case.outbox).select_next()
        if after_activation:
            attached = await case.driver.advance(selected.request.key)
            assert attached.phase == "active"
        else:
            receipt = await case.driver.management.prepare(selected.request)
            assert isinstance(receipt, PoolReceiptV1)
            attached = await case.outbox.accept_grant(selected.request.key, receipt)
            assert attached.phase == "attached"
        await case.outbox.request_cancel(selected.request.key)
        if after_activation:
            await case.actuator.reconcile_full_once()
            async with sessions.begin() as session:
                child = await session.get(ServiceExecutionLease, attached.lease_id)
                assert child.desired_state == "retry"
                await mark_execution_output_unavailable(
                    session,
                    lease_id=child.id,
                    expected_generation=child.generation,
                    reason="test_cleanup_deadline",
                    now=child.cleanup_deadline_at,
                )
            await case.actuator.reconcile_full_once()
            await case.worker.run_once()
            await case.actuator.reconcile_full_once()
            assert (await case.outbox.get(selected.request.key)).phase == "released"
        else:
            assert (await case.driver.advance(selected.request.key)).phase == "cancelled"
        async with sessions() as session:
            child = await session.get(ServiceExecutionLease, attached.lease_id)
            trial = await session.get(Trial, case.trial_id)
            assert child.cleanup_state == "complete" and child.deleted_at is not None
            assert trial.state == "running" and trial.attempt_count == parent.attempt == 1
            assert trial.result["verifier_execution"]["retries"] == 1
        again = await PoolExecutionSelector(outbox=case.outbox).select_next()
        assert again.request.key != selected.request.key
        assert again.request.execution.parent_lease_id == parent.id
        if not after_activation:
            for retry in (1, 2):
                receipt = await case.driver.management.prepare(again.request)
                await case.outbox.accept_grant(again.request.key, receipt)
                await case.outbox.request_cancel(again.request.key)
                assert (await case.driver.advance(again.request.key)).phase == "cancelled"
                async with sessions() as session:
                    trial = await session.get(Trial, case.trial_id)
                    assert trial.attempt_count == 1
                    assert trial.state == ("running" if retry < 2 else "failed")
                again = await PoolExecutionSelector(outbox=case.outbox).select_next()
            assert again is None and trial.failure_reason == "verifier_unavailable"
            return
        active = await case.driver.advance(again.request.key)
        assert active.phase == "active"
        async with sessions() as session:
            child = await session.get(ServiceExecutionLease, active.lease_id)
            assert child.verifier_retry == 1 and child.attempt == 1
            assert (
                await session.scalar(
                    select(func.count()).select_from(ExecutionProvisioningAuthorization)
                )
                == 0
            )


async def test_global_verifier_guard_rejects_foreign_parent_and_retains_history(
    sessions,
    tmp_path,
    separate_setup,
):
    from copy import deepcopy
    from pathlib import Path
    from uuid import uuid4

    from alembic import command
    from alembic.config import Config
    from sqlalchemy.exc import DBAPIError

    from loom.pipeline.keys import canonical_digest
    from loom_execution_actuator.pool_client import PoolRequestUnconfirmedError

    root = Path(__file__).resolve().parents[2]
    config = Config(str(root / "database/migrations/alembic.ini"))
    config.set_main_option("script_location", str(root / "database/migrations"))
    url = (
        sessions.kw["bind"]
        .url.set(drivername="postgresql+psycopg")
        .render_as_string(hide_password=False)
    )
    config.set_main_option("sqlalchemy.url", url.replace("%", "%%"))
    async with connected(sessions, tmp_path) as case:
        await finish_parent(sessions, case)
        # With only attempt history, upgrade and downgrade preserve the old
        # outbox contract. Once verifier history exists it cannot be discarded.
        await asyncio.to_thread(command.downgrade, config, "0178")
        await asyncio.to_thread(command.upgrade, config, "0179")
        selected = await PoolExecutionSelector(outbox=case.outbox).select_next()
        for change in ("parent", "runtime"):
            execution = selected.request.execution
            changed = (
                execution.model_copy(update={"parent_lease_id": uuid4()})
                if change == "parent"
                else execution.model_copy(
                    update={
                        "runtime": execution.runtime.model_copy(
                            update={"task_revision_sha256": "sha256:" + "f" * 64}
                        )
                    }
                )
            )
            wrong = selected.request.model_copy(update={"execution": changed})
            with pytest.raises(PoolRequestUnconfirmedError):
                await case.driver.management.prepare(wrong)
        await case.outbox.request_cancel(selected.request.key)
        assert (await case.driver.advance(selected.request.key)).phase == "cancelled"
        async with sessions() as session:
            original = await session.get(
                NebiusPoolExecutionOutbox, selected.request.key.local_work_id
            )
        for field, value in (
            ("parent_lease_id", str(uuid4())),
            ("manifest_sha256", "sha256:" + "e" * 64),
            ("attempt", 2),
            ("resource_generation", 2),
        ):
            snapshot, request = deepcopy(original.selection_json), deepcopy(original.request_json)
            new_id = uuid4()
            request["key"]["local_work_id"] = str(new_id)
            snapshot["verifier"][field] = value
            with pytest.raises(DBAPIError, match="execution selection identity differs"):
                async with sessions.begin() as session:
                    session.add(
                        NebiusPoolExecutionOutbox(
                            lease_id=new_id,
                            trial_id=case.trial_id,
                            pool_id=original.pool_id,
                            participant_id=original.participant_id,
                            request_json=request,
                            request_sha256=canonical_digest(request).removeprefix("sha256:"),
                            selection_json=snapshot,
                            phase="selected",
                        )
                    )
        with pytest.raises(DBAPIError, match="global verifier history requires revision0179"):
            await asyncio.to_thread(command.downgrade, config, "0178")
        assert (await case.outbox.get(selected.request.key)).phase == "cancelled"


async def test_global_verifier_capacity_timeout_is_terminal_without_agent_retry(
    sessions,
    tmp_path,
    separate_setup,
):
    async with connected(sessions, tmp_path) as case:
        await finish_parent(sessions, case)
        selected = await PoolExecutionSelector(outbox=case.outbox).select_next()
        async with sessions.begin() as session:
            trial = await session.get(Trial, case.trial_id)
            trial.result = {
                **trial.result,
                "verifier_execution": {
                    **trial.result["verifier_execution"],
                    "pending_since": (datetime.now(UTC) - timedelta(minutes=31)).isoformat(),
                },
            }
        assert (await case.driver.advance(selected.request.key)).phase == "cancelled"
        assert await PoolExecutionSelector(outbox=case.outbox).select_next() is None
        async with sessions() as session:
            trial = await session.get(Trial, case.trial_id)
            assert trial.state == "failed" and trial.failure_reason == "verifier_unavailable"
            assert trial.attempt_count == 1
            assert (
                await session.scalar(select(func.count()).select_from(ServiceExecutionLease)) == 1
            )

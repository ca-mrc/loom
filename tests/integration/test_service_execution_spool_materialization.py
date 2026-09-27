"""Real two-endpoint spool/canonical persistence proof for #1765.

This exercises the existing persisted materializer, not the application settings
wiring. Only disposable testcontainers are stopped; no shared service is used.
"""

from __future__ import annotations

import asyncio
import copy
import json
import tarfile
import threading
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from types import SimpleNamespace
from uuid import UUID, uuid4

import pytest
import urllib3
from alembic import command
from minio import Minio
from sqlalchemy import select, text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from testcontainers.core.wait_strategies import HttpWaitStrategy
from testcontainers.minio import MinioContainer

from loom.db.schema import (
    Artifact,
    LlmCall,
    ServiceExecutionLease,
    ServiceExecutionLeaseHistory,
    Task,
    TaskImageMaterialization,
    Team,
    Trial,
    TrialEvent,
    TrialTaskImageMaterialization,
)
from loom.execution_runtime_contract import RuntimeOutputDeclarationV1
from loom.pipeline.artifact_commit import ArtifactCommitService, PartReceiptV1
from loom.pipeline.keys import canonical_document, digest_bytes
from loom.service_execution_terminus_trace import terminus_usage
from loom.trajectory.storage import MinioObjectStore
from loom_control_plane.artifact_commit_runtime import SqlArtifactCommitRepository
from loom_control_plane.service_execution import (
    enqueue_execution_transition,
    finalize_committed_service_execution,
    record_execution_event,
)
from loom_control_plane.service_execution_materializer import (
    MaterializationIntegrityError,
    ServiceExecutionMaterializer,
    run_service_execution_materializer_loop,
)
from loom_control_plane.service_execution_output import (
    ServiceExecutionOutputFileV1,
    ServiceExecutionOutputPrepareV1,
    ServiceExecutionOutputRouteService,
    ServiceExecutionPeerV1,
)
from tests.integration.minio_test_images import MINIO_TEST_IMAGE
from tests.integration.test_migration_service_execution_materialization import _config
from tests.integration.test_service_execution_leases import (
    _complete_output_contract,
    _reserve,
    _runtime_result_payload,
    _seed_ready_trial,
)
from tests.support.minio_images import prepare_test_image


@pytest.fixture
def independent_minio_endpoints() -> Iterator[tuple[MinioContainer, MinioContainer]]:
    label = {"loom.test": "service-execution-spool-materialization"}
    with (
        MinioContainer(prepare_test_image(MINIO_TEST_IMAGE))
        .waiting_for(HttpWaitStrategy(9000, "/minio/health/cluster"))
        .with_kwargs(labels=label) as spool,
        MinioContainer(prepare_test_image(MINIO_TEST_IMAGE))
        .waiting_for(HttpWaitStrategy(9000, "/minio/health/cluster"))
        .with_kwargs(labels=label) as canonical,
    ):
        assert spool.get_config()["endpoint"] != canonical.get_config()["endpoint"]
        spool.get_client().make_bucket("artifacts")
        canonical.get_client().make_bucket("artifacts")
        canonical.get_client().make_bucket("trajectories")
        yield spool, canonical


def _store(container: MinioContainer, *, outage: bool = False) -> MinioObjectStore:
    config = container.get_config()
    return MinioObjectStore(
        endpoint_url=f"http://{config['endpoint']}",
        access_key=config["access_key"],
        secret_key=config["secret_key"],
        # A healthy multipart completion may take longer than 200 ms on CI.
        # Only the deliberate canonical outage needs aggressive transport timeouts.
        connect_timeout=0.2 if outage else 5,
        read_timeout=0.2 if outage else 5,
        operation_timeout=10,
        operation_attempts=1,
    )


async def _wait_for_minio_bucket(container: MinioContainer, bucket: str) -> None:
    config = container.get_config()
    # The SDK default retries honor Retry-After (including long startup 503s),
    # which can outlive an outer polling loop. This loop owns the only retries.
    transport = urllib3.PoolManager(
        timeout=urllib3.Timeout(connect=0.5, read=0.5), retries=False,
    )
    client = Minio(
        config["endpoint"],
        access_key=config["access_key"],
        secret_key=config["secret_key"],
        secure=False,
        region="us-east-1",
        http_client=transport,
    )
    try:
        async with asyncio.timeout(10):
            while True:
                try:
                    if await asyncio.to_thread(client.bucket_exists, bucket):
                        return
                except Exception:
                    pass  # The same persisted bucket may be unavailable during restart.
                await asyncio.sleep(0.1)
    except TimeoutError:
        pytest.fail("disposable canonical MinIO bucket did not become ready within 10s")
    finally:
        transport.clear()


@pytest.mark.parametrize(
    "terminus,legacy_repair,prepared_snapshot,typed_failure,archival_recovery,corrupt_recovery,archival_history_upgrade,usage_recovery",
    [pytest.param(False, False, False, False, False, False, False, False, id="direct"),
     pytest.param(True, False, False, False, False, False, False, False, id="terminus"),
     pytest.param(True, True, False, False, False, False, False, False, id="accounting-repair"),
     pytest.param(True, True, True, False, False, False, False, False, id="prepared-snapshot"),
     pytest.param(True, False, False, True, False, False, False, False, id="typed-failure"),
     pytest.param(True, False, False, False, True, False, False, False, id="verifier-archive"),
     pytest.param(True, False, False, False, True, True, False, False, id="verifier-archive-corrupt"),
     pytest.param(True, False, False, False, True, False, True, False, id="verifier-archive-history-upgrade"),
     pytest.param(True, False, False, False, False, False, False, True, id="usage-archive"),
     pytest.param(True, False, False, False, False, True, False, True, id="usage-archive-corrupt")],
)
async def test_independent_spool_survives_outage_restart_and_ack_gated_gc(
    terminus: bool,
    legacy_repair: bool,
    prepared_snapshot: bool,
    typed_failure: bool,
    archival_recovery: bool,
    corrupt_recovery: bool,
    archival_history_upgrade: bool,
    usage_recovery: bool,
    monkeypatch: pytest.MonkeyPatch,
    isolated_migration_postgres_url: str,
    independent_minio_endpoints: tuple[MinioContainer, MinioContainer],
) -> None:
    spool_container, canonical_container = independent_minio_endpoints
    source_store, canonical_store = _store(spool_container), _store(canonical_container)
    engine = create_async_engine(isolated_migration_postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime.now(UTC)
    plan = _complete_output_contract(now=now)
    exception = {"exception_type": "ContextLengthExceededError",
                 "exception_message": "ContextLengthExceededError", "occurred_at": now.isoformat().replace("+00:00", "Z")}
    if typed_failure:
        plan = plan.model_copy(update={"output_declarations": (*plan.output_declarations,
            RuntimeOutputDeclarationV1(source_path=".loom/agent/exception.json",
                relative_path="diagnostics/agent-exception.json", kind="agent_native", required=False))})

    if archival_recovery:
        exception = {"exception_type": "ServiceExecutionTaskError",
                     "exception_message": "isolated verifier process failed", "occurred_at": now.isoformat()}
        plan = plan.model_copy(update={"output_declarations": (
            RuntimeOutputDeclarationV1(source_path=".loom/verifier/exception.json",
                relative_path="diagnostics/verifier-exception.json", kind="verifier", required=False),
            *plan.output_declarations)})

    def materializer() -> ServiceExecutionMaterializer:
        # Zero retention/claim TTL advances time locally without waiting a day.
        # GC must still require a persisted canonical ACK, not just copied files.
        return ServiceExecutionMaterializer(
            session_factory=sessions,
            source_store=source_store,
            source_bucket="artifacts",
            canonical_store=canonical_store,
            artifacts_bucket="artifacts",
            trajectories_bucket="trajectories",
            retry_base_seconds=0,
            source_retention_seconds=0,
            claim_ttl_seconds=0,
        )

    try:
        async with sessions() as session:
            trial_id, target = await _seed_ready_trial(session, now=now)
            trial = await session.get(Trial, trial_id)
            assert trial is not None
            task = await session.get(Task, trial.task_id)
            assert task is not None
            task.config = {
                "schema_version": "1",
                "task": {"id": task.id, "name": "Independent spool materialization"},
                "environment": {
                    "os": "linux",
                    "cpu_arch": "x86_64",
                    "gpu_vendor": "none",
                    "docker_image": plan.task_image_ref,
                    "network_policies_supported": ["gateway-only"],
                    "baseline_network_policy": {"kind": "gateway-only"},
                },
                "agent": {"name": "direct-completion", "version": "1.0"},
                "verifier": {"name": "script", "env_mode": "shared"},
                "steps": [{"name": "main", "artifacts": ["answer.txt"]}],
            }
            trial.config = {
                "schema_version": "1",
                "agent_name": "direct-completion",
                "agent_model": {"provider": "openai", "name": "gpt-5"},
            }
            if prepared_snapshot:
                task.config = {**task.config, "environment": {
                    **task.config["environment"], "docker_image": None,
                    "dockerfile": "environment/Dockerfile", "cpus": 1,
                    "memory_mb": 1024, "storage_mb": 2048, "tmpfs": ["/tmp"],
                }}
                snapshot_id = uuid4()
                snapshot = TaskImageMaterialization(
                    id=snapshot_id, materialization_key=uuid4().hex * 2,
                    task_id=task.id, task_checksum=plan.task_revision_sha256.removeprefix("sha256:"),
                    cpu_arch="x86_64", task_config=task.config, task_source=task.source,
                    task_source_provenance=task.source_provenance, state="ready",
                    registry_images={"task": plan.task_image_ref},
                )
                session.add(snapshot)
                await session.flush()
                session.add(TrialTaskImageMaterialization(trial_id=trial_id, materialization_id=snapshot_id))
                plan = plan.model_copy(update={
                    "task_image_materialization_id": snapshot_id,
                    "agent_image_ref": plan.task_image_ref,
                })
            lease = await _reserve(
                session, trial_id=trial_id, target=target, now=now, runtime_contract=plan,
            )
            if prepared_snapshot:
                # A successful claim freezes the snapshot; subsequent readiness
                # changes and Task revisions cannot change projection or repair.
                snapshot.state = "retiring"
                snapshot.registry_images = {}
                task.config = {"changed_after_lease": True}
            await enqueue_execution_transition(
                session,
                lease_id=lease.id,
                expected_generation=1,
                desired_state="start",
                now=now,
            )
            await record_execution_event(
                session,
                lease_id=lease.id,
                generation=1,
                ordinal=1,
                event_kind="kubernetes_observed",
                payload={
                    "normalized_state": "running",
                    "job_uid": "job-spool",
                    "pod_uid": "pod-spool",
                    "pod_ip": "10.24.7.21",
                    "resource_version": "1",
                },
                observed_at=now,
            )
            await session.commit()
            session.expunge(lease)

        usage = {
            "rate_card_hash": "rate-v1",
            "gateway_request_id": "gateway-spool",
            "finish_reason": "stop",
            "input_tokens": 1,
            "cached_input_tokens": 0,
            "cache_write_tokens": 0,
            "output_tokens": 1,
            "thinking_tokens": 0,
            "provider_extras": {},
            "cost_usd": 0.01,
            "duration_sec": 1.0,
            "streamed": False,
            "time_to_first_token_sec": None,
            "attempt": 1,
        }
        trace = canonical_document(
            {
                "schema_version": "loom.service-execution-llm-call.v1",
                "turn": 0,
                "started_at": now.isoformat(),
                "finished_at": (now + timedelta(seconds=1)).isoformat(),
                "model": "openai/gpt-5",
                "request": {
                    "messages": [{"role": "user", "content": "answer"}],
                    "request_params": {},
                },
                "response": {"role": "assistant", "content": "42"},
                "usage": usage,
            }
        )
        payloads = {
            "01-agent.stdout": b"42\n",
            "01-agent.stderr": b"",
            "02-verifier.stdout": b"passed\n",
            "02-verifier.stderr": b"",
            "artifacts/answer.txt": b"42\n",
            "trajectory/events.jsonl": trace,
            "accounting/usage.json": canonical_document(
                {
                    "schema_version": "loom.service-execution-usage.v1",
                    "model": "openai/gpt-5",
                    "call_count": 1,
                    "totals": {
                        key: usage[key]
                        for key in (
                            "input_tokens",
                            "cached_input_tokens",
                            "cache_write_tokens",
                            "output_tokens",
                            "thinking_tokens",
                            "cost_usd",
                            "duration_sec",
                        )
                    },
                    "calls": [usage],
                }
            ),
            "verifier/output.json": b'{"rewards":{"passed":1.0}}',
        }
        if terminus:
            from tests.unit.test_service_execution_terminus_accounting import _case

            config, _, native, ledger = _case()
            native = [event.model_copy(update={"trial_id": trial_id}) for event in native]
            payloads["trajectory/events.jsonl"] = b"\n".join(event.model_dump_json().encode() for event in native)
            retained_usage = terminus_usage(native, config)
            if usage_recovery:
                import math
                retained_usage["totals"]["cost_usd"] = math.nextafter(retained_usage["totals"]["cost_usd"], math.inf)
            payloads["accounting/usage.json"] = canonical_document(retained_usage)
            async with sessions() as session:
                trial = await session.get(Trial, trial_id)
                trial.config = config.model_dump(mode="json")
                for row in ledger:
                    session.add(LlmCall(
                        id=UUID(row["id"]), team_id=lease.team_id, trial_id=trial_id, step_id="agent",
                        model=row["model"], dialect=row["dialect"], input_tokens=row["input_tokens"],
                        output_tokens=row["output_tokens"], cost_usd=row["cost_usd"], rate_card_hash="test-rate",
                        captured_at=datetime.fromisoformat(row["captured_at"]), attempt=row["attempt"],
                        provider_extras={"_loom_raw_provider_log": {
                            "service_execution": {"lease_id": str(lease.id), "generation": 1},
                            "response": {"body": {"choices": [{"finish_reason": row["finish_reason"]}]}},
                        }},
                    ))
                await session.commit()
        if legacy_repair:
            from loom_control_plane import service_execution_materializer as materializer_module

            ledger_reader = materializer_module.read_service_execution_llm_calls

            async def legacy_without_ledger(*args, **kwargs):
                return None

            # Reproduce the deployed pre-fix projection while retaining all six
            # authoritative Gateway rows for the later correction.
            monkeypatch.setattr(materializer_module, "read_service_execution_llm_calls", legacy_without_ledger)
        result = _runtime_result_payload(lease, started_at=now)
        if typed_failure:
            payloads["diagnostics/agent-exception.json"] = canonical_document(exception)
            result["status"] = "task_error"
            result["partial_evidence"] = True
            result["phases"][0]["exit_code"] = 1
        if archival_recovery:
            payloads["diagnostics/verifier-exception.json"] = canonical_document(exception)
            payloads["verifier/output.json"] = b'{"rewards":{"passed":0.0}}'
            result["status"] = "verifier_error"
            result["partial_evidence"] = True
            result["phases"][-1]["exit_code"] = 1
        result.update(
            outputs=[
                {
                    **declaration.model_dump(mode="json"),
                    "state": "captured",
                    "size_bytes": len(payloads[declaration.relative_path]),
                    "sha256": digest_bytes(payloads[declaration.relative_path]),
                }
                for declaration in plan.output_declarations
            ],
            verifier_rewards=None if archival_recovery else {"passed": 1.0},
        )
        payloads = {"result.json": canonical_document(result), **dict(sorted(payloads.items()))}
        repository = SqlArtifactCommitRepository(
            session_factory=sessions,
            store=source_store,
            bucket="artifacts",
        )
        route = ServiceExecutionOutputRouteService(
            service=ArtifactCommitService(
                store=source_store,
                bucket="artifacts",
                repository=repository,
            ),
            session_factory=sessions,
        )
        identity = ServiceExecutionPeerV1(
            lease_id=lease.id,
            generation=1,
            execution_role="attempt",
        )
        grant = await route.prepare(
            lease=lease,
            request=ServiceExecutionOutputPrepareV1(
                schema_version="loom.service-execution-output-prepare.v1",
                request_id=uuid4(),
                **identity.model_dump(),
                files=tuple(
                    ServiceExecutionOutputFileV1(
                        relative_path=path,
                        media_type="application/octet-stream",
                        size_bytes=len(body),
                        sha256=digest_bytes(body),
                    )
                    for path, body in payloads.items()
                ),
            ),
        )
        upload_id, upload_token = UUID(grant["upload_session_id"]), str(grant["upload_token"])
        for index, payload in enumerate(payloads.values()):

            async def body(payload: bytes = payload):  # type: ignore[no-untyped-def]
                yield payload

            receipt = PartReceiptV1.model_validate(
                await route.put_part(
                    lease=lease,
                    session_id=upload_id,
                    file_index=index,
                    part_number=1,
                    content_length=len(payload),
                    content_sha256=digest_bytes(payload),
                    upload_token=upload_token,
                    body=body(),
                )
            )
            await route.complete_file(
                lease=lease,
                session_id=upload_id,
                file_index=index,
                ordered_parts=(receipt,),
                upload_token=upload_token,
            )
        await route.commit(lease=lease, session_id=upload_id, upload_token=upload_token)

        async with sessions() as session:
            current = await session.get(ServiceExecutionLease, lease.id, with_for_update=True)
            assert current is not None
            current.observed_state = "finalizing"
            assert await finalize_committed_service_execution(
                session,
                lease_id=lease.id,
                observed_at=now + timedelta(seconds=4),
            )
            # Compute deletion is already observed; storage recovery must not undo it.
            current.desired_state = current.observed_state = "deleted"
            current.cleanup_state = "complete"
            current.deleted_at = now + timedelta(seconds=5)
            await session.commit()

        source_prefix = f"service-executions/{lease.team_id}/{lease.id}/1/output/"
        source_keys = [
            obj.object_name
            for obj in spool_container.get_client().list_objects(
                "artifacts",
                prefix=source_prefix,
                recursive=True,
            )
        ]
        assert source_keys
        source_snapshot = {
            key: await source_store.get_object(bucket="artifacts", key=key) for key in source_keys
        }
        # Same bucket AND key exist in both endpoints: source cleanup must not
        # accidentally delete this object in the canonical endpoint.
        sentinel_key = source_keys[0]
        await canonical_store.put_object(
            bucket="artifacts",
            key=sentinel_key,
            body=b"canonical-endpoint-sentinel",
        )
        canonical_docker = canonical_container.get_wrapped_container()
        canonical_store = _store(canonical_container, outage=True)
        await asyncio.to_thread(canonical_docker.stop, timeout=1)
        try:
            assert await materializer().run_once()
            assert not await materializer().cleanup_source_once()
            async with sessions() as session:
                current = await session.get(ServiceExecutionLease, lease.id)
                trial = await session.get(Trial, trial_id)
                assert current is not None and trial is not None
                assert current.materialization_state == "pending"
                assert current.materialization_error_code == "transient_materialization_error"
                assert current.cleanup_state == "complete"
                assert current.deleted_at is not None
                assert trial.state == ("failed" if typed_failure or archival_recovery else "materializing")
            for key, expected in source_snapshot.items():
                assert await source_store.get_object(bucket="artifacts", key=key) == expected
        finally:
            await asyncio.to_thread(canonical_docker.start)
        await _wait_for_minio_bucket(canonical_container, "artifacts")
        # Docker may allocate a new ephemeral host port on container restart;
        # reconnect the fresh worker to that same canonical container/storage.
        canonical_store = _store(canonical_container)

        original_outcome = None
        original_execution = None
        if archival_recovery or usage_recovery:
            from loom_control_plane import service_execution_materializer as materializer_module

            original_builder = materializer_module.build_canonical_events
            error_code = "usage_output_identity_drift" if usage_recovery else "verifier_reward_drift"
            retry = (materializer().retry_usage_roundoff_archive if usage_recovery
                     else materializer().retry_legacy_verifier_archive)

            def old_reward_projection(**kwargs):
                raise MaterializationIntegrityError(error_code)

            with monkeypatch.context() as legacy:
                legacy.setattr(materializer_module, "validate_usage_accounting" if usage_recovery else "build_canonical_events", old_reward_projection)
                assert await materializer().run_once(lease_id=lease.id)
            assert materializer_module.build_canonical_events is original_builder
            async with sessions() as session:
                current = await session.get(ServiceExecutionLease, lease.id)
                trial = await session.get(Trial, trial_id)
                assert current.materialization_state == "unavailable"
                assert current.materialization_error_code == error_code
                original_outcome = copy.deepcopy((trial.state, trial.result, trial.finished_at,
                                                  trial.failure_reason, trial.failure_message, trial.attempt_count))
                original_execution = (current.desired_state, current.observed_state, current.deleted_at,
                                      current.finalized_at, current.output_manifest_sha256, current.output_marker_sha256)
            # No general reopening: the database requires the one-use audited transition.
            async with sessions() as session:
                with pytest.raises(DBAPIError, match="terminal materialization state is immutable"):
                    await session.execute(text("UPDATE execution_leases SET materialization_state='pending', "
                        "materialization_next_attempt_at=now() WHERE id=:id"), {"id": lease.id})
                await session.rollback()
            assert not await materializer().run_once(lease_id=lease.id)
            assert not await retry(lease_id=lease.id, team_id=uuid4())
            async with sessions() as session:
                histories_before = {
                    item.id: (item.snapshot_json, item.snapshot_sha256, item.changed_at)
                    for item in (await session.scalars(select(ServiceExecutionLeaseHistory).where(
                        ServiceExecutionLeaseHistory.lease_id == lease.id))).all()
                }
            if archival_history_upgrade:
                await asyncio.to_thread(command.downgrade, _config(isolated_migration_postgres_url), "0157")
            requeues = await asyncio.gather(*(
                retry(lease_id=lease.id, team_id=lease.team_id)
                for _ in range(2)
            ))
            assert sorted(requeues) == [False, True]
            async with sessions() as session:
                current = await session.get(ServiceExecutionLease, lease.id)
                assert current.materialization_recovery_requested_at is not None
                histories = (await session.scalars(select(ServiceExecutionLeaseHistory).where(
                    ServiceExecutionLeaseHistory.lease_id == lease.id))).all()
                assert {item.id: (item.snapshot_json, item.snapshot_sha256, item.changed_at)
                        for item in histories if item.id in histories_before} == histories_before
                new_history = [item for item in histories if item.id not in histories_before]
                assert len(new_history) == (0 if archival_history_upgrade else 1)
                if not archival_history_upgrade:
                    assert datetime.fromisoformat(new_history[0].snapshot_json[
                        "materialization_recovery_requested_at"
                    ]) == current.materialization_recovery_requested_at
                    assert new_history[0].snapshot_json["materialization_state"] == "pending"
                with pytest.raises(DBAPIError, match="archival recovery requires one diagnosed deleted verifier attempt"):
                    await session.execute(text("UPDATE execution_leases SET materialization_recovery_requested_at=NULL "
                        "WHERE id=:id"), {"id": lease.id})
                await session.rollback()
            assert not await retry(lease_id=lease.id, team_id=lease.team_id)

        if corrupt_recovery:
            verifier_key = next(key for key in source_keys if key.endswith("/verifier/output.json"))
            await source_store.put_object(bucket="artifacts", key=verifier_key,
                                          body=payloads["verifier/output.json"].replace(b"1.0", b"0.0") if usage_recovery else payloads["verifier/output.json"].replace(b"0.0", b"1.0"))
            assert await materializer().run_once(lease_id=lease.id)
            async with sessions() as session:
                current = await session.get(ServiceExecutionLease, lease.id)
                trial = await session.get(Trial, trial_id)
                assert current.materialization_state == "unavailable"
                assert current.materialization_error_code == "source_object_digest_mismatch"
                assert current.canonical_trajectory_sha256 is None
                assert current.source_cleanup_state == "not_ready"
                assert (trial.state, trial.result, trial.finished_at, trial.failure_reason,
                        trial.failure_message, trial.attempt_count) == original_outcome
            assert not await materializer().retry_legacy_verifier_archive(lease_id=lease.id, team_id=lease.team_id)
            assert not await materializer().cleanup_source_once()
            return

        # Crash after object copies but before DB ACK. A fresh worker reclaims the
        # expired persisted claim, re-copies idempotently, and owns the only ACK.
        interrupted = materializer()
        stale_claim = await interrupted.claim_one()
        assert stale_claim is not None
        stale_result = await interrupted._load_and_materialize(stale_claim)
        assert not await materializer().cleanup_source_once()
        async with sessions() as session:
            pending = await session.get(ServiceExecutionLease, lease.id)
            assert pending is not None and pending.materialization_state == "running"
            assert pending.canonical_trajectory_sha256 is None
        restarted = materializer()
        assert await restarted.run_once()
        assert not await interrupted._commit(stale_claim, stale_result)
        assert not await restarted.run_once()

        async with sessions() as session:
            current = await session.get(ServiceExecutionLease, lease.id)
            trial = await session.get(Trial, trial_id)
            assert current is not None and trial is not None
            assert current.materialization_state == "committed"
            assert current.materialization_attempts == 3 + archival_recovery + usage_recovery
            assert current.source_cleanup_state == "retained"
            assert trial.state == ("failed" if typed_failure or archival_recovery else "succeeded")
            if archival_recovery:
                assert (trial.state, trial.result, trial.finished_at, trial.failure_reason,
                        trial.failure_message, trial.attempt_count) == original_outcome
                assert (current.desired_state, current.observed_state, current.deleted_at,
                        current.finalized_at, current.output_manifest_sha256, current.output_marker_sha256) == original_execution
            if usage_recovery:
                assert trial.result == original_outcome[1]
                assert trial.attempt_count == original_outcome[-1] == 1
                assert trial.failure_reason is None and trial.state == "succeeded"
                assert (current.desired_state, current.observed_state, current.deleted_at,
                        current.finalized_at, current.output_manifest_sha256, current.output_marker_sha256) == original_execution
            if typed_failure:
                assert trial.result["exception_info"] == exception
                assert trial.failure_reason == "task_error"
                assert "ContextLengthExceededError" in trial.failure_message
            artifact = (
                await session.scalars(
                    select(Artifact).where(
                        Artifact.control_producer_id == lease.id,
                    )
                )
            ).one()
            if usage_recovery:
                audit = artifact.artifact_metadata["usage_roundoff_archival_recovery"]
                assert audit["error_code"] == "usage_output_identity_drift"
                assert audit["previous_trial_state"] == "failed"
                assert audit["previous_failure_reason"] == "output_unavailable"
            assert artifact.lifecycle_authority_id is not None
            files = artifact.storage["files"]
            evidence = artifact.storage["source_evidence"]
            assert {item["relative_path"] for item in files} == set(payloads) | ({"accounting/gateway-calls.json"} if terminus and not legacy_repair else set())
            assert len(evidence) == (5 if terminus and not legacy_repair else 3)
            assert trial.trajectory_index is not None
            trajectory_index = trial.trajectory_index
            events = list(
                await session.scalars(
                    select(TrialEvent).where(
                        TrialEvent.trial_id == trial_id,
                    )
                )
            )
            assert len(events) == (27 if legacy_repair else 28 if terminus else 7) + typed_failure + archival_recovery
            if typed_failure:
                assert next(event.payload for event in events if event.kind == "trial_error")["error_type"] == "ContextLengthExceededError"
            if archival_recovery:
                assert next(event.payload for event in events if event.kind == "trial_end")["reward"] is None
                assert next(event.payload for event in events if event.kind == "verifier_end")["result"]["rewards"] == {"passed": 0.0}
                assert artifact.artifact_metadata["legacy_verifier_archival_recovery"]["error_code"] == "verifier_reward_drift"
            assert len({event.seq for event in events}) == len(events)

        if archival_history_upgrade:
            async with sessions() as session:
                lease_before = await session.scalar(text(
                    "SELECT to_jsonb(e) FROM execution_leases e WHERE id=:id"), {"id": lease.id})
            await asyncio.to_thread(command.upgrade, _config(isolated_migration_postgres_url), "head")
            async with sessions() as session:
                assert await session.scalar(text(
                    "SELECT to_jsonb(e) FROM execution_leases e WHERE id=:id"), {"id": lease.id}) == lease_before
                trial = await session.get(Trial, trial_id)
                assert (trial.state, trial.result, trial.finished_at, trial.failure_reason,
                        trial.failure_message, trial.attempt_count) == original_outcome
                first_history = (await session.execute(text(
                    "SELECT to_jsonb(h) FROM execution_lease_history h WHERE lease_id=:id "
                    "ORDER BY transition_ordinal"), {"id": lease.id})).scalars().all()
            # A rollback/re-upgrade must retain the first audit and never append a duplicate.
            await asyncio.to_thread(command.downgrade, _config(isolated_migration_postgres_url), "0157")
            await asyncio.to_thread(command.upgrade, _config(isolated_migration_postgres_url), "head")
            async with sessions() as session:
                assert (await session.execute(text(
                    "SELECT to_jsonb(h) FROM execution_lease_history h WHERE lease_id=:id "
                    "ORDER BY transition_ordinal"), {"id": lease.id})).scalars().all() == first_history
            async with sessions() as session:
                current = await session.get(ServiceExecutionLease, lease.id)
                histories = (await session.scalars(select(ServiceExecutionLeaseHistory).where(
                    ServiceExecutionLeaseHistory.lease_id == lease.id))).all()
                assert {item.id: (item.snapshot_json, item.snapshot_sha256, item.changed_at)
                        for item in histories if item.id in histories_before} == histories_before
                new_history = [item for item in histories if item.id not in histories_before]
                assert len(new_history) == 1, "Upgrade must observe the already-committed recovery"
                assert datetime.fromisoformat(new_history[0].snapshot_json[
                    "materialization_recovery_requested_at"
                ]) == current.materialization_recovery_requested_at
                assert new_history[0].snapshot_json["materialization_state"] == "committed"
                assert new_history[0].changed_at >= current.materialization_committed_at

        if legacy_repair:
            from loom_control_plane.service_execution_accounting_repair import repair_accounting
            from loom_service.delivery_export import build_canonical_trial_bundle_archive
            from loom_service.trial_bundles import canonical_bundle_from_artifact

            monkeypatch.setattr(materializer_module, "read_service_execution_llm_calls", ledger_reader)
            original_objects = {
                (item["bucket"], item["key"]): await canonical_store.get_object(bucket=item["bucket"], key=item["key"])
                for item in [*files, *evidence]
            }
            for name in ("trajectory", "atif"):
                key = trajectory_index[f"{name}_uri"].removeprefix("s3://trajectories/")
                original_objects[("trajectories", key)] = await canonical_store.get_object(bucket="trajectories", key=key)
            preserved = (trial.state, trial.finished_at, trial.result, trial.attempt_count,
                         current.materialization_state, current.materialization_committed_at,
                         current.source_retain_until, current.output_manifest_sha256,
                         current.canonical_trajectory_sha256, current.canonical_atif_sha256)
            old_usage = next(item for item in files if item["relative_path"] == "accounting/usage.json")
            assert json.loads(original_objects[(old_usage["bucket"], old_usage["key"])])["call_count"] == 5
            kwargs = dict(session_factory=sessions, store=canonical_store, artifacts_bucket="artifacts",
                          trajectories_bucket="trajectories", lease_id=lease.id, team_id=lease.team_id)
            prepared = await repair_accounting(**kwargs)
            assert prepared["status"] == "prepared" and prepared["usage"]["call_count"] == 6
            assert (await repair_accounting(**kwargs, apply=True))["status"] == "corrected"
            assert (await repair_accounting(**kwargs, apply=True))["status"] == "already_corrected"
            async with sessions() as session:
                trial = await session.get(Trial, trial_id)
                current = await session.get(ServiceExecutionLease, lease.id)
                artifact = await session.get(Artifact, artifact.id)
                assert trial is not None and current is not None and artifact is not None
                assert preserved == (trial.state, trial.finished_at, trial.result, trial.attempt_count,
                                     current.materialization_state, current.materialization_committed_at,
                                     current.source_retain_until, current.output_manifest_sha256,
                         current.canonical_trajectory_sha256, current.canonical_atif_sha256)
                files, evidence = artifact.storage["files"], artifact.storage["source_evidence"]
                trajectory_index = trial.trajectory_index
                corrected_events = list((await session.scalars(select(TrialEvent).where(TrialEvent.trial_id == trial_id))).all())
                assert len(corrected_events) == 28
                assert sum(event.kind == "llm_call" for event in corrected_events) == 6
                bundle = canonical_bundle_from_artifact(artifact, trial=trial)
                assert bundle is not None
            archive = build_canonical_trial_bundle_archive(client=canonical_store._client, bundle=bundle)
            try:
                with tarfile.open(fileobj=archive.body, mode="r:gz") as tar:
                    corrected_usage = json.load(tar.extractfile("files/accounting/usage.json"))
                    assert corrected_usage["call_count"] == 6
                    assert sum(corrected_usage["totals"][key] for key in ("input_tokens", "output_tokens")) == 25381
                    assert json.load(tar.extractfile("source/accounting/usage.json"))["call_count"] == 5
            finally:
                archive.body.close()
            for (bucket, key), expected in original_objects.items():
                assert await canonical_store.get_object(bucket=bucket, key=key) == expected

        # Compute is gone and source GC is now ACK-authorized. Canonical files,
        # raw trace/accounting, source evidence, and derived ATIF remain readable.
        assert await restarted.cleanup_source_once()
        assert not await restarted.cleanup_source_once()
        assert not list(
            spool_container.get_client().list_objects(
                "artifacts",
                prefix=source_prefix,
                recursive=True,
            )
        )
        for item in [*files, *evidence]:
            downloaded = await canonical_store.get_object(bucket=item["bucket"], key=item["key"])
            assert digest_bytes(downloaded) == item["sha256"]
            assert len(downloaded) == item["size_bytes"]
            path = item["relative_path"]
            if terminus and path == "accounting/usage.json":
                usage = json.loads(downloaded)
                assert usage["call_count"] == 6
                assert usage["totals"]["input_tokens"] + usage["totals"]["output_tokens"] == 25381
            elif terminus and path == "trajectory/events.jsonl":
                assert sum(json.loads(line)["kind"] == "llm_call" for line in downloaded.splitlines()) == 6
            elif path in payloads:
                assert downloaded == payloads[path]
            elif path.startswith("source/") and path.removeprefix("source/") in payloads:
                assert downloaded == payloads[path.removeprefix("source/")]
        for name in ("trajectory", "atif"):
            key = trajectory_index[f"{name}_uri"].removeprefix("s3://trajectories/")
            downloaded = await canonical_store.get_object(bucket="trajectories", key=key)
            if name == "atif":
                document = json.loads(downloaded)
                assert document["schema_version"] == ("harbor-tb2-v2-projection" if terminus else "1.7")
                if terminus:
                    assert document["accounting"]["call_count"] == 6
                    assert len(document["steps"]) == 6
            else:
                assert b'"kind":"llm_call"' in downloaded
        assert (
            await canonical_store.get_object(
                bucket="artifacts",
                key=sentinel_key,
            )
            == b"canonical-endpoint-sentinel"
        )
        async with sessions() as session:
            current = await session.get(ServiceExecutionLease, lease.id)
            assert current is not None and current.source_cleanup_state == "complete"

        if terminus and not legacy_repair:
            from loom_service.delivery_export import build_canonical_trial_bundle_archive
            from loom_service.trial_bundles import canonical_bundle_from_artifact

            # Exercise the actual SQL selector and MinIO-backed repair after
            # source GC. The request started before materialization but its
            # immutable Gateway row commits later; timestamps cannot detect it.
            preserved = (trial.state, trial.finished_at, copy.deepcopy(trial.result), trial.attempt_count,
                         current.materialization_state, current.materialization_committed_at,
                         current.source_retain_until, current.output_manifest_sha256,
                         current.canonical_trajectory_sha256, current.canonical_atif_sha256,
                         current.source_cleanup_state, current.desired_state, current.observed_state)
            original_evidence = copy.deepcopy(evidence)
            original_index = copy.deepcopy(trajectory_index)
            old_objects = {
                (item["bucket"], item["key"]): await canonical_store.get_object(bucket=item["bucket"], key=item["key"])
                for item in [*files, *evidence]
            }
            assert not await restarted.reconcile_accounting_once()

            def late_call(*, team_id=lease.team_id, generation=1, step_id="agent"):
                return LlmCall(
                    id=uuid4(), team_id=team_id, trial_id=trial_id, step_id=step_id,
                    model=ledger[0]["model"], dialect=ledger[0]["dialect"],
                    input_tokens=999, output_tokens=8192, cost_usd=0.01,
                    rate_card_hash="test-rate", captured_at=now - timedelta(days=1), attempt=1,
                    provider_extras={"_loom_raw_provider_log": {
                        "service_execution": {"lease_id": str(lease.id), "generation": generation},
                        "response": {"body": {"choices": [{"finish_reason": "length"}]}},
                    }},
                )

            # Foreign team/generation/role rows must neither select the archive
            # for correction nor contaminate its exported accounting.
            foreign_team = uuid4()
            async with sessions() as session:
                session.add(Team(id=foreign_team, name="foreign-accounting-" + foreign_team.hex))
                await session.flush()
                excluded = [late_call(team_id=foreign_team), late_call(generation=2), late_call(step_id="verifier")]
                session.add_all(excluded)
                await session.commit()
            assert not await restarted.reconcile_accounting_once()
            included = late_call()
            async with sessions() as session:
                session.add(included)
                await session.commit()
            # The prior materialization is still committed, so its ordinary
            # pending-materialization pass cannot discover the newly bound row.
            assert not await restarted.run_once()
            async with sessions() as session:
                unchanged = await session.get(Artifact, artifact.id)
                assert unchanged.artifact_metadata["accounting_call_count"] == 6
            stop = asyncio.Event()
            loop = asyncio.create_task(run_service_execution_materializer_loop(
                materializer=restarted, interval_seconds=0.01, stop_event=stop,
            ))
            try:
                async with asyncio.timeout(10):
                    while True:
                        async with sessions() as session:
                            updated = await session.get(Artifact, artifact.id)
                            if updated.artifact_metadata["accounting_call_count"] == 7:
                                break
                        await asyncio.sleep(0.01)
            finally:
                stop.set()
                await asyncio.wait_for(loop, timeout=10)
            assert not await restarted.reconcile_accounting_once()
            async with sessions() as session:
                trial = await session.get(Trial, trial_id)
                current = await session.get(ServiceExecutionLease, lease.id)
                artifact = await session.get(Artifact, artifact.id)
                assert trial is not None and current is not None and artifact is not None
                assert preserved == (trial.state, trial.finished_at, trial.result, trial.attempt_count,
                                     current.materialization_state, current.materialization_committed_at,
                                     current.source_retain_until, current.output_manifest_sha256,
                                     current.canonical_trajectory_sha256, current.canonical_atif_sha256,
                                     current.source_cleanup_state, current.desired_state, current.observed_state)
                assert artifact.artifact_metadata["accounting_call_count"] == 7
                assert artifact.storage["source_evidence"] == original_evidence
                assert trial.trajectory_index != original_index
                corrected_events = list((await session.scalars(select(TrialEvent).where(TrialEvent.trial_id == trial_id))).all())
                assert len(corrected_events) == 29 + typed_failure + archival_recovery
                if typed_failure:
                    assert next(event.payload for event in corrected_events if event.kind == "trial_error")["error_type"] == "ContextLengthExceededError"
                assert sum(event.kind == "llm_call" for event in corrected_events) == 7
                published_index = copy.deepcopy(trial.trajectory_index)
                bundle = canonical_bundle_from_artifact(artifact, trial=trial)
                assert bundle is not None
            archive = build_canonical_trial_bundle_archive(client=canonical_store._client, bundle=bundle)
            try:
                with tarfile.open(fileobj=archive.body, mode="r:gz") as tar:
                    archived_usage = json.load(tar.extractfile("files/accounting/usage.json"))
                    assert archived_usage["call_count"] == 7
                    assert sum(archived_usage["totals"][key] for key in ("input_tokens", "output_tokens")) == 34572
                    calls = json.load(tar.extractfile("files/accounting/gateway-calls.json"))["calls"]
                    assert {row["id"] for row in calls} == {row["id"] for row in ledger} | {str(included.id)}
                    assert not any("_loom_raw_provider_log" in row["provider_extras"] for row in calls)
                    assert json.load(tar.extractfile("source/accounting/usage.json"))["call_count"] == 5
                    assert tar.extractfile("source/trajectory/events.jsonl").read() == payloads["trajectory/events.jsonl"]
                    names = tar.getnames()
                    assert len(names) == len(set(names))
                    if typed_failure:
                        assert json.load(tar.extractfile("files/diagnostics/agent-exception.json")) == exception
            finally:
                archive.body.close()
            atif_key = published_index["atif_uri"].removeprefix("s3://trajectories/")
            atif = json.loads(await canonical_store.get_object(bucket="trajectories", key=atif_key))
            assert atif["accounting"] == archived_usage
            assert len(atif["steps"]) == 6
            for (bucket, key), body in old_objects.items():
                assert await canonical_store.get_object(bucket=bucket, key=key) == body
            # A fresh process uses durable published count, not in-memory state.
            assert not await materializer().reconcile_accounting_once()
            async with sessions() as session:
                trial = await session.get(Trial, trial_id)
                assert trial.trajectory_index == published_index
    finally:
        await engine.dispose()


@pytest.mark.timeout(5)
async def test_restart_probe_does_not_follow_server_retry_after() -> None:
    requests = []

    class Handler(BaseHTTPRequestHandler):
        def do_HEAD(self):
            requests.append(self.command)
            if len(requests) <= 2:
                self.send_response(503)
                self.send_header("Retry-After", "3600")
            else:
                self.send_response(200)
            self.send_header("Content-Length", "0")
            self.end_headers()

        def log_message(self, *_args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    container = SimpleNamespace(get_config=lambda: {
        "endpoint": f"127.0.0.1:{server.server_port}",
        "access_key": "disposable", "secret_key": "disposable",
    })
    try:
        await _wait_for_minio_bucket(container, "artifacts")
        assert requests == ["HEAD", "HEAD", "HEAD"]
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)

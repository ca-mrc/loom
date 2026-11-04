"""Native samples survive owner reads, exports and exact-authority retention."""

from __future__ import annotations

import io
import tarfile
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import uuid4

import httpx
import pytest
from sqlalchemy import create_engine, delete, select, text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom.data_lifecycle_gc_sql import ExecutionMetadataPurger
from loom.db.schema import (
    Batch,
    DataLifecycleAuthority,
    ServiceExecutionClass,
    ServiceExecutionLease,
    ServiceExecutionTarget,
    Trial,
    TrialResourceUsage,
)
from loom.execution_contract import NEBIUS_CPU_EXECUTION_CLASS_V1
from loom.models.resource_usage import ResourceCounters, TrialResourceUsageReport
from loom.resource_usage_store import report_values
from loom_control_plane.service_execution import persist_execution_catalog
from loom_service.app import create_app
from loom_service.config import LoomServiceSettings
from loom_service.delivery_export import SelectedTrial, _build_archive, _resource_usage_for_selected
from loom_service.trial_bundles import ObjectRef
from tests.integration.test_service_execution_leases import _target
from tests.integration.test_trial_resource_usage import resource_seed  # noqa: F401


@pytest.fixture
async def native_usage(postgres_url: str, resource_seed: dict[str, Any]):  # noqa: F811
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    target = _target(uuid4().hex[:12])
    now = datetime.now(UTC)
    lease_id, authority_id = uuid4(), uuid4()
    async with sessions() as session, session.begin():
        created_execution_class = (
            await session.get(ServiceExecutionClass, target.execution_class_id) is None
        )
        await persist_execution_catalog(
            session, execution_class=NEBIUS_CPU_EXECUTION_CLASS_V1, targets=(target,)
        )
        session.add(
            DataLifecycleAuthority(
                id=authority_id,
                environment="staging",
                namespace=target.namespace_name,
                team_id=resource_seed["team_id"],
                data_class="event",
                owner_kind="trial",
                owner_id=str(resource_seed["trial_id"]),
                created_at=now,
                expires_at=now + timedelta(days=7),
                pinned=False,
                state="active",
            )
        )
        await session.flush()
        session.add(
            ServiceExecutionLease(
                id=lease_id,
                request_id=uuid4(),
                trial_id=resource_seed["trial_id"],
                team_id=resource_seed["team_id"],
                lifecycle_authority_id=authority_id,
                attempt=1,
                generation=1,
                resource_generation=1,
                execution_class_id=target.execution_class_id,
                target_id=target.target_id,
                routing_generation=1,
                selected_pool_id=target.logical_pool_id,
                routing_reason="admin_target_binding",
                routing_decision_sha256="sha256:" + "1" * 64,
                workload_requirements_json={},
                workload_requirements_sha256="sha256:" + "2" * 64,
                desired_state="deleted",
                observed_state="deleted",
                cleanup_state="complete",
                cleanup_requested_at=now,
                cleanup_deadline_at=now + timedelta(minutes=1),
                deleted_at=now,
                revoked_at=now,
                provider_scope_key=str(uuid4()),
                namespace_name=target.namespace_name,
                job_name="usage-" + uuid4().hex,
                execution_unit_key=uuid4(),
                pod_uid="pod-usage",
                deadline_at=now + timedelta(minutes=5),
            )
        )
        await session.flush()
        for index, (role, counters) in enumerate(
            (
                ("controller", ResourceCounters(cpu_usage_usec=10, memory_sampled_max_bytes=20)),
                ("task", ResourceCounters(cpu_usage_usec=30, memory_sampled_max_bytes=40)),
                ("pod", ResourceCounters(ephemeral_storage_sampled_max_bytes=50)),
            )
        ):
            report = TrialResourceUsageReport(
                trial_id=resource_seed["trial_id"],
                attempt_count=1,
                execution_lease_id=lease_id,
                resource_generation=1,
                target_id=target.target_id,
                pod_uid="pod-usage",
                execution_key=f"{index + 1:064x}",
                container_role=role,
                role_name=role,
                backend="nebius_kubernetes",
                source="kubelet_summary",
                observation_seq=3,
                first_observed_at=now,
                last_observed_at=now,
                finalized_at=now,
                completeness="partial",
                diagnostic_code="sampled_maxima_not_kernel_peaks",
                counters=counters,
            )
            session.add(
                TrialResourceUsage(**report_values(report, lifecycle_authority_id=authority_id))
            )
    try:
        yield {
            **resource_seed,
            "sessions": sessions,
            "lease_id": lease_id,
            "authority_id": authority_id,
        }
    finally:
        async with sessions() as session, session.begin():
            await session.execute(
                delete(TrialResourceUsage).where(TrialResourceUsage.execution_lease_id == lease_id)
            )
            await session.execute(
                delete(ServiceExecutionLease).where(ServiceExecutionLease.id == lease_id)
            )
            await session.execute(
                delete(DataLifecycleAuthority).where(DataLifecycleAuthority.id == authority_id)
            )
            await session.execute(
                delete(ServiceExecutionTarget).where(ServiceExecutionTarget.id == target.target_id)
            )
            if created_execution_class:
                await session.execute(
                    delete(ServiceExecutionClass).where(
                        ServiceExecutionClass.id == target.execution_class_id
                    )
                )
        await engine.dispose()


async def test_native_usage_owner_reads_keep_identity_and_aggregate_once(
    native_usage: dict[str, Any],
    postgres_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    for key, value in {
        "LOOM_SVC_DB_URL": postgres_url,
        "LOOM_SVC_MINIO_ENDPOINT": "http://minio:9000",
        "LOOM_SVC_MINIO_ACCESS_KEY": "x",
        "LOOM_SVC_MINIO_SECRET_KEY": "x",
        "LOOM_SVC_CONTROL_PLANE_URL": "http://cp:8080/",
        "LOOM_SVC_GATEWAY_URL": "http://gw:9100/",
    }.items():
        monkeypatch.setenv(key, value)
    app = create_app(LoomServiceSettings(_env_file=None))
    app.state.session_factory = native_usage["sessions"]
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        for kind, plural in (("trial", "trials"), ("batch", "batches")):
            path = f"/api/v1/{plural}/{native_usage[kind + '_id']}/resource-usage"
            own = await client.get(
                path, headers={"Authorization": f"Bearer {native_usage['team_token']}"}
            )
            other = await client.get(
                path, headers={"Authorization": f"Bearer {native_usage['other_token']}"}
            )
            assert own.status_code == 200
            assert other.status_code == 403
            body = own.json()
            assert len(body["items"]) == 3
            assert all(item["worker_id"] is None for item in body["items"])
            assert all(
                item["execution_lease_id"] == str(native_usage["lease_id"])
                for item in body["items"]
            )
            assert body["aggregate"]["cpu_usage_usec"] == 40
            assert body["aggregate"]["memory_sampled_max_sum_bytes"] == 60
            assert body["aggregate"]["memory_peak_upper_bound_bytes"] is None
            assert body["aggregate"]["pod_ephemeral_storage_sampled_max_sum_bytes"] == 50
            assert body["aggregate"]["telemetry_status"] == "partial"

        second_id = uuid4()
        now = datetime.now(UTC)
        try:
            async with native_usage["sessions"]() as session, session.begin():
                first_trial = await session.get(Trial, native_usage["trial_id"])
                assert first_trial is not None
                session.add(
                    Trial(
                        id=second_id,
                        team_id=first_trial.team_id,
                        task_id=first_trial.task_id,
                        batch_id=first_trial.batch_id,
                        config={},
                        requires_caps={},
                        state="running",
                        worker_id=native_usage["worker_id"],
                        attempt_count=1,
                    )
                )
                await session.flush()
                report = TrialResourceUsageReport(
                    trial_id=second_id,
                    worker_id=native_usage["worker_id"],
                    attempt_count=1,
                    execution_key="f" * 64,
                    container_role="agent",
                    role_name="primary",
                    backend="docker",
                    source="docker_stats",
                    observation_seq=1,
                    first_observed_at=now,
                    last_observed_at=now,
                    completeness="partial",
                    counters=ResourceCounters(cpu_usage_usec=0, memory_sampled_max_bytes=70),
                )
                session.add(TrialResourceUsage(**report_values(report, lifecycle_authority_id=None)))
            summary_path = f"/api/v1/batches/{native_usage['batch_id']}/resource-usage/summary"
            summary = await client.get(
                summary_path, headers={"Authorization": f"Bearer {native_usage['team_token']}"}
            )
            denied = await client.get(
                summary_path, headers={"Authorization": f"Bearer {native_usage['other_token']}"}
            )
            assert denied.status_code == 403
            assert summary.status_code == 200
            result = summary.json()
            assert datetime.fromisoformat(result.pop("last_observed_at")) == now
            assert result == {
                "cpu_usage_usec": 40,
                "largest_trial_memory_peak_bound_bytes": 70,
                "largest_trial_disk_peak_bound_bytes": 50,
                "sampled_trial_count": 2,
                "trial_count": 2,
            }
        finally:
            async with native_usage["sessions"]() as session, session.begin():
                await session.execute(delete(Trial).where(Trial.id == second_id))


async def test_native_usage_delivery_ledger_roundtrips_persisted_samples(
    native_usage: dict[str, Any],
) -> None:
    async with native_usage["sessions"]() as session:
        trial = await session.get(Trial, native_usage["trial_id"])
        batch = await session.get(Batch, native_usage["batch_id"])
        item = SelectedTrial(
            trial=trial,
            batch=batch,
            priority=0,
            selection_source="main",
            reward=1,
            trajectory=ObjectRef("trajectory", trial.id, "test", "trajectory"),
            atif=ObjectRef("atif", trial.id, "test", "atif"),
        )
        usage = await _resource_usage_for_selected(session, [item])

    class Objects:
        def get_object(self, **kwargs):
            return {"Body": io.BytesIO(b"{}\n"), "ContentLength": 3}

    archive = _build_archive(
        client=Objects(),
        manifest={},
        summary={},
        selected=[item],
        mode="lightweight",
        rows=[{"trajectory_file": "trajectory.jsonl", "atif_file": "atif.json"}],
        resource_usage_by_trial=usage,
    )
    try:
        with tarfile.open(fileobj=archive.body, mode="r:gz") as tar:
            ledger = tar.extractfile("ledger/resource_usage.jsonl")
            assert ledger is not None
            reports = [TrialResourceUsageReport.model_validate_json(line) for line in ledger]
        assert len(reports) == 3
        assert {report.container_role for report in reports} == {"controller", "task", "pod"}
        assert all(
            report.worker_id is None and report.execution_lease_id == native_usage["lease_id"]
            for report in reports
        )
        assert (
            next(
                report for report in reports if report.container_role == "pod"
            ).counters.ephemeral_storage_sampled_max_bytes
            == 50
        )
    finally:
        archive.body.close()


async def test_native_usage_gc_deletes_dependents_before_lease(
    native_usage: dict[str, Any],
    postgres_url: str,
) -> None:
    engine = create_engine(postgres_url)
    try:
        with engine.begin() as connection:
            connection.execute(
                text(
                    "CREATE TEMP TABLE gc_authority_delete_plan (authority_id uuid) ON COMMIT DROP"
                )
            )
            connection.execute(
                text("INSERT INTO gc_authority_delete_plan VALUES (:id)"),
                {"id": native_usage["authority_id"]},
            )
            ExecutionMetadataPurger().delete_exact(connection, [native_usage["authority_id"]])
            assert (
                connection.scalar(
                    select(TrialResourceUsage.id).where(
                        TrialResourceUsage.execution_lease_id == native_usage["lease_id"]
                    )
                )
                is None
            )
            assert (
                connection.scalar(
                    select(ServiceExecutionLease.id).where(
                        ServiceExecutionLease.id == native_usage["lease_id"]
                    )
                )
                is None
            )
            # Only event authority was selected; the Trial and Batch remain owned and readable.
            assert connection.scalar(select(Trial.id).where(Trial.id == native_usage["trial_id"]))
            assert connection.scalar(select(Batch.id).where(Batch.id == native_usage["batch_id"]))
    finally:
        engine.dispose()

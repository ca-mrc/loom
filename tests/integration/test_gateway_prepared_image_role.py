"""Read prepared image inputs using the actual SELECT-only Gateway login."""
from datetime import UTC, datetime
from pathlib import PurePosixPath
from uuid import uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom import nebius_platform_bootstrap as bootstrap
from loom.db.schema import (
    Task,
    TaskImageMaterialization,
    TaskImageMaterializationAttempt,
    Trial,
    TrialTaskImageMaterialization,
)
from loom.execution_contract import nebius_guest_execution_class, workload_requirements_from_task
from loom.models.compose import ComposeBuild
from loom_control_plane.service_execution_output import (
    ServiceExecutionBrokerError,
    resolve_service_execution_image_archives,
)
from tests.integration.test_nebius_platform_bootstrap import platform_database  # noqa: F401
from tests.integration.test_service_execution_leases import _reserve, _seed_ready_trial
from tests.unit.test_compose_image_inputs import prepared_plan


async def test_gateway_reads_frozen_image_receipt_without_source_admission(platform_database, monkeypatch):  # noqa: F811
    monkeypatch.setattr(bootstrap, "database_url", lambda _value, _namespace: platform_database)
    monkeypatch.setenv("LOOM_DB_URL", platform_database)
    monkeypatch.setenv("LOOM_COLLECTOR_TOKEN", "loom_ecc_" + "f" * 64)
    monkeypatch.setenv("LOOM_BATCH_RUNNER_TOKEN", "loom_br_" + "b" * 64)
    password = "restricted-role-password-" + "a" * 32
    for role in ("SERVICE", "CONTROL_PLANE", "GATEWAY", "ACTUATOR"):
        monkeypatch.setenv("LOOM_DB_" + role + "_PASSWORD", password)
    bootstrap.bootstrap_database({"namespace": "loom-nebius-platform"})
    url = make_url(platform_database).set(drivername="postgresql+psycopg")
    owner = create_async_engine(url)
    gateway = create_async_engine(url.set(username="loom_gateway", password=password))
    owners, readers = async_sessionmaker(owner, expire_on_commit=False), async_sessionmaker(gateway, expire_on_commit=False)
    prepared, trial_config, plan = prepared_plan()
    compose = prepared.environment.native_compose
    source = prepared.model_copy(update={
        "environment": prepared.environment.model_copy(update={
            "dockerfile": PurePosixPath("environment/Dockerfile"), "docker_build_context": PurePosixPath("environment"),
            "docker_image": None, "native_compose": compose.model_copy(update={"services": tuple(
                service.model_copy(update={"build": ComposeBuild(context="environment", dockerfile="Dockerfile")})
                for service in compose.services)}),
        }),
        "verifier": prepared.verifier.model_copy(update={"environment": prepared.verifier.environment.model_copy(update={
            "dockerfile": PurePosixPath("tests/Dockerfile"), "docker_build_context": PurePosixPath("tests"), "docker_image": None,
        })}),
    })
    images = {"task": prepared.environment.docker_image, "compose:database": compose.services[1].image,
              "verifier": prepared.verifier.environment.docker_image}
    archives = {component: {"image_ref": images[component], "object": {"bucket": "artifacts",
        "object_key": "prepared/" + component.replace(":", "-"), "version_id": "version-1",
        "content_sha256": "a" * 64, "size_bytes": 20}} for component in ("task", "compose:database")}
    now = datetime.now(UTC)
    try:
        async with owners.begin() as session:
            trial_id, target = await _seed_ready_trial(session, now=now, task_id=source.task.id,
                                                      execution_class=nebius_guest_execution_class())
            trial = await session.get(Trial, trial_id)
            trial.config = trial_config.model_dump(mode="json")
            catalog = await session.get(Task, source.task.id)
            catalog.config = source.model_dump(mode="json")
            catalog.checksum = plan.task_revision_sha256.removeprefix("sha256:")
            row = TaskImageMaterialization(id=plan.task_image_materialization_id, materialization_key="f" * 64,
                task_id=source.task.id, task_checksum=catalog.checksum, cpu_arch="x86_64", task_config=catalog.config,
                state="ready", lease_epoch=1, registry_images=images)
            session.add(row)
            await session.flush()
            session.add(TrialTaskImageMaterialization(trial_id=trial_id, materialization_id=row.id))
            session.add(TaskImageMaterializationAttempt(id=uuid4(), materialization_id=row.id,
                attempt_number=1, lease_epoch=1, builder_id="nebius:test", claimed_at=now,
                native_build={"image_archives": archives}))
            await session.flush()
            lease = await _reserve(session, trial_id=trial_id, target=target, now=now,
                runtime_contract=plan, requirements=workload_requirements_from_task(prepared, trial_config))
            lease_id = lease.id
        from loom.db.schema import ServiceExecutionLease
        async with readers.begin() as session:
            assert await session.scalar(text("SELECT current_user")) == "loom_gateway"
            for table in ("task_image_materializations", "task_image_materialization_attempts", "trial_task_image_materializations"):
                assert not await session.scalar(text("SELECT has_table_privilege(current_user,:table,'UPDATE')"), {"table": table})
            lease = await session.get(ServiceExecutionLease, lease_id)
            result = await resolve_service_execution_image_archives(session, lease=lease, artifacts_bucket="artifacts")
            assert set(result) == {"main", "database"}
            assert result["main"].object.version_id == "version-1"
            session.expunge(lease)
            lease.team_id = uuid4()
            with pytest.raises(ServiceExecutionBrokerError, match="grant_drift"):
                await resolve_service_execution_image_archives(session, lease=lease, artifacts_bucket="artifacts")
    finally:
        await gateway.dispose()
        await owner.dispose()

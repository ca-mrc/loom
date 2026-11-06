"""Observer samples of build Pods through the real controller and database (#2750)."""
from __future__ import annotations

import asyncio
import time
from datetime import UTC, datetime, timedelta

from sqlalchemy import select

from loom.db.schema import TaskImageBuildResourceUsage
from loom_execution_actuator.task_image_build_usage import (
    BuildSample,
    BuildUsageSampler,
    record_build_sample,
)
from tests.integration.test_nebius_task_image_claims import claim_setup  # noqa: F401
from tests.integration.test_nebius_task_image_controller import (  # noqa: F401
    controller_setup,
    rows,
    seed_image,
)

NOW = datetime(2026, 11, 5, 20, 0, tzinfo=UTC)


def _sample(at, storage=4096, codes=(), memory=100):
    return BuildSample(snapshot_at=at, storage_bytes=storage, unknown_codes=codes,
                       containers={"build": {"cpu_nanocores": 5, "memory_bytes": memory, "filesystem_bytes": None}})


def _schedule(kube):
    """The created build Job's Pod is scheduled and its build container is running."""
    job = next(iter(kube.jobs.values()))
    job["pods"] = [{"metadata": {"uid": "pod-uid", "name": "pod", "ownerReferences": [
        {"kind": "Job", "uid": job["metadata"]["uid"]}]},
        "spec": {"nodeName": "node-a", "initContainers": job["spec"]["template"]["spec"]["initContainers"]},
        "status": {"initContainerStatuses": [{"name": "build", "state": {"running": {"startedAt": "2026-11-05T20:00:00Z"}}}]}}]


async def _usage(sessions):
    async with sessions() as session:
        return (await session.scalars(select(TaskImageBuildResourceUsage))).all()


async def _attempt(controller_setup):  # noqa: F811
    controller, sessions, team_id, _ = controller_setup
    image_id, _ = await seed_image(sessions, team_id)
    await controller.run_once()
    _, attempts = await rows(sessions, image_id)
    return image_id, attempts[0]


async def test_samples_are_deduplicated_unknown_is_counted_and_maxima_only_grow(controller_setup):  # noqa: F811
    _, sessions, _, _ = controller_setup
    _, attempt = await _attempt(controller_setup)

    async def record(observation, *, epoch=None, node="node-a"):
        async with sessions() as session, session.begin():
            await record_build_sample(session, attempt_id=attempt.id, materialization_id=attempt.materialization_id,
                                      lease_epoch=epoch or attempt.lease_epoch, pod_uid="pod-uid", node_name=node,
                                      observation=observation, now=NOW)

    await record(_sample(NOW))
    await record(_sample(NOW))  # same producer snapshot: counted once
    await record(_sample(NOW - timedelta(seconds=15), storage=10**9))  # older snapshot: ignored
    await record(_sample(NOW + timedelta(seconds=15), storage=None, codes=("filesystem_limit",), memory=50))
    await record(_sample(NOW + timedelta(seconds=30), storage=8192, memory=900))
    await record(_sample(NOW + timedelta(seconds=45), storage=2048))  # lower: max unchanged
    await record("pod_not_in_snapshot")
    await record(_sample(NOW + timedelta(seconds=60), storage=10**12), epoch=attempt.lease_epoch + 1)  # other epoch
    await record(_sample(NOW + timedelta(seconds=60), storage=10**12), node="node-b")  # other placement
    (row,) = await _usage(sessions)
    assert (row.snapshots, row.storage_complete, row.storage_unknown, row.unavailable) == (4, 3, 1, 1)
    assert row.unknown_codes == {"filesystem_limit": 1} and row.last_unavailable_code == "pod_not_in_snapshot"
    assert row.ephemeral_storage_max_bytes == 8192 and row.ephemeral_storage_max_at == NOW + timedelta(seconds=30)
    assert row.containers == {"build": {"samples": 4, "cpu_max_nanocores": 5, "memory_max_bytes": 900}}
    assert row.source == "native_node_observer" and row.lease_epoch == attempt.lease_epoch


async def test_concurrent_first_samples_create_one_row(controller_setup):  # noqa: F811
    _, sessions, _, _ = controller_setup
    _, attempt = await _attempt(controller_setup)

    async def record(at):
        async with sessions() as session, session.begin():
            await record_build_sample(session, attempt_id=attempt.id, materialization_id=attempt.materialization_id,
                                      lease_epoch=attempt.lease_epoch, pod_uid="pod-uid", node_name="node-a",
                                      observation=_sample(at), now=NOW)

    await asyncio.gather(record(NOW), record(NOW + timedelta(seconds=15)))
    (row,) = await _usage(sessions)
    assert row.snapshots in (1, 2) and row.storage_complete == row.snapshots


async def _drive(controller_setup, reader):  # noqa: F811
    controller, sessions, team_id, kube = controller_setup
    controller.build_usage = BuildUsageSampler(sessions=sessions, namespace="test-builds", reader=reader)
    image_id, _ = await seed_image(sessions, team_id)
    await controller.run_once()
    _schedule(kube)
    started = time.monotonic()
    await controller.run_once()
    elapsed = time.monotonic() - started
    if controller._sampling is not None:
        await asyncio.wait({controller._sampling}, timeout=10)  # telemetry finishes (or is bounded) on its own
    kube.finish()
    await controller.run_once()
    await controller.run_once()
    row, attempts = await rows(sessions, image_id)
    usage = await _usage(sessions)
    await controller.close()
    return row, attempts[0], usage, elapsed


async def test_controller_samples_a_running_build_without_changing_its_lifecycle(controller_setup):  # noqa: F811
    async def reader(node_name):
        assert node_name == "node-a"
        return {"pods": [{"podRef": {"uid": "pod-uid", "namespace": "test-builds", "name": "pod"},
                          "ephemeral-storage": {"usedBytes": 4096},
                          "containers": [{"name": "build", "memory": {"usageBytes": 77}}]}],
                "_native_observation": {"observed_at": datetime.now(UTC).isoformat(), "max_age_seconds": 60}}

    row, attempt, usage, _ = await _drive(controller_setup, reader)
    assert row.state == "ready" and attempt.native_build["capacity_released_at"]
    (sample,) = usage
    assert sample.attempt_id == attempt.id and sample.pod_uid == "pod-uid" and sample.node_name == "node-a"
    assert sample.ephemeral_storage_max_bytes == 4096 and sample.containers["build"]["memory_max_bytes"] == 77
    assert "boundary_usage" not in attempt.native_build and "observer_usage" not in attempt.native_build


async def test_reader_failures_are_recorded_as_unavailable_and_never_block_the_build(controller_setup):  # noqa: F811
    async def reader(node_name):
        raise TimeoutError("private transport detail")

    row, attempt, usage, _ = await _drive(controller_setup, reader)
    assert row.state == "ready" and attempt.native_build["capacity_released_at"]
    (sample,) = usage
    assert sample.snapshots == 0 and sample.unavailable >= 1 and sample.ephemeral_storage_max_bytes is None
    assert "private" not in (sample.last_unavailable_code or "")


async def test_a_hanging_reader_is_bounded_and_the_build_still_completes(controller_setup):  # noqa: F811
    async def reader(node_name):
        await asyncio.sleep(3600)

    row, attempt, usage, elapsed = await _drive(controller_setup, reader)
    assert row.state == "ready" and attempt.native_build["capacity_released_at"]
    assert elapsed < 2  # a pass never awaits telemetry
    # The 5 s per-sample bound cancels the stuck read before any write: nothing is recorded as usage.
    assert all(sample.snapshots == 0 and sample.ephemeral_storage_max_bytes is None for sample in usage)


async def test_report_export_joins_observer_samples_from_the_real_schema(controller_setup):  # noqa: F811
    import json

    from scripts import task_image_boundary_usage_report as report_module
    from sqlalchemy import text

    async def reader(node_name):
        return {"pods": [{"podRef": {"uid": "pod-uid", "namespace": "test-builds", "name": "pod"},
                          "ephemeral-storage": {"usedBytes": 4096},
                          "containers": [{"name": "build", "memory": {"usageBytes": 77}}]}],
                "_native_observation": {"observed_at": datetime.now(UTC).isoformat(), "max_age_seconds": 60}}

    _, attempt, usage, _ = await _drive(controller_setup, reader)
    assert usage
    _, sessions, _, _ = controller_setup
    async with sessions() as session:
        exported = [value for (value,) in (await session.execute(
            text(report_module.QUERY), {"since": datetime.now(UTC) - timedelta(hours=1)})).all()
            if value["attempt_id"] == str(attempt.id)]
    (export,) = exported
    assert set(export) == report_module.EXPORT_FIELDS
    (sample,) = export["observer_usage"]
    assert sample["pod_uid"] == "pod-uid" and sample["ephemeral_storage_max_bytes"] == 4096
    result = report_module.report([json.dumps(export, default=str)])
    assert result["summary"]["observer"]["storage_complete"] >= 1
    assert result["attempts"][0]["observer"][0]["containers"]["build"]["memory_max_bytes"] == 77

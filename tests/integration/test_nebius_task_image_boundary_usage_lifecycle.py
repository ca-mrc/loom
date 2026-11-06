"""Build readings through the real controller lifecycle and database (#2750).

A reading is telemetry: valid, malformed and unexpectedly failing messages must
all leave the build ready and its capacity released, with the outcome recorded.
"""
from __future__ import annotations

import json

import pytest

from tests.integration.test_nebius_task_image_claims import claim_setup  # noqa: F401
from tests.integration.test_nebius_task_image_controller import (  # noqa: F401
    controller_setup,
    rows,
    seed_image,
)

_VALID = {"v": 1, "point": "solve_end", "component": 0, "at": 1_793_900_000, "status": "ok", "elapsed_ms": 20,
          "dirs": {"scratch": {"entries": 32, "kib": 5132, "errors": 0, "rc": 0},
                   "build": {"entries": 4, "kib": 16, "errors": 0, "rc": 0}}}


def _instrument(kube, message):
    """Give the finished fake Pod the rendered spec and a build termination message."""
    job = next(iter(kube.jobs.values()))
    pod = job["pods"][0]
    pod["spec"] = {"initContainers": job["spec"]["template"]["spec"]["initContainers"]}
    for status in pod["status"]["initContainerStatuses"]:
        if status["name"] == "build":
            status["state"]["terminated"]["message"] = message


@pytest.mark.parametrize(("message", "explode", "status", "records"), [
    (json.dumps(_VALID), False, "complete", 1),
    (json.dumps({**_VALID, "point": []}), False, "rejected", 0),
    ("[" * 300, False, "rejected", 0),
    (json.dumps(_VALID), True, "rejected", 0),
])
async def test_reading_outcome_never_blocks_ready_or_release(controller_setup, monkeypatch,  # noqa: F811
                                                             message, explode, status, records):
    controller, sessions, team_id, kube = controller_setup
    controller.settings = controller.settings.model_copy(update={"record_boundary_usage": True})
    if explode:
        def fail(message):
            raise RuntimeError("unexpected parser failure")

        monkeypatch.setattr("loom_execution_actuator.task_image_boundary_usage.parse_boundary_usage", fail)
    image_id, _ = await seed_image(sessions, team_id)
    await controller.run_once()
    job = next(iter(kube.jobs.values()))
    assert "loom_boundary_usage() {" in job["spec"]["template"]["spec"]["initContainers"][1]["command"][-1]
    kube.finish()
    _instrument(kube, message)
    await controller.run_once()
    row, attempts = await rows(sessions, image_id)
    assert row.state == "ready" and "task" in row.registry_images
    await controller.run_once()
    row, attempts = await rows(sessions, image_id)
    native = attempts[0].native_build
    assert native["capacity_released_at"]
    usage = native["boundary_usage"]
    assert usage["source"] == "build_phase_self_report" and usage["status"] == status
    assert len(usage["records"]) == records
    if records:
        assert usage["records"][0]["dirs"]["scratch"]["kib"] == 5132


async def test_uninstrumented_builds_store_no_reading(controller_setup):  # noqa: F811
    controller, sessions, team_id, kube = controller_setup
    image_id, _ = await seed_image(sessions, team_id)
    await controller.run_once()
    kube.finish()
    _instrument(kube, json.dumps(_VALID))
    await controller.run_once()
    await controller.run_once()
    row, attempts = await rows(sessions, image_id)
    assert row.state == "ready" and attempts[0].native_build["capacity_released_at"]
    assert "boundary_usage" not in attempts[0].native_build


@pytest.mark.parametrize("instrument", [True, False])
async def test_report_export_is_the_minimal_projection_of_real_attempts(controller_setup, instrument):  # noqa: F811
    import hashlib
    from datetime import UTC, datetime, timedelta

    from scripts import task_image_boundary_usage_report as report_module
    from sqlalchemy import text

    from loom.db.schema import TaskImageMaterialization

    controller, sessions, team_id, kube = controller_setup
    if instrument:
        controller.settings = controller.settings.model_copy(update={"record_boundary_usage": True})
    image_id, _ = await seed_image(sessions, team_id)
    await controller.run_once()
    kube.finish()
    _instrument(kube, json.dumps(_VALID))
    await controller.run_once()
    row, attempts = await rows(sessions, image_id)
    assert row.state == "ready"
    stored = json.dumps(attempts[0].native_build)
    claim = attempts[0].native_build["configmap"]["data"]["claim.json"]
    async with sessions() as session:
        task_id = (await session.get(TaskImageMaterialization, image_id)).task_id
        exported = [value for (value,) in (await session.execute(
            text(report_module.QUERY), {"since": datetime.now(UTC) - timedelta(hours=1)})).all()
            if value["attempt_id"] == str(attempts[0].id)]
    (export,) = exported
    assert set(export) == report_module.EXPORT_FIELDS
    identifiers = {str(export[key]) for key in ("attempt_id", "materialization_id", "task_checksum", "target_id",
                                                "pod_uid", "cpu_arch") if export.get(key) is not None}
    claim_values = [value for value in json.loads(claim).values()
                    if isinstance(value, str) and len(value) > 6 and value not in identifiers]
    assert claim_values  # the stored claim carries repository, bucket and source values
    text_export = json.dumps(export)
    # The attempt does store all of these; the export must not carry any of them.
    for private in ("claim.json", "configmap", "registry_repository", "secretName", "loom_task_image_stage",
                    "command", task_id, *claim_values):
        assert private == task_id or private in stored or private in claim
        assert private not in text_export
    assert export["task_digest"] == hashlib.sha256(task_id.encode()).hexdigest()[:12]
    assert export["instrumented"] is instrument and export["build_exit_code"] == 0
    assert set(export["volume_size_limits"]) == {"build", "builder-tmp", "prepare-tmp", "publish-tmp"}
    assert export["init_storage_requests"] and export["container_storage_requests"]
    result = report_module.report([json.dumps(export, default=str)])
    (attempt,) = result["attempts"]
    assert attempt["usage_status"] == ("complete" if instrument else "not_instrumented")
    assert attempt["effective_pod_request_kib"] == controller.settings.ephemeral_storage_mib * 1024

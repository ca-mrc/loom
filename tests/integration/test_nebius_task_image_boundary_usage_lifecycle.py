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
    (json.dumps(_VALID), False, "recorded", 1),
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

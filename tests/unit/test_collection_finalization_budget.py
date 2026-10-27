from datetime import UTC, datetime

import pytest

from loom.execution_runtime_contract import ProcessPhaseV1
from loom_control_plane.service_execution_scheduler import _deadline


def test_collection_budget_extends_lease_without_moving_agent_cutoff():
    from tests.unit.test_compose_image_inputs import prepared_plan

    _, _, plan = prepared_plan()
    old = _deadline(plan, now=datetime(2026, 1, 1, tzinfo=UTC), maximum_seconds=100000)
    main = plan.main.model_copy(update={"finalization_timeout_seconds": 420})
    extended = plan.model_copy(update={"main": main})
    new = _deadline(extended, now=datetime(2026, 1, 1, tzinfo=UTC), maximum_seconds=100000)
    assert (new - old).total_seconds() == 420 - plan.main.finalization_timeout_seconds
    assert extended.main.timeout_seconds == plan.main.timeout_seconds


def test_collection_budget_is_agent_only_bounded_and_legacy_serialization_unchanged():
    value = dict(role="agent", argv=("true",), working_directory="/app", timeout_seconds=60)
    assert "finalization_timeout_seconds" not in ProcessPhaseV1(**value).model_dump()
    for override in ({"role": "verifier", "finalization_timeout_seconds": 1},
                     {"finalization_timeout_seconds": 3601}):
        with pytest.raises(ValueError):
            ProcessPhaseV1(**{**value, **override})


@pytest.mark.parametrize("compose", [False, True])
def test_native_artifact_handoff_does_not_extend_deferred_verifier_budget(compose):
    from loom.execution_runtime_contract import RuntimeHandoffInputV1
    from loom.harbor_verifier_script import VERIFIER_SCRIPT_PATH
    from loom.hosted_harness import hosted_harness
    from loom.models.harbor import ArtifactSource, HarborConversion
    from loom.service_execution_materialization import service_execution_input_binding
    from loom.task_sandbox_planner import (
        TaskSandboxPlanRequest,
        compile_deferred_verifier_plan,
        compile_task_sandbox_plan,
    )
    from tests.unit.test_compose_image_inputs import prepared_plan
    from tests.unit.test_guest_execution_materialization import _guest_inputs
    from tests.unit.test_independent_verifier_environment import _independent_plan
    from tests.unit.test_service_execution_materialization import _REVISION, _provenance

    if compose:
        task, trial, previous = prepared_plan(native=True)
    else:
        _, task, trial, previous = _independent_plan()
    trial = trial.model_copy(update={"agent_name": "oracle", "agent_model": None})
    task = task.model_copy(update={
        "verifier": task.verifier.model_copy(update={"args": {"script_path": VERIFIER_SCRIPT_PATH}}),
        "upstream_conversion": HarborConversion(changes=()),
        "steps": [task.steps[0].model_copy(update={
            "artifact_sources": (ArtifactSource(source="/app/answer.txt"),),
        })],
    })
    _, _, profile = _guest_inputs("nested_docker")
    agent = compile_task_sandbox_plan(TaskSandboxPlanRequest(
        spec=hosted_harness("oracle"), task=task, trial=trial, profile=profile,
        binding=service_execution_input_binding(_provenance()), task_revision_sha256=_REVISION,
        command_identity="sha256:" + "4" * 64, output_paths=(),
        effective_network_policy=task.environment.baseline_network_policy,
        controller_image=profile.agent_image_ref,
        task_image_materialization_id=previous.task_image_materialization_id,
        has_resolved_task_environment=True,
    ))
    assert agent.main.finalization_timeout_seconds == 120
    cutoff = agent.main.timeout_seconds
    verifier_seconds = round(task.verifier.timeout_sec * trial.verifier_timeout_multiplier)
    verifier = compile_deferred_verifier_plan(agent, task, verifier_timeout_seconds=verifier_seconds,
        handoff_input=RuntimeHandoffInputV1(manifest_sha256="sha256:" + "a" * 64, file_count=1, total_bytes=1))
    assert verifier.main.role == "verifier"
    assert verifier.main.finalization_timeout_seconds == 0
    assert verifier.main.timeout_seconds == verifier_seconds
    assert agent.main.timeout_seconds == cutoff
    assert agent.main.finalization_timeout_seconds == 120

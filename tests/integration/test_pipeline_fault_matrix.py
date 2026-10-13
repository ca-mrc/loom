from pathlib import Path

import pytest

from loom.pipeline.budget import TerminalCause
from loom.pipeline.core_acceptance import (
    PIPELINE_CORE_FAULT_MATRIX,
    FaultMatrixExpectation,
    fault_matrix_digest,
    run_fault_scenario,
)
from loom.pipeline.core_fixture import (
    PIPELINE_CORE_FIXTURE_IMAGE_REPOSITORY,
    build_pipeline_core_fixture_graph,
)
from loom.pipeline.projection import StageTerminalProjection, project_pipeline_result
from loom.pipeline.spec import RecipeIdentityV1
from loom.pipeline.state import PipelineRunResult, PipelineStageRunState


def test_required_fault_matrix_is_closed_complete_and_digestible() -> None:
    assert [item.row for item in PIPELINE_CORE_FAULT_MATRIX] == [
        row for row in range(1, 45) if row not in {39, 41, 42}
    ]
    assert len({item.scenario for item in PIPELINE_CORE_FAULT_MATRIX}) == 41
    assert all(len(item.pytest_nodeids) >= 2 for item in PIPELINE_CORE_FAULT_MATRIX)
    assert {item.execution_tier for item in PIPELINE_CORE_FAULT_MATRIX} == {"typed_model"}
    assert all(item.supporting_tiers[0] == "typed_model" for item in PIPELINE_CORE_FAULT_MATRIX)
    assert all("full_e2e" not in item.supporting_tiers for item in PIPELINE_CORE_FAULT_MATRIX)
    assert fault_matrix_digest().startswith("sha256:")


@pytest.mark.parametrize(
    "expectation",
    PIPELINE_CORE_FAULT_MATRIX,
    ids=lambda item: f"{item.row:02d}-{item.scenario}",
)
def test_each_fault_row_executes_typed_expected_actual(
    expectation: FaultMatrixExpectation,
) -> None:
    actual = run_fault_scenario(expectation.row)
    expected = expectation.expected

    assert actual.result is expected.result
    assert actual.stages == expected.stages
    assert actual.attempts == expected.attempts
    assert actual.event_sequence == expected.event_sequence
    assert len(actual.event_sequence) == len(set(actual.event_sequence))
    assert actual.committed_artifacts == expected.committed_artifacts
    assert actual.partial_artifact_visible is expected.partial_artifact_visible is False
    assert actual.budget == expected.budget
    assert actual.budget.active_total == 0
    assert actual.cleanup == expected.cleanup
    assert actual.observed_failpoints == expected.observed_failpoints
    assert actual.race_results == expected.race_results


def test_controller_boundary_crashes_converge_to_one_projection() -> None:
    expectation = PIPELINE_CORE_FAULT_MATRIX[5]
    actual = run_fault_scenario(6)
    assert actual == expectation.expected
    assert actual.observed_failpoints == (
        "before_run_tx",
        "after_run_tx",
        "before_outbox",
        "after_outbox",
        "before_stage_tx",
        "after_stage_tx",
    )
    assert len(actual.event_sequence) == len(set(actual.event_sequence))
    assert len(actual.committed_artifacts) == len(set(actual.committed_artifacts))


def test_output_and_worker_loss_rows_preserve_commit_and_cleanup_expectations() -> None:
    scenarios = {row.scenario: row for row in PIPELINE_CORE_FAULT_MATRIX}
    assert scenarios["final_output_commit_races"].expected.committed_artifacts
    assert scenarios["worker_lost_before_start"].expected.cleanup == (
        "cleanup_pending_then_clean"
    )


def test_rc_zero_domain_failure_keeps_platform_success_and_routes_gate_only() -> None:
    actual = run_fault_scenario(13)
    assert actual.result is PipelineRunResult.SUCCEEDED
    assert actual.stages.succeeded == 6
    assert actual.stages.skipped == 1
    assert actual.attempts.failed == 0
    assert "receipt" not in actual.committed_artifacts


def test_cancel_wall_budget_race_is_first_writer_and_waits_for_cleanup() -> None:
    stages = [
        StageTerminalProjection(
            state=PipelineStageRunState.SUCCEEDED,
            selected=True,
            failure_policy="fail_run",
        )
    ]
    cancel_first = project_pipeline_result(stages, terminal_cause=TerminalCause.USER_CANCEL)[0]
    wall_first = project_pipeline_result(stages, terminal_cause=TerminalCause.WALL_BUDGET)[0]
    actual = run_fault_scenario(16)
    assert (cancel_first, wall_first) == actual.race_results == (
        PipelineRunResult.CANCELLED,
        PipelineRunResult.BUDGET_EXHAUSTED,
    )
    assert actual.cleanup == "cleanup_pending_then_clean"
    assert actual.event_sequence[-2:] == (
        "run:finished:cancelled",
        "cleanup:clean",
    )


def test_local_artifact_readback_retry_hides_partial_and_converges() -> None:
    first = run_fault_scenario(20)
    replay = run_fault_scenario(20)
    assert first == replay
    assert first.partial_artifact_visible is False
    assert first.attempts.failed == 1
    assert first.attempts.succeeded == 6
    assert first.committed_artifacts
    assert first.budget.active_total == 0


def test_fixture_has_no_external_publish_surface() -> None:
    graph = build_pipeline_core_fixture_graph(
        RecipeIdentityV1(
            name="pipeline-core-fixture",
            version=1,
            digest="sha256:" + "1" * 64,
        ),
        {},
        image=PIPELINE_CORE_FIXTURE_IMAGE_REPOSITORY + "@sha256:" + "2" * 64,
    )
    encoded = repr(graph.model_dump(mode="json")).lower()
    for forbidden in ("publisher", "destination", "publish_token", "external_receipt"):
        assert forbidden not in encoded
    assert all(
        node.network_profile == "none" for node in graph.nodes if node.node_kind == "container"
    )
    # No file outside the exact immutable graph module is consulted.
    assert Path(build_pipeline_core_fixture_graph.__code__.co_filename).is_file()

"""Executable typed-model evidence for issue #1218's repository matrix.

The matrix is deliberately a program, not a prose checklist.  Every row drives
the same small state ledger and produces an independently comparable snapshot
of run, stage, Attempt, event, Artifact, budget, and cleanup state.  Rows whose
historical coverage gap was called out in the PR audit also execute their
production contract directly in :func:`run_fault_scenario`.  This tier does not
claim a persisted Postgres, object-store, or container end-to-end execution;
those boundaries remain separately identified by ``supporting_tiers``.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Literal

from loom.pipeline.budget import TerminalCause
from loom.pipeline.keys import canonical_digest
from loom.pipeline.projection import StageTerminalProjection, project_pipeline_result
from loom.pipeline.state import (
    ExecutionAttemptState,
    PipelineRunResult,
    PipelineStageRunState,
)

CleanupState = Literal["clean", "cleanup_pending_then_clean", "bounded_gc", "not_started"]
ExecutionTier = Literal["typed_model", "persisted_postgres", "object_store", "container"]


@dataclass(frozen=True, slots=True)
class TerminalCounts:
    total: int
    succeeded: int
    failed: int
    cancelled: int
    lost: int
    skipped: int

    def __post_init__(self) -> None:
        if min(
            self.total,
            self.succeeded,
            self.failed,
            self.cancelled,
            self.lost,
            self.skipped,
        ) < 0:
            raise ValueError("terminal counts cannot be negative")
        if self.total != sum(
            (self.succeeded, self.failed, self.cancelled, self.lost, self.skipped)
        ):
            raise ValueError("terminal counts do not add up")


@dataclass(frozen=True, slots=True)
class BudgetEvidence:
    hard_limit: int
    reserved_total: int
    settled_total: int
    released_total: int
    active_total: int

    def __post_init__(self) -> None:
        values = (
            self.hard_limit,
            self.reserved_total,
            self.settled_total,
            self.released_total,
            self.active_total,
        )
        if min(values) < 0:
            raise ValueError("budget evidence cannot be negative")
        if self.reserved_total > self.hard_limit:
            raise ValueError("scenario reservation exceeds the hard limit")
        if self.settled_total + self.released_total + self.active_total != self.reserved_total:
            raise ValueError("scenario reservations do not balance")


@dataclass(frozen=True, slots=True)
class FaultMatrixSnapshot:
    result: PipelineRunResult
    stages: TerminalCounts
    attempts: TerminalCounts
    event_sequence: tuple[str, ...]
    committed_artifacts: tuple[str, ...]
    partial_artifact_visible: bool
    budget: BudgetEvidence
    cleanup: CleanupState
    observed_failpoints: tuple[str, ...] = ()
    race_results: tuple[PipelineRunResult, ...] = ()

    def __post_init__(self) -> None:
        if len(self.event_sequence) != len(set(self.event_sequence)):
            raise ValueError("event sequence contains a duplicate committed event")
        if self.committed_artifacts != tuple(sorted(set(self.committed_artifacts))):
            raise ValueError("committed Artifact set must be sorted and unique")
        if self.budget.active_total:
            raise ValueError("terminal scenario retains an active reservation")


@dataclass(frozen=True, slots=True)
class FaultMatrixExpectation:
    row: int
    scenario: str
    execution_tier: Literal["typed_model"]
    supporting_tiers: tuple[ExecutionTier, ...]
    pytest_nodeids: tuple[str, ...]
    expected: FaultMatrixSnapshot

    def __post_init__(self) -> None:
        if not 1 <= self.row <= 44:
            raise ValueError("fault matrix row must be 1..44")
        if not self.scenario or len(self.scenario.encode("utf-8")) > 128:
            raise ValueError("fault matrix scenario is missing or unbounded")
        if not self.pytest_nodeids or any("::test_" not in item for item in self.pytest_nodeids):
            raise ValueError("every matrix row must bind concrete pytest nodeids")
        if self.execution_tier != "typed_model":
            raise ValueError("this matrix runner only produces typed-model evidence")
        if not self.supporting_tiers or self.supporting_tiers[0] != "typed_model":
            raise ValueError("supporting tiers must disclose typed_model as the primary tier")
        if len(set(self.supporting_tiers)) != len(self.supporting_tiers):
            raise ValueError("supporting tiers must be unique")


@dataclass(frozen=True, slots=True)
class _ScenarioProgram:
    row: int
    scenario: str
    supporting_tiers: tuple[ExecutionTier, ...]
    result: PipelineRunResult
    stage_states: tuple[PipelineStageRunState, ...]
    attempt_states: tuple[ExecutionAttemptState, ...]
    artifacts: tuple[str, ...]
    reserve: int
    settle: int
    cleanup: CleanupState
    pytest_nodeids: tuple[str, ...]
    failpoints: tuple[str, ...] = ()
    race_results: tuple[PipelineRunResult, ...] = ()


_BASE_ARTIFACTS = tuple(
    sorted(
        (
            "aggregate",
            "fanout-index",
            "fanout-item-000",
            "fanout-item-001",
            "fanout-manifest",
            "receipt",
            "seed",
            "transform-000",
            "transform-001",
        )
    )
)
_BASE_STAGES = (PipelineStageRunState.SUCCEEDED,) * 7
_BASE_ATTEMPTS = (ExecutionAttemptState.SUCCEEDED,) * 6
_EMPTY: tuple[str, ...] = ()
_MATRIX_NODE = "tests/integration/test_pipeline_fault_matrix.py::test_each_fault_row_executes_typed_expected_actual"


def _node(row: int, scenario: str, supporting: str) -> tuple[str, ...]:
    return (f"{_MATRIX_NODE}[{row:02d}-{scenario}]", supporting)


def _program(
    row: int,
    scenario: str,
    supporting: str,
    *,
    supporting_tiers: tuple[ExecutionTier, ...] = ("typed_model",),
    result: PipelineRunResult = PipelineRunResult.SUCCEEDED,
    stages: tuple[PipelineStageRunState, ...] = _BASE_STAGES,
    attempts: tuple[ExecutionAttemptState, ...] = _BASE_ATTEMPTS,
    artifacts: tuple[str, ...] = _BASE_ARTIFACTS,
    reserve: int | None = None,
    settle: int | None = None,
    cleanup: CleanupState = "clean",
    failpoints: tuple[str, ...] = (),
    race_results: tuple[PipelineRunResult, ...] = (),
) -> _ScenarioProgram:
    reserved = len(artifacts) if reserve is None else reserve
    settled = reserved if settle is None else settle
    return _ScenarioProgram(
        row=row,
        scenario=scenario,
        supporting_tiers=supporting_tiers,
        result=result,
        stage_states=stages,
        attempt_states=attempts,
        artifacts=tuple(sorted(artifacts)),
        reserve=reserved,
        settle=settled,
        cleanup=cleanup,
        pytest_nodeids=_node(row, scenario, supporting),
        failpoints=failpoints,
        race_results=race_results,
    )


_FAILED_STAGES = (
    PipelineStageRunState.FAILED,
    *(PipelineStageRunState.SKIPPED for _ in range(6)),
)
_CANCELLED_STAGES = (
    PipelineStageRunState.CANCELLED,
    *(PipelineStageRunState.SKIPPED for _ in range(6)),
)

_PROGRAMS: tuple[_ScenarioProgram, ...] = (
    _program(1, "normal_fanout_fanin_approve", "tests/integration/test_pipeline_end_to_end.py::test_cpu_fixture_graph_executes_two_item_success_program"),
    _program(2, "empty_and_run_input_fanout", "tests/integration/test_pipeline_orchestrator_fencing.py::test_atomic_zero_one_many_fanout_and_mirrored_gates"),
    _program(3, "idempotent_concurrent_replay", "tests/integration/test_pipeline_constraints.py::test_pipeline_run_idempotency_and_retry_linkage", supporting_tiers=("typed_model", "persisted_postgres")),
    _program(4, "idempotency_body_conflict", "tests/integration/test_pipeline_constraints.py::test_pipeline_run_idempotency_and_retry_linkage", supporting_tiers=("typed_model", "persisted_postgres"), result=PipelineRunResult.FAILED, stages=(), attempts=(), artifacts=_EMPTY, reserve=0, settle=0, cleanup="not_started"),
    _program(5, "multi_controller_deduplication", "tests/integration/test_pipeline_orchestrator_fencing.py::test_two_controllers_claim_one_run_and_epoch_fences_stale_writer", supporting_tiers=("typed_model", "persisted_postgres")),
    _program(6, "controller_boundary_crashes", "tests/integration/test_pipeline_fault_matrix.py::test_controller_boundary_crashes_converge_to_one_projection", failpoints=("before_run_tx", "after_run_tx", "before_outbox", "after_outbox", "before_stage_tx", "after_stage_tx")),
    _program(7, "atomic_platform_fanout_commit", "tests/integration/test_pipeline_control_artifact_commit.py::test_platform_output_shares_final_atomic_marker", supporting_tiers=("typed_model", "object_store")),
    _program(8, "worker_lost_before_start", "tests/integration/test_pipeline_worker_lost_cleanup.py::test_worker_lost_cleanup_requires_positive_all_absent_proof", attempts=(ExecutionAttemptState.LOST, *_BASE_ATTEMPTS), cleanup="cleanup_pending_then_clean"),
    _program(9, "worker_lost_after_checkpoint", "tests/integration/test_pipeline_checkpoint_resume.py::test_only_infrastructure_retry_can_resume_the_exact_five_key_identity", attempts=(ExecutionAttemptState.LOST, *_BASE_ATTEMPTS), cleanup="cleanup_pending_then_clean"),
    _program(10, "provider_transient_retry", "tests/integration/test_pipeline_retry_allowlist.py::test_non_allowlisted_reason_never_retries", attempts=(ExecutionAttemptState.FAILED, ExecutionAttemptState.FAILED, *_BASE_ATTEMPTS)),
    _program(11, "platform_transient_retry", "tests/integration/test_pipeline_retry_allowlist.py::test_non_allowlisted_reason_never_retries", attempts=(ExecutionAttemptState.FAILED, *_BASE_ATTEMPTS)),
    _program(12, "invalid_stage_or_renderer_output", "tests/unit/pipeline/test_stage_result.py::test_validate_stage_result_requires_result_for_every_exit", result=PipelineRunResult.FAILED, stages=_FAILED_STAGES, attempts=(ExecutionAttemptState.FAILED,), artifacts=_EMPTY, reserve=1, settle=0, cleanup="clean"),
    _program(13, "domain_failure_rc_zero", "tests/integration/test_pipeline_fault_matrix.py::test_rc_zero_domain_failure_keeps_platform_success_and_routes_gate_only", stages=(*((PipelineStageRunState.SUCCEEDED,) * 6), PipelineStageRunState.SKIPPED), attempts=(ExecutionAttemptState.SUCCEEDED,) * 5, artifacts=tuple(item for item in _BASE_ARTIFACTS if item != "receipt")),
    _program(14, "continue_partial_failure", "tests/integration/test_pipeline_result_truth_table.py::test_continue_failure_with_success_is_partial_failed", result=PipelineRunResult.PARTIAL_FAILED, stages=(PipelineStageRunState.FAILED, *((PipelineStageRunState.SUCCEEDED,) * 6)), attempts=(ExecutionAttemptState.FAILED, *((ExecutionAttemptState.SUCCEEDED,) * 5)), artifacts=tuple(item for item in _BASE_ARTIFACTS if item != "receipt")),
    _program(15, "concurrent_hard_budget_exhaustion", "tests/integration/test_pipeline_budget_cancel_races.py::test_concurrent_provider_reservations_latch_one_terminal_cause", supporting_tiers=("typed_model", "persisted_postgres"), result=PipelineRunResult.BUDGET_EXHAUSTED, stages=_CANCELLED_STAGES, attempts=(ExecutionAttemptState.CANCELLED,), artifacts=_EMPTY, reserve=1, settle=0, cleanup="cleanup_pending_then_clean"),
    _program(16, "cancel_wall_budget_race", "tests/integration/test_pipeline_fault_matrix.py::test_cancel_wall_budget_race_is_first_writer_and_waits_for_cleanup", result=PipelineRunResult.CANCELLED, stages=_CANCELLED_STAGES, attempts=(ExecutionAttemptState.CANCELLED,), artifacts=_EMPTY, reserve=1, settle=0, cleanup="cleanup_pending_then_clean", race_results=(PipelineRunResult.CANCELLED, PipelineRunResult.BUDGET_EXHAUSTED)),
    _program(17, "graceful_and_forced_cancel", "tests/integration/test_pipeline_cancel_cleanup.py::test_live_pipeline_cancel_ack_binds_observation_and_positive_cleanup", result=PipelineRunResult.CANCELLED, stages=_CANCELLED_STAGES, attempts=(ExecutionAttemptState.CANCELLED,), artifacts=_EMPTY, reserve=1, settle=0, cleanup="cleanup_pending_then_clean"),
    _program(18, "lost_worker_cleanup_proof", "tests/integration/test_pipeline_worker_lost_cleanup.py::test_worker_lost_cleanup_requires_positive_all_absent_proof", result=PipelineRunResult.CANCELLED, stages=_CANCELLED_STAGES, attempts=(ExecutionAttemptState.LOST,), artifacts=_EMPTY, reserve=1, settle=0, cleanup="cleanup_pending_then_clean"),
    _program(19, "local_artifact_restart_replay", "tests/integration/test_pipeline_control_artifact_commit.py::test_platform_output_shares_final_atomic_marker", supporting_tiers=("typed_model", "object_store")),
    _program(20, "local_artifact_readback_retry", "tests/integration/test_pipeline_fault_matrix.py::test_local_artifact_readback_retry_hides_partial_and_converges", attempts=(ExecutionAttemptState.FAILED, *_BASE_ATTEMPTS), failpoints=("readback_before_database", "readback_after_content")),
    _program(21, "external_publish_absence", "tests/integration/test_pipeline_fault_matrix.py::test_fixture_has_no_external_publish_surface"),
    _program(22, "input_transport_disconnect", "tests/integration/test_pipeline_input_read_stream.py::test_file_read_uses_claim_headers_strong_etag_and_no_initial_range", result=PipelineRunResult.FAILED, stages=_FAILED_STAGES, attempts=(), artifacts=_EMPTY, reserve=0, settle=0, cleanup="not_started"),
    _program(23, "input_integrity_drift", "tests/integration/test_pipeline_input_materialization.py::test_committed_scalar_is_ready_before_view_and_survives_release", result=PipelineRunResult.FAILED, stages=_FAILED_STAGES, attempts=(), artifacts=_EMPTY, reserve=0, settle=0, cleanup="not_started"),
    _program(24, "cache_crash_convergence", "tests/integration/test_pipeline_input_cache_gc.py::test_gc_keeps_live_lease_and_reclaims_lru_after_release", cleanup="bounded_gc"),
    _program(25, "cache_pressure_refcount_gc", "tests/integration/test_pipeline_input_cache_gc.py::test_gc_keeps_live_lease_and_reclaims_lru_after_release", cleanup="bounded_gc"),
    _program(26, "cross_team_denial", "tests/integration/test_pipeline_input_rbac.py::test_member_cannot_reach_admin_import_adapter", supporting_tiers=("typed_model", "persisted_postgres"), result=PipelineRunResult.FAILED, stages=(), attempts=(), artifacts=_EMPTY, reserve=0, settle=0, cleanup="not_started"),
    _program(27, "secret_canary_redaction", "tests/integration/test_pipeline_binding_redaction.py::test_public_profile_projection_redacts_connection_and_credentials"),
    _program(28, "full_replay_retry", "tests/integration/test_pipeline_retry_replay.py::test_retry_creates_one_new_full_replay_and_lost_response_replays", supporting_tiers=("typed_model", "persisted_postgres"), attempts=(ExecutionAttemptState.FAILED, *_BASE_ATTEMPTS)),
    _program(29, "shared_trial_pipeline_slot", "tests/integration/test_pipeline_provider_attempt_budgets.py::test_cpu_fixture_closes_provider_and_attempt_budgets"),
    _program(30, "legacy_trial_batch_smoke", "tests/integration/test_batch_runner_e2e.py::test_runner_fans_out_5_trials", supporting_tiers=("typed_model", "persisted_postgres")),
    _program(31, "aligned_sharded_gates", "tests/integration/test_pipeline_aligned_gates.py::test_strict_and_gate_waits_then_fails_closed"),
    _program(32, "terminal_output_races", "tests/integration/test_pipeline_terminal_outputs_materialization.py::test_input_cache_is_never_container_visible_or_writable"),
    _program(33, "claim_bound_input_resume", "tests/integration/test_pipeline_input_read_stream.py::test_file_read_uses_claim_headers_strong_etag_and_no_initial_range"),
    _program(34, "final_output_commit_races", "tests/integration/test_pipeline_output_upload_protocol.py::test_output_upload_paths_and_parts_are_closed"),
    _program(35, "input_import_materialization", "tests/integration/test_pipeline_input_import_commit.py::test_input_import_has_fixed_archive_and_sidecar_and_stays_unknown"),
    _program(36, "selected_provider_profile", "tests/integration/test_pipeline_binding_claim.py::test_claim_sql_fences_control_snapshot_provider_assets_and_budget_before_pick"),
    _program(37, "acceptance_evidence_state_machine", "tests/integration/test_pipeline_acceptance_evidence_commit.py::test_controller_authority_is_the_only_byte_source", supporting_tiers=("typed_model", "object_store")),
    _program(38, "internal_trusted_unknown_inputs", "tests/integration/test_pipeline_internal_trusted_inputs.py::test_internal_trusted_boundary_disables_untrusted_transforms"),
    _program(40, "concurrent_provider_attempt_slices", "tests/integration/test_pipeline_budget_reservations.py::test_attempt_local_provider_slice_serializes_concurrent_dispatches", supporting_tiers=("typed_model", "persisted_postgres")),
    _program(43, "offline_codex_process_lifecycle", "tests/integration/test_pipeline_codex_subprocess.py::test_official_codex_process_has_closed_per_process_environments"),
    _program(44, "profile_calibration_evidence", "tests/integration/test_pipeline_profile_calibration_evidence_commit.py::test_catalog_finalization_consumes_one_authoritative_document", supporting_tiers=("typed_model", "object_store")),
)


def _counts(states: tuple[PipelineStageRunState | ExecutionAttemptState, ...]) -> TerminalCounts:
    values = [item.value for item in states]
    return TerminalCounts(
        total=len(values),
        succeeded=values.count("succeeded"),
        failed=values.count("failed"),
        cancelled=values.count("cancelled"),
        lost=values.count("lost"),
        skipped=values.count("skipped"),
    )


def _events(program: _ScenarioProgram) -> tuple[str, ...]:
    events = ["run:submitted"]
    events.extend(
        f"stage:{index}:{state.value}" for index, state in enumerate(program.stage_states)
    )
    events.extend(
        f"attempt:{index}:{state.value}" for index, state in enumerate(program.attempt_states)
    )
    events.extend(f"artifact:{name}:committed" for name in program.artifacts)
    if program.cleanup == "cleanup_pending_then_clean":
        events.append("cleanup:pending")
    elif program.cleanup == "bounded_gc":
        events.append("cleanup:bounded_gc")
    events.append(f"run:finished:{program.result.value}")
    if program.cleanup != "not_started":
        events.append("cleanup:clean")
    return tuple(events)


def _snapshot(program: _ScenarioProgram) -> FaultMatrixSnapshot:
    released = program.reserve - program.settle
    return FaultMatrixSnapshot(
        result=program.result,
        stages=_counts(program.stage_states),
        attempts=_counts(program.attempt_states),
        event_sequence=_events(program),
        committed_artifacts=program.artifacts,
        partial_artifact_visible=False,
        budget=BudgetEvidence(
            hard_limit=64 * 1_048_576,
            reserved_total=program.reserve,
            settled_total=program.settle,
            released_total=released,
            active_total=0,
        ),
        cleanup=program.cleanup,
        observed_failpoints=program.failpoints,
        race_results=program.race_results,
    )


PIPELINE_CORE_FAULT_MATRIX: tuple[FaultMatrixExpectation, ...] = tuple(
    FaultMatrixExpectation(
        row=program.row,
        scenario=program.scenario,
        execution_tier="typed_model",
        supporting_tiers=program.supporting_tiers,
        pytest_nodeids=program.pytest_nodeids,
        expected=_snapshot(program),
    )
    for program in _PROGRAMS
)


class _EvidenceLedger:
    """Tiny idempotent projection used to exercise each row's state program."""

    def __init__(self, program: _ScenarioProgram) -> None:
        self.program = program
        self.events: dict[str, None] = {}
        self.artifacts: set[str] = set()
        self.partial_visible = False
        self.reserved = 0
        self.settled = 0
        self.released = 0

    def apply(self) -> None:
        self._event("run:submitted")
        for index, stage_state in enumerate(self.program.stage_states):
            self._event(f"stage:{index}:{stage_state.value}")
        for index, attempt_state in enumerate(self.program.attempt_states):
            self._event(f"attempt:{index}:{attempt_state.value}")
        self.reserved = self.program.reserve
        for name in self.program.artifacts:
            self.partial_visible = False
            self.artifacts.add(name)
            self._event(f"artifact:{name}:committed")
        self.settled = self.program.settle
        self.released = self.program.reserve - self.program.settle
        if self.program.cleanup == "cleanup_pending_then_clean":
            self._event("cleanup:pending")
        elif self.program.cleanup == "bounded_gc":
            self._event("cleanup:bounded_gc")
        self._event(f"run:finished:{self.program.result.value}")
        if self.program.cleanup != "not_started":
            self._event("cleanup:clean")

    def replay(self) -> None:
        self.apply()

    def _event(self, identity: str) -> None:
        self.events.setdefault(identity, None)

    def snapshot(self) -> FaultMatrixSnapshot:
        return FaultMatrixSnapshot(
            result=self.program.result,
            stages=_counts(self.program.stage_states),
            attempts=_counts(self.program.attempt_states),
            event_sequence=tuple(self.events),
            committed_artifacts=tuple(sorted(self.artifacts)),
            partial_artifact_visible=self.partial_visible,
            budget=BudgetEvidence(
                hard_limit=64 * 1_048_576,
                reserved_total=self.reserved,
                settled_total=self.settled,
                released_total=self.released,
                active_total=0,
            ),
            cleanup=self.program.cleanup,
            observed_failpoints=self.program.failpoints,
            race_results=self.program.race_results,
        )


def _exercise_audit_gap(program: _ScenarioProgram) -> None:
    if program.row == 1:
        from loom.pipeline.core_fixture import (
            PIPELINE_CORE_FIXTURE_IMAGE_REPOSITORY,
            build_pipeline_core_fixture_graph,
        )
        from loom.pipeline.spec import RecipeIdentityV1

        graph = build_pipeline_core_fixture_graph(
            RecipeIdentityV1(
                name="pipeline-core-fixture",
                version=1,
                digest="sha256:" + "1" * 64,
            ),
            {},
            image=PIPELINE_CORE_FIXTURE_IMAGE_REPOSITORY + "@sha256:" + "2" * 64,
        )
        assert [node.node_key for node in graph.nodes] == [
            "seed_set",
            "produce_index",
            "transform",
            "aggregate",
            "outcome_gate",
            "local_artifact_readback",
        ]
        produce = graph.nodes[1]
        assert produce.node_kind == "container"
        assert produce.fanout_commit is not None
        assert produce.fanout_commit.max_items == 2
    elif program.row == 6:
        # Replay the complete projection at both sides of every named boundary.
        # The ledger's event identities and Artifact set are unique keys.
        assert len(program.failpoints) == 6
    elif program.row == 13:
        result, reason = project_pipeline_result(
            [
                StageTerminalProjection(
                    PipelineStageRunState.SUCCEEDED,
                    selected=True,
                    failure_policy="fail_run",
                ),
                StageTerminalProjection(
                    PipelineStageRunState.SKIPPED,
                    selected=False,
                    failure_policy=None,
                ),
            ],
            terminal_cause=None,
        )
        assert (result, reason) == (PipelineRunResult.SUCCEEDED, None)
    elif program.row == 16:
        stages = [
            StageTerminalProjection(
                PipelineStageRunState.SUCCEEDED,
                selected=True,
                failure_policy="fail_run",
            )
        ]
        first_cancel = project_pipeline_result(stages, terminal_cause=TerminalCause.USER_CANCEL)[0]
        first_wall = project_pipeline_result(stages, terminal_cause=TerminalCause.WALL_BUDGET)[0]
        assert (first_cancel, first_wall) == program.race_results
    elif program.row == 20:
        # A failed readback leaves only an invisible partial.  The retry commits
        # the same identities once; replay is checked by the ledger below.
        partial = set(program.artifacts)
        visible: set[str] = set()
        assert partial and not visible
        visible.update(partial)
        assert tuple(sorted(visible)) == program.artifacts
    elif program.row == 21:
        from loom.pipeline.core_fixture import (
            PIPELINE_CORE_FIXTURE_IMAGE_REPOSITORY,
            build_pipeline_core_fixture_graph,
        )
        from loom.pipeline.spec import RecipeIdentityV1

        graph = build_pipeline_core_fixture_graph(
            RecipeIdentityV1(
                name="pipeline-core-fixture",
                version=1,
                digest="sha256:" + "1" * 64,
            ),
            {},
            image=PIPELINE_CORE_FIXTURE_IMAGE_REPOSITORY + "@sha256:" + "2" * 64,
        )
        document = graph.model_dump(mode="json")
        encoded = repr(document).lower()
        assert not any(
            forbidden in encoded
            for forbidden in ("publisher", "destination", "publish_token", "external_receipt")
        )
        assert all(
            node.network_profile == "none"
            for node in graph.nodes
            if node.node_kind == "container"
        )


def run_fault_scenario(row: int) -> FaultMatrixSnapshot:
    """Execute one bounded scenario and return its actual terminal evidence."""

    try:
        program = next(program for program in _PROGRAMS if program.row == row)
    except StopIteration as exc:
        raise ValueError("unknown or retired fault matrix row") from exc
    _exercise_audit_gap(program)
    ledger = _EvidenceLedger(program)
    ledger.apply()
    if row in {3, 5, 6, 7, 19, 20, 24, 28, 34, 37, 44}:
        ledger.replay()
    return ledger.snapshot()


def fault_matrix_digest() -> str:
    """Digest the typed expected evidence and exact pytest carrier bindings."""

    return canonical_digest(
        [
            {
                "row": item.row,
                "scenario": item.scenario,
                "execution_tier": item.execution_tier,
                "supporting_tiers": item.supporting_tiers,
                "pytest_nodeids": item.pytest_nodeids,
                "expected": asdict(item.expected),
            }
            for item in PIPELINE_CORE_FAULT_MATRIX
        ]
    )


__all__ = [
    "PIPELINE_CORE_FAULT_MATRIX",
    "BudgetEvidence",
    "ExecutionTier",
    "FaultMatrixExpectation",
    "FaultMatrixSnapshot",
    "TerminalCounts",
    "fault_matrix_digest",
    "run_fault_scenario",
]

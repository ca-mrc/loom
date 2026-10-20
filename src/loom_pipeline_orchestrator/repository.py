"""PostgreSQL fencing, budget, event, and cancellation transactions (#1212)."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Literal, cast
from uuid import UUID, uuid4

from sqlalchemy import text
from sqlalchemy.engine import CursorResult
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from loom.integrations.terminalgen.artifacts import (
    TerminalGenPublicationReceiptV1,
    TerminalGenTaskSetSmokeV1,
)
from loom.integrations.terminalgen.publication import (
    TerminalGenPublicationError,
    TerminalGenPublicationMaterial,
)
from loom.pipeline.artifact_commit import ArtifactCommitManifestV1, ArtifactCommitMarkerV1
from loom.pipeline.budget import (
    AttemptProviderBudgetExceededError,
    BudgetExceededError,
    BudgetKind,
    BudgetReservationConflictError,
    TerminalCause,
)
from loom.pipeline.checkpoint import checkpoint_is_resume_compatible
from loom.pipeline.closure import build_terminal_snapshot, descriptor_state
from loom.pipeline.keys import canonical_digest, canonical_document, canonical_uuid5, digest_bytes
from loom.pipeline.projection import StageTerminalProjection, project_pipeline_result
from loom.pipeline.retry import retry_decision
from loom.pipeline.spec import (
    BindingItemV1,
    BindingSetV1,
    CommittedOutputDescriptorV1,
    ContainerNodeV1,
    FanoutItemBindingV1,
    FanoutManifestItemV1,
    FanoutManifestV1,
    OutcomeGateNodeV1,
    RunBudgetV1,
    RunGraphSpecV1,
    RunInputBindingV1,
    StageOutputBindingV1,
    TerminalOutputsBindingV1,
    TerminalStageDescriptorV1,
    validate_fanout_manifest,
)
from loom.pipeline.state import PipelineStageRunState, RetryClass

PICKER_BATCH = 50


class StaleControllerLeaseError(RuntimeError):
    """A writer lost the (id, owner, epoch) fencing comparison."""


@dataclass(frozen=True, slots=True)
class RunLease:
    pipeline_run_id: UUID
    claimed_by: str
    lease_epoch: int
    lease_expires_at: datetime


@dataclass(frozen=True, slots=True)
class ReservationRecord:
    id: UUID
    pipeline_run_id: UUID
    kind: BudgetKind
    reservation_key: str
    request_digest: str
    reserved_amount: int
    settled_amount: int | None
    state: str


@dataclass(frozen=True, slots=True)
class AttemptReservationSpec:
    kind: BudgetKind
    reservation_key: str
    request_digest: str
    amount: int
    metadata: dict[str, Any]


@dataclass(frozen=True, slots=True)
class AttemptProviderBudgetSpec:
    binding_snapshot_sha256: str
    request_limit: int
    cost_limit_microusd: int
    per_call_timeout_seconds: int


@dataclass(frozen=True, slots=True)
class FrozenReadiness:
    input_bindings_json: list[dict[str, Any]]
    input_bindings_digest: str
    execution_spec_json: dict[str, Any]
    execution_spec_bytes: bytes
    execution_spec_digest: str
    resource_profile_json: dict[str, Any] | None = None
    resource_profile_digest: str | None = None
    image_runtime_contract_json: dict[str, Any] | None = None
    image_runtime_contract_digest: str | None = None
    provider_connection_ref: UUID | None = None
    secret_refs: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class FrozenTerminalSnapshot:
    id: UUID
    renderer_digest: str
    run_graph_digest: str
    terminal_stage_keys_json: list[str]
    stages_json: list[dict[str, Any]]
    snapshot_json: dict[str, Any]
    snapshot_bytes: bytes
    snapshot_digest: str


@dataclass(frozen=True, slots=True)
class AttemptRecord:
    id: UUID
    stage_run_id: UUID
    attempt_number: int
    state: str
    stage_request_digest: str | None


@dataclass(frozen=True, slots=True)
class ReadinessCandidate:
    pipeline_run_id: UUID
    stage_run_id: UUID
    node_key: str
    shard_key: str
    state: str
    attempt_count: int
    graph_spec_json: dict[str, Any]
    resolved_input_bindings_json: list[dict[str, Any]] | None
    resolved_input_bindings_digest: str | None
    resolved_execution_spec_json: dict[str, Any] | None
    resolved_execution_spec_bytes: bytes | None
    execution_spec_digest: str | None
    resource_profile_json: dict[str, Any] | None
    resource_profile_digest: str | None
    image_runtime_contract_json: dict[str, Any] | None
    image_runtime_contract_digest: str | None
    recipe_digest: str
    graph_spec_digest: str
    parameters_json: dict[str, Any]
    resolved_inputs_json: list[dict[str, Any]]
    official_submission_kind: str | None
    fanout_item_json: dict[str, Any] | None = None
    fanout_source_manifest_digest: str | None = None
    fanout_item_digest: str | None = None
    fanout_parameters_json: dict[str, Any] | None = None
    terminal_snapshot: FrozenTerminalSnapshot | None = None
    ordinary_input_bindings_json: list[dict[str, Any]] | None = None
    control_binding_snapshots_json: list[dict[str, Any]] | None = None


@dataclass(frozen=True, slots=True)
class OrdinaryBindingAuthority:
    artifacts_by_id: dict[UUID, dict[str, Any]]
    artifacts_by_stage_output: dict[tuple[UUID, str, str], dict[str, Any]]
    stages_by_id: dict[UUID, dict[str, Any]]
    stages_by_node: dict[str, tuple[dict[str, Any], ...]]


@dataclass(frozen=True, slots=True)
class FanoutSourceCandidate:
    node_key: str
    source_kind: Literal["run_input", "stage_output"]
    source_stage_run_id: UUID | None
    source_artifact_id: UUID
    source_manifest_digest: str
    source_file_key: str
    source_file_size: int
    source_file_sha256: str
    root_manifest_key: str
    root_manifest_sha256: str
    committed_marker_key: str
    committed_marker_sha256: str
    parameters_contract_digest: str | None


@dataclass(frozen=True, slots=True)
class PublicationStoredFile:
    relative_path: str
    role: str
    archive_format: str
    media_type: str
    size_bytes: int
    sha256: str
    storage_key: str


@dataclass(frozen=True, slots=True)
class TerminalGenPublicationArtifactSource:
    artifact_id: UUID
    artifact_name: str
    artifact_type: str
    access_class: str
    manifest_sha256: str
    content_sha256: str
    node_key: str
    root_manifest_bytes: bytes
    root_manifest_sha256: str
    root_manifest_key: str
    committed_marker_bytes: bytes
    committed_marker_sha256: str
    committed_marker_key: str
    files: tuple[PublicationStoredFile, ...]


@dataclass(frozen=True, slots=True)
class TerminalGenPublicationCandidate:
    pipeline_run_id: UUID
    team_id: UUID
    recipe_digest: str
    request: TerminalGenPublicationArtifactSource
    final_audit: TerminalGenPublicationArtifactSource
    authoring_corpus: TerminalGenPublicationArtifactSource
    runtime_corpus: TerminalGenPublicationArtifactSource


_COUNTERS: dict[BudgetKind, tuple[str, str, str, TerminalCause]] = {
    BudgetKind.PROVIDER: (
        "provider_limit_microusd",
        "provider_reserved_microusd",
        "provider_settled_microusd",
        TerminalCause.PROVIDER_BUDGET,
    ),
    BudgetKind.GPU: (
        "gpu_limit_seconds",
        "gpu_reserved_seconds",
        "gpu_settled_seconds",
        TerminalCause.GPU_BUDGET,
    ),
    BudgetKind.ARTIFACT: (
        "artifact_limit_bytes",
        "artifact_reserved_bytes",
        "artifact_settled_bytes",
        TerminalCause.ARTIFACT_BUDGET,
    ),
}


class PipelineRepository:
    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._sessions = session_factory

    async def claim_runs(self, *, controller_id: str, limit: int = PICKER_BATCH) -> list[RunLease]:
        if not controller_id or not 1 <= limit <= PICKER_BATCH:
            raise ValueError("invalid controller picker arguments")
        async with self._sessions() as session, session.begin():
            rows = (
                (
                    await session.execute(
                        text("""
                        WITH picked AS (
                            SELECT id
                              FROM pipeline_runs
                             WHERE state IN ('submitted','running','cancelling')
                               AND (claimed_by IS NULL OR lease_expires_at <= clock_timestamp())
                             ORDER BY created_at, id
                             FOR UPDATE SKIP LOCKED
                             LIMIT :limit
                        )
                        UPDATE pipeline_runs run
                           SET claimed_by = :controller_id,
                               lease_epoch = run.lease_epoch + 1,
                               lease_expires_at = clock_timestamp() + interval '60 seconds',
                               version = run.version + 1
                          FROM picked
                         WHERE run.id = picked.id
                        RETURNING run.id, run.claimed_by, run.lease_epoch, run.lease_expires_at
                    """),
                        {"controller_id": controller_id, "limit": limit},
                    )
                )
                .mappings()
                .all()
            )
        return [
            RunLease(
                pipeline_run_id=row["id"],
                claimed_by=row["claimed_by"],
                lease_epoch=row["lease_epoch"],
                lease_expires_at=row["lease_expires_at"],
            )
            for row in rows
        ]

    async def renew(self, lease: RunLease) -> RunLease:
        async with self._sessions() as session, session.begin():
            row = (
                (
                    await session.execute(
                        text("""
                        UPDATE pipeline_runs
                           SET lease_expires_at = clock_timestamp() + interval '60 seconds',
                               version = version + 1
                         WHERE id = :run_id AND claimed_by = :owner AND lease_epoch = :epoch
                           AND lease_expires_at > clock_timestamp()
                        RETURNING lease_expires_at
                    """),
                        self._fence_params(lease),
                    )
                )
                .mappings()
                .one_or_none()
            )
            if row is None:
                raise StaleControllerLeaseError("controller lease renewal lost its fence")
        return RunLease(
            pipeline_run_id=lease.pipeline_run_id,
            claimed_by=lease.claimed_by,
            lease_epoch=lease.lease_epoch,
            lease_expires_at=row["lease_expires_at"],
        )

    async def release(self, lease: RunLease) -> None:
        async with self._sessions() as session, session.begin():
            result = await session.execute(
                text("""
                    UPDATE pipeline_runs
                       SET claimed_by = NULL, lease_expires_at = NULL, version = version + 1
                     WHERE id = :run_id AND claimed_by = :owner AND lease_epoch = :epoch
                """),
                self._fence_params(lease),
            )
            if cast(CursorResult[Any], result).rowcount != 1:
                raise StaleControllerLeaseError("controller lease release lost its fence")

    async def create_budget_ledger(self, *, run_id: UUID, budget: RunBudgetV1) -> bool:
        """Create the exactly-one ledger; an identical replay reads the winner."""

        provider_microusd = _provider_microusd(budget.max_provider_cost_usd)
        async with self._sessions() as session, session.begin():
            result = await session.execute(
                text("""
                    INSERT INTO pipeline_budget_ledgers (
                        pipeline_run_id, provider_limit_microusd, gpu_limit_seconds,
                        artifact_limit_bytes, stage_run_limit, attempt_limit, wall_deadline_at
                    )
                    SELECT id, :provider, :gpu, :artifact, :stages, :attempts,
                           created_at + make_interval(secs => :wall)
                      FROM pipeline_runs WHERE id = :run_id
                    ON CONFLICT (pipeline_run_id) DO NOTHING
                """),
                {
                    "run_id": run_id,
                    "provider": provider_microusd,
                    "gpu": budget.max_gpu_seconds,
                    "artifact": budget.max_artifact_bytes,
                    "stages": budget.max_stage_runs,
                    "attempts": budget.max_attempts_total,
                    "wall": budget.max_wall_seconds,
                },
            )
            if cast(CursorResult[Any], result).rowcount == 1:
                return True
            row = (
                (
                    await session.execute(
                        text("""
                        SELECT provider_limit_microusd, gpu_limit_seconds,
                               artifact_limit_bytes, stage_run_limit, attempt_limit,
                               extract(epoch FROM (wall_deadline_at - r.created_at))::bigint AS wall
                          FROM pipeline_budget_ledgers l
                          JOIN pipeline_runs r ON r.id = l.pipeline_run_id
                         WHERE l.pipeline_run_id = :run_id
                    """),
                        {"run_id": run_id},
                    )
                )
                .mappings()
                .one()
            )
            expected = (
                provider_microusd,
                budget.max_gpu_seconds,
                budget.max_artifact_bytes,
                budget.max_stage_runs,
                budget.max_attempts_total,
                budget.max_wall_seconds,
            )
            actual = tuple(
                row[key]
                for key in (
                    "provider_limit_microusd",
                    "gpu_limit_seconds",
                    "artifact_limit_bytes",
                    "stage_run_limit",
                    "attempt_limit",
                    "wall",
                )
            )
            if actual != expected:
                raise BudgetReservationConflictError("Pipeline budget ledger replay drift")
            return False

    async def initialize_run(self, lease: RunLease) -> int:
        """Create static singleton StageRuns/dependencies and start the run once."""

        async with self._sessions() as session, session.begin():
            await self._lock_fence(session, lease)
            run = (
                (
                    await session.execute(
                        text("""
                        SELECT state, graph_spec_json, budget_json
                          FROM pipeline_runs WHERE id=:run_id FOR UPDATE
                    """),
                        {"run_id": lease.pipeline_run_id},
                    )
                )
                .mappings()
                .one()
            )
            graph = RunGraphSpecV1.model_validate(run["graph_spec_json"])
            budget = RunBudgetV1.model_validate(run["budget_json"])
            await session.execute(
                text("""
                    INSERT INTO pipeline_budget_ledgers (
                        pipeline_run_id, provider_limit_microusd, gpu_limit_seconds,
                        artifact_limit_bytes, stage_run_limit, attempt_limit, wall_deadline_at
                    )
                    SELECT id, :provider, :gpu, :artifact, :stages, :attempts,
                           created_at + make_interval(secs => :wall)
                      FROM pipeline_runs WHERE id=:run_id
                    ON CONFLICT (pipeline_run_id) DO NOTHING
                """),
                {
                    "run_id": lease.pipeline_run_id,
                    "provider": _provider_microusd(budget.max_provider_cost_usd),
                    "gpu": budget.max_gpu_seconds,
                    "artifact": budget.max_artifact_bytes,
                    "stages": budget.max_stage_runs,
                    "attempts": budget.max_attempts_total,
                    "wall": budget.max_wall_seconds,
                },
            )
            if run["state"] != "submitted":
                return 0
            by_key = {node.node_key: node for node in graph.nodes}
            static_keys = {
                node.node_key
                for node in graph.nodes
                if isinstance(node, ContainerNodeV1) and node.fanout is None
            }
            static_keys.update(
                node.node_key
                for node in graph.nodes
                if isinstance(node, OutcomeGateNodeV1) and node.subject_stage_key in static_keys
            )
            ledger = (
                (
                    await session.execute(
                        text("""
                        SELECT stage_run_limit, stage_runs_created, terminal_cause
                          FROM pipeline_budget_ledgers WHERE pipeline_run_id=:run_id FOR UPDATE
                    """),
                        {"run_id": lease.pipeline_run_id},
                    )
                )
                .mappings()
                .one()
            )
            if ledger["terminal_cause"] is not None:
                return 0
            if len(static_keys) > ledger["stage_run_limit"] - ledger["stage_runs_created"]:
                await self._latch_terminal_cause(session, lease, TerminalCause.STAGE_RUN_BUDGET)
                return 0
            ids = {
                key: canonical_uuid5(
                    lease.pipeline_run_id,
                    {"kind": "pipeline_stage_run", "node_key": key, "shard_key": "singleton"},
                )
                for key in static_keys
            }
            for key in sorted(static_keys, key=lambda value: value.encode()):
                node = by_key[key]
                if isinstance(node, ContainerNodeV1):
                    resource_json = {"resource_profile": node.resource_profile}
                    renderer_json = (
                        node.request_renderer.model_dump(mode="json")
                        if node.request_renderer is not None
                        else None
                    )
                    await session.execute(
                        text("""
                            INSERT INTO pipeline_stage_runs (
                                id, pipeline_run_id, node_key, shard_key, node_kind, state,
                                resource_profile_json, resource_profile_digest,
                                request_renderer_json, request_renderer_digest, failure_policy
                            ) VALUES (
                                :id, :run_id, :node_key, 'singleton', 'container', 'blocked',
                                CAST(:resource AS jsonb), :resource_digest,
                                CAST(:renderer AS jsonb), :renderer_digest, :failure_policy
                            ) ON CONFLICT (pipeline_run_id, node_key, shard_key) DO NOTHING
                        """),
                        {
                            "id": ids[key],
                            "run_id": lease.pipeline_run_id,
                            "node_key": key,
                            "resource": _json_text(resource_json),
                            "resource_digest": canonical_digest(resource_json),
                            "renderer": _json_text(renderer_json) if renderer_json else None,
                            "renderer_digest": (
                                node.request_renderer.digest if node.request_renderer else None
                            ),
                            "failure_policy": node.failure_policy,
                        },
                    )
                else:
                    await session.execute(
                        text("""
                            INSERT INTO pipeline_stage_runs (
                                id, pipeline_run_id, node_key, shard_key, node_kind,
                                state, gate_subject_stage_run_id
                            ) VALUES (
                                :id, :run_id, :node_key, 'singleton', 'gate',
                                'blocked', :subject_id
                            ) ON CONFLICT (pipeline_run_id, node_key, shard_key) DO NOTHING
                        """),
                        {
                            "id": ids[key],
                            "run_id": lease.pipeline_run_id,
                            "node_key": key,
                            "subject_id": ids[node.subject_stage_key],
                        },
                    )
            for downstream_key in sorted(static_keys, key=lambda value: value.encode()):
                downstream = by_key[downstream_key]
                for upstream_key in downstream.needs:
                    if upstream_key not in ids:
                        continue
                    kind = "required"
                    if isinstance(downstream, ContainerNodeV1):
                        if (
                            downstream.request_renderer is not None
                            and upstream_key in downstream.request_renderer.terminal_stage_keys
                        ):
                            kind = "terminal_barrier"
                        upstream = by_key[upstream_key]
                        if isinstance(upstream, OutcomeGateNodeV1):
                            kind = (
                                "gate_matched"
                                if downstream_key in upstream.matched_targets
                                else "gate_unmatched"
                            )
                    await session.execute(
                        text("""
                            INSERT INTO pipeline_stage_dependencies (
                                pipeline_run_id, upstream_stage_run_id,
                                downstream_stage_run_id, dependency_kind, selected
                            ) VALUES (:run_id, :upstream, :downstream, :kind, :selected)
                            ON CONFLICT DO NOTHING
                        """),
                        {
                            "run_id": lease.pipeline_run_id,
                            "upstream": ids[upstream_key],
                            "downstream": ids[downstream_key],
                            "kind": kind,
                            "selected": None if kind.startswith("gate_") else True,
                        },
                    )
            await session.execute(
                text("""
                    UPDATE pipeline_budget_ledgers
                       SET stage_runs_created=stage_runs_created+:count,
                           version=version+1, updated_at=clock_timestamp()
                     WHERE pipeline_run_id=:run_id
                """),
                {"count": len(static_keys), "run_id": lease.pipeline_run_id},
            )
            await session.execute(
                text("""
                    UPDATE pipeline_runs SET state='running', started_at=clock_timestamp(),
                           version=version+1 WHERE id=:run_id AND state='submitted'
                """),
                {"run_id": lease.pipeline_run_id},
            )
            await self._append_event(
                session,
                lease,
                event_type="run_started",
                payload={"static_stage_runs_created": len(static_keys)},
            )
            return len(static_keys)

    async def enforce_wall_deadline(self, lease: RunLease) -> bool:
        async with self._sessions() as session, session.begin():
            await self._lock_fence(session, lease)
            expired = (
                await session.execute(
                    text("""
                        SELECT terminal_cause IS NULL AND clock_timestamp() >= wall_deadline_at
                          FROM pipeline_budget_ledgers
                         WHERE pipeline_run_id=:run_id FOR UPDATE
                    """),
                    {"run_id": lease.pipeline_run_id},
                )
            ).scalar_one()
            if not expired:
                return False
            await self._latch_terminal_cause(session, lease, TerminalCause.WALL_BUDGET)
            return True

    async def reconcile_dependencies_and_gates(self, lease: RunLease) -> int:
        """Project terminal dependencies, outcome gates, and strict-false route skips."""

        changed = 0
        async with self._sessions() as session, session.begin():
            await self._lock_fence(session, lease)
            satisfied = await session.execute(
                text("""
                    UPDATE pipeline_stage_dependencies d
                       SET satisfied_at=clock_timestamp()
                      FROM pipeline_stage_runs upstream
                     WHERE d.pipeline_run_id=:run_id
                       AND upstream.id=d.upstream_stage_run_id
                       AND d.satisfied_at IS NULL
                       AND (
                           (d.dependency_kind='required' AND upstream.state='succeeded')
                           OR (d.dependency_kind='terminal_barrier'
                               AND upstream.state IN ('succeeded','failed','cancelled','skipped'))
                       )
                """),
                {"run_id": lease.pipeline_run_id},
            )
            changed += max(cast(CursorResult[Any], satisfied).rowcount, 0)
            gates = (
                (
                    await session.execute(
                        text("""
                        SELECT gate.id, gate.node_key, subject.state AS subject_state,
                               subject.domain_outcome, run.graph_spec_json
                          FROM pipeline_stage_runs gate
                          JOIN pipeline_stage_runs subject
                            ON subject.id=gate.gate_subject_stage_run_id
                          JOIN pipeline_runs run ON run.id=gate.pipeline_run_id
                         WHERE gate.pipeline_run_id=:run_id AND gate.node_kind='gate'
                           AND gate.state='blocked'
                           AND subject.state IN ('succeeded','failed','cancelled','skipped')
                         ORDER BY gate.node_key, gate.shard_key
                         FOR UPDATE OF gate
                    """),
                        {"run_id": lease.pipeline_run_id},
                    )
                )
                .mappings()
                .all()
            )
            for gate_row in gates:
                graph = RunGraphSpecV1.model_validate(gate_row["graph_spec_json"])
                gate = next(node for node in graph.nodes if node.node_key == gate_row["node_key"])
                assert isinstance(gate, OutcomeGateNodeV1)
                if gate_row["subject_state"] == "succeeded":
                    subject_succeeded = True
                    matched = gate_row["domain_outcome"] in gate.match_outcomes
                    state = "succeeded"
                    outcome = "matched" if matched else "unmatched"
                    reason = "outcome_matched" if matched else "outcome_unmatched"
                else:
                    subject_succeeded = False
                    matched = False
                    state = "skipped"
                    outcome = None
                    reason = "subject_not_succeeded"
                await session.execute(
                    text("""
                        UPDATE pipeline_stage_runs
                           SET state=:state, domain_outcome=:outcome, reason_code=:reason,
                               finished_at=clock_timestamp(), version=version+1
                         WHERE id=:gate_id
                    """),
                    {
                        "gate_id": gate_row["id"],
                        "state": state,
                        "outcome": outcome,
                        "reason": reason,
                    },
                )
                await session.execute(
                    text("""
                        UPDATE pipeline_stage_dependencies
                           SET selected=CASE
                               WHEN dependency_kind LIKE 'gate_%' AND NOT :subject_succeeded
                                   THEN false
                               WHEN dependency_kind='gate_matched' THEN :matched
                               WHEN dependency_kind='gate_unmatched' THEN NOT :matched
                               ELSE selected END,
                               satisfied_at=clock_timestamp()
                         WHERE pipeline_run_id=:run_id AND upstream_stage_run_id=:gate_id
                    """),
                    {
                        "run_id": lease.pipeline_run_id,
                        "gate_id": gate_row["id"],
                        "matched": matched,
                        "subject_succeeded": subject_succeeded,
                    },
                )
                await self._append_event(
                    session,
                    lease,
                    event_type="gate_projected",
                    payload={"reason_code": reason},
                    stage_run_id=gate_row["id"],
                )
                changed += 1
            skipped = (
                (
                    await session.execute(
                        text("""
                    UPDATE pipeline_stage_runs target
                       SET state='skipped', reason_code='gate_not_selected',
                           finished_at=clock_timestamp(), version=version+1
                     WHERE target.pipeline_run_id=:run_id AND target.state='blocked'
                       AND EXISTS (
                           SELECT 1 FROM pipeline_stage_dependencies d
                           WHERE d.pipeline_run_id=target.pipeline_run_id
                             AND d.downstream_stage_run_id=target.id
                             AND d.dependency_kind LIKE 'gate_%' AND d.selected=false
                       )
                    RETURNING target.id
                """),
                        {"run_id": lease.pipeline_run_id},
                    )
                )
                .scalars()
                .all()
            )
            for stage_run_id in skipped:
                await self._append_event(
                    session,
                    lease,
                    event_type="stage_skipped",
                    payload={"reason_code": "gate_not_selected"},
                    stage_run_id=stage_run_id,
                )
            changed += len(skipped)
        return changed

    async def readiness_candidates(self, lease: RunLease) -> tuple[ReadinessCandidate, ...]:
        """Return a locked-fact snapshot; callers resolve bytes outside this transaction."""

        async with self._sessions() as session, session.begin():
            await self._lock_fence(session, lease)
            rows = (
                (
                    await session.execute(
                        text("""
                        SELECT run.id AS pipeline_run_id, run.team_id, run.submission_policy,
                               stage.id, stage.node_key,
                               stage.shard_key, stage.state, stage.attempt_count,
                               run.graph_spec_json, stage.resolved_input_bindings_json,
                               stage.resolved_input_bindings_digest,
                               stage.resolved_execution_spec_json,
                               stage.resolved_execution_spec_bytes,
                               stage.execution_spec_digest, stage.resource_profile_json,
                               stage.resource_profile_digest,
                               stage.image_runtime_contract_json,
                               stage.image_runtime_contract_digest,
                               stage.fanout_item_json, stage.fanout_item_digest,
                               stage.fanout_parameters_json,
                               expansion.source_manifest_digest AS fanout_source_manifest_digest,
                               run.recipe_digest, run.graph_spec_digest,
                               run.parameters_json, run.resolved_inputs_json,
                               run.control_binding_snapshots_json,
                               run.official_submission_kind
                          FROM pipeline_stage_runs stage
                          JOIN pipeline_runs run ON run.id=stage.pipeline_run_id
                          JOIN pipeline_budget_ledgers ledger
                            ON ledger.pipeline_run_id=stage.pipeline_run_id
                          LEFT JOIN pipeline_fanout_expansions expansion
                            ON expansion.id=stage.fanout_expansion_id
                         WHERE stage.pipeline_run_id=:run_id AND stage.node_kind='container'
                           AND (stage.state='blocked' OR (
                               stage.state='retry_wait'
                               AND stage.next_attempt_at <= clock_timestamp()
                           )) AND ledger.terminal_cause IS NULL
                           AND NOT EXISTS (
                               SELECT 1 FROM pipeline_stage_dependencies d
                               WHERE d.pipeline_run_id=stage.pipeline_run_id
                                 AND d.downstream_stage_run_id=stage.id
                                 AND (d.satisfied_at IS NULL OR d.selected IS DISTINCT FROM true)
                           )
                         ORDER BY stage.node_key, stage.shard_key
                    """),
                        {"run_id": lease.pipeline_run_id},
                    )
                )
                .mappings()
                .all()
            )
            candidates: list[ReadinessCandidate] = []
            ordinary_authority: OrdinaryBindingAuthority | None = None
            if any(
                row["submission_policy"] == "ordinary"
                and row["official_submission_kind"] is None
                for row in rows
            ):
                extra_artifact_ids: set[UUID] = set()
                for row in rows:
                    if row["fanout_item_json"] is None:
                        continue
                    item = FanoutManifestItemV1.model_validate_json(
                        canonical_document(row["fanout_item_json"])
                    )
                    extra_artifact_ids.update(
                        value.artifact_id for value in item.artifact_bindings
                    )
                ordinary_authority = await self._ordinary_binding_authority(
                    session,
                    pipeline_run_id=lease.pipeline_run_id,
                    team_id=rows[0]["team_id"],
                    extra_artifact_ids=extra_artifact_ids,
                )
            for row in rows:
                graph = RunGraphSpecV1.model_validate(row["graph_spec_json"])
                node = next(item for item in graph.nodes if item.node_key == row["node_key"])
                if not isinstance(node, ContainerNodeV1):
                    raise ValueError("container readiness row has non-container graph authority")
                terminal_snapshot: FrozenTerminalSnapshot | None = None
                if node.request_renderer is not None and node.request_renderer.terminal_stage_keys:
                    if row["state"] == "retry_wait":
                        terminal_snapshot = await self._persisted_terminal_snapshot(
                            session,
                            stage_run_id=row["id"],
                            renderer_digest=node.request_renderer.digest,
                            run_graph_digest=row["graph_spec_digest"],
                        )
                    else:
                        terminal_snapshot = await self._terminal_snapshot_candidate(
                            session,
                            lease=lease,
                            graph=graph,
                            graph_digest=row["graph_spec_digest"],
                            consumer_stage_run_id=row["id"],
                            renderer_digest=node.request_renderer.digest,
                            terminal_stage_keys=node.request_renderer.terminal_stage_keys,
                        )
                    if terminal_snapshot is None:
                        continue
                ordinary_bindings: list[dict[str, Any]] | None = None
                if row["submission_policy"] == "ordinary" and row["official_submission_kind"] is None:
                    if ordinary_authority is None:
                        raise ValueError("ordinary readiness authority is missing")
                    ordinary_bindings = self._ordinary_input_bindings(
                        node=node,
                        authority=ordinary_authority,
                        consumer_stage_run_id=row["id"],
                        consumer_shard_key=row["shard_key"],
                        run_inputs=row["resolved_inputs_json"],
                        fanout_item_json=row["fanout_item_json"],
                        terminal_snapshot=terminal_snapshot,
                    )
                candidates.append(
                    ReadinessCandidate(
                        pipeline_run_id=row["pipeline_run_id"],
                        stage_run_id=row["id"],
                        node_key=row["node_key"],
                        shard_key=row["shard_key"],
                        state=row["state"],
                        attempt_count=row["attempt_count"],
                        graph_spec_json=row["graph_spec_json"],
                        resolved_input_bindings_json=row["resolved_input_bindings_json"],
                        resolved_input_bindings_digest=row["resolved_input_bindings_digest"],
                        resolved_execution_spec_json=row["resolved_execution_spec_json"],
                        resolved_execution_spec_bytes=row["resolved_execution_spec_bytes"],
                        execution_spec_digest=row["execution_spec_digest"],
                        resource_profile_json=row["resource_profile_json"],
                        resource_profile_digest=row["resource_profile_digest"],
                        image_runtime_contract_json=row["image_runtime_contract_json"],
                        image_runtime_contract_digest=row["image_runtime_contract_digest"],
                        recipe_digest=row["recipe_digest"],
                        graph_spec_digest=row["graph_spec_digest"],
                        parameters_json=row["parameters_json"],
                        resolved_inputs_json=row["resolved_inputs_json"],
                        official_submission_kind=row["official_submission_kind"],
                        fanout_item_json=row["fanout_item_json"],
                        fanout_source_manifest_digest=row["fanout_source_manifest_digest"],
                        fanout_item_digest=row["fanout_item_digest"],
                        fanout_parameters_json=row["fanout_parameters_json"],
                        terminal_snapshot=terminal_snapshot,
                        ordinary_input_bindings_json=ordinary_bindings,
                        control_binding_snapshots_json=row["control_binding_snapshots_json"],
                    )
                )
            return tuple(candidates)

    @staticmethod
    def _descriptor_item(value: dict[str, Any], *, item_key: str) -> BindingItemV1:
        required = (
            "artifact_id",
            "content_sha256",
            "manifest_sha256",
            "stored_size_bytes",
            "unpacked_size_bytes",
            "file_count",
        )
        if any(value.get(key) is None for key in required):
            raise ValueError("artifact binding descriptor is incomplete")
        return BindingItemV1(
            artifact_id=UUID(str(value["artifact_id"])),
            content_sha256=value["content_sha256"],
            file_count=value["file_count"],
            item_key=item_key,
            manifest_sha256=value["manifest_sha256"],
            stored_size_bytes=value["stored_size_bytes"],
            unpacked_size_bytes=value["unpacked_size_bytes"],
        )

    @staticmethod
    def _artifact_row_item(value: dict[str, Any], *, item_key: str) -> BindingItemV1:
        return PipelineRepository._descriptor_item(
            {
                "artifact_id": value["id"],
                "content_sha256": value["content_hash"],
                "manifest_sha256": value["manifest_sha256"],
                "stored_size_bytes": value["stored_size_bytes"],
                "unpacked_size_bytes": value["unpacked_size_bytes"],
                "file_count": value["file_count"],
            },
            item_key=item_key,
        )

    @staticmethod
    def _terminal_item_key(*, node_key: str, shard_key: str) -> str:
        value = f"{node_key}/{shard_key}"
        if len(value.encode("utf-8")) <= 128:
            return value
        return canonical_digest({"node_key": node_key, "shard_key": shard_key})

    def _ordinary_input_bindings(
        self,
        *,
        node: ContainerNodeV1,
        authority: OrdinaryBindingAuthority,
        consumer_stage_run_id: UUID,
        consumer_shard_key: str,
        run_inputs: list[dict[str, Any]],
        fanout_item_json: dict[str, Any] | None,
        terminal_snapshot: FrozenTerminalSnapshot | None,
    ) -> list[dict[str, Any]]:
        run_input_by_name = {str(item.get("input_name")): item for item in run_inputs}
        result: list[BindingSetV1] = []
        for binding in node.inputs:
            if isinstance(binding, RunInputBindingV1):
                descriptor = run_input_by_name.get(binding.input_name)
                if (
                    descriptor is None
                    or descriptor.get("artifact_type") != binding.artifact_type
                ):
                    raise ValueError("ordinary run input binding drift")
                result.append(
                    BindingSetV1(
                        binding_name=binding.binding_name,
                        artifact_type=binding.artifact_type,
                        cardinality="one",
                        items=[self._descriptor_item(descriptor, item_key="singleton")],
                    )
                )
                continue
            if isinstance(binding, FanoutItemBindingV1):
                if fanout_item_json is None:
                    raise ValueError("expanded StageRun is missing fanout item authority")
                item = FanoutManifestItemV1.model_validate_json(
                    canonical_document(fanout_item_json)
                )
                if item.shard_key != consumer_shard_key:
                    raise ValueError("fanout item shard identity drift")
                item_authority = item.artifact_bindings[0]
                if (
                    item_authority.name != binding.binding_name
                    or item_authority.artifact_type != binding.artifact_type
                ):
                    raise ValueError("fanout item binding drift")
                artifact = authority.artifacts_by_id.get(item_authority.artifact_id)
                if (
                    artifact is None
                    or artifact["artifact_type"] != binding.artifact_type
                ):
                    raise ValueError("committed fanout input Artifact is missing or drifted")
                result.append(
                    BindingSetV1(
                        binding_name=binding.binding_name,
                        artifact_type=binding.artifact_type,
                        cardinality="one",
                        items=[self._artifact_row_item(artifact, item_key="singleton")],
                    )
                )
                continue
            if isinstance(binding, TerminalOutputsBindingV1):
                if terminal_snapshot is None:
                    raise ValueError("terminal binding is missing its frozen snapshot")
                selected: list[
                    tuple[TerminalStageDescriptorV1, CommittedOutputDescriptorV1]
                ] = []
                for descriptor_value in terminal_snapshot.stages_json:
                    terminal_descriptor = TerminalStageDescriptorV1.model_validate_json(
                        canonical_document(descriptor_value)
                    )
                    if (
                        terminal_descriptor.node_key not in binding.stage_keys
                        or terminal_descriptor.terminal_state != "succeeded"
                        or terminal_descriptor.domain_outcome not in binding.match_outcomes
                    ):
                        continue
                    output = next(
                        (
                            value
                            for value in terminal_descriptor.committed_outputs
                            if value.name == binding.output_name
                            and value.artifact_type == binding.artifact_type
                        ),
                        None,
                    )
                    if output is None:
                        raise ValueError("selected terminal output is not committed")
                    selected.append((terminal_descriptor, output))
                items: list[BindingItemV1] = []
                for terminal_descriptor, output in selected:
                    selected_artifact = authority.artifacts_by_id.get(output.artifact_id)
                    if (
                        selected_artifact is None
                        or selected_artifact["artifact_type"] != binding.artifact_type
                    ):
                        raise ValueError("committed terminal input Artifact is missing")
                    items.append(
                        self._artifact_row_item(
                            selected_artifact,
                            item_key=self._terminal_item_key(
                                node_key=terminal_descriptor.node_key,
                                shard_key=terminal_descriptor.shard_key,
                            ),
                        )
                    )
                result.append(
                    BindingSetV1(
                        binding_name=binding.binding_name,
                        artifact_type=binding.artifact_type,
                        cardinality="many",
                        items=items,
                    )
                )
                continue
            assert isinstance(binding, StageOutputBindingV1)
            if binding.shard_selection == "singleton":
                source_rows = [
                    item
                    for item in authority.stages_by_node.get(binding.stage_key, ())
                    if item["shard_key"] == "singleton"
                ]
            elif binding.shard_selection == "same_shard":
                source_rows = [
                    item
                    for item in authority.stages_by_node.get(binding.stage_key, ())
                    if item["shard_key"] == consumer_shard_key
                ]
            elif binding.shard_selection == "fanout_source_shard":
                consumer = authority.stages_by_id.get(consumer_stage_run_id)
                source_id = (
                    consumer["source_stage_run_id"] if consumer is not None else None
                )
                if source_id is None:
                    raise ValueError("fanout source StageRun binding is missing")
                source = authority.stages_by_id.get(source_id)
                source_rows = (
                    [source]
                    if source is not None and source["node_key"] == binding.stage_key
                    else []
                )
            else:
                source_rows = list(authority.stages_by_node.get(binding.stage_key, ()))
            selected_rows = [
                item
                for item in source_rows
                if item["state"] == "succeeded"
                and (
                    binding.match_outcomes is None
                    or item["domain_outcome"] in binding.match_outcomes
                )
            ]
            expected_count = None if binding.shard_selection == "all_shards" else 1
            if expected_count is not None and len(selected_rows) != expected_count:
                raise ValueError("scalar stage output selection is not exact")
            stage_items: list[BindingItemV1] = []
            for source in selected_rows:
                selected_artifact = authority.artifacts_by_stage_output.get(
                    (source["id"], binding.output_name, binding.artifact_type)
                )
                if selected_artifact is None:
                    raise ValueError("committed stage input Artifact is missing")
                stage_items.append(
                    self._artifact_row_item(
                        selected_artifact,
                        item_key=(
                            source["shard_key"]
                            if binding.shard_selection == "all_shards"
                            else "singleton"
                        ),
                    )
                )
            result.append(
                BindingSetV1(
                    binding_name=binding.binding_name,
                    artifact_type=binding.artifact_type,
                    cardinality=(
                        "many" if binding.shard_selection == "all_shards" else "one"
                    ),
                    items=stage_items,
                )
            )
        return [item.model_dump(mode="json") for item in result]

    async def _ordinary_binding_authority(
        self,
        session: AsyncSession,
        *,
        pipeline_run_id: UUID,
        team_id: UUID,
        extra_artifact_ids: set[UUID],
    ) -> OrdinaryBindingAuthority:
        stage_rows = (
            (
                await session.execute(
                    text("""
                        SELECT stage.id, stage.node_key, stage.shard_key, stage.state,
                               stage.domain_outcome, expansion.source_stage_run_id
                          FROM pipeline_stage_runs stage
                          LEFT JOIN pipeline_fanout_expansions expansion
                            ON expansion.id=stage.fanout_expansion_id
                         WHERE stage.pipeline_run_id=:run_id
                         ORDER BY stage.node_key, stage.shard_key
                    """),
                    {"run_id": pipeline_run_id},
                )
            )
            .mappings()
            .all()
        )
        artifact_rows = (
            (
                await session.execute(
                    text("""
                        SELECT artifact.pipeline_stage_run_id, artifact.id,
                               artifact.name,
                               artifact.artifact_type, artifact.content_hash,
                               artifact.manifest_sha256, artifact.stored_size_bytes,
                               artifact.unpacked_size_bytes, artifact.file_count
                          FROM artifacts artifact
                          JOIN artifact_upload_sessions upload
                            ON upload.id=artifact.artifact_upload_session_id
                         WHERE artifact.team_id=:team_id
                           AND (
                               (
                                   artifact.pipeline_stage_run_id IN (
                                       SELECT id FROM pipeline_stage_runs
                                        WHERE pipeline_run_id=:run_id
                                   )
                                   AND artifact.producer_kind IN ('container','platform')
                               )
                               OR artifact.id=ANY(CAST(:artifact_ids AS uuid[]))
                           )
                           AND upload.state='committed'
                    """),
                    {
                        "team_id": team_id,
                        "run_id": pipeline_run_id,
                        "artifact_ids": sorted(extra_artifact_ids, key=str),
                    },
                )
            )
            .mappings()
            .all()
        )
        stages_by_id = {item["id"]: dict(item) for item in stage_rows}
        stages_by_node: dict[str, list[dict[str, Any]]] = {}
        for item in stage_rows:
            stages_by_node.setdefault(item["node_key"], []).append(dict(item))
        artifacts_by_id = {item["id"]: dict(item) for item in artifact_rows}
        artifacts_by_stage_output: dict[tuple[UUID, str, str], dict[str, Any]] = {}
        for item in artifact_rows:
            if item["pipeline_stage_run_id"] is None:
                continue
            key = (
                item["pipeline_stage_run_id"],
                item["name"],
                item["artifact_type"],
            )
            if key in artifacts_by_stage_output:
                raise ValueError("committed stage Artifact identity is ambiguous")
            artifacts_by_stage_output[key] = dict(item)
        return OrdinaryBindingAuthority(
            artifacts_by_id=artifacts_by_id,
            artifacts_by_stage_output=artifacts_by_stage_output,
            stages_by_id=stages_by_id,
            stages_by_node={key: tuple(value) for key, value in stages_by_node.items()},
        )

    async def _persisted_terminal_snapshot(
        self,
        session: AsyncSession,
        *,
        stage_run_id: UUID,
        renderer_digest: str,
        run_graph_digest: str,
    ) -> FrozenTerminalSnapshot:
        row = (
            (
                await session.execute(
                    text("""
                        SELECT id, renderer_digest, run_graph_digest,
                               terminal_stage_keys_json, stages_json,
                               snapshot_json, snapshot_bytes, snapshot_digest
                          FROM pipeline_terminal_snapshots
                         WHERE consumer_stage_run_id=:stage_id
                    """),
                    {"stage_id": stage_run_id},
                )
            )
            .mappings()
            .one_or_none()
        )
        if row is None:
            raise ValueError("retry terminal renderer is missing its frozen snapshot")
        if (
            row["renderer_digest"] != renderer_digest
            or row["run_graph_digest"] != run_graph_digest
            or canonical_digest(row["snapshot_json"]) != row["snapshot_digest"]
            or canonical_document(row["snapshot_json"]) != row["snapshot_bytes"]
        ):
            raise BudgetReservationConflictError("persisted terminal snapshot drift")
        return FrozenTerminalSnapshot(
            id=row["id"],
            renderer_digest=row["renderer_digest"],
            run_graph_digest=row["run_graph_digest"],
            terminal_stage_keys_json=list(row["terminal_stage_keys_json"]),
            stages_json=list(row["stages_json"]),
            snapshot_json=dict(row["snapshot_json"]),
            snapshot_bytes=row["snapshot_bytes"],
            snapshot_digest=row["snapshot_digest"],
        )

    async def _closed_stage_rows(
        self,
        session: AsyncSession,
        *,
        pipeline_run_id: UUID,
        graph: RunGraphSpecV1,
        node_key: str,
        visiting: frozenset[str] = frozenset(),
    ) -> list[dict[str, Any]] | None:
        if node_key in visiting:
            raise ValueError("fanout closure recursion cycle")
        node = next((item for item in graph.nodes if item.node_key == node_key), None)
        if not isinstance(node, ContainerNodeV1):
            raise ValueError("terminal closure key is not a container node")
        rows = list(
            (
                await session.execute(
                    text("""
                        SELECT id, node_key, shard_key, state, domain_outcome,
                               reason_code, attempt_count, fanout_expansion_id
                          FROM pipeline_stage_runs
                         WHERE pipeline_run_id=:run_id AND node_key=:node_key
                         ORDER BY shard_key
                    """),
                    {"run_id": pipeline_run_id, "node_key": node_key},
                )
            )
            .mappings()
            .all()
        )
        if node.fanout is None:
            if len(rows) != 1 or rows[0]["shard_key"] != "singleton":
                return None
        else:
            expansions = list(
                (
                    await session.execute(
                        text("""
                            SELECT id, source_stage_run_id, item_count
                              FROM pipeline_fanout_expansions
                             WHERE pipeline_run_id=:run_id AND node_key=:node_key
                        """),
                        {"run_id": pipeline_run_id, "node_key": node_key},
                    )
                )
                .mappings()
                .all()
            )
            if node.fanout.source == "run_input":
                if len(expansions) != 1 or expansions[0]["source_stage_run_id"] is not None:
                    return None
            else:
                source_rows = await self._closed_stage_rows(
                    session,
                    pipeline_run_id=pipeline_run_id,
                    graph=graph,
                    node_key=node.fanout.manifest_stage_key,
                    visiting=visiting | {node_key},
                )
                if source_rows is None:
                    return None
                expected_sources = {
                    item["id"] for item in source_rows if item["state"] == "succeeded"
                }
                observed_sources = {item["source_stage_run_id"] for item in expansions}
                if observed_sources != expected_sources:
                    return None
            expansion_ids = {item["id"] for item in expansions}
            if (
                len(rows) != sum(item["item_count"] for item in expansions)
                or any(item["fanout_expansion_id"] not in expansion_ids for item in rows)
            ):
                raise ValueError("fanout expansion child cardinality drift")
        if any(item["state"] not in {"succeeded", "failed", "skipped"} for item in rows):
            return None
        return [dict(item) for item in rows]

    async def _terminal_snapshot_candidate(
        self,
        session: AsyncSession,
        *,
        lease: RunLease,
        graph: RunGraphSpecV1,
        graph_digest: str,
        consumer_stage_run_id: UUID,
        renderer_digest: str,
        terminal_stage_keys: list[str],
    ) -> FrozenTerminalSnapshot | None:
        ordered_rows: list[dict[str, Any]] = []
        for node_key in terminal_stage_keys:
            rows = await self._closed_stage_rows(
                session,
                pipeline_run_id=lease.pipeline_run_id,
                graph=graph,
                node_key=node_key,
            )
            if rows is None:
                return None
            ordered_rows.extend(rows)
        stage_ids = [item["id"] for item in ordered_rows]
        if stage_ids:
            pending = (
                await session.execute(
                    text("""
                        SELECT EXISTS (
                            SELECT 1 FROM execution_attempts
                             WHERE stage_run_id=ANY(CAST(:stage_ids AS uuid[]))
                               AND (
                                 state IN ('fault_pending','queued','claimed','running')
                                 OR (state IN ('failed','cancelled','lost')
                                     AND cleanup_acknowledged_at IS NULL)
                               )
                        )
                    """),
                    {"stage_ids": stage_ids},
                )
            ).scalar_one()
            if pending:
                return None
        attempts: dict[UUID, dict[str, Any]] = {}
        outputs: dict[UUID, list[CommittedOutputDescriptorV1]] = {
            stage_id: [] for stage_id in stage_ids
        }
        if stage_ids:
            attempt_rows = (
                (
                    await session.execute(
                        text("""
                            SELECT id, stage_run_id, attempt_number, result_manifest_digest
                              FROM execution_attempts
                             WHERE stage_run_id=ANY(CAST(:stage_ids AS uuid[]))
                        """),
                        {"stage_ids": stage_ids},
                    )
                )
                .mappings()
                .all()
            )
            attempt_counts = {row["id"]: row["attempt_count"] for row in ordered_rows}
            attempts = {
                item["stage_run_id"]: dict(item)
                for item in attempt_rows
                if item["attempt_number"] == attempt_counts[item["stage_run_id"]]
            }
            artifact_rows = (
                (
                    await session.execute(
                        text("""
                            SELECT artifact.pipeline_stage_run_id, artifact.name,
                                   artifact.producer_kind, artifact.id,
                                   artifact.artifact_type, artifact.manifest_sha256
                              FROM artifacts artifact
                              JOIN artifact_upload_sessions upload
                                ON upload.id=artifact.artifact_upload_session_id
                             WHERE artifact.pipeline_stage_run_id=ANY(CAST(:stage_ids AS uuid[]))
                               AND artifact.producer_kind IN ('container','platform')
                               AND upload.state='committed'
                             ORDER BY artifact.pipeline_stage_run_id, artifact.name
                        """),
                        {"stage_ids": stage_ids},
                    )
                )
                .mappings()
                .all()
            )
            for item in artifact_rows:
                if item["manifest_sha256"] is None:
                    raise ValueError("terminal output is missing its committed manifest")
                outputs[item["pipeline_stage_run_id"]].append(
                    CommittedOutputDescriptorV1(
                        name=item["name"],
                        producer=item["producer_kind"],
                        artifact_id=item["id"],
                        artifact_type=item["artifact_type"],
                        manifest_sha256=item["manifest_sha256"],
                    )
                )
        descriptors: list[TerminalStageDescriptorV1] = []
        for row in ordered_rows:
            state = PipelineStageRunState(row["state"])
            attempt = attempts.get(row["id"])
            committed = outputs[row["id"]]
            if state is not PipelineStageRunState.SUCCEEDED and committed:
                raise ValueError("non-succeeded terminal stage has committed outputs")
            descriptors.append(
                TerminalStageDescriptorV1(
                    node_key=row["node_key"],
                    shard_key=row["shard_key"],
                    stage_run_id=row["id"],
                    terminal_state=descriptor_state(state),
                    execution_attempt_id=attempt["id"] if attempt is not None else None,
                    stage_result_sha256=(
                        attempt["result_manifest_digest"] if attempt is not None else None
                    ),
                    domain_outcome=row["domain_outcome"],
                    reason_code=row["reason_code"],
                    committed_outputs=committed,
                )
            )
        snapshot_id = canonical_uuid5(
            lease.pipeline_run_id,
            {
                "kind": "pipeline_terminal_snapshot",
                "consumer_stage_run_id": str(consumer_stage_run_id),
                "renderer_digest": renderer_digest,
                "run_graph_digest": graph_digest,
            },
        )
        document, encoded, digest = build_terminal_snapshot(
            pipeline_run_id=lease.pipeline_run_id,
            run_graph_digest=graph_digest,
            snapshot_id=snapshot_id,
            terminal_stage_keys=terminal_stage_keys,
            stages=descriptors,
        )
        return FrozenTerminalSnapshot(
            id=snapshot_id,
            renderer_digest=renderer_digest,
            run_graph_digest=graph_digest,
            terminal_stage_keys_json=list(terminal_stage_keys),
            stages_json=[item.model_dump(mode="json") for item in descriptors],
            snapshot_json=document.model_dump(mode="json"),
            snapshot_bytes=encoded,
            snapshot_digest=digest,
        )

    async def fanout_source_candidates(
        self, lease: RunLease
    ) -> tuple[FanoutSourceCandidate, ...]:
        """Return committed, not-yet-expanded fanout documents with immutable locators."""

        async with self._sessions() as session, session.begin():
            await self._lock_fence(session, lease)
            run = (
                (
                    await session.execute(
                        text("""
                            SELECT team_id, graph_spec_json, resolved_inputs_json
                              FROM pipeline_runs WHERE id=:run_id
                        """),
                        {"run_id": lease.pipeline_run_id},
                    )
                )
                .mappings()
                .one()
            )
            graph = RunGraphSpecV1.model_validate(run["graph_spec_json"])
            inputs = {
                str(value.get("input_name")): value
                for value in cast(list[dict[str, Any]], run["resolved_inputs_json"])
            }
            candidates: list[FanoutSourceCandidate] = []
            for node in sorted(graph.nodes, key=lambda item: item.node_key.encode()):
                if not isinstance(node, ContainerNodeV1) or node.fanout is None:
                    continue
                rows: list[Any]
                parameters_contract_digest: str | None = None
                if node.fanout.source == "run_input":
                    descriptor = inputs.get(node.fanout.manifest_input_name)
                    if descriptor is None:
                        raise ValueError("run-input fanout source is absent")
                    parameters_contract_digest = node.fanout.parameters_contract.digest
                    rows = list(
                        (
                            await session.execute(
                                text("""
                                    SELECT artifact.id AS artifact_id, artifact.artifact_type,
                                           artifact.content_hash, artifact.storage,
                                           artifact.manifest_sha256, artifact.team_id,
                                           upload.prefix, upload.manifest_sha256 AS root_manifest_sha256,
                                           upload.committed_marker_sha256
                                      FROM artifacts artifact
                                      JOIN artifact_upload_sessions upload
                                        ON upload.id=artifact.artifact_upload_session_id
                                     WHERE artifact.id=:artifact_id
                                       AND artifact.team_id=:team_id
                                       AND upload.state='committed'
                                       AND NOT EXISTS (
                                           SELECT 1 FROM pipeline_fanout_expansions expansion
                                            WHERE expansion.pipeline_run_id=:run_id
                                              AND expansion.node_key=:node_key
                                              AND expansion.source_artifact_id=artifact.id
                                       )
                                """),
                                {
                                    "artifact_id": UUID(str(descriptor.get("artifact_id"))),
                                    "team_id": run["team_id"],
                                    "run_id": lease.pipeline_run_id,
                                    "node_key": node.node_key,
                                },
                            )
                        )
                        .mappings()
                        .all()
                    )
                    for row in rows:
                        if (
                            row["artifact_type"] != "loom.fanout-manifest.v1"
                            or row["content_hash"] != descriptor.get("content_sha256")
                            or row["manifest_sha256"] != descriptor.get("manifest_sha256")
                        ):
                            raise ValueError("run-input fanout source descriptor drift")
                else:
                    rows = list(
                        (
                            await session.execute(
                                text("""
                                    SELECT source.id AS source_stage_run_id,
                                           source.shard_key AS source_shard_key,
                                           artifact.id AS artifact_id, artifact.artifact_type,
                                           artifact.content_hash, artifact.storage,
                                           artifact.manifest_sha256, artifact.team_id,
                                           upload.prefix, upload.manifest_sha256 AS root_manifest_sha256,
                                           upload.committed_marker_sha256
                                      FROM pipeline_stage_runs source
                                      JOIN artifacts artifact
                                        ON artifact.pipeline_stage_run_id=source.id
                                       AND artifact.name=:output_name
                                       AND artifact.producer_kind='platform'
                                      JOIN artifact_upload_sessions upload
                                        ON upload.id=artifact.artifact_upload_session_id
                                     WHERE source.pipeline_run_id=:run_id
                                       AND source.node_key=:source_node_key
                                       AND source.state='succeeded'
                                       AND artifact.team_id=:team_id
                                       AND upload.state='committed'
                                       AND NOT EXISTS (
                                           SELECT 1 FROM pipeline_fanout_expansions expansion
                                            WHERE expansion.pipeline_run_id=:run_id
                                              AND expansion.node_key=:node_key
                                              AND expansion.source_artifact_id=artifact.id
                                       )
                                     ORDER BY source.shard_key
                                """),
                                {
                                    "output_name": node.fanout.manifest_output_name,
                                    "run_id": lease.pipeline_run_id,
                                    "source_node_key": node.fanout.manifest_stage_key,
                                    "team_id": run["team_id"],
                                    "node_key": node.node_key,
                                },
                            )
                        )
                        .mappings()
                        .all()
                    )
                for row in rows:
                    storage = cast(dict[str, Any], row["storage"])
                    files = cast(list[dict[str, Any]], storage.get("files", []))
                    semantic = [item for item in files if item.get("role") == "semantic_document"]
                    if (
                        row["artifact_type"] != "loom.fanout-manifest.v1"
                        or row["team_id"] != run["team_id"]
                        or row["manifest_sha256"] is None
                        or row["root_manifest_sha256"] is None
                        or row["committed_marker_sha256"] is None
                        or len(semantic) != 1
                        or semantic[0].get("sha256") != row["content_hash"]
                    ):
                        raise ValueError("fanout source Artifact storage drift")
                    relative_path = str(semantic[0].get("relative_path"))
                    size_bytes = semantic[0].get("size_bytes")
                    if (
                        not relative_path
                        or relative_path.startswith("/")
                        or ".." in relative_path.split("/")
                        or not isinstance(size_bytes, int)
                        or isinstance(size_bytes, bool)
                        or size_bytes < 1
                    ):
                        raise ValueError("fanout source Artifact path drift")
                    prefix = str(row["prefix"])
                    artifact_id = cast(UUID, row["artifact_id"])
                    candidates.append(
                        FanoutSourceCandidate(
                            node_key=node.node_key,
                            source_kind=node.fanout.source,
                            source_stage_run_id=row.get("source_stage_run_id"),
                            source_artifact_id=artifact_id,
                            source_manifest_digest=cast(str, row["content_hash"]),
                            source_file_key=(
                                f"{prefix}artifacts/{artifact_id}/{relative_path}"
                            ),
                            source_file_size=size_bytes,
                            source_file_sha256=cast(str, semantic[0]["sha256"]),
                            root_manifest_key=prefix + "_manifest.json",
                            root_manifest_sha256=cast(str, row["root_manifest_sha256"]),
                            committed_marker_key=prefix + "_COMMITTED",
                            committed_marker_sha256=cast(
                                str, row["committed_marker_sha256"]
                            ),
                            parameters_contract_digest=parameters_contract_digest,
                        )
                    )
            return tuple(candidates)

    async def expand_fanout(
        self,
        lease: RunLease,
        *,
        node_key: str,
        source_kind: str,
        source_artifact_id: UUID,
        source_manifest_digest: str,
        manifest: FanoutManifestV1,
        source_stage_run_id: UUID | None = None,
        run_input_parameters_validated: bool = False,
    ) -> int:
        """Atomically insert one marker, every child/mirrored gate, and counters."""

        if source_kind not in {"run_input", "stage_output"}:
            raise ValueError("invalid fanout source kind")
        if (source_kind == "run_input") != (source_stage_run_id is None):
            raise ValueError("source StageRun is required exactly for stage-output fanout")
        if source_kind == "run_input" and not run_input_parameters_validated:
            raise ValueError("run-input fanout parameters contract must be validated before commit")
        async with self._sessions() as session, session.begin():
            await self._lock_fence(session, lease)
            run = (
                (
                    await session.execute(
                        text("""
                        SELECT graph_spec_json, team_id FROM pipeline_runs
                         WHERE id=:run_id FOR UPDATE
                    """),
                        {"run_id": lease.pipeline_run_id},
                    )
                )
                .mappings()
                .one()
            )
            graph = RunGraphSpecV1.model_validate(run["graph_spec_json"])
            node = next((item for item in graph.nodes if item.node_key == node_key), None)
            if not isinstance(node, ContainerNodeV1) or node.fanout is None:
                raise ValueError("fanout expansion node is absent from the immutable graph")
            if node.fanout.source != source_kind:
                raise ValueError("fanout source kind drift")
            validate_fanout_manifest(manifest, node.fanout)
            source = (
                (
                    await session.execute(
                        text("""
                        SELECT artifact_type, content_hash, team_id, pipeline_run_id,
                               pipeline_stage_run_id, producer_kind
                          FROM artifacts WHERE id=:artifact_id FOR UPDATE
                    """),
                        {"artifact_id": source_artifact_id},
                    )
                )
                .mappings()
                .one()
            )
            if (
                source["team_id"] != run["team_id"]
                or source["artifact_type"] != "loom.fanout-manifest.v1"
                or source["content_hash"] != source_manifest_digest
            ):
                raise ValueError("fanout source Artifact identity/type/digest/team drift")
            if source_kind == "stage_output" and (
                source["pipeline_run_id"] != lease.pipeline_run_id
                or source["pipeline_stage_run_id"] != source_stage_run_id
                or source["producer_kind"] != "platform"
            ):
                raise ValueError("stage-output fanout source provenance drift")
            for item in manifest.items:
                binding = item.artifact_bindings[0]
                valid_item = (
                    await session.execute(
                        text("""
                            SELECT 1 FROM artifacts
                             WHERE id=:artifact_id AND team_id=:team_id
                               AND artifact_type=:artifact_type
                        """),
                        {
                            "artifact_id": binding.artifact_id,
                            "team_id": run["team_id"],
                            "artifact_type": binding.artifact_type,
                        },
                    )
                ).scalar_one_or_none()
                if valid_item is None:
                    raise ValueError("fanout item Artifact identity/type/team drift")
            fanout_digest = canonical_digest(node.fanout)
            existing = (
                (
                    await session.execute(
                        text("""
                        SELECT id, source_manifest_digest, fanout_spec_digest, item_count
                          FROM pipeline_fanout_expansions
                         WHERE pipeline_run_id=:run_id AND node_key=:node_key
                           AND source_artifact_id=:artifact_id FOR UPDATE
                    """),
                        {
                            "run_id": lease.pipeline_run_id,
                            "node_key": node_key,
                            "artifact_id": source_artifact_id,
                        },
                    )
                )
                .mappings()
                .one_or_none()
            )
            if existing is not None:
                if (
                    existing["source_manifest_digest"] != source_manifest_digest
                    or existing["fanout_spec_digest"] != fanout_digest
                    or existing["item_count"] != len(manifest.items)
                ):
                    raise BudgetReservationConflictError("fanout expansion replay drift")
                return 0
            gates = [
                gate
                for gate in graph.nodes
                if isinstance(gate, OutcomeGateNodeV1) and gate.subject_stage_key == node_key
            ]
            count = len(manifest.items) * (1 + len(gates))
            ledger = (
                (
                    await session.execute(
                        text("""
                        SELECT stage_run_limit, stage_runs_created, terminal_cause
                          FROM pipeline_budget_ledgers WHERE pipeline_run_id=:run_id FOR UPDATE
                    """),
                        {"run_id": lease.pipeline_run_id},
                    )
                )
                .mappings()
                .one()
            )
            if ledger["terminal_cause"] is not None:
                raise BudgetExceededError(TerminalCause(ledger["terminal_cause"]))
            if count > ledger["stage_run_limit"] - ledger["stage_runs_created"]:
                await self._latch_terminal_cause(session, lease, TerminalCause.STAGE_RUN_BUDGET)
                return 0
            expansion_id = canonical_uuid5(
                lease.pipeline_run_id,
                {
                    "kind": "pipeline_fanout_expansion",
                    "node_key": node_key,
                    "source_artifact_id": str(source_artifact_id),
                },
            )
            await session.execute(
                text("""
                    INSERT INTO pipeline_fanout_expansions (
                        id, pipeline_run_id, node_key, source_kind, source_stage_run_id,
                        source_artifact_id, source_manifest_digest, fanout_spec_digest, item_count
                    ) VALUES (
                        :id, :run_id, :node_key, :source_kind, :source_stage_id,
                        :artifact_id, :manifest_digest, :fanout_digest, :item_count
                    )
                """),
                {
                    "id": expansion_id,
                    "run_id": lease.pipeline_run_id,
                    "node_key": node_key,
                    "source_kind": source_kind,
                    "source_stage_id": source_stage_run_id,
                    "artifact_id": source_artifact_id,
                    "manifest_digest": source_manifest_digest,
                    "fanout_digest": fanout_digest,
                    "item_count": len(manifest.items),
                },
            )
            for item in manifest.items:
                stage_id = canonical_uuid5(
                    lease.pipeline_run_id,
                    {
                        "kind": "pipeline_stage_run",
                        "node_key": node_key,
                        "shard_key": item.shard_key,
                        "source_artifact_id": str(source_artifact_id),
                    },
                )
                resource_json = {"resource_profile": node.resource_profile}
                renderer_json = (
                    node.request_renderer.model_dump(mode="json")
                    if node.request_renderer is not None
                    else None
                )
                await session.execute(
                    text("""
                        INSERT INTO pipeline_stage_runs (
                            id, pipeline_run_id, node_key, shard_key, node_kind, state,
                            resource_profile_json, resource_profile_digest,
                            fanout_parameters_json, fanout_item_json, fanout_item_digest,
                            fanout_expansion_id,
                            request_renderer_json, request_renderer_digest, failure_policy
                        ) VALUES (
                            :id, :run_id, :node_key, :shard_key, 'container', 'blocked',
                            CAST(:resource AS jsonb), :resource_digest,
                            CAST(:parameters AS jsonb), CAST(:item AS jsonb),
                            :item_digest, :expansion_id,
                            CAST(:renderer AS jsonb), :renderer_digest, :failure_policy
                        )
                    """),
                    {
                        "id": stage_id,
                        "run_id": lease.pipeline_run_id,
                        "node_key": node_key,
                        "shard_key": item.shard_key,
                        "resource": _json_text(resource_json),
                        "resource_digest": canonical_digest(resource_json),
                        "parameters": _json_text(item.parameters),
                        "item": _json_text(item.model_dump(mode="json")),
                        "item_digest": digest_bytes(canonical_document(item)),
                        "expansion_id": expansion_id,
                        "renderer": _json_text(renderer_json) if renderer_json else None,
                        "renderer_digest": (
                            node.request_renderer.digest if node.request_renderer else None
                        ),
                        "failure_policy": node.failure_policy,
                    },
                )
                for need_key in node.needs:
                    source_shard_binding = any(
                        isinstance(binding, StageOutputBindingV1)
                        and binding.stage_key == need_key
                        and binding.shard_selection == "fanout_source_shard"
                        for binding in node.inputs
                    )
                    if source_shard_binding and source_stage_run_id is not None:
                        upstream = (
                            (
                                await session.execute(
                                    text("""
                                    SELECT id, node_kind
                                      FROM pipeline_stage_runs
                                     WHERE id=:source_stage_id
                                       AND pipeline_run_id=:run_id
                                       AND node_key=:need_key
                                """),
                                    {
                                        "run_id": lease.pipeline_run_id,
                                        "need_key": need_key,
                                        "source_stage_id": source_stage_run_id,
                                    },
                                )
                            )
                            .mappings()
                            .one_or_none()
                        )
                    else:
                        upstream = (
                            (
                                await session.execute(
                                    text("""
                                SELECT id, node_kind
                                  FROM pipeline_stage_runs
                                 WHERE pipeline_run_id=:run_id AND node_key=:need_key
                                   AND shard_key IN (:shard_key, 'singleton')
                                 ORDER BY (shard_key=:shard_key) DESC
                                 LIMIT 1
                            """),
                                {
                                    "run_id": lease.pipeline_run_id,
                                    "need_key": need_key,
                                    "shard_key": item.shard_key,
                                },
                                )
                            )
                            .mappings()
                            .one_or_none()
                        )
                    if upstream is None:
                        continue
                    dependency_kind = "required"
                    if (
                        node.request_renderer is not None
                        and need_key in node.request_renderer.terminal_stage_keys
                    ):
                        dependency_kind = "terminal_barrier"
                    upstream_node = next(
                        graph_node for graph_node in graph.nodes if graph_node.node_key == need_key
                    )
                    if isinstance(upstream_node, OutcomeGateNodeV1):
                        dependency_kind = (
                            "gate_matched"
                            if node_key in upstream_node.matched_targets
                            else "gate_unmatched"
                        )
                    await session.execute(
                        text("""
                            INSERT INTO pipeline_stage_dependencies (
                                pipeline_run_id, upstream_stage_run_id,
                                downstream_stage_run_id, dependency_kind, selected
                            ) VALUES (:run_id, :upstream, :downstream, :kind, :selected)
                            ON CONFLICT DO NOTHING
                        """),
                        {
                            "run_id": lease.pipeline_run_id,
                            "upstream": upstream["id"],
                            "downstream": stage_id,
                            "kind": dependency_kind,
                            "selected": (None if dependency_kind.startswith("gate_") else True),
                        },
                    )
                for downstream in graph.nodes:
                    if (
                        not isinstance(downstream, ContainerNodeV1)
                        or downstream.fanout is not None
                        or node_key not in downstream.needs
                    ):
                        continue
                    downstream_id = (
                        await session.execute(
                            text("""
                                SELECT id FROM pipeline_stage_runs
                                 WHERE pipeline_run_id=:run_id
                                   AND node_key=:node_key AND shard_key='singleton'
                            """),
                            {
                                "run_id": lease.pipeline_run_id,
                                "node_key": downstream.node_key,
                            },
                        )
                    ).scalar_one_or_none()
                    if downstream_id is None:
                        continue
                    dependency_kind = (
                        "terminal_barrier"
                        if downstream.request_renderer is not None
                        and node_key in downstream.request_renderer.terminal_stage_keys
                        else "required"
                    )
                    await session.execute(
                        text("""
                            INSERT INTO pipeline_stage_dependencies (
                                pipeline_run_id, upstream_stage_run_id,
                                downstream_stage_run_id, dependency_kind, selected
                            ) VALUES (:run_id, :upstream, :downstream, :kind, true)
                            ON CONFLICT DO NOTHING
                        """),
                        {
                            "run_id": lease.pipeline_run_id,
                            "upstream": stage_id,
                            "downstream": downstream_id,
                            "kind": dependency_kind,
                        },
                    )
                for gate in gates:
                    gate_id = canonical_uuid5(
                        lease.pipeline_run_id,
                        {
                            "kind": "pipeline_stage_run",
                            "node_key": gate.node_key,
                            "shard_key": item.shard_key,
                            "source_artifact_id": str(source_artifact_id),
                        },
                    )
                    await session.execute(
                        text("""
                            INSERT INTO pipeline_stage_runs (
                                id, pipeline_run_id, node_key, shard_key, node_kind,
                                state, gate_subject_stage_run_id
                            ) VALUES (
                                :id, :run_id, :node_key, :shard_key, 'gate', 'blocked', :subject_id
                            )
                        """),
                        {
                            "id": gate_id,
                            "run_id": lease.pipeline_run_id,
                            "node_key": gate.node_key,
                            "shard_key": item.shard_key,
                            "subject_id": stage_id,
                        },
                    )
                    for downstream in graph.nodes:
                        if (
                            not isinstance(downstream, ContainerNodeV1)
                            or downstream.fanout is not None
                            or gate.node_key not in downstream.needs
                        ):
                            continue
                        downstream_id = (
                            await session.execute(
                                text("""
                                    SELECT id FROM pipeline_stage_runs
                                     WHERE pipeline_run_id=:run_id
                                       AND node_key=:node_key AND shard_key='singleton'
                                """),
                                {
                                    "run_id": lease.pipeline_run_id,
                                    "node_key": downstream.node_key,
                                },
                            )
                        ).scalar_one_or_none()
                        if downstream_id is None:
                            continue
                        dependency_kind = (
                            "gate_matched"
                            if downstream.node_key in gate.matched_targets
                            else "gate_unmatched"
                        )
                        await session.execute(
                            text("""
                                INSERT INTO pipeline_stage_dependencies (
                                    pipeline_run_id, upstream_stage_run_id,
                                    downstream_stage_run_id, dependency_kind, selected
                                ) VALUES (:run_id, :upstream, :downstream, :kind, NULL)
                                ON CONFLICT DO NOTHING
                            """),
                            {
                                "run_id": lease.pipeline_run_id,
                                "upstream": gate_id,
                                "downstream": downstream_id,
                                "kind": dependency_kind,
                            },
                        )
                    await session.execute(
                        text("""
                            INSERT INTO pipeline_stage_dependencies (
                                pipeline_run_id, upstream_stage_run_id,
                                downstream_stage_run_id, dependency_kind, selected
                            ) VALUES (:run_id, :subject_id, :gate_id, 'required', true)
                        """),
                        {
                            "run_id": lease.pipeline_run_id,
                            "subject_id": stage_id,
                            "gate_id": gate_id,
                        },
                    )
            await session.execute(
                text("""
                    UPDATE pipeline_budget_ledgers
                       SET stage_runs_created=stage_runs_created+:count,
                           version=version+1, updated_at=clock_timestamp()
                     WHERE pipeline_run_id=:run_id
                """),
                {"count": count, "run_id": lease.pipeline_run_id},
            )
            await self._append_event(
                session,
                lease,
                event_type="fanout_expanded",
                payload={
                    "item_count": len(manifest.items),
                    "node_key": node_key,
                    "source_manifest_digest": source_manifest_digest,
                    "stage_runs_created": count,
                },
            )
            return count

    async def reserve_budget(
        self,
        lease: RunLease,
        *,
        kind: BudgetKind,
        reservation_key: str,
        request_digest: str,
        amount: int,
        execution_attempt_id: UUID | None,
        metadata: dict[str, Any] | None = None,
    ) -> ReservationRecord:
        if amount < 0:
            raise ValueError("reservation amount cannot be negative")
        limit_col, reserved_col, settled_col, cause = _COUNTERS[kind]
        exhausted = False
        record: ReservationRecord | None = None
        async with self._sessions() as session, session.begin():
            await self._lock_fence(session, lease)
            existing = (
                (
                    await session.execute(
                        text("""
                        SELECT id, pipeline_run_id, kind, reservation_key, request_digest,
                               reserved_amount, settled_amount, state
                          FROM pipeline_budget_reservations
                         WHERE pipeline_run_id = :run_id AND kind = :kind
                           AND reservation_key = :key
                         FOR UPDATE
                    """),
                        {
                            "run_id": lease.pipeline_run_id,
                            "kind": kind.value,
                            "key": reservation_key,
                        },
                    )
                )
                .mappings()
                .one_or_none()
            )
            if existing is not None:
                if existing["request_digest"] != request_digest:
                    raise BudgetReservationConflictError("reservation key digest drift")
                return self._reservation_record(existing)
            ledger = (
                (
                    await session.execute(
                        text(
                            f"SELECT {limit_col} AS budget_limit, {reserved_col} AS reserved, "
                            f"{settled_col} AS settled, terminal_cause "
                            "FROM pipeline_budget_ledgers WHERE pipeline_run_id = :run_id FOR UPDATE"
                        ),
                        {"run_id": lease.pipeline_run_id},
                    )
                )
                .mappings()
                .one()
            )
            if ledger["terminal_cause"] is not None:
                raise BudgetExceededError(TerminalCause(ledger["terminal_cause"]))
            if amount > ledger["budget_limit"] - ledger["reserved"] - ledger["settled"]:
                await self._latch_terminal_cause(session, lease, cause)
                exhausted = True
            else:
                reservation_id = uuid4()
                await session.execute(
                    text("""
                        INSERT INTO pipeline_budget_reservations (
                            id, pipeline_run_id, execution_attempt_id, kind, reservation_key,
                            request_digest, reserved_amount, metadata_json
                        ) VALUES (
                            :id, :run_id, :attempt_id, :kind, :key, :digest, :amount,
                            CAST(:metadata AS jsonb)
                        )
                    """),
                    {
                        "id": reservation_id,
                        "run_id": lease.pipeline_run_id,
                        "attempt_id": execution_attempt_id,
                        "kind": kind.value,
                        "key": reservation_key,
                        "digest": request_digest,
                        "amount": amount,
                        "metadata": _json_text(metadata or {}),
                    },
                )
                await session.execute(
                    text(
                        f"UPDATE pipeline_budget_ledgers SET {reserved_col} = {reserved_col} + :amount, "
                        "version = version + 1, updated_at = clock_timestamp() "
                        "WHERE pipeline_run_id = :run_id"
                    ),
                    {"amount": amount, "run_id": lease.pipeline_run_id},
                )
                await self._append_event(
                    session,
                    lease,
                    event_type="budget_reserved",
                    payload={
                        "kind": kind.value,
                        "reservation_key": reservation_key,
                        "amount": amount,
                    },
                    execution_attempt_id=execution_attempt_id,
                )
                record = ReservationRecord(
                    id=reservation_id,
                    pipeline_run_id=lease.pipeline_run_id,
                    kind=kind,
                    reservation_key=reservation_key,
                    request_digest=request_digest,
                    reserved_amount=amount,
                    settled_amount=None,
                    state="active",
                )
        if exhausted:
            raise BudgetExceededError(cause)
        assert record is not None
        return record

    async def freeze_readiness(
        self,
        lease: RunLease,
        *,
        stage_run_id: UUID,
        frozen: FrozenReadiness,
        terminal_snapshot: FrozenTerminalSnapshot | None = None,
    ) -> bool:
        """Phase 1: persist immutable bindings/spec and ready, but no Attempt."""

        optional_runtime = (
            frozen.image_runtime_contract_json,
            frozen.image_runtime_contract_digest,
        )
        if (optional_runtime[0] is None) != (optional_runtime[1] is None):
            raise ValueError("image runtime contract snapshot group is incomplete")
        optional_profile = (
            frozen.resource_profile_json,
            frozen.resource_profile_digest,
        )
        if (optional_profile[0] is None) != (optional_profile[1] is None):
            raise ValueError("resource profile snapshot group is incomplete")

        async with self._sessions() as session, session.begin():
            await self._lock_fence(session, lease)
            row = (
                (
                    await session.execute(
                        text("""
                        SELECT state, resolved_input_bindings_digest, execution_spec_digest,
                               resource_profile_digest, image_runtime_contract_digest,
                               attempt_count
                          FROM pipeline_stage_runs
                         WHERE id = :stage_id AND pipeline_run_id = :run_id
                         FOR UPDATE
                    """),
                        {"stage_id": stage_run_id, "run_id": lease.pipeline_run_id},
                    )
                )
                .mappings()
                .one()
            )
            if row["resolved_input_bindings_digest"] is not None:
                if (
                    row["resolved_input_bindings_digest"] != frozen.input_bindings_digest
                    or row["execution_spec_digest"] != frozen.execution_spec_digest
                    or (
                        frozen.resource_profile_digest is not None
                        and row["resource_profile_digest"] != frozen.resource_profile_digest
                    )
                    or (
                        frozen.image_runtime_contract_digest is not None
                        and row["image_runtime_contract_digest"]
                        != frozen.image_runtime_contract_digest
                    )
                ):
                    raise BudgetReservationConflictError("frozen readiness replay drift")
                if terminal_snapshot is not None:
                    persisted = (
                        await session.execute(
                            text("""
                                SELECT snapshot_digest FROM pipeline_terminal_snapshots
                                 WHERE consumer_stage_run_id=:stage_id
                            """),
                            {"stage_id": stage_run_id},
                        )
                    ).scalar_one_or_none()
                    if persisted != terminal_snapshot.snapshot_digest:
                        raise BudgetReservationConflictError("terminal snapshot replay drift")
                return False
            if row["state"] != "blocked" or row["attempt_count"] != 0:
                raise ValueError("only an unattempted blocked StageRun may freeze readiness")
            if terminal_snapshot is not None:
                await session.execute(
                    text("""
                        INSERT INTO pipeline_terminal_snapshots (
                            id, pipeline_run_id, consumer_stage_run_id, renderer_digest,
                            run_graph_digest, terminal_stage_keys_json, stages_json,
                            snapshot_json, snapshot_bytes, snapshot_digest
                        ) VALUES (
                            :id, :run_id, :stage_id, :renderer_digest, :graph_digest,
                            CAST(:terminal_keys AS jsonb), CAST(:stages AS jsonb),
                            CAST(:snapshot AS jsonb), :snapshot_bytes, :snapshot_digest
                        )
                    """),
                    {
                        "id": terminal_snapshot.id,
                        "run_id": lease.pipeline_run_id,
                        "stage_id": stage_run_id,
                        "renderer_digest": terminal_snapshot.renderer_digest,
                        "graph_digest": terminal_snapshot.run_graph_digest,
                        "terminal_keys": _json_text(terminal_snapshot.terminal_stage_keys_json),
                        "stages": _json_text(terminal_snapshot.stages_json),
                        "snapshot": _json_text(terminal_snapshot.snapshot_json),
                        "snapshot_bytes": terminal_snapshot.snapshot_bytes,
                        "snapshot_digest": terminal_snapshot.snapshot_digest,
                    },
                )
            await session.execute(
                text("""
                    UPDATE pipeline_stage_runs
                       SET resolved_input_bindings_json = CAST(:bindings AS jsonb),
                           resolved_input_bindings_digest = :bindings_digest,
                           resolved_execution_spec_json = CAST(:spec AS jsonb),
                           resolved_execution_spec_bytes = :spec_bytes,
                           execution_spec_digest = :spec_digest,
                           resource_profile_json = COALESCE(
                               CAST(:resource_profile AS jsonb), resource_profile_json),
                           resource_profile_digest = COALESCE(
                               :resource_profile_digest, resource_profile_digest),
                           image_runtime_contract_json = COALESCE(
                               CAST(:image_contract AS jsonb), image_runtime_contract_json),
                           image_runtime_contract_digest = COALESCE(
                               :image_contract_digest, image_runtime_contract_digest),
                           provider_connection_ref = :provider_connection_ref,
                           secret_refs = CAST(:secret_refs AS text[]),
                           state = 'ready', ready_at = clock_timestamp(), version = version + 1
                     WHERE id = :stage_id AND pipeline_run_id = :run_id
                """),
                {
                    "bindings": _json_text(frozen.input_bindings_json),
                    "bindings_digest": frozen.input_bindings_digest,
                    "spec": _json_text(frozen.execution_spec_json),
                    "spec_bytes": frozen.execution_spec_bytes,
                    "spec_digest": frozen.execution_spec_digest,
                    "resource_profile": (
                        _json_text(frozen.resource_profile_json)
                        if frozen.resource_profile_json is not None
                        else None
                    ),
                    "resource_profile_digest": frozen.resource_profile_digest,
                    "image_contract": (
                        _json_text(frozen.image_runtime_contract_json)
                        if frozen.image_runtime_contract_json is not None
                        else None
                    ),
                    "image_contract_digest": frozen.image_runtime_contract_digest,
                    "provider_connection_ref": frozen.provider_connection_ref,
                    "secret_refs": list(frozen.secret_refs),
                    "stage_id": stage_run_id,
                    "run_id": lease.pipeline_run_id,
                },
            )
            await self._append_event(
                session,
                lease,
                event_type="stage_ready",
                payload={"execution_spec_digest": frozen.execution_spec_digest},
                stage_run_id=stage_run_id,
            )
            return True

    async def fail_renderer(
        self, lease: RunLease, *, stage_run_id: UUID, detail: str = "stage_request_invalid"
    ) -> bool:
        """Persist the one post-freeze renderer failure with zero Attempts."""

        async with self._sessions() as session, session.begin():
            await self._lock_fence(session, lease)
            result = await session.execute(
                text("""
                    UPDATE pipeline_stage_runs
                       SET state = 'failed', reason_code = :reason,
                           finished_at = clock_timestamp(), version = version + 1
                     WHERE id = :stage_id AND pipeline_run_id = :run_id
                       AND state = 'ready' AND attempt_count = 0
                       AND resolved_execution_spec_json IS NOT NULL
                    RETURNING id
                """),
                {
                    "stage_id": stage_run_id,
                    "run_id": lease.pipeline_run_id,
                    "reason": detail,
                },
            )
            if result.scalar_one_or_none() is None:
                existing = (
                    (
                        await session.execute(
                            text("""
                            SELECT state, reason_code, attempt_count
                              FROM pipeline_stage_runs
                             WHERE id = :stage_id AND pipeline_run_id = :run_id
                        """),
                            {"stage_id": stage_run_id, "run_id": lease.pipeline_run_id},
                        )
                    )
                    .mappings()
                    .one()
                )
                if (
                    existing["state"] == "failed"
                    and existing["reason_code"] == detail
                    and existing["attempt_count"] == 0
                ):
                    return False
                raise BudgetReservationConflictError("renderer failure lost its Phase-1 fence")
            await self._append_event(
                session,
                lease,
                event_type="stage_failed",
                payload={"reason_code": detail},
                stage_run_id=stage_run_id,
            )
            return True

    async def create_attempt(
        self,
        lease: RunLease,
        *,
        stage_run_id: UUID,
        attempt_id: UUID,
        stage_request_json: dict[str, Any] | None,
        stage_request_bytes: bytes | None,
        stage_request_digest: str | None,
        reservations: tuple[AttemptReservationSpec, ...],
        provider_budget: AttemptProviderBudgetSpec | None = None,
        fault_pending: bool = False,
    ) -> AttemptRecord:
        """Phase 2: one fenced Attempt, all counters, reservations, and event."""

        request_group = (stage_request_json, stage_request_bytes, stage_request_digest)
        if any(value is None for value in request_group) and not all(
            value is None for value in request_group
        ):
            raise ValueError("StageRequest snapshot group is incomplete")
        if stage_request_json is not None and (
            stage_request_bytes != canonical_document(stage_request_json)
            or stage_request_digest != canonical_digest(stage_request_json)
        ):
            raise ValueError("StageRequest bytes or digest drift")

        exhausted: TerminalCause | None = None
        record: AttemptRecord | None = None
        async with self._sessions() as session, session.begin():
            await self._lock_fence(session, lease)
            existing = (
                (
                    await session.execute(
                        text("""
                        SELECT id, stage_run_id, attempt_number, state, stage_request_digest
                          FROM execution_attempts WHERE id = :attempt_id FOR UPDATE
                    """),
                        {"attempt_id": attempt_id},
                    )
                )
                .mappings()
                .one_or_none()
            )
            if existing is not None:
                if (
                    existing["stage_run_id"] != stage_run_id
                    or existing["stage_request_digest"] != stage_request_digest
                ):
                    raise BudgetReservationConflictError("Attempt UUID replay drift")
                persisted_reservations = (
                    (
                        await session.execute(
                            text("""
                            SELECT kind, reservation_key, request_digest,
                                   reserved_amount, metadata_json
                              FROM pipeline_budget_reservations
                             WHERE execution_attempt_id=:attempt_id
                             ORDER BY kind, reservation_key
                        """),
                            {"attempt_id": attempt_id},
                        )
                    )
                    .mappings()
                    .all()
                )
                expected_reservations = sorted(
                    [
                        (
                            reservation.kind.value,
                            reservation.reservation_key,
                            reservation.request_digest,
                            reservation.amount,
                            reservation.metadata,
                        )
                        for reservation in reservations
                    ],
                    key=lambda item: (item[0], item[1]),
                )
                actual_reservations = [
                    (
                        row["kind"],
                        row["reservation_key"],
                        row["request_digest"],
                        row["reserved_amount"],
                        row["metadata_json"],
                    )
                    for row in persisted_reservations
                ]
                if actual_reservations != expected_reservations:
                    raise BudgetReservationConflictError("Attempt reservation replay drift")
                persisted_provider_budget = (
                    (
                        await session.execute(
                            text("""
                            SELECT binding_snapshot_sha256, request_limit,
                                   cost_limit_microusd, per_call_timeout_seconds
                              FROM execution_attempt_provider_budgets
                             WHERE attempt_id=:attempt_id
                        """),
                            {"attempt_id": attempt_id},
                        )
                    )
                    .mappings()
                    .one_or_none()
                )
                expected_provider_budget = (
                    {
                        "binding_snapshot_sha256": provider_budget.binding_snapshot_sha256,
                        "request_limit": provider_budget.request_limit,
                        "cost_limit_microusd": provider_budget.cost_limit_microusd,
                        "per_call_timeout_seconds": provider_budget.per_call_timeout_seconds,
                    }
                    if provider_budget is not None
                    else None
                )
                if (
                    dict(persisted_provider_budget)
                    if persisted_provider_budget is not None
                    else None
                ) != expected_provider_budget:
                    raise BudgetReservationConflictError("Attempt provider budget replay drift")
                return AttemptRecord(**existing)
            stage = (
                (
                    await session.execute(
                        text("""
                        SELECT state, attempt_count, execution_spec_digest,
                               resolved_execution_spec_json, resolved_input_bindings_digest
                          FROM pipeline_stage_runs
                         WHERE id = :stage_id AND pipeline_run_id = :run_id FOR UPDATE
                    """),
                        {"stage_id": stage_run_id, "run_id": lease.pipeline_run_id},
                    )
                )
                .mappings()
                .one()
            )
            if stage["state"] not in {"ready", "retry_wait"}:
                raise ValueError("Attempt creation requires ready or due retry_wait")
            attempt_number = int(stage["attempt_count"]) + 1
            if attempt_number > 3:
                raise ValueError("v1 forbids Attempt 4")
            ledger = (
                (
                    await session.execute(
                        text(
                            "SELECT * FROM pipeline_budget_ledgers WHERE pipeline_run_id=:run_id FOR UPDATE"
                        ),
                        {"run_id": lease.pipeline_run_id},
                    )
                )
                .mappings()
                .one()
            )
            if ledger["terminal_cause"] is not None:
                exhausted = TerminalCause(ledger["terminal_cause"])
            elif ledger["attempts_created"] >= ledger["attempt_limit"]:
                exhausted = TerminalCause.ATTEMPT_BUDGET
            totals = {kind: 0 for kind in BudgetKind}
            reservation_identities: set[tuple[BudgetKind, str]] = set()
            for reservation in reservations:
                if reservation.amount < 0:
                    raise ValueError("reservation amount cannot be negative")
                identity = (reservation.kind, reservation.reservation_key)
                if identity in reservation_identities:
                    raise BudgetReservationConflictError("Attempt reservation key is duplicated")
                reservation_identities.add(identity)
                totals[reservation.kind] += reservation.amount
            if exhausted is None:
                for kind, amount in totals.items():
                    limit_col, reserved_col, settled_col, cause = _COUNTERS[kind]
                    if amount > ledger[limit_col] - ledger[reserved_col] - ledger[settled_col]:
                        exhausted = cause
                        break
            resumed_checkpoint_artifact_id: UUID | None = None
            if exhausted is None and attempt_number > 1:
                previous_reason = (
                    await session.execute(
                        text("""
                            SELECT reason_code FROM execution_attempts
                             WHERE stage_run_id=:stage_id AND attempt_number=:number
                        """),
                        {"stage_id": stage_run_id, "number": attempt_number - 1},
                    )
                ).scalar_one_or_none()
                if previous_reason is not None:
                    frozen = stage["resolved_execution_spec_json"] or {}
                    container = frozen.get("container_node") or {}
                    recipe_digest = frozen.get("recipe_digest")
                    image_digest = container.get("image")
                    bindings_digest = stage["resolved_input_bindings_digest"]
                    execution_digest = stage["execution_spec_digest"]
                    if all(
                        isinstance(value, str)
                        for value in (
                            recipe_digest,
                            image_digest,
                            bindings_digest,
                            execution_digest,
                        )
                    ):
                        checkpoint = (
                            (
                                await session.execute(
                                    text("""
                                        SELECT artifact_id, recipe_digest,
                                               resolved_input_bindings_digest,
                                               execution_spec_digest, image_digest,
                                               resume_compatibility_key
                                          FROM pipeline_checkpoints
                                         WHERE pipeline_stage_run_id=:stage_id
                                         ORDER BY attempt_number DESC, checkpoint_sequence DESC
                                         LIMIT 1
                                    """),
                                    {"stage_id": stage_run_id},
                                )
                            )
                            .mappings()
                            .one_or_none()
                        )
                        if checkpoint is not None:
                            if checkpoint_is_resume_compatible(
                                previous_reason=previous_reason,
                                recipe_digest=cast(str, recipe_digest),
                                resolved_input_bindings_digest=bindings_digest,
                                execution_spec_digest=execution_digest,
                                image_digest=cast(str, image_digest),
                                observed_recipe_digest=checkpoint["recipe_digest"],
                                observed_input_bindings_digest=checkpoint[
                                    "resolved_input_bindings_digest"
                                ],
                                observed_execution_spec_digest=checkpoint["execution_spec_digest"],
                                observed_image_digest=checkpoint["image_digest"],
                                observed_resume_compatibility_key=checkpoint[
                                    "resume_compatibility_key"
                                ],
                            ):
                                resumed_checkpoint_artifact_id = checkpoint["artifact_id"]
                            elif previous_reason in {
                                "worker_lost",
                                "node_setup_health",
                                "object_store_transport",
                                "container_start_transient",
                                "stage_helper_transient",
                            }:
                                await self._append_event(
                                    session,
                                    lease,
                                    event_type="checkpoint_incompatible",
                                    payload={"reason_code": "checkpoint_identity_drift"},
                                    stage_run_id=stage_run_id,
                                )
            if exhausted is not None:
                await self._latch_terminal_cause(session, lease, exhausted)
            else:
                state = "fault_pending" if fault_pending else "queued"
                await session.execute(
                    text("""
                        INSERT INTO execution_attempts (
                            id, stage_run_id, attempt_number, state,
                            stage_request_json, stage_request_bytes, stage_request_digest,
                            resumed_checkpoint_artifact_id, queued_at
                        ) VALUES (
                            :id, :stage_id, :number, :state, CAST(:request AS jsonb),
                            :request_bytes, :request_digest, :checkpoint_artifact_id,
                            CASE WHEN :state = 'queued' THEN clock_timestamp() ELSE NULL END
                        )
                    """),
                    {
                        "id": attempt_id,
                        "stage_id": stage_run_id,
                        "number": attempt_number,
                        "state": state,
                        "request": (
                            None if stage_request_json is None else _json_text(stage_request_json)
                        ),
                        "request_bytes": stage_request_bytes,
                        "request_digest": stage_request_digest,
                        "checkpoint_artifact_id": resumed_checkpoint_artifact_id,
                    },
                )
                if provider_budget is not None:
                    if (
                        provider_budget.request_limit <= 0
                        or provider_budget.cost_limit_microusd <= 0
                        or provider_budget.per_call_timeout_seconds <= 0
                    ):
                        raise ValueError("Attempt provider budget values must be positive")
                    await session.execute(
                        text("""
                            INSERT INTO execution_attempt_provider_budgets (
                                attempt_id, binding_snapshot_sha256, request_limit,
                                cost_limit_microusd, per_call_timeout_seconds
                            ) VALUES (
                                :attempt_id, :binding_digest, :request_limit,
                                :cost_limit, :timeout
                            )
                        """),
                        {
                            "attempt_id": attempt_id,
                            "binding_digest": provider_budget.binding_snapshot_sha256,
                            "request_limit": provider_budget.request_limit,
                            "cost_limit": provider_budget.cost_limit_microusd,
                            "timeout": provider_budget.per_call_timeout_seconds,
                        },
                    )
                for reservation in reservations:
                    await session.execute(
                        text("""
                            INSERT INTO pipeline_budget_reservations (
                                pipeline_run_id, execution_attempt_id, kind, reservation_key,
                                request_digest, reserved_amount, metadata_json
                            ) VALUES (
                                :run_id, :attempt_id, :kind, :key, :digest, :amount,
                                CAST(:metadata AS jsonb)
                            )
                        """),
                        {
                            "run_id": lease.pipeline_run_id,
                            "attempt_id": attempt_id,
                            "kind": reservation.kind.value,
                            "key": reservation.reservation_key,
                            "digest": reservation.request_digest,
                            "amount": reservation.amount,
                            "metadata": _json_text(reservation.metadata),
                        },
                    )
                assignments = ["attempts_created = attempts_created + 1"]
                params: dict[str, Any] = {"run_id": lease.pipeline_run_id}
                for kind, amount in totals.items():
                    _limit_col, reserved_col, _settled_col, _cause = _COUNTERS[kind]
                    param = f"{kind.value}_amount"
                    assignments.append(f"{reserved_col} = {reserved_col} + :{param}")
                    params[param] = amount
                await session.execute(
                    text(
                        "UPDATE pipeline_budget_ledgers SET "
                        + ", ".join(assignments)
                        + ", version=version+1, updated_at=clock_timestamp() "
                        "WHERE pipeline_run_id=:run_id"
                    ),
                    params,
                )
                await session.execute(
                    text("""
                        UPDATE pipeline_stage_runs
                           SET state = 'queued', attempt_count = :number,
                               next_attempt_at = NULL, version = version + 1
                         WHERE id = :stage_id
                    """),
                    {"stage_id": stage_run_id, "number": attempt_number},
                )
                await self._append_event(
                    session,
                    lease,
                    event_type="attempt_created",
                    payload={
                        "attempt_number": attempt_number,
                        "fault_pending": fault_pending,
                        "stage_request_digest": stage_request_digest,
                        "resumed_checkpoint_artifact_id": (
                            str(resumed_checkpoint_artifact_id)
                            if resumed_checkpoint_artifact_id is not None
                            else None
                        ),
                    },
                    stage_run_id=stage_run_id,
                    execution_attempt_id=attempt_id,
                )
                record = AttemptRecord(
                    id=attempt_id,
                    stage_run_id=stage_run_id,
                    attempt_number=attempt_number,
                    state=state,
                    stage_request_digest=stage_request_digest,
                )
        if exhausted is not None:
            raise BudgetExceededError(exhausted)
        assert record is not None
        return record

    async def reserve_provider_dispatch(
        self,
        lease: RunLease,
        *,
        attempt_id: UUID,
        provider_request_id: UUID,
        request_digest: str,
        worst_case_cost_microusd: int,
        metadata: dict[str, Any] | None = None,
    ) -> ReservationRecord:
        """Reserve one Attempt-local request slot and run-wide provider cost atomically."""

        if worst_case_cost_microusd < 0:
            raise ValueError("provider dispatch bound cannot be negative")
        key = f"provider:{attempt_id}:{provider_request_id}"
        exhausted: TerminalCause | None = None
        local_exhausted = False
        record: ReservationRecord | None = None
        async with self._sessions() as session, session.begin():
            await self._lock_fence(session, lease)
            existing = (
                (
                    await session.execute(
                        text("""
                        SELECT id, pipeline_run_id, kind, reservation_key, request_digest,
                               reserved_amount, settled_amount, state
                          FROM pipeline_budget_reservations
                         WHERE pipeline_run_id=:run_id AND kind='provider'
                           AND reservation_key=:key FOR UPDATE
                    """),
                        {"run_id": lease.pipeline_run_id, "key": key},
                    )
                )
                .mappings()
                .one_or_none()
            )
            if existing is not None:
                if existing["request_digest"] != request_digest:
                    raise BudgetReservationConflictError("provider dispatch replay drift")
                return self._reservation_record(existing)
            attempt_budget = (
                (
                    await session.execute(
                        text("""
                        SELECT b.*, s.pipeline_run_id, a.state AS attempt_state
                          FROM execution_attempt_provider_budgets b
                          JOIN execution_attempts a ON a.id=b.attempt_id
                          JOIN pipeline_stage_runs s ON s.id=a.stage_run_id
                         WHERE b.attempt_id=:attempt_id AND s.pipeline_run_id=:run_id
                         FOR UPDATE OF b, a, s
                    """),
                        {"attempt_id": attempt_id, "run_id": lease.pipeline_run_id},
                    )
                )
                .mappings()
                .one()
            )
            if attempt_budget["attempt_state"] not in {"claimed", "running"}:
                raise ValueError("provider dispatch requires a claimed or running Attempt")
            ledger = (
                (
                    await session.execute(
                        text("""
                        SELECT provider_limit_microusd, provider_reserved_microusd,
                               provider_settled_microusd, terminal_cause
                          FROM pipeline_budget_ledgers
                         WHERE pipeline_run_id=:run_id FOR UPDATE
                    """),
                        {"run_id": lease.pipeline_run_id},
                    )
                )
                .mappings()
                .one()
            )
            if ledger["terminal_cause"] is not None:
                exhausted = TerminalCause(ledger["terminal_cause"])
            elif (
                attempt_budget["requests_reserved"] + attempt_budget["requests_settled"]
                >= attempt_budget["request_limit"]
                or worst_case_cost_microusd
                > attempt_budget["cost_limit_microusd"]
                - attempt_budget["cost_reserved_microusd"]
                - attempt_budget["cost_settled_microusd"]
            ):
                local_exhausted = True
                await self._fail_provider_attempt_budget(session, lease, attempt_id)
            elif worst_case_cost_microusd > (
                ledger["provider_limit_microusd"]
                - ledger["provider_reserved_microusd"]
                - ledger["provider_settled_microusd"]
            ):
                exhausted = TerminalCause.PROVIDER_BUDGET
                await self._latch_terminal_cause(session, lease, exhausted)
            else:
                reservation_id = uuid4()
                await session.execute(
                    text("""
                        INSERT INTO pipeline_budget_reservations (
                            id, pipeline_run_id, execution_attempt_id, kind,
                            reservation_key, request_digest, reserved_amount, metadata_json
                        ) VALUES (
                            :id, :run_id, :attempt_id, 'provider',
                            :key, :digest, :amount, CAST(:metadata AS jsonb)
                        )
                    """),
                    {
                        "id": reservation_id,
                        "run_id": lease.pipeline_run_id,
                        "attempt_id": attempt_id,
                        "key": key,
                        "digest": request_digest,
                        "amount": worst_case_cost_microusd,
                        "metadata": _json_text(metadata or {}),
                    },
                )
                await session.execute(
                    text("""
                        UPDATE execution_attempt_provider_budgets
                           SET requests_reserved=requests_reserved+1,
                               cost_reserved_microusd=cost_reserved_microusd+:amount,
                               version=version+1
                         WHERE attempt_id=:attempt_id
                    """),
                    {"attempt_id": attempt_id, "amount": worst_case_cost_microusd},
                )
                await session.execute(
                    text("""
                        UPDATE pipeline_budget_ledgers
                           SET provider_reserved_microusd=provider_reserved_microusd+:amount,
                               version=version+1, updated_at=clock_timestamp()
                         WHERE pipeline_run_id=:run_id
                    """),
                    {"run_id": lease.pipeline_run_id, "amount": worst_case_cost_microusd},
                )
                await self._append_event(
                    session,
                    lease,
                    event_type="provider_dispatch_reserved",
                    payload={"amount": worst_case_cost_microusd},
                    execution_attempt_id=attempt_id,
                )
                record = ReservationRecord(
                    reservation_id,
                    lease.pipeline_run_id,
                    BudgetKind.PROVIDER,
                    key,
                    request_digest,
                    worst_case_cost_microusd,
                    None,
                    "active",
                )
        if local_exhausted:
            raise AttemptProviderBudgetExceededError("provider_attempt_budget_exhausted")
        if exhausted is not None:
            raise BudgetExceededError(exhausted)
        assert record is not None
        return record

    async def settle_provider_dispatch(
        self,
        lease: RunLease,
        *,
        reservation_id: UUID,
        actual_cost_microusd: int,
    ) -> ReservationRecord:
        """Settle one reached provider request and authoritative cost truth."""

        if actual_cost_microusd < 0:
            raise ValueError("provider settlement cannot be negative")
        async with self._sessions() as session, session.begin():
            await self._lock_fence(session, lease)
            reservation = await self._lock_reservation(session, lease, reservation_id)
            if reservation["kind"] != BudgetKind.PROVIDER.value:
                raise ValueError("provider settlement requires a provider reservation")
            if reservation["state"] == "settled":
                if reservation["settled_amount"] != actual_cost_microusd:
                    raise BudgetReservationConflictError("provider settlement replay drift")
                return self._reservation_record(reservation)
            if reservation["state"] != "active":
                raise BudgetReservationConflictError("released provider request cannot settle")
            attempt_id = reservation["execution_attempt_id"]
            attempt_budget = (
                (
                    await session.execute(
                        text("""
                        SELECT * FROM execution_attempt_provider_budgets
                         WHERE attempt_id=:attempt_id FOR UPDATE
                    """),
                        {"attempt_id": attempt_id},
                    )
                )
                .mappings()
                .one()
            )
            ledger = (
                (
                    await session.execute(
                        text("""
                        SELECT provider_limit_microusd, provider_reserved_microusd,
                               provider_settled_microusd
                          FROM pipeline_budget_ledgers
                         WHERE pipeline_run_id=:run_id FOR UPDATE
                    """),
                        {"run_id": lease.pipeline_run_id},
                    )
                )
                .mappings()
                .one()
            )
            overage = (
                actual_cost_microusd > reservation["reserved_amount"]
                or attempt_budget["cost_settled_microusd"] + actual_cost_microusd
                > attempt_budget["cost_limit_microusd"]
                or ledger["provider_settled_microusd"] + actual_cost_microusd
                > ledger["provider_limit_microusd"]
            )
            if overage:
                await self._latch_terminal_cause(session, lease, TerminalCause.ACCOUNTING_VIOLATION)
                await self._fail_accounting_violation(session, lease, attempt_id)
            await session.execute(
                text("""
                    UPDATE execution_attempt_provider_budgets
                       SET requests_reserved=requests_reserved-1,
                           requests_settled=requests_settled+1,
                           cost_reserved_microusd=cost_reserved_microusd-:reserved,
                           cost_settled_microusd=cost_settled_microusd+:actual,
                           version=version+1
                     WHERE attempt_id=:attempt_id
                """),
                {
                    "attempt_id": attempt_id,
                    "reserved": reservation["reserved_amount"],
                    "actual": actual_cost_microusd,
                },
            )
            await session.execute(
                text("""
                    UPDATE pipeline_budget_ledgers
                       SET provider_reserved_microusd=provider_reserved_microusd-:reserved,
                           provider_settled_microusd=provider_settled_microusd+:actual,
                           version=version+1, updated_at=clock_timestamp()
                     WHERE pipeline_run_id=:run_id
                """),
                {
                    "run_id": lease.pipeline_run_id,
                    "reserved": reservation["reserved_amount"],
                    "actual": actual_cost_microusd,
                },
            )
            await session.execute(
                text("""
                    UPDATE pipeline_budget_reservations
                       SET state='settled', settled_amount=:actual,
                           settled_at=clock_timestamp()
                     WHERE id=:id
                """),
                {"id": reservation_id, "actual": actual_cost_microusd},
            )
            await self._append_event(
                session,
                lease,
                event_type="provider_dispatch_settled",
                payload={"actual_amount": actual_cost_microusd, "overage": overage},
                execution_attempt_id=attempt_id,
            )
            updated = dict(reservation)
            updated.update(state="settled", settled_amount=actual_cost_microusd)
            return self._reservation_record(updated)

    async def release_provider_dispatch(
        self, lease: RunLease, *, reservation_id: UUID
    ) -> ReservationRecord:
        """Release a request slot only when dispatch is proven not to have occurred."""

        async with self._sessions() as session, session.begin():
            await self._lock_fence(session, lease)
            reservation = await self._lock_reservation(session, lease, reservation_id)
            if reservation["kind"] != BudgetKind.PROVIDER.value:
                raise ValueError("provider release requires a provider reservation")
            if reservation["state"] == "released":
                return self._reservation_record(reservation)
            if reservation["state"] != "active":
                raise BudgetReservationConflictError("settled provider request cannot release")
            attempt_id = reservation["execution_attempt_id"]
            await session.execute(
                text("""
                    UPDATE execution_attempt_provider_budgets
                       SET requests_reserved=requests_reserved-1,
                           cost_reserved_microusd=cost_reserved_microusd-:amount,
                           version=version+1
                     WHERE attempt_id=:attempt_id
                """),
                {"attempt_id": attempt_id, "amount": reservation["reserved_amount"]},
            )
            await session.execute(
                text("""
                    UPDATE pipeline_budget_ledgers
                       SET provider_reserved_microusd=provider_reserved_microusd-:amount,
                           version=version+1, updated_at=clock_timestamp()
                     WHERE pipeline_run_id=:run_id
                """),
                {"run_id": lease.pipeline_run_id, "amount": reservation["reserved_amount"]},
            )
            await session.execute(
                text("""
                    UPDATE pipeline_budget_reservations
                       SET state='released', settled_at=clock_timestamp()
                     WHERE id=:id
                """),
                {"id": reservation_id},
            )
            updated = dict(reservation)
            updated["state"] = "released"
            return self._reservation_record(updated)

    async def schedule_retry(
        self,
        lease: RunLease,
        *,
        stage_run_id: UUID,
        max_attempts: int,
        cleanup_acknowledged: bool | None = None,
    ) -> bool:
        async with self._sessions() as session, session.begin():
            await self._lock_fence(session, lease)
            row = (
                (
                    await session.execute(
                        text("""
                        SELECT s.state, s.attempt_count, a.retry_class, a.reason_code,
                               a.finished_at, a.cleanup_acknowledged_at, l.terminal_cause
                          FROM pipeline_stage_runs s
                          JOIN execution_attempts a ON a.stage_run_id=s.id
                                                   AND a.attempt_number=s.attempt_count
                          JOIN pipeline_budget_ledgers l ON l.pipeline_run_id=s.pipeline_run_id
                         WHERE s.id=:stage_id AND s.pipeline_run_id=:run_id
                         FOR UPDATE OF s, a, l
                    """),
                        {"stage_id": stage_run_id, "run_id": lease.pipeline_run_id},
                    )
                )
                .mappings()
                .one()
            )
            if row["state"] == "retry_wait":
                return False
            decision = retry_decision(
                completed_attempt_number=row["attempt_count"],
                max_attempts=max_attempts,
                retry_class=RetryClass(row["retry_class"]),
                reason_code=row["reason_code"],
                terminal_cause=row["terminal_cause"],
                cleanup_acknowledged=(
                    row.get("cleanup_acknowledged_at") is not None
                    and cleanup_acknowledged is not False
                ),
            )
            if not decision.retry:
                return False
            await session.execute(
                text("""
                    UPDATE pipeline_stage_runs
                       SET state='retry_wait',
                           next_attempt_at=:finished_at + make_interval(secs => :delay),
                           version=version+1
                     WHERE id=:stage_id
                """),
                {
                    "stage_id": stage_run_id,
                    "finished_at": row["finished_at"],
                    "delay": decision.delay_seconds,
                },
            )
            await self._append_event(
                session,
                lease,
                event_type="retry_scheduled",
                payload={
                    "delay_seconds": decision.delay_seconds,
                    "reason_code": row["reason_code"],
                },
                stage_run_id=stage_run_id,
            )
            return True

    async def terminalgen_publication_candidate(
        self,
        lease: RunLease,
    ) -> TerminalGenPublicationCandidate | None:
        expected = {
            ("global_finalize", "final_audit"): (
                "terminalgen_final_audit.v1",
                "sanitized_audit",
            ),
            ("package_authoring", "corpus"): (
                "terminalgen_corpus.v1",
                "authoring_restricted",
            ),
            ("package_runtime", "corpus"): (
                "terminalgen_corpus.v1",
                "team_runtime",
            ),
            ("publish_boundary", "publication_request"): (
                "terminalgen.publication-request.v1",
                "authoring_restricted",
            ),
        }
        async with self._sessions() as session, session.begin():
            await self._lock_fence(session, lease)
            run = (
                (
                    await session.execute(
                        text("""
                            SELECT r.team_id, r.recipe_name, r.recipe_version,
                                   r.recipe_digest, r.result, p.state AS publication_state,
                                   (
                                     SELECT s.state
                                       FROM pipeline_stage_runs s
                                      WHERE s.pipeline_run_id=r.id
                                        AND s.node_key='publish_boundary'
                                        AND s.shard_key='singleton'
                                   ) AS publish_stage_state
                              FROM pipeline_runs r
                              LEFT JOIN terminalgen_corpus_publications p
                                ON p.pipeline_run_id=r.id
                             WHERE r.id=:run_id
                        """),
                        {"run_id": lease.pipeline_run_id},
                    )
                )
                .mappings()
                .one()
            )
            if (
                run["result"] is not None
                or run["publication_state"] == "published"
                or (run["recipe_name"], run["recipe_version"])
                != ("terminalgen-authoring", 1)
            ):
                return None
            rows = (
                (
                    await session.execute(
                        text("""
                            SELECT s.node_key, s.shard_key, s.state AS stage_state,
                                   a.id AS artifact_id, a.name AS artifact_name,
                                   a.artifact_type, a.access_class, a.content_hash,
                                   a.manifest_sha256, u.id AS upload_session_id,
                                   u.prefix, u.state AS upload_state, u.commit_kind,
                                   u.canonical_manifest_json, u.manifest_sha256 AS root_sha256,
                                   u.committed_marker_sha256
                              FROM pipeline_stage_runs s
                              JOIN artifacts a ON a.pipeline_stage_run_id=s.id
                              JOIN artifact_upload_sessions u
                                ON u.id=a.artifact_upload_session_id
                             WHERE s.pipeline_run_id=:run_id
                               AND a.team_id=:team_id
                               AND s.node_key IN (
                                 'global_finalize','package_authoring',
                                 'package_runtime','publish_boundary'
                               )
                               AND a.name IN ('final_audit','corpus','publication_request')
                               AND a.producer_kind IN ('container','platform')
                             ORDER BY s.node_key, a.name, a.id
                        """),
                        {
                            "run_id": lease.pipeline_run_id,
                            "team_id": run["team_id"],
                        },
                    )
                )
                .mappings()
                .all()
            )
            by_identity = {(row["node_key"], row["artifact_name"]): row for row in rows}
            if set(by_identity) != set(expected):
                if run["publish_stage_state"] == "succeeded":
                    await self._fail_terminalgen_candidate_authority(
                        session,
                        lease,
                        reason_code="publication_artifact_set_incomplete",
                    )
                return None
            sources: dict[tuple[str, str], TerminalGenPublicationArtifactSource] = {}
            for identity, (artifact_type, access_class) in expected.items():
                row = by_identity[identity]
                if (
                    row["shard_key"] != "singleton"
                    or row["stage_state"] != "succeeded"
                    or row["artifact_type"] != artifact_type
                    or row["access_class"] != access_class
                    or row["upload_state"] != "committed"
                    or row["commit_kind"] != "final_output"
                    or row["canonical_manifest_json"] is None
                    or row["root_sha256"] is None
                    or row["committed_marker_sha256"] is None
                    or row["manifest_sha256"] is None
                ):
                    await self._fail_terminalgen_candidate_authority(
                        session,
                        lease,
                        reason_code="publication_artifact_authority_drift",
                    )
                    return None
                try:
                    root = ArtifactCommitManifestV1.model_validate_json(
                        canonical_document(row["canonical_manifest_json"])
                    )
                except ValueError:
                    await self._fail_terminalgen_candidate_authority(
                        session,
                        lease,
                        reason_code="publication_root_manifest_invalid",
                    )
                    return None
                record = next(
                    (item for item in root.artifacts if item.artifact_id == row["artifact_id"]),
                    None,
                )
                if (
                    record is None
                    or record.artifact_name != row["artifact_name"]
                    or record.artifact_type != row["artifact_type"]
                    or record.manifest_sha256 != row["manifest_sha256"]
                    or record.content_sha256 != row["content_hash"]
                ):
                    await self._fail_terminalgen_candidate_authority(
                        session,
                        lease,
                        reason_code="publication_root_manifest_drift",
                    )
                    return None
                semantic_count = sum(
                    item.role == "semantic_document" for item in record.stored_files
                )
                if semantic_count != 1:
                    await self._fail_terminalgen_candidate_authority(
                        session,
                        lease,
                        reason_code="publication_semantic_document_drift",
                    )
                    return None
                marker = ArtifactCommitMarkerV1(
                    commit_kind="final_output",
                    manifest_sha256=row["root_sha256"],
                    session_id=row["upload_session_id"],
                )
                sources[identity] = TerminalGenPublicationArtifactSource(
                    artifact_id=row["artifact_id"],
                    artifact_name=row["artifact_name"],
                    artifact_type=row["artifact_type"],
                    access_class=row["access_class"],
                    manifest_sha256=row["manifest_sha256"],
                    content_sha256=row["content_hash"],
                    node_key=row["node_key"],
                    root_manifest_bytes=canonical_document(root),
                    root_manifest_sha256=row["root_sha256"],
                    root_manifest_key=f"{row['prefix']}_manifest.json",
                    committed_marker_bytes=canonical_document(marker),
                    committed_marker_sha256=row["committed_marker_sha256"],
                    committed_marker_key=f"{row['prefix']}_COMMITTED",
                    files=tuple(
                        PublicationStoredFile(
                            relative_path=item.relative_path,
                            role=item.role,
                            archive_format=item.archive_format,
                            media_type=item.media_type,
                            size_bytes=item.size_bytes,
                            sha256=item.sha256,
                            storage_key=(
                                f"{row['prefix']}artifacts/{row['artifact_id']}/"
                                f"{item.relative_path}"
                            ),
                        )
                        for item in record.stored_files
                    ),
                )
            return TerminalGenPublicationCandidate(
                pipeline_run_id=lease.pipeline_run_id,
                team_id=run["team_id"],
                recipe_digest=run["recipe_digest"],
                request=sources[("publish_boundary", "publication_request")],
                final_audit=sources[("global_finalize", "final_audit")],
                authoring_corpus=sources[("package_authoring", "corpus")],
                runtime_corpus=sources[("package_runtime", "corpus")],
            )

    async def _fail_terminalgen_candidate_authority(
        self,
        session: AsyncSession,
        lease: RunLease,
        *,
        reason_code: str,
    ) -> None:
        await session.execute(
            text("""
                UPDATE pipeline_runs
                   SET state='finished', result='failed', result_reason=:reason,
                       finished_at=clock_timestamp(), version=version+1
                 WHERE id=:run_id AND result IS NULL
            """),
            {"run_id": lease.pipeline_run_id, "reason": reason_code},
        )
        await self._append_event(
            session,
            lease,
            event_type="terminalgen_corpus_publication_failed",
            payload={"reason_code": reason_code},
        )

    async def publish_terminalgen_corpus(
        self,
        lease: RunLease,
        *,
        candidate: TerminalGenPublicationCandidate,
        request_sha256: str,
        material: TerminalGenPublicationMaterial,
        smoke: TerminalGenTaskSetSmokeV1,
        smoke_object_key: str,
        taskset_manifest_object_key: str,
        taskset_manifest_json: dict[str, Any],
    ) -> TerminalGenPublicationReceiptV1:
        publication_id = canonical_uuid5(
            UUID("71114678-0cb8-4844-961e-ac165240a6f8"),
            {"pipeline_run_id": str(candidate.pipeline_run_id), "request_sha256": request_sha256},
        )
        corpus_version_id = canonical_uuid5(
            UUID("65c62e9d-e233-4948-89bb-aa3d0ea6766b"),
            {
                "team_id": str(candidate.team_id),
                "version_sha256": material.corpus_version_sha256,
            },
        )
        request = material.request
        async with self._sessions() as session, session.begin():
            await self._lock_fence(session, lease)
            existing = (
                (
                    await session.execute(
                        text("""
                            SELECT request_sha256, state, receipt_json
                              FROM terminalgen_corpus_publications
                             WHERE pipeline_run_id=:run_id FOR UPDATE
                        """),
                        {"run_id": lease.pipeline_run_id},
                    )
                )
                .mappings()
                .one_or_none()
            )
            if existing is not None:
                if existing["request_sha256"] != request_sha256:
                    raise ValueError("TerminalGen publication idempotency drift")
                if existing["state"] != "published" or existing["receipt_json"] is None:
                    raise ValueError("TerminalGen publication replayed a failed terminal result")
                return TerminalGenPublicationReceiptV1.model_validate_json(
                    canonical_document(existing["receipt_json"])
                )
            await session.execute(
                text("SELECT id FROM teams WHERE id=:team_id FOR UPDATE"),
                {"team_id": candidate.team_id},
            )
            alias_row = (
                (
                    await session.execute(
                        text("""
                            SELECT a.corpus_version_id, a.generation, v.version_sha256
                              FROM terminalgen_corpus_aliases a
                              JOIN terminalgen_corpus_versions v ON v.id=a.corpus_version_id
                             WHERE a.team_id=:team_id AND a.alias=:alias FOR UPDATE OF a
                        """),
                        {"team_id": candidate.team_id, "alias": request.alias},
                    )
                )
                .mappings()
                .one_or_none()
            )
            observed_previous = None if alias_row is None else alias_row["version_sha256"]
            if observed_previous != request.expected_previous_version_sha256:
                raise TerminalGenPublicationError("publication_alias_fence_conflict")
            identity_conflict = (
                await session.execute(
                    text("""
                        SELECT 1 FROM terminalgen_corpus_versions
                         WHERE team_id=:team_id
                           AND ((corpus_id=:corpus_id AND corpus_version=:corpus_version)
                                OR version_sha256=:version_sha256)
                    """),
                    {
                        "team_id": candidate.team_id,
                        "corpus_id": request.corpus_id,
                        "corpus_version": request.corpus_version,
                        "version_sha256": material.corpus_version_sha256,
                    },
                )
            ).one_or_none()
            if identity_conflict is not None:
                raise TerminalGenPublicationError("publication_corpus_identity_conflict")
            expected_task_artifacts: dict[UUID, tuple[str, str, str]] = {}
            for task in material.runtime_corpus.tasks:
                for reference, expected_type in (
                    (task.source_task_artifact, "terminalgen_task_bundle.v1"),
                    (task.validation_artifact, "terminalgen_task_validation.v1"),
                ):
                    expected_value = (
                        expected_type,
                        reference.manifest_sha256,
                        "authoring_restricted",
                    )
                    previous = expected_task_artifacts.setdefault(
                        reference.artifact_id,
                        expected_value,
                    )
                    if previous != expected_value:
                        raise TerminalGenPublicationError(
                            "publication_task_artifact_reference_drift"
                        )
            artifact_rows = (
                (
                    await session.execute(
                        text("""
                            SELECT id, artifact_type, manifest_sha256, access_class
                             FROM artifacts
                             WHERE pipeline_run_id=:run_id
                               AND team_id=:team_id
                               AND artifact_type IN (
                                 'terminalgen_task_bundle.v1',
                                 'terminalgen_task_validation.v1'
                               )
                        """),
                        {
                            "run_id": candidate.pipeline_run_id,
                            "team_id": candidate.team_id,
                        },
                    )
                )
                .mappings()
                .all()
            )
            observed_task_artifacts = {
                row["id"]: (
                    row["artifact_type"],
                    row["manifest_sha256"],
                    row["access_class"],
                )
                for row in artifact_rows
                if row["id"] in expected_task_artifacts
            }
            if observed_task_artifacts != expected_task_artifacts:
                raise TerminalGenPublicationError("publication_task_artifact_lineage_incomplete")
            await session.execute(
                text("""
                    INSERT INTO terminalgen_corpus_versions (
                        id, team_id, pipeline_run_id, corpus_id, corpus_version,
                        version_sha256, recipe_digest, plan_identity_sha256,
                        final_audit_artifact_id, authoring_corpus_artifact_id,
                        runtime_corpus_artifact_id, authoring_tree_sha256,
                        runtime_tree_sha256, task_count, taskset_smoke_task_count,
                        taskset_smoke_object_key, taskset_smoke_sha256,
                        taskset_smoke_size_bytes, taskset_manifest_object_key,
                        taskset_manifest_json,
                        taskset_manifest_sha256
                    ) VALUES (
                        :id, :team_id, :run_id, :corpus_id, :corpus_version,
                        :version_sha256, :recipe_digest, :plan_identity_sha256,
                        :final_audit, :authoring_corpus, :runtime_corpus,
                        :authoring_tree, :runtime_tree, :task_count, :smoke_count,
                        :smoke_key, :smoke_sha256, :smoke_size, :manifest_key,
                        CAST(:manifest AS jsonb), :manifest_sha256
                    )
                """),
                {
                    "id": corpus_version_id,
                    "team_id": candidate.team_id,
                    "run_id": candidate.pipeline_run_id,
                    "corpus_id": request.corpus_id,
                    "corpus_version": request.corpus_version,
                    "version_sha256": material.corpus_version_sha256,
                    "recipe_digest": request.recipe_digest,
                    "plan_identity_sha256": material.final_audit.plan_identity_sha256,
                    "final_audit": request.final_audit_artifact.artifact_id,
                    "authoring_corpus": request.authoring_corpus_artifact.artifact_id,
                    "runtime_corpus": request.runtime_corpus_artifact.artifact_id,
                    "authoring_tree": material.authoring_corpus.corpus_tree_sha256,
                    "runtime_tree": material.runtime_corpus.corpus_tree_sha256,
                    "task_count": material.runtime_corpus.task_count,
                    "smoke_count": smoke.task_count,
                    "smoke_key": smoke_object_key,
                    "smoke_sha256": smoke.archive_sha256,
                    "smoke_size": smoke.archive_size_bytes,
                    "manifest_key": taskset_manifest_object_key,
                    "manifest": _json_text(taskset_manifest_json),
                    "manifest_sha256": smoke.manifest_sha256,
                },
            )
            task_rows = []
            for ordinal, (authoring, runtime) in enumerate(
                zip(material.authoring_corpus.tasks, material.runtime_corpus.tasks, strict=True)
            ):
                task_rows.append(
                    {
                        "version_id": corpus_version_id,
                        "ordinal": ordinal,
                        "slot_id": runtime.slot_id,
                        "task_id": runtime.task_id,
                        "task_name": runtime.task_name,
                        "source_tree": runtime.source_task_tree_sha256,
                        "projected_tree": runtime.projected_task_tree_sha256,
                        "source_artifact": runtime.source_task_artifact.artifact_id,
                        "validation_artifact": runtime.validation_artifact.artifact_id,
                        "authoring_path": authoring.bundle_relative_path,
                        "authoring_sha256": authoring.bundle_sha256,
                        "authoring_size": authoring.bundle_size_bytes,
                        "runtime_path": runtime.bundle_relative_path,
                        "runtime_sha256": runtime.bundle_sha256,
                        "runtime_size": runtime.bundle_size_bytes,
                        "verifier_sha256": runtime.verifier_bridge_sha256,
                    }
                )
            await session.execute(
                text("""
                    INSERT INTO terminalgen_corpus_tasks (
                        corpus_version_id, task_ordinal, slot_id, task_id, task_name,
                        source_task_tree_sha256, projected_task_tree_sha256,
                        source_task_artifact_id,
                        validation_artifact_id, authoring_bundle_path,
                        authoring_bundle_sha256, authoring_bundle_size_bytes,
                        runtime_bundle_path, runtime_bundle_sha256,
                        runtime_bundle_size_bytes, verifier_bridge_sha256
                    ) VALUES (
                        :version_id, :ordinal, :slot_id, :task_id, :task_name,
                        :source_tree, :projected_tree, :source_artifact,
                        :validation_artifact, :authoring_path, :authoring_sha256,
                        :authoring_size, :runtime_path, :runtime_sha256,
                        :runtime_size, :verifier_sha256
                    )
                """),
                task_rows,
            )
            previous_id = None if alias_row is None else alias_row["corpus_version_id"]
            generation = 1 if alias_row is None else int(alias_row["generation"]) + 1
            if alias_row is None:
                await session.execute(
                    text("""
                        INSERT INTO terminalgen_corpus_aliases (
                            team_id, alias, corpus_version_id,
                            previous_corpus_version_id, generation
                        ) VALUES (:team_id, :alias, :version_id, NULL, 1)
                    """),
                    {
                        "team_id": candidate.team_id,
                        "alias": request.alias,
                        "version_id": corpus_version_id,
                    },
                )
            else:
                await session.execute(
                    text("""
                        UPDATE terminalgen_corpus_aliases
                           SET previous_corpus_version_id=corpus_version_id,
                               corpus_version_id=:version_id,
                               generation=generation+1,
                               updated_at=clock_timestamp()
                         WHERE team_id=:team_id AND alias=:alias
                    """),
                    {
                        "team_id": candidate.team_id,
                        "alias": request.alias,
                        "version_id": corpus_version_id,
                    },
                )
            receipt = TerminalGenPublicationReceiptV1(
                schema_version="terminalgen.publication-receipt.v1",
                publication_id=publication_id,
                pipeline_run_id=candidate.pipeline_run_id,
                corpus_version_id=corpus_version_id,
                corpus_version_sha256=material.corpus_version_sha256,
                corpus_id=request.corpus_id,
                corpus_version=request.corpus_version,
                alias=request.alias,
                alias_generation=generation,
                previous_corpus_version_id=previous_id,
                authoring_corpus_artifact=request.authoring_corpus_artifact,
                runtime_corpus_artifact=request.runtime_corpus_artifact,
                taskset_smoke=smoke,
            )
            receipt_json = receipt.model_dump(mode="json")
            receipt_bytes = canonical_document(receipt_json)
            await session.execute(
                text("""
                    INSERT INTO terminalgen_corpus_publications (
                        id, team_id, pipeline_run_id, request_artifact_id,
                        request_sha256, state, corpus_version_id,
                        receipt_json, receipt_bytes, receipt_sha256
                    ) VALUES (
                        :id, :team_id, :run_id, :request_artifact_id,
                        :request_sha256, 'published', :version_id,
                        CAST(:receipt AS jsonb), :receipt_bytes, :receipt_sha256
                    )
                """),
                {
                    "id": publication_id,
                    "team_id": candidate.team_id,
                    "run_id": candidate.pipeline_run_id,
                    "request_artifact_id": candidate.request.artifact_id,
                    "request_sha256": request_sha256,
                    "version_id": corpus_version_id,
                    "receipt": _json_text(receipt_json),
                    "receipt_bytes": receipt_bytes,
                    "receipt_sha256": digest_bytes(receipt_bytes),
                },
            )
            await self._append_event(
                session,
                lease,
                event_type="terminalgen_corpus_published",
                payload={
                    "alias": request.alias,
                    "alias_generation": generation,
                    "corpus_version_id": str(corpus_version_id),
                    "corpus_version_sha256": material.corpus_version_sha256,
                    "task_count": material.runtime_corpus.task_count,
                },
            )
            return receipt

    async def fail_terminalgen_publication(
        self,
        lease: RunLease,
        *,
        candidate: TerminalGenPublicationCandidate,
        request_sha256: str,
        reason_code: str,
    ) -> None:
        publication_id = canonical_uuid5(
            UUID("71114678-0cb8-4844-961e-ac165240a6f8"),
            {"pipeline_run_id": str(candidate.pipeline_run_id), "request_sha256": request_sha256},
        )
        async with self._sessions() as session, session.begin():
            await self._lock_fence(session, lease)
            existing = (
                await session.execute(
                    text("""
                        SELECT request_sha256, state FROM terminalgen_corpus_publications
                         WHERE pipeline_run_id=:run_id FOR UPDATE
                    """),
                    {"run_id": lease.pipeline_run_id},
                )
            ).mappings().one_or_none()
            if existing is not None:
                if existing["request_sha256"] != request_sha256:
                    raise ValueError("TerminalGen publication idempotency drift")
                return
            await session.execute(
                text("""
                    INSERT INTO terminalgen_corpus_publications (
                        id, team_id, pipeline_run_id, request_artifact_id,
                        request_sha256, state, reason_code
                    ) VALUES (
                        :id, :team_id, :run_id, :request_artifact_id,
                        :request_sha256, 'failed', :reason_code
                    )
                """),
                {
                    "id": publication_id,
                    "team_id": candidate.team_id,
                    "run_id": candidate.pipeline_run_id,
                    "request_artifact_id": candidate.request.artifact_id,
                    "request_sha256": request_sha256,
                    "reason_code": reason_code,
                },
            )
            await session.execute(
                text("""
                    UPDATE pipeline_runs
                       SET state='finished', result='failed', result_reason=:reason,
                           finished_at=clock_timestamp(), version=version+1
                     WHERE id=:run_id AND result IS NULL
                """),
                {"run_id": lease.pipeline_run_id, "reason": reason_code},
            )
            await self._append_event(
                session,
                lease,
                event_type="terminalgen_corpus_publication_failed",
                payload={"reason_code": reason_code},
            )

    async def project_run_result(self, lease: RunLease) -> tuple[str, str | None] | None:
        async with self._sessions() as session, session.begin():
            await self._lock_fence(session, lease)
            run = (
                (
                    await session.execute(
                        text("""
                        SELECT r.state, r.result, r.result_reason, l.terminal_cause,
                               EXISTS (
                                   SELECT 1 FROM execution_attempts a
                                   JOIN pipeline_stage_runs s ON s.id=a.stage_run_id
                                   WHERE s.pipeline_run_id=r.id
                                     AND a.state IN ('fault_pending','queued','claimed','running')
                               ) AS active_attempt,
                               EXISTS (
                                   SELECT 1 FROM pipeline_budget_reservations b
                                   WHERE b.pipeline_run_id=r.id AND b.state='active'
                               ) AS active_reservation,
                               EXISTS (
                                   SELECT 1 FROM pipeline_cancellation_outbox o
                                   JOIN execution_attempts a ON a.id=o.execution_attempt_id
                                   WHERE o.pipeline_run_id=r.id
                                     AND (o.state='pending' OR a.cancellation_observed_at IS NULL)
                               ) AS pending_cancel
                          FROM pipeline_runs r
                          JOIN pipeline_budget_ledgers l ON l.pipeline_run_id=r.id
                         WHERE r.id=:run_id FOR UPDATE OF r, l
                    """),
                        {"run_id": lease.pipeline_run_id},
                    )
                )
                .mappings()
                .one()
            )
            if run["result"] is not None:
                return (run["result"], run["result_reason"])
            stages = (
                (
                    await session.execute(
                        text("""
                        SELECT state, failure_policy FROM pipeline_stage_runs
                         WHERE pipeline_run_id=:run_id ORDER BY node_key, shard_key
                    """),
                        {"run_id": lease.pipeline_run_id},
                    )
                )
                .mappings()
                .all()
            )
            terminal = {"succeeded", "failed", "cancelled", "skipped"}
            if (
                not stages
                or any(row["state"] not in terminal for row in stages)
                or run["active_attempt"]
                or run["active_reservation"]
                or run["pending_cancel"]
            ):
                return None
            projection = [
                StageTerminalProjection(
                    state=PipelineStageRunState(row["state"]),
                    selected=True,
                    failure_policy=row["failure_policy"],
                )
                for row in stages
            ]
            cause = TerminalCause(run["terminal_cause"]) if run["terminal_cause"] else None
            result, reason = project_pipeline_result(projection, terminal_cause=cause)
            await session.execute(
                text("""
                    UPDATE pipeline_runs SET state='finished', result=:result,
                           result_reason=:reason, finished_at=clock_timestamp(), version=version+1
                     WHERE id=:run_id AND result IS NULL
                """),
                {"run_id": lease.pipeline_run_id, "result": result.value, "reason": reason},
            )
            await self._append_event(
                session,
                lease,
                event_type="run_finished",
                payload={"result": result.value, "reason": reason},
            )
            return result.value, reason

    async def settle_budget(
        self, lease: RunLease, *, reservation_id: UUID, actual_amount: int
    ) -> ReservationRecord:
        if actual_amount < 0:
            raise ValueError("settlement amount cannot be negative")
        async with self._sessions() as session, session.begin():
            await self._lock_fence(session, lease)
            reservation = await self._lock_reservation(session, lease, reservation_id)
            if reservation["state"] == "settled":
                if reservation["settled_amount"] != actual_amount:
                    raise BudgetReservationConflictError("settlement replay amount drift")
                return self._reservation_record(reservation)
            if reservation["state"] != "active":
                raise BudgetReservationConflictError("released reservation cannot settle")
            kind = BudgetKind(reservation["kind"])
            limit_col, reserved_col, settled_col, _cause = _COUNTERS[kind]
            ledger = (
                (
                    await session.execute(
                        text(
                            f"SELECT {limit_col} AS budget_limit, {reserved_col} AS reserved, "
                            f"{settled_col} AS settled FROM pipeline_budget_ledgers "
                            "WHERE pipeline_run_id = :run_id FOR UPDATE"
                        ),
                        {"run_id": lease.pipeline_run_id},
                    )
                )
                .mappings()
                .one()
            )
            overage = (
                actual_amount > reservation["reserved_amount"]
                or ledger["settled"] + actual_amount > ledger["budget_limit"]
            )
            if overage and kind is BudgetKind.ARTIFACT:
                raise ValueError("artifact settlement cannot exceed its reservation")
            if overage:
                await self._latch_terminal_cause(session, lease, TerminalCause.ACCOUNTING_VIOLATION)
            await session.execute(
                text(
                    f"UPDATE pipeline_budget_ledgers SET {reserved_col} = {reserved_col} - :reserved, "
                    f"{settled_col} = {settled_col} + :actual, version = version + 1, "
                    "updated_at = clock_timestamp() WHERE pipeline_run_id = :run_id"
                ),
                {
                    "reserved": reservation["reserved_amount"],
                    "actual": actual_amount,
                    "run_id": lease.pipeline_run_id,
                },
            )
            await session.execute(
                text("""
                    UPDATE pipeline_budget_reservations
                       SET state = 'settled', settled_amount = :actual,
                           settled_at = clock_timestamp()
                     WHERE id = :id
                """),
                {"actual": actual_amount, "id": reservation_id},
            )
            await self._append_event(
                session,
                lease,
                event_type="budget_settled",
                payload={"kind": kind.value, "actual_amount": actual_amount, "overage": overage},
                execution_attempt_id=reservation["execution_attempt_id"],
            )
            updated = dict(reservation)
            updated.update(state="settled", settled_amount=actual_amount)
            result = self._reservation_record(updated)
        return result

    async def release_budget(self, lease: RunLease, *, reservation_id: UUID) -> ReservationRecord:
        async with self._sessions() as session, session.begin():
            await self._lock_fence(session, lease)
            reservation = await self._lock_reservation(session, lease, reservation_id)
            if reservation["state"] == "released":
                return self._reservation_record(reservation)
            if reservation["state"] != "active":
                raise BudgetReservationConflictError("settled reservation cannot release")
            kind = BudgetKind(reservation["kind"])
            _limit_col, reserved_col, _settled_col, _cause = _COUNTERS[kind]
            await session.execute(
                text(
                    f"UPDATE pipeline_budget_ledgers SET {reserved_col} = {reserved_col} - :amount, "
                    "version = version + 1, updated_at = clock_timestamp() "
                    "WHERE pipeline_run_id = :run_id"
                ),
                {"amount": reservation["reserved_amount"], "run_id": lease.pipeline_run_id},
            )
            await session.execute(
                text("""
                    UPDATE pipeline_budget_reservations
                       SET state = 'released', settled_at = clock_timestamp()
                     WHERE id = :id
                """),
                {"id": reservation_id},
            )
            await self._append_event(
                session,
                lease,
                event_type="budget_released",
                payload={"kind": kind.value, "reserved_amount": reservation["reserved_amount"]},
                execution_attempt_id=reservation["execution_attempt_id"],
            )
            updated = dict(reservation)
            updated["state"] = "released"
            return self._reservation_record(updated)

    async def latch_terminal_cause(self, lease: RunLease, cause: TerminalCause) -> TerminalCause:
        async with self._sessions() as session, session.begin():
            await self._lock_fence(session, lease)
            return await self._latch_terminal_cause(session, lease, cause)

    async def acknowledge_cancellation(
        self, *, outbox_id: UUID, request_digest: str, ack: dict[str, Any]
    ) -> None:
        ack_digest = canonical_digest(ack)
        async with self._sessions() as session, session.begin():
            row = (
                (
                    await session.execute(
                        text("""
                        SELECT state, request_digest, ack_digest
                          FROM pipeline_cancellation_outbox WHERE id = :id FOR UPDATE
                    """),
                        {"id": outbox_id},
                    )
                )
                .mappings()
                .one()
            )
            if row["request_digest"] != request_digest:
                raise BudgetReservationConflictError("cancellation acknowledgement request drift")
            if row["state"] == "acked":
                if row["ack_digest"] != ack_digest:
                    raise BudgetReservationConflictError("cancellation acknowledgement drift")
                return
            await session.execute(
                text("""
                    UPDATE pipeline_cancellation_outbox
                       SET state = 'acked', ack_json = CAST(:ack AS jsonb), ack_digest = :digest,
                           acked_at = clock_timestamp(), version = version + 1
                     WHERE id = :id
                """),
                {"id": outbox_id, "ack": _json_text(ack), "digest": ack_digest},
            )

    async def _lock_fence(self, session: AsyncSession, lease: RunLease) -> None:
        row = (
            await session.execute(
                text("""
                    SELECT id FROM pipeline_runs
                     WHERE id = :run_id AND claimed_by = :owner AND lease_epoch = :epoch
                       AND lease_expires_at > clock_timestamp()
                     FOR UPDATE
                """),
                self._fence_params(lease),
            )
        ).one_or_none()
        if row is None:
            raise StaleControllerLeaseError("Pipeline write lost its controller fence")

    async def _fail_provider_attempt_budget(
        self, session: AsyncSession, lease: RunLease, attempt_id: UUID
    ) -> None:
        stage_id = (
            await session.execute(
                text("""
                    UPDATE execution_attempts
                       SET state='failed', retry_class='none',
                           reason_code='provider_attempt_budget_exhausted',
                           finished_at=clock_timestamp(), version=version+1
                     WHERE id=:attempt_id AND state IN ('claimed','running')
                    RETURNING stage_run_id
                """),
                {"attempt_id": attempt_id},
            )
        ).scalar_one()
        await session.execute(
            text("""
                UPDATE pipeline_stage_runs
                   SET state='failed', reason_code='provider_attempt_budget_exhausted',
                       finished_at=clock_timestamp(), version=version+1
                 WHERE id=:stage_id
            """),
            {"stage_id": stage_id},
        )
        await self._append_event(
            session,
            lease,
            event_type="provider_attempt_budget_exhausted",
            payload={"reason_code": "provider_attempt_budget_exhausted"},
            stage_run_id=stage_id,
            execution_attempt_id=attempt_id,
        )

    async def _fail_accounting_violation(
        self, session: AsyncSession, lease: RunLease, attempt_id: UUID
    ) -> None:
        stage_id = (
            await session.execute(
                text("SELECT stage_run_id FROM execution_attempts WHERE id=:attempt_id FOR UPDATE"),
                {"attempt_id": attempt_id},
            )
        ).scalar_one()
        await session.execute(
            text("""
                UPDATE execution_attempts
                   SET state='failed', retry_class='none', reason_code='accounting_violation',
                       finished_at=COALESCE(finished_at, clock_timestamp()), version=version+1
                 WHERE id=:attempt_id
            """),
            {"attempt_id": attempt_id},
        )
        await session.execute(
            text("""
                UPDATE pipeline_stage_runs
                   SET state='failed', reason_code='accounting_violation',
                       finished_at=COALESCE(finished_at, clock_timestamp()), version=version+1
                 WHERE id=:stage_id
            """),
            {"stage_id": stage_id},
        )

    async def _latch_terminal_cause(
        self, session: AsyncSession, lease: RunLease, cause: TerminalCause
    ) -> TerminalCause:
        row = (
            (
                await session.execute(
                    text("""
                    SELECT terminal_cause FROM pipeline_budget_ledgers
                     WHERE pipeline_run_id = :run_id FOR UPDATE
                """),
                    {"run_id": lease.pipeline_run_id},
                )
            )
            .mappings()
            .one()
        )
        if row["terminal_cause"] is not None:
            winner = TerminalCause(row["terminal_cause"])
            await self._append_event(
                session,
                lease,
                event_type="terminal_cause_lost_race",
                payload={"attempted": cause.value, "winner": winner.value},
            )
            return winner
        await session.execute(
            text("""
                UPDATE pipeline_budget_ledgers
                   SET terminal_cause = :cause, terminal_cause_at = clock_timestamp(),
                       version = version + 1, updated_at = clock_timestamp()
                 WHERE pipeline_run_id = :run_id AND terminal_cause IS NULL
            """),
            {"run_id": lease.pipeline_run_id, "cause": cause.value},
        )
        await session.execute(
            text("""
                UPDATE pipeline_runs
                   SET state = 'cancelling',
                       cancellation_requested_at = CASE WHEN :cause = 'user_cancel'
                           THEN clock_timestamp() ELSE cancellation_requested_at END,
                       budget_exhausted_at = CASE WHEN :cause != 'user_cancel'
                           THEN clock_timestamp() ELSE budget_exhausted_at END,
                       version = version + 1
                 WHERE id = :run_id AND state IN ('submitted','running')
            """),
            {"run_id": lease.pipeline_run_id, "cause": cause.value},
        )
        await self._append_event(
            session,
            lease,
            event_type="terminal_cause_latched",
            payload={"terminal_cause": cause.value},
        )
        await self._cancel_not_started_attempts(session, lease, cause)
        attempts = (
            (
                await session.execute(
                    text("""
                    SELECT a.id
                      FROM execution_attempts a
                      JOIN pipeline_stage_runs s ON s.id = a.stage_run_id
                     WHERE s.pipeline_run_id = :run_id
                       AND a.state IN ('claimed','running')
                     FOR UPDATE OF a
                """),
                    {"run_id": lease.pipeline_run_id},
                )
            )
            .scalars()
            .all()
        )
        for attempt_id in attempts:
            request = {
                "execution_attempt_id": str(attempt_id),
                "pipeline_run_id": str(lease.pipeline_run_id),
                "terminal_cause": cause.value,
            }
            request_digest = canonical_digest(request)
            await session.execute(
                text("""
                    INSERT INTO pipeline_cancellation_outbox (
                        pipeline_run_id, execution_attempt_id, terminal_cause,
                        idempotency_key, request_json, request_digest
                    ) VALUES (
                        :run_id, :attempt_id, :cause, :key, CAST(:request AS jsonb), :digest
                    ) ON CONFLICT (execution_attempt_id) DO NOTHING
                """),
                {
                    "run_id": lease.pipeline_run_id,
                    "attempt_id": attempt_id,
                    "cause": cause.value,
                    "key": f"pipeline-cancel:{attempt_id}",
                    "request": _json_text(request),
                    "digest": request_digest,
                },
            )
            await session.execute(
                text("""
                    UPDATE execution_attempts
                       SET cancellation_requested_at=COALESCE(
                               cancellation_requested_at, clock_timestamp()),
                           version=version+1
                     WHERE id=:attempt_id AND state IN ('claimed','running')
                """),
                {"attempt_id": attempt_id},
            )
            await session.execute(
                text("""
                    INSERT INTO execution_attempt_control_commands (
                        execution_attempt_id, seq, command
                    ) VALUES (
                        :attempt_id,
                        COALESCE((SELECT max(seq)+1 FROM execution_attempt_control_commands
                                  WHERE execution_attempt_id=:attempt_id), 1),
                        'cancel_requested'
                    ) ON CONFLICT DO NOTHING
                """),
                {"attempt_id": attempt_id},
            )
        await session.execute(
            text("""
                UPDATE pipeline_stage_runs
                   SET state = 'cancelled', finished_at = clock_timestamp(),
                       reason_code = :cause, version = version + 1
                 WHERE pipeline_run_id = :run_id
                   AND state IN ('blocked','ready','queued','retry_wait')
            """),
            {"run_id": lease.pipeline_run_id, "cause": cause.value},
        )
        return cause

    async def _cancel_not_started_attempts(
        self,
        session: AsyncSession,
        lease: RunLease,
        cause: TerminalCause,
    ) -> None:
        """Cancel unclaimable Attempts and release every reservation in the latch tx."""

        released = (
            (
                await session.execute(
                    text("""
                    SELECT b.kind, b.reserved_amount
                      FROM pipeline_budget_reservations b
                      JOIN execution_attempts a ON a.id=b.execution_attempt_id
                      JOIN pipeline_stage_runs s ON s.id=a.stage_run_id
                     WHERE s.pipeline_run_id=:run_id AND b.state='active'
                       AND a.state IN ('fault_pending','queued')
                     FOR UPDATE OF b, a, s
                """),
                    {"run_id": lease.pipeline_run_id},
                )
            )
            .mappings()
            .all()
        )
        amounts = {kind.value: 0 for kind in BudgetKind}
        for row in released:
            amounts[row["kind"]] += row["reserved_amount"]
        await session.execute(
            text("""
                UPDATE pipeline_budget_reservations b
                   SET state='released', settled_at=clock_timestamp()
                  FROM execution_attempts a, pipeline_stage_runs s
                 WHERE b.execution_attempt_id=a.id AND s.id=a.stage_run_id
                   AND s.pipeline_run_id=:run_id AND b.state='active'
                   AND a.state IN ('fault_pending','queued')
            """),
            {"run_id": lease.pipeline_run_id},
        )
        if released:
            await session.execute(
                text("""
                    UPDATE pipeline_budget_ledgers
                       SET provider_reserved_microusd=provider_reserved_microusd-:provider,
                           gpu_reserved_seconds=gpu_reserved_seconds-:gpu,
                           artifact_reserved_bytes=artifact_reserved_bytes-:artifact,
                           version=version+1, updated_at=clock_timestamp()
                     WHERE pipeline_run_id=:run_id
                """),
                {
                    "run_id": lease.pipeline_run_id,
                    "provider": amounts.get("provider", 0),
                    "gpu": amounts.get("gpu", 0),
                    "artifact": amounts.get("artifact", 0),
                },
            )
        attempt_ids = (
            (
                await session.execute(
                    text("""
                    UPDATE execution_attempts a
                       SET state='cancelled', cancellation_requested_at=clock_timestamp(),
                           cancellation_observed_at=clock_timestamp(),
                           cancellation_outcome='not_started', finished_at=clock_timestamp(),
                           retry_class='cancelled', reason_code=:cause,
                           cleanup_acknowledged_at=clock_timestamp(),
                           cleanup_proof_json=CAST(:cleanup_proof AS jsonb),
                           cleanup_proof_digest=:cleanup_digest,
                           version=a.version+1
                      FROM pipeline_stage_runs s
                     WHERE s.id=a.stage_run_id AND s.pipeline_run_id=:run_id
                       AND a.state IN ('fault_pending','queued')
                    RETURNING a.id
                """),
                    {
                        "run_id": lease.pipeline_run_id,
                        "cause": cause.value,
                        "cleanup_proof": '{"not_started":true}',
                        "cleanup_digest": canonical_digest({"not_started": True}),
                    },
                )
            )
            .scalars()
            .all()
        )
        if attempt_ids:
            await self._append_event(
                session,
                lease,
                event_type="not_started_attempts_cancelled",
                payload={
                    "attempt_count": len(attempt_ids),
                    "terminal_cause": cause.value,
                },
            )

    async def _append_event(
        self,
        session: AsyncSession,
        lease: RunLease,
        *,
        event_type: str,
        payload: dict[str, Any],
        stage_run_id: UUID | None = None,
        execution_attempt_id: UUID | None = None,
    ) -> int:
        seq = (
            await session.execute(
                text("""
                    UPDATE pipeline_runs
                       SET next_event_seq = next_event_seq + 1, version = version + 1
                     WHERE id = :run_id AND claimed_by = :owner AND lease_epoch = :epoch
                    RETURNING next_event_seq - 1
                """),
                self._fence_params(lease),
            )
        ).scalar_one_or_none()
        if seq is None:
            raise StaleControllerLeaseError("Pipeline event allocation lost its fence")
        await session.execute(
            text("""
                INSERT INTO pipeline_events (
                    pipeline_run_id, seq, stage_run_id, execution_attempt_id,
                    event_type, actor_kind, actor_id, payload_json
                ) VALUES (
                    :run_id, :seq, :stage_id, :attempt_id,
                    :event_type, 'pipeline_controller', :owner, CAST(:payload AS jsonb)
                )
            """),
            {
                "run_id": lease.pipeline_run_id,
                "seq": seq,
                "stage_id": stage_run_id,
                "attempt_id": execution_attempt_id,
                "event_type": event_type,
                "owner": lease.claimed_by,
                "payload": _json_text(payload),
            },
        )
        return int(seq)

    async def _lock_reservation(
        self, session: AsyncSession, lease: RunLease, reservation_id: UUID
    ) -> Any:
        return (
            (
                await session.execute(
                    text("""
                    SELECT id, pipeline_run_id, execution_attempt_id, kind, reservation_key,
                           request_digest, reserved_amount, settled_amount, state
                      FROM pipeline_budget_reservations
                     WHERE id = :id AND pipeline_run_id = :run_id FOR UPDATE
                """),
                    {"id": reservation_id, "run_id": lease.pipeline_run_id},
                )
            )
            .mappings()
            .one()
        )

    @staticmethod
    def _fence_params(lease: RunLease) -> dict[str, Any]:
        return {
            "run_id": lease.pipeline_run_id,
            "owner": lease.claimed_by,
            "epoch": lease.lease_epoch,
        }

    @staticmethod
    def _reservation_record(row: Any) -> ReservationRecord:
        return ReservationRecord(
            id=row["id"],
            pipeline_run_id=row["pipeline_run_id"],
            kind=BudgetKind(row["kind"]),
            reservation_key=row["reservation_key"],
            request_digest=row["request_digest"],
            reserved_amount=row["reserved_amount"],
            settled_amount=row["settled_amount"],
            state=row["state"],
        )


def _provider_microusd(value: str) -> int:
    from loom.pipeline.budget import provider_usd_to_microusd

    return provider_usd_to_microusd(value)


def _json_text(value: Any) -> str:
    return json.dumps(value, separators=(",", ":"), sort_keys=True)

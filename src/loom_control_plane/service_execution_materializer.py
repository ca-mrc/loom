"""Restart-safe projection of committed service-execution bundles into Loom.

The execution Pod owns only the immutable source commit.  This worker runs in
the control plane, validates that commit byte-for-byte, copies it to stable
Trial identities, derives Loom trajectory formats, and only then publishes the
terminal Trial state.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
from collections.abc import AsyncIterator, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, cast
from uuid import UUID, uuid4

from pydantic import ValidationError
from sqlalchemy import Text, and_, func, or_, select
from sqlalchemy import cast as sql_cast
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from loom.agent.terminus2.mapper import Terminus2TrajectoryMapper
from loom.data_lifecycle_registry import (
    ensure_artifact_lifecycle_authority,
    ensure_trial_event_lifecycle_authority,
    register_lifecycle_object,
)
from loom.db.schema import (
    Artifact,
    ArtifactUploadFile,
    ArtifactUploadSession,
    DataLifecycleAuthority,
    LlmCall,
    ServiceExecutionLease,
    Trial,
    TrialEvent,
)
from loom.execution_runtime_contract import ExecutionRuntimeResultV1
from loom.llm_call_ledger import read_service_execution_llm_calls
from loom.models.result import ExceptionInfo
from loom.models.task import TaskConfig
from loom.models.trajectory import (
    LLMCallEvent,
    StepEndEvent,
    StepStartEvent,
    Terminus2TurnEvent,
    TrajectoryEvent,
    TrialEndEvent,
    TrialErrorEvent,
    TrialStartEvent,
    VerifierEndEvent,
    VerifierStartEvent,
)
from loom.models.trial import TrialConfig
from loom.models.verifier import VerifierResult
from loom.pipeline.artifact_commit import (
    ArtifactCommitManifestV1,
    ArtifactCommitMarkerV1,
    ArtifactManifestV1,
)
from loom.pipeline.keys import canonical_document
from loom.service_execution_terminus_trace import (
    matches_terminus_usage,
    parse_terminus_events,
    reconcile_terminus_ledger,
    terminus_usage,
)
from loom.trajectory.atif import project_to_atif
from loom.trajectory.object_identity import TrajectoryObjectIdentity
from loom.trajectory.storage import ObjectStore
from loom_control_plane.metrics import (
    SERVICE_EXECUTION_MATERIALIZATION_BACKLOG,
    SERVICE_EXECUTION_MATERIALIZATION_COMPLETED_TOTAL,
    SERVICE_EXECUTION_MATERIALIZATION_FAILURES_TOTAL,
    SERVICE_EXECUTION_MATERIALIZATION_OLDEST_AGE_SECONDS,
    SERVICE_EXECUTION_MATERIALIZATION_PENDING_BYTES,
    SERVICE_EXECUTION_MATERIALIZATION_RETRIES_TOTAL,
    SERVICE_EXECUTION_MATERIALIZATION_UNAVAILABLE,
    SERVICE_EXECUTION_MATERIALIZATION_UNAVAILABLE_BYTES,
    SERVICE_EXECUTION_SOURCE_CLEANUP_COMPLETED_TOTAL,
    SERVICE_EXECUTION_SOURCE_CLEANUP_RETRIES_TOTAL,
    SERVICE_EXECUTION_SOURCE_SPOOL_BYTES,
    SERVICE_EXECUTION_SOURCE_SPOOL_RETAINED,
)
from loom_control_plane.service_execution import recover_deleted_committed_trial
from loom_control_plane.service_execution_task_snapshot import (
    ServiceExecutionTaskSnapshotError,
    resolve_service_execution_task_snapshot,
)

logger = logging.getLogger(__name__)


def _raise_if_cancellation_requested(exc: Exception) -> None:
    current_task = asyncio.current_task()
    if current_task is not None and current_task.cancelling():
        raise asyncio.CancelledError from exc


_TRACE_PATH = "trajectory/events.jsonl"
_USAGE_PATH = "accounting/usage.json"
_VERIFIER_PATH = "verifier/output.json"
_RESULT_PATH = "result.json"
_EXCEPTION_PATHS = {"diagnostics/agent-exception.json", "diagnostics/verifier-exception.json"}
_MAX_DERIVATION_BYTES = 256 * 1024 * 1024


class MaterializationIntegrityError(RuntimeError):
    """The immutable source commit is missing, corrupt, or contradictory."""

    def __init__(self, code: str, message: str | None = None) -> None:
        super().__init__(message or code)
        self.code = code


@dataclass(frozen=True, slots=True)
class MaterializationClaim:
    lease_id: UUID
    claim_id: UUID


@dataclass(frozen=True, slots=True)
class SourceCleanupClaim:
    lease_id: UUID
    claim_id: UUID


@dataclass(frozen=True, slots=True)
class MaterializedFile:
    relative_path: str
    media_type: str
    size_bytes: int
    sha256: str
    key: str


@dataclass(frozen=True, slots=True)
class MaterializationResult:
    artifact_id: UUID
    files: tuple[MaterializedFile, ...]
    source_evidence: tuple[MaterializedFile, ...]
    events: tuple[TrajectoryEvent, ...]
    events_body: bytes
    atif_body: bytes
    events_uri: str
    atif_uri: str
    events_sha256: str
    atif_sha256: str
    final_trial_state: str
    failure_reason: str | None
    accounting_call_count: int | None = None
    exception_info: ExceptionInfo | None = None
    preserve_trial_outcome: bool = False


def _digest(body: bytes) -> str:
    return "sha256:" + hashlib.sha256(body).hexdigest()


def _canonical_jsonl(events: Sequence[TrajectoryEvent]) -> bytes:
    return b"".join(
        (
            json.dumps(
                event.model_dump(mode="json"),
                sort_keys=True,
                separators=(",", ":"),
            )
            + "\n"
        ).encode("utf-8")
        for event in events
    )


def read_exception_info(
    runtime_result: ExecutionRuntimeResultV1, inputs: dict[str, bytes],
) -> ExceptionInfo | None:
    if runtime_result.status in {"succeeded", "cancelled"}:
        return None
    phase = "verifier" if runtime_result.status == "verifier_error" else "agent"
    body = inputs.get(f"diagnostics/{phase}-exception.json")
    if body is None:
        return None
    try:
        return ExceptionInfo.model_validate_json(body)
    except ValidationError as exc:
        raise MaterializationIntegrityError("exception_info_invalid") from exc


def _model_matches(source: str, trial_config: TrialConfig) -> bool:
    model = trial_config.agent_model
    return model is not None and source == model.to_gateway_model_string()


def _parse_trace_calls(trace_body: bytes | None) -> list[dict[str, Any]]:
    if trace_body is None:
        lines: list[str] = []
    else:
        try:
            lines = trace_body.decode("utf-8").splitlines()
        except UnicodeDecodeError as exc:
            raise MaterializationIntegrityError("trajectory_not_utf8") from exc
    calls: list[dict[str, Any]] = []
    for line_number, line in enumerate(lines, start=1):
        if not line:
            raise MaterializationIntegrityError(
                "trajectory_invalid", f"empty trajectory line {line_number}"
            )
        try:
            call = json.loads(line)
        except json.JSONDecodeError as exc:
            raise MaterializationIntegrityError(
                "trajectory_invalid", f"invalid trajectory line {line_number}"
            ) from exc
        if not isinstance(call, dict) or call.get("schema_version") != (
            "loom.service-execution-llm-call.v1"
        ):
            raise MaterializationIntegrityError("trajectory_schema_invalid")
        if call.get("turn") != len(calls):
            raise MaterializationIntegrityError("trajectory_turn_order_invalid")
        calls.append(call)
    return calls


def validate_usage_accounting(
    *, trace_body: bytes | None, usage_body: bytes | None, trial_config: TrialConfig
) -> None:
    """Bind the summarized accounting file to every lossless trace call."""

    if usage_body is None:
        raise MaterializationIntegrityError("usage_output_missing")
    try:
        document = json.loads(usage_body)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise MaterializationIntegrityError("usage_output_invalid") from exc
    if trial_config.agent_name == "terminus-2":
        try:
            events = parse_terminus_events(trace_body, trial=trial_config)
        except ValueError as exc:
            raise MaterializationIntegrityError("trajectory_invalid") from exc
        if not matches_terminus_usage(document, terminus_usage(events, trial_config)):
            raise MaterializationIntegrityError("usage_output_identity_drift")
        return
    calls = _parse_trace_calls(trace_body)
    usages = [call.get("usage") for call in calls]
    if (
        not isinstance(document, dict)
        or document.get("schema_version") != "loom.service-execution-usage.v1"
        or not isinstance(document.get("model"), str)
        or not _model_matches(document["model"], trial_config)
        or document.get("call_count") != len(calls)
        or document.get("calls") != usages
        or not isinstance(document.get("totals"), dict)
    ):
        raise MaterializationIntegrityError("usage_output_identity_drift")
    counters = (
        "input_tokens",
        "cached_input_tokens",
        "cache_write_tokens",
        "output_tokens",
        "thinking_tokens",
    )
    if any(not isinstance(usage, dict) for usage in usages):
        raise MaterializationIntegrityError("usage_output_invalid")
    typed_usages = cast(list[dict[str, Any]], usages)
    try:
        expected_totals: dict[str, int | float] = {
            name: sum(int(usage[name]) for usage in typed_usages) for name in counters
        }
        expected_totals["cost_usd"] = sum(
            float(usage["cost_usd"]) for usage in typed_usages
        )
        expected_totals["duration_sec"] = sum(
            float(usage["duration_sec"]) for usage in typed_usages
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise MaterializationIntegrityError("usage_output_invalid") from exc
    if document["totals"] != expected_totals:
        raise MaterializationIntegrityError("usage_output_totals_drift")


def _legacy_verifier_reward_projection(runtime: ExecutionRuntimeResultV1) -> bool:
    """Recognize the old runtime reading exception JSON as the verifier score."""
    if runtime.status != "verifier_error" or not runtime.partial_evidence or runtime.verifier_rewards is not None:
        return False
    outputs = [item for item in runtime.outputs if item.kind == "verifier" and item.state == "captured"]
    phases = [phase for phase in runtime.phases if phase.role == "verifier"]
    return (
        len(outputs) == 2
        and outputs[0].relative_path == "diagnostics/verifier-exception.json"
        and outputs[0].source_path == ".loom/verifier/exception.json"
        and not outputs[0].required
        and outputs[1].relative_path == _VERIFIER_PATH
        and outputs[1].source_path == ".loom/verifier/output.json"
        and outputs[1].required
        and len(phases) == 1
        and phases[0].exit_code not in {None, 0}
        and not phases[0].timed_out
        and phases[0].signal is None
    )


def build_canonical_events(
    *,
    trial_id: UUID,
    task_id: str,
    task_config: TaskConfig,
    trial_config: TrialConfig,
    runtime_result: ExecutionRuntimeResultV1,
    trace_body: bytes | None,
    verifier_body: bytes | None,
    gateway_calls: list[dict[str, Any]] | None = None,
    exception_info: ExceptionInfo | None = None,
) -> tuple[TrajectoryEvent, ...]:
    """Validate the lossless source trace and project it to Loom event rows."""

    terminus = trial_config.agent_name == "terminus-2"
    calls = [] if terminus else _parse_trace_calls(trace_body)
    try:
        native_events = parse_terminus_events(
            trace_body, trial=trial_config, trial_id=trial_id,
        ) if terminus else []
        if terminus and gateway_calls is not None:
            native_events = reconcile_terminus_ledger(
                native_events, gateway_calls, trial_config, trial_id,
            )
    except ValueError as exc:
        raise MaterializationIntegrityError("trajectory_invalid") from exc
    step_id = "agent" if terminus else task_config.steps[0].name if task_config.steps else "main"
    emitted = runtime_result.started_at
    events: list[TrajectoryEvent] = [
        TrialStartEvent(
            emitted_at=emitted,
            trial_id=trial_id,
            step_id="trial",
            seq=0,
            task_id=task_id,
            agent_name=trial_config.agent_name,
            agent_mode="out-of-box",
        ),
        StepStartEvent(
            emitted_at=emitted,
            trial_id=trial_id,
            step_id=step_id,
            seq=1,
            instruction_excerpt=(task_config.task.description or task_config.task.name)[:500],
        ),
    ]
    for event in native_events:
        events.append(event.model_copy(update={"seq": len(events)}))
    for call in calls:
        request = call.get("request")
        usage = call.get("usage")
        response = call.get("response")
        model = call.get("model")
        if (
            not isinstance(request, dict)
            or not isinstance(usage, dict)
            or not isinstance(response, dict)
            or not isinstance(model, str)
            or not _model_matches(model, trial_config)
        ):
            raise MaterializationIntegrityError("trajectory_call_identity_invalid")
        try:
            events.append(
                LLMCallEvent(
                    emitted_at=call["finished_at"],
                    trial_id=trial_id,
                    step_id=step_id,
                    seq=len(events),
                    model=trial_config.agent_model,
                    rate_card_hash=usage["rate_card_hash"],
                    system_prompt=None,
                    messages=request["messages"],
                    tools=None,
                    tool_choice=None,
                    response=response,
                    finish_reason=usage["finish_reason"],
                    input_tokens=usage["input_tokens"],
                    cached_input_tokens=usage["cached_input_tokens"],
                    cache_write_tokens=usage["cache_write_tokens"],
                    output_tokens=usage["output_tokens"],
                    thinking_tokens=usage["thinking_tokens"],
                    provider_extras=usage["provider_extras"],
                    request_params=request["request_params"],
                    cost_usd_snapshot=usage["cost_usd"],
                    duration_sec=usage["duration_sec"],
                    streamed=usage["streamed"],
                    time_to_first_token_sec=usage["time_to_first_token_sec"],
                    gateway_request_id=usage["gateway_request_id"],
                    attempt=usage["attempt"],
                    requested_model=model,
                    response_model=model,
                )
            )
        except (KeyError, ValidationError, ValueError) as exc:
            raise MaterializationIntegrityError("trajectory_call_invalid") from exc

    error_phase = {
        "setup_error": "prepare",
        "task_error": "agent",
        "timed_out": "agent",
        "runtime_error": "agent",
        "artifact_upload_failed": "artifacts",
        "missing_required_artifacts": "artifacts",
        "trajectory_flush_failed": "artifacts",
        "verifier_error": "verifier",
    }.get(runtime_result.status)
    events.append(
        StepEndEvent(
            emitted_at=runtime_result.finished_at,
            trial_id=trial_id,
            step_id=step_id,
            seq=len(events),
            summary={"llm_calls": float(sum(isinstance(e, LLMCallEvent) for e in events))},
            error_phase=error_phase,
        )
    )
    verifier: VerifierResult | None = None
    if verifier_body is not None:
        try:
            verifier = VerifierResult.model_validate_json(verifier_body)
        except ValidationError as exc:
            raise MaterializationIntegrityError("verifier_result_invalid") from exc
        legacy_projection = (
            _legacy_verifier_reward_projection(runtime_result)
            and exception_info is not None
            and exception_info.exception_type == "ServiceExecutionTaskError"
            and exception_info.exception_message == "isolated verifier process failed"
        )
        if runtime_result.verifier_rewards != verifier.rewards and not legacy_projection:
            raise MaterializationIntegrityError("verifier_reward_drift")
        events.extend(
            (
                VerifierStartEvent(
                    emitted_at=runtime_result.finished_at,
                    trial_id=trial_id,
                    step_id=step_id,
                    seq=len(events),
                    verifier_name=task_config.verifier.name,
                    env_mode=task_config.verifier.env_mode,
                ),
                VerifierEndEvent(
                    emitted_at=runtime_result.finished_at,
                    trial_id=trial_id,
                    step_id=step_id,
                    seq=len(events) + 1,
                    result=verifier,
                ),
            )
        )
    elif runtime_result.verifier_rewards is not None or _legacy_verifier_reward_projection(runtime_result):
        raise MaterializationIntegrityError("verifier_output_missing")
    final_state = (
        "succeeded"
        if runtime_result.status == "succeeded"
        else "cancelled"
        if runtime_result.status == "cancelled"
        else "failed"
    )
    if final_state == "failed":
        events.append(
            TrialErrorEvent(
                emitted_at=runtime_result.finished_at,
                trial_id=trial_id,
                step_id="trial",
                seq=len(events),
                error_type=exception_info.exception_type if exception_info else runtime_result.status,
                message=(exception_info.exception_message if exception_info else
                         f"service execution runtime reported {runtime_result.status}"),
                traceback="",
            )
        )
    events.append(
        TrialEndEvent(
            emitted_at=runtime_result.finished_at,
            trial_id=trial_id,
            step_id="trial",
            seq=len(events),
            final_state=final_state,
            reward=runtime_result.verifier_rewards,
            failure_reason=(None if final_state == "succeeded" else runtime_result.status),
        )
    )
    return tuple(events)


def build_canonical_atif(
    events: Sequence[TrajectoryEvent], *, task_id: str, agent_name: str, agent_version: str,
) -> bytes:
    """Use the existing per-turn Harbor exporter for Terminus execution.

    Generic Loom ATIF intentionally aggregates LLM calls by task step. Its
    synthetic Gateway call events do not contain Terminus prompts or shell
    commands, which live in typed Harbor events. Reuse the established Harbor
    mapper instead of dropping those semantics during canonical publication.
    """
    generic = project_to_atif(
        events, task_id=task_id, agent_name=agent_name, agent_version=agent_version,
    )
    if agent_name != "terminus-2":
        return generic.model_dump_json(indent=2).encode("utf-8")
    if generic.metadata.final_state == "succeeded":
        if (
            not any(isinstance(event, Terminus2TurnEvent) for event in events)
            or Terminus2TrajectoryMapper.validate_turn_joins(events)
        ):
            raise MaterializationIntegrityError("terminus_turn_join_invalid")
    # Failed/cancelled attempts retain honest partial traces, including a turn
    # interrupted before its final observation; do not erase them as bad input.
    document = Terminus2TrajectoryMapper.project_to_atif(
        events, task_id=task_id, agent_name=agent_name, agent_version=agent_version,
    )
    model = next((event.model for event in events if isinstance(event, LLMCallEvent)), None)
    document["accounting"] = terminus_usage(
        list(events), TrialConfig(agent_name=agent_name, agent_model=model),
    )
    document.update({
        "trajectory_id": generic.trajectory_id,
        "session_id": generic.session_id,
        "metadata": generic.metadata.model_dump(mode="json"),
    })
    return json.dumps(document, ensure_ascii=False, indent=2).encode("utf-8")


class ServiceExecutionMaterializer:
    def __init__(
        self,
        *,
        session_factory: async_sessionmaker[AsyncSession],
        source_store: ObjectStore,
        source_bucket: str,
        canonical_store: ObjectStore,
        artifacts_bucket: str,
        trajectories_bucket: str,
        claim_ttl_seconds: float = 300.0,
        retry_base_seconds: float = 5.0,
        retry_max_seconds: float = 300.0,
        source_retention_seconds: int = 86_400,
    ) -> None:
        self._session_factory = session_factory
        self._source_store = source_store
        self._source_bucket = source_bucket
        self._canonical_store = canonical_store
        self._artifacts_bucket = artifacts_bucket
        self._trajectories_bucket = trajectories_bucket
        self._claim_ttl = timedelta(seconds=claim_ttl_seconds)
        self._retry_base = retry_base_seconds
        self._retry_max = retry_max_seconds
        self._source_retention = timedelta(seconds=max(0, source_retention_seconds))
        self._accounting_retry_after: dict[UUID, datetime] = {}

    async def retry_legacy_verifier_archive(self, *, lease_id: UUID, team_id: UUID) -> bool:
        """Requeue one diagnosed historical archive, without rerunning its execution."""
        return await self._retry_archive(lease_id=lease_id, team_id=team_id, usage_roundoff=False)

    async def retry_usage_roundoff_archive(self, *, lease_id: UUID, team_id: UUID) -> bool:
        """Recover retained successful native output after #2199, with no execution."""
        return await self._retry_archive(lease_id=lease_id, team_id=team_id, usage_roundoff=True)

    async def _retry_archive(self, *, lease_id: UUID, team_id: UUID, usage_roundoff: bool) -> bool:
        error_code = "usage_output_identity_drift" if usage_roundoff else "verifier_reward_drift"
        audit_key = "usage_roundoff_archival_recovery" if usage_roundoff else "legacy_verifier_archival_recovery"
        now = datetime.now(UTC)
        async with self._session_factory() as session:
            lease = await session.get(ServiceExecutionLease, lease_id, with_for_update=True)
            if lease is None or lease.team_id != team_id:
                return False
            trial = await session.get(Trial, lease.trial_id, with_for_update=True)
            if (
                trial is None or trial.team_id != team_id or trial.attempt_count != lease.attempt
                or trial.state != "failed" or trial.failure_reason != "output_unavailable"
                or lease.execution_role != "attempt" or lease.finalized_at is None
                or lease.desired_state != "deleted" or lease.observed_state != "deleted"
                or lease.deleted_at is None or lease.cleanup_state != "complete"
                or lease.output_commit_state != "committed"
                or lease.output_generation != lease.resource_generation
                or lease.materialization_state != "unavailable"
                or lease.materialization_error_code != error_code
                or lease.materialization_recovery_requested_at is not None
                or lease.canonical_trajectory_sha256 is not None or lease.canonical_atif_sha256 is not None
                or lease.source_cleanup_state != "not_ready"
            ):
                return False
            try:
                runtime = ExecutionRuntimeResultV1.model_validate((trial.result or {}).get("runtime_result"))
            except ValidationError:
                return False
            if usage_roundoff:
                if runtime.status != "succeeded" or trial.config.get("agent_name") != "terminus-2":
                    return False
            elif not _legacy_verifier_reward_projection(runtime):
                return False
            artifact = await session.scalar(select(Artifact).where(
                Artifact.control_producer_kind == "service_execution",
                Artifact.control_producer_id == lease.id,
            ).with_for_update())
            if artifact is None or artifact.team_id != team_id or artifact.trial_id != trial.id:
                return False
            metadata = artifact.artifact_metadata or {}
            if audit_key in metadata:
                return False
            artifact.artifact_metadata = {
                **metadata,
                audit_key: {
                    "previous_trial_state": trial.state,
                    "previous_failure_reason": trial.failure_reason,
                    "previous_failure_message": trial.failure_message,
                    "previous_finished_at": trial.finished_at.isoformat() if trial.finished_at else None,
                    "requested_at": now.isoformat(),
                    "error_code": lease.materialization_error_code,
                    "error_message": lease.materialization_error_message,
                    "output_manifest_sha256": lease.output_manifest_sha256,
                },
            }
            lease.materialization_state = "pending"
            lease.materialization_next_attempt_at = now
            lease.materialization_recovery_requested_at = now
            lease.materialization_claim_id = None
            lease.materialization_claim_expires_at = None
            lease.updated_at = now
            await session.commit()
            return True

    async def claim_one(
        self, *, now: datetime | None = None, lease_id: UUID | None = None,
    ) -> MaterializationClaim | None:
        current = now or datetime.now(UTC)
        async with self._session_factory() as session:
            lease = (
                await session.execute(
                    select(ServiceExecutionLease)
                    .join(Trial, Trial.id == ServiceExecutionLease.trial_id)
                    .where(
                        ServiceExecutionLease.output_commit_state == "committed",
                        *([ServiceExecutionLease.id == lease_id] if lease_id is not None else []),
                        or_(
                            and_(
                                ServiceExecutionLease.finalized_at.is_not(None),
                                Trial.state.in_(("materializing", "succeeded", "failed", "cancelled")),
                            ),
                            and_(
                                ServiceExecutionLease.finalized_at.is_(None),
                                ServiceExecutionLease.desired_state == "deleted",
                                ServiceExecutionLease.observed_state == "deleted",
                                ServiceExecutionLease.deleted_at.is_not(None),
                                ServiceExecutionLease.cleanup_state == "complete",
                                ServiceExecutionLease.execution_role == "attempt",
                                ServiceExecutionLease.output_generation
                                == ServiceExecutionLease.resource_generation,
                                Trial.attempt_count == ServiceExecutionLease.attempt,
                                Trial.state.in_(
                                    ("claimed", "running", "materializing", "succeeded", "failed", "cancelled")
                                ),
                            ),
                        ),
                        or_(
                            (
                                (ServiceExecutionLease.materialization_state == "pending")
                                & (
                                    ServiceExecutionLease.materialization_next_attempt_at
                                    <= current
                                )
                            ),
                            (
                                (ServiceExecutionLease.materialization_state == "running")
                                & (
                                    ServiceExecutionLease.materialization_claim_expires_at
                                    <= current
                                )
                            ),
                        ),
                    )
                    .order_by(
                        ServiceExecutionLease.materialization_next_attempt_at.asc().nullsfirst(),
                        ServiceExecutionLease.output_committed_at.asc(),
                        ServiceExecutionLease.id.asc(),
                    )
                    .with_for_update(skip_locked=True)
                    .limit(1)
                )
            ).scalar_one_or_none()
            if lease is None:
                return None
            if lease.finalized_at is None:
                await recover_deleted_committed_trial(session, lease=lease, observed_at=current)
            claim_id = uuid4()
            lease.materialization_state = "running"
            lease.materialization_attempts += 1
            lease.materialization_claim_id = claim_id
            lease.materialization_claim_expires_at = current + self._claim_ttl
            lease.materialization_started_at = current
            lease.materialization_next_attempt_at = None
            lease.updated_at = current
            await session.commit()
            return MaterializationClaim(lease_id=lease.id, claim_id=claim_id)

    async def _read_exact(self, *, key: str, expected: str, size: int) -> bytes:
        if size > _MAX_DERIVATION_BYTES:
            raise MaterializationIntegrityError("derivation_input_too_large")
        chunks = bytearray()
        try:
            async for chunk in self._source_store.stream_object(
                bucket=self._source_bucket, key=key, chunk_size=8 * 1024 * 1024
            ):
                if not chunk or len(chunks) + len(chunk) > size:
                    raise MaterializationIntegrityError("source_object_size_mismatch")
                chunks.extend(chunk)
        except KeyError as exc:
            raise MaterializationIntegrityError("source_object_missing") from exc
        body = bytes(chunks)
        if len(body) != size or _digest(body) != expected:
            raise MaterializationIntegrityError("source_object_digest_mismatch")
        return body

    async def _copy_exact(
        self, *, source_key: str, destination_key: str, expected: str, size: int
    ) -> None:
        observed = hashlib.sha256()
        total = 0

        async def chunks() -> AsyncIterator[bytes]:
            nonlocal total
            try:
                async for chunk in self._source_store.stream_object(
                    bucket=self._source_bucket,
                    key=source_key,
                    chunk_size=8 * 1024 * 1024,
                ):
                    if not chunk or total + len(chunk) > size:
                        raise MaterializationIntegrityError("source_object_size_mismatch")
                    total += len(chunk)
                    observed.update(chunk)
                    yield chunk
            except KeyError as exc:
                raise MaterializationIntegrityError("source_object_missing") from exc

        await self._canonical_store.put_object_stream(
            bucket=self._artifacts_bucket, key=destination_key, body=chunks()
        )
        if total != size or "sha256:" + observed.hexdigest() != expected:
            raise MaterializationIntegrityError("source_object_digest_mismatch")
        readback = await self._canonical_store.stat_object(
            bucket=self._artifacts_bucket, key=destination_key
        )
        if readback.content_length != size or (
            readback.checksum_sha256 is not None and readback.checksum_sha256 != expected
        ):
            raise MaterializationIntegrityError("canonical_object_readback_mismatch")

    async def _load_and_materialize(self, claim: MaterializationClaim) -> MaterializationResult:
        async with self._session_factory() as session:
            lease = await session.get(ServiceExecutionLease, claim.lease_id)
            if (
                lease is None
                or lease.materialization_state != "running"
                or lease.materialization_claim_id != claim.claim_id
                or lease.output_upload_session_id is None
            ):
                raise MaterializationIntegrityError("materialization_claim_lost")
            upload = await session.get(ArtifactUploadSession, lease.output_upload_session_id)
            trial = await session.get(Trial, lease.trial_id)
            artifact = (
                await session.execute(
                    select(Artifact).where(
                        Artifact.control_producer_kind == "service_execution",
                        Artifact.control_producer_id == lease.id,
                    )
                )
            ).scalar_one_or_none()
            files = list(
                (
                    await session.execute(
                        select(ArtifactUploadFile)
                        .where(ArtifactUploadFile.session_id == lease.output_upload_session_id)
                        .order_by(ArtifactUploadFile.file_index)
                    )
                ).scalars()
            )
            if upload is None or trial is None or artifact is None:
                raise MaterializationIntegrityError("source_identity_missing")
            try:
                task = await resolve_service_execution_task_snapshot(session, lease=lease, trial=trial)
            except ServiceExecutionTaskSnapshotError as exc:
                raise MaterializationIntegrityError(str(exc)) from exc
            upload_data = {
                "id": upload.id,
                "prefix": upload.prefix,
                "manifest": upload.canonical_manifest_json,
                "manifest_sha256": upload.manifest_sha256,
                "marker_sha256": upload.committed_marker_sha256,
                "state": upload.state,
            }
            lease_data = {
                "id": lease.id,
                "team_id": lease.team_id,
                "trial_id": lease.trial_id,
                "attempt": lease.attempt,
                "output_generation": lease.output_generation,
                "upload_session_id": lease.output_upload_session_id,
                "output_manifest_sha256": lease.output_manifest_sha256,
                "output_marker_sha256": lease.output_marker_sha256,
            }
            gateway_calls = (await read_service_execution_llm_calls(
                session, lease, generation=lease.output_generation,
            )) if trial.config.get("agent_name") == "terminus-2" else None
            trial_config_raw = trial.config
            trial_result_raw = trial.result
            trial_task_id = trial.task_id
            task_config_raw = task.config
            artifact_id = artifact.id

        if upload_data["state"] != "committed" or not isinstance(
            upload_data["manifest"], dict
        ):
            raise MaterializationIntegrityError("source_commit_not_committed")
        try:
            manifest = ArtifactCommitManifestV1.model_validate_json(
                canonical_document(upload_data["manifest"])
            )
            task_config = TaskConfig.model_validate(task_config_raw)
            trial_config = TrialConfig.model_validate(trial_config_raw)
        except ValidationError as exc:
            raise MaterializationIntegrityError("source_metadata_invalid", str(exc)) from exc
        # The task snapshot was bound through Trial.task_id above. Uploaded TaskSets
        # namespace that catalog identity without rewriting the source config's
        # task.id. Canonical events and ATIF must use the catalog identity, while
        # the original config remains intact for task semantics and provenance.
        manifest_body = canonical_document(manifest)
        if (
            manifest.session_id != upload_data["id"]
            or manifest.commit_kind != "service_execution_output"
            or len(manifest.artifacts) != 1
            or manifest.artifacts[0].artifact_id != artifact_id
            or _digest(manifest_body) != upload_data["manifest_sha256"]
            or upload_data["manifest_sha256"] != lease_data["output_manifest_sha256"]
        ):
            raise MaterializationIntegrityError("root_manifest_identity_drift")
        producer = manifest.producer_identity
        if (
            producer.get("team_id") != str(lease_data["team_id"])
            or producer.get("service_execution_lease_id") != str(lease_data["id"])
            or producer.get("service_execution_generation") != (
                lease_data["output_generation"]
            )
        ):
            raise MaterializationIntegrityError("source_producer_identity_drift")
        prefix = str(upload_data["prefix"])
        root_source_key = prefix + "_manifest.json"
        marker_source_key = prefix + "_COMMITTED"
        await self._read_exact(
            key=root_source_key, expected=str(upload_data["manifest_sha256"]), size=len(manifest_body)
        )
        marker_body = await self._read_exact(
            key=marker_source_key,
            expected=str(upload_data["marker_sha256"]),
            size=len(
                canonical_document(
                    ArtifactCommitMarkerV1(
                        commit_kind="service_execution_output",
                        manifest_sha256=str(upload_data["manifest_sha256"]),
                        session_id=cast(UUID, upload_data["id"]),
                    )
                )
            ),
        )
        try:
            marker = ArtifactCommitMarkerV1.model_validate_json(marker_body)
        except ValidationError as exc:
            raise MaterializationIntegrityError("commit_marker_invalid") from exc
        if (
            marker.session_id != upload_data["id"]
            or marker.manifest_sha256 != upload_data["manifest_sha256"]
            or upload_data["marker_sha256"] != lease_data["output_marker_sha256"]
        ):
            raise MaterializationIntegrityError("commit_marker_identity_drift")

        record = manifest.artifacts[0]
        item_source_key = f"{prefix}artifacts/{artifact_id}/_artifact_manifest.json"
        item_body = await self._read_exact(
            key=item_source_key,
            expected=record.manifest_sha256,
            size=len(
                canonical_document(
                    ArtifactManifestV1(
                        artifact_id=record.artifact_id,
                        artifact_name=record.artifact_name,
                        artifact_type=record.artifact_type,
                        content_sha256=record.content_sha256,
                        stored_size_bytes=sum(item.size_bytes for item in record.stored_files),
                        unpacked_size_bytes=sum(item.size_bytes for item in record.stored_files),
                        file_count=len(record.stored_files),
                        stored_files=record.stored_files,
                        lineage_artifact_ids=manifest.input_lineage_artifact_ids,
                        lineage_digests=manifest.input_lineage_digests,
                    )
                )
            ),
        )
        try:
            item_manifest = ArtifactManifestV1.model_validate_json(item_body)
        except ValidationError as exc:
            raise MaterializationIntegrityError("artifact_manifest_invalid") from exc
        if item_manifest.stored_files != record.stored_files:
            raise MaterializationIntegrityError("artifact_manifest_inventory_drift")
        db_inventory = [
            (row.file_index, row.relative_path, row.actual_size, row.computed_sha256, row.state)
            for row in files
        ]
        manifest_inventory = [
            (row.file_index, row.relative_path, row.size_bytes, row.sha256, "verified")
            for row in record.stored_files
        ]
        if db_inventory != manifest_inventory:
            raise MaterializationIntegrityError("artifact_file_inventory_drift")

        destination_prefix = (
            f"trials/{lease_data['team_id']}/{lease_data['trial_id']}/attempts/"
            f"{lease_data['attempt']}/bundles/{artifact_id}/"
        )
        materialized: list[MaterializedFile] = []
        source_evidence: list[MaterializedFile] = []
        derivation_inputs: dict[str, bytes] = {}
        for file in record.stored_files:
            source_key = f"{prefix}artifacts/{artifact_id}/{file.relative_path}"
            destination_key = destination_prefix + "files/" + file.relative_path
            await self._copy_exact(
                source_key=source_key,
                destination_key=destination_key,
                expected=file.sha256,
                size=file.size_bytes,
            )
            materialized.append(
                MaterializedFile(
                    relative_path=file.relative_path,
                    media_type=file.media_type,
                    size_bytes=file.size_bytes,
                    sha256=file.sha256,
                    key=destination_key,
                )
            )
            if file.relative_path in {
                _RESULT_PATH,
                _TRACE_PATH,
                _USAGE_PATH,
                _VERIFIER_PATH,
                *_EXCEPTION_PATHS,
            }:
                derivation_inputs[file.relative_path] = await self._read_exact(
                    key=source_key, expected=file.sha256, size=file.size_bytes
                )
        for source_key, destination_name, body, expected in (
            (root_source_key, "source/_manifest.json", manifest_body, str(upload_data["manifest_sha256"])),
            (marker_source_key, "source/_COMMITTED", marker_body, str(upload_data["marker_sha256"])),
            (item_source_key, "source/_artifact_manifest.json", item_body, record.manifest_sha256),
        ):
            await self._copy_exact(
                source_key=source_key,
                destination_key=destination_prefix + destination_name,
                expected=expected,
                size=len(body),
            )
            source_evidence.append(
                MaterializedFile(
                    relative_path=destination_name,
                    media_type="application/json",
                    size_bytes=len(body),
                    sha256=expected,
                    key=destination_prefix + destination_name,
                )
            )
        try:
            runtime_result = ExecutionRuntimeResultV1.model_validate_json(
                derivation_inputs[_RESULT_PATH]
            )
        except (KeyError, ValidationError) as exc:
            raise MaterializationIntegrityError("runtime_result_invalid") from exc
        if (
            not isinstance(trial_result_raw, dict)
            or trial_result_raw.get("runtime_result") != runtime_result.model_dump(mode="json")
            or trial_result_raw.get("output_manifest_sha256")
            != lease_data["output_manifest_sha256"]
            or trial_result_raw.get("output_marker_sha256")
            != lease_data["output_marker_sha256"]
        ):
            raise MaterializationIntegrityError("runtime_result_projection_drift")
        trace_body = derivation_inputs.get(_TRACE_PATH)
        exception_info = read_exception_info(runtime_result, derivation_inputs)
        if runtime_result.status == "succeeded" and trace_body is None:
            raise MaterializationIntegrityError("trajectory_output_missing")
        if runtime_result.status == "succeeded":
            validate_usage_accounting(
                trace_body=trace_body,
                usage_body=derivation_inputs.get(_USAGE_PATH),
                trial_config=trial_config,
            )
        events = build_canonical_events(
            trial_id=cast(UUID, lease_data["trial_id"]),
            task_id=trial_task_id,
            task_config=task_config,
            trial_config=trial_config,
            runtime_result=runtime_result,
            trace_body=trace_body,
            verifier_body=derivation_inputs.get(_VERIFIER_PATH),
            gateway_calls=gateway_calls,
            exception_info=exception_info,
        )
        if gateway_calls is not None:
            # Preserve the immutable runtime projection as source evidence. The
            # canonical accounting is independently derived from the DB ledger.
            corrected = {
                _TRACE_PATH: _canonical_jsonl(events),
                _USAGE_PATH: canonical_document(terminus_usage(list(events), trial_config)),
                "accounting/gateway-calls.json": canonical_document({
                    "schema_version": "loom.gateway-lease-ledger.v1", "calls": gateway_calls,
                }),
            }
            for item in list(materialized):
                if item.relative_path in corrected:
                    source_evidence.append(MaterializedFile(
                        relative_path="source/" + item.relative_path,
                        media_type=item.media_type, size_bytes=item.size_bytes,
                        sha256=item.sha256, key=item.key,
                    ))
                    materialized.remove(item)
            for path, body in corrected.items():
                key = destination_prefix + "canonical/" + path
                await self._canonical_store.put_object_with_metadata(
                    bucket=self._artifacts_bucket, key=key, body=body,
                )
                materialized.append(MaterializedFile(
                    relative_path=path,
                    media_type="application/x-ndjson" if path.endswith(".jsonl") else "application/json",
                    size_bytes=len(body), sha256=_digest(body), key=key,
                ))
        events_body = _canonical_jsonl(events)
        atif_body = build_canonical_atif(
            events,
            task_id=trial_task_id,
            agent_name=trial_config.agent_name,
            agent_version=trial_config.agent_version or task_config.agent.version or "service-execution-v1",
        )
        identity = TrajectoryObjectIdentity(
            bucket=self._trajectories_bucket,
            team_id=cast(UUID, lease_data["team_id"]),
            trial_id=cast(UUID, lease_data["trial_id"]),
            attempt_count=cast(int, lease_data["attempt"]),
        )
        await self._canonical_store.put_object_with_metadata(
            bucket=self._trajectories_bucket, key=identity.events_key, body=events_body
        )
        await self._canonical_store.put_object_with_metadata(
            bucket=self._trajectories_bucket, key=identity.atif_key, body=atif_body
        )
        for key, body in ((identity.events_key, events_body), (identity.atif_key, atif_body)):
            readback = await self._canonical_store.stat_object(
                bucket=self._trajectories_bucket, key=key
            )
            if readback.content_length != len(body) or (
                readback.checksum_sha256 is not None
                and readback.checksum_sha256 != _digest(body)
            ):
                raise MaterializationIntegrityError("canonical_trajectory_readback_mismatch")
        return MaterializationResult(
            artifact_id=artifact_id,
            files=tuple(materialized),
            source_evidence=tuple(source_evidence),
            events=events,
            events_body=events_body,
            atif_body=atif_body,
            events_uri=identity.events_uri,
            atif_uri=identity.atif_uri,
            events_sha256=_digest(events_body),
            atif_sha256=_digest(atif_body),
            final_trial_state=(
                "succeeded"
                if runtime_result.status == "succeeded"
                else "cancelled"
                if runtime_result.status == "cancelled"
                else "failed"
            ),
            failure_reason=(
                None if runtime_result.status == "succeeded" else runtime_result.status
            ),
            accounting_call_count=len(gateway_calls) if gateway_calls is not None else None,
            exception_info=exception_info,
            preserve_trial_outcome=_legacy_verifier_reward_projection(runtime_result),
        )

    async def _commit(self, claim: MaterializationClaim, result: MaterializationResult) -> bool:
        now = datetime.now(UTC)
        async with self._session_factory() as session:
            lease = await session.get(ServiceExecutionLease, claim.lease_id, with_for_update=True)
            if (
                lease is None
                or lease.materialization_state != "running"
                or lease.materialization_claim_id != claim.claim_id
            ):
                return False
            trial = await session.get(Trial, lease.trial_id, with_for_update=True)
            artifact = await session.get(Artifact, result.artifact_id, with_for_update=True)
            if (
                trial is None
                or artifact is None
                or trial.state not in {"materializing", "succeeded", "failed", "cancelled"}
            ):
                raise MaterializationIntegrityError("canonical_projection_identity_drift")
            existing = list(
                (
                    await session.execute(
                        select(TrialEvent)
                        .where(TrialEvent.trial_id == trial.id)
                        .order_by(TrialEvent.seq)
                    )
                ).scalars()
            )
            event_authority_id = await ensure_trial_event_lifecycle_authority(
                session,
                trial_id=trial.id,
                expected_team_id=trial.team_id,
            )
            by_seq = {row.seq: row for row in existing}
            for event in result.events:
                payload = event.model_dump(mode="json")
                present = by_seq.get(event.seq)
                if present is not None:
                    if present.kind != event.kind.value or present.payload != payload:
                        raise MaterializationIntegrityError("trial_event_identity_drift")
                    continue
                session.add(
                    TrialEvent(
                        trial_id=trial.id,
                        seq=event.seq,
                        kind=event.kind.value,
                        source="service-execution-materializer",
                        schema_version=1,
                        payload=payload,
                        lifecycle_authority_id=event_authority_id,
                    )
                )
            file_rows = [
                {
                    "relative_path": item.relative_path,
                    "media_type": item.media_type,
                    "size_bytes": item.size_bytes,
                    "sha256": item.sha256,
                    "bucket": self._artifacts_bucket,
                    "key": item.key,
                }
                for item in result.files
            ]
            artifact.storage = {
                "schema_version": "loom.canonical-trial-bundle-storage.v1",
                "attempt": lease.attempt,
                "source_upload_session_id": str(lease.output_upload_session_id),
                "files": file_rows,
                "source_evidence": [
                    {
                        "relative_path": item.relative_path,
                        "media_type": item.media_type,
                        "size_bytes": item.size_bytes,
                        "sha256": item.sha256,
                        "bucket": self._artifacts_bucket,
                        "key": item.key,
                    }
                    for item in result.source_evidence
                ],
            }
            artifact.artifact_metadata = {
                **(artifact.artifact_metadata or {}),
                "materialization_state": "committed",
                "materialized_at": now.isoformat(),
                **({"accounting_source": "gateway_lease_ledger"} if any(
                    item.relative_path == "accounting/gateway-calls.json" for item in result.files
                ) else {}),
                **({"accounting_call_count": result.accounting_call_count}
                   if result.accounting_call_count is not None else {}),
            }
            trial.trajectory_index = {
                "schema_version": "1",
                "trial_id": str(trial.id),
                "team_id": str(trial.team_id),
                "task_id": trial.task_id,
                "trajectory_uri": result.events_uri,
                "trajectory_sha256": result.events_sha256.removeprefix("sha256:"),
                "trajectory_size_bytes": len(result.events_body),
                "trajectory_version_id": None,
                "atif_uri": result.atif_uri,
                "atif_sha256": result.atif_sha256.removeprefix("sha256:"),
                "atif_size_bytes": len(result.atif_body),
                "atif_version_id": None,
                "atif_schema_version": json.loads(result.atif_body)["schema_version"],
                "attempt": lease.attempt,
                # Replace the complete index for this materialized attempt.
                # Never accumulate files from earlier Trial retries.
                "artifacts": file_rows if any(
                    isinstance(event, Terminus2TurnEvent) for event in result.events
                ) else [],
            }
            artifact_authority_id = await ensure_artifact_lifecycle_authority(
                session,
                artifact_id=artifact.id,
                team_id=artifact.team_id,
                created_at=artifact.created_at,
            )
            if artifact.lifecycle_authority_id is None:
                artifact.lifecycle_authority_id = artifact_authority_id
            elif artifact.lifecycle_authority_id != artifact_authority_id:
                raise MaterializationIntegrityError("artifact_lifecycle_authority_drift")
            for item in (*result.files, *result.source_evidence):
                await register_lifecycle_object(
                    session,
                    authority_id=artifact_authority_id,
                    bucket=self._artifacts_bucket,
                    object_key=item.key,
                    version_id=None,
                    content_sha256=item.sha256.removeprefix("sha256:"),
                    size_bytes=item.size_bytes,
                    created_at=artifact.created_at,
                )
            for bucket, uri, digest, size in (
                (
                    self._trajectories_bucket,
                    result.events_uri,
                    result.events_sha256,
                    len(result.events_body),
                ),
                (
                    self._trajectories_bucket,
                    result.atif_uri,
                    result.atif_sha256,
                    len(result.atif_body),
                ),
            ):
                prefix = f"s3://{bucket}/"
                if not uri.startswith(prefix):
                    raise MaterializationIntegrityError("canonical_trajectory_identity_drift")
                await register_lifecycle_object(
                    session,
                    authority_id=artifact_authority_id,
                    bucket=bucket,
                    object_key=uri.removeprefix(prefix),
                    version_id=None,
                    content_sha256=digest.removeprefix("sha256:"),
                    size_bytes=size,
                    created_at=artifact.created_at,
                )
            if result.final_trial_state == "succeeded":
                recovered_usage = (
                    lease.materialization_recovery_requested_at is not None
                    and "usage_roundoff_archival_recovery" in (artifact.artifact_metadata or {})
                    and trial.state == "failed" and trial.failure_reason == "output_unavailable"
                    and (trial.result or {}).get("runtime_result", {}).get("status") == "succeeded"
                )
                if trial.state not in {"materializing", "succeeded"} and not recovered_usage:
                    raise MaterializationIntegrityError("terminal_trial_state_drift")
                trial.state = "succeeded"
                trial.finished_at = now
                trial.failure_reason = None
                trial.failure_message = None
            elif trial.state != result.final_trial_state:
                raise MaterializationIntegrityError("terminal_trial_state_drift")
            if result.exception_info is not None and not result.preserve_trial_outcome:
                trial.result = {**(trial.result or {}),
                                "exception_info": result.exception_info.model_dump(mode="json")}
                trial.failure_message = (
                    f"{result.exception_info.exception_type}: {result.exception_info.exception_message}"
                )[:2000]
            lease.materialization_state = "committed"
            lease.materialization_claim_id = None
            lease.materialization_claim_expires_at = None
            lease.materialization_next_attempt_at = None
            lease.materialization_committed_at = now
            lease.materialization_error_code = None
            lease.materialization_error_message = None
            lease.canonical_trajectory_sha256 = result.events_sha256
            lease.canonical_atif_sha256 = result.atif_sha256
            lease.source_cleanup_state = "retained"
            lease.source_retain_until = now + self._source_retention
            lease.source_cleanup_claim_id = None
            lease.source_cleanup_claim_expires_at = None
            lease.source_cleanup_error_message = None
            lease.updated_at = now
            await session.commit()
            SERVICE_EXECUTION_MATERIALIZATION_COMPLETED_TOTAL.inc()
            return True

    async def _retry(self, claim: MaterializationClaim, exc: Exception) -> None:
        now = datetime.now(UTC)
        async with self._session_factory() as session:
            lease = await session.get(ServiceExecutionLease, claim.lease_id, with_for_update=True)
            if (
                lease is None
                or lease.materialization_state != "running"
                or lease.materialization_claim_id != claim.claim_id
            ):
                return
            delay = min(
                self._retry_max,
                self._retry_base * (2 ** max(0, lease.materialization_attempts - 1)),
            )
            lease.materialization_state = "pending"
            lease.materialization_next_attempt_at = now + timedelta(seconds=delay)
            lease.materialization_claim_id = None
            lease.materialization_claim_expires_at = None
            lease.materialization_error_code = "transient_materialization_error"
            lease.materialization_error_message = str(exc)[:2000] or type(exc).__name__
            lease.updated_at = now
            await session.commit()
            SERVICE_EXECUTION_MATERIALIZATION_RETRIES_TOTAL.inc()

    async def _unavailable(
        self, claim: MaterializationClaim, exc: MaterializationIntegrityError
    ) -> None:
        now = datetime.now(UTC)
        async with self._session_factory() as session:
            lease = await session.get(ServiceExecutionLease, claim.lease_id, with_for_update=True)
            if (
                lease is None
                or lease.materialization_state != "running"
                or lease.materialization_claim_id != claim.claim_id
            ):
                return
            trial = await session.get(Trial, lease.trial_id, with_for_update=True)
            if trial is None:
                return
            lease.materialization_state = "unavailable"
            lease.materialization_next_attempt_at = None
            lease.materialization_claim_id = None
            lease.materialization_claim_expires_at = None
            lease.materialization_error_code = exc.code[:120]
            lease.materialization_error_message = str(exc)[:2000]
            lease.updated_at = now
            # A recovery failure belongs to the archive audit, not a new Trial outcome.
            if lease.materialization_recovery_requested_at is None:
                trial.state = "failed"
                trial.failure_reason = "output_unavailable"
                trial.failure_message = f"canonical materialization failed: {exc.code}"
                trial.finished_at = now
            await session.commit()
            SERVICE_EXECUTION_MATERIALIZATION_FAILURES_TOTAL.labels(exc.code).inc()

    async def claim_source_cleanup(
        self, *, now: datetime | None = None
    ) -> SourceCleanupClaim | None:
        current = now or datetime.now(UTC)
        async with self._session_factory() as session:
            lease = (
                await session.execute(
                    select(ServiceExecutionLease)
                    .where(
                        ServiceExecutionLease.materialization_state == "committed",
                        or_(
                            (
                                (ServiceExecutionLease.source_cleanup_state == "retained")
                                & (ServiceExecutionLease.source_retain_until <= current)
                            ),
                            (
                                (ServiceExecutionLease.source_cleanup_state == "running")
                                & (
                                    ServiceExecutionLease.source_cleanup_claim_expires_at
                                    <= current
                                )
                            ),
                        ),
                    )
                    .order_by(
                        ServiceExecutionLease.source_retain_until.asc(),
                        ServiceExecutionLease.id.asc(),
                    )
                    .with_for_update(skip_locked=True)
                    .limit(1)
                )
            ).scalar_one_or_none()
            if lease is None:
                return None
            claim_id = uuid4()
            lease.source_cleanup_state = "running"
            lease.source_cleanup_attempts += 1
            lease.source_cleanup_claim_id = claim_id
            lease.source_cleanup_claim_expires_at = current + self._claim_ttl
            lease.updated_at = current
            await session.commit()
            return SourceCleanupClaim(lease_id=lease.id, claim_id=claim_id)

    async def _source_cleanup_keys(self, claim: SourceCleanupClaim) -> tuple[str, ...]:
        async with self._session_factory() as session:
            lease = await session.get(ServiceExecutionLease, claim.lease_id)
            if (
                lease is None
                or lease.source_cleanup_state != "running"
                or lease.source_cleanup_claim_id != claim.claim_id
                or lease.output_upload_session_id is None
                or lease.output_generation is None
            ):
                raise MaterializationIntegrityError("source_cleanup_claim_lost")
            upload = await session.get(ArtifactUploadSession, lease.output_upload_session_id)
            if upload is None or not isinstance(upload.canonical_manifest_json, dict):
                raise MaterializationIntegrityError("source_cleanup_manifest_missing")
            expected_prefix = (
                f"service-executions/{lease.team_id}/{lease.id}/"
                f"{lease.output_generation}/output/"
            )
            if upload.prefix != expected_prefix:
                raise MaterializationIntegrityError("source_cleanup_prefix_drift")
            try:
                manifest = ArtifactCommitManifestV1.model_validate_json(
                    canonical_document(upload.canonical_manifest_json)
                )
            except ValidationError as exc:
                raise MaterializationIntegrityError("source_cleanup_manifest_invalid") from exc
        keys: list[str] = []
        for record in manifest.artifacts:
            keys.extend(
                f"{expected_prefix}artifacts/{record.artifact_id}/{item.relative_path}"
                for item in record.stored_files
            )
            keys.append(
                f"{expected_prefix}artifacts/{record.artifact_id}/_artifact_manifest.json"
            )
        keys.extend((expected_prefix + "_manifest.json", expected_prefix + "_COMMITTED"))
        return tuple(keys)

    async def _finish_source_cleanup(self, claim: SourceCleanupClaim) -> None:
        now = datetime.now(UTC)
        async with self._session_factory() as session:
            lease = await session.get(ServiceExecutionLease, claim.lease_id, with_for_update=True)
            if (
                lease is None
                or lease.source_cleanup_state != "running"
                or lease.source_cleanup_claim_id != claim.claim_id
            ):
                return
            lease.source_cleanup_state = "complete"
            lease.source_cleanup_claim_id = None
            lease.source_cleanup_claim_expires_at = None
            lease.source_cleanup_error_message = None
            lease.updated_at = now
            await session.commit()
            SERVICE_EXECUTION_SOURCE_CLEANUP_COMPLETED_TOTAL.inc()

    async def _retry_source_cleanup(
        self, claim: SourceCleanupClaim, exc: Exception
    ) -> None:
        now = datetime.now(UTC)
        async with self._session_factory() as session:
            lease = await session.get(ServiceExecutionLease, claim.lease_id, with_for_update=True)
            if (
                lease is None
                or lease.source_cleanup_state != "running"
                or lease.source_cleanup_claim_id != claim.claim_id
            ):
                return
            delay = min(
                self._retry_max,
                self._retry_base * (2 ** max(0, lease.source_cleanup_attempts - 1)),
            )
            lease.source_cleanup_state = "retained"
            lease.source_retain_until = now + timedelta(seconds=delay)
            lease.source_cleanup_claim_id = None
            lease.source_cleanup_claim_expires_at = None
            lease.source_cleanup_error_message = str(exc)[:2000] or type(exc).__name__
            lease.updated_at = now
            await session.commit()
            SERVICE_EXECUTION_SOURCE_CLEANUP_RETRIES_TOTAL.inc()

    async def cleanup_source_once(self) -> bool:
        claim = await self.claim_source_cleanup()
        if claim is None:
            return False
        try:
            for key in await self._source_cleanup_keys(claim):
                await self._source_store.delete_object(bucket=self._source_bucket, key=key)
            await self._finish_source_cleanup(claim)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            _raise_if_cancellation_requested(exc)
            logger.warning("service execution source cleanup retry: %s", exc)
            await self._retry_source_cleanup(claim, exc)
        return True

    async def refresh_metrics(self, *, now: datetime | None = None) -> None:
        current = now or datetime.now(UTC)
        async with self._session_factory() as session:
            row = (
                await session.execute(
                    select(
                        func.count(ServiceExecutionLease.id),
                        func.coalesce(func.sum(ArtifactUploadSession.actual_total_bytes), 0),
                        func.min(ServiceExecutionLease.output_committed_at),
                    )
                    .join(
                        ArtifactUploadSession,
                        ArtifactUploadSession.id
                        == ServiceExecutionLease.output_upload_session_id,
                    )
                    .where(
                        ServiceExecutionLease.materialization_state.in_(("pending", "running"))
                    )
                )
            ).one()
            cleanup_row = (
                await session.execute(
                    select(
                        func.count(ServiceExecutionLease.id),
                        func.coalesce(func.sum(ArtifactUploadSession.actual_total_bytes), 0),
                    )
                    .join(
                        ArtifactUploadSession,
                        ArtifactUploadSession.id
                        == ServiceExecutionLease.output_upload_session_id,
                    )
                    .where(
                        ServiceExecutionLease.source_cleanup_state.in_(("retained", "running"))
                    )
                )
            ).one()
            unavailable_row = (
                await session.execute(
                    select(
                        func.count(ServiceExecutionLease.id),
                        func.coalesce(func.sum(ArtifactUploadSession.actual_total_bytes), 0),
                    )
                    .join(
                        ArtifactUploadSession,
                        ArtifactUploadSession.id
                        == ServiceExecutionLease.output_upload_session_id,
                    )
                    .where(ServiceExecutionLease.materialization_state == "unavailable")
                )
            ).one()
        backlog, pending_bytes, oldest = row
        SERVICE_EXECUTION_MATERIALIZATION_BACKLOG.set(int(backlog or 0))
        SERVICE_EXECUTION_MATERIALIZATION_PENDING_BYTES.set(int(pending_bytes or 0))
        SERVICE_EXECUTION_MATERIALIZATION_OLDEST_AGE_SECONDS.set(
            max(0.0, (current - oldest).total_seconds()) if oldest is not None else 0.0
        )
        SERVICE_EXECUTION_SOURCE_SPOOL_RETAINED.set(int(cleanup_row[0] or 0))
        SERVICE_EXECUTION_SOURCE_SPOOL_BYTES.set(int(cleanup_row[1] or 0))
        SERVICE_EXECUTION_MATERIALIZATION_UNAVAILABLE.set(int(unavailable_row[0] or 0))
        SERVICE_EXECUTION_MATERIALIZATION_UNAVAILABLE_BYTES.set(
            int(unavailable_row[1] or 0)
        )

    async def reconcile_accounting_once(self, *, now: datetime | None = None) -> bool:
        """Refresh one retained terminal archive after a newly committed Gateway row.

        Calls are immutable inserts. Compare the number actually included in the
        archive, not wall time: a request can begin before materialization and
        commit after it. No new execution or change to the source ACK is needed.
        """
        from loom_control_plane.service_execution_accounting_repair import repair_accounting

        current = now or datetime.now(UTC)
        self._accounting_retry_after = {
            key: until for key, until in self._accounting_retry_after.items() if until > current
        }
        binding = LlmCall.provider_extras["_loom_raw_provider_log"]["service_execution"]
        call_count = (
            select(func.count(LlmCall.id)).where(
                LlmCall.team_id == ServiceExecutionLease.team_id,
                LlmCall.trial_id == ServiceExecutionLease.trial_id,
                LlmCall.step_id == "agent",
                binding["lease_id"].astext == sql_cast(ServiceExecutionLease.id, Text),
                binding["generation"].astext == sql_cast(ServiceExecutionLease.output_generation, Text),
            ).correlate(ServiceExecutionLease).scalar_subquery()
        )
        async with self._session_factory() as session:
            candidate = (await session.execute(
                select(ServiceExecutionLease.id, ServiceExecutionLease.team_id)
                .join(Trial, Trial.id == ServiceExecutionLease.trial_id)
                .join(Artifact, (Artifact.control_producer_kind == "service_execution")
                      & (Artifact.control_producer_id == ServiceExecutionLease.id)
                      & (Artifact.team_id == ServiceExecutionLease.team_id)
                      & (Artifact.trial_id == ServiceExecutionLease.trial_id))
                .join(DataLifecycleAuthority, DataLifecycleAuthority.id == Artifact.lifecycle_authority_id)
                .where(
                    ServiceExecutionLease.output_commit_state == "committed",
                    ServiceExecutionLease.materialization_state == "committed",
                    ServiceExecutionLease.output_generation.is_not(None),
                    Trial.team_id == ServiceExecutionLease.team_id,
                    Trial.attempt_count == ServiceExecutionLease.attempt,
                    Trial.state.in_(("succeeded", "failed", "cancelled")),
                    Trial.config["agent_name"].astext == "terminus-2",
                    DataLifecycleAuthority.state == "active",
                    or_(DataLifecycleAuthority.pinned, DataLifecycleAuthority.expires_at > current),
                    Artifact.artifact_metadata["accounting_call_count"].is_distinct_from(func.to_jsonb(call_count)),
                    ServiceExecutionLease.id.not_in(tuple(self._accounting_retry_after)),
                ).order_by(Artifact.created_at, ServiceExecutionLease.id).limit(1)
            )).one_or_none()
        if candidate is None:
            return False
        lease_id, team_id = candidate
        try:
            await repair_accounting(
                session_factory=self._session_factory, store=self._canonical_store,
                artifacts_bucket=self._artifacts_bucket, trajectories_bucket=self._trajectories_bucket,
                lease_id=lease_id, team_id=team_id, apply=True,
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            _raise_if_cancellation_requested(exc)
            # A broken old archive must not starve current materialization or
            # other corrections. State remains in the existing ledger on restart.
            if len(self._accounting_retry_after) >= 128:
                self._accounting_retry_after.pop(next(iter(self._accounting_retry_after)))
            self._accounting_retry_after[lease_id] = current + timedelta(seconds=self._retry_max)
            logger.warning("native accounting refresh deferred for lease %s (%s)", lease_id, type(exc).__name__)
            return False
        return True

    async def run_once(self, *, lease_id: UUID | None = None) -> bool:
        """Materialize one eligible commit, optionally restricted to one execution."""
        claim = (
            await self.claim_one(lease_id=lease_id)
            if lease_id is not None else await self.claim_one()
        )
        if claim is None:
            return False
        try:
            result = await self._load_and_materialize(claim)
            await self._commit(claim, result)
        except MaterializationIntegrityError as exc:
            await self._unavailable(claim, exc)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # object-store and database transport failures retry
            _raise_if_cancellation_requested(exc)
            logger.warning("service execution materialization retry: %s", exc)
            await self._retry(claim, exc)
        return True


async def run_service_execution_materializer_loop(
    *,
    materializer: ServiceExecutionMaterializer,
    interval_seconds: float,
    concurrency: int = 1,
    stop_event: asyncio.Event | None = None,
) -> None:
    stop = stop_event or asyncio.Event()

    async def wait_for_next_cycle() -> None:
        try:
            await asyncio.wait_for(stop.wait(), timeout=interval_seconds)
        except TimeoutError:
            pass

    async def worker(index: int) -> None:
        while not stop.is_set():
            try:
                processed = await materializer.run_once()
                if stop.is_set():
                    return
                cleaned = await materializer.cleanup_source_once()
                if stop.is_set():
                    return
                if index == 0:
                    await materializer.refresh_metrics()
                    processed = await materializer.reconcile_accounting_once() or processed
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                _raise_if_cancellation_requested(exc)
                if stop.is_set():
                    return
                logger.exception("service execution materializer loop failed; retrying")
                await wait_for_next_cycle()
                continue
            if stop.is_set():
                return
            if not processed and not cleaned:
                await wait_for_next_cycle()

    async with asyncio.TaskGroup() as workers:
        for index in range(max(1, concurrency)):
            workers.create_task(
                worker(index),
                name=f"loom-cp-service-execution-materializer-{index}",
            )


__all__ = [
    "MaterializationClaim",
    "MaterializationIntegrityError",
    "ServiceExecutionMaterializer",
    "SourceCleanupClaim",
    "build_canonical_events",
    "run_service_execution_materializer_loop",
    "validate_usage_accounting",
]

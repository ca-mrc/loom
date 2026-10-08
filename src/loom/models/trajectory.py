"""Trajectory event catalog — JSONL events written during a trial.

Spec §4.4 (catalog) + §4.4.1 (LLMCallEvent — the training-data load-bearing one).
All events share an envelope and discriminate via `kind`. Per-event-type
classes follow. ATIF v1.7 projection consumes this stream at finalize.
"""

from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

from loom.request_params import legacy_request_params


class EventKind(StrEnum):
    # Trial-level
    TRIAL_START = "trial_start"
    TRIAL_END = "trial_end"
    TRIAL_ERROR = "trial_error"
    TRIAL_CANCELLED = "trial_cancelled"
    # Step-level
    STEP_START = "step_start"
    STEP_END = "step_end"
    # Environment-level
    ENV_START = "env_start"
    ENV_READY = "env_ready"
    ENV_STOP = "env_stop"
    ENV_EXEC = "env_exec"
    # File ops
    FILE_UPLOAD = "file_upload"
    FILE_DOWNLOAD = "file_download"
    # Agent-level
    LLM_CALL = "llm_call"
    TOOL_USE = "tool_use"
    AGENT_THOUGHT = "agent_thought"
    AGENT_RETRY = "agent_retry"
    AGENT_TIMEOUT = "agent_timeout"
    AGENT_ATTEMPT_START = "agent_attempt_start"
    AGENT_STEP_TOKEN_GRANT = "agent_step_token_grant"
    # Verifier-level
    VERIFIER_START = "verifier_start"
    VERIFIER_END = "verifier_end"
    VERIFIER_CHECK = "verifier_check"
    # Net/policy
    NETWORK_POLICY_CHANGE = "network_policy_change"
    # Sys
    WORKER_LOST_CLAIM = "worker_lost_claim"
    WORKER_DRAIN_INTERRUPTED = "worker_drain_interrupted"
    # Terminus-2 native runtime (#744)
    TERMINUS2_RUNTIME_PROVENANCE = "terminus2_runtime_provenance"
    TERMINUS2_USER_PROMPT = "terminus2_user_prompt"
    TERMINUS2_TURN = "terminus2_turn"
    TERMINUS2_COMMAND = "terminus2_command"
    TERMINUS2_TERMINAL_OBSERVATION = "terminus2_terminal_observation"
    TERMINUS2_PARSE_RETRY = "terminus2_parse_retry"
    TERMINUS2_CONTEXT_BOUNDARY = "terminus2_context_boundary"
    TERMINUS2_ARTIFACT_REF = "terminus2_artifact_ref"
    TERMINUS2_MODEL_SWITCH = "terminus2_model_switch"
    TERMINUS2_MODEL_SWITCH_PLANNED = "terminus2_model_switch_planned"
    TERMINUS2_MODEL_MIX_PLANNED = "terminus2_model_mix_planned"
    TERMINUS2_LLM_CALL_STARTED = "terminus2_llm_call_started"
    TERMINUS2_LLM_CALL_COMPLETED = "terminus2_llm_call_completed"
    TERMINUS2_LLM_CALL_FAILED = "terminus2_llm_call_failed"
    TERMINUS2_EPISODE_CHECKPOINT = "terminus2_episode_checkpoint"
    TERMINUS2_RECOVERY_FAILED = "terminus2_recovery_failed"
    # OpenHands SDK native runtime (#1590)
    OPENHANDS_SDK_RUNTIME_PROVENANCE = "openhands_sdk_runtime_provenance"
    OPENHANDS_SDK_ARTIFACT_REF = "openhands_sdk_artifact_ref"


class _EventBase(BaseModel):
    """Common envelope fields. All events carry these."""

    model_config = ConfigDict(frozen=True, extra="forbid")
    emitted_at: datetime
    trial_id: UUID
    step_id: str
    seq: int = Field(ge=0)


class TrialStartEvent(_EventBase):
    kind: Literal[EventKind.TRIAL_START] = EventKind.TRIAL_START
    task_id: str
    agent_name: str
    agent_mode: Literal["out-of-box", "in-box"]


# Trial-level ──────────────────────────────────────────────────────────────────


class TrialEndEvent(_EventBase):
    kind: Literal[EventKind.TRIAL_END] = EventKind.TRIAL_END
    final_state: Literal["succeeded", "failed", "cancelled"]
    reward: dict[str, float] | None = None
    failure_reason: str | None = None


class TrialErrorEvent(_EventBase):
    kind: Literal[EventKind.TRIAL_ERROR] = EventKind.TRIAL_ERROR
    error_type: str
    message: str
    traceback: str


class TrialCancelledEvent(_EventBase):
    kind: Literal[EventKind.TRIAL_CANCELLED] = EventKind.TRIAL_CANCELLED
    cancellation_requested_at: datetime
    observed_at: datetime


# Step-level ───────────────────────────────────────────────────────────────────


class StepStartEvent(_EventBase):
    kind: Literal[EventKind.STEP_START] = EventKind.STEP_START
    instruction_excerpt: str


class StepEndEvent(_EventBase):
    kind: Literal[EventKind.STEP_END] = EventKind.STEP_END
    summary: dict[str, float] | None = None
    error_phase: Literal["prepare", "agent", "artifacts", "verifier"] | None = None


# Environment-level ────────────────────────────────────────────────────────────


class EnvStartEvent(_EventBase):
    kind: Literal[EventKind.ENV_START] = EventKind.ENV_START
    image_ref: str
    build_time_sec: float


class EnvReadyEvent(_EventBase):
    kind: Literal[EventKind.ENV_READY] = EventKind.ENV_READY
    healthcheck_attempts: int


class EnvStopEvent(_EventBase):
    kind: Literal[EventKind.ENV_STOP] = EventKind.ENV_STOP
    duration_sec: float
    exit_status: int | None = None


class EnvExecEvent(_EventBase):
    kind: Literal[EventKind.ENV_EXEC] = EventKind.ENV_EXEC
    cmd: str
    user: str | int | None
    cwd: str | None
    return_code: int
    stdout_bytes: int
    stderr_bytes: int
    truncated: bool
    duration_sec: float


# File ops ─────────────────────────────────────────────────────────────────────


class FileUploadEvent(_EventBase):
    kind: Literal[EventKind.FILE_UPLOAD] = EventKind.FILE_UPLOAD
    src_size_bytes: int
    dst_path: str
    duration_sec: float


class FileDownloadEvent(_EventBase):
    kind: Literal[EventKind.FILE_DOWNLOAD] = EventKind.FILE_DOWNLOAD
    src_path: str
    dst_size_bytes: int
    duration_sec: float


# Agent — LLM call (training-data load-bearing per spec §4.4.1) ────────────────

from typing import Any  # noqa: E402  (intentionally local to keep top imports tight)

from loom.models.types import ModelSpec  # noqa: E402


class ChatMessage(BaseModel):
    """OpenAI-compatible chat message used in LLMCallEvent.messages / .response."""

    model_config = ConfigDict(frozen=True, extra="allow")  # allow provider-specific fields
    role: Literal["system", "user", "assistant", "tool"]
    content: str | list[dict[str, Any]] | None = None
    name: str | None = None
    tool_calls: list[dict[str, Any]] | None = None
    tool_call_id: str | None = None


class ToolSpec(BaseModel):
    """Tool definition sent in LLMCallEvent.tools."""

    model_config = ConfigDict(frozen=True, extra="allow")
    name: str
    description: str | None = None
    parameters: dict[str, Any] = {}


class LLMCallEvent(_EventBase):
    """The training-data load-bearing event (spec §4.4.1)."""

    kind: Literal[EventKind.LLM_CALL] = EventKind.LLM_CALL

    # Model identification (frozen at call time)
    model: ModelSpec
    rate_card_hash: str

    # Input
    system_prompt: str | None
    messages: list[ChatMessage]
    tools: list[ToolSpec] | None = None
    tool_choice: str | dict[str, Any] | None = None

    # Output
    response: ChatMessage
    finish_reason: str

    # Explicit audit state keeps missing usage distinct from numeric placeholders.
    # None preserves legacy records whose usage completeness was not classified.
    call_status: Literal["completed", "failed"] = "completed"
    usage_status: Literal["missing", "partial"] | None = None
    failure_category: Literal[
        "upstream_timeout", "upstream_transport", "upstream_http_4xx",
        "upstream_http_5xx", "upstream_http_error", "attempt_deadline_reached",
    ] | None = None

    # Usage — RAW, NOT derived (spec H5)
    input_tokens: int = Field(ge=0)
    cached_input_tokens: int = Field(ge=0)
    cache_write_tokens: int = Field(ge=0)
    output_tokens: int = Field(ge=0)
    thinking_tokens: int = Field(ge=0)
    provider_extras: dict[str, int]  # NAMED counters (int-valued)
    request_params: dict[str, Any] = Field(default_factory=legacy_request_params)

    # Derived (recomputable)
    cost_usd_snapshot: float = Field(ge=0)

    # Timing
    duration_sec: float = Field(ge=0)
    streamed: bool
    time_to_first_token_sec: float | None

    # Attribution
    gateway_request_id: str
    cache_keys: list[str] = []

    # #298 Slice B: gateway-internal retry attempt count. 1 = first
    # try succeeded (the historical default). > 1 = the gateway
    # retried N-1 transient upstream failures before this attempt
    # produced the response. Surfaces in ATIF for retry-rate
    # analysis without parsing logs.
    attempt: int = Field(default=1, ge=1)
    requested_model: str | None = None
    response_model: str | None = None
    role: Literal["student", "teacher"] | None = None


# Agent (continued) + verifier + network + sys ────────────────────────────────

from typing import Annotated  # noqa: E402

from loom.models.verifier import CheckResult, VerifierResult  # noqa: E402


class ToolUseEvent(_EventBase):
    kind: Literal[EventKind.TOOL_USE] = EventKind.TOOL_USE
    tool_name: str
    args: dict[str, Any]
    result: dict[str, Any] | None = None
    error: str | None = None
    duration_sec: float = Field(ge=0)
    reasoning_content: str | None = None
    thought: str | None = None
    summary: str | None = None


class AgentThoughtEvent(_EventBase):
    kind: Literal[EventKind.AGENT_THOUGHT] = EventKind.AGENT_THOUGHT
    content: str
    tokens: int | None = None
    sdk_event_type: str | None = None
    reasoning_content: str | None = None
    thought: str | None = None
    summary: str | None = None
    tool_call_id: str | None = None


class AgentRetryEvent(_EventBase):
    kind: Literal[EventKind.AGENT_RETRY] = EventKind.AGENT_RETRY
    attempt: int = Field(ge=1)
    max_attempts: int = Field(ge=1)
    failure_reason: str
    failure_message: str | None = None
    retry_after_sec: float = Field(ge=0)


class AgentTimeoutEvent(_EventBase):
    """One supervisor-owned diagnostic emitted when an attempt deadline wins."""

    kind: Literal[EventKind.AGENT_TIMEOUT] = EventKind.AGENT_TIMEOUT
    configured_timeout_sec: float = Field(gt=0)
    elapsed_monotonic_sec: float = Field(ge=0)
    cancellation_drain_sec: float = Field(ge=0)
    transport_close_required: bool
    task_stopped: bool
    agent_attempt_id: UUID | None = None


class AgentAttemptStartEvent(_EventBase):
    """Supervisor-owned retry identity; not a scheduled Trial attempt counter."""

    kind: Literal[EventKind.AGENT_ATTEMPT_START] = EventKind.AGENT_ATTEMPT_START
    agent_attempt_id: UUID
    attempt_deadline_wall_clock: datetime


class AgentStepTokenGrantEvent(_EventBase):
    """Safe CP grant correlation, without the bearer or provider credentials."""

    kind: Literal[EventKind.AGENT_STEP_TOKEN_GRANT] = EventKind.AGENT_STEP_TOKEN_GRANT
    agent_attempt_id: UUID
    step_jwt_id: UUID


class VerifierStartEvent(_EventBase):
    kind: Literal[EventKind.VERIFIER_START] = EventKind.VERIFIER_START
    verifier_name: str
    env_mode: Literal["shared", "separate"]


class VerifierEndEvent(_EventBase):
    kind: Literal[EventKind.VERIFIER_END] = EventKind.VERIFIER_END
    result: VerifierResult


class VerifierCheckEvent(_EventBase):
    kind: Literal[EventKind.VERIFIER_CHECK] = EventKind.VERIFIER_CHECK
    check: CheckResult


class NetworkPolicyChangeEvent(_EventBase):
    kind: Literal[EventKind.NETWORK_POLICY_CHANGE] = EventKind.NETWORK_POLICY_CHANGE
    from_policy: dict[str, Any]
    to_policy: dict[str, Any]
    phase: Literal["agent", "verifier", "baseline_restore"]


class WorkerLostClaimEvent(_EventBase):
    kind: Literal[EventKind.WORKER_LOST_CLAIM] = EventKind.WORKER_LOST_CLAIM
    original_worker_id: UUID
    detected_at: datetime


class WorkerDrainInterruptedEvent(_EventBase):
    kind: Literal[EventKind.WORKER_DRAIN_INTERRUPTED] = EventKind.WORKER_DRAIN_INTERRUPTED
    drain_timeout_sec: float


# Terminus-2 native runtime (#744) ────────────────────────────────────────────


class Terminus2RuntimeProvenanceEvent(_EventBase):
    kind: Literal[EventKind.TERMINUS2_RUNTIME_PROVENANCE] = EventKind.TERMINUS2_RUNTIME_PROVENANCE
    loom_runtime_revision: str
    harbor_compat_sha: str
    harbor_version: str | None = None
    parser_name: Literal["json", "xml"]
    prompt_hash: str
    template_hashes: dict[str, str]
    terminal_image_digest: str | None = None
    benchmark_provenance: dict[str, str] | None = None
    effective_options: dict[str, Any] = Field(default_factory=dict)


class Terminus2UserPromptEvent(_EventBase):
    """Harbor ``source=user`` step — initial system/task prompt (and later user nudges)."""

    kind: Literal[EventKind.TERMINUS2_USER_PROMPT] = EventKind.TERMINUS2_USER_PROMPT
    prompt_id: str
    harbor_step_id: int = Field(ge=1)
    message: str
    is_initial: bool = False


class Terminus2TurnEvent(_EventBase):
    kind: Literal[EventKind.TERMINUS2_TURN] = EventKind.TERMINUS2_TURN
    turn_id: str
    turn_index: int = Field(ge=0)
    gateway_request_id: str
    parse_state: Literal["ok", "error", "retry"]
    completion_state: Literal["continue", "pending_confirm", "complete"]
    analysis: str = ""
    plan: str = ""
    raw_response_excerpt: str = ""
    reasoning_content: str = ""
    harbor_step_id: int | None = Field(default=None, ge=1)


class Terminus2CommandEvent(_EventBase):
    kind: Literal[EventKind.TERMINUS2_COMMAND] = EventKind.TERMINUS2_COMMAND
    turn_id: str
    command_batch_id: str
    command_id: str
    index: int = Field(ge=0)
    keystrokes: str
    duration_sec: float = Field(ge=0)


class Terminus2TerminalObservationEvent(_EventBase):
    kind: Literal[EventKind.TERMINUS2_TERMINAL_OBSERVATION] = (
        EventKind.TERMINUS2_TERMINAL_OBSERVATION
    )
    turn_id: str
    command_batch_id: str
    observation_id: str
    text: str
    capture_source: Literal["incremental", "timeout", "initial", "error_feedback"]
    byte_len: int = Field(ge=0)
    truncated: bool
    completeness: Literal["full", "partial"]
    content_hash: str
    redaction_applied: bool
    is_aggregate: bool


class Terminus2ParseRetryEvent(_EventBase):
    kind: Literal[EventKind.TERMINUS2_PARSE_RETRY] = EventKind.TERMINUS2_PARSE_RETRY
    turn_id: str
    attempt: int = Field(ge=1)
    error_excerpt: str


class Terminus2ContextBoundaryEvent(_EventBase):
    kind: Literal[EventKind.TERMINUS2_CONTEXT_BOUNDARY] = EventKind.TERMINUS2_CONTEXT_BOUNDARY
    turn_id: str
    reason: str
    tokens_before: int = Field(ge=0)


class Terminus2ArtifactRefEvent(_EventBase):
    kind: Literal[EventKind.TERMINUS2_ARTIFACT_REF] = EventKind.TERMINUS2_ARTIFACT_REF
    artifact_kind: Literal["recording.cast", "terminus_2.pane"]
    sandbox_path: str
    content_hash: str
    size_bytes: int = Field(ge=0)
    share_policy: Literal["restricted", "shared"]


class Terminus2ModelSwitchEvent(_EventBase):
    """Emitted on each student/teacher role cut (#1380)."""

    kind: Literal[EventKind.TERMINUS2_MODEL_SWITCH] = EventKind.TERMINUS2_MODEL_SWITCH
    switch_episode: int = Field(ge=1)
    from_role: Literal["student", "teacher"] = "student"
    to_role: Literal["student", "teacher"] = "teacher"
    from_model: ModelSpec
    to_model: ModelSpec


class Terminus2ModelSwitchPlannedEvent(_EventBase):
    kind: Literal[EventKind.TERMINUS2_MODEL_SWITCH_PLANNED] = (
        EventKind.TERMINUS2_MODEL_SWITCH_PLANNED
    )
    switch_episode: int = Field(ge=2)
    from_role: Literal["student", "teacher"]
    to_role: Literal["student", "teacher"]
    from_model: ModelSpec
    to_model: ModelSpec


class Terminus2ModelMixPlannedEvent(_EventBase):
    """Durable mix plan (episode beta or turn-grain rising schedule)."""

    kind: Literal[EventKind.TERMINUS2_MODEL_MIX_PLANNED] = (
        EventKind.TERMINUS2_MODEL_MIX_PLANNED
    )
    policy: Literal["beta_mixture", "student_to_teacher_turns"] = "beta_mixture"
    beta: float | None = Field(default=None, ge=0.0, le=1.0)
    grain: Literal["episode", "turn"] = "episode"
    step_start: int | None = Field(default=None, ge=2)
    step_end: int | None = Field(default=None, ge=3)
    seed_fingerprint: str
    student_model: ModelSpec
    teacher_model: ModelSpec


class Terminus2LlmCallStartedEvent(_EventBase):
    kind: Literal[EventKind.TERMINUS2_LLM_CALL_STARTED] = EventKind.TERMINUS2_LLM_CALL_STARTED
    client_call_id: UUID
    episode: int = Field(ge=0)
    call_ordinal: int = Field(ge=1)
    role: Literal["student", "teacher"]
    requested_model: str


class Terminus2LlmCallCompletedEvent(_EventBase):
    kind: Literal[EventKind.TERMINUS2_LLM_CALL_COMPLETED] = EventKind.TERMINUS2_LLM_CALL_COMPLETED
    client_call_id: UUID
    episode: int = Field(ge=0)
    call_ordinal: int = Field(ge=1)
    role: Literal["student", "teacher"]
    requested_model: str
    response_model: str | None = None


class Terminus2LlmCallFailedEvent(_EventBase):
    kind: Literal[EventKind.TERMINUS2_LLM_CALL_FAILED] = EventKind.TERMINUS2_LLM_CALL_FAILED
    client_call_id: UUID
    episode: int = Field(ge=0)
    call_ordinal: int = Field(ge=1)
    role: Literal["student", "teacher"]
    requested_model: str
    error: str


class Terminus2EpisodeCheckpointEvent(_EventBase):
    kind: Literal[EventKind.TERMINUS2_EPISODE_CHECKPOINT] = EventKind.TERMINUS2_EPISODE_CHECKPOINT
    episode: int = Field(ge=1)
    active_role: Literal["student", "teacher"]
    checksum: str
    last_call_ordinal: int = Field(ge=0)


class Terminus2RecoveryFailedEvent(_EventBase):
    kind: Literal[EventKind.TERMINUS2_RECOVERY_FAILED] = EventKind.TERMINUS2_RECOVERY_FAILED
    reason: str
    last_episode: int | None = None


# OpenHands SDK native runtime (#1590) ──────────────────────────────────────────


class OpenHandsSdkRuntimeProvenanceEvent(_EventBase):
    kind: Literal[EventKind.OPENHANDS_SDK_RUNTIME_PROVENANCE] = (
        EventKind.OPENHANDS_SDK_RUNTIME_PROVENANCE
    )
    sdk_version: str
    openhands_tools_version: str
    loom_bridge_revision: str
    terminus_style: bool = False


class OpenHandsSdkArtifactRefEvent(_EventBase):
    kind: Literal[EventKind.OPENHANDS_SDK_ARTIFACT_REF] = EventKind.OPENHANDS_SDK_ARTIFACT_REF
    artifact_kind: Literal["openhands_sdk.events"]
    sandbox_path: str
    content_hash: str
    size_bytes: int = Field(ge=0)
    share_policy: Literal["restricted", "shared"]


TrajectoryEvent = Annotated[
    TrialStartEvent
    | TrialEndEvent
    | TrialErrorEvent
    | TrialCancelledEvent
    | StepStartEvent
    | StepEndEvent
    | EnvStartEvent
    | EnvReadyEvent
    | EnvStopEvent
    | EnvExecEvent
    | FileUploadEvent
    | FileDownloadEvent
    | LLMCallEvent
    | ToolUseEvent
    | AgentThoughtEvent
    | AgentRetryEvent
    | AgentTimeoutEvent
    | AgentAttemptStartEvent
    | AgentStepTokenGrantEvent
    | VerifierStartEvent
    | VerifierEndEvent
    | VerifierCheckEvent
    | NetworkPolicyChangeEvent
    | WorkerLostClaimEvent
    | WorkerDrainInterruptedEvent
    | Terminus2RuntimeProvenanceEvent
    | Terminus2UserPromptEvent
    | Terminus2TurnEvent
    | Terminus2CommandEvent
    | Terminus2TerminalObservationEvent
    | Terminus2ParseRetryEvent
    | Terminus2ContextBoundaryEvent
    | Terminus2ArtifactRefEvent
    | Terminus2ModelSwitchEvent
    | Terminus2ModelSwitchPlannedEvent
    | Terminus2ModelMixPlannedEvent
    | Terminus2LlmCallStartedEvent
    | Terminus2LlmCallCompletedEvent
    | Terminus2LlmCallFailedEvent
    | Terminus2EpisodeCheckpointEvent
    | Terminus2RecoveryFailedEvent
    | OpenHandsSdkRuntimeProvenanceEvent
    | OpenHandsSdkArtifactRefEvent,
    Field(discriminator="kind"),
]

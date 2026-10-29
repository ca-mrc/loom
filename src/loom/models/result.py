"""TrialResult and friends — what gets persisted (spec §4.5)."""

from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import Any, Literal
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field, field_validator

from loom.models.trial import TrialConfig
from loom.models.types import ModelSpec
from loom.models.verifier import VerifierResult
from loom.security.redaction import redact_text


class TrialState(StrEnum):
    QUEUED = "queued"
    CLAIMED = "claimed"
    RUNNING = "running"
    MATERIALIZING = "materializing"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"


class FailureReason(StrEnum):
    AGENT_ERROR = "agent_error"
    AGENT_TIMEOUT = "agent_timeout"
    ENV_START_FAILURE = "env_start_failure"
    ENV_HEALTHCHECK_FAILED = "env_healthcheck_failed"
    VERIFIER_ERROR = "verifier_error"
    VERIFIER_TIMEOUT = "verifier_timeout"
    ARTIFACT_UPLOAD_FAILED = "artifact_upload_failed"
    MISSING_REQUIRED_ARTIFACTS = "missing_required_artifacts"
    TRAJECTORY_FLUSH_FAILED = "trajectory_flush_failed"
    OUTPUT_UNAVAILABLE = "output_unavailable"
    TASK_IMAGE_BUILD_TIMEOUT = "task_image_build_timeout"
    TASK_IMAGE_ADMISSION_UNAVAILABLE = "task_image_admission_unavailable"
    NODE_SETUP_HEALTH = "node_setup_health"
    TASK_COMPATIBILITY = "task_compatibility"
    EXHAUSTED_RETRIES = "exhausted_retries"
    WORKER_LOST_CLAIM = "worker_lost_claim"
    INTERNAL_ERROR = "internal_error"
    PROVIDER_ERROR = "provider_error"   # upstream provider returned 4xx
    GATEWAY_ERROR = "gateway_error"     # loom gateway itself returned 5xx
    PROVIDER_TRANSPORT_DISCONNECT = "provider_transport_disconnect"
    STEP_CREDENTIAL_INVALID_OR_EXPIRED = "step_credential_invalid_or_expired"
    STEP_CREDENTIAL_SCOPE_INVALID = "step_credential_scope_invalid"


class AgentInfo(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    name: str
    version: str
    mode: Literal["out-of-box", "in-box"]
    model: ModelSpec | None = None


class ExceptionInfo(BaseModel):
    """Sanitized exception identity, independent of Loom's retry taxonomy."""

    model_config = ConfigDict(frozen=True, extra="forbid")
    exception_type: str = Field(min_length=1, max_length=200, pattern=r"^[A-Za-z_][A-Za-z0-9_.]*$")
    exception_message: str = Field(max_length=2000)
    occurred_at: datetime

    @field_validator("exception_message")
    @classmethod
    def redact_message(cls, value: str) -> str:
        return redact_text(value, limit=2000)


class StepError(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    phase: Literal["prepare", "agent", "artifacts", "verifier"]
    reason: Literal["timeout", "exception", "missing_artifacts", "cancelled"]
    message: str
    exception_type: str | None = None
    traceback: str | None = None
    occurred_at: datetime


class ArtifactRef(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    step_name: str
    bucket: str
    key: str
    size: int = Field(ge=0)
    content_hash: str | None = None
    version_id: str | None = None
    share_status: Literal["pending_scan", "shared", "blocked"] = "pending_scan"
    blocked_reason: str | None = None


class StepResult(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    step_name: str
    started_at: datetime | None = None
    finished_at: datetime | None = None
    verifier_result: VerifierResult | None = None
    error: StepError | None = None
    artifacts_uri: str | None = None
    artifacts: list[ArtifactRef] = []


class TrialResult(BaseModel):
    model_config = ConfigDict(extra="forbid")  # NOT frozen — built incrementally by Trial.run()
    schema_version: Literal["1"] = "1"
    id: UUID = Field(default_factory=uuid4)
    task_id: str
    task_checksum: str
    team_id: UUID
    agent: AgentInfo
    config: TrialConfig
    tool_network_profile: dict[str, Any] | None = Field(default=None, exclude_if=lambda value: value is None)
    state: TrialState
    failure_reason: FailureReason | None = None
    failure_message: str | None = None
    started_at: datetime | None = None
    finished_at: datetime | None = None
    reward: dict[str, float] | None = None
    steps: list[StepResult] = []
    trajectory_uri: str | None = None
    trajectory_sha256: str | None = None
    trajectory_size_bytes: int | None = None
    trajectory_version_id: str | None = None
    atif_uri: str | None = None
    atif_sha256: str | None = None
    atif_size_bytes: int | None = None
    atif_version_id: str | None = None
    atif_schema_version: str | None = None
    artifacts_prefix: str | None = None

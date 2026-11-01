"""Streaming, immutable Pipeline Artifact commit protocol.

This module is deliberately independent from HTTP and SQLAlchemy.  The control
plane adapters supply claim/service authorization and a durable repository;
the protocol owns closed plans, multipart idempotency, cryptographic readback,
canonical manifests, marker ordering, and crash-convergent state transitions.
"""

from __future__ import annotations

import hashlib
import hmac
import math
import secrets
import unicodedata
from collections import defaultdict
from collections.abc import AsyncIterator, Iterable, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Annotated, Any, Literal, Protocol, TypeAlias
from uuid import UUID

from prometheus_client import Counter, Histogram
from pydantic import Field, StringConstraints, field_validator, model_validator

from loom.pipeline.keys import (
    canonical_digest,
    canonical_document,
    canonical_identity,
    digest_bytes,
)
from loom.pipeline.spec import (
    ArtifactType,
    Digest,
    NonNegativeSafeInt,
    PipelineModel,
    PositiveSafeInt,
)
from loom.trajectory.storage import MultipartUpload, ObjectStore

MiB = 1024 * 1024
GiB = 1024 * MiB
MAX_APPLICATION_BUFFER_BYTES = 64 * MiB
MAX_MULTIPART_PARTS = 9_990
MAX_PART_BYTES = 5 * GiB
UPLOAD_TOKEN_TTL = timedelta(minutes=15)
SMALL_FILE_MAX_BYTES = 256 * 1024
SMALL_FILE_BATCH_MAX_BYTES = MiB
SMALL_FILE_BATCH_MAX_FILES = 16

ARTIFACT_PARTS = Counter(
    "loom_pipeline_artifact_parts_total",
    "Persisted Pipeline Artifact multipart parts",
    ("commit_kind",),
)
ARTIFACT_READBACK_FALLBACKS = Counter(
    "loom_pipeline_artifact_readback_fallback_total",
    "Full GET verification fallbacks",
    ("commit_kind",),
)
ARTIFACT_ABORTS = Counter(
    "loom_pipeline_artifact_aborts_total",
    "Aborted Pipeline Artifact sessions",
    ("commit_kind", "reason"),
)
ARTIFACT_RECONCILIATION_AGE = Histogram(
    "loom_pipeline_artifact_reconciliation_age_seconds",
    "Age of Artifact sessions selected for reconciliation",
    ("commit_kind", "state"),
)

CommitKind: TypeAlias = Literal[
    "final_output",
    "checkpoint",
    "service_execution_output",
    "input_import",
    "input_materialization",
    "acceptance_evidence",
    "profile_calibration_evidence",
]
UploadRole: TypeAlias = Literal["semantic_document", "payload", "payload_archive"]
ArchiveFormat: TypeAlias = Literal["none", "tar", "tar.zst", "zip"]
ArtifactName: TypeAlias = Annotated[str, StringConstraints(pattern=r"^[a-z][a-z0-9_-]{0,127}$")]


class ArtifactCommitError(RuntimeError):
    """Stable protocol failure suitable for mapping to a closed HTTP reason."""

    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


def multipart_part_size(expected_file_max_bytes: int) -> int:
    """Return the deterministic <=9990-part size required by the contract."""

    if isinstance(expected_file_max_bytes, bool) or expected_file_max_bytes < 1:
        raise ValueError("expected file maximum must be positive")
    required = math.ceil(expected_file_max_bytes / MAX_MULTIPART_PARTS)
    rounded = math.ceil(required / MiB) * MiB
    result = max(MAX_APPLICATION_BUFFER_BYTES, rounded)
    if result > MAX_PART_BYTES:
        raise ValueError("file maximum cannot be represented by S3 multipart")
    return result


def _nfc(value: str) -> str:
    normalized = unicodedata.normalize("NFC", value)
    normalized.encode("utf-8", errors="strict")
    if normalized != value:
        raise ValueError("text must already be NFC")
    return normalized


def confined_relative_path(value: str) -> str:
    value = _nfc(value)
    parts = value.split("/")
    if (
        not value
        or value.startswith("/")
        or "\\" in value
        or "\x00" in value
        or any(part in {"", ".", ".."} for part in parts)
    ):
        raise ValueError("artifact path must be a confined relative POSIX path")
    return value


class FinalOutputProducerV1(PipelineModel):
    commit_kind: Literal["final_output"]
    team_id: UUID
    pipeline_run_id: UUID
    pipeline_stage_run_id: UUID
    execution_attempt_id: UUID
    attempt_number: PositiveSafeInt
    stage_result_json: dict[str, Any]
    stage_result_digest: Digest
    inventory_digest: Digest
    input_lineage_artifact_ids: list[UUID] = Field(default_factory=list)
    input_lineage_digests: list[Digest] = Field(default_factory=list)


class CheckpointProducerV1(PipelineModel):
    commit_kind: Literal["checkpoint"]
    team_id: UUID
    pipeline_run_id: UUID
    pipeline_stage_run_id: UUID
    execution_attempt_id: UUID
    attempt_number: PositiveSafeInt
    checkpoint_sequence: NonNegativeSafeInt
    input_lineage_artifact_ids: list[UUID] = Field(default_factory=list)
    input_lineage_digests: list[Digest] = Field(default_factory=list)


class ServiceExecutionOutputProducerV1(PipelineModel):
    """Immutable producer identity for one Pod-native execution generation."""

    commit_kind: Literal["service_execution_output"]
    team_id: UUID
    service_execution_lease_id: UUID
    service_execution_generation: PositiveSafeInt
    service_execution_role: Literal["attempt", "verifier"]
    runtime_contract_sha256: Digest
    candidate_sha: Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{40}$")]
    task_revision_sha256: Digest
    command_identity_sha256: Digest
    input_lineage_artifact_ids: list[UUID] = Field(default_factory=list)
    input_lineage_digests: list[Digest] = Field(default_factory=list)


class InputImportProducerV1(PipelineModel):
    commit_kind: Literal["input_import"]
    team_id: UUID
    pipeline_input_import_id: UUID
    actor_user_id: UUID
    input_lineage_artifact_ids: list[UUID] = Field(default_factory=list)
    input_lineage_digests: list[Digest] = Field(default_factory=list)


class InputMaterializationProducerV1(PipelineModel):
    commit_kind: Literal["input_materialization"]
    team_id: UUID
    pipeline_input_materialization_id: UUID
    actor_user_id: UUID
    input_lineage_artifact_ids: list[UUID] = Field(default_factory=list)
    input_lineage_digests: list[Digest] = Field(default_factory=list)


class AcceptanceEvidenceProducerV1(PipelineModel):
    commit_kind: Literal["acceptance_evidence"]
    team_id: UUID
    pipeline_acceptance_authorization_id: UUID
    acceptance_action: Literal["matrix", "soak"]
    acceptance_candidate_sha256: Digest
    acceptance_result_kind: Literal["success", "terminal"]
    acceptance_termination_reason: str | None
    actor_user_id: UUID
    input_lineage_artifact_ids: list[UUID] = Field(default_factory=list)
    input_lineage_digests: list[Digest] = Field(default_factory=list)

    @model_validator(mode="after")
    def terminal_reason_group(self) -> AcceptanceEvidenceProducerV1:
        if (self.acceptance_result_kind == "terminal") != (
            self.acceptance_termination_reason is not None
        ):
            raise ValueError("acceptance terminal reason group is invalid")
        return self


class ProfileCalibrationEvidenceProducerV1(PipelineModel):
    commit_kind: Literal["profile_calibration_evidence"]
    team_id: UUID
    pipeline_profile_calibration_authorization_id: UUID
    profile_calibration_spec_sha256: Digest
    profile_calibration_result_kind: Literal["certification", "catalog", "terminal"]
    profile_calibration_scenario_id: (
        Literal["S01", "S02", "S03", "S04", "S06", "S07", "S08", "S09", "S11"] | None
    )
    profile_calibration_candidate_identity_sha256: Digest | None
    profile_calibration_run_ordinal: Annotated[int, Field(strict=True, ge=1, le=3)] | None
    profile_calibration_source_pipeline_run_id: UUID | None
    profile_calibration_termination_reason: str | None
    actor_user_id: UUID
    input_lineage_artifact_ids: list[UUID] = Field(default_factory=list)
    input_lineage_digests: list[Digest] = Field(default_factory=list)

    @model_validator(mode="after")
    def conditional_identity(self) -> ProfileCalibrationEvidenceProducerV1:
        certification = self.profile_calibration_result_kind == "certification"
        cert_values = (
            self.profile_calibration_scenario_id,
            self.profile_calibration_candidate_identity_sha256,
            self.profile_calibration_run_ordinal,
            self.profile_calibration_source_pipeline_run_id,
        )
        if certification != all(value is not None for value in cert_values):
            raise ValueError("profile certification identity group is invalid")
        terminal = self.profile_calibration_result_kind == "terminal"
        if terminal != (self.profile_calibration_termination_reason is not None):
            raise ValueError("profile terminal reason group is invalid")
        return self


CommitProducerV1: TypeAlias = (
    FinalOutputProducerV1
    | CheckpointProducerV1
    | ServiceExecutionOutputProducerV1
    | InputImportProducerV1
    | InputMaterializationProducerV1
    | AcceptanceEvidenceProducerV1
    | ProfileCalibrationEvidenceProducerV1
)


class UploadFilePlanV1(PipelineModel):
    file_index: NonNegativeSafeInt
    preallocated_artifact_id: UUID
    relative_path: str
    artifact_name: ArtifactName
    artifact_type: ArtifactType
    producer: Literal["container", "platform", "service"]
    media_type: Annotated[str, StringConstraints(min_length=1, max_length=255)]
    role: UploadRole
    archive_format: ArchiveFormat
    expected_max_bytes: PositiveSafeInt
    expected_sha256: Digest | None
    expected_size: NonNegativeSafeInt | None

    _path_is_confined = field_validator("relative_path")(confined_relative_path)

    @model_validator(mode="after")
    def role_archive_pair(self) -> UploadFilePlanV1:
        if self.role == "payload_archive":
            if self.archive_format == "none":
                raise ValueError("payload archives require an archive format")
        elif self.archive_format != "none":
            raise ValueError("ordinary files cannot declare an archive format")
        if (self.expected_sha256 is None) != (self.expected_size is None):
            raise ValueError("expected hash and size must be both null or both present")
        if self.expected_size is not None and self.expected_size > self.expected_max_bytes:
            raise ValueError("expected size exceeds maximum")
        return self


class UploadAuthV1(PipelineModel):
    upload_token: Annotated[str, StringConstraints(min_length=32, max_length=512)]


class ProducerAuthV1(PipelineModel):
    subject_kind: Literal["worker", "team_admin", "official_service"]
    subject_id: UUID


class WorkerClaimAuthV1(ProducerAuthV1):
    subject_kind: Literal["worker"]
    claim_id: UUID
    lease_epoch: PositiveSafeInt
    lease_token: Annotated[str, StringConstraints(min_length=32, max_length=512)]


class AcceptanceControllerServiceAuthV1(ProducerAuthV1):
    subject_kind: Literal["official_service"]
    service_name: Literal["loom-pipeline-acceptance-controller"]


class PartReceiptV1(PipelineModel):
    file_index: NonNegativeSafeInt
    part_number: PositiveSafeInt
    size_bytes: NonNegativeSafeInt
    sha256: Digest


class VerifiedFileV1(PipelineModel):
    file_index: NonNegativeSafeInt
    size_bytes: NonNegativeSafeInt
    sha256: Digest
    state: Literal["verified"] = "verified"


class UploadSessionGrantV1(PipelineModel):
    schema_version: Literal["loom.upload-session-grant.v1"] = "loom.upload-session-grant.v1"
    upload_session_id: UUID
    state: Literal["uploading"] = "uploading"
    upload_token: str
    token_expires_at: datetime
    files: list[UploadFilePlanV1]


class StoredFileV1(PipelineModel):
    file_index: NonNegativeSafeInt
    relative_path: str
    role: UploadRole
    archive_format: ArchiveFormat
    media_type: str
    size_bytes: NonNegativeSafeInt
    sha256: Digest


class ArtifactManifestV1(PipelineModel):
    schema_version: Literal["loom.artifact-manifest.v1"] = "loom.artifact-manifest.v1"
    artifact_id: UUID
    artifact_name: ArtifactName
    artifact_type: ArtifactType
    content_sha256: Digest
    stored_size_bytes: NonNegativeSafeInt
    unpacked_size_bytes: NonNegativeSafeInt
    file_count: PositiveSafeInt
    stored_files: list[StoredFileV1]
    lineage_artifact_ids: list[UUID]
    lineage_digests: list[Digest]

    @model_validator(mode="after")
    def one_semantic_document(self) -> ArtifactManifestV1:
        if sum(item.role == "semantic_document" for item in self.stored_files) != 1:
            raise ValueError("each Artifact requires exactly one semantic document")
        if [item.file_index for item in self.stored_files] != sorted(
            item.file_index for item in self.stored_files
        ):
            raise ValueError("stored files must be ordered by file index")
        return self


class RootArtifactRecordV1(PipelineModel):
    artifact_id: UUID
    artifact_name: ArtifactName
    artifact_type: ArtifactType
    manifest_sha256: Digest
    content_sha256: Digest
    stored_files: list[StoredFileV1]


class ArtifactCommitManifestV1(PipelineModel):
    schema_version: Literal["loom.artifact-commit-manifest.v1"] = "loom.artifact-commit-manifest.v1"
    session_id: UUID
    commit_kind: CommitKind
    producer_identity: dict[str, Any]
    artifacts: list[RootArtifactRecordV1]
    total_bytes: NonNegativeSafeInt
    input_lineage_artifact_ids: list[UUID]
    input_lineage_digests: list[Digest]
    request_digest: Digest


class ArtifactCommitMarkerV1(PipelineModel):
    commit_kind: CommitKind
    manifest_sha256: Digest
    schema_version: Literal["loom.artifact-commit-marker.v1"] = "loom.artifact-commit-marker.v1"
    session_id: UUID


class CommittedReadySessionV1(PipelineModel):
    upload_session_id: UUID
    state: Literal["committed_ready"]
    manifest_sha256: Digest
    committed_marker_sha256: Digest


class CommittedArtifactV1(PipelineModel):
    id: UUID
    name: ArtifactName
    artifact_type: ArtifactType
    content_sha256: Digest
    manifest_sha256: Digest
    stored_size_bytes: NonNegativeSafeInt
    file_count: PositiveSafeInt
    visibility: Literal["team"] = "team"
    share_status: Literal["pending_scan"] = "pending_scan"
    safety_state: Literal["unknown", "verified_internal"]


class CommittedArtifactsV1(PipelineModel):
    upload_session_id: UUID
    state: Literal["committed"]
    artifacts: list[CommittedArtifactV1]


class AuthoritativeArtifactDocumentV1(PipelineModel):
    producer: CommitProducerV1
    artifact_id: UUID
    artifact_name: ArtifactName
    artifact_type: ArtifactType
    relative_path: Literal[
        "artifact.json", "evidence.json", "certification.json", "catalog.json", "terminal.json"
    ]
    semantic_document: dict[str, Any] | list[Any]
    max_bytes: Annotated[int, Field(strict=True, ge=1, le=16_777_216)]
    declaration_digest: Digest

    @model_validator(mode="after")
    def declaration_is_exact(self) -> AuthoritativeArtifactDocumentV1:
        identity = self.model_dump(mode="python", exclude={"declaration_digest"})
        if canonical_digest(identity, persisted=False) != self.declaration_digest:
            raise ValueError("authoritative declaration digest drift")
        return self


class AcceptanceEvidenceAuthorityV1(Protocol):
    async def load_success_and_lock(
        self,
        *,
        authorization_id: UUID,
        action: Literal["matrix", "soak"],
        candidate_sha256: str,
        auth: AcceptanceControllerServiceAuthV1,
    ) -> AuthoritativeArtifactDocumentV1: ...

    async def load_terminal_and_lock(
        self,
        *,
        authorization_id: UUID,
        action: Literal["matrix", "soak"],
        candidate_sha256: str,
        reason: str,
        auth: AcceptanceControllerServiceAuthV1,
    ) -> AuthoritativeArtifactDocumentV1: ...

    async def complete_locked(self, *, artifact_id: UUID) -> None: ...


class ProfileCalibrationEvidenceAuthorityV1(Protocol):
    async def load_certification_and_lock(
        self, **identity: Any
    ) -> AuthoritativeArtifactDocumentV1: ...
    async def load_catalog_and_lock(self, **identity: Any) -> AuthoritativeArtifactDocumentV1: ...
    async def load_terminal_and_lock(self, **identity: Any) -> AuthoritativeArtifactDocumentV1: ...
    async def complete_locked(self, *, artifact_id: UUID) -> None: ...


class MultipartFaultHookV1(Protocol):
    async def after_part_persisted(
        self,
        *,
        session_id: UUID,
        execution_attempt_id: UUID,
        part_number: int,
    ) -> bool: ...


class VerifiedInputImportV1(PipelineModel):
    artifact_document: dict[str, Any]
    archive_sha256: Digest
    archive_size_bytes: NonNegativeSafeInt
    unpacked_size_bytes: NonNegativeSafeInt
    file_count: NonNegativeSafeInt
    lineage_artifact_ids: list[UUID]
    lineage_digests: list[Digest]


class InputImportVerifier(Protocol):
    async def verify(
        self,
        *,
        locked_input_import: Any,
        payload: VerifiedFileV1,
        frozen_create_manifest_bytes: bytes,
    ) -> VerifiedInputImportV1: ...


@dataclass
class _FileState:
    plan: UploadFilePlanV1
    upload: MultipartUpload | None = None
    receipts: dict[int, PartReceiptV1] = field(default_factory=dict)
    verified: VerifiedFileV1 | None = None


@dataclass
class _SessionState:
    id: UUID
    producer: CommitProducerV1
    files: list[_FileState]
    idempotency_key: str
    request_digest: str
    prefix: str
    expected_total_max_bytes: int
    upload_token_digest: bytes
    token_expires_at: datetime
    state: str = "uploading"
    actual_total_bytes: int = 0
    manifest: ArtifactCommitManifestV1 | None = None
    manifest_sha256: str | None = None
    marker_sha256: str | None = None
    item_manifests: dict[UUID, ArtifactManifestV1] = field(default_factory=dict)
    committed: CommittedArtifactsV1 | None = None
    checkpoint_envelope_json: dict[str, Any] | None = None
    checkpoint_envelope_digest: str | None = None
    version: int = 0
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    updated_at: datetime = field(default_factory=lambda: datetime.now(UTC))


class ArtifactCommitRepositoryV1(Protocol):
    async def find_idempotent(
        self, producer_identity: bytes, idempotency_key: str
    ) -> _SessionState | None: ...
    async def add(self, session: _SessionState) -> None: ...
    async def get(self, session_id: UUID) -> _SessionState: ...
    async def save(self, session: _SessionState) -> None: ...
    async def save_file(self, session: _SessionState, *, file_index: int) -> None: ...
    async def save_files(self, session: _SessionState, *, file_indexes: Sequence[int]) -> None: ...
    async def active(self) -> Iterable[_SessionState]: ...


@dataclass
class InMemoryArtifactCommitRepository:
    sessions: dict[UUID, _SessionState] = field(default_factory=dict)
    identities: dict[tuple[bytes, str], UUID] = field(default_factory=dict)

    async def find_idempotent(
        self, producer_identity: bytes, idempotency_key: str
    ) -> _SessionState | None:
        session_id = self.identities.get((producer_identity, idempotency_key))
        return None if session_id is None else self.sessions[session_id]

    async def add(self, session: _SessionState) -> None:
        identity = canonical_identity(session.producer)
        key = (identity, session.idempotency_key)
        if key in self.identities:
            raise ArtifactCommitError("idempotency_conflict")
        self.sessions[session.id] = session
        self.identities[key] = session.id

    async def get(self, session_id: UUID) -> _SessionState:
        try:
            return self.sessions[session_id]
        except KeyError as exc:
            raise ArtifactCommitError("not_found") from exc

    async def save(self, session: _SessionState) -> None:
        self.sessions[session.id] = session

    async def save_file(self, session: _SessionState, *, file_index: int) -> None:
        await self.save_files(session, file_indexes=[file_index])

    async def save_files(self, session: _SessionState, *, file_indexes: Sequence[int]) -> None:
        current = await self.get(session.id)
        if current.state not in {"uploading", "uploaded"}:
            raise ArtifactCommitError("session_not_uploading")
        if not file_indexes or len(set(file_indexes)) != len(file_indexes) or any(
            index < 0 or index >= len(current.files) for index in file_indexes
        ):
            raise ArtifactCommitError("file_not_found")
        for file_index in file_indexes:
            current.files[file_index] = session.files[file_index]
        if all(item.verified is not None for item in current.files):
            current.state = "uploaded"
            current.actual_total_bytes = sum(item.verified.size_bytes for item in current.files if item.verified)
        current.updated_at = session.updated_at
        session.state, session.actual_total_bytes = current.state, current.actual_total_bytes

    async def active(self) -> Iterable[_SessionState]:
        return self.sessions.values()


def _producer_prefix(producer: CommitProducerV1) -> str:
    if isinstance(producer, FinalOutputProducerV1):
        return (
            f"pipelines/{producer.team_id}/{producer.pipeline_run_id}/"
            f"{producer.pipeline_stage_run_id}/{producer.attempt_number}/final/"
        )
    if isinstance(producer, CheckpointProducerV1):
        return (
            f"pipelines/{producer.team_id}/{producer.pipeline_run_id}/"
            f"{producer.pipeline_stage_run_id}/{producer.attempt_number}/checkpoints/"
            f"{producer.checkpoint_sequence:012d}/"
        )
    if isinstance(producer, ServiceExecutionOutputProducerV1):
        return (
            f"service-executions/{producer.team_id}/"
            f"{producer.service_execution_lease_id}/"
            f"{producer.service_execution_generation}/output/"
        )
    if isinstance(producer, InputImportProducerV1):
        return f"pipeline-input-imports/{producer.team_id}/{producer.pipeline_input_import_id}/"
    if isinstance(producer, InputMaterializationProducerV1):
        return (
            f"pipeline-input-materializations/{producer.team_id}/"
            f"{producer.pipeline_input_materialization_id}/"
        )
    if isinstance(producer, AcceptanceEvidenceProducerV1):
        return (
            f"pipeline-acceptance/{producer.team_id}/"
            f"{producer.pipeline_acceptance_authorization_id}/"
            f"{producer.acceptance_action}/evidence/"
        )
    suffix: str
    if producer.profile_calibration_result_kind == "certification":
        assert producer.profile_calibration_scenario_id is not None
        assert producer.profile_calibration_candidate_identity_sha256 is not None
        assert producer.profile_calibration_run_ordinal is not None
        suffix = (
            "certifications/"
            f"{producer.profile_calibration_scenario_id}/"
            f"{producer.profile_calibration_candidate_identity_sha256.removeprefix('sha256:')}/"
            f"{producer.profile_calibration_run_ordinal:02d}/"
        )
    else:
        suffix = f"{producer.profile_calibration_result_kind}/"
    return (
        f"pipeline-profile-calibration/{producer.team_id}/"
        f"{producer.pipeline_profile_calibration_authorization_id}/{suffix}"
    )


def _data_object_key(session: _SessionState, file_state: _FileState) -> str:
    return (
        f"{session.prefix}artifacts/{file_state.plan.preallocated_artifact_id}/"
        f"{file_state.plan.relative_path}"
    )


def _validate_plan(producer: CommitProducerV1, files: Sequence[UploadFilePlanV1]) -> None:
    kind = producer.commit_kind
    if not files or [item.file_index for item in files] != list(range(len(files))):
        raise ArtifactCommitError("invalid_file_plan")
    identities = [(item.artifact_name.encode(), item.relative_path.encode()) for item in files]
    if len(identities) != len(set(identities)):
        raise ArtifactCommitError("invalid_file_plan")
    groups: dict[UUID, list[UploadFilePlanV1]] = defaultdict(list)
    for item in files:
        groups[item.preallocated_artifact_id].append(item)
    if any(
        sum(item.role == "semantic_document" for item in group) != 1 for group in groups.values()
    ):
        raise ArtifactCommitError("invalid_file_plan")
    if kind == "input_import":
        if len(groups) != 1 or [
            (item.relative_path, item.role, item.archive_format) for item in files
        ] != [
            ("payload.tar.zst", "payload_archive", "tar.zst"),
            ("artifact.json", "semantic_document", "none"),
        ]:
            raise ArtifactCommitError("invalid_input_import_plan")
    expected_semantic = {
        "checkpoint": "checkpoint.json",
        "service_execution_output": "result.json",
        "input_materialization": "artifact.json",
        "acceptance_evidence": "evidence.json",
    }.get(kind)
    if expected_semantic is not None and any(
        item.role == "semantic_document" and item.relative_path != expected_semantic
        for item in files
    ):
        raise ArtifactCommitError("invalid_semantic_document_path")
    if kind == "service_execution_output":
        if (
            len(groups) != 1
            or any(item.artifact_name != "trial_bundle" for item in files)
            or any(item.artifact_type != "loom.trial-artifact-bundle.v1" for item in files)
            or any(item.producer != "service" for item in files)
        ):
            raise ArtifactCommitError("invalid_service_execution_output_plan")
    if kind == "profile_calibration_evidence":
        assert isinstance(producer, ProfileCalibrationEvidenceProducerV1)
        expected_path = f"{producer.profile_calibration_result_kind}.json"
        expected_type = {
            "certification": "behavior_recovery_profile_certification.v1",
            "catalog": "behavior_recovery_profile_catalog.v1",
            "terminal": "behavior_recovery_profile_terminal_evidence.v1",
        }[producer.profile_calibration_result_kind]
        if (
            len(files) != 1
            or files[0].relative_path != expected_path
            or files[0].artifact_name != producer.profile_calibration_result_kind
            or files[0].artifact_type != expected_type
            or files[0].producer != "service"
        ):
            raise ArtifactCommitError("invalid_semantic_document_path")
    if kind == "acceptance_evidence":
        assert isinstance(producer, AcceptanceEvidenceProducerV1)
        expected_type = (
            "pipeline_acceptance_evidence.v1"
            if producer.acceptance_result_kind == "success"
            else "pipeline_acceptance_terminal_evidence.v1"
        )
        if (
            len(files) != 1
            or files[0].artifact_name != "evidence"
            or files[0].artifact_type != expected_type
            or files[0].producer != "service"
        ):
            raise ArtifactCommitError("invalid_acceptance_evidence_plan")


async def _one_chunk(value: bytes) -> AsyncIterator[bytes]:
    yield value


class ArtifactCommitService:
    """Protocol authority for all six immutable Pipeline commit kinds."""

    def __init__(
        self,
        *,
        store: ObjectStore,
        bucket: str,
        repository: ArtifactCommitRepositoryV1 | None = None,
        acceptance_authority: AcceptanceEvidenceAuthorityV1 | None = None,
        profile_calibration_authority: ProfileCalibrationEvidenceAuthorityV1 | None = None,
        multipart_fault_hook: MultipartFaultHookV1 | None = None,
        now: Any = None,
    ) -> None:
        self._store = store
        self._bucket = bucket
        self._repository = repository or InMemoryArtifactCommitRepository()
        self._acceptance_authority = acceptance_authority
        self._profile_calibration_authority = profile_calibration_authority
        self._multipart_fault_hook = multipart_fault_hook
        self._now = now or (lambda: datetime.now(UTC))
        self.readback_fallback_count = 0

    async def replay_committed(
        self,
        *,
        producer: CommitProducerV1,
        idempotency_key: str,
        request_digest: str,
    ) -> CommittedArtifactsV1 | None:
        """Return an exact prior terminal result, rejecting changed replay."""

        existing = await self._repository.find_idempotent(
            canonical_identity(producer), idempotency_key
        )
        if existing is None:
            return None
        if not hmac.compare_digest(existing.request_digest, request_digest):
            raise ArtifactCommitError("idempotency_conflict")
        return existing.committed

    async def committed_session_evidence(
        self, session_id: UUID
    ) -> tuple[ArtifactCommitManifestV1, str, str]:
        """Return read-only marker evidence after a successful common commit."""

        session = await self._repository.get(session_id)
        if (
            session.state != "committed"
            or session.manifest is None
            or session.manifest_sha256 is None
            or session.marker_sha256 is None
        ):
            raise ArtifactCommitError("session_not_committed")
        return session.manifest, session.manifest_sha256, session.marker_sha256

    async def prepare_session(
        self,
        *,
        producer: CommitProducerV1,
        files: list[UploadFilePlanV1],
        idempotency_key: str,
        request_digest: str,
    ) -> UploadSessionGrantV1:
        _validate_plan(producer, files)
        if not idempotency_key or len(idempotency_key.encode()) > 255:
            raise ArtifactCommitError("invalid_idempotency_key")
        if not request_digest.startswith("sha256:") or len(request_digest) != 71:
            raise ArtifactCommitError("invalid_request_digest")
        identity = canonical_identity(producer)
        total_max = sum(item.expected_max_bytes for item in files)
        if isinstance(producer, InputMaterializationProducerV1) and total_max > 268_435_456:
            raise ArtifactCommitError("input_materialization_session_limit")
        existing = await self._repository.find_idempotent(identity, idempotency_key)
        if existing is not None:
            if not hmac.compare_digest(existing.request_digest, request_digest):
                raise ArtifactCommitError("idempotency_conflict")
            if existing.state != "uploading":
                raise ArtifactCommitError("session_not_uploading")
            # The authenticated prepare replay is also a rotation boundary. It
            # makes a lost first response recoverable while ensuring no raw
            # upload token is persisted or remains valid after replay.
            replay_token = secrets.token_urlsafe(48)
            existing.upload_token_digest = hashlib.sha256(replay_token.encode()).digest()
            existing.token_expires_at = self._now() + UPLOAD_TOKEN_TTL
            existing.updated_at = self._now()
            await self._repository.save(existing)
            return self._grant(existing, replay_token)
        token = secrets.token_urlsafe(48)
        session_id = UUID(bytes=secrets.token_bytes(16), version=4)
        session = _SessionState(
            id=session_id,
            producer=producer,
            files=[_FileState(plan=item) for item in files],
            idempotency_key=idempotency_key,
            request_digest=request_digest,
            prefix=_producer_prefix(producer),
            expected_total_max_bytes=total_max,
            upload_token_digest=hashlib.sha256(token.encode()).digest(),
            token_expires_at=self._now() + UPLOAD_TOKEN_TTL,
        )
        await self._repository.add(session)
        return self._grant(session, token)

    @staticmethod
    def _grant(session: _SessionState, token: str) -> UploadSessionGrantV1:
        return UploadSessionGrantV1(
            upload_session_id=session.id,
            upload_token=token,
            token_expires_at=session.token_expires_at,
            files=[item.plan for item in session.files],
        )

    async def renew_upload_token(
        self, *, session_id: UUID, auth: ProducerAuthV1
    ) -> UploadSessionGrantV1:
        del auth  # The boundary adapter validates the producer principal.
        session = await self._repository.get(session_id)
        if session.state not in {"uploading", "uploaded"}:
            raise ArtifactCommitError("session_not_uploading")
        token = secrets.token_urlsafe(48)
        session.upload_token_digest = hashlib.sha256(token.encode()).digest()
        session.token_expires_at = self._now() + UPLOAD_TOKEN_TTL
        session.updated_at = self._now()
        await self._repository.save(session)
        return self._grant(session, token)

    async def _authorized(self, session_id: UUID, auth: UploadAuthV1) -> _SessionState:
        session = await self._repository.get(session_id)
        observed = hashlib.sha256(auth.upload_token.encode()).digest()
        if not hmac.compare_digest(observed, session.upload_token_digest):
            raise ArtifactCommitError("upload_token_invalid")
        if session.token_expires_at <= self._now():
            raise ArtifactCommitError("upload_token_expired")
        return session

    async def write_part(
        self,
        *,
        session_id: UUID,
        file_index: int,
        part_number: int,
        content_length: int,
        content_sha256: str,
        body: AsyncIterator[bytes],
        auth: UploadAuthV1,
    ) -> PartReceiptV1:
        session = await self._authorized(session_id, auth)
        return await self._write_part(
            session=session, file_index=file_index, part_number=part_number,
            content_length=content_length, content_sha256=content_sha256, body=body,
        )

    async def _write_part(
        self, *, session: _SessionState, file_index: int, part_number: int,
        content_length: int, content_sha256: str, body: AsyncIterator[bytes],
        persist_progress: bool = True,
    ) -> PartReceiptV1:
        if session.state != "uploading" or not 0 <= file_index < len(session.files):
            raise ArtifactCommitError("session_not_uploading")
        file_state = session.files[file_index]
        if part_number < 1 or part_number > MAX_MULTIPART_PARTS:
            raise ArtifactCommitError("invalid_part_number")
        previous = file_state.receipts.get(part_number)
        if previous is not None:
            if previous.size_bytes != content_length or not hmac.compare_digest(
                previous.sha256, content_sha256
            ):
                raise ArtifactCommitError("part_conflict")
            return previous
        if part_number != len(file_state.receipts) + 1:
            raise ArtifactCommitError("noncontiguous_part")
        part_size = multipart_part_size(file_state.plan.expected_max_bytes)
        if content_length > part_size or content_length < 0:
            raise ArtifactCommitError("part_size_invalid")
        digest = hashlib.sha256()
        observed = 0

        async def verified_body() -> AsyncIterator[bytes]:
            nonlocal observed
            async for chunk in body:
                if not chunk:
                    continue
                if len(chunk) > MAX_APPLICATION_BUFFER_BYTES:
                    raise ArtifactCommitError("stream_chunk_invalid")
                observed += len(chunk)
                if observed > content_length:
                    raise ArtifactCommitError("content_length_mismatch")
                digest.update(chunk)
                yield bytes(chunk)

        created_upload = file_state.upload is None
        if file_state.upload is None:
            file_state.upload = await self._store.create_multipart_upload(
                bucket=self._bucket,
                key=_data_object_key(session, file_state),
            )
        await self._store.upload_part_stream(
            file_state.upload,
            part_number=part_number,
            body=verified_body(),
        )
        computed = f"sha256:{digest.hexdigest()}"
        if observed != content_length or not hmac.compare_digest(computed, content_sha256):
            await self._store.abort_multipart_upload(file_state.upload)
            file_state.upload = None
            raise ArtifactCommitError("part_digest_mismatch")
        receipt = PartReceiptV1(
            file_index=file_index,
            part_number=part_number,
            size_bytes=observed,
            sha256=computed,
        )
        file_state.receipts[part_number] = receipt
        ARTIFACT_PARTS.labels(session.producer.commit_kind).inc()
        session.updated_at = self._now()
        if persist_progress:
            try:
                await self._repository.save_file(session, file_index=file_index)
            except ArtifactCommitError:
                # An explicitly rejected new upload was never attached. An
                # ambiguous database/network error cannot establish that fact.
                if created_upload:
                    await self._store.abort_multipart_upload(file_state.upload)
                raise
        if (
            persist_progress and part_number == 1
            and self._multipart_fault_hook is not None
            and isinstance(session.producer, FinalOutputProducerV1)
            and await self._multipart_fault_hook.after_part_persisted(
                session_id=session.id,
                execution_attempt_id=session.producer.execution_attempt_id,
                part_number=part_number,
            )
        ):
            await self._store.abort_multipart_upload(file_state.upload)
            file_state.upload = None
            session.state = "aborted"
            session.updated_at = self._now()
            await self._repository.save(session)
            raise ArtifactCommitError("acceptance_fault_fired")
        return receipt

    async def _readback_digest(
        self, *, key: str, commit_kind: CommitKind | None = None
    ) -> tuple[int, str]:
        facts = await self._store.stat_object(bucket=self._bucket, key=key)
        if facts.checksum_sha256 is not None:
            return facts.content_length, facts.checksum_sha256
        self.readback_fallback_count += 1
        if commit_kind is not None:
            ARTIFACT_READBACK_FALLBACKS.labels(commit_kind).inc()
        digest = hashlib.sha256()
        size = 0
        async for chunk in self._store.stream_object(
            bucket=self._bucket,
            key=key,
            chunk_size=MAX_APPLICATION_BUFFER_BYTES,
        ):
            if len(chunk) > MAX_APPLICATION_BUFFER_BYTES:
                raise ArtifactCommitError("object_store_chunk_too_large")
            size += len(chunk)
            digest.update(chunk)
        return size, f"sha256:{digest.hexdigest()}"

    async def complete_file(
        self,
        *,
        session_id: UUID,
        file_index: int,
        ordered_parts: list[PartReceiptV1],
        auth: UploadAuthV1,
    ) -> VerifiedFileV1:
        session = await self._authorized(session_id, auth)
        return await self._complete_file(session=session, file_index=file_index, ordered_parts=ordered_parts)

    async def _complete_file(
        self, *, session: _SessionState, file_index: int, ordered_parts: list[PartReceiptV1],
        persist_progress: bool = True,
    ) -> VerifiedFileV1:
        if session.state == "aborted":
            raise ArtifactCommitError("session_not_uploading")
        if not 0 <= file_index < len(session.files):
            raise ArtifactCommitError("file_not_found")
        file_state = session.files[file_index]
        if file_state.verified is not None:
            if ordered_parts != list(file_state.receipts.values()):
                raise ArtifactCommitError("file_completion_conflict")
            if persist_progress and session.state in {"uploading", "uploaded"}:
                session.updated_at = self._now()
                await self._repository.save_file(session, file_index=file_index)
            return file_state.verified
        if session.state != "uploading":
            raise ArtifactCommitError("session_not_uploading")
        expected = [file_state.receipts[index] for index in range(1, len(file_state.receipts) + 1)]
        if ordered_parts != expected or file_state.upload is None:
            raise ArtifactCommitError("part_receipt_drift")
        await self._store.complete_multipart_upload(file_state.upload)
        size, digest = await self._readback_digest(
            key=_data_object_key(session, file_state),
            commit_kind=session.producer.commit_kind,
        )
        expected_size = sum(item.size_bytes for item in expected)
        plan = file_state.plan
        if (
            size != expected_size
            or size > plan.expected_max_bytes
            or (plan.expected_size is not None and size != plan.expected_size)
            or (
                plan.expected_sha256 is not None
                and not hmac.compare_digest(digest, plan.expected_sha256)
            )
        ):
            raise ArtifactCommitError("object_readback_mismatch")
        file_state.verified = VerifiedFileV1(
            file_index=file_index,
            size_bytes=size,
            sha256=digest,
        )
        if all(item.verified is not None for item in session.files):
            session.state = "uploaded"
            session.actual_total_bytes = sum(
                item.verified.size_bytes for item in session.files if item.verified
            )
        session.updated_at = self._now()
        if persist_progress:
            await self._repository.save_file(session, file_index=file_index)
        return file_state.verified

    async def write_small_files(
        self, *, session_id: UUID, files: Sequence[tuple[int, bytes]], auth: UploadAuthV1,
    ) -> list[VerifiedFileV1]:
        """Persist bounded groups of receipts before completing any objects.

        Two metadata transactions replace two per file. Durable multipart IDs
        still fence aborts and recover lost completions; a failed group saves
        its completed prefix before propagating the failure. Large files retain
        their existing streaming protocol and per-part durability.
        """
        if (
            not 1 <= len(files) <= SMALL_FILE_BATCH_MAX_FILES
            or len({index for index, _ in files}) != len(files)
            or sum(len(body) for _, body in files) > SMALL_FILE_BATCH_MAX_BYTES
            or any(len(body) > SMALL_FILE_MAX_BYTES for _, body in files)
        ):
            raise ArtifactCommitError("small_file_batch_invalid")
        session = await self._authorized(session_id, auth)
        if not isinstance(session.producer, ServiceExecutionOutputProducerV1):
            raise ArtifactCommitError("commit_kind_mismatch")
        if session.state not in {"uploading", "uploaded"}:
            raise ArtifactCommitError("session_not_uploading")
        for index, body in files:
            if not 0 <= index < len(session.files):
                raise ArtifactCommitError("file_not_found")
            plan = session.files[index].plan
            if plan.producer != "service":
                raise ArtifactCommitError("small_file_batch_invalid")
            if (
                plan.expected_size != len(body)
                or plan.expected_sha256 is None
                or len(body) > plan.expected_max_bytes
            ):
                raise ArtifactCommitError("part_digest_mismatch")
        indexes = [index for index, _ in files]
        unattached: list[_FileState] = []

        async def persist() -> None:
            try:
                await self._repository.save_files(session, file_indexes=indexes)
            except ArtifactCommitError:
                # Only an explicit rejection proves new uploads were not
                # attached. Never abort them on an ambiguous DB response.
                for state in unattached:
                    if state.upload is not None:
                        await self._store.abort_multipart_upload(state.upload)
                raise
            unattached.clear()

        # Stage every part, then durably attach all upload IDs/receipts before
        # any CompleteMultipartUpload can publish an object. Abort/recovery use
        # precisely the same durable multipart identities as the legacy path.
        try:
            for index, payload in files:
                file_state = session.files[index]
                previous = file_state.receipts.get(1)
                known_digest = (
                    file_state.verified.sha256 if file_state.verified is not None
                    else previous.sha256 if previous is not None else None
                )
                if known_digest is not None and known_digest != digest_bytes(payload):
                    raise ArtifactCommitError("part_digest_mismatch")
                if file_state.verified is not None:
                    continue
                if file_state.upload is None:
                    unattached.append(file_state)

                async def stream_body(payload: bytes = payload) -> AsyncIterator[bytes]:
                    yield payload

                expected_digest = file_state.plan.expected_sha256
                assert expected_digest is not None
                await self._write_part(
                    session=session, file_index=index, part_number=1,
                    content_length=len(payload), content_sha256=expected_digest,
                    body=stream_body(), persist_progress=False,
                )
        except BaseException:
            await persist()
            raise
        await persist()

        result = []
        try:
            for index, _ in files:
                file_state = session.files[index]
                if file_state.verified is not None:
                    result.append(file_state.verified)
                    continue
                result.append(await self._complete_file(
                    session=session, file_index=index,
                    ordered_parts=[file_state.receipts[1]], persist_progress=False,
                ))
        except BaseException:
            await persist()
            raise
        await persist()
        return result

    async def read_verified_file(
        self,
        *,
        session_id: UUID,
        file_index: int,
        auth: UploadAuthV1,
        max_bytes: int,
    ) -> bytes:
        """Read one already verified session file for a server-owned derivation."""

        session = await self._authorized(session_id, auth)
        if max_bytes <= 0 or not 0 <= file_index < len(session.files):
            raise ArtifactCommitError("file_not_found")
        state = session.files[file_index]
        if state.verified is None or state.verified.size_bytes > max_bytes:
            raise ArtifactCommitError("file_not_verified")
        payload = bytearray()
        async for chunk in self._store.stream_object(
            bucket=self._bucket,
            key=_data_object_key(session, state),
            chunk_size=min(MAX_APPLICATION_BUFFER_BYTES, max_bytes),
        ):
            if not chunk or len(payload) + len(chunk) > max_bytes:
                raise ArtifactCommitError("object_store_chunk_too_large")
            payload.extend(chunk)
        value = bytes(payload)
        if len(value) != state.verified.size_bytes or not hmac.compare_digest(
            digest_bytes(value), state.verified.sha256
        ):
            raise ArtifactCommitError("object_readback_mismatch")
        return value

    async def commit_platform_document(
        self,
        *,
        session_id: UUID,
        file_index: int,
        value: Any,
        auth: UploadAuthV1,
    ) -> VerifiedFileV1:
        """Commit one canonical platform-owned document inside an open session."""

        session = await self._authorized(session_id, auth)
        if not 0 <= file_index < len(session.files):
            raise ArtifactCommitError("file_not_found")
        state = session.files[file_index]
        plan = state.plan
        if (
            plan.producer != "platform"
            or plan.role != "semantic_document"
            or plan.archive_format != "none"
        ):
            raise ArtifactCommitError("platform_file_plan_invalid")
        payload = canonical_document(value)
        payload_digest = digest_bytes(payload)
        if not payload or len(payload) > plan.expected_max_bytes:
            raise ArtifactCommitError("platform_document_size_invalid")
        if state.verified is not None:
            if state.verified.size_bytes != len(payload) or not hmac.compare_digest(
                state.verified.sha256, payload_digest
            ):
                raise ArtifactCommitError("platform_document_replay_drift")
            return state.verified
        receipt = await self.write_part(
            session_id=session_id,
            file_index=file_index,
            part_number=1,
            content_length=len(payload),
            content_sha256=payload_digest,
            body=_one_chunk(payload),
            auth=auth,
        )
        return await self.complete_file(
            session_id=session_id,
            file_index=file_index,
            ordered_parts=[receipt],
            auth=auth,
        )

    async def _put_canonical(self, *, key: str, value: Any, commit_kind: CommitKind) -> str:
        payload = canonical_document(value)
        expected = digest_bytes(payload)
        await self._store.put_object_stream(
            bucket=self._bucket,
            key=key,
            body=_one_chunk(payload),
        )
        size, observed = await self._readback_digest(key=key, commit_kind=commit_kind)
        if size != len(payload) or not hmac.compare_digest(observed, expected):
            raise ArtifactCommitError("canonical_object_readback_mismatch")
        return expected

    def _build_item_manifests(self, session: _SessionState) -> dict[UUID, ArtifactManifestV1]:
        groups: dict[UUID, list[_FileState]] = defaultdict(list)
        for item in session.files:
            groups[item.plan.preallocated_artifact_id].append(item)
        result: dict[UUID, ArtifactManifestV1] = {}
        for artifact_id, states in groups.items():
            first = states[0].plan
            if any(
                state.plan.artifact_name != first.artifact_name
                or state.plan.artifact_type != first.artifact_type
                for state in states
            ):
                raise ArtifactCommitError("artifact_plan_identity_drift")
            stored: list[StoredFileV1] = []
            for state in states:
                if state.verified is None:
                    raise ArtifactCommitError("file_not_verified")
                stored.append(
                    StoredFileV1(
                        file_index=state.plan.file_index,
                        relative_path=state.plan.relative_path,
                        role=state.plan.role,
                        archive_format=state.plan.archive_format,
                        media_type=state.plan.media_type,
                        size_bytes=state.verified.size_bytes,
                        sha256=state.verified.sha256,
                    )
                )
            semantic = next(item for item in stored if item.role == "semantic_document")
            result[artifact_id] = ArtifactManifestV1(
                artifact_id=artifact_id,
                artifact_name=first.artifact_name,
                artifact_type=first.artifact_type,
                content_sha256=semantic.sha256,
                stored_size_bytes=sum(item.size_bytes for item in stored),
                unpacked_size_bytes=sum(item.size_bytes for item in stored),
                file_count=max(1, len(stored)),
                stored_files=stored,
                lineage_artifact_ids=session.producer.input_lineage_artifact_ids,
                lineage_digests=session.producer.input_lineage_digests,
            )
        return result

    async def commit_session(
        self,
        *,
        session_id: UUID,
        auth: ProducerAuthV1 | UploadAuthV1,
    ) -> CommittedReadySessionV1 | CommittedArtifactsV1:
        session = (
            await self._authorized(session_id, auth)
            if isinstance(auth, UploadAuthV1)
            else await self._repository.get(session_id)
        )
        if session.committed is not None:
            return session.committed
        if session.state == "committed_ready":
            assert session.manifest_sha256 and session.marker_sha256
            return CommittedReadySessionV1(
                upload_session_id=session.id,
                state="committed_ready",
                manifest_sha256=session.manifest_sha256,
                committed_marker_sha256=session.marker_sha256,
            )
        if session.state != "uploaded":
            raise ArtifactCommitError("session_not_uploaded")
        session.state = "committing"
        item_manifests = self._build_item_manifests(session)
        root_records: list[RootArtifactRecordV1] = []
        for artifact_id in sorted(item_manifests, key=lambda item: item.bytes):
            item_manifest = item_manifests[artifact_id]
            manifest_digest = await self._put_canonical(
                key=f"{session.prefix}artifacts/{artifact_id}/_artifact_manifest.json",
                value=item_manifest,
                commit_kind=session.producer.commit_kind,
            )
            root_records.append(
                RootArtifactRecordV1(
                    artifact_id=artifact_id,
                    artifact_name=item_manifest.artifact_name,
                    artifact_type=item_manifest.artifact_type,
                    manifest_sha256=manifest_digest,
                    content_sha256=item_manifest.content_sha256,
                    stored_files=item_manifest.stored_files,
                )
            )
        root_records.sort(key=lambda item: item.artifact_name.encode())
        producer_identity = session.producer.model_dump(mode="json", exclude_none=False)
        manifest = ArtifactCommitManifestV1(
            session_id=session.id,
            commit_kind=session.producer.commit_kind,
            producer_identity=producer_identity,
            artifacts=root_records,
            total_bytes=session.actual_total_bytes,
            input_lineage_artifact_ids=session.producer.input_lineage_artifact_ids,
            input_lineage_digests=session.producer.input_lineage_digests,
            request_digest=session.request_digest,
        )
        manifest_digest = await self._put_canonical(
            key=session.prefix + "_manifest.json",
            value=manifest,
            commit_kind=session.producer.commit_kind,
        )
        marker = ArtifactCommitMarkerV1(
            commit_kind=session.producer.commit_kind,
            manifest_sha256=manifest_digest,
            session_id=session.id,
        )
        marker_digest = await self._put_canonical(
            key=session.prefix + "_COMMITTED",
            value=marker,
            commit_kind=session.producer.commit_kind,
        )
        session.manifest = manifest
        session.item_manifests = item_manifests
        session.manifest_sha256 = manifest_digest
        session.marker_sha256 = marker_digest
        session.updated_at = self._now()
        if session.producer.commit_kind == "final_output":
            session.state = "committed_ready"
            await self._repository.save(session)
            return CommittedReadySessionV1(
                upload_session_id=session.id,
                state="committed_ready",
                manifest_sha256=manifest_digest,
                committed_marker_sha256=marker_digest,
            )
        result = self._publish(session)
        await self._repository.save(session)
        return result

    def _publish(self, session: _SessionState) -> CommittedArtifactsV1:
        if session.committed is not None:
            return session.committed
        safety: Literal["unknown", "verified_internal"] = (
            "unknown" if session.producer.commit_kind == "input_import" else "verified_internal"
        )
        records = [
            CommittedArtifactV1(
                id=item.artifact_id,
                name=item.artifact_name,
                artifact_type=item.artifact_type,
                content_sha256=item.content_sha256,
                manifest_sha256=digest_bytes(canonical_document(item)),
                stored_size_bytes=item.stored_size_bytes,
                file_count=item.file_count,
                safety_state=safety,
            )
            for item in session.item_manifests.values()
        ]
        records.sort(key=lambda item: item.name.encode())
        result = CommittedArtifactsV1(
            upload_session_id=session.id,
            state="committed",
            artifacts=records,
        )
        session.committed = result
        session.state = "committed"
        session.updated_at = self._now()
        return result

    async def finalize_final_output(
        self,
        *,
        session_id: UUID,
        completion: Any,
        auth: WorkerClaimAuthV1,
    ) -> CommittedArtifactsV1:
        del auth  # Claim fencing and Attempt transaction live in the SQL adapter.
        session = await self._repository.get(session_id)
        if not isinstance(session.producer, FinalOutputProducerV1):
            raise ArtifactCommitError("commit_kind_mismatch")
        completion_session = getattr(completion, "final_output_upload_session_id", session_id)
        if completion_session != session_id:
            raise ArtifactCommitError("completion_session_drift")
        if session.state not in {"committed_ready", "committed"}:
            raise ArtifactCommitError("session_not_committed_ready")
        result = self._publish(session)
        await self._repository.save(session)
        return result

    async def abort_session(
        self,
        *,
        session_id: UUID,
        auth: ProducerAuthV1 | UploadAuthV1,
        reason: str,
    ) -> None:
        session = (
            await self._authorized(session_id, auth)
            if isinstance(auth, UploadAuthV1)
            else await self._repository.get(session_id)
        )
        if session.state == "aborted":
            return
        if session.state == "committed":
            raise ArtifactCommitError("committed_session_immutable")
        for file_state in session.files:
            if file_state.upload is not None and file_state.verified is None:
                await self._store.abort_multipart_upload(file_state.upload)
        # A committed-ready final output is not publication authority. Its
        # marker-valid prefix is deleted only by this explicit fenced abort.
        keys = [_data_object_key(session, state) for state in session.files]
        keys.extend(
            f"{session.prefix}artifacts/{artifact_id}/_artifact_manifest.json"
            for artifact_id in session.item_manifests
        )
        keys.extend((session.prefix + "_manifest.json", session.prefix + "_COMMITTED"))
        for key in keys:
            await self._store.delete_object(bucket=self._bucket, key=key)
        session.manifest = None
        session.item_manifests.clear()
        session.manifest_sha256 = None
        session.marker_sha256 = None
        session.committed = None
        session.state = "aborted"
        session.updated_at = self._now()
        metric_reason = (
            reason
            if reason in {"abandoned", "cancelled", "integrity_failure", "producer_abort"}
            else "producer_abort"
        )
        ARTIFACT_ABORTS.labels(session.producer.commit_kind, metric_reason).inc()
        await self._repository.save(session)

    async def abort_abandoned(self, *, older_than: timedelta = timedelta(hours=24)) -> int:
        cutoff = self._now() - older_than
        count = 0
        system = ProducerAuthV1(subject_kind="official_service", subject_id=UUID(int=0))
        for session in list(await self._repository.active()):
            ARTIFACT_RECONCILIATION_AGE.labels(session.producer.commit_kind, session.state).observe(
                max(0.0, (self._now() - session.updated_at).total_seconds())
            )
            if (
                session.state
                in {
                    "uploading",
                    "uploaded",
                    "committing",
                    "committed_ready",
                }
                and session.updated_at <= cutoff
            ):
                await self.abort_session(session_id=session.id, auth=system, reason="abandoned")
                count += 1
        return count

    async def commit_authoritative_document(
        self,
        *,
        declaration: AuthoritativeArtifactDocumentV1,
    ) -> CommittedArtifactsV1:
        """Write a service-generated canonical document through the same protocol."""

        payload = canonical_document(declaration.semantic_document)
        if not payload or len(payload) > declaration.max_bytes:
            raise ArtifactCommitError("authoritative_document_size_invalid")
        existing = await self._repository.find_idempotent(
            canonical_identity(declaration.producer), declaration.declaration_digest
        )
        if existing is not None and existing.committed is not None:
            return existing.committed
        plan = UploadFilePlanV1(
            file_index=0,
            preallocated_artifact_id=declaration.artifact_id,
            relative_path=declaration.relative_path,
            artifact_name=declaration.artifact_name,
            artifact_type=declaration.artifact_type,
            producer="service",
            media_type="application/json",
            role="semantic_document",
            archive_format="none",
            expected_max_bytes=declaration.max_bytes,
            expected_sha256=digest_bytes(payload),
            expected_size=len(payload),
        )
        grant = await self.prepare_session(
            producer=declaration.producer,
            files=[plan],
            idempotency_key=declaration.declaration_digest,
            request_digest=declaration.declaration_digest,
        )
        auth = UploadAuthV1(upload_token=grant.upload_token)
        receipt = await self.write_part(
            session_id=grant.upload_session_id,
            file_index=0,
            part_number=1,
            content_length=len(payload),
            content_sha256=digest_bytes(payload),
            body=_one_chunk(payload),
            auth=auth,
        )
        await self.complete_file(
            session_id=grant.upload_session_id,
            file_index=0,
            ordered_parts=[receipt],
            auth=auth,
        )
        committed = await self.commit_session(
            session_id=grant.upload_session_id,
            auth=ProducerAuthV1(subject_kind="official_service", subject_id=UUID(int=0)),
        )
        if not isinstance(committed, CommittedArtifactsV1):
            raise ArtifactCommitError("service_commit_cannot_be_committed_ready")
        return committed

    async def commit_authoritative_batch(
        self,
        *,
        declarations: list[AuthoritativeArtifactDocumentV1],
        idempotency_key: str,
    ) -> CommittedArtifactsV1:
        if not declarations or len(declarations) > 202:
            raise ArtifactCommitError("authoritative_batch_count_invalid")
        producer = declarations[0].producer
        if any(item.producer != producer for item in declarations):
            raise ArtifactCommitError("authoritative_batch_producer_drift")
        if not isinstance(producer, InputMaterializationProducerV1):
            raise ArtifactCommitError("authoritative_batch_kind_invalid")
        plans: list[UploadFilePlanV1] = []
        payloads: list[bytes] = []
        for index, declaration in enumerate(declarations):
            payload = canonical_document(declaration.semantic_document)
            if not payload or len(payload) > declaration.max_bytes:
                raise ArtifactCommitError("authoritative_document_size_invalid")
            plans.append(
                UploadFilePlanV1(
                    file_index=index,
                    preallocated_artifact_id=declaration.artifact_id,
                    relative_path=declaration.relative_path,
                    artifact_name=declaration.artifact_name,
                    artifact_type=declaration.artifact_type,
                    producer="service",
                    media_type="application/json",
                    role="semantic_document",
                    archive_format="none",
                    expected_max_bytes=declaration.max_bytes,
                    expected_sha256=digest_bytes(payload),
                    expected_size=len(payload),
                )
            )
            payloads.append(payload)
        request_digest = canonical_digest(
            [item.model_dump(mode="json") for item in declarations], persisted=False
        )
        existing = await self._repository.find_idempotent(
            canonical_identity(producer), idempotency_key
        )
        if existing is not None:
            if not hmac.compare_digest(existing.request_digest, request_digest):
                raise ArtifactCommitError("idempotency_conflict")
            if existing.committed is not None:
                return existing.committed
        grant = await self.prepare_session(
            producer=producer,
            files=plans,
            idempotency_key=idempotency_key,
            request_digest=request_digest,
        )
        auth = UploadAuthV1(upload_token=grant.upload_token)
        for plan, payload in zip(plans, payloads, strict=True):
            receipt = await self.write_part(
                session_id=grant.upload_session_id,
                file_index=plan.file_index,
                part_number=1,
                content_length=len(payload),
                content_sha256=digest_bytes(payload),
                body=_one_chunk(payload),
                auth=auth,
            )
            await self.complete_file(
                session_id=grant.upload_session_id,
                file_index=plan.file_index,
                ordered_parts=[receipt],
                auth=auth,
            )
        committed = await self.commit_session(
            session_id=grant.upload_session_id,
            auth=ProducerAuthV1(subject_kind="official_service", subject_id=UUID(int=0)),
        )
        if not isinstance(committed, CommittedArtifactsV1):
            raise ArtifactCommitError("service_commit_cannot_be_committed_ready")
        return committed

    async def finalize_acceptance_evidence(
        self,
        *,
        authorization_id: UUID,
        action: Literal["matrix", "soak"],
        candidate_sha256: str,
        auth: AcceptanceControllerServiceAuthV1,
    ) -> CommittedArtifactsV1:
        if self._acceptance_authority is None:
            raise ArtifactCommitError("acceptance_authority_unavailable")
        declaration = await self._acceptance_authority.load_success_and_lock(
            authorization_id=authorization_id,
            action=action,
            candidate_sha256=candidate_sha256,
            auth=auth,
        )
        result = await self.commit_authoritative_document(declaration=declaration)
        await self._acceptance_authority.complete_locked(artifact_id=result.artifacts[0].id)
        return result

    async def terminate_acceptance_evidence(
        self,
        *,
        authorization_id: UUID,
        action: Literal["matrix", "soak"],
        candidate_sha256: str,
        reason: str,
        auth: AcceptanceControllerServiceAuthV1,
    ) -> CommittedArtifactsV1:
        if self._acceptance_authority is None:
            raise ArtifactCommitError("acceptance_authority_unavailable")
        declaration = await self._acceptance_authority.load_terminal_and_lock(
            authorization_id=authorization_id,
            action=action,
            candidate_sha256=candidate_sha256,
            reason=reason,
            auth=auth,
        )
        result = await self.commit_authoritative_document(declaration=declaration)
        await self._acceptance_authority.complete_locked(artifact_id=result.artifacts[0].id)
        return result

    async def commit_profile_calibration_certification(
        self,
        *,
        authorization_id: UUID,
        calibration_spec_sha256: str,
        scenario_id: str,
        candidate_identity_sha256: str,
        run_ordinal: int,
        source_pipeline_run_id: UUID,
        auth: AcceptanceControllerServiceAuthV1,
    ) -> CommittedArtifactsV1:
        if self._profile_calibration_authority is None:
            raise ArtifactCommitError("profile_calibration_authority_unavailable")
        declaration = await self._profile_calibration_authority.load_certification_and_lock(
            authorization_id=authorization_id,
            calibration_spec_sha256=calibration_spec_sha256,
            scenario_id=scenario_id,
            candidate_identity_sha256=candidate_identity_sha256,
            run_ordinal=run_ordinal,
            source_pipeline_run_id=source_pipeline_run_id,
            auth=auth,
        )
        result = await self.commit_authoritative_document(declaration=declaration)
        await self._profile_calibration_authority.complete_locked(
            artifact_id=result.artifacts[0].id
        )
        return result

    async def finalize_profile_calibration_catalog(
        self,
        *,
        authorization_id: UUID,
        calibration_spec_sha256: str,
        auth: AcceptanceControllerServiceAuthV1,
    ) -> CommittedArtifactsV1:
        if self._profile_calibration_authority is None:
            raise ArtifactCommitError("profile_calibration_authority_unavailable")
        declaration = await self._profile_calibration_authority.load_catalog_and_lock(
            authorization_id=authorization_id,
            calibration_spec_sha256=calibration_spec_sha256,
            auth=auth,
        )
        result = await self.commit_authoritative_document(declaration=declaration)
        await self._profile_calibration_authority.complete_locked(
            artifact_id=result.artifacts[0].id
        )
        return result

    async def terminate_profile_calibration(
        self,
        *,
        authorization_id: UUID,
        calibration_spec_sha256: str,
        reason: str,
        auth: AcceptanceControllerServiceAuthV1,
    ) -> CommittedArtifactsV1:
        if self._profile_calibration_authority is None:
            raise ArtifactCommitError("profile_calibration_authority_unavailable")
        declaration = await self._profile_calibration_authority.load_terminal_and_lock(
            authorization_id=authorization_id,
            calibration_spec_sha256=calibration_spec_sha256,
            reason=reason,
            auth=auth,
        )
        result = await self.commit_authoritative_document(declaration=declaration)
        await self._profile_calibration_authority.complete_locked(
            artifact_id=result.artifacts[0].id
        )
        return result


__all__ = [
    "MAX_APPLICATION_BUFFER_BYTES",
    "AcceptanceControllerServiceAuthV1",
    "AcceptanceEvidenceAuthorityV1",
    "AcceptanceEvidenceProducerV1",
    "ArtifactCommitError",
    "ArtifactCommitManifestV1",
    "ArtifactCommitMarkerV1",
    "ArtifactCommitService",
    "ArtifactManifestV1",
    "AuthoritativeArtifactDocumentV1",
    "CheckpointProducerV1",
    "CommittedArtifactsV1",
    "CommittedReadySessionV1",
    "FinalOutputProducerV1",
    "InMemoryArtifactCommitRepository",
    "InputImportProducerV1",
    "InputImportVerifier",
    "InputMaterializationProducerV1",
    "MultipartFaultHookV1",
    "PartReceiptV1",
    "ProducerAuthV1",
    "ProfileCalibrationEvidenceAuthorityV1",
    "ProfileCalibrationEvidenceProducerV1",
    "UploadAuthV1",
    "UploadFilePlanV1",
    "UploadSessionGrantV1",
    "VerifiedFileV1",
    "VerifiedInputImportV1",
    "WorkerClaimAuthV1",
    "confined_relative_path",
    "multipart_part_size",
]

"""Durable control-plane adapters for the Pipeline Artifact protocol."""

from __future__ import annotations

import hashlib
from collections.abc import AsyncIterator, Sequence
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from typing import Any, cast
from uuid import UUID, uuid4

from pydantic import TypeAdapter
from sqlalchemy import func, null, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from loom.db.schema import (
    Artifact,
    ArtifactLineageEdge,
    ArtifactUploadFile,
    ArtifactUploadSession,
    ExecutionAttempt,
    PipelineBudgetLedger,
    PipelineBudgetReservation,
    PipelineExecutionCheckpoint,
    PipelineInputImport,
    PipelineRun,
    PipelineStageRun,
)
from loom.pipeline.artifact_access import pipeline_output_access_class
from loom.pipeline.artifact_commit import (
    AcceptanceEvidenceProducerV1,
    ArtifactCommitError,
    ArtifactCommitManifestV1,
    ArtifactCommitRepositoryV1,
    ArtifactCommitService,
    CheckpointProducerV1,
    CommitProducerV1,
    CommittedArtifactsV1,
    CommittedArtifactV1,
    FinalOutputProducerV1,
    InputImportProducerV1,
    InputMaterializationProducerV1,
    PartReceiptV1,
    ProducerAuthV1,
    ProfileCalibrationEvidenceProducerV1,
    ServiceExecutionOutputProducerV1,
    UploadAuthV1,
    UploadFilePlanV1,
    _FileState,
    _SessionState,
)
from loom.pipeline.budget import checkpoint_artifact_reservation_key
from loom.pipeline.keys import canonical_digest, canonical_document, digest_bytes
from loom.pipeline.platform_fanout_commit import synthesize_fanout_manifest
from loom.pipeline.spec import (
    BindingItemV1,
    BindingSetV1,
    FanoutManifestV1,
    PlatformFanoutIndexV1,
)
from loom.pipeline.state import StageResultInputV1, StageResultProvenanceV1
from loom.pipeline.work_protocol import (
    CheckpointPrepareRequestV1,
    ExecutionCompleteV1,
    FinalOutputFileCompleteV1,
    FinalOutputPrepareRequestV1,
)
from loom.trajectory.storage import ObjectStore
from loom.trajectory.upload_observation import UploadSpan
from loom_control_plane.artifact_read_service import (
    ResolvedArtifactInput,
    ResolvedStoredFile,
)
from loom_control_plane.metrics import PIPELINE_ARTIFACT_BYTES_TOTAL

_PRODUCER_ADAPTER: TypeAdapter[CommitProducerV1] = TypeAdapter(CommitProducerV1)


def _session_values(state: _SessionState) -> dict[str, Any]:
    producer = state.producer
    values: dict[str, Any] = {
        "id": state.id,
        "team_id": producer.team_id,
        "commit_kind": producer.commit_kind,
        "idempotency_key": state.idempotency_key,
        "request_digest": state.request_digest,
        "prefix": state.prefix,
        "state": state.state,
        "expected_total_max_bytes": state.expected_total_max_bytes,
        "actual_total_bytes": state.actual_total_bytes,
        "upload_token_digest": state.upload_token_digest,
        "expires_at": state.token_expires_at,
        "created_at": state.created_at,
        "updated_at": state.updated_at,
        "canonical_manifest_json": null()
        if state.manifest is None
        else state.manifest.model_dump(mode="json"),
        "manifest_sha256": state.manifest_sha256,
        "committed_marker_sha256": state.marker_sha256,
        "committed_ready_at": state.updated_at if state.state == "committed_ready" else None,
        "committed_at": state.updated_at if state.state == "committed" else None,
        "aborted_at": state.updated_at if state.state == "aborted" else None,
        "checkpoint_envelope_json": (
            null() if state.checkpoint_envelope_json is None else state.checkpoint_envelope_json
        ),
        "checkpoint_envelope_digest": state.checkpoint_envelope_digest,
        "stage_result_json": null(),
    }
    if isinstance(producer, FinalOutputProducerV1 | CheckpointProducerV1):
        values.update(
            pipeline_run_id=producer.pipeline_run_id,
            pipeline_stage_run_id=producer.pipeline_stage_run_id,
            execution_attempt_id=producer.execution_attempt_id,
            attempt_number=producer.attempt_number,
        )
    if isinstance(producer, FinalOutputProducerV1):
        values.update(
            stage_result_json=producer.stage_result_json,
            stage_result_digest=producer.stage_result_digest,
            inventory_digest=producer.inventory_digest,
        )
    elif isinstance(producer, CheckpointProducerV1):
        values["checkpoint_sequence"] = producer.checkpoint_sequence
    elif isinstance(producer, ServiceExecutionOutputProducerV1):
        values.update(
            service_execution_lease_id=producer.service_execution_lease_id,
            service_execution_generation=producer.service_execution_generation,
            service_execution_role=producer.service_execution_role,
            service_execution_runtime_contract_sha256=producer.runtime_contract_sha256,
            service_execution_candidate_sha=producer.candidate_sha,
            service_execution_task_revision_sha256=producer.task_revision_sha256,
            service_execution_command_identity_sha256=producer.command_identity_sha256,
        )
    elif isinstance(producer, InputImportProducerV1):
        values.update(
            pipeline_input_import_id=producer.pipeline_input_import_id,
            actor_user_id=producer.actor_user_id,
        )
    elif isinstance(producer, InputMaterializationProducerV1):
        values.update(
            pipeline_input_materialization_id=producer.pipeline_input_materialization_id,
            actor_user_id=producer.actor_user_id,
        )
    elif isinstance(producer, AcceptanceEvidenceProducerV1):
        values.update(
            pipeline_acceptance_authorization_id=(producer.pipeline_acceptance_authorization_id),
            acceptance_action=producer.acceptance_action,
            acceptance_candidate_sha256=producer.acceptance_candidate_sha256,
            acceptance_result_kind=producer.acceptance_result_kind,
            acceptance_termination_reason=producer.acceptance_termination_reason,
            actor_user_id=producer.actor_user_id,
        )
    elif isinstance(producer, ProfileCalibrationEvidenceProducerV1):
        values.update(
            pipeline_profile_calibration_authorization_id=(
                producer.pipeline_profile_calibration_authorization_id
            ),
            profile_calibration_spec_sha256=producer.profile_calibration_spec_sha256,
            profile_calibration_result_kind=producer.profile_calibration_result_kind,
            profile_calibration_scenario_id=producer.profile_calibration_scenario_id,
            profile_calibration_candidate_identity_sha256=(
                producer.profile_calibration_candidate_identity_sha256
            ),
            profile_calibration_run_ordinal=producer.profile_calibration_run_ordinal,
            profile_calibration_source_pipeline_run_id=(
                producer.profile_calibration_source_pipeline_run_id
            ),
            profile_calibration_termination_reason=(
                producer.profile_calibration_termination_reason
            ),
            actor_user_id=producer.actor_user_id,
        )
    return values


class SqlArtifactCommitRepository(ArtifactCommitRepositoryV1):
    def __init__(
        self,
        *,
        session_factory: async_sessionmaker[AsyncSession],
        store: ObjectStore,
        bucket: str,
    ) -> None:
        self._session_factory = session_factory
        self._store = store
        self._bucket = bucket

    @asynccontextmanager
    async def _database(self, operation: str) -> AsyncIterator[AsyncSession]:
        async with self._session_factory() as db:
            with UploadSpan("database_pool", operation):
                await db.connection()
            with UploadSpan("database", operation):
                yield db

    async def find_idempotent(
        self, producer_identity: bytes, idempotency_key: str
    ) -> _SessionState | None:
        producer = _PRODUCER_ADAPTER.validate_json(producer_identity)
        predicates = [
            ArtifactUploadSession.commit_kind == producer.commit_kind,
            ArtifactUploadSession.idempotency_key == idempotency_key,
            ArtifactUploadSession.team_id == producer.team_id,
        ]
        for name in (
            "pipeline_run_id",
            "pipeline_stage_run_id",
            "execution_attempt_id",
            "service_execution_lease_id",
            "pipeline_input_import_id",
            "pipeline_input_materialization_id",
            "pipeline_acceptance_authorization_id",
            "pipeline_profile_calibration_authorization_id",
        ):
            if hasattr(producer, name):
                predicates.append(getattr(ArtifactUploadSession, name) == getattr(producer, name))
        async with self._session_factory() as db:
            session_id = (
                await db.execute(select(ArtifactUploadSession.id).where(*predicates))
            ).scalar_one_or_none()
        return None if session_id is None else await self.get(session_id)

    async def add(self, state: _SessionState) -> None:
        async with self._session_factory() as db:
            db.add(ArtifactUploadSession(**_session_values(state)))
            # These models intentionally have no mutable ORM relationship.
            # Flush the fenced parent explicitly so PostgreSQL observes the FK
            # authority before the independently persisted file-plan rows.
            await db.flush()
            for item in state.files:
                db.add(
                    ArtifactUploadFile(
                        session_id=state.id,
                        **item.plan.model_dump(mode="python"),
                        state="planned",
                        ordered_part_receipts_json=[],
                    )
                )
            await db.commit()

    async def get(self, session_id: UUID) -> _SessionState:
        async with self._database("load_session") as db:
            row = await db.get(ArtifactUploadSession, session_id)
            if row is None:
                raise ArtifactCommitError("not_found")
            file_rows = list(
                (
                    await db.execute(
                        select(ArtifactUploadFile)
                        .where(ArtifactUploadFile.session_id == session_id)
                        .order_by(ArtifactUploadFile.file_index)
                    )
                ).scalars()
            )
        producer = self._producer(row)
        files: list[_FileState] = []
        for file_row in file_rows:
            plan = UploadFilePlanV1(
                file_index=file_row.file_index,
                preallocated_artifact_id=file_row.preallocated_artifact_id,
                relative_path=file_row.relative_path,
                artifact_name=file_row.artifact_name,
                artifact_type=file_row.artifact_type,
                producer=cast(Any, file_row.producer),
                media_type=file_row.media_type,
                role=cast(Any, file_row.role),
                archive_format=cast(Any, file_row.archive_format),
                expected_max_bytes=file_row.expected_max_bytes,
                expected_sha256=file_row.expected_sha256,
                expected_size=file_row.expected_size,
            )
            receipts = {
                int(value["part_number"]): PartReceiptV1.model_validate(value)
                for value in file_row.ordered_part_receipts_json
            }
            upload = None
            verified = None
            if (row.state != "aborted" and file_row.multipart_upload_id is not None
                    and file_row.state != "verified"):
                object_key = (
                    f"{row.prefix}artifacts/{file_row.preallocated_artifact_id}/"
                    f"{file_row.relative_path}"
                )
                try:
                    upload = await self._store.resume_multipart_upload(
                        bucket=self._bucket,
                        key=object_key,
                        upload_id=file_row.multipart_upload_id,
                    )
                except Exception as exc:
                    # CompleteMultipartUpload may have succeeded while the DB
                    # response was lost. Recover only from a cryptographically
                    # matching final object; never guess from ETag/metadata.
                    facts = await self._store.stat_object(bucket=self._bucket, key=object_key)
                    expected_size = sum(item.size_bytes for item in receipts.values())
                    observed_digest = facts.checksum_sha256
                    if observed_digest is None:
                        digest = hashlib.sha256()
                        async for chunk in self._store.stream_object(
                            bucket=self._bucket, key=object_key
                        ):
                            digest.update(chunk)
                        observed_digest = f"sha256:{digest.hexdigest()}"
                    if facts.content_length != expected_size or (
                        file_row.expected_sha256 is not None
                        and observed_digest != file_row.expected_sha256
                    ):
                        raise ArtifactCommitError("multipart_completion_recovery_drift") from exc
                    from loom.pipeline.artifact_commit import VerifiedFileV1

                    verified = VerifiedFileV1(
                        file_index=file_row.file_index,
                        size_bytes=facts.content_length,
                        sha256=observed_digest,
                    )
            if file_row.state == "verified":
                from loom.pipeline.artifact_commit import VerifiedFileV1

                verified = VerifiedFileV1(
                    file_index=file_row.file_index,
                    size_bytes=cast(int, file_row.actual_size),
                    sha256=cast(str, file_row.computed_sha256),
                )
            files.append(_FileState(plan=plan, upload=upload, receipts=receipts, verified=verified))
        state = _SessionState(
            id=row.id,
            producer=producer,
            files=files,
            idempotency_key=row.idempotency_key,
            request_digest=row.request_digest,
            prefix=row.prefix,
            expected_total_max_bytes=row.expected_total_max_bytes,
            upload_token_digest=(
                b"" if row.upload_token_digest is None else bytes(row.upload_token_digest)
            ),
            token_expires_at=row.expires_at,
            state=row.state,
            actual_total_bytes=row.actual_total_bytes,
            manifest_sha256=row.manifest_sha256,
            marker_sha256=row.committed_marker_sha256,
            created_at=row.created_at,
            updated_at=row.updated_at,
            checkpoint_envelope_json=row.checkpoint_envelope_json,
            checkpoint_envelope_digest=row.checkpoint_envelope_digest,
            version=row.version,
        )
        if row.canonical_manifest_json is not None:
            from loom.pipeline.artifact_commit import (
                ArtifactCommitManifestV1,
                ArtifactManifestV1,
            )

            state.manifest = ArtifactCommitManifestV1.model_validate_json(
                canonical_document(row.canonical_manifest_json)
            )
            for record in state.manifest.artifacts:
                # Per-item immutable facts can be reconstructed from the root;
                # lineage is identical for every item in this v1 session.
                state.item_manifests[record.artifact_id] = ArtifactManifestV1(
                    artifact_id=record.artifact_id,
                    artifact_name=record.artifact_name,
                    artifact_type=record.artifact_type,
                    content_sha256=record.content_sha256,
                    stored_size_bytes=sum(item.size_bytes for item in record.stored_files),
                    unpacked_size_bytes=sum(item.size_bytes for item in record.stored_files),
                    file_count=max(1, len(record.stored_files)),
                    stored_files=record.stored_files,
                    lineage_artifact_ids=state.manifest.input_lineage_artifact_ids,
                    lineage_digests=state.manifest.input_lineage_digests,
                )
            if row.state == "committed":
                state.committed = CommittedArtifactsV1(
                    upload_session_id=row.id,
                    state="committed",
                    artifacts=[
                        CommittedArtifactV1(
                            id=item.artifact_id,
                            name=item.artifact_name,
                            artifact_type=item.artifact_type,
                            content_sha256=item.content_sha256,
                            manifest_sha256=digest_bytes(canonical_document(item)),
                            stored_size_bytes=item.stored_size_bytes,
                            file_count=item.file_count,
                            safety_state="verified_internal",
                        )
                        for item in sorted(
                            state.item_manifests.values(),
                            key=lambda value: value.artifact_name.encode(),
                        )
                    ],
                )
        return state

    @staticmethod
    def _producer(row: ArtifactUploadSession) -> CommitProducerV1:
        if row.commit_kind == "final_output":
            return FinalOutputProducerV1(
                commit_kind="final_output",
                team_id=row.team_id,
                pipeline_run_id=cast(UUID, row.pipeline_run_id),
                pipeline_stage_run_id=cast(UUID, row.pipeline_stage_run_id),
                execution_attempt_id=cast(UUID, row.execution_attempt_id),
                attempt_number=cast(int, row.attempt_number),
                stage_result_json=cast(dict[str, Any], row.stage_result_json),
                stage_result_digest=cast(str, row.stage_result_digest),
                inventory_digest=cast(str, row.inventory_digest),
            )
        if row.commit_kind == "checkpoint":
            return CheckpointProducerV1(
                commit_kind="checkpoint",
                team_id=row.team_id,
                pipeline_run_id=cast(UUID, row.pipeline_run_id),
                pipeline_stage_run_id=cast(UUID, row.pipeline_stage_run_id),
                execution_attempt_id=cast(UUID, row.execution_attempt_id),
                attempt_number=cast(int, row.attempt_number),
                checkpoint_sequence=cast(int, row.checkpoint_sequence),
            )
        if row.commit_kind == "service_execution_output":
            return ServiceExecutionOutputProducerV1(
                commit_kind="service_execution_output",
                team_id=row.team_id,
                service_execution_lease_id=cast(UUID, row.service_execution_lease_id),
                service_execution_generation=cast(int, row.service_execution_generation),
                service_execution_role=cast(Any, row.service_execution_role),
                runtime_contract_sha256=cast(str, row.service_execution_runtime_contract_sha256),
                candidate_sha=cast(str, row.service_execution_candidate_sha),
                task_revision_sha256=cast(str, row.service_execution_task_revision_sha256),
                command_identity_sha256=cast(str, row.service_execution_command_identity_sha256),
            )
        if row.commit_kind == "input_import":
            return InputImportProducerV1(
                commit_kind="input_import",
                team_id=row.team_id,
                pipeline_input_import_id=cast(UUID, row.pipeline_input_import_id),
                actor_user_id=cast(UUID, row.actor_user_id),
            )
        if row.commit_kind == "input_materialization":
            return InputMaterializationProducerV1(
                commit_kind="input_materialization",
                team_id=row.team_id,
                pipeline_input_materialization_id=cast(UUID, row.pipeline_input_materialization_id),
                actor_user_id=cast(UUID, row.actor_user_id),
            )
        if row.commit_kind == "acceptance_evidence":
            return AcceptanceEvidenceProducerV1(
                commit_kind="acceptance_evidence",
                team_id=row.team_id,
                pipeline_acceptance_authorization_id=cast(
                    UUID, row.pipeline_acceptance_authorization_id
                ),
                acceptance_action=cast(Any, row.acceptance_action),
                acceptance_candidate_sha256=cast(str, row.acceptance_candidate_sha256),
                acceptance_result_kind=cast(Any, row.acceptance_result_kind),
                acceptance_termination_reason=row.acceptance_termination_reason,
                actor_user_id=cast(UUID, row.actor_user_id),
            )
        return ProfileCalibrationEvidenceProducerV1(
            commit_kind="profile_calibration_evidence",
            team_id=row.team_id,
            pipeline_profile_calibration_authorization_id=cast(
                UUID, row.pipeline_profile_calibration_authorization_id
            ),
            profile_calibration_spec_sha256=cast(str, row.profile_calibration_spec_sha256),
            profile_calibration_result_kind=cast(Any, row.profile_calibration_result_kind),
            profile_calibration_scenario_id=cast(Any, row.profile_calibration_scenario_id),
            profile_calibration_candidate_identity_sha256=(
                row.profile_calibration_candidate_identity_sha256
            ),
            profile_calibration_run_ordinal=row.profile_calibration_run_ordinal,
            profile_calibration_source_pipeline_run_id=(
                row.profile_calibration_source_pipeline_run_id
            ),
            profile_calibration_termination_reason=row.profile_calibration_termination_reason,
            actor_user_id=cast(UUID, row.actor_user_id),
        )

    async def save(self, state: _SessionState) -> None:
        async with self._database("save_session") as db:
            row = await db.get(ArtifactUploadSession, state.id, with_for_update=True)
            if row is None:
                raise ArtifactCommitError("not_found")
            if row.state == "committed" and state.state != "committed":
                raise ArtifactCommitError("committed_session_immutable")
            if row.state == "aborted" and state.state != "aborted":
                raise ArtifactCommitError("session_not_uploading")
            if row.version != state.version and state.state != "aborted":
                raise ArtifactCommitError("session_progress_conflict")
            for name, value in _session_values(state).items():
                if name != "id":
                    setattr(row, name, value)
            file_rows = list(
                (
                    await db.execute(
                        select(ArtifactUploadFile)
                        .where(ArtifactUploadFile.session_id == state.id)
                        .order_by(ArtifactUploadFile.file_index)
                    )
                ).scalars()
            )
            for persisted, current in zip(file_rows, state.files, strict=True):
                persisted.multipart_upload_id = (
                    None if current.upload is None else current.upload.upload_id
                )
                persisted.ordered_part_receipts_json = [
                    current.receipts[index].model_dump(mode="json")
                    for index in sorted(current.receipts)
                ]
                if current.verified is not None:
                    persisted.computed_sha256 = current.verified.sha256
                    persisted.actual_size = current.verified.size_bytes
                    persisted.state = "verified"
                elif current.upload is not None:
                    persisted.state = "uploading"
                elif state.state == "aborted":
                    persisted.state = "aborted"
            row.version += 1
            await db.commit()
            state.version = row.version

    async def save_file(self, state: _SessionState, *, file_index: int) -> None:
        await self._save_files(state, file_indexes=[file_index], operation="save_file")

    async def save_files(self, state: _SessionState, *, file_indexes: Sequence[int]) -> None:
        await self._save_files(state, file_indexes=file_indexes, operation="save_files")

    async def _save_files(
        self, state: _SessionState, *, file_indexes: Sequence[int], operation: str,
    ) -> None:
        """Merge only the selected files in one parent-locked transaction.

        Lifecycle saves use the session version; independent file writes can
        advance together, but may neither regress receipts nor revive an abort.
        No transaction is held while waiting on object storage.
        """
        if not file_indexes or len(set(file_indexes)) != len(file_indexes) or any(
            index < 0 or index >= len(state.files) for index in file_indexes
        ):
            raise ArtifactCommitError("file_not_found")
        async with self._database(operation) as db:
            row = await db.get(ArtifactUploadSession, state.id, with_for_update=True)
            if row is None:
                raise ArtifactCommitError("not_found")
            if row.state not in {"uploading", "uploaded"}:
                raise ArtifactCommitError("session_not_uploading")
            if row.upload_token_digest != state.upload_token_digest:
                raise ArtifactCommitError("upload_token_invalid")
            files = {item.file_index: item for item in (await db.scalars(
                select(ArtifactUploadFile).where(
                    ArtifactUploadFile.session_id == state.id,
                    ArtifactUploadFile.file_index.in_(file_indexes),
                ).order_by(ArtifactUploadFile.file_index)
            )).all()}
            if len(files) != len(file_indexes):
                raise ArtifactCommitError("file_not_found")
            for file_index in file_indexes:
                current, persisted = state.files[file_index], files[file_index]
                receipts = [current.receipts[index].model_dump(mode="json") for index in sorted(current.receipts)]
                old_receipts = persisted.ordered_part_receipts_json
                upload_id = None if current.upload is None else current.upload.upload_id
                if (
                    receipts[:len(old_receipts)] != old_receipts
                    or (current.verified is None and persisted.multipart_upload_id is not None and persisted.multipart_upload_id != upload_id)
                    or (persisted.state == "verified" and (
                        current.verified is None
                        or current.verified.sha256 != persisted.computed_sha256
                        or current.verified.size_bytes != persisted.actual_size
                    ))
                ):
                    raise ArtifactCommitError("session_progress_conflict")
                persisted.multipart_upload_id = upload_id
                persisted.ordered_part_receipts_json = receipts
                if current.verified is not None:
                    persisted.computed_sha256 = current.verified.sha256
                    persisted.actual_size = current.verified.size_bytes
                    persisted.state = "verified"
                elif current.upload is not None:
                    persisted.state = "uploading"
            await db.flush()
            remaining, total_bytes = (await db.execute(
                select(
                    func.count().filter(ArtifactUploadFile.state != "verified"),
                    func.coalesce(func.sum(ArtifactUploadFile.actual_size), 0),
                ).where(ArtifactUploadFile.session_id == state.id)
            )).one()
            if remaining == 0:
                row.state = "uploaded"
                row.actual_total_bytes = total_bytes
            row.updated_at = max(row.updated_at, state.updated_at)
            row.version += 1
            await db.commit()
            state.state = row.state
            state.actual_total_bytes = row.actual_total_bytes
            state.version = row.version

    async def active(self) -> list[_SessionState]:
        async with self._session_factory() as db:
            ids = list(
                (
                    await db.execute(
                        select(ArtifactUploadSession.id).where(
                            ArtifactUploadSession.state.in_(
                                ["uploading", "uploaded", "committing", "committed_ready"]
                            )
                        )
                    )
                ).scalars()
            )
        return [await self.get(item) for item in ids]


class FinalOutputRouteService:
    def __init__(
        self,
        *,
        service: ArtifactCommitService,
        session_factory: async_sessionmaker[AsyncSession],
    ) -> None:
        self._service = service
        self._session_factory = session_factory

    async def _producer_and_outputs(
        self, attempt: ExecutionAttempt, request: FinalOutputPrepareRequestV1
    ) -> tuple[
        FinalOutputProducerV1,
        dict[str, dict[str, Any]],
        dict[str, Any] | None,
    ]:
        async with self._session_factory() as db:
            stage = await db.get(PipelineStageRun, attempt.stage_run_id)
            if stage is None:
                raise ArtifactCommitError("not_found")
            run = await db.get(PipelineRun, stage.pipeline_run_id)
            if run is None:
                raise ArtifactCommitError("not_found")
        frozen_spec = stage.resolved_execution_spec_json
        if frozen_spec is None:
            raise ArtifactCommitError("execution_spec_not_frozen")
        outputs = {
            str(item["name"]): item
            for item in cast(list[dict[str, Any]], frozen_spec["container_node"]["outputs"])
        }
        fanout_commit = cast(
            dict[str, Any] | None,
            frozen_spec["container_node"].get("fanout_commit"),
        )
        bindings = [
            BindingSetV1.model_validate_json(canonical_document(item))
            for item in (stage.resolved_input_bindings_json or [])
        ]
        producer = FinalOutputProducerV1(
            commit_kind="final_output",
            team_id=run.team_id,
            pipeline_run_id=run.id,
            pipeline_stage_run_id=stage.id,
            execution_attempt_id=attempt.id,
            attempt_number=attempt.attempt_number,
            stage_result_json=request.stage_result.model_dump(mode="json"),
            stage_result_digest=request.stage_result_sha256,
            inventory_digest=canonical_digest(request.files),
            input_lineage_artifact_ids=[
                item.artifact_id for binding in bindings for item in binding.items
            ],
            input_lineage_digests=[
                item.manifest_sha256 for binding in bindings for item in binding.items
            ],
        )
        return producer, outputs, fanout_commit

    async def prepare(self, **kwargs: Any) -> dict[str, Any]:
        attempt = cast(ExecutionAttempt, kwargs["attempt"])
        request = cast(FinalOutputPrepareRequestV1, kwargs["request"])
        producer, outputs, fanout_commit = await self._producer_and_outputs(attempt, request)
        actual_outputs = {item.name: item for item in request.stage_result.outputs}
        if len(actual_outputs) != len(request.stage_result.outputs):
            raise ArtifactCommitError("invalid_stage_result")
        template_name = (
            None if fanout_commit is None else cast(str, fanout_commit["item_binding_name"])
        )
        artifact_ids = {name: uuid4() for name in actual_outputs}
        files: list[UploadFilePlanV1] = []
        for index, item in enumerate(request.files):
            declaration = outputs.get(item.output_name)
            if declaration is None and template_name is not None:
                declaration = outputs.get(template_name)
            actual = actual_outputs.get(item.output_name)
            if (
                declaration is None
                or actual is None
                or declaration.get("producer") != "container"
                or declaration.get("artifact_type") != actual.artifact_type
            ):
                raise ArtifactCommitError("invalid_stage_result")
            relative_path = item.relative_path
            workspace_prefix = f"artifacts/{item.output_name}/"
            if relative_path.startswith(workspace_prefix):
                relative_path = relative_path.removeprefix(workspace_prefix)
            semantic = relative_path == "artifact.json"
            files.append(
                UploadFilePlanV1(
                    file_index=index,
                    preallocated_artifact_id=artifact_ids[item.output_name],
                    relative_path=relative_path,
                    artifact_name=item.output_name,
                    artifact_type=cast(str, declaration["artifact_type"]),
                    producer="container",
                    media_type="application/json" if semantic else "application/octet-stream",
                    role="semantic_document" if semantic else "payload",
                    archive_format="none",
                    expected_max_bytes=cast(int, declaration["max_bytes"]),
                    expected_sha256=item.sha256,
                    expected_size=item.size_bytes,
                )
            )
        if fanout_commit is not None:
            manifest_name = cast(str, fanout_commit["manifest_output_name"])
            declaration = outputs.get(manifest_name)
            if (
                declaration is None
                or declaration.get("producer") != "platform"
                or declaration.get("artifact_type") != "loom.fanout-manifest.v1"
            ):
                raise ArtifactCommitError("invalid_stage_result")
            files.append(
                UploadFilePlanV1(
                    file_index=len(files),
                    preallocated_artifact_id=uuid4(),
                    relative_path="artifact.json",
                    artifact_name=manifest_name,
                    artifact_type="loom.fanout-manifest.v1",
                    producer="platform",
                    media_type="application/json",
                    role="semantic_document",
                    archive_format="none",
                    expected_max_bytes=cast(int, declaration["max_bytes"]),
                    expected_sha256=None,
                    expected_size=None,
                )
            )
        grant = await self._service.prepare_session(
            producer=producer,
            files=files,
            idempotency_key=str(kwargs["request_id"]),
            request_digest=canonical_digest(request),
        )
        return grant.model_dump(mode="json")

    async def renew(self, **kwargs: Any) -> dict[str, Any]:
        attempt = cast(ExecutionAttempt, kwargs["attempt"])
        result = await self._service.renew_upload_token(
            session_id=kwargs["session_id"],
            auth=ProducerAuthV1(subject_kind="worker", subject_id=cast(UUID, attempt.worker_id)),
        )
        return result.model_dump(mode="json")

    async def put_part(self, **kwargs: Any) -> dict[str, Any]:
        result = await self._service.write_part(
            session_id=kwargs["session_id"],
            file_index=kwargs["file_index"],
            part_number=kwargs["part_number"],
            content_length=kwargs["content_length"],
            content_sha256=kwargs["content_sha256"],
            body=cast(AsyncIterator[bytes], kwargs["body"]),
            auth=UploadAuthV1(upload_token=kwargs["upload_token"]),
        )
        return result.model_dump(mode="json")

    async def complete_file(self, **kwargs: Any) -> dict[str, Any]:
        request = cast(FinalOutputFileCompleteV1, kwargs["request"])
        result = await self._service.complete_file(
            session_id=kwargs["session_id"],
            file_index=kwargs["file_index"],
            ordered_parts=[PartReceiptV1.model_validate(item) for item in request.ordered_parts],
            auth=UploadAuthV1(upload_token=kwargs["upload_token"]),
        )
        return result.model_dump(mode="json")

    async def commit(self, **kwargs: Any) -> dict[str, Any]:
        session_id = cast(UUID, kwargs["session_id"])
        auth = UploadAuthV1(upload_token=kwargs["upload_token"])
        await self._commit_platform_fanout_document(
            attempt=cast(ExecutionAttempt, kwargs["attempt"]),
            session_id=session_id,
            auth=auth,
        )
        result = await self._service.commit_session(
            session_id=session_id,
            auth=auth,
        )
        return result.model_dump(mode="json")

    async def _commit_platform_fanout_document(
        self,
        *,
        attempt: ExecutionAttempt,
        session_id: UUID,
        auth: UploadAuthV1,
    ) -> None:
        async with self._session_factory() as db:
            stage = await db.get(PipelineStageRun, attempt.stage_run_id)
            upload = await db.get(ArtifactUploadSession, session_id)
            if (
                stage is None
                or upload is None
                or upload.execution_attempt_id != attempt.id
                or upload.commit_kind != "final_output"
                or stage.resolved_execution_spec_json is None
            ):
                raise ArtifactCommitError("completion_identity_drift")
            node = cast(dict[str, Any], stage.resolved_execution_spec_json.get("container_node"))
            fanout = cast(dict[str, Any] | None, node.get("fanout_commit"))
            if fanout is None:
                return
            rows = list(
                (
                    await db.execute(
                        select(ArtifactUploadFile)
                        .where(ArtifactUploadFile.session_id == session_id)
                        .order_by(ArtifactUploadFile.file_index)
                    )
                ).scalars()
            )
            index_name = cast(str, fanout["index_output_name"])
            manifest_name = cast(str, fanout["manifest_output_name"])
            index_row = next(
                (
                    row
                    for row in rows
                    if row.artifact_name == index_name
                    and row.relative_path == "artifact.json"
                    and row.role == "semantic_document"
                    and row.producer == "container"
                ),
                None,
            )
            platform_row = next(
                (
                    row
                    for row in rows
                    if row.artifact_name == manifest_name
                    and row.relative_path == "artifact.json"
                    and row.role == "semantic_document"
                    and row.producer == "platform"
                ),
                None,
            )
            if index_row is None or platform_row is None:
                raise ArtifactCommitError("platform_fanout_plan_missing")
            artifact_ids_by_output: dict[str, UUID] = {}
            artifact_types_by_output: dict[str, str] = {}
            result = cast(dict[str, Any], upload.stage_result_json)
            for output in cast(list[dict[str, Any]], result.get("outputs", [])):
                name = cast(str, output["name"])
                artifact_types_by_output[name] = cast(str, output["artifact_type"])
            for row in rows:
                name = row.artifact_name
                if row.producer != "container" or name not in artifact_types_by_output:
                    continue
                existing = artifact_ids_by_output.setdefault(name, row.preallocated_artifact_id)
                if existing != row.preallocated_artifact_id:
                    raise ArtifactCommitError("committed_output_drift")
            index_declaration = next(
                (
                    value
                    for value in cast(list[dict[str, Any]], node.get("outputs", []))
                    if value["name"] == index_name
                ),
                None,
            )
            if index_declaration is None:
                raise ArtifactCommitError("platform_fanout_plan_missing")
            index_max_bytes = cast(int, index_declaration["max_bytes"])
            index_file_index = index_row.file_index
            platform_file_index = platform_row.file_index
        index_bytes = await self._service.read_verified_file(
            session_id=session_id,
            file_index=index_file_index,
            auth=auth,
            max_bytes=index_max_bytes,
        )
        if (
            canonical_document(PlatformFanoutIndexV1.model_validate_json(index_bytes))
            != index_bytes
        ):
            raise ArtifactCommitError("platform_fanout_index_noncanonical")
        index = PlatformFanoutIndexV1.model_validate_json(index_bytes)
        if len(index.items) > cast(int, fanout["max_items"]):
            raise ArtifactCommitError("platform_fanout_index_too_large")
        item_binding_name = cast(str, fanout["item_binding_name"])
        template = next(
            (
                value
                for value in cast(list[dict[str, Any]], node.get("outputs", []))
                if value["name"] == item_binding_name
            ),
            None,
        )
        if template is None:
            raise ArtifactCommitError("platform_fanout_plan_missing")
        item_artifact_type = cast(str, template["artifact_type"])
        triples: list[tuple[str, str, str]] = []
        for item in index.items:
            if artifact_types_by_output.get(item.output_name) != item_artifact_type:
                raise ArtifactCommitError("platform_fanout_item_type_drift")
            triples.append((item.shard_key, item.output_name, item_artifact_type))
        manifest_value = FanoutManifestV1.model_validate_json(
            canonical_document(
                synthesize_fanout_manifest(
                    triples,
                    namespace=attempt.id,
                    item_binding_name=item_binding_name,
                    artifact_ids_by_output=artifact_ids_by_output,
                )
            )
        )
        await self._service.commit_platform_document(
            session_id=session_id,
            file_index=platform_file_index,
            value=manifest_value,
            auth=auth,
        )

    async def abort(self, **kwargs: Any) -> dict[str, Any]:
        attempt = cast(ExecutionAttempt, kwargs["attempt"])
        await self._service.abort_session(
            session_id=kwargs["session_id"],
            auth=ProducerAuthV1(subject_kind="worker", subject_id=cast(UUID, attempt.worker_id)),
            reason=kwargs["request"].reason,
        )
        return {"upload_session_id": str(kwargs["session_id"]), "state": "aborted"}


class CheckpointRouteService(FinalOutputRouteService):
    """Claim-fenced checkpoint upload plus atomic Artifact/latest publication."""

    async def prepare(self, **kwargs: Any) -> dict[str, Any]:
        attempt = cast(ExecutionAttempt, kwargs["attempt"])
        request = cast(CheckpointPrepareRequestV1, kwargs["request"])
        async with self._session_factory() as db:
            stage = await db.get(PipelineStageRun, attempt.stage_run_id)
            if stage is None:
                raise ArtifactCommitError("not_found")
            run = await db.get(PipelineRun, stage.pipeline_run_id)
            if run is None or stage.resolved_execution_spec_json is None:
                raise ArtifactCommitError("execution_spec_not_frozen")
            policy = stage.resolved_execution_spec_json["container_node"].get("checkpoint")
        if policy is None:
            raise ArtifactCommitError("checkpoints_disabled")
        envelope = request.checkpoint
        if (
            envelope.pipeline_run_id != run.id
            or envelope.stage_run_id != stage.id
            or envelope.attempt_id != attempt.id
            or envelope.recipe_digest != stage.resolved_execution_spec_json["recipe_digest"]
            or envelope.execution_spec_digest != stage.execution_spec_digest
            or envelope.resolved_input_bindings_digest != stage.resolved_input_bindings_digest
            or envelope.image_digest
            != stage.resolved_execution_spec_json["container_node"]["image"]
        ):
            raise ArtifactCommitError("checkpoint_contract_mismatch")
        checkpoint_bytes = envelope.persisted_bytes()
        exact_bytes = len(checkpoint_bytes) + sum(item.size_bytes for item in request.files)
        if exact_bytes > int(policy["max_bytes"]):
            raise ArtifactCommitError("checkpoint_too_large")
        reservation_key = checkpoint_artifact_reservation_key(attempt.id, envelope.sequence)
        request_digest = canonical_digest(request)
        async with self._session_factory() as db, db.begin():
            ledger = await db.get(PipelineBudgetLedger, run.id, with_for_update=True)
            if ledger is None or (
                ledger.terminal_cause is not None
                and not (ledger.terminal_cause == "user_cancel" and request.cancel_drain)
            ):
                raise ArtifactCommitError("checkpoint_budget_unavailable")
            if request.cancel_drain:
                if attempt.cancellation_requested_at is None:
                    raise ArtifactCommitError("checkpoint_cancel_drain_forbidden")
                active_cancel_drain = await db.scalar(
                    select(ArtifactUploadSession.id)
                    .where(
                        ArtifactUploadSession.execution_attempt_id == attempt.id,
                        ArtifactUploadSession.commit_kind == "checkpoint",
                        ArtifactUploadSession.created_at >= attempt.cancellation_requested_at,
                        ArtifactUploadSession.state != "aborted",
                    )
                    .limit(1)
                )
                if active_cancel_drain is not None:
                    raise ArtifactCommitError("checkpoint_cancel_drain_forbidden")
            reservation = (
                await db.execute(
                    select(PipelineBudgetReservation)
                    .where(
                        PipelineBudgetReservation.pipeline_run_id == run.id,
                        PipelineBudgetReservation.kind == "artifact",
                        PipelineBudgetReservation.reservation_key == reservation_key,
                    )
                    .with_for_update()
                )
            ).scalar_one_or_none()
            if reservation is None:
                amount = int(policy["max_bytes"])
                if (
                    ledger.artifact_reserved_bytes + ledger.artifact_settled_bytes + amount
                    > ledger.artifact_limit_bytes
                ):
                    raise ArtifactCommitError("checkpoint_budget_unavailable")
                reservation = PipelineBudgetReservation(
                    pipeline_run_id=run.id,
                    execution_attempt_id=attempt.id,
                    kind="artifact",
                    reservation_key=reservation_key,
                    request_digest=request_digest,
                    reserved_amount=amount,
                    metadata_json={"checkpoint_sequence": envelope.sequence},
                )
                db.add(reservation)
                ledger.artifact_reserved_bytes += amount
            elif reservation.request_digest != request_digest or reservation.state not in {
                "active",
                "settled",
            }:
                raise ArtifactCommitError("checkpoint_reservation_conflict")
        producer = CheckpointProducerV1(
            commit_kind="checkpoint",
            team_id=run.team_id,
            pipeline_run_id=run.id,
            pipeline_stage_run_id=stage.id,
            execution_attempt_id=attempt.id,
            attempt_number=attempt.attempt_number,
            checkpoint_sequence=envelope.sequence,
        )
        artifact_id = uuid4()
        name = f"checkpoint-{envelope.sequence:012d}"
        files = [
            UploadFilePlanV1(
                file_index=0,
                preallocated_artifact_id=artifact_id,
                relative_path="checkpoint.json",
                artifact_name=name,
                artifact_type="loom.execution-checkpoint.v1",
                producer="platform",
                media_type="application/json",
                role="semantic_document",
                archive_format="none",
                expected_max_bytes=int(policy["max_bytes"]),
                expected_sha256=request.checkpoint_sha256,
                expected_size=len(checkpoint_bytes),
            )
        ]
        for index, item in enumerate(request.files, start=1):
            files.append(
                UploadFilePlanV1(
                    file_index=index,
                    preallocated_artifact_id=artifact_id,
                    relative_path=item.relative_path,
                    artifact_name=name,
                    artifact_type="loom.execution-checkpoint.v1",
                    producer="platform",
                    media_type="application/json"
                    if item.relative_path.endswith(".json")
                    else "application/octet-stream",
                    role="payload",
                    archive_format="none",
                    expected_max_bytes=int(policy["max_bytes"]),
                    expected_sha256=item.sha256,
                    expected_size=item.size_bytes,
                )
            )
        grant = await self._service.prepare_session(
            producer=producer,
            files=files,
            idempotency_key=str(kwargs["request_id"]),
            request_digest=request_digest,
        )
        async with self._session_factory() as db, db.begin():
            upload = await db.get(
                ArtifactUploadSession, grant.upload_session_id, with_for_update=True
            )
            if upload is None:
                raise ArtifactCommitError("not_found")
            upload.checkpoint_envelope_json = envelope.model_dump(mode="json")
            upload.checkpoint_envelope_digest = request.checkpoint_sha256
        return grant.model_dump(mode="json")

    async def commit(self, **kwargs: Any) -> dict[str, Any]:
        result = await self._service.commit_session(
            session_id=kwargs["session_id"],
            auth=UploadAuthV1(upload_token=kwargs["upload_token"]),
        )
        committed_bytes = 0
        async with self._session_factory() as db, db.begin():
            upload = await db.get(ArtifactUploadSession, kwargs["session_id"], with_for_update=True)
            if upload is None or upload.commit_kind != "checkpoint" or upload.state != "committed":
                raise ArtifactCommitError("checkpoint_not_committed")
            if upload.checkpoint_envelope_json is None or upload.checkpoint_envelope_digest is None:
                raise ArtifactCommitError("checkpoint_contract_mismatch")
            attempt = await db.get(
                ExecutionAttempt, upload.execution_attempt_id, with_for_update=True
            )
            stage = await db.get(
                PipelineStageRun, upload.pipeline_stage_run_id, with_for_update=True
            )
            if attempt is None or stage is None or attempt.state not in {"claimed", "running"}:
                raise ArtifactCommitError("claim_fenced")
            envelope = upload.checkpoint_envelope_json
            from loom.pipeline.checkpoint import ExecutionCheckpointV1

            parsed_envelope = ExecutionCheckpointV1.model_validate(envelope)
            if digest_bytes(parsed_envelope.persisted_bytes()) != upload.checkpoint_envelope_digest:
                raise ArtifactCommitError("checkpoint_contract_mismatch")
            existing = (
                await db.execute(
                    select(PipelineExecutionCheckpoint).where(
                        PipelineExecutionCheckpoint.execution_attempt_id == attempt.id,
                        PipelineExecutionCheckpoint.checkpoint_sequence
                        == upload.checkpoint_sequence,
                    )
                )
            ).scalar_one_or_none()
            if existing is None:
                manifest = cast(dict[str, Any], upload.canonical_manifest_json)
                record = manifest["artifacts"][0]
                artifact_id = UUID(record["artifact_id"])
                artifact = await db.get(Artifact, artifact_id)
                if artifact is None:
                    stored_files = record["stored_files"]
                    artifact = Artifact(
                        id=artifact_id,
                        artifact_type="loom.execution-checkpoint.v1",
                        name=f"checkpoint-{upload.checkpoint_sequence:012d}",
                        team_id=upload.team_id,
                        pipeline_run_id=upload.pipeline_run_id,
                        pipeline_stage_run_id=upload.pipeline_stage_run_id,
                        execution_attempt_id=attempt.id,
                        producer_kind="checkpoint",
                        content_hash=record["content_sha256"],
                        storage={"session_id": str(upload.id), "files": stored_files},
                        visibility="team",
                        share_status="pending_scan",
                        safety_state="verified_internal",
                        access_class="team_runtime",
                        artifact_upload_session_id=upload.id,
                        manifest_sha256=record["manifest_sha256"],
                        stored_size_bytes=sum(item["size_bytes"] for item in stored_files),
                        unpacked_size_bytes=sum(item["size_bytes"] for item in stored_files),
                        file_count=len(stored_files),
                    )
                    db.add(artifact)
                    await db.flush()
                previous = (
                    await db.execute(
                        select(PipelineExecutionCheckpoint)
                        .where(PipelineExecutionCheckpoint.pipeline_stage_run_id == stage.id)
                        .order_by(
                            PipelineExecutionCheckpoint.attempt_number.desc(),
                            PipelineExecutionCheckpoint.checkpoint_sequence.desc(),
                        )
                        .limit(1)
                    )
                ).scalar_one_or_none()
                checkpoint = PipelineExecutionCheckpoint(
                    artifact_id=artifact_id,
                    pipeline_run_id=cast(UUID, upload.pipeline_run_id),
                    pipeline_stage_run_id=stage.id,
                    execution_attempt_id=attempt.id,
                    attempt_number=attempt.attempt_number,
                    checkpoint_sequence=cast(int, upload.checkpoint_sequence),
                    recipe_digest=envelope["recipe_digest"],
                    resolved_input_bindings_digest=envelope["resolved_input_bindings_digest"],
                    execution_spec_digest=envelope["execution_spec_digest"],
                    image_digest=envelope["image_digest"],
                    resume_compatibility_key=envelope["resume_compatibility_key"],
                    checkpoint_json=envelope,
                    checkpoint_digest=upload.checkpoint_envelope_digest,
                    source_attempt_state=attempt.state,
                )
                db.add(checkpoint)
                if previous is None or (
                    attempt.attempt_number,
                    cast(int, upload.checkpoint_sequence),
                ) > (
                    previous.attempt_number,
                    previous.sequence,
                ):
                    stage.latest_checkpoint_artifact_id = artifact_id
                    stage.version += 1
            reservation_key = checkpoint_artifact_reservation_key(
                attempt.id, cast(int, upload.checkpoint_sequence)
            )
            reservation = (
                await db.execute(
                    select(PipelineBudgetReservation)
                    .where(
                        PipelineBudgetReservation.pipeline_run_id == stage.pipeline_run_id,
                        PipelineBudgetReservation.kind == "artifact",
                        PipelineBudgetReservation.reservation_key == reservation_key,
                    )
                    .with_for_update()
                )
            ).scalar_one_or_none()
            if reservation is None:
                raise ArtifactCommitError("checkpoint_reservation_missing")
            if reservation.state == "active":
                actual = upload.actual_total_bytes
                if actual > reservation.reserved_amount:
                    raise ArtifactCommitError("checkpoint_reservation_exceeded")
                ledger = await db.get(
                    PipelineBudgetLedger, stage.pipeline_run_id, with_for_update=True
                )
                if ledger is None:
                    raise ArtifactCommitError("checkpoint_budget_unavailable")
                ledger.artifact_reserved_bytes -= reservation.reserved_amount
                ledger.artifact_settled_bytes += actual
                reservation.state = "settled"
                reservation.settled_amount = actual
                reservation.settled_at = datetime.now(UTC)
                committed_bytes = actual
            elif reservation.state != "settled":
                raise ArtifactCommitError("checkpoint_reservation_conflict")
        if committed_bytes:
            PIPELINE_ARTIFACT_BYTES_TOTAL.labels(artifact_class="checkpoint").inc(committed_bytes)
        return result.model_dump(mode="json")


class ExecutionAttemptCompletionService:
    @staticmethod
    async def _validate_contract(
        *,
        attempt: ExecutionAttempt,
        report: ExecutionCompleteV1,
        upload: ArtifactUploadSession,
        session: AsyncSession,
    ) -> tuple[
        PipelineStageRun,
        ArtifactCommitManifestV1,
        dict[UUID, str],
        dict[str, int],
    ]:
        stage = await session.get(PipelineStageRun, attempt.stage_run_id, with_for_update=True)
        if stage is None or stage.resolved_execution_spec_json is None:
            raise ArtifactCommitError("execution_spec_not_frozen")
        run = await session.get(PipelineRun, stage.pipeline_run_id)
        if run is None:
            raise ArtifactCommitError("not_found")
        if (
            upload.commit_kind != "final_output"
            or upload.pipeline_run_id != run.id
            or upload.pipeline_stage_run_id != stage.id
            or upload.execution_attempt_id != attempt.id
            or upload.attempt_number != attempt.attempt_number
        ):
            raise ArtifactCommitError("completion_identity_drift")
        if (
            upload.stage_result_digest != report.stage_result_sha256
            or upload.stage_result_json != report.stage_result.model_dump(mode="json")
        ):
            raise ArtifactCommitError("stage_result_drift")
        if upload.canonical_manifest_json is None:
            raise ArtifactCommitError("manifest_missing")
        manifest = ArtifactCommitManifestV1.model_validate_json(
            canonical_document(upload.canonical_manifest_json)
        )
        if (
            manifest.session_id != upload.id
            or manifest.commit_kind != "final_output"
            or manifest.request_digest != upload.request_digest
            or manifest.total_bytes != upload.actual_total_bytes
            or canonical_digest(manifest) != upload.manifest_sha256
            or sum(item.size_bytes for record in manifest.artifacts for item in record.stored_files)
            != manifest.total_bytes
        ):
            raise ArtifactCommitError("manifest_session_drift")

        frozen = stage.resolved_execution_spec_json
        node = cast(dict[str, Any], frozen.get("container_node"))
        expected_inputs = [
            StageResultInputV1(
                binding_name=binding.binding_name,
                item_key=item.item_key,
                artifact_id=item.artifact_id,
                artifact_type=binding.artifact_type,
                manifest_sha256=item.manifest_sha256,
            )
            for binding in (
                BindingSetV1.model_validate_json(canonical_document(value))
                for value in (stage.resolved_input_bindings_json or [])
            )
            for item in binding.items
        ]
        expected_provenance = StageResultProvenanceV1(
            pipeline_run_id=run.id,
            stage_run_id=stage.id,
            execution_attempt_id=attempt.id,
            recipe_digest=run.recipe_digest,
            execution_spec_digest=cast(str, stage.execution_spec_digest),
            image_digest=cast(str, frozen.get("resolved_image_manifest_digest")),
        )
        result = report.stage_result
        if result.inputs != expected_inputs or result.provenance != expected_provenance:
            raise ArtifactCommitError("stage_result_claim_drift")
        if result.domain_outcome is None or result.retry_class.value != "none" or result.error:
            raise ArtifactCommitError("invalid_stage_result")

        declarations = {
            cast(str, item["name"]): item
            for item in cast(list[dict[str, Any]], node.get("outputs", []))
        }
        fanout = cast(dict[str, Any] | None, node.get("fanout_commit"))
        template_name = None if fanout is None else cast(str, fanout["item_binding_name"])
        actual_outputs = {item.name: item for item in result.outputs}
        for name, output in actual_outputs.items():
            declaration = declarations.get(name)
            if declaration is None and template_name is not None:
                declaration = declarations.get(template_name)
            if (
                declaration is None
                or declaration.get("producer") != "container"
                or declaration.get("artifact_type") != output.artifact_type
            ):
                raise ArtifactCommitError("invalid_stage_result")
        required = {
            name
            for name, declaration in declarations.items()
            if declaration.get("producer") == "container"
            and declaration.get("required") is True
            and name != template_name
        }
        if not required.issubset(actual_outputs):
            raise ArtifactCommitError("invalid_stage_result")

        file_rows = list(
            (
                await session.execute(
                    select(ArtifactUploadFile)
                    .where(ArtifactUploadFile.session_id == upload.id)
                    .order_by(ArtifactUploadFile.file_index)
                )
            ).scalars()
        )
        producer_kind_by_id = {row.preallocated_artifact_id: row.producer for row in file_rows}
        if any(value not in {"container", "platform"} for value in producer_kind_by_id.values()):
            raise ArtifactCommitError("final_output_producer_invalid")
        observed_files = {
            (
                row.preallocated_artifact_id,
                row.artifact_name,
                row.artifact_type,
                row.file_index,
                row.relative_path,
                row.role,
                row.archive_format,
                row.media_type,
                row.actual_size,
                row.computed_sha256,
            )
            for row in file_rows
            if row.state == "verified"
        }
        expected_files = {
            (
                record.artifact_id,
                record.artifact_name,
                record.artifact_type,
                item.file_index,
                item.relative_path,
                item.role,
                item.archive_format,
                item.media_type,
                item.size_bytes,
                item.sha256,
            )
            for record in manifest.artifacts
            for item in record.stored_files
        }
        if observed_files != expected_files or len(observed_files) != len(file_rows):
            raise ArtifactCommitError("committed_output_drift")
        records_by_name = {record.artifact_name: record for record in manifest.artifacts}
        platform_outputs = {
            name
            for name, declaration in declarations.items()
            if declaration.get("producer") == "platform"
        }
        if (
            len(records_by_name) != len(manifest.artifacts)
            or set(records_by_name) != set(actual_outputs) | platform_outputs
        ):
            raise ArtifactCommitError("committed_output_drift")
        committed_bytes = {"final_output": 0, "control": 0}
        for name, record in records_by_name.items():
            actual_output = actual_outputs.get(name)
            if actual_output is not None:
                if (
                    record.artifact_type != actual_output.artifact_type
                    or producer_kind_by_id.get(record.artifact_id) != "container"
                ):
                    raise ArtifactCommitError("committed_output_drift")
                committed_bytes["final_output"] += sum(
                    item.size_bytes for item in record.stored_files
                )
            else:
                declaration = declarations.get(name)
                if (
                    declaration is None
                    or declaration.get("producer") != "platform"
                    or record.artifact_type != declaration.get("artifact_type")
                    or producer_kind_by_id.get(record.artifact_id) != "platform"
                ):
                    raise ArtifactCommitError("committed_output_drift")
                committed_bytes["control"] += sum(item.size_bytes for item in record.stored_files)
        expected_lineage = [item.artifact_id for item in expected_inputs]
        expected_lineage_digests = [item.manifest_sha256 for item in expected_inputs]
        if (
            manifest.input_lineage_artifact_ids != expected_lineage
            or manifest.input_lineage_digests != expected_lineage_digests
        ):
            raise ArtifactCommitError("output_lineage_drift")
        return stage, manifest, producer_kind_by_id, committed_bytes

    async def complete(
        self,
        *,
        attempt: ExecutionAttempt,
        report: ExecutionCompleteV1,
        session: AsyncSession,
    ) -> dict[str, int]:
        upload = (
            await session.execute(
                select(ArtifactUploadSession)
                .where(
                    ArtifactUploadSession.id == report.final_output_upload_session_id,
                    ArtifactUploadSession.execution_attempt_id == attempt.id,
                )
                .with_for_update()
            )
        ).scalar_one_or_none()
        if upload is None or upload.state not in {"committed_ready", "committed"}:
            raise ArtifactCommitError("session_not_committed_ready")
        stage, manifest, producer_kind_by_id, committed_bytes = await self._validate_contract(
            attempt=attempt,
            report=report,
            upload=upload,
            session=session,
        )
        run = await session.get(PipelineRun, stage.pipeline_run_id)
        if run is None:
            raise ArtifactCommitError("not_found")
        if upload.state == "committed":
            persisted = list(
                (
                    await session.execute(
                        select(Artifact).where(Artifact.artifact_upload_session_id == upload.id)
                    )
                ).scalars()
            )
            expected = {
                (
                    record.artifact_id,
                    record.artifact_name,
                    record.artifact_type,
                    record.manifest_sha256,
                )
                for record in manifest.artifacts
            }
            observed = {
                (item.id, item.name, item.artifact_type, item.manifest_sha256) for item in persisted
            }
            if observed != expected:
                raise ArtifactCommitError("committed_output_drift")
            return committed_bytes
        lineage = manifest.input_lineage_artifact_ids
        for record in manifest.artifacts:
            artifact_id = record.artifact_id
            stored_files = record.stored_files
            session.add(
                Artifact(
                    id=artifact_id,
                    artifact_type=record.artifact_type,
                    name=record.artifact_name,
                    team_id=upload.team_id,
                    pipeline_run_id=upload.pipeline_run_id,
                    pipeline_stage_run_id=upload.pipeline_stage_run_id,
                    execution_attempt_id=attempt.id,
                    producer_kind=producer_kind_by_id[artifact_id],
                    content_hash=record.content_sha256,
                    storage={
                        "session_id": str(upload.id),
                        "files": [item.model_dump(mode="json") for item in stored_files],
                    },
                    visibility="team",
                    share_status="pending_scan",
                    safety_state="verified_internal",
                    access_class=pipeline_output_access_class(
                        record.artifact_type,
                        recipe_name=run.recipe_name,
                        node_key=stage.node_key,
                        artifact_name=record.artifact_name,
                    ),
                    artifact_upload_session_id=upload.id,
                    manifest_sha256=record.manifest_sha256,
                    stored_size_bytes=sum(item.size_bytes for item in stored_files),
                    unpacked_size_bytes=sum(item.size_bytes for item in stored_files),
                    file_count=max(1, len(stored_files)),
                )
            )
            for parent_id in lineage:
                session.add(
                    ArtifactLineageEdge(
                        child_artifact_id=artifact_id,
                        parent_artifact_id=parent_id,
                        relation="pipeline_input",
                    )
                )
        upload.state = "committed"
        upload.committed_at = datetime.now(UTC)
        upload.updated_at = datetime.now(UTC)
        return committed_bytes


class SqlArtifactInputResolver:
    """Resolve only frozen Attempt bindings and revalidate marker authority."""

    def __init__(
        self,
        *,
        session_factory: async_sessionmaker[AsyncSession],
        store: ObjectStore,
        bucket: str,
    ) -> None:
        self._session_factory = session_factory
        self._store = store
        self._bucket = bucket

    async def _object_matches(self, *, key: str, expected_digest: str) -> bool:
        facts = await self._store.stat_object(bucket=self._bucket, key=key)
        if facts.checksum_sha256 is not None:
            return facts.checksum_sha256 == expected_digest
        observed = __import__("hashlib").sha256()
        async for chunk in self._store.stream_object(
            bucket=self._bucket, key=key, chunk_size=64 * 1024 * 1024
        ):
            observed.update(chunk)
        return f"sha256:{observed.hexdigest()}" == expected_digest

    async def resolve(
        self,
        *,
        attempt_id: UUID,
        binding_name: str,
        item_key: str,
    ) -> ResolvedArtifactInput:
        async with self._session_factory() as db:
            attempt = await db.get(ExecutionAttempt, attempt_id)
            if attempt is None:
                raise KeyError(attempt_id)
            stage = await db.get(PipelineStageRun, attempt.stage_run_id)
            if stage is None or stage.resolved_input_bindings_json is None:
                raise KeyError(attempt_id)
            bindings = [
                BindingSetV1.model_validate_json(canonical_document(value))
                for value in stage.resolved_input_bindings_json
            ]
            if (
                binding_name == "loom_checkpoint"
                and item_key == "singleton"
                and attempt.resumed_checkpoint_artifact_id is not None
            ):
                checkpoint = await db.get(Artifact, attempt.resumed_checkpoint_artifact_id)
                if (
                    checkpoint is None
                    or checkpoint.manifest_sha256 is None
                    or checkpoint.stored_size_bytes is None
                    or checkpoint.unpacked_size_bytes is None
                    or checkpoint.file_count is None
                ):
                    raise KeyError(binding_name)
                bindings.append(
                    BindingSetV1(
                        binding_name="loom_checkpoint",
                        artifact_type=checkpoint.artifact_type,
                        cardinality="one",
                        items=[
                            BindingItemV1(
                                artifact_id=checkpoint.id,
                                content_sha256=checkpoint.content_hash,
                                file_count=checkpoint.file_count,
                                item_key="singleton",
                                manifest_sha256=checkpoint.manifest_sha256,
                                stored_size_bytes=checkpoint.stored_size_bytes,
                                unpacked_size_bytes=checkpoint.unpacked_size_bytes,
                            )
                        ],
                    )
                )
            binding = next(
                (value for value in bindings if value.binding_name == binding_name), None
            )
            if binding is None:
                raise KeyError(binding_name)
            item = next((value for value in binding.items if value.item_key == item_key), None)
            if item is None:
                raise KeyError(item_key)
            artifact = await db.get(Artifact, item.artifact_id)
            if artifact is None or artifact.artifact_upload_session_id is None:
                raise KeyError(item.artifact_id)
            upload = await db.get(ArtifactUploadSession, artifact.artifact_upload_session_id)
            run = await db.get(PipelineRun, stage.pipeline_run_id)
            if upload is None or upload.state != "committed":
                raise KeyError(item.artifact_id)
            if (
                run is None
                or artifact.team_id != run.team_id
                or artifact.team_id != upload.team_id
                or artifact.artifact_type != binding.artifact_type
                or artifact.content_hash != item.content_sha256
                or artifact.manifest_sha256 != item.manifest_sha256
                or artifact.stored_size_bytes != item.stored_size_bytes
                or artifact.unpacked_size_bytes != item.unpacked_size_bytes
                or artifact.file_count != item.file_count
            ):
                raise ArtifactCommitError("input_descriptor_drift")
            if (
                artifact.access_class == "authoring_restricted"
                and artifact.pipeline_run_id != run.id
            ):
                raise KeyError(item.artifact_id)
            if artifact.safety_state != "verified_internal":
                if (
                    artifact.producer_kind != "input_import"
                    or artifact.pipeline_input_import_id is None
                ):
                    raise KeyError(item.artifact_id)
                imported = await db.get(PipelineInputImport, artifact.pipeline_input_import_id)
                frozen = stage.resolved_execution_spec_json or {}
                ordinary_authorized = (
                    run.submission_policy == "ordinary"
                    and imported is not None
                    and imported.recipe_digest == frozen.get("recipe_digest")
                )
                if (
                    imported is None
                    or imported.state != "committed"
                    or imported.trust_class != "internal_trusted"
                    or not ordinary_authorized
                ):
                    raise KeyError(item.artifact_id)
            root = cast(dict[str, Any], upload.canonical_manifest_json)
            record = next(
                (
                    value
                    for value in root.get("artifacts", [])
                    if value["artifact_id"] == str(artifact.id)
                ),
                None,
            )
            if record is None:
                raise ArtifactCommitError("input_descriptor_drift")
            from loom.pipeline.artifact_commit import ArtifactManifestV1, StoredFileV1

            files = [
                StoredFileV1.model_validate_json(canonical_document(value))
                for value in record["stored_files"]
            ]
            item_manifest = ArtifactManifestV1(
                artifact_id=artifact.id,
                artifact_name=artifact.name,
                artifact_type=artifact.artifact_type,
                content_sha256=artifact.content_hash,
                stored_size_bytes=artifact.stored_size_bytes,
                unpacked_size_bytes=artifact.unpacked_size_bytes,
                file_count=artifact.file_count,
                stored_files=files,
                lineage_artifact_ids=[UUID(value) for value in root["input_lineage_artifact_ids"]],
                lineage_digests=root["input_lineage_digests"],
            )
            manifest_bytes = canonical_document(item_manifest)
            if digest_bytes(manifest_bytes) != artifact.manifest_sha256:
                raise ArtifactCommitError("input_descriptor_drift")
            prefix = upload.prefix
            manifest_digest = cast(str, upload.manifest_sha256)
            marker_digest = cast(str, upload.committed_marker_sha256)
        marker_valid = await self._object_matches(
            key=prefix + "_manifest.json", expected_digest=manifest_digest
        ) and await self._object_matches(key=prefix + "_COMMITTED", expected_digest=marker_digest)
        return ResolvedArtifactInput(
            artifact_id=artifact.id,
            manifest_bytes=manifest_bytes,
            manifest_sha256=artifact.manifest_sha256,
            root_marker_valid=marker_valid,
            files=tuple(
                ResolvedStoredFile(
                    file_index=value.file_index,
                    storage_key=f"{prefix}artifacts/{artifact.id}/{value.relative_path}",
                    media_type=value.media_type,
                    size_bytes=value.size_bytes,
                    sha256=value.sha256,
                )
                for value in files
            ),
        )


__all__ = [
    "CheckpointRouteService",
    "ExecutionAttemptCompletionService",
    "FinalOutputRouteService",
    "SqlArtifactCommitRepository",
    "SqlArtifactInputResolver",
]

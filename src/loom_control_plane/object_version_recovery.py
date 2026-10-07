"""Bounded adoption of verified surviving versions for historical native output.

Storage verification is read-only and finishes before the database fence. This
does not reconstruct a write receipt, replay execution, or authorize retention GC.
"""

from __future__ import annotations

import asyncio
import copy
import hashlib
import json
import re
import time
from dataclasses import dataclass
from typing import Annotated, Any
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator
from sqlalchemy import inspect, select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from loom.data_lifecycle_registry import RuntimeLifecycleScope
from loom.db.schema import (
    AdminAuditEvent,
    Artifact,
    ArtifactUploadSession,
    DataLifecycleAuthority,
    DataLifecycleGcAuthority,
    DataLifecycleGcItem,
    DataLifecycleObject,
    ServiceExecutionLease,
    Trial,
)
from loom.execution_runtime_contract import ExecutionRuntimePlanV1
from loom.pipeline.artifact_commit import (
    ArtifactCommitManifestV1,
    ArtifactCommitMarkerV1,
    ArtifactManifestV1,
    ServiceExecutionOutputProducerV1,
)
from loom.pipeline.keys import canonical_digest, canonical_document

ACTION = "object_version_recovery"
MAX_BYTES = 256 * 1024 * 1024
MAX_METADATA_BYTES = 4 * 1024 * 1024
VERIFICATION_SECONDS = 90
Digest = Annotated[str, Field(pattern=r"^sha256:[0-9a-f]{64}$")]
VersionId = Annotated[str, Field(min_length=1, max_length=1024)]


class RecoveryConflictError(Exception):
    """A safe, public reason code, never SDK/SQL exception text."""


class ObjectVersion(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    registry_id: UUID
    version_id: VersionId
    equivalent_version_ids: list[VersionId] | None = Field(default=None, min_length=2, max_length=8)

    @field_validator("registry_id", mode="before")
    @classmethod
    def parse_uuid(cls, value: object) -> object:
        return UUID(value) if isinstance(value, str) else value

    @field_validator("version_id")
    @classmethod
    def valid_version(cls, value: str) -> str:
        if value != value.strip() or value.lower() == "null" or any(ord(c) < 32 for c in value):
            raise ValueError("a concrete version ID is required")
        return value

    @field_validator("equivalent_version_ids")
    @classmethod
    def valid_equivalents(cls, value: list[str] | None) -> list[str] | None:
        if value is not None:
            for version in value:
                cls.valid_version(version)
            if len(set(value)) != len(value):
                raise ValueError("equivalent version IDs must be unique")
            return sorted(value)
        return None

    @model_validator(mode="after")
    def selected_version_in_inventory(self) -> ObjectVersion:
        if self.equivalent_version_ids is not None and self.version_id not in self.equivalent_version_ids:
            raise ValueError("the selected version must be in the complete equivalent inventory")
        return self


class RecoveryRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    operation_id: UUID
    trial_id: UUID
    artifact_id: UUID
    expected_storage_sha256: Digest
    expected_index_sha256: Digest
    objects: list[ObjectVersion] = Field(min_length=1, max_length=32)
    apply: bool = False
    plan_sha256: Digest | None = None

    @field_validator("operation_id", "trial_id", "artifact_id", mode="before")
    @classmethod
    def parse_uuid(cls, value: object) -> object:
        return UUID(value) if isinstance(value, str) else value

    @model_validator(mode="after")
    def validate_request(self) -> RecoveryRequest:
        if len({item.registry_id for item in self.objects}) != len(self.objects):
            raise ValueError("registry IDs must be unique")
        if self.apply and self.plan_sha256 is None:
            raise ValueError("apply requires a verified preview plan digest")
        return self

    def identity(self) -> dict[str, Any]:
        # Preserve pre-extension request digests for existing audited operations.
        result = self.model_dump(mode="json", exclude={"apply", "plan_sha256"}, exclude_none=True)
        result["objects"] = sorted(result["objects"], key=lambda item: item["registry_id"])
        return result


def metadata_digest(value: Any) -> str:
    """Hash UTF-8 sorted compact JSON; no trailing newline or ASCII escaping."""
    return "sha256:" + hashlib.sha256(json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False,
    ).encode()).hexdigest()


def _row_digest(row: Any) -> str:
    values = {attr.key: getattr(row, attr.key) for attr in inspect(type(row)).column_attrs}
    # UUID/timestamp/numeric DB scalars are normalized only for internal snapshots.
    return metadata_digest(json.loads(json.dumps(values, default=str)))


def _require(condition: bool, reason: str) -> None:
    if not condition:
        raise RecoveryConflictError(reason)


@dataclass
class RecoveryState:
    trial: Trial
    artifact: Artifact
    lease: ServiceExecutionLease
    upload: ArtifactUploadSession
    authority: DataLifecycleAuthority
    objects: list[DataLifecycleObject]

    def digest(self) -> str:
        return metadata_digest([_row_digest(row) for row in (
            self.trial, self.artifact, self.lease, self.upload, self.authority, *self.objects,
        )])


def _validate_source(state: RecoveryState) -> None:
    """Validate retained commit identity without reopening the expiring spool."""
    lease, upload, artifact = state.lease, state.upload, state.artifact
    _require(len(json.dumps([lease.runtime_contract_json, upload.canonical_manifest_json,
        artifact.storage, state.trial.trajectory_index]).encode()) <= MAX_METADATA_BYTES,
        "metadata_limit_exceeded")
    try:
        runtime = ExecutionRuntimePlanV1.model_validate(lease.runtime_contract_json)
        manifest = ArtifactCommitManifestV1.model_validate_json(canonical_document(upload.canonical_manifest_json))
        producer = ServiceExecutionOutputProducerV1.model_validate_json(canonical_document(manifest.producer_identity))
    except ValueError:
        raise RecoveryConflictError("source_identity_invalid") from None
    _require(canonical_digest(lease.runtime_contract_json) == lease.runtime_contract_sha256
        and runtime.execution_role == "attempt"
        and manifest.commit_kind == "service_execution_output" and manifest.session_id == upload.id
        and canonical_digest(manifest) == upload.manifest_sha256
        and manifest.request_digest == upload.request_digest
        and manifest.total_bytes == upload.actual_total_bytes
        and len(manifest.artifacts) == 1, "source_manifest_conflict")
    _require(producer.team_id == upload.team_id and producer.service_execution_lease_id == lease.id
        and producer.service_execution_generation == upload.service_execution_generation
        and producer.service_execution_role == upload.service_execution_role
        and producer.runtime_contract_sha256 == lease.runtime_contract_sha256
        and producer.candidate_sha == upload.service_execution_candidate_sha == runtime.candidate_sha
        and producer.task_revision_sha256 == upload.service_execution_task_revision_sha256 == runtime.task_revision_sha256
        and producer.command_identity_sha256 == upload.service_execution_command_identity_sha256 == runtime.command_identity_sha256
        and producer.input_lineage_artifact_ids == manifest.input_lineage_artifact_ids
        and producer.input_lineage_digests == manifest.input_lineage_digests, "source_producer_conflict")
    record = manifest.artifacts[0]
    size = sum(item.size_bytes for item in record.stored_files)
    _require(record.artifact_id == artifact.id and record.artifact_name == artifact.name
        and record.artifact_type == artifact.artifact_type and record.content_sha256 == artifact.content_hash
        and record.manifest_sha256 == artifact.manifest_sha256
        and size == manifest.total_bytes == artifact.stored_size_bytes == artifact.unpacked_size_bytes
        and len(record.stored_files) == artifact.file_count, "source_artifact_conflict")
    try:
        item_manifest = ArtifactManifestV1(artifact_id=record.artifact_id, artifact_name=record.artifact_name,
            artifact_type=record.artifact_type, content_sha256=record.content_sha256,
            stored_size_bytes=size, unpacked_size_bytes=size, file_count=len(record.stored_files),
            stored_files=record.stored_files, lineage_artifact_ids=manifest.input_lineage_artifact_ids,
            lineage_digests=manifest.input_lineage_digests)
    except ValueError:
        raise RecoveryConflictError("source_artifact_invalid") from None
    _require(canonical_digest(item_manifest) == artifact.manifest_sha256
        and canonical_digest(ArtifactCommitMarkerV1(commit_kind="service_execution_output",
            manifest_sha256=canonical_digest(manifest), session_id=upload.id)) == upload.committed_marker_sha256,
        "source_manifest_digest_conflict")


async def _reject_detached_gc_claims(session: AsyncSession, state: RecoveryState) -> None:
    # 0071's physical-key snapshots survive registry removal. Older resume plans
    # retain these same identities in run.inventory instead. Both can still
    # authorize deletion under a different registry UUID or namespace.
    claimed = await session.scalar(text("""
        WITH requested AS (
            SELECT * FROM jsonb_to_recordset(CAST(:objects AS jsonb)) AS item(bucket text, object_key text)
        )
        SELECT EXISTS (
            SELECT 1 FROM data_lifecycle_gc_items AS item
            WHERE item.authority_id = :authority_id OR EXISTS (
                SELECT 1 FROM requested WHERE requested.bucket = item.bucket AND requested.object_key = item.object_key
            )
        ) OR EXISTS (
            SELECT 1 FROM data_lifecycle_gc_runs AS run WHERE NOT run.dry_run AND (
                run.inventory->'authority_ids' @> CAST(:authority_ids AS jsonb) OR EXISTS (
                    SELECT 1 FROM jsonb_array_elements(CASE
                        WHEN jsonb_typeof(run.inventory->'objects') = 'array' THEN run.inventory->'objects'
                        ELSE '[]'::jsonb END) AS item, requested
                    WHERE item->>'bucket' = requested.bucket AND item->>'object_key' = requested.object_key
                )
            )
        )
    """), {"objects": json.dumps([{"bucket": obj.bucket, "object_key": obj.object_key} for obj in state.objects]),
           "authority_id": state.authority.id, "authority_ids": json.dumps([str(state.authority.id)])})
    _require(not claimed, "physical_object_gc_claim")


async def _load(
    session: AsyncSession, request: RecoveryRequest, *, locked: bool,
) -> RecoveryState:
    # Lifecycle/GC table locks prevent a conflicting version or GC journal INSERT
    # between the final predicate check and commit. Row locks alone cannot do so.
    if locked:
        await session.execute(text("SET LOCAL lock_timeout = '2s'"))
        await session.execute(text("SET LOCAL statement_timeout = '5s'"))
        await session.execute(text(
            "LOCK TABLE data_lifecycle_authorities, data_lifecycle_objects, "
            "data_lifecycle_gc_items, data_lifecycle_gc_authorities, data_lifecycle_gc_runs, admin_audit_events "
            "IN SHARE ROW EXCLUSIVE MODE"
        ))
    artifact = await session.get(Artifact, request.artifact_id, with_for_update=locked)
    _require(artifact is not None, "artifact_missing")
    assert artifact is not None
    _require(artifact.control_producer_id is not None, "native_producer_required")
    lease = await session.get(ServiceExecutionLease, artifact.control_producer_id, with_for_update=locked)
    trial = await session.get(Trial, request.trial_id, with_for_update=locked)
    upload = await session.get(ArtifactUploadSession, artifact.artifact_upload_session_id, with_for_update=locked)
    authority = await session.get(DataLifecycleAuthority, artifact.lifecycle_authority_id, with_for_update=locked)
    _require(all(row is not None for row in (lease, trial, upload, authority)), "owner_missing")
    assert lease is not None and trial is not None and upload is not None and authority is not None
    ids = [item.registry_id for item in request.objects]
    objects = list((await session.scalars(select(DataLifecycleObject).where(
        DataLifecycleObject.id.in_(ids),
    ).order_by(DataLifecycleObject.id))).all())
    _require(len(objects) == len(ids), "registry_object_missing")
    state = RecoveryState(trial, artifact, lease, upload, authority, objects)
    scope = RuntimeLifecycleScope.from_environ()
    _require(
        artifact.trial_id == trial.id == lease.trial_id
        and artifact.team_id == trial.team_id == lease.team_id == upload.team_id == authority.team_id
        and artifact.control_producer_kind == "service_execution"
        and artifact.producer_kind is None
        and artifact.artifact_type == "loom.trial-artifact-bundle.v1"
        and trial.state in {"succeeded", "failed", "cancelled"}
        and trial.attempt_count == lease.attempt
        and lease.execution_role == "attempt"
        and lease.output_commit_state == lease.materialization_state == "committed"
        and (artifact.artifact_metadata or {}).get("materialization_state") == "committed"
        and lease.output_upload_session_id == upload.id
        and upload.commit_kind == "service_execution_output" and upload.state == "committed"
        and upload.service_execution_lease_id == lease.id
        and upload.service_execution_role == "attempt"
        and upload.service_execution_generation == lease.output_generation
        and upload.service_execution_runtime_contract_sha256 == lease.runtime_contract_sha256
        and upload.manifest_sha256 == lease.output_manifest_sha256
        and upload.committed_marker_sha256 == lease.output_marker_sha256,
        "native_owner_or_commit_conflict",
    )
    _validate_source(state)
    _require(
        authority.environment == scope.environment and authority.namespace == scope.namespace
        and authority.owner_kind == authority.data_class == "artifact"
        and authority.owner_id == str(artifact.id)
        and authority.state == "active" and authority.pinned and authority.expires_at is None
        and authority.deletion_token is None,
        "lifecycle_authority_conflict",
    )
    _require(await session.scalar(select(DataLifecycleGcAuthority.authority_id).where(
        DataLifecycleGcAuthority.authority_id == authority.id,
    ).limit(1)) is None, "authority_gc_claim")
    _require(await session.scalar(select(DataLifecycleGcItem.object_id).where(
        DataLifecycleGcItem.object_id.in_(ids),
    ).limit(1)) is None, "object_gc_claim")
    await _reject_detached_gc_claims(session, state)
    for obj in objects:
        _require(obj.authority_id == authority.id
            and obj.environment == scope.environment and obj.namespace == scope.namespace
            and obj.state == "active" and obj.deletion_token is None and obj.verified_deleted_at is None,
            "registry_owner_or_state_conflict")
        # A physical key cannot simultaneously be owned in a second namespace,
        # environment or authority; historical inconsistent rows must fail closed.
        _require(await session.scalar(select(DataLifecycleObject.id).where(
            DataLifecycleObject.bucket == obj.bucket,
            DataLifecycleObject.object_key == obj.object_key,
            DataLifecycleObject.id != obj.id,
        ).limit(1)) is None, "competing_registry_object")
    _require(sum(obj.size_bytes for obj in objects) <= MAX_BYTES, "byte_limit_exceeded")
    return state


def _plan(
    state: RecoveryState, request: RecoveryRequest, *, artifacts_bucket: str, trajectories_bucket: str,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    trial, artifact, lease = state.trial, state.artifact, state.lease
    storage, index = copy.deepcopy(artifact.storage), copy.deepcopy(trial.trajectory_index)
    _require(isinstance(storage, dict) and isinstance(index, dict), "metadata_invalid")
    assert isinstance(index, dict)
    _require(len(json.dumps([storage, index]).encode()) <= MAX_METADATA_BYTES, "metadata_limit_exceeded")
    _require(metadata_digest(storage) == request.expected_storage_sha256
        and metadata_digest(index) == request.expected_index_sha256, "published_metadata_drift")
    # Legacy schema-1 indexes omitted this redundant field. The retained lease,
    # canonical storage and every exact object path still bind the attempt below.
    # Preserve the omission; an explicit value must never be inferred or coerced.
    index_attempt = index.get("attempt")
    index_attempt_matches = (
        type(index_attempt) is int and index_attempt == lease.attempt
        if "attempt" in index else index.get("schema_version") == "1"
    )
    _require(storage.get("schema_version") == "loom.canonical-trial-bundle-storage.v1"
        and type(storage.get("attempt")) is int
        and storage.get("attempt") == lease.attempt and index_attempt_matches
        and storage.get("source_upload_session_id") == str(state.upload.id)
        and index.get("trial_id") == str(trial.id) and index.get("team_id") == str(trial.team_id)
        and index.get("task_id") == trial.task_id, "published_owner_conflict")
    prefix = f"trials/{trial.team_id}/{trial.id}/attempts/{lease.attempt}/bundles/{artifact.id}/"
    # Retain actual dict/field references on detached deep copies. Every mirror
    # is validated before any one of them changes.
    references: dict[tuple[str, str], list[tuple[dict[str, Any], str, str, str, int]]] = {}

    def add(bucket: Any, key: Any, row: dict[str, Any], field: str, location: str,
            sha256: Any, size: Any) -> None:
        _require(isinstance(bucket, str) and isinstance(key, str)
            and isinstance(sha256, str) and re.fullmatch(r"[0-9a-f]{64}", sha256) is not None
            and type(size) is int and size >= 0, "published_reference_invalid")
        references.setdefault((bucket, key), []).append((row, field, location, sha256, size))

    files = storage.get("files")
    evidence = storage.get("source_evidence")
    mirrors = index.get("artifacts")
    _require(all(isinstance(rows, list) for rows in (files, evidence, mirrors)), "published_inventory_invalid")
    assert isinstance(files, list) and isinstance(evidence, list) and isinstance(mirrors, list)
    _require(not mirrors or mirrors == files, "published_mirror_conflict")
    for location, rows in (("artifact.storage.files", files),
                           ("artifact.storage.source_evidence", evidence),
                           ("trial.trajectory_index.artifacts", mirrors)):
        identities: set[tuple[str, str]] = set()
        for ordinal, row in enumerate(rows):
            _require(isinstance(row, dict), "published_reference_invalid")
            key, bucket = row.get("key"), row.get("bucket")
            _require(bucket == artifacts_bucket and isinstance(key, str)
                and key.startswith(prefix) and len(key) > len(prefix), "published_key_conflict")
            _require((bucket, key) not in identities, "duplicate_published_reference")
            identities.add((bucket, key))
            sha = row.get("sha256")
            add(bucket, key, row, "version_id", f"{location}[{ordinal}].version_id",
                sha.removeprefix("sha256:") if isinstance(sha, str) else sha, row.get("size_bytes"))
    for name, filename, canonical_sha in (
        ("trajectory", "events.jsonl", lease.canonical_trajectory_sha256),
        ("atif", "atif.json", lease.canonical_atif_sha256),
    ):
        key = f"{trial.team_id}/{trial.id}/attempts/{lease.attempt}/{filename}"
        sha = index.get(f"{name}_sha256")
        _require(index.get(f"{name}_uri") == f"s3://{trajectories_bucket}/{key}"
            and canonical_sha == f"sha256:{sha}", "trajectory_identity_conflict")
        add(trajectories_bucket, key, index, f"{name}_version_id",
            f"trial.trajectory_index.{name}_version_id", sha, index.get(f"{name}_size_bytes"))
    proposed = {item.registry_id: item for item in request.objects}
    # The storage budget charges every copy, including versions not adopted.
    _require(sum(obj.size_bytes * len(proposed[obj.id].equivalent_version_ids or [proposed[obj.id].version_id])
        for obj in state.objects) <= MAX_BYTES, "byte_limit_exceeded")
    changes = []
    for obj in state.objects:
        refs = references.get((obj.bucket, obj.object_key), [])
        _require(bool(refs) and obj.version_id is None, "object_not_unversioned_publication")
        for row, field, _, sha, size in refs:
            _require(row.get(field) is None and sha == obj.content_sha256 and size == obj.size_bytes,
                "published_registry_conflict")
        item = proposed[obj.id]
        version = item.version_id
        before_fields = [{"location": location, "present": field in row, "value": row.get(field)}
                         for row, field, location, _, _ in refs]
        for row, field, _, _, _ in refs:
            row[field] = version
        changes.append({"registry_id": str(obj.id), "authority_id": str(obj.authority_id),
            "bucket": obj.bucket, "key": obj.object_key, "version_id": version,
            "sha256": obj.content_sha256, "size_bytes": obj.size_bytes,
            "locations": [ref[2] for ref in refs], "before_version_fields": before_fields})
        if item.equivalent_version_ids is not None:
            changes[-1]["equivalent_version_ids"] = item.equivalent_version_ids
    plan = {"schema_version": "loom.object-version-recovery.v1", "request": request.identity(),
        "before_state_sha256": state.digest(), "objects": changes,
        "after_storage_sha256": metadata_digest(storage), "after_index_sha256": metadata_digest(index),
        "evidence_kind": "verified_surviving_version"}
    return plan, storage, index


def _verify_objects(client: Any, plan: dict[str, Any]) -> None:
    deadline = time.monotonic() + VERIFICATION_SECONDS

    def inventory(obj: dict[str, Any]) -> None:
        # Prefix is only a listing filter, never an exact-key identity check.
        # Bounded complete pagination rejects pathological buckets safely.
        args = {"Bucket": obj["bucket"], "Prefix": obj["key"], "MaxKeys": 1000}
        found: list[dict[str, Any]] = []
        expected = obj.get("equivalent_version_ids", [obj["version_id"]])
        seen_markers: set[tuple[str, str]] = set()
        for _ in range(16):
            _require(time.monotonic() < deadline, "verification_timeout")
            page = client.list_object_versions(**args)
            _require(type(page.get("IsTruncated")) is bool, "incomplete_version_inventory")
            _require(not any(row.get("Key") == obj["key"] for row in page.get("DeleteMarkers", [])),
                "stored_delete_marker")
            found.extend(row for row in page.get("Versions", []) if row.get("Key") == obj["key"])
            _require(len(found) <= len(expected), "ambiguous_stored_versions")
            if page["IsTruncated"] is False:
                break
            marker = (page.get("NextKeyMarker"), page.get("NextVersionIdMarker"))
            _require(all(isinstance(part, str) and part for part in marker)
                and marker not in seen_markers, "incomplete_version_inventory")
            seen_markers.add(marker)
            args.update(KeyMarker=marker[0], VersionIdMarker=marker[1])
        else:
            raise RecoveryConflictError("version_inventory_limit_exceeded")
        _require(len(found) == len(expected)
            and {row.get("VersionId") for row in found} == set(expected)
            and all(type(row.get("Size")) is int and row["Size"] == obj["size_bytes"]
                and type(row.get("IsLatest")) is bool
                and row["IsLatest"] == (row.get("VersionId") == obj["version_id"]) for row in found),
            "stored_version_conflict")

    try:
        for obj in plan["objects"]:
            inventory(obj)
            for version in obj.get("equivalent_version_ids", [obj["version_id"]]):
                _require(time.monotonic() < deadline, "verification_timeout")
                response = client.get_object(Bucket=obj["bucket"], Key=obj["key"], VersionId=version)
                body = response["Body"]
                try:
                    _require(response.get("VersionId") == version
                        and type(response.get("ContentLength")) is int
                        and response["ContentLength"] == obj["size_bytes"]
                        and not response.get("DeleteMarker"), "stored_receipt_conflict")
                    size, digest = 0, hashlib.sha256()
                    while True:
                        _require(time.monotonic() < deadline, "verification_timeout")
                        chunk = body.read(min(1024 * 1024, obj["size_bytes"] - size + 1))
                        if not chunk:
                            break
                        size += len(chunk)
                        _require(size <= obj["size_bytes"], "stored_content_conflict")
                        digest.update(chunk)
                    _require(size == obj["size_bytes"] and digest.hexdigest() == obj["sha256"],
                        "stored_content_conflict")
                finally:
                    body.close()
        # Recheck every key after all reads, including early keys that could have
        # changed while a later object's payload was being streamed.
        for obj in plan["objects"]:
            inventory(obj)
    except RecoveryConflictError:
        raise
    except Exception:
        raise RecoveryConflictError("storage_verification_failed") from None


class ObjectVersionRecovery:
    def __init__(self) -> None:
        self._verification_slot = asyncio.Semaphore(1)

    async def _verify(self, client: Any, plan: dict[str, Any]) -> None:
        # Cancellation abandons read-only storage work, never database work. Keep
        # the slot occupied until that worker actually exits to bound late reads.
        async with asyncio.timeout(VERIFICATION_SECONDS):
            await self._verification_slot.acquire()
            future = asyncio.get_running_loop().run_in_executor(None, _verify_objects, client, plan)

            def finished(result: asyncio.Future[None]) -> None:
                self._verification_slot.release()
                if not result.cancelled():
                    result.exception()

            future.add_done_callback(finished)
            await asyncio.shield(future)

    async def recover(
        self, request: RecoveryRequest, *, sessions: async_sessionmaker[AsyncSession],
        client: Any, actor: str, artifacts_bucket: str, trajectories_bucket: str,
    ) -> dict[str, Any]:
        request_digest = metadata_digest(request.identity())
        async with asyncio.timeout(15), sessions() as session:
            await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"))
            await session.execute(text("SET LOCAL statement_timeout = '5s'"))
            state = await _load(session, request, locked=False)
            audit = await session.get(AdminAuditEvent, request.operation_id)
            if audit is not None:
                return self._replay(audit, request, state, request_digest)
            plan, _, _ = _plan(state, request, artifacts_bucket=artifacts_bucket,
                               trajectories_bucket=trajectories_bucket)
        plan_digest = metadata_digest(plan)
        if request.plan_sha256 is not None:
            _require(request.plan_sha256 == plan_digest, "preview_plan_drift")
        await self._verify(client, plan)
        if not request.apply:
            return {"status": "preview", "plan_sha256": plan_digest, "plan": plan}
        async with asyncio.timeout(15), sessions() as session, session.begin():
            # READ COMMITTED after waiting on the locks observes earlier winners.
            await session.execute(text("SET TRANSACTION ISOLATION LEVEL READ COMMITTED"))
            state = await _load(session, request, locked=True)
            audit = await session.get(AdminAuditEvent, request.operation_id)
            if audit is not None:
                return self._replay(audit, request, state, request_digest)
            current_plan, storage, index = _plan(state, request, artifacts_bucket=artifacts_bucket,
                                                 trajectories_bucket=trajectories_bucket)
            _require(metadata_digest(current_plan) == plan_digest, "state_changed_during_verification")
            state.artifact.storage, state.trial.trajectory_index = storage, index
            versions = {item.registry_id: item.version_id for item in request.objects}
            for obj in state.objects:
                obj.version_id = versions[obj.id]
            await session.flush()
            session.add(AdminAuditEvent(id=request.operation_id, actor=actor, action=ACTION,
                target_type="artifact", target_id=str(request.artifact_id),
                request_id=str(request.operation_id), event_metadata={
                    "request_sha256": request_digest, "plan_sha256": plan_digest, "plan": plan,
                    "after_state_sha256": state.digest(),
                }))
        return {"status": "applied", "operation_id": str(request.operation_id),
                "plan_sha256": plan_digest, "plan": plan}

    @staticmethod
    def _replay(audit: AdminAuditEvent, request: RecoveryRequest, state: RecoveryState,
                request_digest: str) -> dict[str, Any]:
        metadata = audit.event_metadata
        _require(request.apply and audit.action == ACTION and audit.target_type == "artifact"
            and audit.target_id == str(request.artifact_id)
            and metadata.get("request_sha256") == request_digest
            and metadata.get("plan_sha256") == request.plan_sha256, "operation_id_conflict")
        _require(metadata.get("after_state_sha256") == state.digest(), "replay_state_drift")
        return {"status": "replayed", "operation_id": str(request.operation_id),
                "plan_sha256": metadata["plan_sha256"], "plan": metadata["plan"]}

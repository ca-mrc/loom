"""Owner build intent and exact replay, before shared-pool dispatch.

Only verified source can create a queued intent; only completed, cleanup-qualified
attempts yield releases. No network call, capacity reservation or Kubernetes write.
"""
from __future__ import annotations

import re
from uuid import UUID, uuid4

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from loom.application_image_build import (
    ApplicationImageBuildBindingV1,
    ApplicationImageBuildClaimV1,
    ApplicationImageBuildStatusV1,
    ApplicationImagePublicationV1,
)
from loom.application_source_upload import ApplicationSourceUploadRequestV1
from loom.auth import AuthContext
from loom.db.nebius_application_build_schema import (
    NebiusApplicationBuild,
    NebiusApplicationBuildAttempt,
)
from loom.db.nebius_application_source_schema import NebiusApplicationSourceUpload
from loom.nebius_application_contract import ApplicationReleaseV1
from loom.nebius_pool_application_image import PoolApplicationImagePrepareV1
from loom.nebius_pool_contract import PoolReceiptV1
from loom.nebius_pool_native_runtime import PoolNativeRuntimeV1
from loom.pipeline.keys import canonical_digest
from loom_service.application_management.build_publication import qualify_publication
from loom_service.application_management.source_registry import ApplicationSourceRegistry
from loom_service.environment_management.registry import ManagementError, owner_identity


def _claim(build_id: UUID, attempt: int, source: NebiusApplicationSourceUpload,
           binding: ApplicationImageBuildBindingV1) -> ApplicationImageBuildClaimV1:
    return ApplicationImageBuildClaimV1(
        build_id=build_id, attempt=attempt, upload_id=source.upload_id,
        installation_id=source.installation_id, owner_user_id=source.owner_user_id,
        owner_team_id=source.owner_team_id, data_environment_id=source.data_environment_id,
        cluster_id=source.cluster_id, source=ApplicationSourceUploadRequestV1.model_validate(source, from_attributes=True),
        recipe=binding.recipe, storage_endpoint=binding.storage_endpoint, storage_region=binding.storage_region,
        source_bucket=source.source_bucket, cache_bucket=binding.cache_bucket, registry_repository=binding.registry_repository)


def retained_build_claim(row: NebiusApplicationBuild, attempt: NebiusApplicationBuildAttempt | None,
                         source: NebiusApplicationSourceUpload | None) -> tuple[
    ApplicationImageBuildBindingV1, ApplicationImageBuildClaimV1,
]:
    """One consistency check for owner status and shared-pool admission."""
    if attempt is None or source is None or source.phase != "source_verified":
        raise ValueError("application_build_history_conflict")
    binding = ApplicationImageBuildBindingV1.model_validate(row.binding_json)
    claim = ApplicationImageBuildClaimV1.model_validate(attempt.claim_json)
    if (canonical_digest(attempt.claim_json).removeprefix("sha256:") != attempt.claim_sha256
            or (attempt.build_id, attempt.attempt) != (row.build_id, row.current_attempt)
            or claim != _claim(row.build_id, row.current_attempt, source, binding)
            or (row.upload_id, row.owner_user_id, row.owner_team_id, row.installation_id, row.data_environment_id, row.cluster_id) != (
                source.upload_id, source.owner_user_id, source.owner_team_id, source.installation_id,
                source.data_environment_id, source.cluster_id)
            or (source.installation_id, source.data_environment_id, source.cluster_id, source.source_bucket) != (
                binding.source.installation_id, binding.source.data_environment_id,
                binding.source.cluster_id, binding.source.source_bucket)):
        raise ValueError("application_build_history_conflict")
    return binding, claim


def _ready_release(row: NebiusApplicationBuild, attempt: NebiusApplicationBuildAttempt,
                   claim: ApplicationImageBuildClaimV1) -> ApplicationReleaseV1:
    """Read immutable journal evidence; never turn Job completion alone into a release."""
    settlement = attempt.settlement_json
    if (attempt.phase != "ready" or row.desired_state != "running" or attempt.lease_token is not None
            or settlement is None or settlement.get("outcome") != "ready" or settlement.get("cause") != "completed"):
        raise ValueError("application_build_history_conflict")
    request = PoolApplicationImagePrepareV1.model_validate(attempt.pool_request_json)
    receipt = PoolReceiptV1.model_validate(attempt.terminal_receipt_json)
    observed = PoolNativeRuntimeV1.model_validate(settlement["runtime"]).receipt
    if (request.build != claim or canonical_digest(attempt.pool_request_json).removeprefix("sha256:") != attempt.pool_request_sha256
            or receipt.phase != "released" or receipt.pool_id != request.pool_id or receipt.request_key != request.key
            or receipt.admission_epoch != request.admission_epoch or receipt.request_sha256 != attempt.pool_request_sha256
            or any(getattr(receipt, field) != getattr(observed, field) for field in (
                "reservation_id", "pool_id", "request_key", "admission_epoch", "request_sha256", "plan_sha256", "job_uid"))):
        raise ValueError("application_build_history_conflict")
    publication = ApplicationImagePublicationV1.model_validate(settlement["publication"])
    qualify_publication(claim, publication)
    # Ready attempts are terminal and cannot be retried, so this identity cannot
    # later refer to different images. No parallel release table/catalog needed.
    return ApplicationReleaseV1(release_id=row.build_id, source_digest=publication.source_digest,
        schema_revision=publication.schema_revision, service_image_ref=publication.registry_images["service"],
        web_image_ref=publication.registry_images["web"])


class ApplicationBuildRegistry:
    def __init__(self, session_factory: async_sessionmaker[AsyncSession], *, binding: ApplicationImageBuildBindingV1):
        self.session_factory = session_factory
        self.binding = ApplicationImageBuildBindingV1.model_validate_json(binding.model_dump_json())
        self.sources = ApplicationSourceRegistry(session_factory, binding=self.binding.source)

    async def _view(self, session: AsyncSession, row: NebiusApplicationBuild) -> ApplicationImageBuildStatusV1:
        attempt = await session.get(NebiusApplicationBuildAttempt, (row.build_id, row.current_attempt))
        source = await session.get(NebiusApplicationSourceUpload, row.upload_id)
        try:
            _, claim = retained_build_claim(row, attempt, source)
            assert attempt is not None
            return ApplicationImageBuildStatusV1.model_validate({
                "build_id": row.build_id, "upload_id": row.upload_id, "attempt": row.current_attempt,
                "phase": attempt.phase, "desired_state": row.desired_state,
                "source_digest": claim.source.source_digest, "recipe_digest": claim.recipe.digest,
                "created_at": row.created_at,
                "release": _ready_release(row, attempt, claim) if attempt.phase == "ready" else None})
        except (ValueError, TypeError, KeyError):
            raise ManagementError("application_build_history_conflict") from None

    def _scope(self, principal: AuthContext, *, mutation: bool = False) -> tuple[UUID, UUID]:
        return owner_identity(principal, mutation=mutation)

    async def _owned(self, session: AsyncSession, build_id: UUID, principal: AuthContext, *,
                     mutation: bool = False, lock: bool = False) -> NebiusApplicationBuild:
        owner, team = self._scope(principal, mutation=mutation)
        query = select(NebiusApplicationBuild).where(
            NebiusApplicationBuild.build_id == build_id,
            NebiusApplicationBuild.owner_user_id == owner, NebiusApplicationBuild.owner_team_id == team,
            NebiusApplicationBuild.installation_id == self.binding.source.installation_id,
            NebiusApplicationBuild.data_environment_id == self.binding.source.data_environment_id,
            NebiusApplicationBuild.cluster_id == self.binding.source.cluster_id)
        row = await session.scalar(query.with_for_update() if lock else query)
        if row is None:
            raise ManagementError("application_build_forbidden", 403)
        return row

    async def create(self, *, principal: AuthContext, upload_id: UUID,
                     idempotency_key: str) -> ApplicationImageBuildStatusV1:
        owner, team = self._scope(principal, mutation=True)
        if not isinstance(upload_id, UUID) or not upload_id.int:
            raise ManagementError("invalid_application_source_id", 422)
        if re.fullmatch(r"[A-Za-z0-9._:-]{1,128}", idempotency_key) is None:
            raise ManagementError("invalid_idempotency_key", 422)
        fingerprint = canonical_digest({"upload_id": upload_id, "owner": owner, "team": team,
            "installation": self.binding.source.installation_id, "data": self.binding.source.data_environment_id,
            "cluster": self.binding.source.cluster_id}).removeprefix("sha256:")
        async with self.session_factory.begin() as session:
            query = select(NebiusApplicationBuild).where(NebiusApplicationBuild.owner_user_id == owner,
                NebiusApplicationBuild.idempotency_key == idempotency_key)
            existing = await session.scalar(query)
            if existing is not None:
                if existing.request_sha256 != fingerprint:
                    raise ManagementError("idempotency_conflict")
                return await self._view(session, await self._owned(session, existing.build_id, principal))
            source = await self.sources._owned(session, upload_id, principal, mutation=True, lock=True)
            if source.phase != "source_verified":
                raise ManagementError("application_source_not_verified")
            identity = uuid4()
            created = await session.scalar(insert(NebiusApplicationBuild).values(
                build_id=identity, upload_id=upload_id, owner_user_id=owner, owner_team_id=team,
                installation_id=source.installation_id, data_environment_id=source.data_environment_id,
                cluster_id=source.cluster_id, idempotency_key=idempotency_key, request_sha256=fingerprint,
                binding_json=self.binding.model_dump(mode="json"), current_attempt=1, desired_state="running",
            ).on_conflict_do_nothing(index_elements=["owner_user_id", "idempotency_key"]).returning(NebiusApplicationBuild.build_id))
            if created is not None:
                claim = _claim(identity, 1, source, self.binding).model_dump(mode="json")
                session.add(NebiusApplicationBuildAttempt(build_id=identity, attempt=1,
                    claim_json=claim, claim_sha256=canonical_digest(claim).removeprefix("sha256:"), phase="queued"))
                await session.flush()
            row = await session.scalar(query)
            if row is None or row.request_sha256 != fingerprint:
                raise ManagementError("idempotency_conflict")
            return await self._view(session, await self._owned(session, row.build_id, principal))

    async def status(self, build_id: UUID, *, principal: AuthContext) -> ApplicationImageBuildStatusV1:
        async with self.session_factory() as session:
            return await self._view(session, await self._owned(session, build_id, principal))

    async def release(self, build_id: UUID, *, principal: AuthContext) -> ApplicationReleaseV1:
        status = await self.status(build_id, principal=principal)
        if status.release is None:
            raise ManagementError("application_build_not_ready")
        return status.release

    @staticmethod
    def _attempt(value: int) -> None:
        if type(value) is not int or not 0 < value < 2**63:
            raise ManagementError("invalid_application_build_attempt", 422)

    async def cancel(self, build_id: UUID, *, principal: AuthContext, attempt: int) -> ApplicationImageBuildStatusV1:
        """Record owner intent only; the worker still owes pool cancellation/cleanup."""
        self._attempt(attempt)
        async with self.session_factory.begin() as session:
            row = await self._owned(session, build_id, principal, mutation=True, lock=True)
            if row.current_attempt != attempt:
                raise ManagementError("application_build_attempt_conflict")
            saved = await session.get(NebiusApplicationBuildAttempt, (build_id, attempt), with_for_update=True)
            if saved is None or saved.phase in {"ready", "failed"}:
                raise ManagementError("application_build_already_terminal")
            row.desired_state = "cancelled"
            await session.flush()
            return await self._view(session, row)

    async def retry(self, build_id: UUID, *, principal: AuthContext, attempt: int) -> ApplicationImageBuildStatusV1:
        """One explicit successor per expected generation, including lost-reply replay.

        The expected attempt is the retry key: repeating it cannot increment a
        second time. Source and recipe remain the original build's inputs.
        """
        self._attempt(attempt)
        async with self.session_factory.begin() as session:
            row = await self._owned(session, build_id, principal, mutation=True, lock=True)
            if row.current_attempt == attempt + 1:
                return await self._view(session, row)
            if row.current_attempt != attempt or attempt == 2**63 - 1:
                raise ManagementError("application_build_attempt_conflict")
            saved = await session.get(NebiusApplicationBuildAttempt, (build_id, attempt), with_for_update=True)
            if (saved is None or saved.phase not in {"failed", "cancelled"} or saved.lease_token is not None
                    or (saved.pool_request_json is not None and (saved.terminal_receipt_json is None
                        or saved.terminal_receipt_json.get("phase") not in {"released", "cancelled_unstarted"}))):
                raise ManagementError("application_build_cleanup_required")
            source = await session.get(NebiusApplicationSourceUpload, row.upload_id, with_for_update={"read": True})
            try:
                _, claim = retained_build_claim(row, saved, source)
            except (ValueError, TypeError):
                raise ManagementError("application_build_history_conflict") from None
            row.current_attempt, row.desired_state = attempt + 1, "running"
            await session.flush()  # SQL qualifies the previous terminal attempt.
            payload = claim.model_copy(update={"attempt": row.current_attempt}).model_dump(mode="json")
            session.add(NebiusApplicationBuildAttempt(build_id=build_id, attempt=row.current_attempt,
                claim_json=payload, claim_sha256=canonical_digest(payload).removeprefix("sha256:"), phase="queued"))
            await session.flush()
            return await self._view(session, row)

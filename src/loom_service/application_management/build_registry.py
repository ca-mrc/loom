"""Owner build intent and exact replay, before shared-pool dispatch.

Only verified source can create a queued intent. This registry performs no
network call, capacity reservation, Kubernetes write or release qualification.
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
)
from loom.application_source_upload import ApplicationSourceUploadRequestV1
from loom.auth import AuthContext
from loom.db.nebius_application_build_schema import (
    NebiusApplicationBuild,
    NebiusApplicationBuildAttempt,
)
from loom.db.nebius_application_source_schema import NebiusApplicationSourceUpload
from loom.pipeline.keys import canonical_digest
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
                "created_at": row.created_at})
        except (ValueError, TypeError):
            raise ManagementError("application_build_history_conflict") from None

    def _scope(self, principal: AuthContext, *, mutation: bool = False) -> tuple[UUID, UUID]:
        return owner_identity(principal, mutation=mutation)

    async def _owned(self, session: AsyncSession, build_id: UUID, principal: AuthContext) -> NebiusApplicationBuild:
        owner, team = self._scope(principal)
        row = await session.scalar(select(NebiusApplicationBuild).where(
            NebiusApplicationBuild.build_id == build_id,
            NebiusApplicationBuild.owner_user_id == owner, NebiusApplicationBuild.owner_team_id == team,
            NebiusApplicationBuild.installation_id == self.binding.source.installation_id,
            NebiusApplicationBuild.data_environment_id == self.binding.source.data_environment_id,
            NebiusApplicationBuild.cluster_id == self.binding.source.cluster_id))
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

"""Owner intent uses protected releases or completed owner builds, with frozen replay."""
from __future__ import annotations

from uuid import UUID, uuid4

from loom.auth import AuthContext
from loom.nebius_application_authority import ApplicationNamespaceAuthorityV1
from loom.nebius_application_contract import (
    ApplicationCreateRequestV1,
    ApplicationOperationRequestV1,
    ApplicationOperationV1,
    ApplicationRegistrationV1,
    ApplicationReleaseV1,
    SharedDevelopmentBindingV1,
    new_application_registration,
)
from loom.nebius_application_render import RenderedApplication, render_application
from loom.nebius_application_versions import ApplicationReleaseCompatibilityV1
from loom.nebius_environment_contract import FoundationBinding
from loom_service.application_management.build_registry import ApplicationBuildRegistry
from loom_service.application_management.registry import ApplicationRegistry
from loom_service.environment_management.registry import ManagementError, owner_identity


class ApplicationManager:
    def __init__(self, registry: ApplicationRegistry, *, foundation: FoundationBinding,
                 shared: SharedDevelopmentBindingV1, authority: ApplicationNamespaceAuthorityV1,
                 releases: tuple[ApplicationReleaseV1, ...], builds: ApplicationBuildRegistry | None = None):
        self.registry = registry
        self.foundation = FoundationBinding.model_validate(foundation.model_dump())
        self.shared = SharedDevelopmentBindingV1.model_validate(shared.model_dump())
        self.authority = ApplicationNamespaceAuthorityV1.model_validate(authority.model_dump())
        self.shared.validate_foundation(self.foundation)
        if (self.authority.cluster_id != self.shared.cluster_id
                or self.authority.data_environment_id != self.shared.data_environment_id
                or self.authority.shared_namespace != self.shared.platform_namespace):
            raise ValueError("application authority differs from shared binding")
        self._releases = {release.release_id: ApplicationReleaseV1.model_validate(release.model_dump()) for release in releases}
        if len(self._releases) != len(releases) or len(releases) > 1000:
            raise ValueError("invalid protected application release catalog")
        if builds is not None and (builds.binding.source.installation_id, builds.binding.source.data_environment_id,
                builds.binding.source.cluster_id) != (self.authority.installation_id, self.shared.data_environment_id,
                    self.shared.cluster_id):
            raise ValueError("application builder differs from shared authority")
        self.builds = builds

    async def _release(self, release_id: UUID, principal: AuthContext) -> ApplicationReleaseV1:
        release = self._releases.get(release_id)
        if release is None and self.builds is not None:
            release = await self.builds.release(release_id, principal=principal)
        if release is None:
            raise ManagementError("application_release_unavailable", 404)
        return release

    async def check_release(self, principal: AuthContext, release_id: UUID) -> ApplicationReleaseCompatibilityV1:
        owner_identity(principal)
        release = await self._release(release_id, principal)
        return ApplicationReleaseCompatibilityV1(release=release, shared_schema_revision=self.shared.schema_revision,
            compatibility="compatible" if release.schema_revision == self.shared.schema_revision else "schema_mismatch")

    async def _prepare(self, row: ApplicationRegistrationV1, principal: AuthContext) -> tuple[RenderedApplication, ApplicationReleaseV1]:
        release = await self._release(row.release_id, principal)
        if release.schema_revision != self.shared.schema_revision:
            raise ManagementError("application_schema_mismatch", 409, details={
                "release_schema_revision": release.schema_revision, "shared_schema_revision": self.shared.schema_revision})
        try:
            return render_application(row, release, self.shared, self.foundation, authority=self.authority), release
        except (ValueError, KeyError, TypeError):
            raise ManagementError("application_release_incompatible", 409) from None

    async def create(self, principal: AuthContext, request: ApplicationCreateRequestV1, *,
                     idempotency_key: str) -> ApplicationOperationV1:
        replay = await self.registry.replay_create(principal=principal, idempotency_key=idempotency_key,
                                                   slug=request.slug, release_id=request.release_id)
        if replay is not None:
            return replay
        owner, team = owner_identity(principal, mutation=True)
        row = new_application_registration(self.foundation, self.shared, application_id=uuid4(), incarnation=uuid4(),
            owner_user_id=owner, owner_team_id=team, slug=request.slug, release_id=request.release_id)
        prepared, release = await self._prepare(row, principal)
        return await self.registry.create(principal=principal, idempotency_key=idempotency_key,
                                         prepared=prepared, release=release, shared=self.shared)

    async def transition(self, principal: AuthContext, application_id: UUID,
                         request: ApplicationOperationRequestV1, *, idempotency_key: str) -> ApplicationOperationV1:
        replay = await self.registry.replay_transition(application_id, principal=principal,
            idempotency_key=idempotency_key, action=request.action, expected_generation=request.expected_generation,
            release_id=request.release_id)
        if replay is not None:
            return replay
        prepared, release = None, None
        if request.action in {"update", "resume"}:
            current = await self.registry.status(application_id, principal=principal, for_mutation=True)
            if current.registration.deployment_generation != request.expected_generation:
                # A peer may have committed this exact request after our first
                # replay read. Return that operation before declaring a conflict.
                replay = await self.registry.replay_transition(application_id, principal=principal,
                    idempotency_key=idempotency_key, action=request.action,
                    expected_generation=request.expected_generation, release_id=request.release_id)
                if replay is not None:
                    return replay
                raise ManagementError("application_generation_conflict")
            row = current.registration.model_copy(update={
                "deployment_generation": current.registration.deployment_generation + 1,
                "access_generation": current.registration.access_generation + 1,
                "desired_state": "active", "release_id": request.release_id or current.registration.release_id,
            })
            prepared, release = await self._prepare(row, principal)
        return await self.registry.transition(application_id, principal=principal, idempotency_key=idempotency_key,
            action=request.action, expected_generation=request.expected_generation, release_id=request.release_id,
            prepared=prepared, release=release, shared=self.shared if prepared is not None else None)

"""Qualify explicit installed builder authority before starting any owner work."""
from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import AsyncExitStack, asynccontextmanager

from loom.db.nebius_pool_schema import NebiusPoolBinding
from loom_execution_actuator.pool_client import PoolClient
from loom_execution_actuator.task_image_controller import NativeBuildKubernetesApi
from loom_execution_capacity_collector.control_plane import read_owner_only_secret
from loom_service.application_management.build_journal import ApplicationBuildJournal
from loom_service.application_management.build_worker import ApplicationBuildWorker
from loom_service.application_management.installation import ApplicationInstallation
from loom_service.application_management.manager import ApplicationManager
from loom_service.environment_management.kubernetes_credentials import create_projected_api_client
from loom_service.pool_management.auth import authorize_pool_machine, resolve_pool_machine
from loom_service.pool_management.locks import acquire_pool_mutation_lock
from loom_service.pool_management.observations import read_pool_registration
from loom_service.pool_management.registry import PoolProfiles


async def _qualify(installation: ApplicationInstallation, manager: ApplicationManager,
                   profiles: PoolProfiles | None, management_origin: str) -> None:
    settings = installation.runtime.build
    assert settings is not None
    binding = settings.binding
    installation.validate_foundation(manager.foundation)
    if (profiles is None or profiles.catalog_sha256 is None
            or settings.management_origin.rstrip("/") != management_origin.rstrip("/")):
        raise ValueError("unbound builder transport or catalog")
    profile = profiles.application_images[binding.profile_id]
    if (profile.recipe != binding.recipe or profile.target.target_id != binding.target_id
            or any(getattr(binding, name) != getattr(profile.settings, name) for name in (
                "storage_endpoint", "storage_region", "cache_bucket", "registry_repository"))
            or binding.source.source_bucket != profile.settings.source_bucket):
        raise ValueError("unbound builder profile")
    token = read_owner_only_secret(settings.bearer_token_file)
    async with manager.registry.session_factory.begin() as session:
        # Startup qualification is read-only. Common operations independently
        # reauthorize under these same locks, including after token revocation.
        await acquire_pool_mutation_lock(session)
        principal = await resolve_pool_machine(session, "Bearer " + token)
        if principal is None:
            raise ValueError("unregistered builder")
        principal = await authorize_pool_machine(session, principal, role="participant", pool_id=binding.pool_id,
            participant_id=binding.participant_id, workload_kind="application_image_build")
        if ((principal.installation_id, principal.environment_id, principal.pool_epoch,
             principal.participant_revision, principal.participant_phase) != (
                binding.source.installation_id, binding.source.data_environment_id, binding.admission_epoch,
                binding.participant_revision, "active") or principal.pool_mode not in {"closed", "global"}):
            raise ValueError("stale builder registration")
        pool = await session.get(NebiusPoolBinding, binding.pool_id)
        if (pool is None or pool.cluster_id != binding.source.cluster_id
                or pool.binding_json.get("profile_catalog_sha256") != profiles.catalog_sha256):
            raise ValueError("unbound builder catalog")
        participants, _ = await read_pool_registration(session, pool)
        participant = next(row for row in participants if row.participant_id == binding.participant_id)
        target = participant.target(binding.target_id, "application_image_build")
        if (participant.environment_class != "development" or target.profile_id != binding.profile_id
                or target.workload_kinds != ("application_image_build",)
                or profile.target.namespace != participant.build_namespace.name
                or profile.settings.namespace != participant.build_namespace.name
                or profile.target.node_selector != pool.binding_json["node_selector"]):
            raise ValueError("unbound builder target")


@asynccontextmanager
async def open_build_worker(installation: ApplicationInstallation, manager: ApplicationManager, *,
                            profiles: PoolProfiles | None, management_origin: str,
                            ) -> AsyncIterator[ApplicationBuildWorker | None]:
    settings = installation.runtime.build
    if settings is None:
        yield None
        return
    async with AsyncExitStack() as resources:
        try:
            await _qualify(installation, manager, profiles, management_origin)
            management = PoolClient(origin=settings.management_origin, bearer_token_file=settings.bearer_token_file,
                timeout_seconds=settings.timeout_seconds)
            resources.push_async_callback(management.close)
            api, credentials = create_projected_api_client(installation.runtime.kubernetes)
            kubernetes = NativeBuildKubernetesApi(api_client=api)
            resources.push_async_callback(credentials.close)
            resources.push_async_callback(kubernetes.close)
            await credentials.get_token()
            registry = manager.builds
            if registry is None or registry.binding != settings.binding:
                raise ValueError("unbound application release resolver")
            worker = ApplicationBuildWorker(ApplicationBuildJournal(registry), management, kubernetes)
        except Exception:
            # Authentication/SDK errors must not expose token bytes or config.
            raise ValueError("invalid_application_build_runtime") from None
        yield worker

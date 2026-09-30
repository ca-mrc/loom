"""Concrete application lifecycle coordination; no polling worker or owner API."""
from __future__ import annotations

from loom_service.application_management.credentials import ApplicationCredentialProvider
from loom_service.application_management.effects import ApplicationEffect
from loom_service.application_management.leases import ApplicationLease
from loom_service.application_management.object_access import ApplicationObjectAccessVerifier
from loom_service.application_management.proofs import (
    ApplicationReadyEvidence,
    ApplicationStartupPreparation,
    ApplicationStopEvidence,
)
from loom_service.application_management.registry import ApplicationRegistry
from loom_service.application_management.runtime import ApplicationRuntimeProvider
from loom_service.environment_management.provider import ProviderBlockedError


class ApplicationLifecycleCoordinator:
    def __init__(self, registry: ApplicationRegistry, runtime: ApplicationRuntimeProvider,
                 credentials: ApplicationCredentialProvider, object_verifier: ApplicationObjectAccessVerifier):
        self.registry, self.runtime = registry, runtime
        self.credentials, self.object_verifier = credentials, object_verifier

    async def prepare(self, lease: ApplicationLease) -> ApplicationStartupPreparation:
        """Prepare an active generation without opening admission or releasing it."""
        await self.runtime.resume_preparation(lease)
        await self.runtime.stop_workloads(lease)
        database = await self.credentials.retire_database(lease)
        objects = await self.credentials.retire_cloud(lease, self.object_verifier)
        await self.runtime.prepare_static(lease)
        await self.runtime.read_shared_network(lease)
        await self.credentials.deliver(lease, self.runtime.kubernetes)
        access = await self.credentials.qualify(lease, self.object_verifier)
        prepared = await self.runtime.read_prepared(lease)
        network = await self.runtime.read_shared_network(lease)
        workloads = await self.runtime.stop_workloads(lease)
        if await self.registry.activation_started(lease):
            raise ProviderBlockedError("application_activation_started")
        return ApplicationStartupPreparation(workloads=workloads, database=database, objects=objects,
            prepared=prepared, access=access, network=network)

    async def activate(self, lease: ApplicationLease) -> ApplicationEffect:
        """Refresh concrete prerequisites and open admission, without readiness."""
        opening = await self.runtime.activation_effect(lease)
        if opening is None:
            await self.prepare(lease)
        else:
            # Even a rejected opening permanently leaves preparation. A lost
            # reply may already have opened admission; never close it again.
            await self.credentials.retire_database(lease)
            await self.credentials.retire_cloud(lease, self.object_verifier)
        await self.credentials.qualify(lease, self.object_verifier)
        await self.runtime.read_prepared(lease)
        await self.runtime.read_shared_network(lease)
        return await self.runtime.open_admission(lease)

    async def start(self, lease: ApplicationLease) -> None:
        """Converge a qualified personal version and record current readiness."""
        await self.activate(lease)
        await self.runtime.start_workloads(lease)
        database = await self.credentials.retire_database(lease)
        objects = await self.credentials.retire_cloud(lease, self.object_verifier)
        access = await self.credentials.qualify(lease, self.object_verifier)
        prepared = await self.runtime.read_prepared(lease)
        network = await self.runtime.read_shared_network(lease)
        workloads = await self.runtime.read_ready(lease)
        await self.registry.complete_ready(lease, ApplicationReadyEvidence(workloads=workloads,
            database=database, objects=objects, access=access, prepared=prepared, network=network))

    async def stop(self, lease: ApplicationLease) -> None:
        plan = await self.registry.frozen_plan(lease)
        if plan["registration"]["desired_state"] not in {"suspended", "destroyed"}:
            raise ProviderBlockedError("application_stop_not_requested")
        await self.runtime.stop_workloads(lease)
        database = await self.credentials.retire_database(lease)
        objects = await self.credentials.retire_cloud(lease, self.object_verifier)
        # Provider calls may outlive the first process observation. Refresh live
        # namespace, fence, controllers and the unfiltered PodList before release.
        workloads = await self.runtime.stop_workloads(lease)
        await self.registry.complete_stopped(lease, ApplicationStopEvidence(
            workloads=workloads, database=database, objects=objects))

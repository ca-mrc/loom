"""Explicit shared adapters and owned personal-worker lifecycle in management."""
from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator
from contextlib import AsyncExitStack, asynccontextmanager
from pathlib import Path

import httpx
from psycopg.conninfo import conninfo_to_dict

from loom.nebius_kubernetes import NebiusKubernetesCredentials
from loom_service.application_management.build_runtime import open_build_worker
from loom_service.application_management.build_worker import ApplicationBuildWorker
from loom_service.application_management.cloud_provider import ApplicationCloudProvider
from loom_service.application_management.coordinator import ApplicationLifecycleCoordinator
from loom_service.application_management.credentials import ApplicationCredentialProvider
from loom_service.application_management.database import AsyncApplicationDatabaseAccess
from loom_service.application_management.installation import (
    ApplicationInstallation,
    read_protected_file,
)
from loom_service.application_management.kubernetes import ApplicationKubernetesProvider
from loom_service.application_management.login import ApplicationLogin
from loom_service.application_management.manager import ApplicationManager
from loom_service.application_management.object_access import ApplicationObjectAccessVerifier
from loom_service.application_management.runtime import ApplicationRuntimeProvider
from loom_service.application_management.source_runtime import open_source_uploader
from loom_service.application_management.source_upload import ApplicationSourceUploader
from loom_service.application_management.worker import ApplicationWorker
from loom_service.environment_management.kubernetes_credentials import (
    ProjectedKubernetesCredentials,
)
from loom_service.environment_management.nebius_api import NebiusSdkEnvironmentApi
from loom_service.environment_management.runtime import NebiusManagementAuth
from loom_service.pool_management.registry import PoolProfiles

_LOG = logging.getLogger(__name__)


class ApplicationServiceRuntime:
    def __init__(self, worker: ApplicationWorker, installation: ApplicationInstallation, login: ApplicationLogin,
                 source_uploader: ApplicationSourceUploader | None = None,
                 build_worker: ApplicationBuildWorker | None = None):
        self.worker = worker
        self.login = login
        self.source_uploader = source_uploader
        self.build_worker = build_worker
        self.build_task: asyncio.Task[None] | None = None
        if build_worker is not None:
            assert installation.runtime.build is not None
            self.build_task = asyncio.create_task(build_worker.run(concurrency=installation.runtime.build.concurrency,
                poll_seconds=installation.runtime.build.poll_seconds), name="loom-management-application-build-worker")
            self.build_task.add_done_callback(self._finished)
        self.kubernetes = worker.coordinator.runtime.kubernetes
        self.object_verifier = worker.coordinator.object_verifier
        self.task = asyncio.create_task(worker.run(concurrency=installation.runtime.concurrency,
            poll_seconds=installation.runtime.poll_seconds), name="loom-management-application-worker")
        self.task.add_done_callback(self._finished)

    @staticmethod
    def _finished(task: asyncio.Task[None]) -> None:
        if not task.cancelled() and task.exception() is not None:
            _LOG.error("application_worker_stopped")

    @property
    def ready(self) -> bool:
        return (not self.task.done() and self.worker.healthy and (self.build_worker is None or (
            self.build_task is not None and not self.build_task.done() and self.build_worker.healthy)))

    async def close(self) -> None:
        tasks = [self.task, *([self.build_task] if self.build_task is not None else [])]
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

    @classmethod
    @asynccontextmanager
    async def open(cls, installation: ApplicationInstallation, manager: ApplicationManager, *,
                   pool_profiles: PoolProfiles | None = None, management_origin: str = ""
                   ) -> AsyncIterator[ApplicationServiceRuntime]:
        from nebius.sdk import SDK

        dsn, shared_credentials = installation.load_credentials()
        settings = installation.runtime
        async with AsyncExitStack() as resources:
            try:
                read_protected_file(settings.cloud_credentials_file, limit=1024 * 1024)
                credentials = ProjectedKubernetesCredentials(settings.kubernetes)
                resources.push_async_callback(credentials.close)
                sdk = SDK(credentials_file_name=str(settings.cloud_credentials_file),
                          user_agent_prefix="loom-application-management/1.0")
                resources.push_async_callback(sdk.close)
                await credentials.get_token()
                NebiusKubernetesCredentials._usable(await sdk.get_token(timeout=30))
            except Exception:
                raise ValueError("invalid_application_provider_credentials") from None
            http = await resources.enter_async_context(httpx.AsyncClient(
                base_url=settings.kubernetes.endpoint, verify=credentials.ssl_context,
                auth=NebiusManagementAuth(settings.kubernetes, credentials),
                trust_env=False, timeout=30, follow_redirects=False,
            ))
            objects = await resources.enter_async_context(httpx.AsyncClient(
                base_url=manager.foundation.platform_config["storage_endpoint"],
                trust_env=False, timeout=30, follow_redirects=False,
            ))
            registry = manager.registry
            kubernetes = ApplicationKubernetesProvider(registry, http)
            provider = ApplicationRuntimeProvider(registry, kubernetes, authority=installation.authority)
            access = ApplicationCredentialProvider(registry, ApplicationCloudProvider(registry, NebiusSdkEnvironmentApi(sdk)),
                AsyncApplicationDatabaseAccess(dsn, installation.shared.data_environment_id),
                storage_binding=installation.storage.model_dump(mode="json"), shared=shared_credentials)
            coordinator = ApplicationLifecycleCoordinator(registry, provider, access, ApplicationObjectAccessVerifier(objects,
                foundation=manager.foundation, shared=installation.shared, storage=installation.storage))
            login = ApplicationLogin(registry, shared=installation.shared, credentials=shared_credentials,
                ca_file=Path(str(conninfo_to_dict(dsn)["sslrootcert"])))
            source_uploader = await resources.enter_async_context(open_source_uploader(installation, manager))
            build_worker = await resources.enter_async_context(open_build_worker(installation, manager,
                profiles=pool_profiles, management_origin=management_origin))
            runtime = cls(ApplicationWorker(registry, coordinator), installation, login, source_uploader, build_worker)
            resources.push_async_callback(runtime.close)  # Drain before HTTP, SDK or parent DB closes.
            yield runtime

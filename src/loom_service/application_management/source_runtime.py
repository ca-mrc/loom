"""Explicit source storage lifetime, derived from protected shared installation."""
from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import AsyncExitStack, asynccontextmanager

from loom.application_source_upload import ApplicationSourceUploadBindingV1
from loom.trajectory.storage import MinioObjectStore
from loom_service.application_management.installation import (
    ApplicationInstallation,
    read_protected_file,
)
from loom_service.application_management.manager import ApplicationManager
from loom_service.application_management.source_registry import ApplicationSourceRegistry
from loom_service.application_management.source_upload import ApplicationSourceUploader
from loom_service.environment_management.candidates import _json


@asynccontextmanager
async def open_source_uploader(installation: ApplicationInstallation, manager: ApplicationManager,
                               ) -> AsyncIterator[ApplicationSourceUploader | None]:
    settings = installation.runtime.source_upload
    if settings is None:
        yield None
        return
    async with AsyncExitStack() as resources:
        try:
            material = _json(read_protected_file(settings.credentials_file, limit=16384))
            if (set(material) != {"access-key", "secret-key"}
                    or any(not isinstance(value, str) or not 1 <= len(value) <= 4096
                           or not value.isascii() or any(ord(char) < 33 or ord(char) == 127 for char in value)
                           for value in material.values())):
                raise ValueError
            installation.validate_foundation(manager.foundation)
            config = manager.foundation.platform_config
            registry = ApplicationSourceRegistry(manager.registry.session_factory, binding=ApplicationSourceUploadBindingV1(
                installation_id=installation.authority.installation_id,
                data_environment_id=installation.shared.data_environment_id,
                cluster_id=installation.shared.cluster_id, source_bucket=config["buckets"]["source"],
                upload_ttl_seconds=settings.upload_ttl_seconds,
            ))
            store = MinioObjectStore(endpoint_url=config["storage_endpoint"], region=config["region"],
                access_key=material["access-key"], secret_key=material["secret-key"])
            resources.callback(store.close)
            uploader = ApplicationSourceUploader(registry, store, spool_directory=settings.spool_directory,
                max_inflight=settings.max_inflight, receive_timeout_seconds=settings.receive_timeout_seconds,
                storage_timeout_seconds=settings.storage_timeout_seconds)
        except Exception:
            # Configuration and SDK errors can carry secret material; never echo
            # it in startup logs. AsyncExitStack also closes failed partial opens.
            raise ValueError("invalid_application_source_runtime") from None
        yield uploader

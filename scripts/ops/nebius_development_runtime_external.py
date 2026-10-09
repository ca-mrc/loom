"""Read-only external authority for the protected closed development runtime."""
from __future__ import annotations

import asyncio
import base64
import json
import tempfile
from pathlib import Path

import httpx
from scripts.ops import nebius_certificates as private_state
from scripts.ops.nebius_development_build_cloud import qualify_registry_cloud
from scripts.ops.nebius_development_collector_cloud import qualify_collector_cloud
from scripts.ops.nebius_development_collector_runtime import prepare_collector_runtime
from scripts.ops.nebius_development_runtime_install import DevelopmentRuntimeInstallRequest
from scripts.ops.nebius_pool_runtime import _RetainedPoolCollectorSettings

from loom.execution_image_admission import ImageAdmissionKeyring
from loom_execution_capacity_collector.nebius import NebiusCapacityReader
from loom_service.environment_management.candidates import GitHubCandidateCatalog, _json


async def qualify_runtime_publication(*, request: DevelopmentRuntimeInstallRequest,
                                      http: httpx.AsyncClient) -> None:
    """Resolve the exact protected artifact, using the retained reader identity."""
    try:
        manager = request.database.manager
        retained = manager.retained.request.retained
        installation = manager.deployment.installation
        selected = manager.publication.validate(installation.registry_prefix)
        history = _json(retained.files[Path(retained.operation['state_dir']) / 'supplied/stage.json'])
        key = 'Secret:' + retained.binding.namespace + ':loom-management-publications'
        token = base64.b64decode(history['resources'][key]['observed']['data']['token'], validate=True).decode()
        catalog = GitHubCandidateCatalog(http, token=token, publications=[selected.publication],
            registry_prefix=installation.registry_prefix,
            keyring=ImageAdmissionKeyring.from_json(json.dumps(installation.keyring)))
        if await catalog.resolve(selected.publication.candidate_id) != selected.bundle:
            raise ValueError
    except Exception:
        raise ValueError('development runtime publication unqualified') from None


async def qualify_runtime_cloud(*, request: DevelopmentRuntimeInstallRequest,
                                operator_credentials: Path) -> None:
    """Read IAM with the operator, then read the pool with its observer key.

    No observation is published, image pushed, permit changed or capacity
    requested. Zero allowance and busy/scaling pools remain valid observations;
    startup cannot manufacture capacity or retire another environment's writer.
    """
    from nebius.sdk import SDK

    try:
        original = private_state._private_read(operator_credentials, limit=1024**2)
        config = request.database.foundation.inputs.config
        spec = request.database.manager.retained.request.registration.spec
        repositories = tuple(sorted({row.settings.registry_repository for row in spec.profiles.task_images}
            | {row.settings.registry_repository for row in spec.profiles.application_images}))
        async with asyncio.timeout(180):
            sdk = SDK(credentials_file_name=str(operator_credentials), user_agent_prefix='loom-dev-runtime/1.0')
            try:
                await qualify_collector_cloud(sdk=sdk, scope=request.collector_scope,
                    config=config, credential=request.collector_credential)
                await qualify_registry_cloud(sdk=sdk, scope=request.registry_scope,
                    config=config, credential=request.registry_credential, repositories=repositories)
            finally:
                await sdk.close()
            with tempfile.TemporaryDirectory(prefix='loom-dev-runtime-cloud-') as scratch:
                credential = Path(scratch) / 'observer.json'
                private_state._write_private(credential, request.collector_credential)
                runtime = prepare_collector_runtime(request.database)
                prefix = 'LOOM_EXECUTION_CAPACITY_COLLECTOR_'
                values = {key.removeprefix(prefix).lower(): value for key, value in runtime.configuration['data'].items()
                    if key != prefix + 'COLLECTION_MODE'}
                values.update(nebius_credentials_file=str(credential),
                    management_bearer_token_file='/var/run/loom-owned/credentials/control-plane-token')
                settings = _RetainedPoolCollectorSettings(_env_file=None, **values)
                reader = NebiusCapacityReader(settings)
                try:
                    await reader.capture_pool(expected_cluster_id=config['cluster_id'])
                finally:
                    await reader.close()
                if private_state._private_read(credential, limit=1024**2) != request.collector_credential:
                    raise ValueError
        if private_state._private_read(operator_credentials, limit=1024**2) != original:
            raise ValueError
    except Exception:
        raise ValueError('development runtime cloud unqualified') from None

"""Read-only qualification of native publication's registry-only identity."""
from __future__ import annotations

import asyncio
import re
from datetime import datetime
from typing import Any

from pydantic import Field
from scripts.ops.nebius_development_collector_cloud import (
    DevelopmentCollectorCloudScope,
    _qualify_runtime_key,
    collector_public_key,
)
from scripts.ops.nebius_management_cloud_scope import _ProviderId, _read, _require, _resource


class DevelopmentRegistryCloudScope(DevelopmentCollectorCloudScope):
    registry_id: _ProviderId
    registry_fqdn: str = Field(pattern=r'^cr\.[a-z0-9-]+\.nebius\.cloud/[a-z0-9]+$')


def _identity(scope: DevelopmentRegistryCloudScope) -> DevelopmentCollectorCloudScope:
    return DevelopmentCollectorCloudScope.model_validate(scope.model_dump(
        include=set(DevelopmentCollectorCloudScope.model_fields)))


def registry_public_key(*, scope: DevelopmentRegistryCloudScope, config: dict[str, Any],
                       credential: bytes, repositories: tuple[str, ...]) -> bytes:
    """Validate the fixed catalog's publication paths and private key, not IAM."""
    try:
        scope = DevelopmentRegistryCloudScope.model_validate(scope.model_dump())
        prefix = scope.registry_fqdn + '/'
        component = r'[a-z0-9]+(?:(?:[._]|__|-+)[a-z0-9]+)*'
        _require(scope.registry_fqdn.startswith('cr.' + scope.region + '.nebius.cloud/')
            and isinstance(repositories, tuple) and bool(repositories)
            and all(isinstance(repository, str) and len(repository) <= 200 and repository.startswith(prefix)
                and re.fullmatch(component + '(?:/' + component + ')*', repository[len(prefix):]) is not None
                for repository in repositories))
        return collector_public_key(scope=_identity(scope), config=config, credential=credential)
    except Exception:
        raise ValueError('development build registry unqualified') from None


async def qualify_registry_cloud(*, sdk: Any, scope: DevelopmentRegistryCloudScope,
        config: dict[str, Any], credential: bytes, repositories: tuple[str, ...],
        clients: dict[str, Any] | None = None, now: datetime | None = None) -> dict[str, str]:
    """Require one registry editor permit, never project/tenant write authority.

    Actual authenticated publication and node pulls remain installed acceptance.
    No cloud grant, key issuance, image push or pool activation occurs here.
    """
    from nebius.api.nebius.registry import v1

    try:
        scope = DevelopmentRegistryCloudScope.model_validate(scope.model_dump())
        registry_public_key(scope=scope, config=config, credential=credential, repositories=repositories)
        async with asyncio.timeout(150):
            proof = await _qualify_runtime_key(sdk=sdk, scope=_identity(scope), config=config, credential=credential,
                permit={'resource_id': scope.registry_id, 'role': 'editor'}, clients=clients, now=now)
            api = clients['registries'] if clients is not None else v1.RegistryServiceClient(sdk)
            registry = await _read(api.get, v1.GetRegistryRequest(id=scope.registry_id))
            _resource(registry, scope.registry_id, scope.project_id)
            _require(registry['status']['state'] == 'ACTIVE'
                and registry['status']['registry_fqdn'] == scope.registry_fqdn)
        return {**proof, 'registry_id': scope.registry_id}
    except Exception:
        raise ValueError('development build registry unqualified') from None

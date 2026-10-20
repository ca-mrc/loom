"""Typed operator-owned inputs and explicit client lifetime for lifecycle commands."""
from __future__ import annotations

import hashlib
import json
from collections.abc import AsyncIterator, Callable
from contextlib import AsyncExitStack, asynccontextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal
from uuid import UUID

import httpx
from pydantic import BaseModel, ConfigDict, Field
from scripts.ops.nebius_development_build_cloud import DevelopmentRegistryCloudScope
from scripts.ops.nebius_environment_foundation import FoundationCloud, FoundationReferenceV2
from scripts.ops.nebius_environment_lifecycle_entry import _private
from scripts.ops.nebius_environment_release_contracts import EnvironmentReleaseV2
from scripts.ops.nebius_platform_runtime_readiness import PlatformRuntimePolicyV2
from scripts.ops.nebius_pool_administration_dispatch import (
    Connection,
    PoolAdministrationHostV2,
    PoolAdministrationReferenceV2,
    _host,
    _runtime,
    authenticated_administration,
)
from scripts.ops.nebius_pool_enrollment_issuer import PoolEnrollmentPolicyV2, _policy
from scripts.ops.nebius_production_promotion import (
    ProductionPromotionV2,
    production_authority,
    require_production_effect,
)

from loom.execution_image_admission import ImageAdmissionKeyring
from loom.nebius_environment_foundation import EnvironmentFoundationDeclarationV2
from loom.nebius_kubernetes import NebiusKubernetesConnection, NebiusKubernetesCredentials
from loom.nebius_pool_contract import PoolParticipantV1
from loom_service.environment_management.candidates import (
    GitHubCandidateCatalog,
    ProtectedPublication,
)
from loom_service.environment_management.kubernetes_provider import KubernetesEnvironmentProvider
from loom_service.environment_management.runtime import NebiusManagementAuth
from loom_service.pool_management.installation import PoolMachineInstallation
from loom_service.pool_management.lifecycle_contracts import PoolWaitingCancellationV2
from loom_service.pool_management.profiles import PoolProfileCatalog


class _Strict(BaseModel):
    model_config = ConfigDict(extra='forbid', frozen=True, hide_input_in_errors=True)


class EnrollmentInputsV2(_Strict):
    host: dict[str, Any]
    management_origin: str
    foundation_dir: Path
    foundation: FoundationReferenceV2
    application_publication: ProtectedPublication
    manager_publication: ProtectedPublication
    participant: PoolParticipantV1
    profiles: PoolProfileCatalog
    machines: tuple[PoolMachineInstallation, ...]

    def policy(self) -> PoolEnrollmentPolicyV2:
        from scripts.ops.nebius_pool_administration_dispatch import PoolAdministrationScopeV2
        body = self.host
        if set(body) != {'runtime', 'scope', 'state_root', 'anchor_root', 'connection'}:
            raise ValueError('environment_lifecycle_host_unqualified')
        host = PoolAdministrationHostV2(runtime=_runtime(body['runtime']), scope=PoolAdministrationScopeV2.model_validate(body['scope']),
            state_root=Path(body['state_root']), anchor_root=Path(body['anchor_root']),
            connection=NebiusKubernetesConnection.model_validate(body['connection']))
        _host(host)
        policy = PoolEnrollmentPolicyV2(host=host, **{key: getattr(self, key) for key in type(self).model_fields if key != 'host'})
        _policy(policy)
        row, selected = policy.foundation.identity, policy.participant
        expected = dict(installation_id=row.installation_id, pool_id=row.pool_id, participant_id=row.participant_id,
            environment_id=row.environment_id, incarnation=row.incarnation, authority=row.kind)
        if (host.scope.model_dump() != expected
                or any(getattr(selected, name) != value for name, value in expected.items() if name != 'authority')
                or selected.environment_class != row.kind or selected.execution_namespace.name != row.execution_namespace
                or selected.build_namespace.name != row.build_namespace):
            raise ValueError('environment_lifecycle_policy_scope_unqualified')
        return policy


class PlatformInputsV2(_Strict):
    host: dict[str, Any]
    authority: PoolAdministrationReferenceV2
    resources: dict[str, dict[str, Any]]
    manager_key: str
    gateway_key: str
    gateway_token_key: str
    catalog_key: str

    def policy(self) -> PlatformRuntimePolicyV2:
        from scripts.ops.nebius_pool_administration_dispatch import PoolAdministrationScopeV2
        body = self.host
        if set(body) != {'runtime', 'scope', 'state_root', 'anchor_root', 'connection'}:
            raise ValueError('environment_lifecycle_host_unqualified')
        host = PoolAdministrationHostV2(runtime=_runtime(body['runtime']), scope=PoolAdministrationScopeV2.model_validate(body['scope']),
            state_root=Path(body['state_root']), anchor_root=Path(body['anchor_root']),
            connection=NebiusKubernetesConnection.model_validate(body['connection']))
        _host(host)
        return PlatformRuntimePolicyV2(host=host, **{key: getattr(self, key) for key in type(self).model_fields if key != 'host'})


class PrivateFileV2(_Strict):
    path: Path
    sha256: str = Field(pattern=r'^[0-9a-f]{64}$')

    def read(self) -> bytes:
        raw = _private(self.path)
        if hashlib.sha256(raw).hexdigest() != self.sha256:
            raise ValueError('environment_lifecycle_material_changed')
        return raw


class LifecycleInputsV2(_Strict):
    schema_version: Literal['loom.environment-lifecycle-inputs.v2']
    enrollment: EnrollmentInputsV2
    github_token: PrivateFileV2
    cloud_credentials: PrivateFileV2
    registry_prefix: str
    runtime_operation_id: UUID
    enrollment_operation_id: UUID
    platform: PlatformInputsV2 | None = None
    token: PrivateFileV2 | None = None
    registry_scope: DevelopmentRegistryCloudScope | None = None
    registry_credential: PrivateFileV2 | None = None
    predecessor: PoolAdministrationReferenceV2 | None = None
    expected_head: UUID | None = None
    cancel_waiting: tuple[PoolWaitingCancellationV2, ...] = ()
    release: EnvironmentReleaseV2 | None = None
    current_release_id: UUID | None = None
    production_promotions: tuple[ProductionPromotionV2, ...] = Field(default=(), max_length=8)

    def private_files(self) -> dict[Path, bytes]:
        rows = [self.github_token, self.cloud_credentials]
        rows.extend(row for row in (self.token, self.registry_credential) if row is not None)
        if len({row.path for row in rows}) != len(rows):
            raise ValueError('environment_lifecycle_credential_alias')
        return {row.path: row.read() for row in rows}


class FoundationInputsV2(_Strict):
    """Provision/import before participant namespace UIDs exist in enrollment."""

    schema_version: Literal['loom.environment-foundation-inputs.v2']
    host: dict[str, Any]
    foundation_dir: Path
    declaration: EnvironmentFoundationDeclarationV2
    application_publication: ProtectedPublication
    image_admission_keyring: dict[str, Any]
    github_token: PrivateFileV2
    cloud_credentials: PrivateFileV2
    registry_prefix: str
    legacy_foundation: PrivateFileV2 | None = None
    production_promotions: tuple[ProductionPromotionV2, ...] = Field(default=(), max_length=8)

    def host_policy(self) -> PoolAdministrationHostV2:
        from scripts.ops.nebius_pool_administration_dispatch import PoolAdministrationScopeV2

        body = self.host
        if set(body) != {'runtime', 'scope', 'state_root', 'anchor_root', 'connection'}:
            raise ValueError('environment_lifecycle_host_unqualified')
        host = PoolAdministrationHostV2(runtime=_runtime(body['runtime']),
            scope=PoolAdministrationScopeV2.model_validate(body['scope']),
            state_root=Path(body['state_root']), anchor_root=Path(body['anchor_root']),
            connection=NebiusKubernetesConnection.model_validate(body['connection']))
        _host(host)
        if (not self.foundation_dir.is_absolute() or self.foundation_dir != self.foundation_dir.resolve()
                or self.foundation_dir == Path('/') or self.foundation_dir in host.state_root.parents
                or host.state_root in self.foundation_dir.parents or self.foundation_dir == host.state_root):
            raise ValueError('environment_lifecycle_foundation_path_unqualified')
        row = self.declaration.registration
        if host.scope.model_dump() != dict(installation_id=row.installation_id, pool_id=row.pool_id,
                participant_id=row.participant_id, environment_id=row.environment_id, incarnation=row.incarnation,
                authority=row.kind):
            raise ValueError('environment_lifecycle_foundation_scope_unqualified')
        return host

    def private_files(self) -> dict[Path, bytes]:
        rows = [self.github_token, self.cloud_credentials]
        if self.legacy_foundation is not None:
            rows.append(self.legacy_foundation)
        if len({row.path for row in rows}) != len(rows):
            raise ValueError('environment_lifecycle_credential_alias')
        return {row.path: row.read() for row in rows}


@dataclass
class LifecycleClients:
    catalog: GitHubCandidateCatalog
    kubernetes: KubernetesEnvironmentProvider
    cloud: FoundationCloud
    public_http: httpx.AsyncClient
    connect: Connection
    registry_sdk: Any

    def common(self) -> dict[str, Any]:
        return {key: getattr(self, key) for key in ('catalog', 'kubernetes', 'cloud', 'public_http', 'connect')}


@asynccontextmanager
async def lifecycle_clients(inputs: LifecycleInputsV2 | FoundationInputsV2, policy: PoolEnrollmentPolicyV2 | None,
        unchanged: Callable[[], Any]) -> AsyncIterator[LifecycleClients]:
    from nebius.sdk import SDK
    from scripts.ops.nebius_environment_legacy_foundation import NebiusFoundationCloud

    material = inputs.private_files()
    host = policy.host if policy is not None else inputs.host_policy() if isinstance(inputs, FoundationInputsV2) else None
    if host is None:
        raise ValueError('environment_lifecycle_host_missing')
    connection = host.connection
    for path in (connection.ca_file, connection.credentials_file):
        if path in material:
            raise ValueError('environment_lifecycle_credential_alias')
        material[path] = _private(path)

    def qualify() -> None:
        unchanged()
        if any(_private(path) != raw for path, raw in material.items()):
            raise ValueError('environment_lifecycle_material_changed')

    def qualify_mutation() -> None:
        qualify()
        require_production_effect(host.scope)

    async def guard_kubernetes(request: httpx.Request) -> None:
        if request.method not in {'GET', 'HEAD', 'OPTIONS'}:
            qualify_mutation()

    async with AsyncExitStack() as resources:
        credentials = NebiusKubernetesCredentials(connection)
        resources.push_async_callback(credentials.close)
        sdk = SDK(credentials_file_name=str(inputs.cloud_credentials.path), user_agent_prefix='loom-environment-lifecycle/2.0')
        resources.push_async_callback(sdk.close)
        await credentials.get_token()
        NebiusKubernetesCredentials._usable(await sdk.get_token(timeout=30))
        qualify()
        kube = await resources.enter_async_context(httpx.AsyncClient(base_url=connection.endpoint,
            verify=credentials.ssl_context, auth=NebiusManagementAuth(connection, credentials), trust_env=False,
            timeout=30, follow_redirects=False, event_hooks={'request': [guard_kubernetes]}))
        public = await resources.enter_async_context(httpx.AsyncClient(trust_env=False, timeout=30, follow_redirects=False))
        github = await resources.enter_async_context(httpx.AsyncClient(trust_env=False, timeout=30, follow_redirects=False))
        if policy is not None:
            selected_publications = [policy.application_publication, policy.manager_publication]
            keyring = policy.profiles.image_admission_keyring
            if isinstance(inputs, LifecycleInputsV2) and inputs.release is not None:
                selected_publications.append(inputs.release.publication)
                if inputs.release.previous_release_id is not None:
                    from scripts.ops.nebius_environment_release import (
                        resolve_completed_release_policy,
                    )

                    current, _, _, _ = resolve_completed_release_policy(policy=policy, release_id=inputs.release.previous_release_id)
                    selected_publications.append(current.application_publication)
            if isinstance(inputs, LifecycleInputsV2) and inputs.current_release_id is not None:
                from scripts.ops.nebius_environment_release import resolve_completed_release_policy

                current, _, _, _ = resolve_completed_release_policy(policy=policy, release_id=inputs.current_release_id)
                selected_publications.append(current.application_publication)
        elif isinstance(inputs, FoundationInputsV2):
            selected_publications = [inputs.application_publication]
            keyring = inputs.image_admission_keyring
        else:
            raise ValueError('environment_lifecycle_publication_missing')
        publications: dict[UUID, ProtectedPublication] = {}
        for row in selected_publications:
            if row.candidate_id in publications and publications[row.candidate_id] != row:
                raise ValueError('environment_lifecycle_publication_alias')
            publications[row.candidate_id] = row
        catalog = GitHubCandidateCatalog(github, token=material[inputs.github_token.path].decode().strip(),
            publications=list(publications.values()), registry_prefix=inputs.registry_prefix,
            keyring=ImageAdmissionKeyring.from_json(json.dumps(keyring)))
        yield LifecycleClients(catalog, KubernetesEnvironmentProvider(kube),
            NebiusFoundationCloud(sdk, qualify_mutation=qualify_mutation), public,
            lambda request: authenticated_administration(host, request, qualify_mutation=qualify_mutation), sdk)
        qualify()


@asynccontextmanager
async def lifecycle_production_authority(inputs: LifecycleInputsV2 | FoundationInputsV2,
        clients: LifecycleClients) -> AsyncIterator[None]:
    """Separate exact source and target approval before any production action."""
    if isinstance(inputs, FoundationInputsV2):
        scope = inputs.host_policy().scope
        publications = [inputs.application_publication]
        declaration = inputs.declaration
    else:
        from scripts.ops.nebius_environment_foundation import _load

        policy = inputs.enrollment.policy()
        scope = policy.host.scope
        publications = [policy.application_publication]
        if inputs.release is not None:
            publications.append(inputs.release.publication)
            if inputs.release.previous_release_id is not None:
                from scripts.ops.nebius_environment_release import resolve_completed_release_policy

                current, _, _, _ = resolve_completed_release_policy(policy=policy, release_id=inputs.release.previous_release_id)
                publications.append(current.application_publication)
        if inputs.current_release_id is not None:
            from scripts.ops.nebius_environment_release import resolve_completed_release_policy

            current, _, _, _ = resolve_completed_release_policy(policy=policy, release_id=inputs.current_release_id)
            publications.append(current.application_publication)
        declaration = None
        if scope.authority == 'production':
            declaration = EnvironmentFoundationDeclarationV2.model_validate(_load(policy.foundation_dir, policy.foundation)['declaration'])
    if scope.authority != 'production':
        if inputs.production_promotions:
            raise ValueError('environment_lifecycle_production_scope_unqualified')
        yield
        return
    references = {row.publication.candidate_id: row for row in inputs.production_promotions}
    selected = {row.candidate_id: row for row in publications}
    if len(references) != len(inputs.production_promotions) or set(references) != set(selected):
        raise ValueError('environment_lifecycle_production_authority_missing')
    async with AsyncExitStack() as authorities:
        from scripts.ops.nebius_production_promotion import require_production_foundation

        for identity, publication in selected.items():
            await authorities.enter_async_context(production_authority(promotion=references[identity], scope=scope,
                publication=publication, catalog=clients.catalog))
            require_production_foundation(scope=scope, publication=publication, declaration=declaration)
        yield

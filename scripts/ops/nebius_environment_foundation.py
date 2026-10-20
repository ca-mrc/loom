"""Protected, create-only foundation provisioning and exact read-only import.

The trusted issuer freezes a rendered plan before any external effect. Public
runner commands cannot supply this plan, state paths, provider clients or Secrets.
Receipts prove observed resources, not DNS/service/runtime readiness or admission.
No cleanup here deletes databases, buckets, namespaces or retained credentials.
"""
from __future__ import annotations

import base64
import json
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal, Protocol, cast
from uuid import UUID

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field
from scripts.ops import nebius_certificates as state
from scripts.ops.nebius_ingress_stage import _key, _uid
from scripts.ops.nebius_management_switch import _matches

from loom.nebius_environment_foundation import (
    EnvironmentFoundationDeclarationV2,
    EnvironmentFoundationIdentityV2,
    render_foundation,
)
from loom_service.environment_management.candidates import _json
from loom_service.environment_management.cloud_provider import (
    NebiusEnvironmentApi,
    NebiusEnvironmentCloudProvider,
)
from loom_service.environment_management.credentials import generate_material
from loom_service.environment_management.kubernetes_provider import KubernetesEnvironmentProvider
from loom_service.environment_management.provider import ProviderBlockedError, ProvisioningContext
from loom_service.environment_management.registry import OperationLease
from loom_service.environment_management.steps import ProvisioningStep
from loom_service.pool_management.capacity import digest

_LIMIT = 8 * 1024 * 1024


class FoundationCloud(NebiusEnvironmentApi, Protocol):
    async def access_key_secret(self, identity: str) -> dict[str, str]: ...


class _Strict(BaseModel):
    model_config = ConfigDict(extra='forbid', frozen=True, hide_input_in_errors=True)


class FoundationReferenceV2(_Strict):
    schema_version: Literal['loom.foundation-reference.v2'] = 'loom.foundation-reference.v2'
    operation_id: UUID
    identity: EnvironmentFoundationIdentityV2
    plan_sha256: str = Field(pattern=r'^[0-9a-f]{64}$')


class FoundationObservationV2(_Strict):
    schema_version: Literal['loom.foundation-observation.v2'] = 'loom.foundation-observation.v2'
    operation_id: UUID
    identity: EnvironmentFoundationIdentityV2
    plan_sha256: str = Field(pattern=r'^[0-9a-f]{64}$')
    observed_at: AwareDatetime
    phase: Literal['resources_verified', 'runtime_bindings_verified', 'legacy_bindings_verified'] = 'resources_verified'
    execution_enabled: Literal[False] | None = False
    namespace_uids: dict[str, str]
    identities: dict[str, str]


def _read(path: Path) -> dict[str, Any]:
    return _json(state._private_read(path, limit=_LIMIT))


def _retain(path: Path, body: dict[str, Any]) -> None:
    if path.exists():
        if _read(path) != body:
            raise ValueError('foundation_retained_intent_conflict')
    else:
        state._write_private(path, json.dumps(body, sort_keys=True).encode())
        state._sync_directory(path.parent)


def _load(state_dir: Path, reference: FoundationReferenceV2) -> dict[str, Any]:
    reference = FoundationReferenceV2.model_validate(reference.model_dump())
    try:
        plan = _read(state_dir / 'plan.json')
    except FileNotFoundError:
        raise ValueError('foundation_retained_plan_missing') from None
    if (digest(plan) != reference.plan_sha256 or plan['operation_id'] != str(reference.operation_id)
            or plan['declaration']['registration'] != reference.identity.model_dump(mode='json')):
        raise ValueError('foundation_retained_plan_conflict')
    EnvironmentFoundationDeclarationV2.model_validate(plan['declaration'])
    return plan


def prepare_foundation(*, state_dir: Path, declaration: EnvironmentFoundationDeclarationV2,
        candidate: dict[str, Any], profile: dict[str, Any], keyring: dict[str, Any], repo_root: Path) -> FoundationReferenceV2:
    """Issuer-only preparation. Publication/target authority is checked by caller."""
    prepared = render_foundation(declaration, candidate, profile=profile, keyring=keyring, repo_root=repo_root)
    plan = dict(schema_version='loom.foundation-plan.v2', operation_id=str(declaration.operation_id),
        mode='create', declaration=declaration.model_dump(mode='json'), files=prepared.files,
        publication_sha256=digest(candidate), profile_sha256=digest(profile),
        import_identities=None, material_sha256=None)
    with state._locked_state(state_dir):
        _retain(state_dir / 'plan.json', plan)
    return FoundationReferenceV2(operation_id=declaration.operation_id, identity=declaration.registration, plan_sha256=digest(plan))


def prepare_foundation_import(*, state_dir: Path, source_dir: Path, source_reference: FoundationReferenceV2,
        source_observation: FoundationObservationV2, operation_id: UUID) -> FoundationReferenceV2:
    """Exact existing-source import; no name-only adoption, relabelling or rotation.

    Original manifests and ownership tokens retain their original operation ID.
    The new import has its own journal and must re-observe every exact provider ID.
    Legacy sources require their separately qualified ancestry adapter first.
    """
    if not operation_id.int or state_dir.resolve() == source_dir.resolve():
        raise ValueError('foundation_import_scope_invalid')
    with state._locked_state(source_dir):
        plan = _load(source_dir, source_reference)
        if (source_observation.model_dump(mode='json') != _read(source_dir / 'observed.json')
                or source_observation.plan_sha256 != source_reference.plan_sha256
                or source_observation.operation_id != source_reference.operation_id
                or source_observation.identity != source_reference.identity):
            raise ValueError('foundation_import_source_unqualified')
        material = _read(source_dir / 'material.json')
        if source_observation.identities.get('credentials:material') != digest(material):
            raise ValueError('foundation_material_conflict')
        plan.update(operation_id=str(operation_id), mode='legacy-import' if plan['mode'] == 'legacy-import' else 'import',
            import_identities=source_observation.identities, material_sha256=digest(material))
    with state._locked_state(state_dir):
        _retain(state_dir / 'plan.json', plan)
        _retain(state_dir / 'material.json', material)
    return FoundationReferenceV2(operation_id=operation_id, identity=source_reference.identity, plan_sha256=digest(plan))


def _steps(plan: dict[str, Any]) -> list[ProvisioningStep]:
    declaration = EnvironmentFoundationDeclarationV2.model_validate(plan['declaration'])
    namespace, config = declaration.registration.application_namespace, declaration.platform_config
    steps: list[ProvisioningStep] = []

    def documents(filename: str) -> None:
        for doc in plan['files'][filename]:
            metadata = doc['metadata']
            key = f"k8s:{doc['kind']}:{metadata.get('namespace', '-')}:{metadata['name']}"
            steps.append(ProvisioningStep(key, 'kubernetes', doc))
            if doc['kind'] == 'Job':
                steps.append(ProvisioningStep('ready:' + key, 'job_ready', {
                    'namespace': namespace, 'name': metadata['name'], 'resource_key': key}))

    documents('00-namespaces.yaml')
    documents('60-execution.yaml')
    purposes = ('canonical', 'source', 'backup') if declaration.backups_enabled else ('canonical', 'source')
    for purpose in purposes:
        for action in ('service_account', 'group', 'membership', 'access_key'):
            steps.append(ProvisioningStep(f'iam:{purpose}:{action}', 'credentials', {'purpose': purpose, 'action': action}))
    buckets = set()
    for purpose, name in sorted(config['buckets'].items()):
        if name in buckets or (purpose == 'backup' and not declaration.backups_enabled):
            continue
        buckets.add(name)
        steps.append(ProvisioningStep('bucket:' + purpose, 'object_bucket', {'purpose': purpose, 'name': name}))
    steps.append(ProvisioningStep('credentials:material', 'credentials', {'action': 'material', 'purposes': list(purposes)}))
    for name in ('loom-platform-db', config['db_tls_secret_name'], 'loom-platform-auth', 'loom-admin-secret',
            'loom-platform-collector', 'loom-platform-batch-runner', 'loom-platform-storage'):
        steps.append(ProvisioningStep('secret:' + name, 'credentials', {'action': 'kubernetes_secret', 'name': name}))
    documents('10-config-network.yaml')
    documents('20-database.yaml')
    steps.append(ProvisioningStep('ready:database', 'database_ready', {'namespace': namespace, 'name': 'loom-postgres'}))
    for filename in ('30-migrate.yaml', '40-services.yaml', '50-configure.yaml', '70-public.yaml', '80-backup.yaml'):
        documents(filename)
        if filename == '40-services.yaml':
            steps.append(ProvisioningStep('ready:services', 'application_ready', {'namespace': namespace}))
    if len({step.key for step in steps}) != len(steps):
        raise ValueError('foundation_duplicate_resource_identity')
    return steps


async def _material(state_dir: Path, context: ProvisioningContext, step: ProvisioningStep, cloud: FoundationCloud,
        *, create: bool) -> dict[str, Any]:
    try:
        return _read(state_dir / 'material.json')
    except FileNotFoundError:
        if not create or step.key in context.identities:
            raise ProviderBlockedError('foundation_retained_material_missing') from None
    # Nothing using this material is sent externally until the private copy is
    # fsynced. Lost replies thereafter always reuse the same credentials.
    material = generate_material(namespace=context.registration['application_namespace'], tls_secret_name=context.config['db_tls_secret_name'])
    storage: dict[str, str] = {}
    for purpose in step.payload['purposes']:
        value = await cloud.access_key_secret(context.identities[f'iam:{purpose}:access_key'])
        if set(value) != {'access-key', 'secret-key'} or not all(isinstance(v, str) and v for v in value.values()):
            raise ProviderBlockedError('foundation_storage_credentials_invalid')
        storage.update({('' if purpose == 'canonical' else purpose + '-') + key: secret for key, secret in value.items()})
    material['loom-platform-storage'] = storage
    _retain(state_dir / 'material.json', material)
    return material


async def reconcile_foundation(*, state_dir: Path, reference: FoundationReferenceV2,
        kubernetes: KubernetesEnvironmentProvider, cloud: FoundationCloud,
        observe_only: bool = False,
        _runtime_successors: dict[str, tuple[dict[str, Any], ...]] | None = None) -> FoundationObservationV2:
    """Recover exact effects and qualify all bindings, including completed steps.

    Holding a nonblocking local operation lock prevents concurrent local runners;
    no database transaction is held across provider IO. Separate environments
    have distinct state roots. The host's pinned issuer supplies the clients.

    Runtime-only callers may supply exact UID-bound successors reconstructed from
    their anchored phase journals. This is not a public skip-list: only the fixed
    app/worker-boundary keys are replaceable, all data and credential bindings
    remain checked, and this mode cannot create or rewrite foundation history.
    The runtime parent separately owns rollout and process readiness.
    """
    with state._locked_state(state_dir):
        observed_at = datetime.now(UTC)
        plan = _load(state_dir, reference)
        if plan['mode'] == 'legacy-import':
            from scripts.ops.nebius_environment_legacy_foundation import (
                LegacyFoundationCloud,
                observe_legacy_foundation,
            )

            return await observe_legacy_foundation(state_dir=state_dir, reference=reference, plan=plan,
                kubernetes=kubernetes, cloud=cast(LegacyFoundationCloud, cloud), successors=_runtime_successors)
        declaration = EnvironmentFoundationDeclarationV2.model_validate(plan['declaration'])
        async def qualify_cluster() -> None:
            if str(kubernetes.http.base_url).rstrip('/') != declaration.platform_config['kubernetes_api_server'].rstrip('/'):
                raise ProviderBlockedError('foundation_cluster_origin_conflict')
            cluster = await kubernetes._request('GET', '/api/v1/namespaces/kube-system')
            if (cluster is None or cluster.get('apiVersion') != 'v1' or cluster.get('kind') != 'Namespace'
                    or cluster.get('metadata', {}).get('name') != 'kube-system'
                    or cluster['metadata'].get('uid') != str(declaration.kube_system_uid)
                    or cluster['metadata'].get('deletionTimestamp')):
                raise ProviderBlockedError('foundation_cluster_identity_conflict')
        await qualify_cluster()
        steps = _steps(plan)
        create = plan['mode'] == 'create' and not observe_only
        identities = dict(plan['import_identities'] or {})
        step_root = state_dir / 'steps'
        state._private_directory(step_root)
        cursor_path = state_dir / 'cursor.json'
        cursor = _read(cursor_path) if cursor_path.exists() else {'records': {}}
        if set(cursor) != {'records'} or not isinstance(cursor['records'], dict):
            raise ValueError('foundation_journal_invalid')
        records: dict[str, str] = cursor['records']
        if set(records) != {f'{index:04d}' for index in range(len(records))} or len(records) > len(steps):
            raise ValueError('foundation_journal_invalid')
        for record_index, checksum in records.items():
            try:
                recorded = _read(step_root / f'{record_index}.observed.json')
            except FileNotFoundError:
                raise ValueError('foundation_journal_missing') from None
            if digest(recorded) != checksum or not (step_root / f'{record_index}.intent.json').exists():
                raise ValueError('foundation_journal_conflict')
        if ((state_dir / 'observed.json').exists() or observe_only) and len(records) != len(steps):
            raise ValueError('foundation_journal_incomplete')
        for index, step in enumerate(steps):
            path = step_root / f'{index:04d}.observed.json'
            if path.exists():
                recorded = _read(path)
                if set(recorded) != {'key', 'identity'} or recorded['key'] != step.key or not isinstance(recorded['identity'], str) or not recorded['identity']:
                    raise ValueError('foundation_recorded_identity_invalid')
                if step.key in identities and identities[step.key] != recorded['identity']:
                    raise ValueError('foundation_import_identity_conflict')
                identities[step.key] = recorded['identity']
        context = ProvisioningContext(OperationLease(declaration.operation_id, declaration.registration.environment_id,
                1, 1, reference.operation_id), declaration.registration.model_dump(mode='json'),
            declaration.platform_config, identities,
            documents={step.key: step.payload for step in steps if step.kind == 'kubernetes'})
        if _runtime_successors is not None:
            row = reference.identity
            allowed = {f'k8s:Deployment:{row.application_namespace}:{name}' for name in (
                'loom-service', 'loom-control-plane', 'loom-web', 'loom-llm-gateway')}
            allowed.add(f'k8s:ConfigMap:{row.application_namespace}:loom-platform-config')
            allowed.add('k8s:Namespace:-:' + row.build_namespace)
            allowed.update(f'k8s:ResourceQuota:{namespace}:loom-execution-disabled'
                for namespace in (row.execution_namespace, row.build_namespace))
            if not observe_only or not _runtime_successors or set(_runtime_successors) - allowed:
                raise ValueError('foundation_runtime_scope_invalid')
            for key, options in _runtime_successors.items():
                if (not options or len(options) > 2 or key not in context.documents or any(
                        key != 'k8s:' + _key(doc)
                        or _uid(doc) != identities[key] for doc in options)):
                    raise ValueError('foundation_runtime_successor_invalid')
                if key == f'k8s:ConfigMap:{row.application_namespace}:loom-platform-config':
                    from scripts.ops.nebius_environment_legacy_foundation import _qualify_successor
                    for option in options:
                        original = kubernetes._expected(context, ProvisioningStep(key, 'kubernetes', context.documents[key]))
                        _qualify_successor(original, option, application_namespace=row.application_namespace)
        cloud_provider = NebiusEnvironmentCloudProvider(cloud)
        for index, step in enumerate(steps):
            # Retain each intended effect before attempting it, including the
            # resolved dependency identities for IAM and bucket policy writes.
            action = step.payload.get('action')
            intent: dict[str, Any] = asdict(step)
            if step.kind == 'object_bucket' or action in {'service_account', 'group', 'membership', 'access_key'}:
                kind, expected = cloud_provider.intent(context, step)
                intent['resolved'] = {'kind': kind, 'body': expected}
            _retain(step_root / f'{index:04d}.intent.json', intent)
            if _runtime_successors is not None and step.key in _runtime_successors:
                _, resource_path = kubernetes._path(context, step.payload)
                actual = await kubernetes._request('GET', resource_path)
                if actual is None or not any(_matches(actual, expected, identities[step.key])
                        for expected in _runtime_successors[step.key]):
                    raise ValueError('foundation_runtime_successor_conflict')
                identity = identities[step.key]
            elif _runtime_successors is not None and step.kind == 'application_ready':
                # Startup intentionally stops these processes. Their exact
                # successor manifests are checked above; the parent must prove
                # drain/start and process configuration, not reuse this receipt.
                identity = identities[step.key]
            elif step.kind == 'object_bucket' or action in {'service_account', 'group', 'membership', 'access_key'}:
                identity = await cloud_provider.apply(context, step, create=create)
            elif action == 'material':
                identity = digest(await _material(state_dir, context, step, cloud, create=create))
                if plan['material_sha256'] is not None and plan['material_sha256'] != identity:
                    raise ProviderBlockedError('foundation_material_conflict')
            elif action == 'kubernetes_secret':
                material = _read(state_dir / 'material.json')
                if digest(material) != identities['credentials:material']:
                    raise ProviderBlockedError('foundation_material_conflict')
                name = step.payload['name']
                document = {'apiVersion': 'v1', 'kind': 'Secret', 'immutable': True,
                    'metadata': {'namespace': declaration.registration.application_namespace, 'name': name},
                    'type': 'kubernetes.io/tls' if name == context.config['db_tls_secret_name'] else 'Opaque',
                    'data': {key: base64.b64encode(value.encode()).decode() for key, value in material[name].items()}}
                identity = await kubernetes.apply(context, ProvisioningStep(step.key, 'kubernetes', document), create=create)
            else:
                identity = await kubernetes.apply(context, step, create=create)
            if step.key in identities and identities[step.key] != identity:
                raise ProviderBlockedError('foundation_resource_identity_conflict')
            _retain(step_root / f'{index:04d}.observed.json', {'key': step.key, 'identity': identity})
            if _runtime_successors is None:
                records[f'{index:04d}'] = digest({'key': step.key, 'identity': identity})
                state._atomic_json(cursor_path, {'records': records})
            identities[step.key] = identity
        await qualify_cluster()
        observed = FoundationObservationV2(operation_id=reference.operation_id, identity=reference.identity,
            plan_sha256=reference.plan_sha256, observed_at=observed_at, identities=identities,
            phase='resources_verified' if _runtime_successors is None else 'runtime_bindings_verified',
            execution_enabled=False if _runtime_successors is None else None,
            namespace_uids={name: identities['k8s:Namespace:-:' + name] for name in context.namespaces})
        if _runtime_successors is None:
            state._atomic_json(state_dir / 'observed.json', observed.model_dump(mode='json'))
        return observed

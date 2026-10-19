"""Journaled independent runtime startup while task/build admission stays closed.

Only the selected API/controller and staged readers change. The execution quota
allows reader Pods but zero Jobs; build quota/PSA remain closed. Actual process,
writer, capacity and admission qualification still precede activation.
"""
from __future__ import annotations

import asyncio
import copy
import json
import tomllib
from collections.abc import Callable, Iterator
from contextlib import AbstractContextManager, contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from uuid import UUID

import httpx
from scripts.ops import nebius_certificates as state
from scripts.ops.nebius_development_runtime_live import (
    runtime_resource_path as _runtime_resource_path,
)
from scripts.ops.nebius_development_runtime_readiness import qualify_started_deployment
from scripts.ops.nebius_development_runtime_transition import (
    DevelopmentRuntimeWorkloadAPI,
    _advance_fixed_resources,
    validate_runtime_transition,
)
from scripts.ops.nebius_environment_foundation import FoundationCloud, _read, reconcile_foundation
from scripts.ops.nebius_environment_foundation import _load as load_foundation
from scripts.ops.nebius_environment_runtime_install import (
    StagedEnvironmentRuntimeV2,
    _observe,
    _resources,
    load_staged_environment_runtime,
    qualify_runtime_defaulted,
)
from scripts.ops.nebius_foundation_readiness import _public
from scripts.ops.nebius_ingress_stage import _key, _snapshot, _uid
from scripts.ops.nebius_management_switch import _matches
from scripts.ops.nebius_pool_administration_dispatch import (
    Connection,
    PoolAdministrationReferenceV2,
)
from scripts.ops.nebius_pool_enrollment_issuer import PoolEnrollmentPolicyV2, _policy
from scripts.ops.nebius_pool_retirement import qualify_closed_workload_drain

from loom.nebius_environment_foundation import EnvironmentFoundationDeclarationV2
from loom_service.environment_management.candidates import GitHubCandidateCatalog
from loom_service.environment_management.kubernetes_provider import KubernetesEnvironmentProvider
from loom_service.pool_management.capacity import digest
from loom_service.pool_management.inspection import PoolInspectionV2, qualify_inspection

_PHASES = ('stop', 'replace', 'reader-quota', 'start')
WorkloadConnection = Callable[[dict[str, dict[str, Any]], dict[str, dict[str, Any]],
    dict[str, str], Callable[[], None]], AbstractContextManager[DevelopmentRuntimeWorkloadAPI]]


def runtime_resource_path(document: dict[str, Any]) -> str:
    if document['kind'] == 'ResourceQuota':
        return '/api/v1/namespaces/' + str(document['metadata']['namespace']) + '/resourcequotas'
    return _runtime_resource_path(document)


def _frozen(document: dict[str, Any]) -> dict[str, Any]:
    return {**_snapshot(document), 'metadata': {**_snapshot(document)['metadata'], 'uid': _uid(document)}}


def _inputs(plan: dict[str, Any], retained: StagedEnvironmentRuntimeV2, phase: str) -> tuple[dict[str, Any], dict[str, Any]]:
    initial, workloads = plan['originals'], retained.plan['workloads']
    apps = {_key(workloads[name]) for name in ('service', 'control_plane')}
    readers = {_key(doc) for name, doc in workloads.items() if name not in {'service', 'control_plane'}}

    def previous(name: str) -> dict[str, Any]:
        record = _read(retained.state_dir / 'start' / name / 'transition.json')
        if any(item['status'] != 'applied' for item in record['resources'].values()):
            raise ValueError('environment_runtime_predecessor_incomplete')
        return {key: {**copy.deepcopy(item['expected']), 'metadata': {
            **item['expected']['metadata'], 'uid': _uid(initial[key])}} for key, item in record['resources'].items()}

    if phase == 'stop':
        before = {key: initial[key] for key in sorted(apps)}
        targets = {key: _snapshot(doc) for key, doc in before.items()}
        for doc in targets.values():
            doc['spec']['replicas'] = 0
    elif phase == 'replace':
        before = previous('stop')
        by_key = {_key(doc): doc for doc in workloads.values()}
        targets = {key: _snapshot(doc) for key, doc in before.items()}
        for key, doc in targets.items():
            desired = by_key[key]
            doc['spec'] = copy.deepcopy(desired['spec'])
            doc['metadata'].setdefault('labels', {}).update(desired['metadata'].get('labels', {}))
    elif phase == 'reader-quota':
        before = {key: doc for key, doc in initial.items() if doc['kind'] == 'ResourceQuota'}
        targets = {key: _snapshot(doc) for key, doc in before.items()}
        for doc in targets.values():
            doc['spec'] = {'hard': {'pods': str(len(readers)), 'count/jobs.batch': '0'}}
    elif phase == 'start':
        before = {**previous('replace'), **{key: initial[key] for key in sorted(readers)}}
        targets = {key: _snapshot(doc) for key, doc in before.items()}
        for doc in targets.values():
            doc['spec']['replicas'] = 1
    else:
        raise ValueError('environment_runtime_phase_invalid')
    return copy.deepcopy(before), targets


def _options(plan: dict[str, Any], retained: StagedEnvironmentRuntimeV2, cursor: dict[str, Any]) -> dict[str, tuple[dict[str, Any], ...]]:
    current: dict[str, tuple[dict[str, Any], ...]] = {key: (doc,) for key, doc in plan['originals'].items()}
    incomplete = False
    for phase in _PHASES:
        status = cursor['phases'][phase]
        path = retained.state_dir / 'start' / phase / 'transition.json'
        if status not in {'prepared', 'intent', 'complete'} or (incomplete and status != 'prepared'):
            raise ValueError('environment_runtime_cursor_invalid')
        incomplete |= status != 'complete'
        if status == 'prepared':
            if path.exists():
                raise ValueError('environment_runtime_untracked_child')
            continue
        before, targets = _inputs(plan, retained, phase)
        record = _read(path)
        validate_runtime_transition(record, originals=before, targets=targets, input_digest='sha256:' + digest(plan),
            phase=phase, qualify_defaulted=qualify_runtime_defaulted)
        for key, item in record['resources'].items():
            if status == 'complete' and item['status'] != 'applied':
                raise ValueError('environment_runtime_child_incomplete')
            if item['status'] != 'prepared':
                desired = copy.deepcopy(item['expected'])
                desired['metadata']['uid'] = _uid(before[key])
                current[key] = (desired,) if item['status'] == 'applied' else (before[key], desired)
    return current


@dataclass(frozen=True, repr=False)
class StartedEnvironmentRuntimeV2:
    staged: StagedEnvironmentRuntimeV2
    resources: dict[str, dict[str, Any]]
    files: dict[Path, bytes]


def load_started_environment_runtime(*, policy: PoolEnrollmentPolicyV2, operation_id: UUID) -> StartedEnvironmentRuntimeV2:
    """Retained exact successors, never an assertion about current readiness."""
    try:
        staged = load_staged_environment_runtime(policy=policy, operation_id=operation_id)
        directory = staged.state_dir / 'start'
        paths = [directory / 'plan.json', directory / 'cursor.json',
            policy.host.anchor_root / ('runtime-start-' + str(operation_id) + '.json'),
            *(directory / phase / 'transition.json' for phase in _PHASES)]
        files = {path: state._private_read(path, limit=8 * 1024**2) for path in paths}
        plan, cursor, anchor = (json.loads(files[path]) for path in paths[:3])
        if (set(plan) != {'runtime_sha256', 'originals'} or plan['runtime_sha256'] != digest(staged.plan)
                or anchor != {'operation_id': str(operation_id), 'plan_sha256': digest(plan)}
                or cursor != {'phases': dict.fromkeys(_PHASES, 'complete')}):
            raise ValueError
        options = _options(plan, staged, cursor)
        resources = {**copy.deepcopy(staged.resources), **{key: docs[0] for key, docs in options.items() if len(docs) == 1}}
        files.update(staged.files)
        if any(len(docs) != 1 for docs in options.values()) or any(
                state._private_read(path, limit=8 * 1024**2) != raw for path, raw in files.items()):
            raise ValueError
        return StartedEnvironmentRuntimeV2(staged, resources, files)
    except Exception:
        raise ValueError('environment_runtime_started_unqualified') from None


class _Workloads:
    """Exact-resource HTTPS CAS; no generic command, retry, deletion or adoption."""
    def __init__(self, api: Any, originals: dict[str, Any], targets: dict[str, Any], region: str,
            readers: dict[str, dict[str, Any]]):
        self.api, self.originals, self.targets, self.region = api, originals, targets, region
        self.readers = readers

    def qualify(self) -> None:
        self.api.verify_identity(self.api.policy.host.runtime.binding)

    def _path(self, key: str) -> str:
        doc = self.originals[key]
        return runtime_resource_path(doc) + '/' + str(doc['metadata']['name'])

    def read_workload(self, key: str) -> dict[str, Any]:
        self.qualify()
        actual = self.api._request('GET', self._path(key))
        if actual is None or _key(actual) != key or _uid(actual) != _uid(self.originals[key]):
            raise ValueError('environment_runtime_identity_changed')
        _snapshot(actual)
        return dict(actual)

    def _reader_boundary(self, namespace: str) -> None:
        # A pods=0 quota can hide a dormant Job. Raising it even for one reader
        # must not accidentally let that Job launch unaccounted task Pods.
        owners = {_uid(doc): doc for doc in self.readers.values()}
        for version, kind, resource in (('v1', 'Pod', 'pods'), ('batch/v1', 'Job', 'jobs'),
                ('batch/v1', 'CronJob', 'cronjobs'), ('apps/v1', 'Deployment', 'deployments'),
                ('apps/v1', 'ReplicaSet', 'replicasets'), ('apps/v1', 'StatefulSet', 'statefulsets'),
                ('apps/v1', 'DaemonSet', 'daemonsets')):
            prefix = '/api/v1' if version == 'v1' else '/apis/' + version
            self.qualify()
            result = self.api._request('GET', prefix + '/namespaces/' + namespace + '/' + resource + '?limit=1000')
            if (result is None or result.get('apiVersion') != version or result.get('kind') != kind + 'List'
                    or not isinstance(result.get('metadata', {}).get('resourceVersion'), str)
                    or not result['metadata']['resourceVersion'] or result['metadata'].get('continue')
                    or not isinstance(result.get('items'), list) or len(result['items']) > 1000):
                raise ValueError('environment_runtime_reader_inventory_unavailable')
            seen = set()
            for item in result['items']:
                row = {'apiVersion': version, 'kind': kind, **item}
                uid = _uid(row)
                if (uid in seen or row['metadata'].get('namespace') != namespace
                        or row['kind'] != kind or row['apiVersion'] != version):
                    raise ValueError('environment_runtime_reader_inventory_unqualified')
                seen.add(uid)
                if kind == 'Deployment' and uid in owners and _matches(row, owners[uid], uid):
                    continue
                if kind == 'ReplicaSet' and row.get('spec', {}).get('replicas') == 0:
                    refs = row['metadata'].get('ownerReferences', [])
                    owner = owners.get(refs[0].get('uid')) if len(refs) == 1 else None
                    if (owner is not None and refs[0].get('controller') is True
                            and refs[0].get('kind') == 'Deployment' and refs[0].get('apiVersion') == 'apps/v1'
                            and refs[0].get('name') == owner['metadata']['name']):
                        continue
                raise ValueError('environment_runtime_reader_namespace_not_empty')

    def _patch(self, key: str, before: dict[str, Any], desired: dict[str, Any], *, preview: bool) -> dict[str, Any] | None:
        if desired != self.targets[key] or not _matches(before, self.originals[key], _uid(self.originals[key])):
            raise ValueError('environment_runtime_patch_unqualified')
        version = before['metadata']['resourceVersion']
        if not isinstance(version, str) or not 0 < len(version) <= 128:
            raise ValueError('environment_runtime_version_invalid')
        if desired['kind'] == 'ResourceQuota':
            self._reader_boundary(desired['metadata']['namespace'])
        patches = [{'op': 'test', 'path': '/metadata/uid', 'value': _uid(before)},
            {'op': 'test', 'path': '/metadata/resourceVersion', 'value': version},
            {'op': 'test', 'path': '/spec', 'value': before['spec']}]
        for field in ('labels', 'annotations'):
            if field in desired['metadata']:
                patches.append({'op': 'add', 'path': '/metadata/' + field, 'value': desired['metadata'][field]})
            elif field in before['metadata']:
                patches.append({'op': 'remove', 'path': '/metadata/' + field})
        patches.append({'op': 'replace', 'path': '/spec', 'value': desired['spec']})
        self.qualify()
        with self.api.client.stream('PATCH', self._path(key) + ('?dryRun=All' if preview else ''), json=patches,
                headers={'Content-Type': 'application/json-patch+json'}) as response:
            if response.status_code not in {200, 409, 422} or response.headers.get('content-encoding', 'identity') != 'identity':
                raise ValueError('environment_runtime_patch_unconfirmed')
            content = bytearray()
            for chunk in response.iter_bytes(chunk_size=16384):
                content.extend(chunk)
                if len(content) > 4 * 1024**2:
                    raise ValueError('environment_runtime_patch_unconfirmed')
            value = json.loads(content)
            if not isinstance(value, dict):
                raise ValueError('environment_runtime_patch_unconfirmed')
            if response.status_code in {409, 422}:
                if (value.get('apiVersion') != 'v1' or value.get('kind') != 'Status' or value.get('status') != 'Failure'
                        or value.get('code') != response.status_code or value.get('reason') != {409: 'Conflict', 422: 'Invalid'}[response.status_code]):
                    raise ValueError('environment_runtime_patch_unconfirmed')
                return None
            if _uid(value) != _uid(before):
                raise ValueError('environment_runtime_patch_unconfirmed')
            qualify_runtime_defaulted(desired, value)
            return value

    def preview_workload(self, key: str, before: dict[str, Any], desired: dict[str, Any]) -> dict[str, Any] | None:
        return self._patch(key, before, desired, preview=True)

    def patch_workload(self, key: str, before: dict[str, Any], desired: dict[str, Any]) -> bool:
        return self._patch(key, before, desired, preview=False) is not None

    def workload_ready(self, key: str, expected: dict[str, Any]) -> bool:
        actual = self.read_workload(key)
        if not _matches(actual, expected, _uid(self.originals[key])):
            raise ValueError('environment_runtime_readiness_changed')
        if actual['kind'] == 'ResourceQuota':
            return bool(actual.get('status', {}).get('hard') == actual['spec']['hard'])
        namespace = actual['metadata']['namespace']
        children = self.api._request('GET', '/apis/apps/v1/namespaces/' + namespace + '/replicasets?limit=1000')
        pods = self.api._request('GET', '/api/v1/namespaces/' + namespace + '/pods?limit=1000')
        if children is None or pods is None:
            raise ValueError('environment_runtime_readiness_unavailable')
        if actual['spec']['replicas'] == 1:
            ready = qualify_started_deployment(current=actual, children=children, pods=pods, region=self.region)
        else:
            ready = qualify_closed_workload_drain(original=self.originals[key], desired=expected,
                current=actual, children=children, pods=pods)
        final = self.read_workload(key)
        if _snapshot(final) != _snapshot(actual) or final['metadata']['generation'] != actual['metadata']['generation']:
            raise ValueError('environment_runtime_readiness_changed')
        return ready


async def start_environment_runtime(*, policy: PoolEnrollmentPolicyV2, operation_id: UUID,
        catalog: GitHubCandidateCatalog, kubernetes: KubernetesEnvironmentProvider, cloud: FoundationCloud,
        public_http: httpx.AsyncClient, connect: Connection, workload_connection: WorkloadConnection | None = None) -> dict[str, Any] | None:
    """Connect retained staging to stopped/replaced/started selected processes.

    Runtime-started is not process-settings, task execution or activation proof.
    Every invocation freshly reads the selected enrolled ledger and all stable
    foundation/runtime objects. No legacy journal or peer application is loaded.
    """
    try:
        retained = load_staged_environment_runtime(policy=policy, operation_id=operation_id)
        directory = retained.state_dir / 'start'
        with state._locked_state(directory):
            observation = await asyncio.to_thread(_observe, policy, directory / 'observations', connect)
            if observation is None:
                return None
            snapshot = qualify_inspection(observation['report']['receipt'],
                PoolInspectionV2.model_validate(observation['input']['request']), UUID(observation['input']['authorization']['authorization_id']))
            enrollment = PoolAdministrationReferenceV2.model_validate(retained.plan['enrollment'])
            selected = snapshot.participant
            if (selected is None or selected.binding != policy.participant or selected.profiles != policy.profiles
                    or selected.head.state != 'enrolled' or selected.head.operation_id != enrollment.operation_id):
                raise ValueError('environment_runtime_not_enrolled')
            if catalog.publications.get(policy.application_publication.candidate_id) != policy.application_publication:
                raise ValueError('environment_runtime_publication_unqualified')
            app = await catalog.resolve(policy.application_publication.candidate_id)
            foundation_plan = load_foundation(policy.foundation_dir, policy.foundation)
            if digest(app.candidate) != foundation_plan['publication_sha256'] or digest(app.profile) != foundation_plan['profile_sha256']:
                raise ValueError('environment_runtime_publication_changed')
            declaration = EnvironmentFoundationDeclarationV2.model_validate(foundation_plan['declaration'])
            path, cursor_path = directory / 'plan.json', directory / 'cursor.json'
            anchor = policy.host.anchor_root / ('runtime-start-' + str(operation_id) + '.json')
            if path.exists() or cursor_path.exists() or anchor.exists():
                plan, cursor = _read(path), _read(cursor_path)
                if (set(plan) != {'runtime_sha256', 'originals'} or plan['runtime_sha256'] != digest(retained.plan)
                        or _read(anchor) != {'operation_id': str(operation_id), 'plan_sha256': digest(plan)}
                        or set(cursor) != {'phases'} or set(cursor['phases']) != set(_PHASES)):
                    raise ValueError('environment_runtime_start_history_changed')
            else:
                await reconcile_foundation(state_dir=policy.foundation_dir, reference=policy.foundation,
                    kubernetes=kubernetes, cloud=cloud, observe_only=True)
                originals = {key: doc for key, doc in retained.resources.items() if doc['kind'] == 'Deployment'}
                selected_docs = [retained.plan['workloads'][name] for name in ('service', 'control_plane')]
                selected_docs.append({'kind': 'ResourceQuota', 'metadata': {'name': 'loom-execution-disabled',
                    'namespace': policy.participant.execution_namespace.name}})
                foundation_docs = {_key(doc): doc for docs in foundation_plan['files'].values() for doc in docs}
                for doc in selected_docs:
                    actual = await kubernetes._request('GET', runtime_resource_path(doc) + '/' + doc['metadata']['name'])
                    if actual is None or _uid(actual) != retained.plan['foundation_identities']['k8s:' + _key(doc)]:
                        raise ValueError('environment_runtime_original_unqualified')
                    kubernetes._identity(actual, foundation_docs[_key(doc)],
                        retained.plan['foundation_identities']['k8s:' + _key(doc)])
                    originals[_key(doc)] = _frozen(actual)
                plan = {'runtime_sha256': digest(retained.plan), 'originals': originals}
                cursor = {'phases': dict.fromkeys(_PHASES, 'prepared')}
                state._write_private(path, json.dumps(plan, sort_keys=True).encode())
                state._atomic_json(cursor_path, cursor)
                state._write_private(anchor, json.dumps({'operation_id': str(operation_id), 'plan_sha256': digest(plan)}).encode())
                state._sync_directory(policy.host.anchor_root)
            options = _options(plan, retained, cursor)
            successors = {'k8s:' + key: docs for key, docs in options.items()
                if 'k8s:' + key in retained.plan['foundation_identities']}
            foundation = await reconcile_foundation(state_dir=policy.foundation_dir, reference=policy.foundation,
                kubernetes=kubernetes, cloud=cloud, observe_only=True, _runtime_successors=successors)
            if foundation.identities != retained.plan['foundation_identities']:
                raise ValueError('environment_runtime_foundation_changed')
            for key, doc in retained.resources.items():
                actual = await kubernetes._request('GET', runtime_resource_path(doc) + '/' + doc['metadata']['name'])
                if actual is None or not any(_matches(actual, allowed, _uid(doc)) for allowed in options.get(key, (doc,))):
                    raise ValueError('environment_runtime_staged_resource_changed')
            expires = min(snapshot.valid_until, foundation.observed_at + timedelta(minutes=5), *(row.expires_at for row in policy.machines))
            def qualify() -> None:
                if (not snapshot.observed_at <= datetime.now(UTC) < expires or _policy(policy) != retained.plan['policy']
                        or _read(path) != plan
                        or _read(anchor) != {'operation_id': str(operation_id), 'plan_sha256': digest(plan)}
                        or any(state._private_read(file, limit=8 * 1024**2) != raw for file, raw in retained.files.items())):
                    raise ValueError('environment_runtime_start_evidence_changed')
            @contextmanager
            def connection(before: dict[str, Any], targets: dict[str, Any], namespaces: dict[str, str], check: Callable[[], None]) -> Iterator[DevelopmentRuntimeWorkloadAPI]:
                with _resources(policy, targets, namespaces, check) as api:
                    yield _Workloads(api, before, targets, declaration.platform_config['region'],
                        {key: doc for key, doc in retained.resources.items() if doc['kind'] == 'Deployment'})
            factory = workload_connection or connection
            for phase in _PHASES:
                if cursor['phases'][phase] == 'complete' and phase != 'start':
                    continue
                before, targets = _inputs(plan, retained, phase)
                child = directory / phase
                if cursor['phases'][phase] == 'prepared':
                    if child.exists():
                        raise ValueError('environment_runtime_untracked_transition')
                    cursor['phases'][phase] = 'intent'
                    state._atomic_json(cursor_path, cursor)
                def advance(before: dict[str, Any], targets: dict[str, Any], phase: str, child: Path) -> bool:
                    with factory(before, targets, retained.plan['namespace_uids'], qualify) as api:
                        return _advance_fixed_resources(originals=before, targets=targets, input_digest='sha256:' + digest(plan),
                            phase=phase, api=api, state_dir=child, phases=frozenset(_PHASES),
                            kinds=frozenset({'Deployment', 'ResourceQuota'}), qualify_defaulted=qualify_runtime_defaulted)
                if not await asyncio.to_thread(advance, before, targets, phase, child):
                    return {'status': 'runtime_start_pending', 'phase': phase, 'operation_id': str(operation_id), 'admission_open': False}
                cursor['phases'][phase] = 'complete'
                state._atomic_json(cursor_path, cursor)
            material = _read(policy.foundation_dir / 'material.json')
            token = tomllib.loads(material['loom-admin-secret']['secrets.toml'])['admin']['token']
            public = await _public(public_http, 'https://' + policy.foundation.identity.public_host, token)
            if public != {'status': 'ready', 'postgres': 'ready', 'object_store': 'ready',
                    'environment': policy.participant.environment_class, 'namespace': policy.foundation.identity.application_namespace, 'blockers': []}:
                raise ValueError('environment_runtime_public_unqualified')
            qualify()
            return {'status': 'runtime_started_closed', 'operation_id': str(operation_id),
                'participant_id': str(policy.participant.participant_id), 'plan_sha256': digest(plan), 'admission_open': False}
    except Exception:
        raise ValueError('environment_runtime_start_unqualified') from None

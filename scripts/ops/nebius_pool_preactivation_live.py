"""Protected CLOSED-only catalog refresh using existing authenticated transports.

The sequencer owns the common handoff lock and durable dispatch history. This
adapter exposes only an immutable catalog create, catalog-only manager CAS and
one fixed registry transaction. Staging is GET-only; admission stays CLOSED.
"""
from __future__ import annotations

import asyncio
import copy
import hashlib
import hmac
import json
import secrets
import ssl
from dataclasses import asdict
from pathlib import Path
from typing import Any, Self

import httpx
from scripts.ops import nebius_certificates as private_state
from scripts.ops.nebius_development_runtime_external import (
    qualify_runtime_publication,
    qualify_staging_source_publication,
)
from scripts.ops.nebius_development_runtime_probes import HTTPSDevelopmentRuntimeProbes
from scripts.ops.nebius_development_runtime_readiness import qualify_started_deployment
from scripts.ops.nebius_development_runtime_retained import load_completed_runtime
from scripts.ops.nebius_development_runtime_transition import validate_runtime_transition
from scripts.ops.nebius_ingress_stage import _key, _snapshot, _uid
from scripts.ops.nebius_management_stage import _validate_record
from scripts.ops.nebius_management_switch import _matches
from scripts.ops.nebius_pool_participant_catalog import (
    AppliedCatalogResources,
    HTTPSParticipantCatalogAPI,
    _projection,
    read_applied_catalog,
)
from scripts.ops.nebius_pool_preactivation_database import (
    pool_preactivation_state_sql,
    qualify_preactivation_report,
    refresh_closed_pool_sql,
)
from scripts.ops.nebius_pool_preactivation_operation import (
    RetainedPreactivationCatalog,
    load_preactivation_catalog,
)
from scripts.ops.nebius_pool_preactivation_registry import (
    PreactivationNotDispatchedError,
    validate_preactivation_registry,
)
from scripts.ops.nebius_pool_retirement import qualify_closed_workload_drain
from scripts.ops.nebius_pool_runtime_settings import (
    BOUND_POOL_SETTINGS_COMMAND,
    expected_pool_runtime_settings,
)
from scripts.ops.nebius_staging_pool_attachment import prepare_staging_pool_attachment

from loom.nebius_platform_render import digest
from loom_service.environment_management.candidates import _json
from loom_service.pool_management.participant_refresh import PoolParticipantRefresh


def preactivation_runtime_view(retained: RetainedPreactivationCatalog
        ) -> tuple[dict[Path, bytes], dict[str, tuple[dict[str, Any], ...]]]:
    """Read pending catalog choices without claiming a completed installation.

    A manager intent permits its exact before/after around the CAS. A completed
    child permits only its retained successor. The parent independently prevents
    lost child receipts being recreated and freezes these reads across effects.
    """
    state = retained.state_dir
    paths = [state / 'operation.json', retained.anchor_dir / (
        str(retained.reference.operation_id) + '-pool-preactivation-dispatch.json')]
    files = {path: private_state._private_read(path, limit=4 * 1024**2) for path in paths}
    identity = {'schema': 'loom.pool-preactivation-operation.v1', 'reference': retained.reference.model_dump(mode='json')}
    frame = _json(files[paths[0]])
    if (_json(files[paths[1]]) != identity or set(frame) != {*identity, 'status', 'phases', 'files'}
            or any(frame[key] != value for key, value in identity.items())
            or frame['status'] not in {'pending', 'complete'}
            or set(frame['phases']) != {'catalog', 'manager', 'registry'}
            or any(value not in {'prepared', 'intent', 'complete'} for value in frame['phases'].values())):
        raise ValueError('preactivation runtime frame differs')
    catalog, manager, fingerprint = _projection(retained.catalog_request)
    before = retained.inputs.manager
    choices: dict[str, tuple[dict[str, Any], ...]] = {
        _key(before): (before,), _key(retained.inputs.catalog): (retained.inputs.catalog,)}
    for phase, suffix in (('catalog', 'catalog/stage.json'), ('manager', 'manager/transition.json')):
        path = state / suffix
        if not path.exists() and not path.is_symlink():
            if frame['phases'][phase] == 'complete':
                raise ValueError('preactivation child missing')
            continue
        if frame['phases'][phase] == 'prepared':
            raise ValueError('preactivation child precedes intent')
        files[path] = private_state._private_read(path, limit=4 * 1024**2)
        if (frame['phases'][phase] == 'complete'
                and frame['files'].get(suffix) != hashlib.sha256(files[path]).hexdigest()):
            raise ValueError('preactivation completed child changed')
        record = _json(files[path])
        if phase == 'catalog':
            documents = {_key(catalog): catalog}
            binding = retained.registration.binding
            _validate_record(record, {'schema': 'loom.nebius-management-stage.v1', 'binding': asdict(binding),
                'revision': digest({'documents': documents, 'binding': asdict(binding)}), 'phase': 'participant-catalog'}, documents)
            item = record['resources'][_key(catalog)]
            if frame['phases'][phase] == 'complete' and item['status'] != 'created':
                raise ValueError
            if item['status'] == 'created':
                actual = copy.deepcopy(item['observed'])
                actual['metadata']['uid'] = item['uid']
                choices[_key(catalog)] = (actual,)
        else:
            key = _key(before)
            validate_runtime_transition(record, originals={key: before}, targets={key: manager},
                input_digest=fingerprint, phase='control')
            item = record['resources'][key]
            if frame['phases'][phase] == 'complete' and item['status'] != 'applied':
                raise ValueError
            if item['status'] != 'prepared':
                desired = copy.deepcopy(item['expected'])
                desired['metadata']['uid'] = _uid(before)
                choices[key] = (before, desired) if item['status'] == 'intent' else (desired,)
    if any(private_state._private_read(path, limit=4 * 1024**2) != raw for path, raw in files.items()):
        raise ValueError('preactivation runtime history changed')
    return files, choices


class HTTPSPreactivationOperationAPI:
    def __init__(self, *, retained: RetainedPreactivationCatalog, repo_root: Path, api_server: str,
            ssl_context: ssl.SSLContext, token: str, private_files: dict[Path, bytes] | None = None):
        self.retained = load_preactivation_catalog(runtime=retained.inputs.runtime,
            reference=retained.reference, repo_root=repo_root)
        if self.retained != retained:
            raise ValueError('preactivation connection selection differs')
        self.completed = load_completed_runtime(retained.runtime)
        self.successor = retained.schema_successor
        spec = retained.inputs.refresh.after
        participant, = (row for row in spec.participants if row.environment_class == 'staging')
        self.staging = prepare_staging_pool_attachment(participant=participant,
            observation=retained.inputs.source.observation, candidate=retained.inputs.source.candidate,
            cluster_id=spec.cluster_id, node_group_id=spec.node_group_id, repo_root=repo_root)
        files = dict(private_files or {})
        for path, raw in retained.files.items():
            if path in files and files[path] != raw:
                raise ValueError('preactivation connection history differs')
            files[path] = raw
        self.probes = HTTPSDevelopmentRuntimeProbes(request=retained.runtime, api_server=api_server,
            ssl_context=ssl_context, token=token, private_files=files, schema_successor=self.successor)
        try:
            self.catalog = HTTPSParticipantCatalogAPI(request=retained.catalog_request, state_dir=retained.state_dir,
                api_server=api_server, ssl_context=ssl_context, token=token)
        except Exception:
            self.probes.__exit__()
            raise
        self.manager = self.catalog
        self.registry = self
        self.external_frame: dict[str, Any] | None = None

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *args: object) -> None:
        try:
            self.catalog.__exit__(*args)
        finally:
            self.probes.__exit__(*args)

    async def _external(self) -> None:
        self.probes._private_inputs()
        async with httpx.AsyncClient(trust_env=False, follow_redirects=False, timeout=30) as http:
            await qualify_runtime_publication(request=self.retained.runtime, http=http, schema_successor=self.successor)
            await qualify_staging_source_publication(request=self.retained.runtime, source=self.retained.inputs.source,
                runtime_profile=self.staging.runtime_profile, http=http)
        self.probes._private_inputs()

    def _scope(self, retained: RetainedPreactivationCatalog) -> None:
        if retained != self.retained:
            raise ValueError('preactivation live selection changed')
        self.probes._private_inputs()

    def _staging(self) -> None:
        plurals = {'Namespace': 'namespaces', 'Deployment': 'deployments', 'StatefulSet': 'statefulsets',
            'CronJob': 'cronjobs', 'Service': 'services', 'ConfigMap': 'configmaps', 'Role': 'roles',
            'RoleBinding': 'rolebindings', 'ClusterRole': 'clusterroles', 'ClusterRoleBinding': 'clusterrolebindings'}
        for row in self.staging.resources:
            prefix = '/api/v1' if row['apiVersion'] == 'v1' else '/apis/' + row['apiVersion']
            namespace = row['metadata'].get('namespace')
            path = prefix + ('/namespaces/' + namespace if namespace else '') + '/' + plurals[row['kind']] + '/' + row['metadata']['name']
            actual = self.probes._read(path)
            if not _matches(actual, row, _uid(row)):
                raise ValueError('staging advanced; capture a new preactivation source')

    def _gateway(self) -> None:
        gateway, = (row for row in (self.completed.resources if self.successor is None else self.successor.resources).values()
            if row['kind'] == 'Deployment' and row['metadata']['name'] == 'loom-pool-gateway')
        current = self.probes._get(gateway)
        namespace = gateway['metadata']['namespace']
        children = self.probes._read('/apis/apps/v1/namespaces/' + namespace + '/replicasets?limit=1000')
        pods = self.probes._read('/api/v1/namespaces/' + namespace + '/pods?limit=1000')
        if (not qualify_closed_workload_drain(original=gateway, desired=gateway, current=current, children=children, pods=pods)
                or not _matches(self.probes._get(gateway), current, _uid(gateway))):
            raise ValueError('preactivation gateway is not stopped')

    def qualify(self, retained: RetainedPreactivationCatalog, phase: str) -> None:
        self._scope(retained)
        if phase not in {'preflight', 'catalog', 'manager', 'registry', 'terminal'}:
            raise ValueError
        frame = _json(private_state._private_read(retained.state_dir / 'operation.json'))
        if phase == 'preflight' or frame != self.external_frame:
            asyncio.run(self._external())
            self.external_frame = copy.deepcopy(frame)
        # After durable intent, finish this exact CLOSED successor even if an
        # independent staging rollout advances. Handoff still requires a fresh
        # source/catalog match; another refresh can follow this completed one.
        if frame['phases']['catalog'] == 'prepared':
            self._staging()
        self.probes.inspect(state_dir=self.completed.state_dir, _preactivation_catalog=retained)
        self._gateway()
        state = qualify_preactivation_report(retained.inputs.refresh, self.observe(retained.inputs.refresh), read_only=True)
        if state == 'complete':
            record = validate_preactivation_registry(request=retained.inputs.refresh, state_dir=retained.state_dir / 'registry')
            if record['status'] not in {'intent', 'complete'}:
                raise ValueError('preactivation successor has no retained intent')
        self._scope(retained)

    def _sql(self, request: PoolParticipantRefresh, *, writing: bool) -> dict[str, Any]:
        try:
            self._scope(self.retained)
            if request != self.retained.inputs.refresh:
                raise ValueError('preactivation SQL selection differs')
            namespace = self.retained.registration.binding.namespace
            before = self.probes._database(namespace)
            command = ['psql', '-X', '-qAt', '-v', 'ON_ERROR_STOP=1', '-U', 'postgres', '-d', 'loom', '-c',
                refresh_closed_pool_sql(request, manager_revision=self.probes.manager_revision) if writing
                else pool_preactivation_state_sql(request, manager_revision=self.probes.manager_revision)]
        except Exception:
            if writing:
                raise PreactivationNotDispatchedError('preactivation SQL preflight failed') from None
            raise
        # Once the transport is invoked, every failure is uncertain, including
        # a lost response or a failed backend/authority check afterward.
        report = self.probes._exec(before, 'loom-postgres', command)
        qualify_preactivation_report(request, report, read_only=not writing)
        if _uid(self.probes._database(namespace)) != _uid(before):
            raise ValueError('preactivation SQL backend changed')
        self._scope(self.retained)
        return report

    def observe(self, request: PoolParticipantRefresh) -> dict[str, Any]:
        return self._sql(request, writing=False)

    def commit(self, request: PoolParticipantRefresh) -> dict[str, Any]:
        try:
            if request != self.retained.inputs.refresh:
                raise ValueError
            record = validate_preactivation_registry(request=request, state_dir=self.retained.state_dir / 'registry')
            if record['status'] != 'intent':
                raise ValueError('preactivation SQL dispatch has no intent')
            self.qualify(self.retained, 'registry')
            self.qualify_manager(self.retained, read_applied_catalog(request=self.retained.catalog_request,
                state_dir=self.retained.state_dir))
        except Exception:
            raise PreactivationNotDispatchedError('preactivation commit preflight failed') from None
        return self._sql(request, writing=True)

    def _running_manager(self, resources: AppliedCatalogResources) -> tuple[dict[str, Any], dict[str, Any]]:
        expected = resources.manager
        current = self.probes._get(expected)
        if not _matches(current, expected, _uid(expected)):
            raise ValueError('preactivation manager differs from applied catalog')
        namespace = current['metadata']['namespace']
        children = self.probes._read('/apis/apps/v1/namespaces/' + namespace + '/replicasets?limit=1000')
        pods = self.probes._read('/api/v1/namespaces/' + namespace + '/pods?limit=1000')
        final = self.probes._get(expected)
        if (_snapshot(final) != _snapshot(current) or final['metadata']['generation'] != current['metadata']['generation']
                or not qualify_started_deployment(current=final, children=children, pods=pods,
                    region=self.retained.runtime.database.foundation.inputs.config['region'])):
            raise ValueError('preactivation manager descendants are not ready')
        labels = current['spec']['selector']['matchLabels']
        pod, = (row for row in pods['items']
            if all(row['metadata'].get('labels', {}).get(name) == value for name, value in labels.items()))
        return final, pod

    def qualify_manager(self, retained: RetainedPreactivationCatalog, resources: AppliedCatalogResources) -> None:
        self._scope(retained)
        if resources != read_applied_catalog(request=retained.catalog_request, state_dir=retained.state_dir):
            raise ValueError('preactivation applied catalog differs')
        workload, before = self._running_manager(resources)
        wanted = expected_pool_runtime_settings('manager', workload,
            catalog_sha256=hashlib.sha256(retained.inputs.refresh.after.profiles.model_dump_json().encode()).hexdigest())
        nonce = secrets.token_hex(32)
        response = hmac.new(bytes.fromhex(nonce), json.dumps(wanted, sort_keys=True, separators=(',', ':')).encode(), 'sha256').hexdigest()
        container, = workload['spec']['template']['spec']['containers']
        report = self.probes._exec(before, container['name'], ['python', '-c', BOUND_POOL_SETTINGS_COMMAND, 'manager', nonce, response])
        after_workload, after = self._running_manager(resources)
        if (report != {'status': 'qualified'} or _snapshot(after_workload) != _snapshot(workload)
                or after_workload['metadata']['generation'] != workload['metadata']['generation']
                or self.probes._process_identity(after) != self.probes._process_identity(before)):
            raise ValueError('preactivation manager process settings differ')
        self._scope(retained)

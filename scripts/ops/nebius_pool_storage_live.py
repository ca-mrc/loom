"""Fixed connected correction for one never-used CLOSED pool's storage binding.

The retained parent owns sequencing, locking and intent. This adapter reads
provider identity and existing runtime/database authority, creates one immutable
collector configuration, CASes that collector, and runs one exact SQL update.
It neither touches staging nor opens admission or starts the execution gateway.
"""
from __future__ import annotations

import asyncio
import copy
import hmac
import json
import secrets
import ssl
from contextlib import ExitStack
from pathlib import Path
from typing import Any, Self

import httpx
from scripts.ops import nebius_certificates as private_state
from scripts.ops.nebius_closed_registry import RegistryNotDispatchedError
from scripts.ops.nebius_development_runtime_external import (
    qualify_runtime_cloud,
    qualify_runtime_publication,
)
from scripts.ops.nebius_development_runtime_probes import HTTPSDevelopmentRuntimeProbes
from scripts.ops.nebius_development_runtime_retained import load_completed_runtime
from scripts.ops.nebius_ingress_stage import _key, _snapshot, _uid
from scripts.ops.nebius_management_switch import _matches
from scripts.ops.nebius_pool_retirement import qualify_closed_workload_drain
from scripts.ops.nebius_pool_storage_binding import (
    ClosedPoolStorageRefresh,
    pool_storage_state_sql,
    qualify_storage_report,
    refresh_closed_storage_sql,
)
from scripts.ops.nebius_pool_storage_capacity import (
    BOUND_CLOSED_STORAGE_CAPACITY_COMMAND,
    expected_storage_capacity,
)
from scripts.ops.nebius_pool_storage_operation import (
    AppliedStorageResources,
    RetainedClosedStorage,
    load_closed_storage,
    read_applied_storage,
)
from scripts.ops.nebius_pool_storage_registry import validate_storage_registry
from scripts.ops.nebius_pool_storage_transport import HTTPSStorageCollectorAPI

from loom_service.environment_management.candidates import _json


class _StorageProbes(HTTPSDevelopmentRuntimeProbes):
    """Keep process/database probes on the exact pending collector journal view."""

    def __init__(self, *, storage: RetainedClosedStorage, **kwargs: Any):
        self.storage_correction = copy.deepcopy(storage)
        self._storage_correction_origin = copy.deepcopy(storage)
        super().__init__(request=storage.runtime, **kwargs)

    def _private_inputs(self) -> None:
        if self.storage_correction != self._storage_correction_origin:
            raise ValueError('closed storage probe selection changed')
        super()._private_inputs()

    def _running_workload(self, key: str, state_dir: Path) -> dict[str, Any]:
        return self.inspect(state_dir=state_dir, _storage_correction=self.storage_correction)[key]


class HTTPSClosedStorageOperationAPI:
    def __init__(self, *, retained: RetainedClosedStorage, operator_credentials: Path,
            api_server: str, ssl_context: ssl.SSLContext, token: str,
            private_files: dict[Path, bytes] | None = None):
        self.retained = load_closed_storage(runtime=retained.inputs.runtime, reference=retained.reference)
        if self.retained != retained:
            raise ValueError('closed storage connection selection differs')
        self.origin = copy.deepcopy(retained)
        self.completed = load_completed_runtime(retained.runtime)
        self.operator_credentials = operator_credentials
        files = dict(private_files or {})
        for path, raw in {**retained.files, operator_credentials: private_state._private_read(
                operator_credentials, limit=1024**2)}.items():
            if path in files and files[path] != raw:
                raise ValueError('closed storage connection history differs')
            files[path] = raw
        self.stack = ExitStack()
        try:
            self.probes = self.stack.enter_context(_StorageProbes(storage=retained, api_server=api_server,
                ssl_context=ssl_context, token=token, private_files=files))
        except Exception:
            self.stack.close()
            raise
        self.api_server, self.ssl_context, self.token = api_server, ssl_context, token
        self.children: dict[str, HTTPSStorageCollectorAPI] = {}
        self.registry = self
        self.external_frame: dict[str, Any] | None = None
        self.manager_key = 'Deployment:' + retained.registration.binding.namespace + ':loom-service'

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *args: object) -> None:
        self.stack.close()

    def _child(self, phase: str) -> HTTPSStorageCollectorAPI:
        self._scope(self.retained)
        if phase not in self.children:
            self.children[phase] = self.stack.enter_context(HTTPSStorageCollectorAPI(
                retained=self.retained, phase=phase, qualify=lambda: self.qualify(self.retained, phase),
                api_server=self.api_server, ssl_context=self.ssl_context, token=self.token))
        return self.children[phase]

    @property
    def configuration(self) -> HTTPSStorageCollectorAPI:
        return self._child('configuration')

    def workload(self, phase: str, originals: dict[str, dict[str, Any]],
            targets: dict[str, dict[str, Any]]) -> HTTPSStorageCollectorAPI:
        child = self._child(phase)
        if child.originals != originals or child.targets != targets:
            raise ValueError('closed storage workload selection differs')
        return child

    def _scope(self, retained: RetainedClosedStorage) -> None:
        if retained != self.origin or self.retained != self.origin:
            raise ValueError('closed storage live selection changed')
        self.probes._private_inputs()

    async def _external(self) -> None:
        self._scope(self.retained)
        async with httpx.AsyncClient(trust_env=False, follow_redirects=False, timeout=30) as http:
            await qualify_runtime_publication(request=self.retained.runtime, http=http)
        await qualify_runtime_cloud(request=self.retained.runtime, operator_credentials=self.operator_credentials,
            storage=self.retained.inputs.refresh)
        self._scope(self.retained)

    def _drained(self, expected: dict[str, Any]) -> bool:
        namespace = expected['metadata']['namespace']
        kind = expected['kind']
        current = self.probes._get(expected)
        children = self.probes._read(('/apis/apps/v1/namespaces/' if kind == 'Deployment'
            else '/apis/batch/v1/namespaces/') + namespace + ('/replicasets?limit=1000'
            if kind == 'Deployment' else '/jobs?limit=1000'))
        pods = self.probes._read('/api/v1/namespaces/' + namespace + '/pods?limit=1000')
        return (qualify_closed_workload_drain(original=expected, desired=expected,
            current=current, children=children, pods=pods)
            and _matches(self.probes._get(expected), current, _uid(expected)))

    def qualify(self, retained: RetainedClosedStorage, phase: str) -> None:
        self._scope(retained)
        if phase not in {'preflight', 'pause', 'configuration', 'reconfigure', 'registry', 'resume', 'terminal'}:
            raise ValueError('closed storage phase differs')
        frame = _json(private_state._private_read(retained.state_dir / 'operation.json'))
        if phase == 'preflight' or frame != self.external_frame:
            asyncio.run(self._external())
            self.external_frame = copy.deepcopy(frame)
        observed = self.probes.inspect(state_dir=self.completed.state_dir, _storage_correction=retained)
        gateway, = (row for row in self.completed.resources.values()
            if row['kind'] == 'Deployment' and row['metadata']['name'] == 'loom-pool-gateway')
        if not self._drained(gateway):
            raise ValueError('closed storage gateway is not stopped')
        # Before allowing any post-pause mutation, also reject late descendants
        # of the old collector. Resume may run only after the registry commits.
        if frame['phases']['pause'] == 'complete' and frame['phases']['resume'] == 'prepared':
            collector = observed[_key(retained.collector.original)]
            if collector['spec']['suspend'] is not True or not self._drained(collector):
                raise ValueError('closed storage collector is not drained')
        state = qualify_storage_report(retained.inputs.refresh, self.observe(retained.inputs.refresh), read_only=True)
        if state == 'complete':
            record = validate_storage_registry(request=retained.inputs.refresh, state_dir=retained.state_dir / 'registry')
            if record['status'] not in {'intent', 'complete'}:
                raise ValueError('closed storage successor has no retained intent')
        elif frame['phases']['registry'] == 'complete':
            raise ValueError('closed storage registry reverted')
        self._scope(retained)

    def _sql(self, request: ClosedPoolStorageRefresh, *, writing: bool) -> dict[str, Any]:
        try:
            self._scope(self.retained)
            if request != self.retained.inputs.refresh:
                raise ValueError('closed storage SQL selection differs')
            namespace = self.retained.registration.binding.namespace
            before = self.probes._database(namespace)
            command = ['psql', '-X', '-qAt', '-v', 'ON_ERROR_STOP=1', '-U', 'postgres', '-d', 'loom', '-c',
                refresh_closed_storage_sql(request) if writing else pool_storage_state_sql(request)]
        except Exception:
            if writing:
                raise RegistryNotDispatchedError('closed storage SQL preflight failed') from None
            raise
        report = self.probes._exec(before, 'loom-postgres', command)
        qualify_storage_report(request, report, read_only=not writing)
        if _uid(self.probes._database(namespace)) != _uid(before):
            raise ValueError('closed storage SQL backend changed')
        self._scope(self.retained)
        return report

    def observe(self, request: ClosedPoolStorageRefresh) -> dict[str, Any]:
        return self._sql(request, writing=False)

    def _manager(self) -> None:
        self.probes.qualify_runtime_database(key=self.manager_key, state_dir=self.completed.state_dir)
        self.probes.qualify_runtime_settings(key=self.manager_key, state_dir=self.completed.state_dir)

    def commit(self, request: ClosedPoolStorageRefresh) -> dict[str, Any]:
        try:
            if request != self.retained.inputs.refresh:
                raise ValueError
            record = validate_storage_registry(request=request, state_dir=self.retained.state_dir / 'registry')
            if record['status'] != 'intent':
                raise ValueError('closed storage SQL dispatch has no intent')
            self.qualify(self.retained, 'registry')
            self._manager()
        except Exception:
            raise RegistryNotDispatchedError('closed storage commit preflight failed') from None
        return self._sql(request, writing=True)

    def qualify_publication(self, retained: RetainedClosedStorage, resources: AppliedStorageResources) -> bool:
        self._scope(retained)
        if resources != read_applied_storage(retained):
            raise ValueError('closed storage publication resources differ')
        self._manager()
        component, workload, before = self.probes._running(self.manager_key, self.completed.state_dir)
        wanted = expected_storage_capacity(retained.inputs.refresh.after)
        nonce = secrets.token_hex(32)
        response = hmac.new(bytes.fromhex(nonce), json.dumps(wanted, sort_keys=True, separators=(',', ':')).encode(), 'sha256').hexdigest()
        container, = workload['spec']['template']['spec']['containers']
        report = self.probes._exec(before, container['name'], ['python', '-c', BOUND_CLOSED_STORAGE_CAPACITY_COMMAND,
            str(retained.inputs.refresh.after.pool_id), nonce, response])
        after_component, after_workload, after = self.probes._running(self.manager_key, self.completed.state_dir)
        if (report not in ({'status': 'qualified'}, {'status': 'pending'}) or component != 'manager'
                or after_component != component or _snapshot(after_workload) != _snapshot(workload)
                or after_workload['metadata']['generation'] != workload['metadata']['generation']
                or self.probes._process_identity(after) != self.probes._process_identity(before)):
            raise ValueError('closed storage aggregate process differs')
        self._scope(retained)
        return report == {'status': 'qualified'}

"""Concrete phase-aware HTTPS authority for one CLOSED development runtime.

All child transports share retained identity, private-input, closed-database and
publication/cloud qualification. No operator callback can claim readiness; only
fixed observations may advance the parent. This is not admission or writer
activation, and never operates on staging workloads.
"""
from __future__ import annotations

import asyncio
import copy
import ssl
from contextlib import ExitStack
from pathlib import Path
from typing import Any

import httpx
from scripts.ops import nebius_certificates as private_state
from scripts.ops.nebius_development_catalog_runtime import validate_catalog_runtime_proof
from scripts.ops.nebius_development_runtime_external import (
    qualify_runtime_cloud,
    qualify_runtime_publication,
)
from scripts.ops.nebius_development_runtime_install import (
    DevelopmentRuntimeInstallRequest,
    DevelopmentRuntimePlan,
    _child_path,
    _read,
    _runtime_record,
)
from scripts.ops.nebius_development_runtime_job import read_runtime_job
from scripts.ops.nebius_development_runtime_live import (
    HTTPSDevelopmentRuntimeResources,
    HTTPSDevelopmentRuntimeWorkloads,
)
from scripts.ops.nebius_development_runtime_probes import HTTPSDevelopmentRuntimeProbes
from scripts.ops.nebius_development_runtime_setup import validate_database_runtime_proof
from scripts.ops.nebius_ingress_stage import _key, _uid
from scripts.ops.nebius_management_switch import _matches


class HTTPSDevelopmentRuntimeAPI(HTTPSDevelopmentRuntimeProbes):
    def __init__(self, *, request: DevelopmentRuntimeInstallRequest, api_server: str,
                 ssl_context: ssl.SSLContext, token: str, operator_credentials: Path,
                 private_files: dict[Path, bytes]):
        if (operator_credentials not in private_files
                or private_state._private_read(operator_credentials, limit=1024**2) != private_files[operator_credentials]):
            raise ValueError('development runtime operator connection unqualified')
        super().__init__(request=request, api_server=api_server, ssl_context=ssl_context,
            token=token, private_files=private_files)
        self.operator_credentials = operator_credentials
        self.context, self.token = ssl_context, token
        self.children = ExitStack()
        self.state = Path(self.manager.retained.operation['state_dir']).parent / 'runtime-installation'
        self.anchor = Path(self.manager.retained.operation['anchor_dir'])
        self.frame: dict[str, Any] | None = None
        self.seen_children: set[str] = set()
        self.external_frame: dict[str, Any] | None = None

    def __exit__(self, *args: object) -> None:
        try:
            self.children.close()
        finally:
            super().__exit__(*args)

    async def _external(self) -> None:
        self._private_inputs()
        async with httpx.AsyncClient(trust_env=False, follow_redirects=False, timeout=30) as http:
            await qualify_runtime_publication(request=self.request, http=http)
        await qualify_runtime_cloud(request=self.request, operator_credentials=self.operator_credentials)
        self._private_inputs()

    def _observe(self, *, prechild: str | None = None) -> None:
        self.inspect(state_dir=self.state, _prechild_phase=prechild)
        self.qualify_closed_pool()
        self._private_inputs()

    def qualify(self, *, plan: DevelopmentRuntimePlan, state_dir: Path, record: dict[str, Any]) -> None:
        if (plan != self.plan or state_dir != self.state
                or _runtime_record(self.request, self.plan, self.state, self.anchor) != record):
            raise ValueError('development runtime parent identity differs')
        self.frame = copy.deepcopy(record)
        self.seen_children.update(phase for phase, item in record['phases'].items() if item['status'] != 'prepared')
        # Recheck external authority once per durable parent frame, not once per
        # resource GET. Private/live data and closed admission remain checked
        # on every child boundary, including immediately before a write.
        if self.external_frame != record:
            asyncio.run(self._external())
            self.external_frame = copy.deepcopy(record)
        self._observe()

    def _child_qualify(self, request: DevelopmentRuntimeInstallRequest, phase: str, writing: bool) -> None:
        if request != self.request or self.frame is None or phase not in self.frame['phases']:
            raise ValueError('development runtime child identity differs')
        disk = _read(self.state / 'installation.json')
        if disk['phases'][phase]['status'] != 'started':
            raise ValueError('development runtime child phase not active')
        prechild = None
        path = _child_path(self.state, phase)
        if not path.exists() and not path.is_symlink():
            expected = copy.deepcopy(self.frame)
            if phase in self.seen_children or expected['phases'][phase]['status'] != 'prepared':
                raise ValueError('development runtime child history lost')
            expected['phases'][phase]['status'] = 'started'
            if disk != expected:
                raise ValueError('development runtime phase-start differs')
            prechild = phase
        else:
            self.seen_children.add(phase)
        if _runtime_record(self.request, self.plan, self.state, self.anchor, _prechild_phase=prechild) != disk:
            raise ValueError('development runtime child history differs')
        self._observe(prechild=prechild)

    def resources(self, phase: str) -> HTTPSDevelopmentRuntimeResources:
        return self.children.enter_context(HTTPSDevelopmentRuntimeResources(request=self.request, phase=phase,
            api_server=self.api_server, ssl_context=self.context, token=self.token, private_files=self.private_files,
            qualify_runtime=self._child_qualify))

    def workloads(self, phase: str) -> HTTPSDevelopmentRuntimeWorkloads:
        return self.children.enter_context(HTTPSDevelopmentRuntimeWorkloads(request=self.request, phase=phase,
            api_server=self.api_server, ssl_context=self.context, token=self.token, private_files=self.private_files,
            qualify_runtime=self._child_qualify, observe_ready=self._ready))

    def _ready(self, request: DevelopmentRuntimeInstallRequest, phase: str, original: dict[str, Any],
               expected: dict[str, Any], actual: dict[str, Any]) -> bool:
        self._child_qualify(request, phase, False)
        if actual['kind'] == 'Deployment' and actual['spec']['replicas'] == 1:
            self.qualify_runtime_settings(key=_key(actual), state_dir=self.state)
            self.qualify_runtime_database(key=_key(actual), state_dir=self.state)
        # The HTTPS child already proves workload drain/controller/Pod lineage.
        # Unsuspending an observer is not evidence of an emitted observation;
        # real collector and multi-owner results remain activation acceptance.
        return True

    def _recorded(self, phase: str) -> dict[str, dict[str, Any]]:
        record = _runtime_record(self.request, self.plan, self.state, self.anchor)
        self.qualify(plan=self.plan, state_dir=self.state, record=record)
        child = _read(_child_path(self.state, phase))
        result = {}
        for key, item in child['resources'].items():
            if item['status'] != 'created':
                raise ValueError('development runtime Job phase incomplete')
            actual = self._get(item['observed'])
            if not _matches(actual, item['observed'], item['uid']):
                raise ValueError('development runtime Job identity differs')
            result[key] = actual
        return result

    def report(self, phase: str, state: Path) -> dict[str, Any] | None:
        if phase not in {'database', 'catalog'} or state != self.state / phase:
            raise ValueError('development runtime report scope differs')
        validate = validate_database_runtime_proof if phase == 'database' else validate_catalog_runtime_proof
        return read_runtime_job(client=self.client, read=lambda path: self._request('GET', path),
            recorded=lambda: self._recorded(phase), private_inputs=self._private_inputs,
            region=self.request.database.foundation.inputs.config['region'],
            report_field='database' if phase == 'database' else 'catalog',
            validate=lambda proof: validate(self.request.database, state, proof, _documents=self.plan.fixed[phase]))

    def inspect_runtime(self, *, plan: DevelopmentRuntimePlan, state_dir: Path, record: dict[str, Any]) -> None:
        self.qualify(plan=plan, state_dir=state_dir, record=record)
        if any(item['status'] != 'complete' for item in record['phases'].values()):
            raise ValueError('development runtime final history incomplete')
        for phase in ('database', 'catalog'):
            if self.report(phase, self.state / phase) != record['phases'][phase]['proof']:
                raise ValueError('development runtime final Job proof differs')
        for key, actual in self.inspect(state_dir=self.state).items():
            _uid(actual)
            if actual['kind'] == 'Deployment':
                self.qualify_runtime_settings(key=key, state_dir=self.state)
                self.qualify_runtime_database(key=key, state_dir=self.state)
        asyncio.run(self._external())
        self._observe()

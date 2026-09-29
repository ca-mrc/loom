"""Resumable refresh barriers bound to qualified predecessor history.

The protected entry must qualify predecessor semantics, publication and authority.
This journal preserves their bytes and sequences fixed connected operations; it
does not itself grant installation authority or accept arbitrary manifests.
"""
from __future__ import annotations

import hashlib
import json
import re
from contextlib import AbstractContextManager
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Protocol

from scripts.ops import nebius_certificates as private_state
from scripts.ops.nebius_ingress_stage import _snapshot, _uid
from scripts.ops.nebius_management_material import _uuid
from scripts.ops.nebius_management_refresh import render_refresh
from scripts.ops.nebius_management_refresh_resources import (
    ManagementRefreshResourcesRequest,
    refresh_resources_ready,
    stage_refresh_resources,
)
from scripts.ops.nebius_management_refresh_switch import (
    ManagementRefreshSwitchAPI,
    ManagementRefreshSwitchRequest,
    switch_refresh,
)
from scripts.ops.nebius_management_stage import ManagementStageAPI

from loom.nebius_management_refresh_probe import SCHEMA, RefreshProbeSettings
from loom.nebius_platform_render import digest

_PHASES = ('config', 'manager-probe', 'shared-probe', 'backup', 'migration', 'post-migration-probe')


@dataclass(frozen=True, repr=False)
class ManagementRefreshInstallRequest:
    resources: ManagementRefreshResourcesRequest
    history: dict[Path, str]
    installation_anchor: Path


class ManagementRefreshInstallError(RuntimeError):
    def __init__(self, stage: str):
        super().__init__('management refresh incomplete; preserve recovery evidence')
        self.stage = stage


class ManagementRefreshInstallAPI(Protocol):
    def preflight(self, request: ManagementRefreshInstallRequest) -> None:
        """Qualify completed predecessor, retained identities and current prerequisites."""
        ...

    def resources(self, request: ManagementRefreshResourcesRequest, phase: str) -> AbstractContextManager[ManagementStageAPI]: ...
    def switch_api(self, request: ManagementRefreshSwitchRequest) -> AbstractContextManager[ManagementRefreshSwitchAPI]: ...
    def verify_probe(self, request: ManagementRefreshInstallRequest, phase: str, state_dir: Path) -> dict[str, Any] | None: ...
    def verify_backup(self, request: ManagementRefreshInstallRequest, state_dir: Path) -> dict[str, Any]:
        """Exact backup Job/Pod execution plus retained-bucket object readback."""
        ...

    def verify_public(self, request: ManagementRefreshInstallRequest, state_dir: Path) -> bool:
        """Exact active Deployment/Pods, provisioner health and authenticated HTTPS."""
        ...


def _hash(path: Path) -> str:
    return hashlib.sha256(private_state._private_read(path, limit=4 * 1024**2)).hexdigest()


def _history(request: ManagementRefreshInstallRequest) -> dict[str, str]:
    if not 1 <= len(request.history) <= 128:
        raise ValueError
    result = {}
    for path, checksum in request.history.items():
        if (not path.is_absolute() or path != path.resolve() or path.is_symlink()
                or not re.fullmatch(r'[0-9a-f]{64}', checksum) or _hash(path) != checksum):
            raise ValueError
        result[str(path)] = checksum
    return result


def _proof(request: ManagementRefreshInstallRequest, phase: str, state: Path, proof: Any) -> None:
    """Connected evidence must still bind this phase's recorded Job and settings."""
    record = json.loads(private_state._private_read(state / phase / 'stage.json', limit=4 * 1024**2))
    job, = (item for item in record['resources'].values() if item['desired']['kind'] == 'Job')
    if not isinstance(proof, dict) or proof.get('job_uid') != job['uid']:
        raise ValueError
    _uuid(proof['job_uid'])
    if phase == 'backup':
        if (set(proof) != {'job_uid', 'key', 'sha256', 'bytes'} or type(proof['bytes']) is not int
                or not 0 < proof['bytes'] <= request.resources.switch.render.after.postgres_storage_gi * 1024**3
                or not isinstance(proof['key'], str) or not 0 < len(proof['key']) <= 1024
                or not isinstance(proof['sha256'], str) or not re.fullmatch(r'[0-9a-f]{64}', proof['sha256'])):
            raise ValueError
    else:
        if set(proof) != {'job_uid', 'pod_uid', 'probe'}:
            raise ValueError
        _uuid(proof['pod_uid'])
        config, = (item for item in record['resources'].values() if item['desired']['kind'] == 'ConfigMap')
        settings = RefreshProbeSettings.model_validate_json(config['desired']['data']['probe.json'])
        report = proof['probe']
        if (not isinstance(report, dict)
                or set(report) != {'schema', 'status', 'mode', 'revision', 'operations_checked'}
                or report['schema'] != SCHEMA or report['status'] != 'qualified' or report['mode'] != settings.mode
                or report['revision'] != settings.expected_revision or type(report['operations_checked']) is not int
                or not 0 <= report['operations_checked'] <= 4096
                or (settings.mode == 'shared' and report['operations_checked'] != 0)):
            raise ValueError


def refresh_management(*, request: ManagementRefreshInstallRequest, api: ManagementRefreshInstallAPI,
                       state_dir: Path, anchor_dir: Path) -> dict[str, Any]:
    """Never restart a lost child journal or activate before all runtime barriers."""
    stage = 'recovery'
    try:
        state, anchor = state_dir.absolute(), anchor_dir.absolute()
        paths = (state, anchor, request.installation_anchor)
        if (any(not path.is_absolute() or path != path.resolve() for path in paths)
                or not request.installation_anchor.is_dir()
                or any(a == b or a in b.parents or b in a.parents for index, a in enumerate(paths) for b in paths[index + 1:])):
            raise ValueError
        resources = request.resources
        switch = resources.switch
        _uuid(str(switch.operation_id))
        rendered = render_refresh(switch.render)
        shared = switch.render.after.installation.applications
        assert shared is not None
        with private_state._locked_state(request.installation_anchor), private_state._locked_state(anchor):
            history = _history(request)
            identity = {'schema': 'loom.nebius-management-refresh-install.v1', 'state_dir': str(state),
                'operation_id': str(switch.operation_id), 'binding': asdict(resources.binding),
                'input_digest': digest({'history': history, 'before': switch.render.before.model_dump(mode='json'),
                    'after': switch.render.after.model_dump(mode='json'), 'active': switch.render.active,
                    'candidate': switch.render.candidate, 'profile': switch.render.profile,
                    'shared_namespace_uid': resources.shared_namespace_uid, 'manager_revision': resources.manager_revision,
                    'target_manager_revision': resources.target_manager_revision, 'installation_anchor': str(request.installation_anchor)})}
            marker, journal = anchor / (str(switch.operation_id) + '.json'), state / 'refresh.json'
            if marker.exists() or marker.is_symlink():
                if json.loads(private_state._private_read(marker)) != identity:
                    raise ValueError
                record = json.loads(private_state._private_read(journal, limit=1024**2))
                if (set(record) != {*identity, 'phases', 'switch_started', 'activation_started', 'completion_sha256'}
                        or any(record[key] != value for key, value in identity.items())
                        or set(record['phases']) != set(_PHASES[:len(record['phases'])])
                        or type(record['switch_started']) is not bool or type(record['activation_started']) is not bool):
                    raise ValueError
                for phase, item in record['phases'].items():
                    child = state / phase / 'stage.json'
                    if (set(item) != {'sha256', 'proof'} or not child.is_file() or child.is_symlink()
                            or (item['sha256'] is not None and _hash(child) != item['sha256'])):
                        raise ValueError
                if record['switch_started'] and not (state / 'switch/cutover.json').is_file():
                    raise ValueError
                if record['activation_started'] and (not record['switch_started']
                        or set(record['phases']) != set(_PHASES)
                        or any(item['sha256'] is None for item in record['phases'].values())
                        or any(record['phases'][phase]['proof'] is None for phase in ('manager-probe', 'shared-probe', 'backup', 'post-migration-probe'))):
                    raise ValueError
                if record['completion_sha256'] is not None and (not record['activation_started']
                        or _hash(state / 'completion.json') != record['completion_sha256']):
                    raise ValueError
            else:
                if state.exists() or state.is_symlink():
                    raise ValueError
                record = {**identity, 'phases': {}, 'switch_started': False, 'activation_started': False, 'completion_sha256': None}
            stage = 'prerequisites'
            api.preflight(request)
            stage = 'recovery'
            if not marker.exists():
                private_state._atomic_json(marker, identity)
            with private_state._locked_state(state):
                def save() -> None:
                    private_state._atomic_json(journal, record)

                save()
                result = {'operation_id': str(switch.operation_id), 'installation_id': resources.binding.installation_id,
                    'namespace_uid': resources.binding.namespace_uid, 'revision': rendered.revision}

                def pending(phase: str) -> dict[str, Any]:
                    return {**result, 'status': 'pending', 'phase': phase}

                def resources_phase(phase: str) -> bool:
                    nonlocal stage
                    stage = phase
                    if phase not in record['phases']:
                        record['phases'][phase] = {'sha256': None, 'proof': None}
                        save()  # Once started, even missing child state may not be reset.
                    with api.resources(resources, phase) as connected:
                        stage_refresh_resources(request=resources, phase=phase, api=connected, state_dir=state / phase)
                        checksum = _hash(state / phase / 'stage.json')
                        item = record['phases'][phase]
                        if item['sha256'] not in (None, checksum):
                            raise ValueError
                        item['sha256'] = checksum
                        save()
                        if not refresh_resources_ready(request=resources, phase=phase, api=connected, state_dir=state / phase):
                            return False
                    proof = None
                    if phase.endswith('probe'):
                        proof = api.verify_probe(request, phase, state / phase)
                        if proof is None:
                            return False
                    elif phase == 'backup':
                        proof = api.verify_backup(request, state / phase)
                    if phase.endswith('probe') or phase == 'backup':
                        _proof(request, phase, state, proof)
                    if item['proof'] is not None and item['proof'] != proof:
                        raise ValueError
                    item['proof'] = proof
                    save()
                    return True

                if not resources_phase('config'):
                    return pending('config')
                with api.switch_api(switch) as connected_switch:
                    if not record['activation_started']:
                        stage = 'retire'
                        record['switch_started'] = True
                        save()
                        if not switch_refresh(request=switch, api=connected_switch, state_dir=state / 'switch', activate=False):
                            return pending('retire')
                    for phase in _PHASES[1:]:
                        if not resources_phase(phase):
                            return pending(phase)
                    stage = 'activate'
                    record['activation_started'] = True
                    save()
                    if not switch_refresh(request=switch, api=connected_switch, state_dir=state / 'switch', activate=True):
                        return pending('activate')
                    active = connected_switch.read()
                stage = 'public'
                ready = api.verify_public(request, state)
                if ready is False:
                    return pending('public')
                if ready is not True:
                    raise ValueError
                _history(request)
                stage = 'completion'
                receipt = {**identity, 'status': 'management_refreshed', 'revision': rendered.revision,
                    'phases': record['phases'], 'active_uid': _uid(active), 'active': _snapshot(active),
                    'switch_sha256': _hash(state / 'switch/cutover.json'), 'history': history,
                    'manager_revision': resources.target_manager_revision, 'shared_revision': shared.shared.schema_revision}
                path = state / 'completion.json'
                if path.exists() or path.is_symlink():
                    if json.loads(private_state._private_read(path, limit=4 * 1024**2)) != receipt:
                        raise ValueError
                else:
                    private_state._atomic_json(path, receipt)
                record['completion_sha256'] = _hash(path)
                save()
                return {**result, 'status': 'management_refreshed'}
    except Exception:
        raise ManagementRefreshInstallError(stage) from None

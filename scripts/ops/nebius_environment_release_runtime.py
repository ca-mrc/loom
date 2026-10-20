"""Fixed release stop/replace/start projections and UID/resourceVersion CAS."""
from __future__ import annotations

import copy
import json
from pathlib import Path
from typing import Any

from scripts.ops.nebius_development_runtime_transition import validate_runtime_transition
from scripts.ops.nebius_environment_boundary_live import EnvironmentBoundaryWorkloads
from scripts.ops.nebius_environment_foundation import _read
from scripts.ops.nebius_environment_runtime_install import qualify_runtime_defaulted
from scripts.ops.nebius_environment_runtime_start import _Workloads
from scripts.ops.nebius_ingress_stage import _snapshot, _uid
from scripts.ops.nebius_management_switch import _matches

from loom_service.pool_management.capacity import digest

PHASES = ('stop', 'replace', 'start')


def release_inputs(plan: dict[str, Any], directory: Path, phase: str) -> tuple[dict[str, Any], dict[str, Any]]:
    originals, following = plan['originals'], plan['targets']
    def previous(name: str) -> dict[str, Any]:
        record = _read(directory / name / 'transition.json')
        if any(row['status'] != 'applied' for row in record['resources'].values()):
            raise ValueError('environment_release_predecessor_incomplete')
        return {key: {**copy.deepcopy(row['expected']), 'metadata': {**row['expected']['metadata'],
            'uid': _uid(originals[key])}} for key, row in record['resources'].items()}
    if phase == 'stop':
        before = {key: originals[key] for key in following if originals[key]['kind'] == 'Deployment'}
        targets = {key: _snapshot(doc) for key, doc in before.items()}
        for doc in targets.values():
            doc['spec']['replicas'] = 0
    elif phase == 'replace':
        before = previous('stop') | {key: originals[key] for key in following if originals[key]['kind'] != 'Deployment'}
        targets = {key: _snapshot(doc) for key, doc in following.items()}
        for doc in targets.values():
            if doc['kind'] == 'Deployment':
                doc['spec']['replicas'] = 0
    elif phase == 'start':
        before = {key: doc for key, doc in previous('replace').items() if doc['kind'] == 'Deployment'}
        targets = {key: _snapshot(doc) for key, doc in before.items()}
        for doc in targets.values():
            doc['spec']['replicas'] = 1
    else:
        raise ValueError('environment_release_phase_invalid')
    return copy.deepcopy(before), targets


def release_options(plan: dict[str, Any], directory: Path, cursor: dict[str, Any]) -> dict[str, tuple[dict[str, Any], ...]]:
    options: dict[str, tuple[dict[str, Any], ...]] = {key: (doc,) for key, doc in plan['originals'].items()}
    incomplete = False
    if set(cursor) != set(PHASES):
        raise ValueError('environment_release_cursor_changed')
    for phase in PHASES:
        status = cursor[phase]
        path = directory / phase / 'transition.json'
        if status not in {'prepared', 'intent', 'complete'} or (incomplete and status != 'prepared'):
            raise ValueError('environment_release_cursor_changed')
        incomplete |= status != 'complete'
        if status == 'prepared':
            if path.exists():
                raise ValueError('environment_release_untracked_child')
            continue
        # Parent intent precedes the child's first journal write. A crash in
        # that gap is a known no-write state, not missing mutation history.
        if not path.exists() and status == 'intent':
            continue
        before, targets = release_inputs(plan, directory, phase)
        record = _read(path)
        validate_runtime_transition(record, originals=before, targets=targets, input_digest='sha256:' + digest(plan),
            phase=phase, qualify_defaulted=qualify_runtime_defaulted)
        for key, item in record['resources'].items():
            if status == 'complete' and item['status'] != 'applied':
                raise ValueError('environment_release_child_incomplete')
            if item['status'] != 'prepared':
                desired = copy.deepcopy(item['expected'])
                desired['metadata']['uid'] = _uid(before[key])
                options[key] = (desired,) if item['status'] == 'applied' else (before[key], desired)
    return options


class EnvironmentReleaseWorkloads(EnvironmentBoundaryWorkloads):
    """Existing Deployment CAS plus an exact ConfigMap-data CAS."""
    def _patch(self, key: str, before: dict[str, Any], desired: dict[str, Any], *, preview: bool) -> dict[str, Any] | None:
        if desired['kind'] != 'ConfigMap':
            return _Workloads._patch(self, key, before, desired, preview=preview)
        if desired != self.targets[key] or not _matches(before, self.originals[key], _uid(self.originals[key])):
            raise ValueError('environment_release_config_unqualified')
        version = before['metadata']['resourceVersion']
        if not isinstance(version, str) or not 0 < len(version) <= 128:
            raise ValueError('environment_release_version_unqualified')
        patches = [{'op': 'test', 'path': '/metadata/uid', 'value': _uid(before)},
            {'op': 'test', 'path': '/metadata/resourceVersion', 'value': version},
            {'op': 'test', 'path': '/data', 'value': before['data']},
            {'op': 'replace', 'path': '/data', 'value': desired['data']}]
        self.qualify()
        with self.api.client.stream('PATCH', self._path(key) + ('?dryRun=All' if preview else ''), json=patches,
                headers={'Content-Type': 'application/json-patch+json'}) as response:
            if response.status_code not in {200, 409, 422} or response.headers.get('content-encoding', 'identity') != 'identity':
                raise ValueError('environment_release_patch_unconfirmed')
            raw = bytearray()
            for chunk in response.iter_bytes(chunk_size=16384):
                raw.extend(chunk)
                if len(raw) > 4 * 1024**2:
                    raise ValueError('environment_release_patch_unconfirmed')
            value = json.loads(raw)
            if response.status_code != 200:
                if (value.get('apiVersion') != 'v1' or value.get('kind') != 'Status' or value.get('status') != 'Failure'
                        or value.get('code') != response.status_code or value.get('reason') != {409: 'Conflict', 422: 'Invalid'}[response.status_code]):
                    raise ValueError('environment_release_patch_unconfirmed')
                return None
            if _uid(value) != _uid(before):
                raise ValueError('environment_release_patch_unconfirmed')
            qualify_runtime_defaulted(desired, value)
            return dict(value)

    def workload_ready(self, key: str, expected: dict[str, Any]) -> bool:
        if expected['kind'] == 'Deployment':
            return _Workloads.workload_ready(self, key, expected)
        if expected['kind'] not in {'ConfigMap', 'ValidatingAdmissionPolicy'}:
            raise ValueError('environment_release_resource_unqualified')
        if not _matches(self.read_workload(key), expected, _uid(self.originals[key])):
            raise ValueError('environment_release_resource_changed')
        return True

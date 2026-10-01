"""Read-only qualification of a definitely pre-migration failed refresh.

This proof never authorizes a retry of the old operation. The protected entry
binds it to a new operation; connected preflight must still verify the fixed live
resources and terminal failed Job before adopting the stopped manager.
"""
from __future__ import annotations

import copy
import hashlib
import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from pydantic import BaseModel, ConfigDict, Field
from scripts.ops import nebius_certificates as private_state
from scripts.ops.nebius_ingress_stage import _uid
from scripts.ops.nebius_management_gateway import validate_operation
from scripts.ops.nebius_management_refresh_install import (
    ManagementRefreshInstallRequest,
    _history,
    _identity,
    _proof,
)
from scripts.ops.nebius_management_refresh_resources import _revision, refresh_documents
from scripts.ops.nebius_management_refresh_switch import (
    refresh_initial,
    refresh_switch_identity,
    refresh_target,
)
from scripts.ops.nebius_management_stage import (
    _comparison_snapshot,
    _qualified_defaulted,
    _validate_record,
)


class SupersededRefreshV1(BaseModel):
    model_config = ConfigDict(extra='forbid', frozen=True, hide_input_in_errors=True)

    operation: dict[str, Any]
    refresh_sha256: str = Field(pattern=r'^[0-9a-f]{64}$')
    switch_sha256: str = Field(pattern=r'^[0-9a-f]{64}$')


@dataclass(frozen=True, repr=False)
class FailedRefreshProof:
    selector: SupersededRefreshV1
    request: ManagementRefreshInstallRequest
    history: dict[Path, str]
    documents: dict[str, list[dict[str, Any]]]
    failed_phase: str
    failed_job_uid: str
    stopped: dict[str, Any]


def load_failed_refresh(request: ManagementRefreshInstallRequest, selector: SupersededRefreshV1) -> FailedRefreshProof:
    """Require an exact failed probe prefix; never create locks or state files."""
    try:
        selector = SupersededRefreshV1.model_validate(selector.model_dump())
        operation = selector.operation
        validate_operation(operation)
        resources, binding = request.resources, request.resources.binding
        switch = resources.switch
        state, anchor = Path(operation['state_dir']), Path(operation['anchor_dir'])
        source = _history(request)
        if (operation['schema'] != 'loom.nebius-management-refresh-operation.v1'
                or operation['operation_id'] != str(switch.operation_id)
                or operation['installation_id'] != binding.installation_id or operation['namespace'] != binding.namespace
                or operation['candidate'] != switch.render.candidate.get('candidate_sha')
                or source.get(operation['inputs_path']) != operation['inputs_sha256']):
            raise ValueError
        history = dict(request.history)

        def read(path: Path, expected: str | None = None) -> dict[str, Any]:
            if not path.is_absolute() or path != path.resolve() or path.is_symlink():
                raise ValueError
            raw = private_state._private_read(path, limit=4 * 1024**2)
            checksum = hashlib.sha256(raw).hexdigest()
            if expected is not None and checksum != expected:
                raise ValueError
            history[path] = checksum
            value = json.loads(raw)
            if not isinstance(value, dict):
                raise ValueError
            return value

        identity = _identity(request, state, source)
        parent = read(state / 'refresh.json', selector.refresh_sha256)
        if (read(anchor / (str(switch.operation_id) + '.json')) != identity
                or set(parent) != {*identity, 'phases', 'switch_started', 'activation_started', 'completion_sha256'}
                or any(parent[key] != value for key, value in identity.items())
                or parent['switch_started'] is not True or parent['activation_started'] is not False
                or parent['completion_sha256'] is not None):
            raise ValueError
        phases = parent['phases']
        if set(phases) == {'config', 'manager-probe'}:
            failed_phase = 'manager-probe'
        elif set(phases) == {'config', 'manager-probe', 'shared-probe'}:
            failed_phase = 'shared-probe'
        else:
            raise ValueError
        forbidden = ['backup', 'migration', 'post-migration-probe', 'completion.json']
        if failed_phase == 'manager-probe':
            forbidden.append('shared-probe')
        if any((state / name).exists() or (state / name).is_symlink() for name in forbidden):
            raise ValueError
        documents: dict[str, list[dict[str, Any]]] = {}
        for phase, item in phases.items():
            if set(item) != {'sha256', 'proof'} or not isinstance(item['sha256'], str):
                raise ValueError
            journal = read(state / phase / 'stage.json', item['sha256'])
            desired = refresh_documents(resources, phase)
            _validate_record(journal, {'schema': 'loom.nebius-management-stage.v1', 'binding': asdict(binding),
                'revision': _revision(resources, desired), 'phase': 'refresh-' + phase}, desired)
            documents[phase] = []
            for entry in journal['resources'].values():
                if entry['status'] != 'created':
                    raise ValueError
                observed = copy.deepcopy(entry['observed'])
                observed['metadata']['uid'] = entry['uid']
                _uid(observed)
                if (_comparison_snapshot(observed) != entry['expected']
                        or _qualified_defaulted(entry['desired'], observed) != entry['expected']):
                    raise ValueError
                documents[phase].append(observed)
            if phase == 'config' or phase == failed_phase:
                if item['proof'] is not None:
                    raise ValueError
            else:
                _proof(request, phase, state, item['proof'])
        cutover = read(state / 'switch/cutover.json', selector.switch_sha256)
        if cutover != {**refresh_switch_identity(switch, state / 'switch'),
                'original': refresh_initial(switch), 'phase': 'stopped', 'active': None}:
            raise ValueError
        job, = (doc for doc in documents[failed_phase] if doc['kind'] == 'Job')
        stopped = refresh_target(switch, 'retire')
        stopped['metadata']['uid'] = _uid(switch.render.active)
        if len(history) > 128:
            raise ValueError
        return FailedRefreshProof(selector, request, history, documents, failed_phase, _uid(job), stopped)
    except Exception:
        raise ValueError('refresh_supersession_unqualified') from None

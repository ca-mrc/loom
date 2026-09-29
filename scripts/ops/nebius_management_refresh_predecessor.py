"""Qualify immutable completed predecessor state without replaying its installer.

This is private-input validation, not current live-state or publication proof.
The connected refresh must re-read all retained identities and its exact manager.
"""
from __future__ import annotations

import copy
import hashlib
import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field
from scripts.ops import nebius_certificates as private_state
from scripts.ops.nebius_application_setup import _documents, _revision
from scripts.ops.nebius_ingress_stage import _uid
from scripts.ops.nebius_management_entry import (
    PrivateInputs,
    UpgradePrivateInputs,
    load_upgrade_inputs,
)
from scripts.ops.nebius_management_install import _journal_names
from scripts.ops.nebius_management_material import _uuid
from scripts.ops.nebius_management_stage import (
    _comparison_snapshot,
    _qualified_defaulted,
    _validate_record,
)
from scripts.ops.nebius_management_switch import (
    ManagementSwitchRequest,
    _desired,
    _matches,
    _stable,
    _target,
)
from scripts.ops.nebius_management_upgrade import _STAGES, ManagementUpgradeRequest, _original

from loom.nebius_platform_render import digest
from loom_service.environment_management.deployment import ManagementDeployment, render_management


class UpgradePredecessorV1(BaseModel):
    model_config = ConfigDict(extra='forbid', frozen=True, hide_input_in_errors=True)

    kind: Literal['upgrade'] = 'upgrade'
    operation: dict[str, Any]
    state_sha256: str = Field(pattern=r'^[0-9a-f]{64}$')
    switch_sha256: str = Field(pattern=r'^[0-9a-f]{64}$')


@dataclass(frozen=True, repr=False)
class CompletedUpgrade:
    selector: UpgradePredecessorV1
    inputs: UpgradePrivateInputs
    upgrade: ManagementUpgradeRequest
    original_inputs: PrivateInputs
    ingress: dict[str, Any]
    deployment: ManagementDeployment
    active: dict[str, Any]
    history: dict[Path, str]
    retained: dict[str, dict[str, Any]]


def load_completed_upgrade(selector: UpgradePredecessorV1) -> CompletedUpgrade:
    """Read the completed initial install, upgrade, staged resources and cutover."""
    try:
        selector = UpgradePredecessorV1.model_validate(selector.model_dump())
        inputs, upgrade, original_inputs, ingress = load_upgrade_inputs(selector.operation)
        setup = upgrade.setup
        state, anchor = Path(selector.operation['state_dir']), Path(selector.operation['anchor_dir'])
        history: dict[Path, str] = {}

        def read(path: Path, expected: str | None = None) -> bytes:
            if not path.is_absolute() or path != path.resolve():
                raise ValueError
            raw = private_state._private_read(path, limit=4 * 1024**2)
            checksum = hashlib.sha256(raw).hexdigest()
            if expected is not None and checksum != expected:
                raise ValueError
            history[path] = checksum
            return raw

        original, original_digest = _original(upgrade)
        read(upgrade.original_state / 'installation.json', original_digest)
        original_history = json.loads(read(upgrade.original_state / 'installation.json'))
        read(upgrade.original_anchor / (setup.binding.installation_id + '.json'))
        for phase, item in original_history['phases'].items():
            for name in _journal_names(phase):
                read(upgrade.original_state / phase / name, item['journals'][name])
        read(Path(inputs.original_operation['inputs_path']), inputs.original_operation['inputs_sha256'])
        read(Path(selector.operation['inputs_path']), selector.operation['inputs_sha256'])
        for group in original_inputs.material_files.values():
            for path in group.values():
                read(path)
        for path in inputs.material_files.values():
            read(path)
        record = json.loads(read(state / 'upgrade.json', selector.state_sha256))
        switch = json.loads(read(state / 'switch/switch.json', selector.switch_sha256))
        rendered = render_management(setup.deployment, candidate=setup.candidate, profile=setup.profile, repo_root=setup.repo_root)
        identity = {'schema': 'loom.nebius-management-upgrade.v1', 'state_dir': str(state),
            'binding': asdict(setup.binding), 'original_installation_sha256': original_digest,
            'input_digest': digest({'revision': rendered.revision, 'shared_namespace_uid': setup.shared_namespace_uid,
                'material': asdict(setup.material) if setup.material is not None else None})}
        started = json.loads(read(anchor / (setup.binding.installation_id + '.json')))
        if set(started) != {*identity, 'operation_id'} or any(started[key] != value for key, value in identity.items()):
            raise ValueError
        _uuid(started['operation_id'])
        if (set(record) != {*started, 'phases', 'switch_started', 'activation_started'}
                or any(record[key] != value for key, value in started.items())
                or record['switch_started'] is not True or record['activation_started'] is not True
                or set(record['phases']) != set(_STAGES)):
            raise ValueError
        retained = {}
        for phase in _STAGES:
            if set(record['phases'][phase]) != {'sha256'} or record['phases'][phase]['sha256'] is None:
                raise ValueError
            journal = json.loads(read(state / phase / 'stage.json', record['phases'][phase]['sha256']))
            documents = _documents(setup, phase)
            _validate_record(journal, {'schema': 'loom.nebius-management-stage.v1', 'binding': asdict(setup.binding),
                'revision': _revision(setup, documents), 'phase': 'application-' + phase}, documents)
            for key, item in journal['resources'].items():
                if item['status'] != 'created':
                    raise ValueError
                observed = copy.deepcopy(item['observed'])
                observed['metadata']['uid'] = item['uid']
                _uid(observed)
                if _comparison_snapshot(observed) != item['expected'] or key in retained:
                    raise ValueError
                retained[key] = observed
        target, revision = _target(ManagementSwitchRequest(setup, original))
        switch_identity = {'schema': 'loom.nebius-management-switch.v1', 'binding': asdict(setup.binding),
            'shared_namespace_uid': setup.shared_namespace_uid, 'revision': revision,
            'original_uid': _uid(original), 'original_digest': digest(_stable(original)),
            'material_digest': digest(asdict(setup.material) if setup.material is not None else None)}
        if (set(switch) != {*switch_identity, 'original', 'operation_id', 'phase', 'active'}
                or any(switch[key] != value for key, value in switch_identity.items())
                or switch['phase'] != 'active' or not _matches(switch['original'], original, _uid(original))):
            raise ValueError
        _uuid(switch['operation_id'])
        wanted = _desired(switch['original'], target, 'activate', switch['operation_id'])
        if _qualified_defaulted(wanted, switch['active']) != switch['active']:
            raise ValueError
        active = copy.deepcopy(switch['active'])
        active['metadata']['uid'] = switch['original_uid']
        return CompletedUpgrade(selector, inputs, upgrade, original_inputs, ingress,
            setup.deployment, active, history, retained)
    except Exception:
        raise ValueError('refresh_predecessor_unqualified') from None

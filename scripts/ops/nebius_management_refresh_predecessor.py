"""Qualify immutable completed predecessor state without replaying its installer.

This is private-input validation, not current live-state or publication proof.
The connected refresh must re-read all retained identities and its exact manager.
"""
from __future__ import annotations

import copy
import hashlib
import json
import re
from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import asdict, dataclass, replace
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal
from uuid import UUID

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
from scripts.ops.nebius_management_refresh import (
    ManagementRefreshRenderRequest,
    _configuration,
    render_refresh,
)
from scripts.ops.nebius_management_refresh_install import (
    _PHASES,
    ManagementRefreshInstallRequest,
    _proof,
    refresh_contract,
)
from scripts.ops.nebius_management_refresh_resources import (
    ManagementRefreshResourcesRequest,
    refresh_documents,
)
from scripts.ops.nebius_management_refresh_resources import (
    _revision as refresh_revision,
)
from scripts.ops.nebius_management_refresh_switch import (
    ManagementRefreshSwitchRequest,
    refresh_initial,
    refresh_switch_identity,
    refresh_target,
)
from scripts.ops.nebius_management_stage import (
    _canonical_quantities,
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

if TYPE_CHECKING:
    from scripts.ops.nebius_pool_predecessor import CompletedPoolCutover

_PREDECESSORS: ContextVar[tuple[tuple[str, str], ...]] = ContextVar('nebius_predecessors', default=())


@contextmanager
def _predecessor_scope(kind: str, operation: str) -> Iterator[None]:
    """Bound cross-kind ancestry; ordinary refreshes still load no ancestors."""
    _uuid(operation)
    identity = (kind, operation)
    prior = _PREDECESSORS.get()
    if identity in prior or len(prior) >= 8:
        raise ValueError('predecessor ancestry is cyclic or too deep')
    token = _PREDECESSORS.set((*prior, identity))
    try:
        yield
    finally:
        _PREDECESSORS.reset(token)


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


class RefreshPredecessorV1(BaseModel):
    model_config = ConfigDict(extra='forbid', frozen=True, hide_input_in_errors=True)

    kind: Literal['refresh'] = 'refresh'
    operation_id: UUID
    inputs_path: Path
    inputs_sha256: str = Field(pattern=r'^[0-9a-f]{64}$')
    state_dir: Path
    anchor_dir: Path
    completion_sha256: str = Field(pattern=r'^[0-9a-f]{64}$')


@dataclass(frozen=True, repr=False)
class CompletedRefresh:
    selector: RefreshPredecessorV1
    deployment: ManagementDeployment
    active: dict[str, Any]
    resources: ManagementRefreshResourcesRequest
    history: dict[Path, str]
    retained: dict[str, dict[str, Any]]
    pool_baseline: CompletedPoolCutover | None = None


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


def load_completed_refresh(selector: RefreshPredecessorV1, *, original: CompletedUpgrade) -> CompletedRefresh:
    """Qualify bounded root/pool/immediate evidence without replaying an upgrade."""
    try:
        with _predecessor_scope('refresh', str(selector.operation_id)):
            return _load_completed_refresh(selector, original=original)
    except Exception:
        raise ValueError('refresh_predecessor_unqualified') from None


def _load_completed_refresh(selector: RefreshPredecessorV1, *, original: CompletedUpgrade) -> CompletedRefresh:
    """Validate a frozen completion, retaining root/pool/immediate evidence only.

    The protected selector's completion hash freezes prior ancestry. Do not load
    ordinary ancestors recursively: this receipt establishes its own barriers.
    Final runtime must satisfy the original fixed authority/material or the
    independently qualified pool baseline, never an arbitrary before snapshot.
    Live Deployment and resource qualification remains mandatory before writes.
    """
    try:
        selector = RefreshPredecessorV1.model_validate(selector.model_dump())
        operation = str(selector.operation_id)
        _uuid(operation)
        root = Path(original.selector.operation['inputs_path']).parent.parent
        directory = root / 'refresh' / operation
        if (selector.inputs_path != directory / 'inputs.json' or selector.state_dir != directory / 'state'
                or selector.anchor_dir != directory / 'anchor'):
            raise ValueError
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

        for path, checksum in original.history.items():
            read(path, checksum)
        read(selector.inputs_path, selector.inputs_sha256)
        state = selector.state_dir
        receipt = json.loads(read(state / 'completion.json', selector.completion_sha256))
        contract, ancestry = receipt['contract'], receipt['history']
        if (not isinstance(ancestry, dict) or not 1 <= len(ancestry) <= 128
                or any(not isinstance(path, str) or not Path(path).is_absolute()
                    or not isinstance(checksum, str) or re.fullmatch(r'[0-9a-f]{64}', checksum) is None
                    for path, checksum in ancestry.items())
                or any(ancestry.get(str(path)) != checksum for path, checksum in history.items()
                    if path != state / 'completion.json')):
            raise ValueError
        setup = original.upgrade.setup
        render = ManagementRefreshRenderRequest(ManagementDeployment.model_validate(contract['before']),
            ManagementDeployment.model_validate(contract['after']), contract['active'],
            contract['candidate'], contract['profile'], setup.repo_root)
        resources = ManagementRefreshResourcesRequest(ManagementRefreshSwitchRequest(render, selector.operation_id,
            contract.get('initial_stopped')),
            setup.binding, setup.shared_namespace_uid, contract['manager_revision'], contract['target_manager_revision'])
        request = ManagementRefreshInstallRequest(resources, {}, original.upgrade.original_anchor,
            contract.get('pool_baseline'))
        if (refresh_contract(request) != contract or _uid(render.active) != _uid(original.active)
                or receipt['active_uid'] != _uid(original.active)):
            raise ValueError
        pool_baseline = None
        baseline: CompletedUpgrade | CompletedPoolCutover = original
        if request.pool_baseline is not None:
            from scripts.ops.nebius_pool_predecessor import PoolPredecessorV1, load_completed_pool

            pool_baseline = load_completed_pool(PoolPredecessorV1.model_validate(request.pool_baseline), original=original)
            baseline = pool_baseline
            for path, checksum in baseline.history.items():
                if ancestry.get(str(path)) != checksum:
                    raise ValueError
                read(path, checksum)
        _configuration(baseline.deployment, render.before)
        rendered = render_refresh(render)
        identity = {'schema': 'loom.nebius-management-refresh-install.v1', 'state_dir': str(state),
            'operation_id': operation, 'binding': asdict(setup.binding),
            'input_digest': digest({'history': ancestry, **contract})}
        applications = render.after.installation.applications
        assert applications is not None
        if (set(receipt) != {*identity, 'status', 'revision', 'phases', 'active_uid', 'active', 'switch_sha256',
                'history', 'contract', 'manager_revision', 'shared_revision'}
                or any(receipt[key] != value for key, value in identity.items())
                or receipt['status'] != 'management_refreshed' or receipt['revision'] != rendered.revision
                or receipt['manager_revision'] != resources.target_manager_revision
                or receipt['shared_revision'] != applications.shared.schema_revision
                or set(receipt['phases']) != set(_PHASES)):
            raise ValueError
        anchor = json.loads(read(selector.anchor_dir / (operation + '.json')))
        parent = json.loads(read(state / 'refresh.json'))
        if (anchor != identity or parent != {**identity, 'phases': receipt['phases'],
                'switch_started': True, 'activation_started': True, 'completion_sha256': selector.completion_sha256}
                or parent['switch_started'] is not True or parent['activation_started'] is not True):
            raise ValueError
        retained = {}
        for phase, item in receipt['phases'].items():
            if set(item) != {'sha256', 'proof'} or not isinstance(item['sha256'], str):
                raise ValueError
            journal = json.loads(read(state / phase / 'stage.json', item['sha256']))
            documents = refresh_documents(resources, phase)
            _validate_record(journal, {'schema': 'loom.nebius-management-stage.v1', 'binding': asdict(setup.binding),
                'revision': refresh_revision(resources, documents), 'phase': 'refresh-' + phase}, documents)
            for key, entry in journal['resources'].items():
                if entry['status'] != 'created':
                    raise ValueError
                observed = copy.deepcopy(entry['observed'])
                observed['metadata']['uid'] = entry['uid']
                _uid(observed)
                if _comparison_snapshot(observed) != entry['expected'] or key in retained:
                    raise ValueError
                retained[key] = observed
            if phase.endswith('probe') or phase == 'backup':
                _proof(request, phase, state, item['proof'])
            elif item['proof'] is not None:
                raise ValueError
        switch = json.loads(read(state / 'switch/cutover.json', receipt['switch_sha256']))
        desired = refresh_target(resources.switch, 'activate')
        switch_identity = refresh_switch_identity(resources.switch, state / 'switch')
        # Completion freezes API spelling; cutover freezes canonical quantities.
        # Normalize only quantities for comparison, never historical bytes or
        # other fields: resource amounts and runtime authority must still match.
        canonical_active = _canonical_quantities(receipt['active'])
        if (switch != {**switch_identity, 'original': refresh_initial(resources.switch), 'phase': 'active', 'active': canonical_active}
                or _qualified_defaulted(desired, receipt['active']) != canonical_active):
            raise ValueError
        # Independently qualify cumulative runtime/credential preservation against
        # the original upgrade or its qualified terminal pool baseline, never
        # just a caller-rewritten before snapshot or unqualified catalog UUID.
        rooted = replace(resources.switch, render=replace(render, before=baseline.deployment, active=baseline.active))
        if _qualified_defaulted(refresh_target(rooted, 'activate'), receipt['active']) != canonical_active:
            raise ValueError
        active = copy.deepcopy(receipt['active'])
        active['metadata']['uid'] = receipt['active_uid']
        return CompletedRefresh(selector, render.after, active, resources, history, retained, pool_baseline)
    except Exception:
        raise ValueError('refresh_predecessor_unqualified') from None

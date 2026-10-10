"""Fixed staging-only machine material and namespace-reader delivery.

The ordinary create-only engine retains its real API default previews and exact
uncertain-create readback. A local preparation receipt exists before the parent
starts; a separate dispatch marker pins that engine's defaults before any create.
Nothing here regenerates tokens, replays dev setup or grants gateway write access.
"""
from __future__ import annotations

import copy
import ssl
from collections.abc import Callable
from dataclasses import asdict
from pathlib import Path
from typing import Any

from scripts.ops import nebius_certificates as private_state
from scripts.ops.nebius_development_runtime_install import DevelopmentRuntimeInstallRequest, _read
from scripts.ops.nebius_development_runtime_live import HTTPSDevelopmentRuntimeResources
from scripts.ops.nebius_ingress_stage import _key, _uid
from scripts.ops.nebius_management_stage import _stage_fixed_documents, _validate_record
from scripts.ops.nebius_management_supplied import _defaulted
from scripts.ops.nebius_management_transport import ManagementKubernetesTransport
from scripts.ops.nebius_shared_pool_handoff_live import SharedPoolGuardBindings
from scripts.ops.nebius_shared_pool_operation import FORWARD_PHASES
from scripts.ops.nebius_shared_pool_workloads import _frame

from loom.nebius_platform_render import digest

_PHASE = 'forward-material'


def _documents(bindings: SharedPoolGuardBindings) -> dict[str, dict[str, Any]]:
    return copy.deepcopy({**bindings.plan.staging_material, **bindings.plan.staging_reader_authority})


def _identity(bindings: SharedPoolGuardBindings, state: Path) -> dict[str, str]:
    return {'schema': 'loom.nebius-shared-pool-material.v1', 'input_digest': bindings.input_digest,
        'documents_sha256': digest(_documents(bindings)), 'state_dir': str(state)}


def _stage_identity(bindings: SharedPoolGuardBindings) -> dict[str, Any]:
    manager = bindings.plan.development.runtime.request.database.manager.retained.request.retained
    return {'schema': 'loom.nebius-management-stage.v1', 'binding': asdict(manager.binding),
        'revision': digest(_documents(bindings)), 'phase': _PHASE}


def _dispatch(bindings: SharedPoolGuardBindings, state: Path, stage: dict[str, Any]) -> dict[str, str]:
    return {**_identity(bindings, state), 'stage_operation': stage['operation_id'],
        'defaults_sha256': digest({key: row['expected'] for key, row in stage['resources'].items()})}


def _active(bindings: SharedPoolGuardBindings, state: Path, status: str) -> dict[str, Any]:
    frame = _frame(bindings, state.parent)
    manager = bindings.plan.development.runtime.request.database.manager.retained.request.retained
    recovery = Path(manager.operation['anchor_dir']) / 'shared-pool-recovery.json'
    index = FORWARD_PHASES.index(_PHASE)
    if (state.name != _PHASE or status not in {'prepared', 'started'} or frame['outcome'] != 'pending'
            or recovery.exists() or recovery.is_symlink()
            or frame['forward'][_PHASE]['status'] != status
            or any(frame['forward'][prior]['status'] != 'complete' for prior in FORWARD_PHASES[:index])
            or any(frame['forward'][later]['status'] != 'prepared' for later in FORWARD_PHASES[index + 1:])):
        raise ValueError
    return frame


def validate_shared_staging_material(*, bindings: SharedPoolGuardBindings, state_dir: Path,
        complete: bool, prepared: bool = False) -> dict[str, Any]:
    """Validate initialization, dispatch authority and the real stage together."""
    frame = _frame(bindings, state_dir.parent)
    identity, documents = _identity(bindings, state_dir), _documents(bindings)
    local = _read(state_dir / 'material.json')
    if (state_dir.name != _PHASE or type(complete) is not bool or type(prepared) is not bool
            or (complete and prepared) or set(local) != {*identity, 'dispatch'}
            or any(local[key] != value for key, value in identity.items()) or type(local['dispatch']) is not bool
            or (prepared and frame['forward'][_PHASE]['status'] != 'prepared')):
        raise ValueError
    stage_path, dispatch_path = state_dir / 'stage.json', state_dir / 'dispatch.json'
    stage = _read(stage_path) if stage_path.exists() or stage_path.is_symlink() else None
    dispatched = _read(dispatch_path) if dispatch_path.exists() or dispatch_path.is_symlink() else None
    if prepared and (local['dispatch'] or stage is not None or dispatched is not None):
        raise ValueError
    if stage is None:
        if local['dispatch'] or dispatched is not None or complete:
            raise ValueError
    else:
        _validate_record(stage, _stage_identity(bindings), documents)
        statuses = []
        for key in documents:
            row = stage['resources'][key]
            # These fixed Secrets and RBAC objects admit no security-changing
            # defaults. The real dry-run uses the same exact default validator.
            if row['expected'] != row['desired']:
                raise ValueError
            if row['status'] == 'created':
                _uid({'metadata': {'uid': row['uid']}})
                if row['observed'] != row['expected']:
                    raise ValueError
            statuses.append(row['status'])
        if not local['dispatch'] and statuses not in (['prepared'] * len(documents), ['create_intent', *(['prepared'] * (len(documents) - 1))]):
            raise ValueError
        if local['dispatch'] and (dispatched is None or all(status == 'prepared' for status in statuses)):
            raise ValueError
        if dispatched is not None and dispatched != _dispatch(bindings, state_dir, stage):
            raise ValueError
        if complete and (not local['dispatch'] or any(status != 'created' for status in statuses)):
            raise ValueError
    return {'local': local, 'stage': stage, 'dispatched': dispatched}


def prepare_shared_staging_material(*, bindings: SharedPoolGuardBindings, state_dir: Path) -> None:
    frame = _active(bindings, state_dir, 'prepared')
    with private_state._locked_state(state_dir):
        path = state_dir / 'material.json'
        if not path.exists() and not path.is_symlink():
            if any((state_dir / name).exists() or (state_dir / name).is_symlink() for name in ('stage.json', 'dispatch.json')):
                raise ValueError
            private_state._atomic_json(path, {**_identity(bindings, state_dir), 'dispatch': False})
        validate_shared_staging_material(bindings=bindings, state_dir=state_dir, complete=False, prepared=True)
        if _active(bindings, state_dir, 'prepared') != frame:
            raise ValueError


class HTTPSSharedStagingMaterial(HTTPSDevelopmentRuntimeResources):
    """Reuse fixed resource paths, private-input checks and no-retry transport."""

    def __init__(self, *, bindings: SharedPoolGuardBindings, api_server: str, ssl_context: ssl.SSLContext,
            qualify_runtime: Callable[[DevelopmentRuntimeInstallRequest, str, bool], None],
            token: str | None = None, private_files: dict[Path, bytes] | None = None):
        self.bindings = copy.deepcopy(bindings)
        self.request = self.bindings.plan.development.runtime.request
        manager = self.request.database.manager.retained.request.retained
        self.state = Path(manager.operation['state_dir']).parent / 'shared-pool-handoff' / _PHASE
        if (not callable(qualify_runtime)
                or api_server.rstrip('/') != self.request.database.foundation.inputs.config['kubernetes_api_server'].rstrip('/')):
            raise ValueError('shared material connection differs')
        _active(self.bindings, self.state, 'started')
        self.binding, self.phase = manager.binding, _PHASE
        self.documents = _documents(self.bindings)
        self.private_files, self.qualify_runtime = dict(private_files or {}), qualify_runtime
        ManagementKubernetesTransport.__init__(self, api_server=api_server, ssl_context=ssl_context, token=token)

    def _private_inputs(self) -> None:
        _active(self.bindings, self.state, 'started')
        validate_shared_staging_material(bindings=self.bindings, state_dir=self.state, complete=False)
        if self.documents != _documents(self.bindings):
            raise ValueError('shared material projection changed')
        super()._private_inputs()

    def _qualify(self, *, writing: bool) -> None:
        frame = _active(self.bindings, self.state, 'started')
        evidence = validate_shared_staging_material(bindings=self.bindings, state_dir=self.state, complete=False)
        super()._qualify(writing=writing)
        if (_active(self.bindings, self.state, 'started') != frame
                or validate_shared_staging_material(bindings=self.bindings, state_dir=self.state, complete=False) != evidence):
            raise ValueError('shared material authority changed')

    def _request(self, method: str, path: str, *, document: dict[str, Any] | None = None) -> dict[str, Any] | None:
        evidence = validate_shared_staging_material(bindings=self.bindings, state_dir=self.state, complete=False)
        result = super()._request(method, path, document=document)
        self._private_inputs()
        if validate_shared_staging_material(bindings=self.bindings, state_dir=self.state, complete=False) != evidence:
            raise ValueError('shared material evidence changed during request')
        return result

    def create_resource(self, document: dict[str, Any]) -> None:
        self._approved(document, writing=True)
        self._qualify(writing=True)
        evidence = validate_shared_staging_material(bindings=self.bindings, state_dir=self.state, complete=False)
        stage, local = evidence['stage'], evidence['local']
        if stage is None or stage['resources'][_key(document)]['status'] != 'create_intent' or stage['resources'][_key(document)]['desired'] != document:
            raise ValueError('shared material create has no exact intent')
        if not local['dispatch']:
            marker = _dispatch(self.bindings, self.state, stage)
            if evidence['dispatched'] is None:
                private_state._atomic_json(self.state / 'dispatch.json', marker)
            if _read(self.state / 'dispatch.json') != marker or _read(self.state / 'material.json') != local:
                raise ValueError('shared material dispatch evidence changed')
            private_state._atomic_json(self.state / 'material.json', {**local, 'dispatch': True})
        super().create_resource(document)


def advance_shared_staging_material(*, bindings: SharedPoolGuardBindings, api: HTTPSSharedStagingMaterial,
        state_dir: Path) -> bool:
    try:
        if api.bindings != bindings or api.state != state_dir:
            raise ValueError
        _active(bindings, state_dir, 'started')
        validate_shared_staging_material(bindings=bindings, state_dir=state_dir, complete=False)
        documents = _documents(bindings)
        _stage_fixed_documents(documents=documents, revision=digest(documents), phase=_PHASE,
            binding=api.binding, api=api, state_dir=state_dir, default_document=_defaulted)
        validate_shared_staging_material(bindings=bindings, state_dir=state_dir, complete=True)
        return True
    except Exception:
        raise ValueError('shared staging material unconfirmed; preserve retained evidence') from None

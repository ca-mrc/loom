"""Fixed shared-gateway grants and startup under closed admission.

Reuses the create-only resource stage and UID/version-tested workload engine.
The parent must supply fresh whole-pool qualification and actual gateway process
challenges; neither a phase marker nor Pod readiness opens the capacity ledger.
"""
from __future__ import annotations

import copy
import ssl
from collections.abc import Callable
from dataclasses import asdict
from pathlib import Path
from typing import Any

from scripts.ops import nebius_certificates as private_state
from scripts.ops.nebius_development_activation import review_development_gateway_permissions
from scripts.ops.nebius_development_runtime_install import DevelopmentRuntimeInstallRequest, _read
from scripts.ops.nebius_development_runtime_live import (
    HTTPSDevelopmentRuntimeResources,
    HTTPSDevelopmentRuntimeWorkloads,
)
from scripts.ops.nebius_development_runtime_transition import (
    advance_runtime_workloads,
    validate_runtime_transition,
)
from scripts.ops.nebius_ingress_stage import _key, _uid
from scripts.ops.nebius_management_stage import _stage_fixed_documents, _validate_record
from scripts.ops.nebius_management_supplied import _defaulted
from scripts.ops.nebius_management_transport import ManagementKubernetesTransport
from scripts.ops.nebius_shared_pool_handoff_live import SharedPoolGuardBindings
from scripts.ops.nebius_shared_pool_operation import FORWARD_PHASES
from scripts.ops.nebius_shared_pool_workloads import _frame

from loom.nebius_platform_render import digest

_PHASE = 'forward-gateway'


def _workloads(bindings: SharedPoolGuardBindings) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]]]:
    plan = bindings.plan.development
    return ({_key(plan.gateway_original): copy.deepcopy(plan.gateway_original)},
        {_key(plan.gateway_target): copy.deepcopy(plan.gateway_target)})


def _identity(bindings: SharedPoolGuardBindings, state: Path) -> dict[str, str]:
    originals, targets = _workloads(bindings)
    return {'schema': 'loom.nebius-shared-pool-gateway.v1', 'input_digest': bindings.input_digest,
        'state_dir': str(state), 'authority_sha256': digest(bindings.plan.development.authority),
        'originals_sha256': digest(originals), 'targets_sha256': digest(targets)}


def _stage_identity(bindings: SharedPoolGuardBindings) -> dict[str, Any]:
    manager = bindings.plan.development.runtime.request.database.manager.retained.request.retained
    return {'schema': 'loom.nebius-management-stage.v1', 'binding': asdict(manager.binding),
        'revision': digest(bindings.plan.development.authority), 'phase': _PHASE + '-authority'}


def _dispatch(bindings: SharedPoolGuardBindings, state: Path, stage: dict[str, Any]) -> dict[str, str]:
    return {**_identity(bindings, state), 'stage_operation': stage['operation_id'],
        'defaults_sha256': digest({key: row['expected'] for key, row in stage['resources'].items()})}


def _active(bindings: SharedPoolGuardBindings, state: Path, status: str) -> dict[str, Any]:
    frame = _frame(bindings, state.parent)
    manager = bindings.plan.development.runtime.request.database.manager.retained.request.retained
    recovery = Path(manager.operation['anchor_dir']) / 'shared-pool-recovery.json'
    index = FORWARD_PHASES.index(_PHASE)
    if (state.name != _PHASE or status not in {'prepared', 'started'} or frame['outcome'] != 'pending'
            or recovery.exists() or recovery.is_symlink() or frame['forward'][_PHASE]['status'] != status
            or any(frame['forward'][prior]['status'] != 'complete' for prior in FORWARD_PHASES[:index])
            or any(frame['forward'][later]['status'] != 'prepared' for later in FORWARD_PHASES[index + 1:])):
        raise ValueError
    return frame


def validate_shared_gateway(*, bindings: SharedPoolGuardBindings, state_dir: Path,
        complete: bool, prepared: bool = False) -> dict[str, Any]:
    frame = _frame(bindings, state_dir.parent)
    identity = _identity(bindings, state_dir)
    local = _read(state_dir / 'gateway.json')
    if (state_dir.name != _PHASE or type(complete) is not bool or type(prepared) is not bool or (complete and prepared)
            or set(local) != {*identity, 'dispatch'} or type(local['dispatch']) is not bool
            or any(local[key] != value for key, value in identity.items())
            or (prepared and frame['forward'][_PHASE]['status'] != 'prepared')):
        raise ValueError

    def read_optional(name: str) -> dict[str, Any] | None:
        path = state_dir / name
        return _read(path) if path.exists() or path.is_symlink() else None

    stage, dispatched = read_optional('authority/stage.json'), read_optional('authority/dispatch.json')
    if prepared and (local['dispatch'] or stage is not None or dispatched is not None):
        raise ValueError
    authority_ready = False
    if stage is None:
        if local['dispatch'] or dispatched is not None or complete:
            raise ValueError
    else:
        documents = bindings.plan.development.authority
        _validate_record(stage, _stage_identity(bindings), documents)
        statuses = []
        for key in documents:
            row = stage['resources'][key]
            if row['expected'] != row['desired']:
                raise ValueError
            if row['status'] == 'created':
                _uid({'metadata': {'uid': row['uid']}})
                if row['observed'] != row['expected']:
                    raise ValueError
            statuses.append(row['status'])
        if ((not local['dispatch'] and statuses not in (['prepared'] * len(documents), ['create_intent', *(['prepared'] * (len(documents) - 1))]))
                or (local['dispatch'] and (dispatched is None or all(status == 'prepared' for status in statuses)))
                or (dispatched is not None and dispatched != _dispatch(bindings, state_dir, stage))):
            raise ValueError
        authority_ready = local['dispatch'] and all(status == 'created' for status in statuses)
    start = read_optional('start/transition.json')
    if start is None:
        if not prepared:
            raise ValueError
    else:
        originals, targets = _workloads(bindings)
        validate_runtime_transition(start, originals=originals, targets=targets,
            input_digest=bindings.input_digest, phase='start')
        if any(((prepared or not authority_ready) and (item['status'] != 'prepared' or item['expected'] is not None))
                or (complete and item['status'] != 'applied') for item in start['resources'].values()):
            raise ValueError
    if complete and not authority_ready:
        raise ValueError
    return {'local': local, 'stage': stage, 'dispatched': dispatched, 'start': start, 'authority_ready': authority_ready}


def prepare_shared_gateway(*, bindings: SharedPoolGuardBindings, state_dir: Path) -> None:
    frame = _active(bindings, state_dir, 'prepared')
    with private_state._locked_state(state_dir):
        path = state_dir / 'gateway.json'
        if not path.exists() and not path.is_symlink():
            if any((state_dir / name).exists() or (state_dir / name).is_symlink() for name in ('authority', 'start')):
                raise ValueError
            private_state._atomic_json(path, {**_identity(bindings, state_dir), 'dispatch': False})
        validate_shared_gateway(bindings=bindings, state_dir=state_dir, complete=False, prepared=True)
        start = state_dir / 'start/transition.json'
        if not start.exists() and not start.is_symlink():
            originals, targets = _workloads(bindings)
            private_state._private_directory(start.parent)
            private_state._atomic_json(start, {'schema': 'loom.nebius-development-runtime-transition.v1',
                'input_digest': bindings.input_digest, 'phase': 'start',
                'originals_digest': digest(originals), 'targets_digest': digest(targets),
                'resources': {key: {'status': 'prepared', 'expected': None} for key in originals}})
        validate_shared_gateway(bindings=bindings, state_dir=state_dir, complete=False, prepared=True)
        if _active(bindings, state_dir, 'prepared') != frame:
            raise ValueError


class HTTPSSharedGateway(HTTPSDevelopmentRuntimeResources, HTTPSDevelopmentRuntimeWorkloads):
    """Reuse the two existing sealed transports with one source-qualified scope.

    Creates resolve only to catalog-derived RBAC; workload PATCHes resolve only
    to the installed stopped gateway. No other caller-supplied manifest exists.
    """

    def __init__(self, *, bindings: SharedPoolGuardBindings, api_server: str, ssl_context: ssl.SSLContext,
            qualify_runtime: Callable[[DevelopmentRuntimeInstallRequest, str, bool], None],
            observe_ready: Callable[[DevelopmentRuntimeInstallRequest, str, dict[str, Any], dict[str, Any], dict[str, Any]], bool],
            token: str | None = None, private_files: dict[Path, bytes] | None = None):
        self.bindings = copy.deepcopy(bindings)
        self.request = self.bindings.plan.development.runtime.request
        manager = self.request.database.manager.retained.request.retained
        self.state = Path(manager.operation['state_dir']).parent / 'shared-pool-handoff' / _PHASE
        if (not callable(qualify_runtime) or not callable(observe_ready)
                or api_server.rstrip('/') != self.request.database.foundation.inputs.config['kubernetes_api_server'].rstrip('/')):
            raise ValueError('shared gateway connection differs')
        _active(self.bindings, self.state, 'started')
        validate_shared_gateway(bindings=self.bindings, state_dir=self.state, complete=False)
        self.identity = _identity(self.bindings, self.state)
        self.binding, self.phase = manager.binding, _PHASE
        self.documents = copy.deepcopy(self.bindings.plan.development.authority)
        self.originals, self.targets = _workloads(self.bindings)
        self.private_files, self.qualify_runtime = dict(private_files or {}), qualify_runtime
        self.observe_ready = observe_ready
        ManagementKubernetesTransport.__init__(self, api_server=api_server, ssl_context=ssl_context, token=token)

    def _private_inputs(self) -> None:
        _active(self.bindings, self.state, 'started')
        validate_shared_gateway(bindings=self.bindings, state_dir=self.state, complete=False)
        if (_identity(self.bindings, self.state) != self.identity or self.documents != self.bindings.plan.development.authority
                or (self.originals, self.targets) != _workloads(self.bindings)):
            raise ValueError('shared gateway projection changed')
        super()._private_inputs()

    def _qualify(self, *, writing: bool) -> None:
        frame = _active(self.bindings, self.state, 'started')
        evidence = validate_shared_gateway(bindings=self.bindings, state_dir=self.state, complete=False)
        super()._qualify(writing=writing)
        if (_active(self.bindings, self.state, 'started') != frame
                or validate_shared_gateway(bindings=self.bindings, state_dir=self.state, complete=False) != evidence):
            raise ValueError('shared gateway phase authority changed')

    def _request(self, method: str, path: str, *, document: dict[str, Any] | None = None) -> dict[str, Any] | None:
        evidence = validate_shared_gateway(bindings=self.bindings, state_dir=self.state, complete=False)
        result = super()._request(method, path, document=document)
        self._private_inputs()
        if validate_shared_gateway(bindings=self.bindings, state_dir=self.state, complete=False) != evidence:
            raise ValueError('shared gateway evidence changed during request')
        return result

    def create_resource(self, document: dict[str, Any]) -> None:
        self._approved(document, writing=True)
        self._qualify(writing=True)
        evidence = validate_shared_gateway(bindings=self.bindings, state_dir=self.state, complete=False)
        stage, local = evidence['stage'], evidence['local']
        if stage is None or stage['resources'][_key(document)]['status'] != 'create_intent' or stage['resources'][_key(document)]['desired'] != document:
            raise ValueError('shared gateway grant has no exact create intent')
        if not local['dispatch']:
            marker = _dispatch(self.bindings, self.state, stage)
            path = self.state / 'authority/dispatch.json'
            if evidence['dispatched'] is None:
                private_state._atomic_json(path, marker)
            if _read(path) != marker or _read(self.state / 'gateway.json') != local:
                raise ValueError('shared gateway grant authority changed')
            private_state._atomic_json(self.state / 'gateway.json', {**local, 'dispatch': True})
        super().create_resource(document)

    def verify_gateway_authority(self) -> None:
        self._qualify(writing=False)
        evidence = validate_shared_gateway(bindings=self.bindings, state_dir=self.state, complete=False)
        if not evidence['authority_ready']:
            raise ValueError('shared gateway grants are incomplete')
        review_development_gateway_permissions(self.request, transport=self, granted=True)
        if validate_shared_gateway(bindings=self.bindings, state_dir=self.state, complete=False) != evidence:
            raise ValueError('shared gateway grants changed during review')
        self._qualify(writing=False)

    def _patch(self, key: str, before: dict[str, Any], desired: dict[str, Any], *, preview: bool) -> dict[str, Any] | None:
        self.verify_gateway_authority()
        record = validate_shared_gateway(bindings=self.bindings, state_dir=self.state, complete=False)
        if not preview and record['start']['resources'][key]['status'] != 'intent':
            raise ValueError('shared gateway startup has no exact intent')
        return HTTPSDevelopmentRuntimeWorkloads._patch(self, key, before, desired, preview=preview)


def advance_shared_gateway(*, bindings: SharedPoolGuardBindings, api: HTTPSSharedGateway, state_dir: Path) -> bool:
    try:
        if api.bindings != bindings or api.state != state_dir:
            raise ValueError
        _active(bindings, state_dir, 'started')
        validate_shared_gateway(bindings=bindings, state_dir=state_dir, complete=False)
        documents = bindings.plan.development.authority
        _stage_fixed_documents(documents=documents, revision=digest(documents), phase=_PHASE + '-authority',
            binding=api.binding, api=api, state_dir=state_dir / 'authority', default_document=_defaulted)
        api.verify_gateway_authority()
        originals, targets = _workloads(bindings)
        if not advance_runtime_workloads(originals=originals, targets=targets, input_digest=bindings.input_digest,
                phase='start', api=api, state_dir=state_dir / 'start'):
            return False
        api.verify_gateway_authority()
        validate_shared_gateway(bindings=bindings, state_dir=state_dir, complete=True)
        return True
    except Exception:
        raise ValueError('shared gateway startup unconfirmed; preserve retained evidence') from None

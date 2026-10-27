"""Read completed fresh-runtime evidence without replaying installation.

This historical projection is not live readiness or permission to open a pool.
The protected activation parent must independently qualify current identities,
sole-writer authority, installed settings and fresh capacity before any write.
"""
from __future__ import annotations

import copy
import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator
from scripts.ops import nebius_certificates as private_state
from scripts.ops.nebius_development_runtime_install import (
    _PHASES,
    DevelopmentRuntimeInstallRequest,
    DevelopmentRuntimePlan,
    _child_path,
    _read,
    _runtime_record,
    prepare_runtime_install,
    runtime_workload_options,
)
from scripts.ops.nebius_ingress_stage import _key, _snapshot, _uid
from scripts.ops.nebius_management_stage import _comparison_snapshot

from loom.nebius_platform_render import digest


class RetainedRuntimeReference(BaseModel):
    model_config = ConfigDict(extra='forbid', frozen=True, hide_input_in_errors=True)
    operation_path: Path
    operation_sha256: str = Field(pattern=r'^[0-9a-f]{64}$')

    @field_validator('operation_path')
    @classmethod
    def canonical_operation(cls, value: Path) -> Path:
        if not value.is_absolute() or value != value.resolve() or value.name != 'operation.json':
            raise ValueError('retained runtime operation path differs')
        return value


@dataclass(frozen=True, repr=False)
class CompletedDevelopmentRuntime:
    request: DevelopmentRuntimeInstallRequest
    plan: DevelopmentRuntimePlan
    state_dir: Path
    anchor_dir: Path
    resources: dict[str, dict[str, Any]]
    files: dict[Path, bytes]
    history_sha256: str


def _canonical_node_selectors(expression: str) -> str:
    """Normalize only JSON map-member order in the rendered selector equality.

    Historical policies predate deterministic JSON rendering. JSON persistence
    sorts profile mappings, but CEL map equality is independent of that order.
    Preserve every other byte, including surrounding operators and operands.
    """
    def mapping(pairs: list[tuple[str, Any]]) -> dict[str, str]:
        if len(dict(pairs)) != len(pairs) or any(not isinstance(value, str) for _, value in pairs):
            raise ValueError('native build selector literal differs')
        return dict(pairs)
    decoder = json.JSONDecoder(object_pairs_hook=mapping)
    prefix = '(variables.p.nodeSelector == '
    pieces = []
    while (position := expression.find(prefix)) >= 0:
        end = position + len(prefix)
        value, length = decoder.raw_decode(expression[end:])
        if not isinstance(value, dict) or expression[end + length:end + length + 1] != ')':
            raise ValueError('native build selector literal differs')
        pieces.append(expression[:end] + json.dumps(value, separators=(',', ':'), sort_keys=True))
        expression = expression[end + length:]
    return ''.join(pieces) + expression


def qualify_build_policy_spec(desired: dict[str, Any], observed: dict[str, Any]) -> None:
    """Only exact rendered admission semantics plus inert API defaults.

    Subset matching is unsafe here: an added match condition, parameter kind or
    selector can disable an otherwise unchanged policy. Kubernetes v1's
    SetDefaults_MatchResources adds only these three defaults.
    """
    try:
        kind = desired['kind']
        field = {'ValidatingAdmissionPolicy': 'matchConstraints',
            'ValidatingAdmissionPolicyBinding': 'matchResources'}[kind]
        if observed['kind'] != kind or observed['apiVersion'] != desired['apiVersion']:
            raise ValueError
        specs = [copy.deepcopy(row['spec']) for row in (desired, observed)]
        for spec in specs:
            match = spec[field]
            match.setdefault('namespaceSelector', {})
            match.setdefault('objectSelector', {})
            match.setdefault('matchPolicy', 'Equivalent')
            if kind == 'ValidatingAdmissionPolicy':
                final = spec['validations'][-1]
                final['expression'] = _canonical_node_selectors(final['expression'])
        if specs[0] != specs[1]:
            raise ValueError
    except Exception:
        raise ValueError('development build policy semantics differ') from None


def load_completed_runtime(request: DevelopmentRuntimeInstallRequest) -> CompletedDevelopmentRuntime:
    """Require every anchored child and return pool/runtime successor identities.

    The request comes from qualified source/material intake. Retained foundation
    and manager evidence are included in the byte pins, but neither an installer
    nor its retired operator connection is used to consume their history.
    """
    try:
        request = copy.deepcopy(request)
        database = request.database
        pool, foundation = database.manager.retained, database.foundation
        manager = pool.request.retained
        state = Path(manager.operation['state_dir']).parent / 'runtime-installation'
        anchor = Path(manager.operation['anchor_dir'])
        files = dict(pool.files)
        for path, raw in foundation.files.items():
            if path in files and files[path] != raw:
                raise ValueError
            files[path] = raw
        for path in (anchor / 'runtime-installation.json', state / 'installation.json',
                *(_child_path(state, phase) for phase in _PHASES)):
            if not path.is_absolute() or path != path.resolve() or path in files:
                raise ValueError
            files[path] = private_state._private_read(path, limit=4 * 1024**2)

        def unchanged() -> None:
            if any(private_state._private_read(path, limit=4 * 1024**2) != raw for path, raw in files.items()):
                raise ValueError

        unchanged()
        plan = prepare_runtime_install(request)
        record = _runtime_record(request, plan, state, anchor)
        if any(item['status'] != 'complete' for item in record['phases'].values()):
            raise ValueError
        resources = copy.deepcopy(pool.resources)
        for phase in plan.fixed:
            child = _read(_child_path(state, phase))
            for key, item in child['resources'].items():
                actual = copy.deepcopy(item['observed'])
                actual['metadata']['uid'] = item['uid']
                _uid(actual)
                if (key in resources or _key(actual) != key or _snapshot(actual) != item['observed']
                        or _comparison_snapshot(actual) != item['expected']):
                    raise ValueError
                if actual['kind'] in {'ValidatingAdmissionPolicy', 'ValidatingAdmissionPolicyBinding'}:
                    qualify_build_policy_spec(plan.fixed[phase][key], actual)
                resources[key] = actual
        # Completed transitions have exactly one successor. Its UID is derived
        # from the original resource, not accepted from an arbitrary projection.
        for key, options in runtime_workload_options(request=request, state_dir=state, _plan=plan).items():
            actual, = options
            if _key(actual) != key:
                raise ValueError
            _uid(actual)
            resources[key] = copy.deepcopy(actual)
        if len({_uid(row) for row in resources.values()}) != len(resources):
            raise ValueError
        unchanged()
        checksum = digest({str(path): hashlib.sha256(raw).hexdigest()
            for path, raw in files.items()})
        return CompletedDevelopmentRuntime(request, plan, state, anchor, resources, files, checksum)
    except Exception:
        raise ValueError('completed development runtime unqualified') from None

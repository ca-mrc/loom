"""Stdlib-only fixed shared handoff selection, phases and diagnostic vocabulary."""
from __future__ import annotations

import re
from pathlib import Path
from typing import Any
from uuid import UUID

DIAGNOSTIC_STAGES = frozenset({'operation', 'inputs', 'connection', 'catalog', 'storage', 'handoff'})


FORWARD_PHASES = (
    'forward-guards', 'forward-drain', 'forward-stop', 'forward-roles',
    'forward-database', 'forward-material', 'forward-wire', 'forward-start',
    'forward-build-policy', 'forward-gateway', 'forward-activate', 'forward-verify',
)
RECOVERY_PHASES = (
    'recovery-fence', 'recovery-drain', 'recovery-stop', 'recovery-revoke',
    'recovery-restore-workloads', 'recovery-restore-roles', 'recovery-restart',
    'recovery-reopen', 'recovery-verify',
)

def validate_operation(value: dict[str, Any]) -> None:
    try:
        fields = {'schema', 'source_sha', 'installation_id', 'namespace',
            'operation_id', 'inputs_path', 'inputs_sha256'}
        prepared = value.get('schema') == 'loom.nebius-shared-pool-operation.v2'
        if prepared:
            fields.add('preparation_id')
        if (not isinstance(value, dict) or set(value) != fields
                or any(not isinstance(item, str) or not 0 < len(item) <= 1024 for item in value.values())
                or value['schema'] not in {'loom.nebius-shared-pool-operation.v1', 'loom.nebius-shared-pool-operation.v2'}
                or value['namespace'] != 'loom-nebius-management-dev'
                or not re.fullmatch(r'[0-9a-f]{40}', value['source_sha'])
                or not re.fullmatch(r'[0-9a-f]{64}', value['inputs_sha256'])):
            raise ValueError()
        for key in ('installation_id', 'operation_id', *(['preparation_id'] if prepared else [])):
            identity = UUID(value[key])
            if not identity.int or str(identity) != value[key]:
                raise ValueError()
        path = Path(value['inputs_path'])
        root = path.parent
        if prepared:
            if root.name != value['preparation_id'] or root.parent.name != 'preparations':
                raise ValueError()
            root = root.parent.parent
        if (not path.is_absolute() or str(path) != value['inputs_path'] or path != path.resolve()
                or not re.fullmatch(r'/[A-Za-z0-9_./-]+', str(path)) or path.name != 'inputs.json'
                or root.name != value['operation_id'] or root.parent.name != value['installation_id']
                or root.parent.parent.name != 'nebius-shared-pool'
                or root.parent.parent.parent.name != '.loom' or root.parent.parent.parent.parent == Path('/')):
            raise ValueError()
    except Exception:
        raise ValueError('shared pool operation unqualified') from None


def operation_root(value: dict[str, Any]) -> Path:
    validate_operation(value)
    return Path(value['inputs_path']).parent


def operator_home(value: dict[str, Any]) -> Path:
    return pool_root(value).parent.parent.parent.parent


def pool_root(value: dict[str, Any]) -> Path:
    """The registered operation's stable directory, not an input preparation."""
    root = operation_root(value)
    return root.parent.parent if value['schema'] == 'loom.nebius-shared-pool-operation.v2' else root

"""Fixed private renewal selection; not initial-install or staging authority."""
from __future__ import annotations

import re
from pathlib import Path
from typing import Any
from uuid import UUID

DIAGNOSTIC_STAGES = frozenset({'operation', 'inputs', 'connection', 'renewal'})


def validate_operation(value: dict[str, Any]) -> None:
    try:
        if (not isinstance(value, dict) or set(value) != {'schema', 'source_sha', 'installation_id', 'namespace',
                'operation_id', 'inputs_path', 'inputs_sha256'}
                or any(not isinstance(item, str) or not 0 < len(item) <= 1024 for item in value.values())
                or value['schema'] != 'loom.nebius-development-management-renewal-operation.v1'
                or value['namespace'] != 'loom-nebius-management-dev'
                or not re.fullmatch(r'[0-9a-f]{40}', value['source_sha'])
                or not re.fullmatch(r'[0-9a-f]{64}', value['inputs_sha256'])):
            raise ValueError()
        for key in ('installation_id', 'operation_id'):
            identity = UUID(value[key])
            if not identity.int or str(identity) != value[key]:
                raise ValueError()
        path = Path(value['inputs_path'])
        root = path.parent
        if (not path.is_absolute() or str(path) != value['inputs_path'] or path != path.resolve()
                or not re.fullmatch(r'/[A-Za-z0-9_./-]+', str(path)) or path.name != 'inputs.json'
                or root.name != value['operation_id'] or root.parent.name != value['installation_id']
                or root.parent.parent.name != 'nebius-development-management-renewal'
                or root.parent.parent.parent.name != '.loom' or root.parent.parent.parent.parent == Path('/')):
            raise ValueError()
    except Exception:
        raise ValueError('development management renewal operation unqualified') from None


def operation_root(value: dict[str, Any]) -> Path:
    validate_operation(value)
    return Path(value['inputs_path']).parent


def operator_home(value: dict[str, Any]) -> Path:
    return operation_root(value).parent.parent.parent.parent

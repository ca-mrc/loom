"""Fixed dev-manager identity for the protected publisher, gateway and entry.

This module grants no authority and performs no writes. Operator-reviewed bundle
and private-input identity checks remain required before using an operation.
"""
from __future__ import annotations

import re
from pathlib import Path
from typing import Any
from uuid import UUID

DIAGNOSTIC_STAGES = frozenset({'operation', 'inputs', 'connection', 'installation', 'render', 'cluster_identity',
    'prerequisites', 'foundation', 'platform_capacity', 'publication', 'cloud_identity', 'backup_quota',
    'backup_access', 'public_route', 'foundation_readback', 'database_storage', 'provider_disk'})


def validate_operation(value: dict[str, Any]) -> None:
    """Refuse legacy management state and noncanonical dev-manager selections."""
    try:
        if (not isinstance(value, dict)
                or set(value) != {"schema", "source_sha", "candidate", "installation_id", "namespace",
                    "inputs_path", "state_dir", "anchor_dir", "inputs_sha256"}
                or any(not isinstance(item, str) or not 0 < len(item) <= 1024 for item in value.values())
                or value["schema"] != "loom.nebius-development-management-operation.v1"
                or value["namespace"] != "loom-nebius-management-dev"
                or not re.fullmatch(r"[0-9a-f]{40}", value["source_sha"])
                or value["source_sha"] != value["candidate"]
                or not re.fullmatch(r"[0-9a-f]{64}", value["inputs_sha256"])):
            raise ValueError()
        identity = UUID(value["installation_id"])
        if not identity.int or str(identity) != value["installation_id"]:
            raise ValueError()
        for key in ("inputs_path", "state_dir", "anchor_dir"):
            path = Path(value[key])
            if (not path.is_absolute() or str(path) != value[key] or path != path.resolve()
                    or not re.fullmatch(r"/[A-Za-z0-9_./-]+", str(path))):
                raise ValueError()
        root = Path(value["inputs_path"]).parent
        if (root.name != str(identity) or root.parent.name != "nebius-development-management"
                or root.parent.parent.name != ".loom" or root.parent.parent.parent == Path("/")
                or Path(value["inputs_path"]) != root / "inputs.json"
                or Path(value["state_dir"]) != root / "state"
                or Path(value["anchor_dir"]) != root.parent.parent / "nebius-development-management-anchors" / str(identity)):
            raise ValueError()
    except Exception:
        raise ValueError("development management operation unqualified") from None


def operation_root(value: dict[str, Any]) -> Path:
    validate_operation(value)
    return Path(value["inputs_path"]).parent


def operator_home(value: dict[str, Any]) -> Path:
    return operation_root(value).parent.parent.parent

"""Stdlib-only dev operation shape shared by the protected gateway and entry."""
from __future__ import annotations

import re
from pathlib import Path
from typing import Any
from uuid import UUID

DIAGNOSTIC_STAGES = frozenset({"operation", "inputs", "connection", "installation", "configuration",
    "source_capacity", "cloud_identity", "object_access", "database_storage", "provider_disk", "private_service"})


def validate_operation(value: dict[str, Any]) -> None:
    """One canonical fresh-dev installation, never a staging or recovery command."""
    try:
        if (set(value) != {"schema", "source_sha", "candidate", "installation_id", "namespace",
                "state_dir", "anchor_dir", "inputs_path", "inputs_sha256"}
                or any(not isinstance(item, str) or not 0 < len(item) <= 1024 for item in value.values())
                or value["schema"] != "loom.nebius-development-operation.v1" or value["namespace"] != "loom-dev"
                or not re.fullmatch(r"[0-9a-f]{40}", value["source_sha"])
                or value["source_sha"] != value["candidate"]
                or not re.fullmatch(r"[0-9a-f]{64}", value["inputs_sha256"])):
            raise ValueError()
        identity = UUID(value["installation_id"])
        if not identity.int or str(identity) != value["installation_id"]:
            raise ValueError()
        for key in ("inputs_path", "state_dir", "anchor_dir"):
            path = Path(value[key])
            if not path.is_absolute() or path != path.resolve() or not re.fullmatch(r"/[A-Za-z0-9_./-]+", str(path)):
                raise ValueError()
        root = Path(value["inputs_path"]).parent
        if (root.name != str(identity) or root.parent.name != "nebius-development"
                or Path(value["inputs_path"]) != root / "inputs.json"
                or Path(value["state_dir"]) != root / "state"
                or Path(value["anchor_dir"]) != root.parent.parent / "nebius-development-anchors" / str(identity)):
            raise ValueError()
    except Exception:
        raise ValueError("development operation unqualified") from None

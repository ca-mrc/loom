"""Fixed read-only gateway probe; emit no private journal or target payloads.

Executed as protected source using only the gateway's standard library. It reads
the existing creation receipt, never takes a write lock or starts an installer.
"""
from __future__ import annotations

import hashlib
import json
import os
import re
import stat
import sys
from pathlib import Path
from typing import Any
from uuid import UUID


def observe(state: Path, expected: dict[str, Any]) -> None:
    if (not state.is_absolute() or state != state.resolve() or state.name != "state"
            or state.parent.name != "retirement" or state.parent.parent.name != "nebius-management"
            or set(expected) != {"binding", "resources"}):
        raise ValueError
    binding = expected["binding"]
    if (set(binding) != {"installation_id", "namespace", "namespace_uid", "kube_system_uid"}
            or re.fullmatch(r"loom-nebius-management(?:-[a-z0-9]+(?:-[a-z0-9]+)*)?", binding["namespace"]) is None):
        raise ValueError
    for key in ("installation_id", "namespace_uid", "kube_system_uid"):
        if str(UUID(binding[key])) != binding[key] or not UUID(binding[key]).int:
            raise ValueError
    path = state / "job" / "stage.json"
    if path != path.resolve():
        raise ValueError
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(descriptor, "rb") as stream:
        info = os.fstat(stream.fileno())
        if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid()
                or info.st_mode & 0o077 or info.st_nlink != 1):
            raise ValueError
        raw = stream.read(4 * 1024**2 + 1)
    if len(raw) > 4 * 1024**2:
        raise ValueError
    record = json.loads(raw)
    if (record["schema"] != "loom.nebius-management-stage.v1" or record["phase"] != "retirement-job"
            or record["binding"] != binding or set(record["resources"]) != set(expected["resources"])
            or len(expected["resources"]) != 2):
        raise ValueError
    kinds = set()
    names = set()
    for key, identity in expected["resources"].items():
        kind, namespace, name = key.split(":")
        kinds.add(kind)
        names.add(name)
        if (namespace != binding["namespace"] or re.fullmatch(r"loom-retirement-[0-9a-f]{12}", name) is None
                or set(identity) != {"uid", "snapshot"}):
            raise ValueError
        item = record["resources"][key]
        snapshot = item["observed"]
        encoded = (json.dumps(snapshot, sort_keys=True, separators=(",", ":")) + "\n").encode()
        if (item["status"] != "created" or item["uid"] != identity["uid"]
                or "sha256:" + hashlib.sha256(encoded).hexdigest() != identity["snapshot"]
                or snapshot["metadata"]["annotations"]["loom.nebius/management-stage-operation"] != record["operation_id"]):
            raise ValueError
    if kinds != {"ConfigMap", "Job"} or len(names) != 1:
        raise ValueError


def main() -> int:
    try:
        if len(sys.argv) != 3 or len(sys.argv[2]) > 8192:
            raise ValueError
        observe(Path(sys.argv[1]), json.loads(sys.argv[2]))
        result = {"status": "matched"}
    except Exception as error:
        kind = type(error).__name__
        result = {"status": "unavailable", "error_type": kind if kind in {
            "ValueError", "KeyError", "TypeError", "FileNotFoundError", "PermissionError", "JSONDecodeError",
        } else "OtherError"}
    print(json.dumps(result))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

"""Read the existing anchored terminal rollback, exposing only workload identities.

This stdlib-only gateway probe accepts one operation UUID, never a path or a
caller-supplied receipt. It neither replays rollback nor grants mutation rights.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import stat
import sys
from pathlib import Path
from typing import Any
from uuid import UUID


def canonical_uuid(value: str) -> str:
    parsed = UUID(value)
    if not parsed.int or str(parsed) != value:
        raise ValueError("invalid operation identity")
    return value


def validate_projection(value: Any, operation: str) -> dict[str, Any]:
    canonical_uuid(operation)
    if (not isinstance(value, dict) or set(value) != {"operation_id", "outcome", "workloads"}
            or value["operation_id"] != operation or value["outcome"] != "legacy"
            or not isinstance(value["workloads"], dict) or not 1 <= len(value["workloads"]) <= 1000):
        raise ValueError("legacy pool completion unavailable")
    for key, uid in value["workloads"].items():
        if not isinstance(key, str) or re.fullmatch(
                r"(?:Deployment|CronJob):[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?:"
                r"[a-z0-9](?:[a-z0-9.-]{0,251}[a-z0-9])?", key) is None:
            raise ValueError("invalid workload identity")
        canonical_uuid(uid)
    return value


def _private_read(path: Path, limit: int) -> bytes:
    if path != path.resolve():
        raise ValueError("noncanonical completion path")
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(descriptor, "rb") as stream:
        info = os.fstat(stream.fileno())
        if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid()
                or info.st_mode & 0o077 or info.st_nlink != 1):
            raise ValueError("private completion required")
        data = stream.read(limit + 1)
    if len(data) > limit:
        raise ValueError("completion exceeds bound")
    return data


def read_completion(operation: str) -> dict[str, Any]:
    """Validate the existing independent anchor once at this trust boundary."""
    canonical_uuid(operation)
    root = Path.home() / ".loom/nebius-management/pool-cutover" / operation
    state = root / "state"
    raw = _private_read(state / "completion.json", 8 * 1024**2)
    anchor = json.loads(_private_read(root / "anchor" / (operation + "-completion.json"), 16 * 1024))
    if anchor != {"schema": "loom.nebius-pool-completion-anchor.v1", "operation_id": operation,
            "state_dir": str(state), "completion_sha256": hashlib.sha256(raw).hexdigest()}:
        raise ValueError("completion anchor differs")
    receipt = json.loads(raw)
    if (not isinstance(receipt, dict) or set(receipt) != {"schema", "operation_id", "state_dir",
            "contract_sha256", "outcome", "phase_sha256", "workloads"}
            or receipt["schema"] != "loom.nebius-pool-completion.v1"
            or receipt["operation_id"] != operation or receipt["state_dir"] != str(state)
            or receipt["outcome"] != "legacy" or not isinstance(receipt["workloads"], dict)):
        raise ValueError("terminal legacy completion required")
    workloads = {}
    for key, workload in receipt["workloads"].items():
        metadata = workload["metadata"]
        if key != ":".join((workload["kind"], metadata["namespace"], metadata["name"])):
            raise ValueError("completion workload identity differs")
        workloads[key] = metadata["uid"]
    return validate_projection({"operation_id": operation, "outcome": "legacy", "workloads": workloads}, operation)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("operation")
    args = parser.parse_args()
    try:
        result = read_completion(args.operation)
    except Exception:
        print("legacy pool completion unavailable", file=sys.stderr)
        return 1
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

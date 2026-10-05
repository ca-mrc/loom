#!/usr/bin/env python3
"""Generate and check issue #1548 execution-contract artifacts."""

from __future__ import annotations

import argparse
import fnmatch
import hashlib
import json
import sys
import tomllib
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src"))

from loom.execution_contract import (  # noqa: E402
    ExecutionClassV1,
    ExecutionRouteCandidateV1,
    ExecutionRoutingDecisionV1,
    ExecutionTargetV1,
    ExecutionTopologyV1,
    PoolCapacityV1,
    WorkloadRequirementsV1,
)
from loom.execution_selection import ExecutionSelectionV1  # noqa: E402

POLICY_PATH = ROOT / "config" / "service-execution-compatibility.toml"
REPORT_PATH = ROOT / "docs" / "evidence" / "service-workload-compatibility-v2.json"
ENTRYPOINT_FILES = (
    ROOT / "packages" / "loom-benchmarks" / "pyproject.toml",
    ROOT / "packages" / "loom-benchmark-terminal-bench-2" / "pyproject.toml",
)
RESOURCE_PROFILES_PATH = ROOT / "config" / "resource-profiles.toml"
TOPOLOGY_PATH = ROOT / "config" / "service-execution-topology.json"
SCHEMA_OUTPUTS = {
    ROOT / "docs" / "evidence" / "loom.execution-class.v1.schema.json": ExecutionClassV1,
    ROOT / "docs" / "evidence" / "loom.execution-target.v1.schema.json": ExecutionTargetV1,
    ROOT / "docs" / "evidence" / "loom.execution-route-candidate.v1.schema.json": (
        ExecutionRouteCandidateV1
    ),
    ROOT / "docs" / "evidence" / "loom.execution-routing-decision.v1.schema.json": (
        ExecutionRoutingDecisionV1
    ),
    ROOT / "docs" / "evidence" / "loom.execution-topology.v1.schema.json": (ExecutionTopologyV1),
    ROOT / "docs" / "evidence" / "loom.execution-selection.v1.schema.json": ExecutionSelectionV1,
    ROOT / "docs" / "evidence" / "loom.pool-capacity.v1.schema.json": PoolCapacityV1,
    ROOT / "docs" / "evidence" / "loom.workload-requirements.v1.schema.json": (
        WorkloadRequirementsV1
    ),
}
_DISPOSITIONS = {
    "supported",
    "conversion_required",
    "runtime_admission_required",
    "unsupported",
}


def _json_bytes(value: object) -> bytes:
    return (json.dumps(value, indent=2, sort_keys=True) + "\n").encode("utf-8")


def _load_entrypoints() -> dict[str, dict[str, str]]:
    result: dict[str, dict[str, str]] = {}
    for path in ENTRYPOINT_FILES:
        raw = tomllib.loads(path.read_text(encoding="utf-8"))
        package = str(raw["project"]["name"])
        entries = raw["project"]["entry-points"]["loom.benchmarks"]
        for slug, entry_point in entries.items():
            if slug in result:
                raise ValueError(f"duplicate benchmark entry point {slug!r}")
            result[slug] = {
                "package": package,
                "entry_point": str(entry_point),
                "source": path.relative_to(ROOT).as_posix(),
            }
    return result


def _validate_policy(raw: dict[str, Any]) -> None:
    if raw.get("schema_version") != "loom.service-workload-compatibility-policy.v2":
        raise ValueError("unsupported compatibility policy schema")
    if raw.get("pool_policies") != [
        {"logical_pool_id": "nebius-cpu", "mode": "workload_specific"}
    ] or raw.get("workload_policy_pool_id") != "nebius-cpu":
        raise ValueError("compatibility policy must define only the Nebius service pool")
    for collection in ("policies", "additional_workloads", "pipeline_profile_policies"):
        for row in raw.get(collection, []):
            if row.get("disposition") not in _DISPOSITIONS:
                raise ValueError(f"invalid disposition in {collection}")
            if not row.get("owner") or not row.get("reason"):
                raise ValueError(f"{collection} entries require owner and reason")
            changes = row.get("required_changes")
            if not isinstance(changes, list) or not changes:
                raise ValueError(f"{collection} entries require required_changes")


def build_report() -> dict[str, Any]:
    policy_bytes = POLICY_PATH.read_bytes()
    raw = tomllib.loads(policy_bytes.decode("utf-8"))
    _validate_policy(raw)
    policies = raw["policies"]
    entries = _load_entrypoints()
    workloads: list[dict[str, Any]] = []
    for slug in sorted(entries, key=str.encode):
        matches = [row for row in policies if fnmatch.fnmatchcase(slug, row["pattern"])]
        if len(matches) != 1:
            raise ValueError(
                f"benchmark {slug!r} must match exactly one compatibility policy; "
                f"matched {len(matches)}",
            )
        decision = matches[0]
        workloads.append(
            {
                "workload_id": slug,
                "kind": "benchmark_entry_point",
                **entries[slug],
                "policy_pattern": decision["pattern"],
                "disposition": decision["disposition"],
                "owner": decision["owner"],
                "reason": decision["reason"],
                "required_changes": decision["required_changes"],
            }
        )
    for row in raw.get("additional_workloads", []):
        workloads.append(
            {
                "workload_id": row["workload_id"],
                "kind": "dynamic_service_workload",
                "disposition": row["disposition"],
                "owner": row["owner"],
                "reason": row["reason"],
                "required_changes": row["required_changes"],
            }
        )
    profile_policy = raw.get("pipeline_profile_policies", [])
    profile_raw = tomllib.loads(RESOURCE_PROFILES_PATH.read_text(encoding="utf-8"))
    for profile in profile_raw.get("profiles", []):
        name = str(profile["name"])
        version = str(profile["version"])
        matches = [row for row in profile_policy if fnmatch.fnmatchcase(name, row["pattern"])]
        if len(matches) != 1:
            raise ValueError(
                f"pipeline profile {name!r} must match exactly one compatibility "
                f"policy; matched {len(matches)}",
            )
        decision = matches[0]
        workloads.append(
            {
                "workload_id": f"pipeline:{name}@{version}",
                "kind": "pipeline_resource_profile",
                "source": RESOURCE_PROFILES_PATH.relative_to(ROOT).as_posix(),
                "policy_pattern": decision["pattern"],
                "disposition": decision["disposition"],
                "owner": decision["owner"],
                "reason": decision["reason"],
                "required_changes": decision["required_changes"],
            }
        )
    ids = [row["workload_id"] for row in workloads]
    if len(ids) != len(set(ids)):
        raise ValueError("compatibility report contains duplicate workload ids")
    pool_policies = sorted(raw["pool_policies"], key=lambda row: row["logical_pool_id"])
    workload_policy_pool_id = raw["workload_policy_pool_id"]
    routed_workloads: list[dict[str, Any]] = []
    for row in workloads:
        specific = {
            "disposition": row.pop("disposition"),
            "owner": row.pop("owner"),
            "reason": row.pop("reason"),
            "required_actions": row.pop("required_changes"),
        }
        pool_dispositions = [
            {"logical_pool_id": workload_policy_pool_id, **specific}
        ]
        routed_workloads.append({**row, "pool_dispositions": pool_dispositions})
    pool_summary = {}
    for pool_id in sorted(row["logical_pool_id"] for row in pool_policies):
        decisions = [
            decision
            for workload in routed_workloads
            for decision in workload["pool_dispositions"]
            if decision["logical_pool_id"] == pool_id
        ]
        pool_summary[pool_id] = {
            "total": len(decisions),
            **{
                disposition: sum(decision["disposition"] == disposition for decision in decisions)
                for disposition in sorted(_DISPOSITIONS)
            },
        }
    source_hash = hashlib.sha256()
    source_hash.update(policy_bytes)
    for path in ENTRYPOINT_FILES:
        source_hash.update(path.relative_to(ROOT).as_posix().encode("utf-8"))
        source_hash.update(path.read_bytes())
    source_hash.update(RESOURCE_PROFILES_PATH.relative_to(ROOT).as_posix().encode("utf-8"))
    source_hash.update(RESOURCE_PROFILES_PATH.read_bytes())
    return {
        "schema_version": "loom.service-workload-compatibility-report.v2",
        "accepted_pool_ids": [row["logical_pool_id"] for row in pool_policies],
        "workload_policy_pool_id": workload_policy_pool_id,
        "execution_class_id": raw["execution_class_id"],
        "source_sha256": source_hash.hexdigest(),
        "summary": {"total_workloads": len(routed_workloads), "pools": pool_summary},
        "workloads": routed_workloads,
    }


def validate_topology() -> None:
    ExecutionTopologyV1.model_validate_json(TOPOLOGY_PATH.read_bytes())


def render_outputs() -> dict[Path, bytes]:
    validate_topology()
    outputs = {REPORT_PATH: _json_bytes(build_report())}
    for path, model in SCHEMA_OUTPUTS.items():
        outputs[path] = _json_bytes(model.model_json_schema(mode="validation"))
    return outputs


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    stale: list[str] = []
    for path, payload in render_outputs().items():
        if args.check:
            if not path.exists() or path.read_bytes() != payload:
                stale.append(path.relative_to(ROOT).as_posix())
        else:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(payload)
    if stale:
        print("execution contract artifacts are stale: " + ", ".join(stale), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

"""Bounded diagnostics for the disposable cutover fixture only."""
from __future__ import annotations

import json
import re
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any
from uuid import UUID

from scripts.ops.nebius_pool_cutover import stage_pool_cutover
from scripts.ops.nebius_pool_cutover_live import HTTPSPoolCutoverAPI
from scripts.ops.nebius_pool_role_fencing import qualify_retained_writer_workloads

_KINDS = frozenset({"Deployment", "ReplicaSet", "StatefulSet", "DaemonSet",
    "ReplicationController", "CronJob", "Job", "Pod"})
_LOCATIONS = {
    stage_pool_cutover.__code__: ("nebius_pool_cutover.py", "stage_pool_cutover"),
    HTTPSPoolCutoverAPI.preflight.__code__: ("nebius_pool_cutover_live.py", "preflight"),
    HTTPSPoolCutoverAPI.qualify_writer_bindings.__code__: ("nebius_pool_cutover_live.py", "qualify_writer_bindings"),
    qualify_retained_writer_workloads.__code__: ("nebius_pool_role_fencing.py", "qualify_retained_writer_workloads"),
}


def _mapping(value: Any) -> dict[str, Any]:
    return value if type(value) is dict else {}


def _name(value: Any) -> str | None:
    return value if (type(value) is str and len(value) <= 253
        and re.fullmatch(r"[a-z0-9](?:[-a-z0-9.]*[a-z0-9])?", value)) else None


def _uid(value: Any) -> str | None:
    if type(value) is str and len(value) == 36:
        try:
            if str(UUID(value)) == value:
                return value
        except ValueError:
            pass
    return None


def _revision(value: Any) -> str | None:
    # This disposable K3s fixture uses numeric revisions. Never print opaque text.
    return value if type(value) is str and re.fullmatch(r"[0-9]{1,20}", value) else None


def _choice(value: Any, choices: frozenset[str]) -> str | None:
    return value if type(value) is str and value in choices else None


def _workload(document: Any, identities: dict[str, Any]) -> dict[str, Any]:
    row = _mapping(document)
    metadata = _mapping(row.get("metadata"))
    uid = _uid(metadata.get("uid"))
    identity = identities.get(uid) if uid is not None else None
    owners = metadata.get("ownerReferences")
    owner_type = ("absent" if "ownerReferences" not in metadata else
        {list: "list", dict: "dict", str: "str", int: "int", bool: "bool", type(None): "null"}.get(type(owners), "other"))
    result: dict[str, Any] = {"kind": _choice(row.get("kind"), _KINDS), "name": _name(metadata.get("name")),
        "namespace": _name(metadata.get("namespace")), "uid": uid,
        "resource_version": _revision(metadata.get("resourceVersion")),
        "service_account": _name(identity[1]) if type(identity) is tuple and len(identity) == 2 else None,
        "owner_references_type": owner_type,
        "owner_references_count": len(owners) if type(owners) is list else None,
        "owners": []}
    if type(owners) is list:
        result["owners"] = [{"kind": _choice(_mapping(owner).get("kind"), _KINDS),
            "name": _name(_mapping(owner).get("name")), "uid": _uid(_mapping(owner).get("uid")),
            "controller": _mapping(owner).get("controller") if type(_mapping(owner).get("controller")) is bool else None}
            for owner in owners[:3]]
    return result


def _workload_failure(values: dict[str, Any]) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    documents, identities = _mapping(values.get("documents")), _mapping(values.get("identities"))
    current = _uid(values.get("uid"))
    row = documents.get(current, values.get("row")) if current is not None else values.get("row")
    workload = _workload(row, identities)
    visited = values.get("visited")
    visited = {_uid(uid) for uid in visited if _uid(uid) is not None} if type(visited) is set else set()
    parents = set()
    for uid in visited:
        owners = _mapping(_mapping(documents.get(uid)).get("metadata")).get("ownerReferences")
        if type(owners) is list and len(owners) == 1:
            parents.add(_uid(_mapping(owners[0]).get("uid")))
    starts = visited - parents
    start = next(iter(starts)) if len(starts) == 1 else current
    ancestry: list[dict[str, Any]] = []
    seen: set[str] = set()
    roots = values.get("roots")
    roots = roots if type(roots) is set else set()
    while start is not None and start not in seen and len(ancestry) < 3:
        seen.add(start)
        if start not in documents:
            break
        node = _workload(documents[start], identities)
        node["retained_root"] = start in roots
        ancestry.append(node)
        if node["owner_references_count"] != 1:
            break
        start = node["owners"][0]["uid"]
    return workload, ancestry


def journal_phases(record: Any) -> dict[str, Any]:
    """Only enum phase names; no resource keys, checksums or journal payloads."""
    if type(record) is not dict:
        return {"record": "absent" if record is None else "unavailable"}
    phases = frozenset({"prepared", "intent", "stopped", "staged"})
    result: dict[str, Any] = {"record": "present"}
    for field in ("producers", "runtime", "runtime_access"):
        values = _mapping(record.get(field)).values()
        known = {_choice(value if field == "runtime_access" else _mapping(value).get("phase"), phases)
            for value in values}
        result[field] = sorted(value for value in known if value is not None)
    result["stages"] = [phase for phase in ("material", "configuration", "authority", "workload")
        if phase in _mapping(record.get("phases"))]
    return result


def cutover_failure_report(error: BaseException, invocation: int) -> dict[str, Any]:
    """Read the actual failed frames, never another Kubernetes or journal read."""
    report: dict[str, Any] = {"invocation": invocation, "locations": [],
        "snapshot_resource_version": None, "journal": {"record": "unavailable"},
        "workload": None, "ancestry": []}
    current: BaseException | None = error
    seen: set[int] = set()
    while current is not None and id(current) not in seen and len(seen) < 8:
        seen.add(id(current))
        frame = current.__traceback__
        for _ in range(64):
            if frame is None:
                break
            code, values = frame.tb_frame.f_code, frame.tb_frame.f_locals
            if code in _LOCATIONS:
                filename, function = _LOCATIONS[code]
                if len(report["locations"]) < 32:
                    report["locations"].append({"file": filename, "function": function, "line": frame.tb_lineno})
                if code is stage_pool_cutover.__code__ and "record" in values:
                    report["journal"] = journal_phases(values["record"])
                elif code is HTTPSPoolCutoverAPI.qualify_writer_bindings.__code__:
                    report["snapshot_resource_version"] = _revision(values.get("revision"))
                elif code is qualify_retained_writer_workloads.__code__:
                    report["workload"], report["ancestry"] = _workload_failure(values)
            frame = frame.tb_next
        current = current.__context__
    return report


@contextmanager
def observe_cutover_stage(invocation: int) -> Iterator[None]:
    try:
        yield
    except Exception as error:
        try:
            print("disposable cutover failure:", json.dumps(cutover_failure_report(error, invocation), sort_keys=True))
        except Exception:
            pass
        raise

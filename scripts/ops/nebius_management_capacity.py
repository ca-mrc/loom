"""Conservative single-platform-node fit for initial management installation.

Count complete controller/Pod inventory, including maintenance that has not
started and rollout surge. This proves scheduler request fit, not persistent-disk
quota, billing allowance or future provider availability. No resources are changed.
"""
from __future__ import annotations

import copy
import json
import re
from collections import defaultdict
from types import SimpleNamespace
from typing import Any
from uuid import UUID

from loom.nebius_environment_render import PlatformEnvelope
from loom_execution_capacity_collector import kubernetes as accounting
from loom_execution_capacity_collector.contracts import ResourceTotals

_KINDS = {"Deployment", "StatefulSet", "ReplicaSet", "DaemonSet", "Job", "CronJob"}
_Key = tuple[str, str, str]


class ManagementCapacityError(RuntimeError):
    """Inventory payloads and private Pod specifications are not diagnostics."""


def capacity_error_type(error: Exception) -> str:
    name = type(error).__name__
    return name if name in {'ValueError', 'KeyError', 'TypeError', 'AttributeError',
        'ManagementCapacityError', 'ManagementPrerequisiteError'} else 'OtherError'


def _key(row: dict[str, Any]) -> _Key:
    return row["kind"], row["metadata"].get("namespace", ""), row["metadata"]["name"]


def _inventory(rows: list[dict[str, Any]]) -> None:
    ids, keys = set(), set()
    for row in rows:
        identity, key = row["metadata"]["uid"], _key(row)
        if str(UUID(identity)) != identity or UUID(identity).int == 0 or identity in ids or key in keys:
            raise ValueError()
        ids.add(identity)
        keys.add(key)


def _root(row: dict[str, Any], controllers: dict[str, dict[str, Any]]) -> _Key:
    seen: set[_Key] = set()
    while True:
        key = _key(row)
        if key in seen or len(seen) >= 16:
            raise ValueError()
        seen.add(key)
        owners = [owner for owner in row["metadata"].get("ownerReferences", []) if owner.get("controller") is True]
        if not owners:
            return key
        if len(owners) != 1:
            raise ValueError()
        owner = owners[0]
        parent = controllers.get(owner["uid"])
        if parent is None:
            # A retained/orphan Pod is still charged, never discarded because
            # its old controller no longer appears in the complete inventory.
            return key
        if _key(parent) != (owner["kind"], key[1], owner["name"]):
            raise ValueError()
        row = parent


def _count(row: dict[str, Any]) -> int:
    kind, spec = row["kind"], row["spec"]
    if kind == "CronJob":
        if spec.get("concurrencyPolicy") != "Forbid":
            raise ValueError("unbounded maintenance concurrency")
        spec = spec["jobTemplate"]["spec"]
    if kind == "Job" and any(c.get("type") in {"Complete", "Failed"} and c.get("status") == "True"
                             for c in row.get("status", {}).get("conditions", [])):
        return 0
    count = spec.get("parallelism", 1) if kind in {"Job", "CronJob"} else spec.get("replicas", 1)
    if type(count) is not int or not 0 <= count <= 10000:
        raise ValueError()
    if kind == "DaemonSet":
        # Fit is per node: a positive fleet-wide surge can put an old and a
        # replacement daemon on this node. It does not put the entire fleet's
        # surge here. Already-observed terminating Pods are charged separately.
        strategy = spec.get("updateStrategy", {})
        if strategy.get("type", "RollingUpdate") == "RollingUpdate":
            surge = strategy.get("rollingUpdate", {}).get("maxSurge", 0)
            if isinstance(surge, str) and re.fullmatch(r"[0-9]+%", surge):
                surge = int(surge[:-1])
            if type(surge) is not int or not 0 <= surge <= 10000:
                raise ValueError()
            return 2 if surge else 1
        if strategy["type"] != "OnDelete":
            raise ValueError()
        return 1
    if kind == "Deployment":
        strategy = spec.get("strategy", {})
        if strategy.get("type", "RollingUpdate") == "RollingUpdate":
            surge = strategy.get("rollingUpdate", {}).get("maxSurge", "25%")
            if isinstance(surge, str) and re.fullmatch(r"[0-9]+%", surge):
                count += (count * int(surge[:-1]) + 99) // 100
            elif type(surge) is int and 0 <= surge <= 10000:
                count += surge
            else:
                raise ValueError()
        elif strategy["type"] != "Recreate":
            raise ValueError()
    return count


def _pod(row: dict[str, Any]) -> Any:
    if row["kind"] != "Pod":
        spec = row["spec"]["jobTemplate"]["spec"] if row["kind"] == "CronJob" else row["spec"]
        row = {"apiVersion": "v1", "kind": "Pod", **copy.deepcopy(spec["template"])}
    response = SimpleNamespace(data=json.dumps({"apiVersion": "v1", "kind": "PodList", "items": [row]}).encode())
    return accounting._decode_pool_pods(response).items[0]


def _matches(pod: Any, node: Any) -> bool:
    return bool(accounting._daemonset_matches_node(SimpleNamespace(spec=SimpleNamespace(template=SimpleNamespace(spec=pod.spec))), node))


def qualify_platform_capacity(*, nodes: list[dict[str, Any]], pods: list[dict[str, Any]], controllers: list[dict[str, Any]],
                              planned: list[dict[str, Any]], reserve: PlatformEnvelope, reserve_pods: int,
                              diagnostic: dict[str, Any] | None = None) -> dict[str, Any]:
    """Fit fixed management manifests plus child allowance beside live workloads.

    Existing and planned controllers with the same identity share one envelope;
    the installer's UID/manifest journals separately reject collisions or drift.
    Parent UIDs associate live ReplicaSets/Jobs/Pods without trusting app labels.
    All declared requests and Pod-level/init/sidecar overhead are included. The
    largest old/new Pod request is reserved across the full rollout population.
    A second unrelated system node is never borrowed as overflow.
    """
    from kubernetes import client

    detail = diagnostic if diagnostic is not None else {}
    detail.clear()
    detail.update(schema='loom.nebius-platform-capacity-diagnostic.v1', stage='validation',
        kind=None, error_type=None, nodes=[])
    try:
        nodes = [{"kind": "Node", "apiVersion": "v1", **row} for row in nodes]
        pods = [{"kind": "Pod", "apiVersion": "v1", **row} for row in pods]
        _inventory([*nodes, *pods, *controllers])
        if (any(row["kind"] not in _KINDS for row in [*controllers, *planned])
                or len({_key(row) for row in planned}) != len(planned)
                or any(row["metadata"].get("ownerReferences") for row in planned)
                or type(reserve_pods) is not int or reserve_pods < 0
                or any(type(value) is not int or value < 0 for value in vars(reserve).values())):
            raise ValueError()
        by_uid = {row["metadata"]["uid"]: row for row in controllers}
        def sample(row: dict[str, Any]) -> tuple[dict[str, Any], Any, int]:
            detail.update(stage='controller_decode', kind=row['kind'])
            pod = _pod(row)
            detail['stage'] = 'controller_count'
            return row, pod, _count(row)
        samples = [sample(row) for row in controllers]
        desired = [sample(row) for row in planned]
        detail.update(stage='pod_decode', kind='Pod')
        actual = [(row, _pod(row)) for row in pods if row.get("status", {}).get("phase") not in {"Succeeded", "Failed"}]
        detail.update(stage='node_decode', kind='Node')
        with client.ApiClient() as decoder:
            decoded = decoder.deserialize(SimpleNamespace(data=json.dumps({"apiVersion": "v1", "kind": "NodeList", "items": nodes}).encode()), "V1NodeList").items
        detail.update(stage='node_eligibility', kind=None)
        for node in decoded:
            if (not accounting._node_ready(node) or accounting._node_draining(node)
                    or node.metadata.labels.get("loom.nebius/node-role") != "system"
                    or node.metadata.labels.get("loom.nebius/platform") != "integration"
                    or any(condition.type in {"DiskPressure", "MemoryPressure", "PIDPressure", "NetworkUnavailable"}
                           and condition.status != "False" for condition in node.status.conditions or [])):
                continue
            if (not re.fullmatch(r"nebius://computeinstance-[a-z0-9]+", node.spec.provider_id or "")
                    or node.metadata.name != node.spec.provider_id.removeprefix("nebius://")):
                raise ValueError()
            detail['stage'] = 'placement'
            if any(not _matches(pod, node) for _, pod, _ in desired):
                detail['nodes'].append({'node_uid': node.metadata.uid, 'placement_matches': False,
                    'allocatable': None, 'required': None})
                continue
            detail['stage'] = 'accounting'
            allocation = accounting._required_node_resources(node.status.allocatable, name="platform")
            slots = accounting._positive_int(node.status.allocatable.get("pods"), name="platform Pod slots")
            requests: dict[_Key, list[ResourceTotals]] = defaultdict(list)
            counts: dict[_Key, int] = defaultdict(int)
            children: dict[_Key, int] = defaultdict(int)
            observed: dict[_Key, int] = defaultdict(int)
            for row, pod, count in [*samples, *desired]:
                if not count or not _matches(pod, node):
                    continue
                root = _root(row, by_uid)
                requests[root].append(accounting._pod_request(pod))
                if root == _key(row):
                    counts[root] = max(counts[root], count)
                else:
                    children[root] += count
            for row, pod in actual:
                if ((pod.spec.node_name and pod.spec.node_name != node.metadata.name)
                        or (not pod.spec.node_name and not _matches(pod, node))):
                    continue
                root = _root(row, by_uid)
                requests[root].append(accounting._pod_request(pod))
                observed[root] += 1
            used = ResourceTotals(cpu_millis=reserve.cpu_millis, memory_mib=reserve.memory_mib, storage_mib=reserve.ephemeral_storage_mib)
            pod_slots = reserve_pods
            for root, values in requests.items():
                per_pod = accounting._maximum(*values)
                count = max(counts[root], children[root], observed[root])
                used = accounting._add(used, ResourceTotals(cpu_millis=per_pod.cpu_millis * count,
                    memory_mib=per_pod.memory_mib * count, storage_mib=per_pod.storage_mib * count))
                pod_slots += count
            required = {'cpu_millis': used.cpu_millis, 'memory_mib': used.memory_mib,
                'ephemeral_storage_mib': used.storage_mib, 'pods': pod_slots}
            detail['nodes'].append({'node_uid': node.metadata.uid, 'placement_matches': True,
                'allocatable': {'cpu_millis': allocation.cpu_millis, 'memory_mib': allocation.memory_mib,
                    'ephemeral_storage_mib': allocation.storage_mib, 'pods': slots}, 'required': required})
            detail['stage'] = 'capacity'
            if (slots >= pod_slots and allocation.cpu_millis >= used.cpu_millis
                    and allocation.memory_mib >= used.memory_mib and allocation.storage_mib >= used.storage_mib):
                detail['stage'] = 'complete'
                return {"node_uid": node.metadata.uid, "required": required}
        raise ManagementCapacityError("eligible platform cannot fit management and reserved child headroom")
    except Exception as error:
        detail['error_type'] = capacity_error_type(error)
        if isinstance(error, ManagementCapacityError):
            raise
        raise ManagementCapacityError("complete management platform inventory unqualified") from None

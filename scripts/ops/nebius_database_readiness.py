"""Pure retained PostgreSQL Pod and Service-backend qualification.

Shared by development startup and migration observation. No transport, SQL,
legacy request, workload mutation or pool admission authority lives here.
"""
from __future__ import annotations

import copy
import ipaddress
from typing import Any

from scripts.ops.nebius_ingress_stage import _snapshot, _uid
from scripts.ops.nebius_management_evidence import _matches_backup_template


def _owner(document: dict[str, Any], *, kind: str, name: str, uid: str) -> None:
    owners = document["metadata"].get("ownerReferences", [])
    if len(owners) != 1:
        raise ValueError
    actual = dict(owners[0])
    actual.pop("blockOwnerDeletion", None)
    if actual != {"apiVersion": "apps/v1", "kind": kind, "name": name, "uid": uid, "controller": True}:
        raise ValueError


def qualify_database_pod(*, namespace: str, database: dict[str, Any], service: dict[str, Any],
        retained_database: dict[str, Any], retained_service: dict[str, Any],
        listing: dict[str, Any]) -> dict[str, Any]:
    """Return the sole ready Pod of the exact retained PostgreSQL StatefulSet."""
    for actual, wanted in ((database, retained_database), (service, retained_service)):
        if _uid(actual) != _uid(wanted) or _snapshot(actual) != _snapshot(wanted):
            raise ValueError
    spec, status = database["spec"], database.get("status", {})
    if (type(spec.get("replicas")) is not int or spec["replicas"] != 1
            or spec.get("serviceName") != "loom-postgres" or spec.get("ordinals", {}).get("start", 0) != 0
            or spec["selector"] != {"matchLabels": {"app": "loom-postgres"}}
            or service["spec"]["selector"] != {"app": "loom-postgres"}
            or service["spec"].get("type", "ClusterIP") != "ClusterIP"
            or len(service["spec"]["ports"]) != 1 or service["spec"]["ports"][0]["port"] != 5432
            or service["spec"]["ports"][0]["targetPort"] != 5432
            or status.get("observedGeneration", 0) < database["metadata"].get("generation", 1)
            or any(type(status.get(key)) is not int or status[key] != 1
                for key in ("replicas", "readyReplicas", "currentReplicas", "updatedReplicas"))
            or not status.get("currentRevision") or status["currentRevision"] != status.get("updateRevision")):
        raise ValueError
    if (listing.get("apiVersion") != "v1" or listing.get("kind") != "PodList"
            or listing.get("metadata", {}).get("continue") or not listing.get("metadata", {}).get("resourceVersion")
            or len(listing.get("items", [])) != 1):
        raise ValueError
    pod = {"apiVersion": "v1", "kind": "Pod", **listing["items"][0]}
    meta = pod["metadata"]
    _uid(pod)
    if (pod["apiVersion"] != "v1" or pod["kind"] != "Pod" or meta.get("namespace") != namespace
            or meta.get("name") != "loom-postgres-0" or meta.get("deletionTimestamp")
            or meta.get("labels", {}).get("app") != "loom-postgres"
            or meta["labels"].get("controller-revision-hash") != status["currentRevision"]):
        raise ValueError
    _owner(pod, kind="StatefulSet", name="loom-postgres", uid=_uid(database))
    expected = copy.deepcopy(spec["template"]["spec"])
    claims = spec.get("volumeClaimTemplates", [])
    if len(claims) != 1 or claims[0]["metadata"].get("name") != "data":
        raise ValueError
    expected.setdefault("volumes", []).append({"name": "data", "persistentVolumeClaim": {"claimName": "data-loom-postgres-0"}})
    actual = pod["spec"]
    # StatefulSet may order its injected PVC volume before other volumes.
    actual = {**actual, "volumes": sorted(actual.get("volumes", []), key=lambda row: row["name"])}
    expected["volumes"].sort(key=lambda row: row["name"])
    if (not _matches_backup_template(actual, expected)
            or actual.get("initContainers", []) != expected.get("initContainers", [])
            or actual.get("securityContext", {}) != expected.get("securityContext", {})
            or actual.get("ephemeralContainers", []) != expected.get("ephemeralContainers", [])
            or actual.get("serviceAccountName", "default") != expected.get("serviceAccountName", "default")
            or any(actual.get(field, False) != expected.get(field, False)
                for field in ("hostNetwork", "hostPID", "hostIPC", "shareProcessNamespace"))
            or len(expected["containers"]) != 1 or expected["containers"][0]["name"] != "loom-postgres"
            or pod.get("status", {}).get("phase") != "Running"):
        raise ValueError
    container, wanted = actual["containers"][0], expected["containers"][0]
    if (container.keys() - wanted.keys() - {"imagePullPolicy", "terminationMessagePath", "terminationMessagePolicy"}
            or container.get("securityContext", {}) != wanted.get("securityContext", {})):
        raise ValueError
    states = pod["status"].get("containerStatuses", [])
    if len(states) != 1 or states[0].get("name") != "loom-postgres" or states[0].get("ready") is not True:
        raise ValueError
    return pod


def qualify_database_backend(*, namespace: str, service: dict[str, Any],
        pod: dict[str, Any], listing: dict[str, Any]) -> None:
    """Require every Service address family to route solely to this ready Pod."""
    if (listing.get("apiVersion") != "discovery.k8s.io/v1" or listing.get("kind") != "EndpointSliceList"
            or listing.get("metadata", {}).get("continue")
            or not isinstance(listing.get("metadata", {}).get("resourceVersion"), str)
            or not 0 < len(listing["metadata"]["resourceVersion"]) <= 128
            or not isinstance(listing.get("items"), list) or not 0 < len(listing["items"]) <= 2
            or service["spec"].get("publishNotReadyAddresses", False) is not False):
        raise ValueError
    status = pod["status"]
    primary = ipaddress.ip_address(status["podIP"])
    addresses = status["podIPs"]
    if not isinstance(addresses, list) or not 0 < len(addresses) <= 2:
        raise ValueError
    pod_ips = {str(ipaddress.ip_address(row["ip"])) for row in addresses if set(row) == {"ip"}}
    families = service["spec"].get("ipFamilies", ["IPv" + str(primary.version)])
    if (len(pod_ips) != len(addresses) or str(primary) not in pod_ips or not isinstance(families, list)
            or not 0 < len(families) <= 2 or len(set(families)) != len(families)
            or not set(families) <= {"IPv4", "IPv6"}):
        raise ValueError
    expected = {address for address in pod_ips if "IPv" + str(ipaddress.ip_address(address).version) in families}
    if len(expected) != len(families):
        raise ValueError
    seen: set[str] = set()
    slices: set[str] = set()
    service_port, = service["spec"]["ports"]
    for row in listing["items"]:
        metadata = row["metadata"]
        if (row.get("apiVersion", "discovery.k8s.io/v1") != "discovery.k8s.io/v1"
                or row.get("kind", "EndpointSlice") != "EndpointSlice" or metadata.get("namespace") != namespace
                or metadata.get("deletionTimestamp") or metadata.get("labels", {}).get("kubernetes.io/service-name") != "loom-postgres"
                or _uid(row) in slices or row["addressType"] not in families):
            raise ValueError
        slices.add(_uid(row))
        owner, = metadata["ownerReferences"]
        owner = dict(owner)
        blocking = owner.pop("blockOwnerDeletion", False)
        if (type(blocking) is not bool or owner != {"apiVersion": "v1", "kind": "Service", "name": "loom-postgres",
                "uid": _uid(service), "controller": True}):
            raise ValueError
        port, = row["ports"]
        if (port.keys() - {"name", "port", "protocol", "appProtocol"}
                or type(port.get("port")) is not int or port["port"] != 5432
                or port.get("protocol", "TCP") != "TCP" or port.get("name") != service_port.get("name")
                or port.get("appProtocol") != service_port.get("appProtocol")):
            raise ValueError
        endpoint, = row["endpoints"]
        conditions, reference = endpoint["conditions"], endpoint["targetRef"]
        if (conditions.get("ready") is not True or conditions.get("serving", True) is not True
                or conditions.get("terminating", False) is not False
                or reference.keys() - {"kind", "namespace", "name", "uid", "apiVersion", "resourceVersion"}
                or reference.get("apiVersion", "v1") != "v1"
                or any(reference.get(key) != value for key, value in {"kind": "Pod", "namespace": namespace,
                    "name": pod["metadata"]["name"], "uid": _uid(pod)}.items())):
            raise ValueError
        address, = endpoint["addresses"]
        parsed = ipaddress.ip_address(address)
        if str(parsed) != address or row["addressType"] != "IPv" + str(parsed.version) or address not in expected or address in seen:
            raise ValueError
        seen.add(address)
    if seen != expected:
        raise ValueError


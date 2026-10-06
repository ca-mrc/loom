"""Read-only Kubernetes node and Pod resource accounting."""

from __future__ import annotations

import asyncio
import json
from collections.abc import Callable
from decimal import ROUND_CEILING, ROUND_FLOOR, Decimal
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any

from loom.nebius_kubernetes import (
    NebiusKubernetesConnection,
    NebiusKubernetesCredentials,
    create_api_client,
)
from loom_execution_capacity_collector.contracts import (
    CapacityTargetScopeV1,
    DaemonSetPlacement,
    KubernetesCapacitySnapshot,
    ManagedPodPlacement,
    NodePlacement,
    NodeTemplateSample,
    ResourceTotals,
)

if TYPE_CHECKING:
    from loom_execution_capacity_collector.pool import PoolObservationScope, PoolPodClassifier

_MIB = Decimal(1024 * 1024)
_TARGET_ANNOTATION = "loom.openai.com/target-id"
_MANAGED_BY = "loom-execution-actuator"
_IMAGE_PULL_REASONS = {"ErrImagePull", "ImagePullBackOff"}


class KubernetesObservationError(RuntimeError):
    """Kubernetes did not return a complete, safely scoped inventory."""


def _quantity(value: object, *, kind: str, capacity: bool = False) -> int:
    if value is None:
        return 0
    try:
        from kubernetes.utils.quantity import parse_quantity

        parsed = Decimal(parse_quantity(str(value)))
    except (ArithmeticError, TypeError, ValueError) as exc:
        raise KubernetesObservationError(f"invalid Kubernetes {kind} quantity") from exc
    if parsed < 0:
        raise KubernetesObservationError(f"negative Kubernetes {kind} quantity")
    if kind == "cpu":
        scaled = parsed * 1000
    else:
        scaled = parsed / _MIB
    rounding = ROUND_FLOOR if capacity else ROUND_CEILING
    return int(scaled.to_integral_value(rounding=rounding))


def _resources(values: object, *, capacity: bool = False) -> ResourceTotals:
    mapping = values if isinstance(values, dict) else {}
    return ResourceTotals(
        cpu_millis=_quantity(mapping.get("cpu"), kind="cpu", capacity=capacity),
        memory_mib=_quantity(mapping.get("memory"), kind="memory", capacity=capacity),
        storage_mib=_quantity(mapping.get("ephemeral-storage"), kind="storage", capacity=capacity),
    )


def _required_node_resources(values: object, *, name: str) -> ResourceTotals:
    result = _resources(values, capacity=True)
    if min(result.cpu_millis, result.memory_mib, result.storage_mib) <= 0:
        raise KubernetesObservationError(f"Kubernetes node {name} resources are incomplete")
    return result


def _add(*values: ResourceTotals) -> ResourceTotals:
    return ResourceTotals(
        cpu_millis=sum(value.cpu_millis for value in values),
        memory_mib=sum(value.memory_mib for value in values),
        storage_mib=sum(value.storage_mib for value in values),
    )


def _maximum(*values: ResourceTotals) -> ResourceTotals:
    return ResourceTotals(
        cpu_millis=max((value.cpu_millis for value in values), default=0),
        memory_mib=max((value.memory_mib for value in values), default=0),
        storage_mib=max((value.storage_mib for value in values), default=0),
    )


def _container_request(container: Any) -> ResourceTotals:
    resources = getattr(container, "resources", None)
    return _resources(getattr(resources, "requests", None) or {})


def _pod_request(pod: Any) -> ResourceTotals:
    """Conservative scheduler accounting, including native init sidecars."""

    if getattr(pod.spec, "_loom_pool_resize_unqualified", False):
        raise KubernetesObservationError("Kubernetes pool Pod resize accounting is unqualified")
    regular = _add(*[_container_request(row) for row in list(pod.spec.containers or [])])
    restartable = ResourceTotals(cpu_millis=0, memory_mib=0, storage_mib=0)
    init_peaks: list[ResourceTotals] = []
    for container in list(getattr(pod.spec, "init_containers", None) or []):
        request = _container_request(container)
        if getattr(container, "restart_policy", None) == "Always":
            restartable = _add(restartable, request)
        else:
            init_peaks.append(_add(restartable, request))
    effective = _maximum(_add(regular, restartable), *init_peaks)
    pod_resources = getattr(pod.spec, "resources", None)
    if pod_resources is not None:
        requests = (pod_resources.get("requests", {}) if isinstance(pod_resources, dict)
                    else getattr(pod_resources, "requests", None) or {})
        # Admission validates Pod requests against container requests. Taking
        # the maximum also stays conservative for unfamiliar API combinations.
        effective = _maximum(effective, _resources(requests))
    return _add(effective, _resources(getattr(pod.spec, "overhead", None) or {}))


def rendered_pod_resources(spec: dict[str, Any]) -> ResourceTotals:
    """Apply the collector's scheduler arithmetic to a fixed, pre-admission Pod.

    Renderers must explicitly request every resource; silently relying on API
    limit-to-request defaulting could undercount a Job before its Pod appears.
    RuntimeClass overhead must be supplied by the qualified protected profile.
    """
    def container(raw: dict[str, Any]) -> SimpleNamespace:
        resources = raw.get("resources") or {}
        requests = resources.get("requests")
        if (not isinstance(requests, dict)
                or any(requests.get(name) is None for name in ("cpu", "memory", "ephemeral-storage"))):
            raise KubernetesObservationError("rendered Pod resource requests are incomplete")
        return SimpleNamespace(resources=SimpleNamespace(requests=requests), restart_policy=raw.get("restartPolicy"))

    if not spec.get("containers"):
        raise KubernetesObservationError("rendered Pod has no containers")
    return _pod_request(SimpleNamespace(spec=SimpleNamespace(
        containers=[container(row) for row in spec["containers"]],
        init_containers=[container(row) for row in spec.get("initContainers", [])],
        resources=spec.get("resources"), overhead=spec.get("overhead"),
    )))


def _pool_resize_unqualified(raw: dict[str, Any], pod_request: ResourceTotals) -> bool:
    """Do not admit against freed spec resources while kubelet still holds them.

    Only used by pool capture. Immutable gateway Jobs do not resize; legitimate
    foreign resizing remains a fail-closed inventory blocker until converged.
    Retain both older and newer API fields before the pinned SDK drops them.
    """
    status = raw.get("status") or {}
    if status.get("resize") or any(
        condition.get("type") in {"PodResizePending", "PodResizeInProgress"}
        and condition.get("status") == "True" for condition in status.get("conditions") or []
    ):
        return True

    def exceeds(allocated: Any, requested: ResourceTotals) -> bool:
        if not isinstance(allocated, dict):
            raise ValueError("invalid resize resources")
        actual = _resources(allocated)
        return any(getattr(actual, key) > getattr(requested, key)
                   for key in ("cpu_millis", "memory_mib", "storage_mib"))

    spec = raw["spec"]
    # Newer kubelets report aggregate Pod allocations even without explicit
    # spec.resources. Compare with the complete request already charged, not
    # an absent PodLevelResources field (zero). Excess still fails closed.
    if (exceeds((status.get("resources") or {}).get("requests") or {}, pod_request)
            or exceeds(status.get("allocatedResources") or {}, pod_request)):
        return True
    for containers_key, statuses_key in (("containers", "containerStatuses"), ("initContainers", "initContainerStatuses")):
        desired = {row["name"]: (row.get("resources") or {}).get("requests") or {}
                   for row in spec.get(containers_key) or []}
        for container in status.get(statuses_key) or []:
            requested_raw = desired.get(container.get("name"), {})
            if not isinstance(requested_raw, dict):
                raise ValueError("invalid resize resources")
            requested = _resources(requested_raw)
            if (exceeds(container.get("allocatedResources") or {}, requested)
                    or exceeds((container.get("resources") or {}).get("requests") or {}, requested)):
                return True
    return False


def _decode_pool_pods(response: Any) -> Any:
    """Preserve PodLevelResources omitted by the pinned Kubernetes SDK model."""
    from kubernetes import client

    try:
        data = response.data
        if not isinstance(data, bytes) or len(data) > 32 * 1024 * 1024:
            raise ValueError("invalid Pod page")
        document = json.loads(data)
        if (not isinstance(document, dict) or document.get("kind") != "PodList"
                or document.get("apiVersion") != "v1" or not isinstance(document.get("items"), list)):
            raise ValueError("incomplete Pod page")
        with client.ApiClient() as api:
            result = api.deserialize(response, "V1PodList")
        for raw, pod in zip(document["items"], result.items, strict=True):
            resources = raw["spec"].get("resources")
            if resources is not None:
                if not isinstance(resources, dict) or not isinstance(resources.get("requests", {}), dict):
                    raise ValueError("invalid Pod resource requests")
                pod.spec.resources = resources
            pod.spec._loom_pool_resize_unqualified = _pool_resize_unqualified(raw, _pod_request(pod))
        return result
    except Exception:
        # Neither the HTTP body nor deserializer diagnostics are evidence: Pod
        # payloads may contain credentials, commands or private source URLs.
        raise KubernetesObservationError("Kubernetes pool Pod page is invalid") from None
    finally:
        # _preload_content=False transfers ownership of the HTTP response.
        release = getattr(response, "release_conn", None)
        if release is not None:
            release()


def _condition(conditions: list[Any] | None, condition_type: str) -> Any | None:
    return next(
        (row for row in conditions or [] if getattr(row, "type", None) == condition_type),
        None,
    )


def _node_draining(node: Any) -> bool:
    # A cordon alone may be maintenance or startup; it does not prove a drain.
    return getattr(node.metadata, "deletion_timestamp", None) is not None or any(
        getattr(taint, "key", None) == "ToBeDeletedByClusterAutoscaler"
        for taint in getattr(node.spec, "taints", None) or []
    )


def _node_ready(node: Any) -> bool:
    ready = _condition(getattr(node.status, "conditions", None), "Ready")
    return (
        ready is not None
        and getattr(ready, "status", None) == "True"
        and getattr(node.metadata, "deletion_timestamp", None) is None
        and not bool(getattr(node.spec, "unschedulable", False))
    )


def _identity(value: object, *, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise KubernetesObservationError(f"Kubernetes {name} is unavailable")
    return value


def _positive_int(value: object, *, name: str) -> int:
    try:
        parsed = int(str(value))
    except (TypeError, ValueError) as exc:
        raise KubernetesObservationError(f"Kubernetes {name} is invalid") from exc
    if isinstance(value, bool) or parsed <= 0:
        raise KubernetesObservationError(f"Kubernetes {name} is invalid")
    return parsed


def _plain(value: Any) -> Any:
    if hasattr(value, "to_dict"):
        return _plain(value.to_dict())
    if isinstance(value, SimpleNamespace):
        return _plain(vars(value))
    if isinstance(value, dict):
        return {key: _plain(item) for key, item in value.items() if item is not None}
    if isinstance(value, (list, tuple)):
        return [_plain(item) for item in value]
    return value


def _scheduling(spec: Any) -> dict[str, Any]:
    # Only scheduling fields, never commands, environment, volumes or credentials.
    return {
        field: _plain(getattr(spec, field))
        for field in (
            "node_selector",
            "tolerations",
            "affinity",
            "topology_spread_constraints",
            "scheduler_name",
            "priority_class_name",
            "runtime_class_name",
            "overhead",
        )
        if getattr(spec, field, None) is not None
    }


def _managed_placement(pod: Any) -> ManagedPodPlacement:
    labels = dict(getattr(pod.metadata, "labels", None) or {})
    native = labels.get("app.kubernetes.io/component") == "task-image-builder"
    identity = ("task-image:" + _identity(labels.get("loom.materialization-id"), name="materialization ID")
                if native else _identity(labels.get("loom.openai.com/lease-id"), name="Pod lease ID"))
    return ManagedPodPlacement(
        uid=_identity(getattr(pod.metadata, "uid", None), name="Pod UID"),
        # This placement-only key does not create a service execution lease.
        lease_id=identity,
        generation=_positive_int(labels.get("loom.lease-epoch" if native else "loom.openai.com/generation"), name="Pod generation"),
        requests=_pod_request(pod),
    )


def _matches_expression(expression: dict[str, Any], labels: dict[str, str]) -> bool:
    key, op = expression.get("key"), expression.get("operator")
    if not isinstance(key, str):
        raise KubernetesObservationError("DaemonSet node selector key is invalid")
    values = expression.get("values", [])
    if op == "In":
        return key in labels and labels[key] in values
    if op == "NotIn":
        return key not in labels or labels[key] not in values
    if op == "Exists":
        return key in labels
    if op == "DoesNotExist":
        return key not in labels
    if op in {"Gt", "Lt"}:
        try:
            left, right = int(labels[key]), int(values[0])
            return left > right if op == "Gt" else left < right
        except (KeyError, IndexError, TypeError, ValueError):
            return False
    raise KubernetesObservationError("unsupported DaemonSet node selector operator")


def _daemonset_matches_node(daemon: Any, node: Any) -> bool:
    """Only hard node constraints; Pod affinity is established by observed Pods."""
    spec = daemon.spec.template.spec
    labels = dict(getattr(node.metadata, "labels", None) or {})
    if any(
        labels.get(key) != value
        for key, value in (getattr(spec, "node_selector", None) or {}).items()
    ):
        return False
    affinity = _plain(getattr(spec, "affinity", None) or {})
    required = affinity.get("node_affinity", {}).get(
        "required_during_scheduling_ignored_during_execution"
    )
    if required is not None:
        terms = required.get("node_selector_terms", [])
        if not any(
            (term.get("match_expressions") or term.get("match_fields"))
            and all(_matches_expression(item, labels) for item in term.get("match_expressions", []))
            and all(
                _matches_expression(item, {"metadata.name": node.metadata.name})
                for item in term.get("match_fields", [])
            )
            for term in terms
        ):
            return False
    tolerations = _plain(getattr(spec, "tolerations", None) or [])
    for taint in _plain(getattr(node.spec, "taints", None) or []):
        if taint.get("effect") not in {"NoSchedule", "NoExecute"}:
            continue
        if not any(
            (not tol.get("effect") or tol["effect"] == taint["effect"])
            and (
                (
                    tol.get("operator") == "Exists"
                    and (not tol.get("key") or tol["key"] == taint["key"])
                )
                or (
                    tol.get("operator", "Equal") == "Equal"
                    and tol.get("key") == taint["key"]
                    and tol.get("value", "") == taint.get("value", "")
                )
            )
            for tol in tolerations
        ):
            return False
    return True


def _template_sample(
    node: Any, placement: NodePlacement, pods: list[Any], daemons: list[Any]
) -> NodeTemplateSample | None:
    if not placement.ready:
        return None
    daemon_requests: list[ResourceTotals] = []
    revisions: dict[str, int] = {}
    for daemon in daemons:
        if not _daemonset_matches_node(daemon, node):
            continue
        uid = _identity(getattr(daemon.metadata, "uid", None), name="DaemonSet UID")
        generation = _positive_int(daemon.metadata.generation, name="DaemonSet generation")
        status = daemon.status
        if (
            getattr(status, "observed_generation", None) != generation
            or getattr(status, "updated_number_scheduled", None)
            != getattr(status, "desired_number_scheduled", None)
            or getattr(daemon.spec.template.spec, "runtime_class_name", None)
        ):
            return None
        matches = [
            pod
            for pod in pods
            if any(
                getattr(owner, "kind", None) == "DaemonSet" and getattr(owner, "uid", None) == uid
                for owner in getattr(pod.metadata, "owner_references", None) or []
            )
        ]
        if (
            len(matches) != 1
            or matches[0].status.phase != "Running"
            or getattr(matches[0].metadata, "deletion_timestamp", None)
        ):
            return None
        actual = _pod_request(matches[0])
        declared = _pod_request(SimpleNamespace(spec=daemon.spec.template.spec))
        if actual != declared:
            return None
        daemon_requests.append(actual)
        revisions[uid] = generation
    # A disappearing controller or a still-running old DaemonSet must not vanish
    # from the next-node overhead estimate.
    observed_daemons = {
        getattr(owner, "uid", None)
        for pod in pods
        for owner in getattr(pod.metadata, "owner_references", None) or []
        if getattr(owner, "kind", None) == "DaemonSet"
    }
    if observed_daemons != set(revisions):
        return None
    version = getattr(getattr(node.status, "node_info", None), "kubelet_version", None)
    if not isinstance(version, str) or not version:
        return None
    return NodeTemplateSample(
        node_uid=placement.uid,
        allocatable=placement.allocatable,
        pod_slots=placement.pod_slots,
        daemonset_requests=_add(*daemon_requests),
        daemonset_slots=len(daemon_requests),
        kubelet_version=version,
        daemonsets=revisions,
    )


def _target_pod(pod: Any, *, namespace: str, target_id: str, target_scope: CapacityTargetScopeV1 | None = None) -> bool:
    metadata = pod.metadata
    labels = dict(getattr(metadata, "labels", None) or {})
    annotations = dict(getattr(metadata, "annotations", None) or {})
    native = labels.get("app.kubernetes.io/component") == "task-image-builder"
    return (
        metadata.namespace == (namespace + "-build" if native else namespace)
        and (labels.get("app.kubernetes.io/managed-by") == _MANAGED_BY
             or labels.get("app.kubernetes.io/component") == "task-image-builder")
        and annotations.get(_TARGET_ANNOTATION) in (target_scope.target_ids if target_scope else [target_id])
    )


def _pending_state(pod: Any) -> tuple[bool, bool, bool, str | None]:
    if getattr(pod.status, "phase", None) != "Pending":
        return False, False, False, None
    scheduled = _condition(getattr(pod.status, "conditions", None), "PodScheduled")
    unschedulable = bool(
        scheduled is not None
        and getattr(scheduled, "status", None) == "False"
        and getattr(scheduled, "reason", None) == "Unschedulable"
    )
    statuses = [
        *list(getattr(pod.status, "init_container_statuses", None) or []),
        *list(getattr(pod.status, "container_statuses", None) or []),
    ]
    waiting_reasons = [
        getattr(getattr(getattr(row, "state", None), "waiting", None), "reason", None)
        for row in statuses
    ]
    image_pull = any(reason in _IMAGE_PULL_REASONS for reason in waiting_reasons)
    if image_pull:
        reason = next(str(reason) for reason in waiting_reasons if reason in _IMAGE_PULL_REASONS)
    elif unschedulable:
        reason = "Unschedulable"
    else:
        raw = getattr(pod.status, "reason", None)
        reason = str(raw)[:120] if isinstance(raw, str) and raw else "Pending"
    return True, unschedulable, image_pull, reason


class InClusterKubernetesCapacityReader:
    def __init__(
        self,
        *,
        connection: NebiusKubernetesConnection | None = None,
        core_api: Any | None = None,
        apps_api: Any | None = None,
        request_timeout_seconds: float = 15.0,
    ) -> None:
        if not 1.0 <= request_timeout_seconds <= 60.0:
            raise ValueError("Kubernetes request timeout must be between 1 and 60 seconds")
        self._api_client: Any | None = None
        self._credentials: NebiusKubernetesCredentials | None = None
        if connection is not None:
            if core_api is not None or apps_api is not None:
                raise ValueError("remote connection cannot be combined with injected clients")
            from kubernetes import client

            self._api_client, self._credentials = create_api_client(connection)
            core_api = client.CoreV1Api(self._api_client)
            apps_api = client.AppsV1Api(self._api_client)
        if core_api is None:
            try:
                from kubernetes import client, config
            except ModuleNotFoundError as exc:
                raise RuntimeError("install Loom with the cluster extra") from exc
            config.load_incluster_config()
            core_api = client.CoreV1Api()
        self._core = core_api
        if apps_api is None:
            from kubernetes import client

            apps_api = client.AppsV1Api()
        self._apps = apps_api
        self._request_timeout = request_timeout_seconds

    async def close(self) -> None:
        try:
            if self._api_client is not None:
                await asyncio.to_thread(self._api_client.close)
        finally:
            if self._credentials is not None:
                await self._credentials.close()

    def _list_all(
        self,
        method: Any,
        *,
        maximum_items: int,
        page_size: int,
        decode: Callable[[Any], Any] | None = None,
        **kwargs: object,
    ) -> tuple[list[Any], str]:
        items: list[Any] = []
        token = ""
        seen_tokens: set[str] = set()
        resource_version: str | None = None
        while True:
            response = method(
                **kwargs,
                limit=page_size,
                _continue=token or None,
                _request_timeout=(self._request_timeout, self._request_timeout),
            )
            if decode is not None:
                response = decode(response)
            metadata = getattr(response, "metadata", None)
            version = getattr(metadata, "resource_version", None)
            if not version or (resource_version is not None and str(version) != resource_version):
                raise KubernetesObservationError(
                    "Kubernetes paginated list resource version is unavailable or changed"
                )
            resource_version = str(version)
            items.extend(list(response.items))
            if len(items) > maximum_items:
                raise KubernetesObservationError("Kubernetes capacity inventory exceeded its bound")
            next_token = str(getattr(metadata, "_continue", None) or "")
            if not next_token:
                return items, resource_version
            if next_token in seen_tokens:
                raise KubernetesObservationError("Kubernetes pagination repeated a token")
            seen_tokens.add(next_token)
            token = next_token

    def _capture_sync(
        self,
        *,
        namespace: str,
        target_id: str,
        node_label_selector: str,
        pool: PoolPodClassifier | None = None,
        target_scope: CapacityTargetScopeV1 | None = None,
    ) -> KubernetesCapacitySnapshot:
        if target_scope is not None and (
            pool is not None or target_scope.owner_target_id != target_id
            or target_scope.namespace_name != namespace
        ):
            raise KubernetesObservationError("capacity target scope does not match collector identity")
        try:
            nodes, node_version = self._list_all(
                self._core.list_node,
                maximum_items=10_000,
                page_size=500,
                label_selector=node_label_selector,
            )
            pods, pod_version = self._list_all(
                self._core.list_pod_for_all_namespaces,
                maximum_items=200_000,
                page_size=1000,
                watch=False,
                decode=_decode_pool_pods if pool is not None else None,
                **({"_preload_content": False} if pool is not None else {}),
            )
            daemons, daemon_version = self._list_all(
                self._apps.list_daemon_set_for_all_namespaces,
                maximum_items=10_000,
                page_size=500,
                watch=False,
            )
        except Exception as exc:
            if isinstance(exc, KubernetesObservationError):
                raise
            raise KubernetesObservationError("Kubernetes capacity list failed") from exc
        if pool is not None:
            pool.validate_inventory(nodes, pods)
        node_names = {
            str(node.metadata.name)
            for node in nodes
            if isinstance(getattr(node.metadata, "name", None), str)
        }
        if len(node_names) != len(nodes):
            raise KubernetesObservationError("Kubernetes node identity is missing or duplicated")
        provisioned = _add(
            *[
                _required_node_resources(
                    getattr(node.status, "capacity", None) or {}, name="capacity"
                )
                for node in nodes
            ]
        )
        allocatable = _add(
            *[
                _required_node_resources(
                    getattr(node.status, "allocatable", None) or {}, name="allocatable"
                )
                for node in nodes
                if _node_ready(node)
            ]
        )
        requested_rows: list[ResourceTotals] = []
        pending_jobs = 0
        unschedulable_jobs = 0
        image_pull_jobs = 0
        reasons: dict[str, int] = {}
        by_node: dict[str, list[Any]] = {name: [] for name in node_names}
        pending_pods: list[ManagedPodPlacement] = []
        managed_by_uid: dict[str, ManagedPodPlacement] = {}
        observed_reservations: set[str] = set()
        for pod in pods:
            phase = getattr(pod.status, "phase", None)
            if phase in {"Succeeded", "Failed"}:
                continue
            node_name = getattr(pod.spec, "node_name", None) or None
            target = (_target_pod(pod, namespace=namespace, target_id=target_id, target_scope=target_scope)
                      if pool is None else pool.registered(pod))
            include_pending = target if pool is None else pool.includes_pending(pod)
            if pool is not None:
                target = target and include_pending
            managed = (_managed_placement(pod) if target else None) if pool is None else pool.managed(pod)
            if managed is not None:
                reservation = managed.lease_id if pool is not None else f"{managed.lease_id}:{managed.generation}"
                if (pool is not None or target_scope is not None) and reservation in observed_reservations:
                    raise KubernetesObservationError("multiple live Pods for one shared reservation")
                observed_reservations.add(reservation)
                managed_by_uid[managed.uid] = managed
            if target and node_name is not None and node_name not in node_names:
                raise KubernetesObservationError(
                    "managed target Pod is scheduled outside the selected node group"
                )
            if node_name in node_names or (include_pending and node_name is None and phase == "Pending"):
                requested_rows.append(_pod_request(pod))
            if node_name in node_names:
                by_node[node_name].append(pod)
            elif include_pending and node_name is None and phase == "Pending":
                pending_pods.append(managed if managed is not None else ManagedPodPlacement(
                    uid=_identity(pod.metadata.uid, name="Pod UID"),
                    lease_id="foreign-pod:" + pod.metadata.uid, generation=1, requests=_pod_request(pod),
                ))
            if not (target or (pool is not None and (node_name in node_names or include_pending))):
                continue
            pending, unschedulable, image_pull, reason = _pending_state(pod)
            pending_jobs += int(pending)
            unschedulable_jobs += int(unschedulable)
            image_pull_jobs += int(image_pull)
            if reason is not None:
                reasons[reason] = reasons.get(reason, 0) + 1
        placements: list[NodePlacement] = []
        samples: list[NodeTemplateSample] = []
        for node in nodes:
            assigned = by_node[node.metadata.name]
            placement = NodePlacement(
                uid=_identity(getattr(node.metadata, "uid", None), name="Node UID"),
                provider_id=_identity(
                    getattr(node.spec, "provider_id", None), name="Node provider ID"
                ),
                ready=_node_ready(node),
                unschedulable=bool(getattr(node.spec, "unschedulable", False)),
                deleting=getattr(node.metadata, "deletion_timestamp", None) is not None,
                draining=_node_draining(node),
                allocatable=_required_node_resources(node.status.allocatable, name="allocatable"),
                requested=_add(*[_pod_request(pod) for pod in assigned]),
                pod_slots=_positive_int(node.status.allocatable.get("pods"), name="node Pod slots"),
                used_pod_slots=len(assigned),
                managed_pods=[
                    managed_by_uid[pod.metadata.uid]
                    for pod in assigned
                    if pod.metadata.uid in managed_by_uid
                ],
            )
            placements.append(placement)
            sample = _template_sample(node, placement, assigned, daemons)
            if sample is not None:
                samples.append(sample)
        return KubernetesCapacitySnapshot(
            source_versions={
                "nodes": str(node_version),
                "pods": str(pod_version),
                "daemonsets": str(daemon_version),
                **({"pool_scope": pool.fingerprint} if pool is not None else {}),
            },
            active_nodes=len(nodes),
            ready_nodes=sum(_node_ready(node) for node in nodes),
            provisioned=provisioned,
            allocatable=allocatable,
            requested=_add(*requested_rows),
            pending_jobs=pending_jobs,
            unschedulable_jobs=unschedulable_jobs,
            image_pull_backoff_jobs=image_pull_jobs,
            pending_reasons=dict(sorted(reasons.items())),
            nodes=sorted(placements, key=lambda row: row.uid),
            pending_pods=sorted(pending_pods, key=lambda row: row.uid),
            daemonsets=sorted(
                [
                    DaemonSetPlacement(
                        uid=_identity(getattr(daemon.metadata, "uid", None), name="DaemonSet UID"),
                        generation=_positive_int(
                            daemon.metadata.generation, name="DaemonSet generation"
                        ),
                        requests=_pod_request(SimpleNamespace(spec=daemon.spec.template.spec)),
                        scheduling=_scheduling(daemon.spec.template.spec),
                    )
                    for daemon in daemons
                ],
                key=lambda row: row.uid,
            ),
            template_samples=sorted(samples, key=lambda row: row.node_uid),
            node_templates={
                node.metadata.uid: {
                    "labels": dict(getattr(node.metadata, "labels", None) or {}),
                    "taints": _plain(getattr(node.spec, "taints", None) or []),
                    "capacity": _required_node_resources(
                        node.status.capacity, name="capacity"
                    ).model_dump(),
                    "os_image": str(
                        getattr(getattr(node.status, "node_info", None), "os_image", None) or ""
                    ),
                }
                for node in nodes
            },
        )

    async def capture(
        self,
        *,
        namespace: str,
        target_id: str,
        node_label_selector: str,
        target_scope: CapacityTargetScopeV1 | None = None,
    ) -> KubernetesCapacitySnapshot:
        return await asyncio.to_thread(
            self._capture_sync,
            namespace=namespace,
            target_id=target_id,
            node_label_selector=node_label_selector,
            target_scope=target_scope,
        )

    async def capture_pool(self, *, scope: PoolObservationScope) -> KubernetesCapacitySnapshot:
        """One physical inventory; never combine per-environment snapshots.

        Only protected registry/Job-journal bindings may populate ``scope``.
        The global admission writer must separately bind this selector to the
        native node group and reject stale/incomplete provider observations.
        """
        from loom_execution_capacity_collector.pool import PoolPodClassifier

        pool = PoolPodClassifier(scope)
        reading = asyncio.create_task(asyncio.to_thread(
            self._capture_sync, namespace="", target_id="",
            node_label_selector=pool.node_selector, pool=pool,
        ))
        try:
            return await asyncio.shield(reading)
        except asyncio.CancelledError:
            # Cancelling to_thread does not stop the SDK read. Keep ownership
            # until its bounded calls finish before outer cleanup closes clients.
            while not reading.done():
                try:
                    await asyncio.shield(reading)
                except (asyncio.CancelledError, Exception):
                    pass
            # Retrieve a concurrent SDK failure without replacing cancellation.
            if not reading.cancelled():
                reading.exception()
            raise


__all__ = ["InClusterKubernetesCapacityReader", "KubernetesObservationError"]

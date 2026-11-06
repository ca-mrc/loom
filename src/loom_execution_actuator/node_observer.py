"""Publish bounded CRI/filesystem observations without an inbound node service."""

from __future__ import annotations

import json
import logging
import math
import re
import subprocess
import threading
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit
from uuid import UUID

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from loom_execution_actuator.host_resource_usage import CpuCounters, capture_host_usage
from loom_execution_actuator.resource_observations import BUILD_CONFIG_MAP_PREFIX, BUILD_SCHEMA

_LOG = logging.getLogger(__name__)
_LIMIT = 700 * 1024
_SELECTOR = "app.kubernetes.io/managed-by=loom-execution-actuator"
# Opt-in builder collection (#2750): separate inventory, schedule, deadline,
# state and ConfigMap, so a slow or failing build walk cannot delay, invalidate
# or replace the Trial snapshot. Storage stays complete-or-unknown per Pod.
_BUILD_LABEL = ("app.kubernetes.io/component", "task-image-builder")
_ID = re.compile(r"containerd://[a-f0-9]{8,128}\Z")
# Time kept for the builder ConfigMap read and write after the last storage walk.
_BUILD_PUBLISH_RESERVE = 4.0


class ObserverSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="LOOM_RESOURCE_OBSERVER_", extra="ignore", hide_input_in_errors=True
    )

    namespace: str = Field(min_length=1, max_length=63)
    execution_namespace: str = Field(min_length=1, max_length=63)
    node_name: str = Field(min_length=1, max_length=253)
    pod_name: str = Field(min_length=1, max_length=253)
    pod_uid: str = Field(min_length=36, max_length=36)
    kubernetes_endpoint: str | None = None
    interval_seconds: float = Field(default=15, ge=5, le=60)
    max_pods: int = Field(default=128, ge=1, le=128)
    # Unset keeps builder collection off; set only by the opt-in render.
    build_namespace: str | None = Field(default=None, min_length=1, max_length=63)
    build_interval_seconds: float = Field(default=30, ge=15, le=120)
    build_max_pods: int = Field(default=32, ge=1, le=64)
    helper_path: str = "/usr/local/bin/loom-node-resource-stats"
    runtime_endpoint: str = "unix:///var/run/loom-node/containerd.sock"
    pod_logs_root: str = "/var/log/pods"
    kubelet_pods_root: str = "/var/lib/kubelet/pods"

    @field_validator("kubernetes_endpoint")
    @classmethod
    def _api_origin(cls, value: str | None) -> str | None:
        if value is None:
            return value
        parts = urlsplit(value)
        try:
            valid_port = parts.port is None or 0 < parts.port <= 65535
        except ValueError:
            valid_port = False
        if (parts.scheme != "https" or not parts.hostname or not valid_port
                or parts.username or parts.password or parts.path not in {"", "/"}
                or parts.query or parts.fragment or any(c.isspace() for c in value)):
            raise ValueError("observer API must be an HTTPS origin")
        return value.rstrip("/")


class ObservationError(RuntimeError):
    """A fixed local reason, never a transport exception or host pathname."""


class HelperError(ObservationError):
    """The helper's own fixed failure code (for example ``observation_deadline``)."""

    def __init__(self, code: str) -> None:
        super().__init__("helper")
        self.code = code if re.fullmatch(r"[a-z_]{1,64}", code) else "helper_unavailable"


def _uuid(value: Any) -> str:
    try:
        if not isinstance(value, str) or str(UUID(value)) != value or not UUID(value).int:
            raise ValueError
        return value
    except (ValueError, AttributeError):
        raise ObservationError("identity") from None


def _json_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    obj: dict[str, Any] = {}
    for key, value in pairs:
        if key in obj:
            raise ObservationError("payload")
        obj[key] = value
    return obj


def _invalid_number(_: str) -> Any:
    raise ObservationError("payload")


def _stamp(value: Any) -> int | None:
    if not isinstance(value, str):
        return None
    try:
        at = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if at.utcoffset() is None:
            return None
        # Preserve nanosecond fractions instead of datetime's microsecond truncation.
        fraction = re.search(r"\.(\d{1,9})(?:Z|[+-]\d\d:\d\d)$", value)
        nanos = int(fraction[1].ljust(9, "0")) if fraction else 0
        return int(at.replace(microsecond=0).timestamp()) * 10**9 + nanos
    except (ValueError, OverflowError, OSError):
        return None


def _integer(value: Any) -> bool:
    return type(value) is int and 0 <= value <= 2**63 - 1


class NodeResourceObserver:
    schema = "loom.native-resource-observation.v1"
    config_map_prefix = "node-"
    include_host_usage = True
    deadline_seconds = 25.0

    def __init__(self, settings: ObserverSettings, core: Any) -> None:
        self.settings, self.core = settings, core
        self._cpu: dict[str, tuple[int, int]] = {}
        self._host_cpu: CpuCounters | None = None

    def _namespace(self) -> str:
        return self.settings.execution_namespace

    def _selector(self) -> tuple[str, str]:
        return ("app.kubernetes.io/managed-by", "loom-execution-actuator")

    def _max_pods(self) -> int:
        return self.settings.max_pods

    def _order(self, selected: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return selected

    def _request(self, pods: list[Any]) -> dict[str, Any]:
        selected: list[dict[str, Any]] = []
        for pod in pods:
            meta, spec = pod.metadata, pod.spec
            label, value = self._selector()
            if (meta.namespace != self._namespace()
                    or spec.node_name != self.settings.node_name
                    or (meta.labels or {}).get(label) != value):
                raise ObservationError("inventory")
            uid = _uuid(meta.uid)
            statuses = list(pod.status.container_statuses or []) + list(pod.status.init_container_statuses or [])
            containers = []
            seen: set[str] = set()
            for status in statuses:
                if status.name in seen:
                    raise ObservationError("inventory")
                seen.add(status.name)
                identity = status.container_id
                state = status.state
                running = getattr(state, "running", None) or getattr(state, "terminated", None)
                started = getattr(running, "started_at", None)
                if (not isinstance(identity, str) or not _ID.fullmatch(identity)
                        or not isinstance(started, datetime) or started.utcoffset() is None):
                    continue  # No invented identity for a container that never started.
                containers.append({"name": status.name, "container_id": identity,
                                   "started_at": started.isoformat()})
            volumes = []
            for volume in spec.volumes or []:
                if volume.empty_dir is not None:
                    volumes.append({"name": volume.name, "kind": "empty_dir",
                                    "medium": volume.empty_dir.medium or ""})
                elif getattr(volume, "config_map", None) is not None:
                    volumes.append({"name": volume.name, "kind": "config_map", "medium": ""})
                elif getattr(volume, "git_repo", None) is not None:
                    volumes.append({"name": volume.name, "kind": "git_repo", "medium": ""})
            selected.append({"uid": uid, "name": meta.name, "containers": containers,
                             "volumes": volumes})
        if len(selected) > self._max_pods():
            raise ObservationError("size")
        return {"schema_version": "loom.node-resource-request.v1",
                "namespace": self._namespace(), "pods": self._order(selected)}

    def _sample(self, request: dict[str, Any], timeout: float) -> dict[str, Any]:
        payload = json.dumps(request, separators=(",", ":"), allow_nan=False).encode()
        if len(payload) > _LIMIT:
            raise ObservationError("size")
        try:
            result = subprocess.run(
                [self.settings.helper_path, "--runtime-endpoint", self.settings.runtime_endpoint,
                 "--pod-logs-root", self.settings.pod_logs_root,
                 "--kubelet-pods-root", self.settings.kubelet_pods_root],
                input=payload, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
                timeout=timeout, check=False,
            )
            if len(result.stdout) > _LIMIT:
                raise ObservationError("helper")
            if result.returncode:
                try:
                    failure = json.loads(result.stdout)
                except ValueError:
                    failure = None
                code = failure.get("error") if isinstance(failure, dict) else None
                raise HelperError(code if isinstance(code, str) else "helper_unavailable")
            value = json.loads(result.stdout, object_pairs_hook=_json_object,
                               parse_constant=_invalid_number)
            if not isinstance(value, dict) or not isinstance(value.get("pods"), list):
                raise ObservationError("payload")
            return value
        except subprocess.TimeoutExpired:
            raise HelperError("helper_timeout") from None
        except (OSError, ValueError):
            raise ObservationError("helper") from None

    def _qualify(
        self, summary: dict[str, Any], request: dict[str, Any], captured: datetime, completed: datetime,
        *, track_cpu: bool = True,
    ) -> None:
        expected = {pod["uid"]: pod for pod in request["pods"]}
        seen: set[str] = set()
        cpu: dict[str, tuple[int, int]] = {}
        for pod in summary["pods"]:
            ref = pod["podRef"]
            uid = ref["uid"]
            if (uid not in expected or uid in seen or ref["namespace"] != self._namespace()
                    or ref["name"] != expected[uid]["name"]):
                raise ObservationError("identity")
            seen.add(uid)
            identities = {c["name"]: c for c in expected[uid]["containers"]}
            containers: set[str] = set()
            for container in pod.get("containers", []):
                name, identity = container["name"], container["_container_id"]
                created = _stamp(container["startTime"])
                started = _stamp(container.get("_started_at"))
                expected_started = _stamp(identities.get(name, {}).get("started_at"))
                if (name in containers or name not in identities
                        or identity != identities[name]["container_id"]
                        or created is None or created <= 0 or created % 10**9 != 0
                        or started is None or expected_started is None or created > started
                        or started // 10**9 != expected_started // 10**9):
                    raise ObservationError("identity")
                containers.add(name)
                # A helper must never persist arbitrary CRI metadata or paths.
                for key in list(container):
                    if key not in {"name", "startTime", "_container_id", "cpu", "memory", "rootfs", "logs"}:
                        del container[key]
                for kind, fields in (
                    ("cpu", {"time", "usageCoreNanoSeconds", "usageNanoCores"}),
                    ("memory", {"time", "usageBytes"}),
                    ("rootfs", {"time", "usedBytes"}), ("logs", {"time", "usedBytes"}),
                ):
                    if kind in container:
                        values = container[kind]
                        if not isinstance(values, dict):
                            raise ObservationError("payload")
                        for key in list(values):
                            if key not in fields or values[key] is None:
                                del values[key]
                            elif key == "time":
                                if _stamp(values[key]) is None:
                                    raise ObservationError("payload")
                            elif not _integer(values[key]):
                                raise ObservationError("payload")
                        if not any(key != "time" for key in values):
                            del container[kind]
                for field in ("cpu", "memory", "rootfs", "logs"):
                    if field in container and _stamp(container[field].get("time")) is None:
                        raise ObservationError("payload")
                for field in ("cpu", "memory"):
                    if field in container:
                        at = _stamp(container[field].get("time"))
                        if at is None or not -5 <= completed.timestamp() - at / 10**9 <= 60:
                            del container[field]
                sample = container.get("cpu", {})
                at, usage = _stamp(sample.get("time")), sample.get("usageCoreNanoSeconds")
                if at is not None and _integer(usage):
                    previous = self._cpu.get(identity)
                    if not _integer(sample.get("usageNanoCores")) and previous is not None:
                        before_at, before_usage = previous
                        elapsed, delta = at - before_at, usage - before_usage
                        if elapsed > 0 and delta >= 0:
                            rate = delta * 10**9 // elapsed
                            if _integer(rate):
                                sample["usageNanoCores"] = rate
                    cpu[identity] = previous if previous is not None and at < previous[0] else (at, usage)
            for key in list(ref):
                if key not in {"uid", "namespace", "name"}:
                    del ref[key]
            for key in list(pod):
                if key not in {"podRef", "containers", "ephemeral-storage", "_diagnostic_codes"}:
                    del pod[key]
            if "_diagnostic_codes" in pod:
                codes = pod["_diagnostic_codes"]
                if (not isinstance(codes, list) or len(codes) > 32
                        or any(not isinstance(code, str) or re.fullmatch(r"[a-z_]{1,64}", code) is None
                               for code in codes)):
                    raise ObservationError("payload")
            if "ephemeral-storage" in pod:
                volume = pod["ephemeral-storage"]
                if not isinstance(volume, dict) or not _integer(volume.get("usedBytes")):
                    raise ObservationError("payload")
                for key in list(volume):
                    if key not in {"usedBytes", "time"}:
                        del volume[key]
                if _stamp(volume.get("time")) is None:
                    raise ObservationError("payload")
        if track_cpu:
            self._cpu = cpu
        for key in list(summary):
            if key != "pods":
                del summary[key]
        summary["node"] = {"nodeName": self.settings.node_name}

    def _qualified(self, request: dict[str, Any], timeout: float, captured: datetime, started: float,
                   *, track_cpu: bool = True) -> dict[str, Any]:
        summary = self._sample(request, timeout)
        try:
            completed = captured + timedelta(seconds=max(0, time.monotonic() - started))
            self._qualify(summary, request, captured, completed, track_cpu=track_cpu)
        except (KeyError, TypeError, AttributeError, ValueError):
            raise ObservationError("payload") from None
        return summary

    def _summarize(self, request: dict[str, Any], captured: datetime, started: float, deadline: float,
                   remaining: Any) -> dict[str, Any]:
        """One helper call for the whole inventory (the Trial contract)."""
        return self._qualified(request, remaining(10), captured, started)

    def collect_once(self, *, now: datetime | None = None) -> None:
        captured = now or datetime.now(UTC)
        started = time.monotonic()
        deadline = started + self.deadline_seconds

        def remaining(maximum: float = 5) -> float:
            result = min(maximum, deadline - time.monotonic())
            if result <= 0 or not math.isfinite(result):
                raise ObservationError("deadline")
            return result

        node = self.core.read_node(name=self.settings.node_name, _request_timeout=remaining())
        if node.metadata.name != self.settings.node_name or node.metadata.deletion_timestamp is not None:
            raise ObservationError("identity")
        node_uid = _uuid(node.metadata.uid)
        producer = self.core.read_namespaced_pod(name=self.settings.pod_name,
            namespace=self.settings.namespace, _request_timeout=remaining())
        if (producer.metadata.uid != self.settings.pod_uid
                or producer.metadata.namespace != self.settings.namespace
                or producer.spec.node_name != self.settings.node_name
                or producer.metadata.deletion_timestamp is not None
                or not any(ref.kind == "DaemonSet" and ref.name == "loom-native-resource-observer"
                           and ref.controller is True for ref in producer.metadata.owner_references or [])):
            raise ObservationError("identity")
        inventory = self.core.list_namespaced_pod(
            namespace=self._namespace(), label_selector="=".join(self._selector()),
            field_selector="spec.nodeName=" + self.settings.node_name,
            _request_timeout=remaining(), limit=self._max_pods() + 1)
        if getattr(inventory.metadata, "_continue", None):
            raise ObservationError("size")
        request = self._request(inventory.items)
        summary = self._summarize(request, captured, started, deadline, remaining)
        body: dict[str, Any] = {"schema_version": self.schema,
                "node_name": self.settings.node_name, "node_uid": node_uid,
                "producer_pod_name": self.settings.pod_name,
                "producer_pod_uid": _uuid(self.settings.pod_uid),
                "observed_at": captured.isoformat(), "summary": summary}
        if self.include_host_usage:
            host_usage, self._host_cpu = capture_host_usage(
                Path("/var/run/loom-node/cpu.stat"),
                Path("/var/run/loom-node/meminfo"),
                self.settings.pod_logs_root,
                self._host_cpu,
            )
            body["host_usage"] = host_usage.model_dump(mode="json")
        serialized = json.dumps(body, separators=(",", ":"), allow_nan=False)
        if len(serialized.encode()) > _LIMIT and "host_usage" in body:
            # Optional host telemetry cannot displace the existing Trial data.
            del body["host_usage"]
            serialized = json.dumps(body, separators=(",", ":"), allow_nan=False)
        if len(serialized.encode()) > _LIMIT:
            raise ObservationError("size")
        name = self.config_map_prefix + node_uid
        record: dict[str, Any] = {"apiVersion": "v1", "kind": "ConfigMap",
                  "metadata": {"name": name, "namespace": self.settings.namespace,
                    "labels": {"app.kubernetes.io/name": "loom-native-resource-observer"},
                    "ownerReferences": [{"apiVersion": "v1", "kind": "Node",
                        "name": self.settings.node_name, "uid": node_uid,
                        "blockOwnerDeletion": False}]},
                  "data": {"snapshot.json": serialized}}
        try:
            current = self.core.read_namespaced_config_map(name=name, namespace=self.settings.namespace,
                                                          _request_timeout=remaining())
        except Exception as error:
            if getattr(error, "status", None) != 404:
                raise
            self.core.create_namespaced_config_map(namespace=self.settings.namespace, body=record,
                                                   _request_timeout=remaining())
        else:
            record["metadata"]["resourceVersion"] = current.metadata.resource_version
            self.core.replace_namespaced_config_map(name=name, namespace=self.settings.namespace,
                                                     body=record, _request_timeout=remaining())


class BuildResourceObserver(NodeResourceObserver):
    """Builder Pods in the build namespace, published to their own ConfigMap.

    It shares only the trusted helper and the observer's identity. Its own
    instance state, deadline and schedule keep Trial collection independent.
    Pods are rotated each cycle so one large tree cannot keep later Pods unseen.
    """

    schema = BUILD_SCHEMA
    config_map_prefix = BUILD_CONFIG_MAP_PREFIX
    include_host_usage = False
    deadline_seconds = 25.0

    def __init__(self, settings: ObserverSettings, core: Any) -> None:
        if settings.build_namespace is None:
            raise ValueError("builder collection requires an explicit build namespace")
        super().__init__(settings, core)
        self._cycle = 0

    def _namespace(self) -> str:
        assert self.settings.build_namespace is not None
        return self.settings.build_namespace

    def _selector(self) -> tuple[str, str]:
        return _BUILD_LABEL

    def _max_pods(self) -> int:
        return self.settings.build_max_pods

    def _order(self, selected: list[dict[str, Any]]) -> list[dict[str, Any]]:
        if not selected:
            return selected
        start = self._cycle % len(selected)
        self._cycle += 1
        return selected[start:] + selected[:start]

    def _summarize(self, request: dict[str, Any], captured: datetime, started: float, deadline: float,
                   remaining: Any) -> dict[str, Any]:
        """Counters for every Pod first, then each Pod's storage walk in its own bounded call.

        A walk that reaches the helper's deadline (or the cycle's) leaves that Pod's
        storage unknown with a fixed reason; it never discards another Pod's
        counters or storage. Pods are rotated, so a slow tree goes first only
        once per inventory size.
        """
        summary = self._qualified({**request, "pod_filesystem": "skip"}, remaining(10), captured, started)
        pods = {pod["podRef"]["uid"]: pod for pod in summary["pods"]}
        for pod in pods.values():
            pod.pop("ephemeral-storage", None)
            pod.pop("_diagnostic_codes", None)
        for requested in request["pods"]:
            pod = pods.get(requested["uid"])
            if pod is None:
                continue
            budget = min(10.0, deadline - time.monotonic() - _BUILD_PUBLISH_RESERVE)
            if budget < 1:
                pod["_diagnostic_codes"] = ["storage_not_reached"]
                continue
            try:
                (walked,) = self._qualified({**request, "pods": [requested]}, budget, captured, started,
                                            track_cpu=False)["pods"]
            except HelperError as error:
                pod["_diagnostic_codes"] = [error.code]
                continue
            except (ObservationError, ValueError):
                pod["_diagnostic_codes"] = ["storage_unavailable"]
                continue
            if "ephemeral-storage" in walked:
                pod["ephemeral-storage"] = walked["ephemeral-storage"]
            elif walked.get("_diagnostic_codes"):
                pod["_diagnostic_codes"] = walked["_diagnostic_codes"]
            else:
                pod["_diagnostic_codes"] = ["storage_unavailable"]
        return summary


def _run_builds(settings: ObserverSettings, make_core: Any, stop: Any) -> None:
    """Independent builder loop; its failures are logged and never reach Trial collection."""
    observer = BuildResourceObserver(settings, make_core())
    while not stop.is_set():
        started = time.monotonic()
        try:
            observer.collect_once()
        except Exception as error:
            reason = str(error) if isinstance(error, ObservationError) else "collection"
            _LOG.warning("Native build observer unavailable (%s)", reason)
        stop.wait(max(0.25, settings.build_interval_seconds - (time.monotonic() - started)))


def _quiet_transport_logs() -> None:
    # Retry warnings can contain a request URL. Our collection boundary emits
    # only fixed safe reasons, including when the API transport is unavailable.
    for name in ("urllib3", "kubernetes.client"):
        transport = logging.getLogger(name)
        transport.handlers = [logging.NullHandler()]
        transport.propagate = False


def _preserve_api_origin(configuration: Any, endpoint: str | None) -> None:
    if endpoint is None:
        return
    configuration.host = endpoint
    refresh = configuration.refresh_api_key_hook
    if refresh is None:
        return

    def refresh_with_origin(current: Any) -> None:
        try:
            refresh(current)
        finally:
            # The SDK restores its service origin and replaces this hook on
            # every refresh. Keep token/CA handling, then restore both bindings.
            current.host = endpoint
            current.refresh_api_key_hook = refresh_with_origin

    configuration.refresh_api_key_hook = refresh_with_origin


def main() -> None:
    from kubernetes import client, config

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    _quiet_transport_logs()
    settings = ObserverSettings()
    config.load_incluster_config()
    configuration = client.Configuration.get_default_copy()
    _preserve_api_origin(configuration, settings.kubernetes_endpoint)
    api_client = client.ApiClient(configuration)
    observer = NodeResourceObserver(settings, client.CoreV1Api(api_client))
    stop = threading.Event()
    clients = [api_client]
    if settings.build_namespace is not None:
        def build_core() -> Any:
            # A separate client: builder requests never share a connection pool with Trial collection.
            build_client = client.ApiClient(configuration)
            clients.append(build_client)
            return client.CoreV1Api(build_client)

        threading.Thread(target=_run_builds, args=(settings, build_core, stop), name="build-observer",
                         daemon=True).start()
    try:
        while True:
            started = time.monotonic()
            try:
                observer.collect_once()
            except Exception as error:
                reason = str(error) if isinstance(error, ObservationError) else "collection"
                _LOG.warning("Native resource observer unavailable (%s)", reason)
            time.sleep(max(0.25, settings.interval_seconds - (time.monotonic() - started)))
    finally:
        stop.set()
        for current in clients:
            current.close()


if __name__ == "__main__":
    main()

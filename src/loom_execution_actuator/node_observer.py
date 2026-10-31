"""Publish bounded CRI/filesystem observations without an inbound node service."""

from __future__ import annotations

import json
import logging
import math
import re
import subprocess
import time
from datetime import UTC, datetime, timedelta
from typing import Any
from urllib.parse import urlsplit
from uuid import UUID

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

_LOG = logging.getLogger(__name__)
_LIMIT = 700 * 1024
_SELECTOR = "app.kubernetes.io/managed-by=loom-execution-actuator"
_ID = re.compile(r"containerd://[a-f0-9]{8,128}\Z")


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
    def __init__(self, settings: ObserverSettings, core: Any) -> None:
        self.settings, self.core = settings, core
        self._cpu: dict[str, tuple[int, int]] = {}

    def _request(self, pods: list[Any]) -> dict[str, Any]:
        selected: list[dict[str, Any]] = []
        for pod in pods:
            meta, spec = pod.metadata, pod.spec
            if (meta.namespace != self.settings.execution_namespace
                    or spec.node_name != self.settings.node_name
                    or (meta.labels or {}).get("app.kubernetes.io/managed-by") != "loom-execution-actuator"):
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
        if len(selected) > self.settings.max_pods:
            raise ObservationError("size")
        return {"schema_version": "loom.node-resource-request.v1",
                "namespace": self.settings.execution_namespace, "pods": selected}

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
            if result.returncode or len(result.stdout) > _LIMIT:
                raise ObservationError("helper")
            value = json.loads(result.stdout, object_pairs_hook=_json_object,
                               parse_constant=_invalid_number)
            if not isinstance(value, dict) or not isinstance(value.get("pods"), list):
                raise ObservationError("payload")
            return value
        except (OSError, subprocess.TimeoutExpired, ValueError):
            raise ObservationError("helper") from None

    def _qualify(
        self, summary: dict[str, Any], request: dict[str, Any], captured: datetime, completed: datetime
    ) -> None:
        expected = {pod["uid"]: pod for pod in request["pods"]}
        seen: set[str] = set()
        cpu: dict[str, tuple[int, int]] = {}
        for pod in summary["pods"]:
            ref = pod["podRef"]
            uid = ref["uid"]
            if (uid not in expected or uid in seen or ref["namespace"] != self.settings.execution_namespace
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
        self._cpu = cpu
        for key in list(summary):
            if key != "pods":
                del summary[key]
        summary["node"] = {"nodeName": self.settings.node_name}

    def collect_once(self, *, now: datetime | None = None) -> None:
        captured = now or datetime.now(UTC)
        started = time.monotonic()
        deadline = started + 25

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
            namespace=self.settings.execution_namespace, label_selector=_SELECTOR,
            field_selector="spec.nodeName=" + self.settings.node_name,
            _request_timeout=remaining(), limit=self.settings.max_pods + 1)
        if getattr(inventory.metadata, "_continue", None):
            raise ObservationError("size")
        request = self._request(inventory.items)
        summary = self._sample(request, remaining(10))
        try:
            completed = captured + timedelta(seconds=max(0, time.monotonic() - started))
            self._qualify(summary, request, captured, completed)
        except (KeyError, TypeError, AttributeError, ValueError):
            raise ObservationError("payload") from None
        body = {"schema_version": "loom.native-resource-observation.v1",
                "node_name": self.settings.node_name, "node_uid": node_uid,
                "producer_pod_name": self.settings.pod_name,
                "producer_pod_uid": _uuid(self.settings.pod_uid),
                "observed_at": captured.isoformat(), "summary": summary}
        serialized = json.dumps(body, separators=(",", ":"), allow_nan=False)
        if len(serialized.encode()) > _LIMIT:
            raise ObservationError("size")
        name = "node-" + node_uid
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


def _quiet_transport_logs() -> None:
    # Retry warnings can contain a request URL. Our collection boundary emits
    # only fixed safe reasons, including when the API transport is unavailable.
    for name in ("urllib3", "kubernetes.client"):
        transport = logging.getLogger(name)
        transport.handlers = [logging.NullHandler()]
        transport.propagate = False


def main() -> None:
    from kubernetes import client, config

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    _quiet_transport_logs()
    settings = ObserverSettings()
    config.load_incluster_config()
    configuration = client.Configuration.get_default_copy()
    if settings.kubernetes_endpoint:
        configuration.host = settings.kubernetes_endpoint
    api_client = client.ApiClient(configuration)
    observer = NodeResourceObserver(settings, client.CoreV1Api(api_client))
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
        api_client.close()


if __name__ == "__main__":
    main()

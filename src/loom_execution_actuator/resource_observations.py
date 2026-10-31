"""Read bounded node observations through the verified Kubernetes API only."""

from __future__ import annotations

import json
import math
import re
from datetime import UTC, datetime
from time import monotonic
from typing import Any, Never, cast
from uuid import UUID

_SCHEMA = "loom.native-resource-observation.v1"
_OBSERVER = "loom-native-resource-observer"
_MAX_BYTES = 700 * 1024
_FUTURE_SKEW_SECONDS = 5
_NAME = re.compile(r"[a-z0-9](?:[a-z0-9.-]*[a-z0-9])?")


def _fail(reason: str) -> Never:
    raise ValueError("native_observation_" + reason)


def _name(value: Any, limit: int = 253) -> bool:
    return isinstance(value, str) and len(value) <= limit and _NAME.fullmatch(value) is not None


def _uid(value: Any) -> bool:
    try:
        return isinstance(value, str) and str(UUID(value)) == value and UUID(value).int != 0
    except (ValueError, AttributeError):
        return False


def _timestamp(value: Any) -> datetime:
    try:
        parsed = (
            datetime.fromisoformat(value.replace("Z", "+00:00")) if isinstance(value, str) else None
        )
        if parsed is not None and parsed.utcoffset() is not None:
            return parsed
    except ValueError:
        pass
    _fail("malformed")


def _fresh(value: Any, now: datetime, max_age_seconds: float) -> None:
    age = (now - _timestamp(value)).total_seconds()
    if age < -_FUTURE_SKEW_SECONDS or age > max_age_seconds:
        _fail("stale")


def _object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            _fail("malformed")
        result[key] = value
    return result


def _reject_constant(_: str) -> None:
    _fail("malformed")


def _validate_summary(
    summary: Any, *, node_name: str, execution_namespace: str, now: datetime, max_age_seconds: float
) -> dict[str, Any]:
    if not isinstance(summary, dict) or not isinstance(summary.get("node"), dict):
        _fail("malformed")
    if summary["node"].get("nodeName") != node_name:
        _fail("identity")
    pods = summary.get("pods")
    if not isinstance(pods, list):
        _fail("malformed")
    # Apply field expiry only after every Pod has passed structural/identity
    # validation. This also serves cached summaries without rewriting clocks.
    expired_fields: list[tuple[dict[str, Any], str]] = []
    seen_pods: set[str] = set()
    for pod in pods:
        if not isinstance(pod, dict) or not isinstance(pod.get("podRef"), dict):
            _fail("malformed")
        ref = pod["podRef"]
        if ref.get("namespace") != execution_namespace:
            _fail("identity")
        uid = ref.get("uid")
        if not isinstance(uid, str) or not uid or len(uid) > 128 or uid in seen_pods:
            _fail("malformed")
        seen_pods.add(uid)
        containers = pod.get("containers")
        if not isinstance(containers, list):
            _fail("malformed")
        seen_containers: set[str] = set()
        storage_expired = False
        for container in containers:
            if not isinstance(container, dict):
                _fail("malformed")
            name = container.get("name")
            if not _name(name, 63) or name in seen_containers:
                _fail("malformed")
            seen_containers.add(cast(str, name))
            started = _timestamp(container.get("startTime"))
            if (started - now).total_seconds() > _FUTURE_SKEW_SECONDS:
                _fail("stale")
            for kind, fields in (
                ("cpu", ("usageCoreNanoSeconds", "usageNanoCores")),
                ("memory", ("usageBytes",)),
                ("rootfs", ("usedBytes",)),
                ("logs", ("usedBytes",)),
            ):
                values = container.get(kind)
                if values is None:
                    continue
                if not isinstance(values, dict):
                    _fail("malformed")
                for field in fields:
                    value = values.get(field)
                    if value is not None and (
                        type(value) is not int or value < 0 or value >= 2**63
                    ):
                        _fail("malformed")
                if values:
                    observed = _timestamp(values.get("time"))
                    if observed < started:
                        _fail("stale")  # A different incarnation is not field expiry.
                    age = (now - observed).total_seconds()
                    if age < -_FUTURE_SKEW_SECONDS or age > max_age_seconds:
                        expired_fields.append((container, kind))
                        if kind in {"rootfs", "logs"}:
                            storage_expired = True
        storage = pod.get("ephemeral-storage")
        if storage is not None:
            if not isinstance(storage, dict):
                _fail("malformed")
            value = storage.get("usedBytes")
            if value is not None and (type(value) is not int or value < 0 or value >= 2**63):
                _fail("malformed")
            if value is not None or "time" in storage:
                age = (now - _timestamp(storage.get("time"))).total_seconds()
                if age < -_FUTURE_SKEW_SECONDS or age > max_age_seconds:
                    storage_expired = True
        if storage_expired:
            # The aggregate's original oldest-component clock also covers
            # volumes. A stale rootfs/logs member cannot leave a fresh total.
            expired_fields.append((pod, "ephemeral-storage"))
    for record, key in expired_fields:
        record.pop(key, None)
    return cast(dict[str, Any], summary)


def validate_cached_resource_observation(
    summary: dict[str, Any],
    *,
    node_name: str,
    execution_namespace: str,
    now: datetime,
) -> None:
    """Expire cached fields in place without renewing any original clock.

    A new producer snapshot replaces the cache. Within one cached snapshot,
    discarded fields cannot regain freshness; the original global observation
    still expires even after all individual fields have been removed.
    """
    metadata = summary.get("_native_observation")
    if not isinstance(metadata, dict):
        _fail("malformed")
    age = metadata.get("max_age_seconds")
    if type(age) not in {int, float}:
        _fail("malformed")
    age = cast(float, age)
    if not math.isfinite(age) or not 15 <= age <= 120:
        _fail("malformed")
    _fresh(metadata.get("observed_at"), now, age)
    _validate_summary(
        summary,
        node_name=node_name,
        execution_namespace=execution_namespace,
        now=now,
        max_age_seconds=age,
    )


def read_resource_observation(
    core_api: Any,
    *,
    node_name: str,
    namespace: str,
    execution_namespace: str,
    expected_node_uid: str | None = None,
    max_age_seconds: float = 60,
    timeout_seconds: float = 10,
    now: datetime | None = None,
) -> dict[str, Any]:
    """GET the exact Node, its ConfigMap and the bound producer Pod.

    No list, kubelet connection, proxy, or fallback is performed. All three API
    reads share one ten-second monotonic budget and the client's existing TLS
    authority. Invalid observations produce only fixed, secret-safe reasons.
    """
    if (
        type(timeout_seconds) not in {int, float}
        or not math.isfinite(timeout_seconds)
        or not 0 < timeout_seconds <= 10
        or type(max_age_seconds) not in {int, float}
        or not math.isfinite(max_age_seconds)
        or not 15 <= max_age_seconds <= 120
    ):
        _fail("malformed")
    if (
        not _name(node_name)
        or not _name(namespace, 63)
        or not _name(execution_namespace, 63)
        or (expected_node_uid is not None and not _uid(expected_node_uid))
    ):
        _fail("identity")
    current = now or datetime.now(UTC)
    if current.utcoffset() is None:
        _fail("malformed")
    configuration = core_api.api_client.configuration
    if configuration.verify_ssl is not True or not configuration.ssl_ca_cert:
        _fail("identity")
    deadline = monotonic() + timeout_seconds

    def remaining() -> float:
        value = deadline - monotonic()
        if value <= 0:
            raise TimeoutError("native observation deadline exceeded")
        return value

    node = core_api.read_node(name=node_name, _request_timeout=remaining())
    uid = node.metadata.uid
    if (
        node.metadata.name != node_name
        or not _uid(uid)
        or node.metadata.deletion_timestamp is not None
        or (expected_node_uid is not None and uid != expected_node_uid)
    ):
        _fail("identity")
    cm = core_api.read_namespaced_config_map(
        name="node-" + uid,
        namespace=namespace,
        _request_timeout=remaining(),
    )
    if (
        cm.metadata.name != "node-" + uid
        or cm.metadata.namespace != namespace
        or cm.metadata.deletion_timestamp is not None
        or not any(
            owner.api_version == "v1"
            and owner.kind == "Node"
            and owner.name == node_name
            and owner.uid == uid
            for owner in (cm.metadata.owner_references or [])
        )
    ):
        _fail("identity")
    raw = (cm.data or {}).get("snapshot.json")
    if not isinstance(raw, str):
        _fail("malformed")
    try:
        too_large = len(raw.encode("utf-8")) > _MAX_BYTES
    except UnicodeError:
        _fail("malformed")
    if too_large:
        _fail("malformed")
    try:
        snapshot = json.loads(raw, object_pairs_hook=_object, parse_constant=_reject_constant)
    except (ValueError, TypeError, RecursionError, UnicodeError):
        _fail("malformed")
    if not isinstance(snapshot, dict) or snapshot.get("schema_version") != _SCHEMA:
        _fail("malformed")
    if snapshot.get("node_name") != node_name or snapshot.get("node_uid") != uid:
        _fail("identity")
    _fresh(snapshot.get("observed_at"), current, max_age_seconds)
    producer_name, producer_uid = (
        snapshot.get("producer_pod_name"),
        snapshot.get("producer_pod_uid"),
    )
    if not _name(producer_name) or not _uid(producer_uid):
        _fail("identity")
    producer = core_api.read_namespaced_pod(
        name=producer_name,
        namespace=namespace,
        _request_timeout=remaining(),
    )
    if (
        producer.metadata.name != producer_name
        or producer.metadata.namespace != namespace
        or producer.metadata.uid != producer_uid
        or producer.metadata.deletion_timestamp is not None
        or producer.spec.node_name != node_name
        or not any(
            owner.api_version == "apps/v1"
            and owner.kind == "DaemonSet"
            and owner.name == _OBSERVER
            and owner.controller is True
            and _uid(owner.uid)
            for owner in (producer.metadata.owner_references or [])
        )
    ):
        _fail("identity")
    remaining()
    summary = _validate_summary(
        snapshot.get("summary"),
        node_name=node_name,
        execution_namespace=execution_namespace,
        now=current,
        max_age_seconds=max_age_seconds,
    )
    # This bounded metadata is derived here, never accepted from the producer.
    summary["_native_observation"] = {
        "observed_at": snapshot["observed_at"],
        "max_age_seconds": max_age_seconds,
    }
    return summary
